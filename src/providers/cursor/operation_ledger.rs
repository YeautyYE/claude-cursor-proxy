//! Crash-safe replay and ownership guard for Cursor live operations.
//!
//! The in-memory live registry owns active process state. This ledger records
//! the durable owner of the one active sampling stage plus a bounded replay
//! history for completed stages. Every mutation is compare-and-set by a unique
//! owner token so stale request futures cannot clear or overwrite newer work.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

const RECORD_VERSION: u32 = 1;
const MAX_RECORD_BYTES: u64 = 64 * 1024;
const MAX_KEY_BYTES: usize = 4 * 1024;
const MAX_OWNER_BYTES: usize = 512;
const MAX_MESSAGE_BYTES: usize = 4 * 1024;
const DEFAULT_MAX_RECORDS: usize = 10_000;
const DEFAULT_MAX_COMPLETED_PER_KEY: usize = 256;
const DEFAULT_COMPLETED_RETENTION_SECS: u64 = 7 * 24 * 60 * 60;
const MIN_UNRESOLVED_RETENTION_SECS: u64 = 24 * 60 * 60;
const MAX_RETENTION_SECS: u64 = 30 * 24 * 60 * 60;
const STALE_TEMP_AGE: Duration = Duration::from_secs(60 * 60);
const MAX_RECENT_TEMP_FILES: usize = 64;

static LEDGER_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static PROCESS_LOCKS: LazyLock<Mutex<HashMap<PathBuf, File>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PROCESS_INSTANCE_ID: LazyLock<String> = LazyLock::new(|| uuid::Uuid::new_v4().to_string());

#[cfg(test)]
pub(crate) static OPERATION_LEDGER_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OperationAdmission {
    Allowed,
    DuplicateCompleted,
    Ambiguous(String),
    Unavailable(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum ActiveState {
    Prepared,
    Dispatched,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct ActiveOperation {
    fingerprint: u64,
    owner_token: String,
    process_instance_id: String,
    state: ActiveState,
    message: Option<String>,
    recorded_at_ms: u64,
    expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CompletedOperation {
    fingerprint: u64,
    completed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RecordPayload {
    operation_key: String,
    active: Option<ActiveOperation>,
    completed: Vec<CompletedOperation>,
    updated_at_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredRecord {
    version: u32,
    payload: RecordPayload,
    checksum: String,
}

// Test-only, thread-scoped ledger directory. The old process-global
// `CCP_CURSOR_OPERATION_DIR` env override enabled the ledger for EVERY
// concurrently running test while one ledger test held it, making unrelated
// registry/driver tests fail with "durable operation owner changed".
#[cfg(test)]
thread_local! {
    static TEST_OPERATION_DIR: std::cell::RefCell<Option<PathBuf>> =
        const { std::cell::RefCell::new(None) };
}

/// Scope the ledger to the current test thread; restores on drop.
#[cfg(test)]
pub(crate) struct TestOperationDirGuard(Option<PathBuf>);

#[cfg(test)]
pub(crate) fn test_operation_dir_guard(path: &std::path::Path) -> TestOperationDirGuard {
    TestOperationDirGuard(TEST_OPERATION_DIR.with(|dir| dir.replace(Some(path.to_path_buf()))))
}

#[cfg(test)]
impl Drop for TestOperationDirGuard {
    fn drop(&mut self) {
        let previous = self.0.take();
        TEST_OPERATION_DIR.with(|dir| {
            *dir.borrow_mut() = previous;
        });
    }
}

#[cfg(test)]
fn operation_dir() -> Option<PathBuf> {
    TEST_OPERATION_DIR.with(|dir| dir.borrow().clone())
}

#[cfg(not(test))]
fn operation_dir() -> Option<PathBuf> {
    if let Ok(raw) = std::env::var("CCP_CURSOR_OPERATION_DIR") {
        let path = PathBuf::from(raw);
        if !path.as_os_str().is_empty() {
            return Some(path);
        }
    }
    // Opt-in until durable completion is gated on downstream delivery.
    // Sealing "completed" before the client observed the terminal output
    // turns a dropped response into a permanent duplicate-replay refusal,
    // which stalls legitimate client retries (2026-08-22 incident).
    let enabled = std::env::var("CCP_CURSOR_OPERATION_LEDGER")
        .map(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    if !enabled {
        return None;
    }
    Some(crate::paths::state_dir().join("cursor").join("operations"))
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn ledger_max_records() -> usize {
    env_usize("CCP_CURSOR_OPERATION_MAX_FILES", DEFAULT_MAX_RECORDS)
}

fn max_completed_per_key() -> usize {
    env_usize(
        "CCP_CURSOR_OPERATION_COMPLETED_PER_KEY",
        DEFAULT_MAX_COMPLETED_PER_KEY,
    )
    .min(4_096)
}

fn unresolved_retention_ms() -> u64 {
    env_u64(
        "CCP_CURSOR_OPERATION_UNRESOLVED_SECS",
        MIN_UNRESOLVED_RETENTION_SECS,
    )
    .clamp(MIN_UNRESOLVED_RETENTION_SECS, MAX_RETENTION_SECS)
    .saturating_mul(1_000)
}

fn completed_retention_ms() -> u64 {
    env_u64(
        "CCP_CURSOR_OPERATION_COMPLETED_RETENTION_SECS",
        DEFAULT_COMPLETED_RETENTION_SECS,
    )
    .clamp(MIN_UNRESOLVED_RETENTION_SECS, MAX_RETENTION_SECS)
    .saturating_mul(1_000)
    .max(unresolved_retention_ms())
}

fn record_path(dir: &Path, operation_key: &str) -> PathBuf {
    let digest = Sha256::digest(operation_key.as_bytes());
    dir.join(format!("{digest:x}.json"))
}

fn checksum(payload: &RecordPayload) -> io::Result<String> {
    let bytes = serde_json::to_vec(payload).map_err(io::Error::other)?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}

fn validate_identity(operation_key: &str, owner_token: Option<&str>) -> io::Result<()> {
    if operation_key.is_empty() || operation_key.len() > MAX_KEY_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Cursor operation key is empty or exceeds its safety limit",
        ));
    }
    if owner_token.is_some_and(|owner| owner.is_empty() || owner.len() > MAX_OWNER_BYTES) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Cursor operation owner is empty or exceeds its safety limit",
        ));
    }
    Ok(())
}

fn sync_directory(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

fn create_dir_all_durable(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Cursor operation directory has no parent",
        )
    })?;
    if !parent.is_dir() {
        create_dir_all_durable(parent)?;
    }
    match std::fs::create_dir(path) {
        Ok(()) => {
            sync_directory(parent)?;
            sync_directory(path)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        Err(error) => Err(error),
    }
}

fn prepare_storage_dir(dir: &Path) -> io::Result<PathBuf> {
    create_dir_all_durable(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    dir.canonicalize()
}

fn ensure_single_process_owner(dir: &Path) -> io::Result<()> {
    let mut locks = PROCESS_LOCKS
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if locks.contains_key(dir) {
        return Ok(());
    }
    let lock_path = dir.join(".instance.lock");
    // The file only carries the flock; its contents are never read.
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o600))?;
    }
    fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "another claude-cursor-proxy process owns operation state {}: {error}",
                dir.display()
            ),
        )
    })?;
    locks.insert(dir.to_path_buf(), lock_file);
    Ok(())
}

fn storage_dir() -> io::Result<Option<PathBuf>> {
    let Some(dir) = operation_dir() else {
        return Ok(None);
    };
    let dir = prepare_storage_dir(&dir)?;
    ensure_single_process_owner(&dir)?;
    Ok(Some(dir))
}

fn read_record(path: &Path, operation_key: &str) -> io::Result<Option<RecordPayload>> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if metadata.len() > MAX_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cursor operation ledger record exceeds its safety limit",
        ));
    }
    let bytes = std::fs::read(path)?;
    let stored: StoredRecord = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if stored.version != RECORD_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cursor operation ledger schema version is unsupported",
        ));
    }
    if stored.payload.operation_key != operation_key {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cursor operation ledger key mismatch",
        ));
    }
    if checksum(&stored.payload)? != stored.checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cursor operation ledger checksum mismatch",
        ));
    }
    Ok(Some(stored.payload))
}

fn read_record_by_path(path: &Path) -> io::Result<RecordPayload> {
    let metadata = std::fs::metadata(path)?;
    if metadata.len() > MAX_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cursor operation ledger record exceeds its safety limit",
        ));
    }
    let bytes = std::fs::read(path)?;
    let stored: StoredRecord = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    if stored.version != RECORD_VERSION || checksum(&stored.payload)? != stored.checksum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cursor operation ledger record is corrupt",
        ));
    }
    Ok(stored.payload)
}

fn remove_record(path: &Path, dir: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_directory(dir),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn cleanup_orphan_temps(dir: &Path) -> io::Result<()> {
    let now = SystemTime::now();
    let mut recent = 0usize;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("tmp") {
            continue;
        }
        let metadata = entry.metadata()?;
        let stale = metadata
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age >= STALE_TEMP_AGE);
        if stale {
            std::fs::remove_file(path)?;
        } else {
            recent = recent.saturating_add(1);
        }
    }
    if recent > MAX_RECENT_TEMP_FILES {
        return Err(io::Error::other(
            "Cursor operation ledger has too many recent temporary files",
        ));
    }
    Ok(())
}

fn ensure_capacity(dir: &Path, target: &Path, now_ms: u64) -> io::Result<()> {
    if target.exists() {
        return Ok(());
    }
    cleanup_orphan_temps(dir)?;
    let mut record_paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
            record_paths.push(path);
        }
    }
    if record_paths.len() < ledger_max_records() {
        return Ok(());
    }

    // Only records whose complete replay history has naturally expired may be
    // reclaimed. Never sacrifice a still-valid tombstone merely to admit work.
    let mut removed = 0usize;
    for path in record_paths {
        let mut payload = read_record_by_path(&path)?;
        normalize(&mut payload, now_ms);
        if payload.active.is_none() && payload.completed.is_empty() {
            std::fs::remove_file(path)?;
            removed = removed.saturating_add(1);
        }
    }
    if removed > 0 {
        sync_directory(dir)?;
    }
    let remaining = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                == Some("json")
        })
        .count();
    if remaining >= ledger_max_records() {
        return Err(io::Error::other(
            "Cursor operation ledger reached its bounded replay-retention capacity",
        ));
    }
    Ok(())
}

fn write_record(dir: &Path, payload: &RecordPayload) -> io::Result<()> {
    let path = record_path(dir, &payload.operation_key);
    ensure_capacity(dir, &path, now_millis())?;
    let stored = StoredRecord {
        version: RECORD_VERSION,
        payload: payload.clone(),
        checksum: checksum(payload)?,
    };
    let bytes = serde_json::to_vec(&stored).map_err(io::Error::other)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Cursor operation ledger record exceeds its safety limit",
        ));
    }
    let tmp = path.with_extension(format!("json.{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        sync_directory(dir)
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

fn persist_or_remove(dir: &Path, payload: &RecordPayload) -> io::Result<()> {
    let path = record_path(dir, &payload.operation_key);
    if payload.active.is_none() && payload.completed.is_empty() {
        remove_record(&path, dir)
    } else {
        write_record(dir, payload)
    }
}

fn empty_record(operation_key: &str, now_ms: u64) -> RecordPayload {
    RecordPayload {
        operation_key: operation_key.to_string(),
        active: None,
        completed: Vec::new(),
        updated_at_ms: now_ms,
    }
}

fn truncate_message(message: &str) -> String {
    if message.len() <= MAX_MESSAGE_BYTES {
        return message.to_string();
    }
    let mut boundary = MAX_MESSAGE_BYTES;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}…", &message[..boundary])
}

fn add_completed(payload: &mut RecordPayload, fingerprint: u64, completed_at_ms: u64) {
    payload
        .completed
        .retain(|entry| entry.fingerprint != fingerprint);
    payload.completed.push(CompletedOperation {
        fingerprint,
        completed_at_ms,
    });
    payload.completed.sort_by_key(|entry| entry.completed_at_ms);
    let excess = payload
        .completed
        .len()
        .saturating_sub(max_completed_per_key());
    if excess > 0 {
        payload.completed.drain(..excess);
    }
}

fn normalize(payload: &mut RecordPayload, now_ms: u64) -> bool {
    let before = payload.clone();
    let oldest = now_ms.saturating_sub(completed_retention_ms());
    payload
        .completed
        .retain(|entry| entry.completed_at_ms >= oldest);
    let mut seen = HashSet::new();
    payload
        .completed
        .retain(|entry| seen.insert(entry.fingerprint));

    if let Some(active) = payload.active.clone() {
        if active.state == ActiveState::Prepared
            && active.process_instance_id != *PROCESS_INSTANCE_ID
        {
            // Prepared is persisted before any network operation is polled.
            // Once its process is gone, replay is definitively safe.
            payload.active = None;
        } else if active.expires_at_ms <= now_ms {
            // The owner is certainly no longer running after this bound. The
            // outcome stays unknown, but converting it into a permanent
            // completed tombstone would refuse every later client retry of a
            // turn that may never have executed; expire to retryable instead,
            // matching the bounded in-memory ambiguity window.
            payload.active = None;
        }
    }
    let excess = payload
        .completed
        .len()
        .saturating_sub(max_completed_per_key());
    if excess > 0 {
        payload.completed.drain(..excess);
    }
    if *payload != before {
        payload.updated_at_ms = now_ms;
        true
    } else {
        false
    }
}

fn active_message(active: &ActiveOperation) -> String {
    active
        .message
        .clone()
        .unwrap_or_else(|| match active.state {
            ActiveState::Prepared => {
                "Cursor operation is owned by another active request before dispatch".into()
            }
            ActiveState::Dispatched => {
                "Cursor operation was dispatched and has no durable terminal outcome".into()
            }
            ActiveState::Ambiguous => "Cursor operation completion is ambiguous".into(),
        })
}

fn classify(payload: &RecordPayload, fingerprint: u64) -> OperationAdmission {
    if payload
        .completed
        .iter()
        .any(|entry| entry.fingerprint == fingerprint)
    {
        return OperationAdmission::DuplicateCompleted;
    }
    if let Some(active) = payload.active.as_ref() {
        return OperationAdmission::Ambiguous(active_message(active));
    }
    OperationAdmission::Allowed
}

fn load_payload(dir: &Path, operation_key: &str, now_ms: u64) -> io::Result<(RecordPayload, bool)> {
    let path = record_path(dir, operation_key);
    let mut payload =
        read_record(&path, operation_key)?.unwrap_or_else(|| empty_record(operation_key, now_ms));
    let changed = normalize(&mut payload, now_ms);
    Ok((payload, changed))
}

/// Read-only classification of an operation key. Production admission goes
/// through [`claim`]; tests use this to observe durable state across resets.
#[cfg(test)]
pub(crate) fn admit(operation_key: &str, fingerprint: u64) -> OperationAdmission {
    if let Err(error) = validate_identity(operation_key, None) {
        return OperationAdmission::Unavailable(error.to_string());
    }
    let _guard = LEDGER_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let Some(dir) = (match storage_dir() {
        Ok(dir) => dir,
        Err(error) => return OperationAdmission::Unavailable(error.to_string()),
    }) else {
        return OperationAdmission::Allowed;
    };
    let now_ms = now_millis();
    let (payload, changed) = match load_payload(&dir, operation_key, now_ms) {
        Ok(record) => record,
        Err(error) => return OperationAdmission::Unavailable(error.to_string()),
    };
    if changed && let Err(error) = persist_or_remove(&dir, &payload) {
        return OperationAdmission::Unavailable(error.to_string());
    }
    classify(&payload, fingerprint)
}

pub(crate) fn claim(
    operation_key: &str,
    fingerprint: u64,
    owner_token: &str,
) -> OperationAdmission {
    if let Err(error) = validate_identity(operation_key, Some(owner_token)) {
        return OperationAdmission::Unavailable(error.to_string());
    }
    let _guard = LEDGER_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let Some(dir) = (match storage_dir() {
        Ok(dir) => dir,
        Err(error) => return OperationAdmission::Unavailable(error.to_string()),
    }) else {
        return OperationAdmission::Allowed;
    };
    let now_ms = now_millis();
    let (mut payload, changed) = match load_payload(&dir, operation_key, now_ms) {
        Ok(record) => record,
        Err(error) => return OperationAdmission::Unavailable(error.to_string()),
    };
    // An ambiguous marker is scoped to the operation that may have crossed
    // Cursor's acceptance boundary.  The in-memory live registry can
    // atomically rotate that marker when a *different* Grok operation arrives;
    // keep the durable record in lock-step with that decision.  Without this
    // branch, the old owner remains ambiguous for the full unresolved
    // retention window and every later stage is rejected before dispatch.
    // Unknown fingerprints stay fail-closed: there is no identity proof that
    // makes replacing that marker safe.
    let can_rotate_ambiguous = payload.active.as_ref().is_some_and(|active| {
        active.state == ActiveState::Ambiguous
            && active.fingerprint != 0
            && fingerprint != 0
            && active.fingerprint != fingerprint
            && !payload
                .completed
                .iter()
                .any(|entry| entry.fingerprint == fingerprint)
    });
    let admission = if can_rotate_ambiguous {
        OperationAdmission::Allowed
    } else {
        classify(&payload, fingerprint)
    };
    if admission != OperationAdmission::Allowed {
        if changed && let Err(error) = persist_or_remove(&dir, &payload) {
            return OperationAdmission::Unavailable(error.to_string());
        }
        return admission;
    }
    payload.active = Some(ActiveOperation {
        fingerprint,
        owner_token: owner_token.to_string(),
        process_instance_id: PROCESS_INSTANCE_ID.clone(),
        state: ActiveState::Prepared,
        message: None,
        recorded_at_ms: now_ms,
        expires_at_ms: now_ms.saturating_add(unresolved_retention_ms()),
    });
    payload.updated_at_ms = now_ms;
    match write_record(&dir, &payload) {
        Ok(()) => OperationAdmission::Allowed,
        Err(error) => OperationAdmission::Unavailable(error.to_string()),
    }
}

fn transition(
    operation_key: &str,
    mutation: impl FnOnce(&mut RecordPayload, u64) -> bool,
) -> io::Result<bool> {
    validate_identity(operation_key, None)?;
    let _guard = LEDGER_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let Some(dir) = storage_dir()? else {
        return Ok(true);
    };
    let now_ms = now_millis();
    let (mut payload, normalized) = load_payload(&dir, operation_key, now_ms)?;
    let applied = mutation(&mut payload, now_ms);
    if normalized || applied {
        payload.updated_at_ms = now_ms;
        persist_or_remove(&dir, &payload)?;
    }
    Ok(applied)
}

pub(crate) fn mark_dispatched_if_owner(
    operation_key: &str,
    fingerprint: u64,
    owner_token: &str,
) -> io::Result<bool> {
    validate_identity(operation_key, Some(owner_token))?;
    transition(operation_key, |payload, now_ms| {
        let Some(active) = payload.active.as_mut() else {
            return false;
        };
        if active.fingerprint != fingerprint || active.owner_token != owner_token {
            return false;
        }
        if active.state == ActiveState::Prepared {
            active.state = ActiveState::Dispatched;
            active.recorded_at_ms = now_ms;
            active.expires_at_ms = now_ms.saturating_add(unresolved_retention_ms());
        }
        true
    })
}

pub(crate) fn transfer_owner_if(
    operation_key: &str,
    fingerprint: u64,
    previous_owner: &str,
    next_owner: &str,
) -> io::Result<bool> {
    validate_identity(operation_key, Some(previous_owner))?;
    validate_identity(operation_key, Some(next_owner))?;
    transition(operation_key, |payload, _| {
        let Some(active) = payload.active.as_mut() else {
            return false;
        };
        if active.fingerprint != fingerprint || active.owner_token != previous_owner {
            return false;
        }
        active.owner_token = next_owner.to_string();
        true
    })
}

pub(crate) fn prepare_stage_if_owner(
    operation_key: &str,
    owner_token: &str,
    previous_fingerprint: u64,
    next_fingerprint: u64,
) -> io::Result<bool> {
    validate_identity(operation_key, Some(owner_token))?;
    transition(operation_key, |payload, now_ms| {
        let Some(active) = payload.active.as_ref() else {
            return false;
        };
        if active.fingerprint != previous_fingerprint
            || active.owner_token != owner_token
            || active.state == ActiveState::Prepared
        {
            return false;
        }
        add_completed(payload, previous_fingerprint, now_ms);
        payload.active = Some(ActiveOperation {
            fingerprint: next_fingerprint,
            owner_token: owner_token.to_string(),
            process_instance_id: PROCESS_INSTANCE_ID.clone(),
            state: ActiveState::Prepared,
            message: None,
            recorded_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(unresolved_retention_ms()),
        });
        true
    })
}

pub(crate) fn rollback_stage_if_owner(
    operation_key: &str,
    owner_token: &str,
    fingerprint: u64,
    previous_fingerprint: u64,
) -> io::Result<bool> {
    validate_identity(operation_key, Some(owner_token))?;
    transition(operation_key, |payload, _| {
        let matches = payload.active.as_ref().is_some_and(|active| {
            active.fingerprint == fingerprint && active.owner_token == owner_token
        });
        if matches {
            let now_ms = now_millis();
            payload.active = Some(ActiveOperation {
                fingerprint: previous_fingerprint,
                owner_token: owner_token.to_string(),
                process_instance_id: PROCESS_INSTANCE_ID.clone(),
                state: ActiveState::Dispatched,
                message: None,
                recorded_at_ms: now_ms,
                expires_at_ms: now_ms.saturating_add(unresolved_retention_ms()),
            });
        }
        matches
    })
}

pub(crate) fn mark_completed_if_owner(
    operation_key: &str,
    fingerprint: u64,
    owner_token: &str,
) -> io::Result<bool> {
    validate_identity(operation_key, Some(owner_token))?;
    transition(operation_key, |payload, now_ms| {
        if payload
            .completed
            .iter()
            .any(|entry| entry.fingerprint == fingerprint)
        {
            return true;
        }
        let matches = payload.active.as_ref().is_some_and(|active| {
            active.fingerprint == fingerprint && active.owner_token == owner_token
        });
        if !matches {
            return false;
        }
        add_completed(payload, fingerprint, now_ms);
        payload.active = None;
        true
    })
}

pub(crate) fn mark_ambiguous_if_owner(
    operation_key: &str,
    fingerprint: u64,
    owner_token: &str,
    message: &str,
) -> io::Result<bool> {
    validate_identity(operation_key, Some(owner_token))?;
    let message = truncate_message(message);
    transition(operation_key, |payload, now_ms| {
        let Some(active) = payload.active.as_mut() else {
            return false;
        };
        if active.fingerprint != fingerprint || active.owner_token != owner_token {
            return false;
        }
        active.state = ActiveState::Ambiguous;
        active.message = Some(message);
        active.recorded_at_ms = now_ms;
        active.expires_at_ms = now_ms.saturating_add(unresolved_retention_ms());
        true
    })
}

pub(crate) fn clear_if_owner(
    operation_key: &str,
    fingerprint: u64,
    owner_token: &str,
) -> io::Result<bool> {
    validate_identity(operation_key, Some(owner_token))?;
    transition(operation_key, |payload, _| {
        let matches = payload.active.as_ref().is_some_and(|active| {
            active.fingerprint == fingerprint && active.owner_token == owner_token
        });
        if matches {
            payload.active = None;
        }
        matches
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LedgerEnv {
        _dir: TestOperationDirGuard,
        previous_retention: Option<std::ffi::OsString>,
    }

    impl LedgerEnv {
        fn new(path: &Path) -> Self {
            let previous_retention =
                std::env::var_os("CCP_CURSOR_OPERATION_COMPLETED_RETENTION_SECS");
            unsafe {
                std::env::set_var(
                    "CCP_CURSOR_OPERATION_COMPLETED_RETENTION_SECS",
                    MIN_UNRESOLVED_RETENTION_SECS.to_string(),
                );
            }
            Self {
                _dir: test_operation_dir_guard(path),
                previous_retention,
            }
        }
    }

    impl Drop for LedgerEnv {
        fn drop(&mut self) {
            unsafe {
                match self.previous_retention.take() {
                    Some(value) => {
                        std::env::set_var("CCP_CURSOR_OPERATION_COMPLETED_RETENTION_SECS", value)
                    }
                    None => std::env::remove_var("CCP_CURSOR_OPERATION_COMPLETED_RETENTION_SECS"),
                }
            }
        }
    }

    fn dispatch(key: &str, fingerprint: u64, owner: &str) {
        assert_eq!(claim(key, fingerprint, owner), OperationAdmission::Allowed);
        assert!(mark_dispatched_if_owner(key, fingerprint, owner).unwrap());
    }

    #[test]
    fn ambiguous_operation_survives_a_fresh_ledger_read() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        dispatch("session-a", 41, "owner-a");
        assert!(
            mark_ambiguous_if_owner("session-a", 41, "owner-a", "completion is ambiguous").unwrap()
        );

        assert_eq!(
            admit("session-a", 41),
            OperationAdmission::Ambiguous("completion is ambiguous".into())
        );
        assert!(matches!(
            admit("session-a", 42),
            OperationAdmission::Ambiguous(_)
        ));
    }

    #[test]
    fn different_operation_atomically_rotates_ambiguous_owner() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        dispatch("session-a-rotate", 41, "owner-a");
        assert!(
            mark_ambiguous_if_owner("session-a-rotate", 41, "owner-a", "acceptance is ambiguous")
                .unwrap()
        );

        // This is the same atomic transition performed by the in-memory live
        // registry when a later Grok stage has a different request id.
        assert_eq!(
            claim("session-a-rotate", 42, "owner-b"),
            OperationAdmission::Allowed
        );
        // The replacement is now prepared under the new owner.  A retry of
        // either the old or new operation remains fail-closed until that
        // owner reaches a terminal state.
        assert!(matches!(
            admit("session-a-rotate", 41),
            OperationAdmission::Ambiguous(_)
        ));
        assert!(matches!(
            admit("session-a-rotate", 42),
            OperationAdmission::Ambiguous(_)
        ));
        assert!(
            !mark_dispatched_if_owner("session-a-rotate", 42, "owner-a").unwrap(),
            "the superseded owner must not advance the replacement"
        );
        assert!(mark_dispatched_if_owner("session-a-rotate", 42, "owner-b").unwrap());
    }

    #[test]
    fn unknown_or_same_ambiguous_fingerprint_stays_fail_closed() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());

        dispatch("session-a-closed", 51, "owner-a");
        assert!(
            mark_ambiguous_if_owner("session-a-closed", 51, "owner-a", "acceptance is ambiguous")
                .unwrap()
        );
        assert!(matches!(
            claim("session-a-closed", 51, "owner-retry"),
            OperationAdmission::Ambiguous(_)
        ));

        dispatch("session-a-unknown", 0, "owner-zero");
        assert!(
            mark_ambiguous_if_owner(
                "session-a-unknown",
                0,
                "owner-zero",
                "acceptance is ambiguous"
            )
            .unwrap()
        );
        assert!(matches!(
            claim("session-a-unknown", 52, "owner-new"),
            OperationAdmission::Ambiguous(_)
        ));
    }

    #[test]
    fn different_fingerprint_cannot_rotate_before_acceptance_boundary() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());

        // Prepared is written before the upstream request is polled, and
        // Dispatched means bytes may have crossed Cursor's acceptance
        // boundary. Neither state can be replaced merely because a later
        // request has a different x-grok-req-id.
        assert_eq!(
            claim("session-prepared", 61, "owner-prepared"),
            OperationAdmission::Allowed
        );
        assert!(matches!(
            claim("session-prepared", 62, "owner-new"),
            OperationAdmission::Ambiguous(_)
        ));

        dispatch("session-dispatched", 71, "owner-dispatched");
        assert!(matches!(
            claim("session-dispatched", 72, "owner-new"),
            OperationAdmission::Ambiguous(_)
        ));
    }

    #[test]
    fn completed_history_blocks_delayed_replays_across_new_stages() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        dispatch("session-b", 51, "run-b1");
        assert!(mark_completed_if_owner("session-b", 51, "run-b1").unwrap());
        dispatch("session-b", 52, "run-b2");
        assert!(mark_completed_if_owner("session-b", 52, "run-b2").unwrap());

        assert_eq!(
            admit("session-b", 51),
            OperationAdmission::DuplicateCompleted
        );
        assert_eq!(
            admit("session-b", 52),
            OperationAdmission::DuplicateCompleted
        );
        assert_eq!(admit("session-b", 53), OperationAdmission::Allowed);
    }

    #[test]
    fn corrupt_ledger_record_fails_closed() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        let dir = storage_dir().unwrap().unwrap();
        std::fs::write(record_path(&dir, "invalid"), b"{not-json").unwrap();

        assert!(matches!(
            admit("invalid", 1),
            OperationAdmission::Unavailable(_)
        ));
    }

    #[test]
    fn stale_owner_cannot_clear_or_complete_newer_owner() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        dispatch("session-c", 61, "owner-old");
        assert!(transfer_owner_if("session-c", 61, "owner-old", "owner-new").unwrap());
        assert!(!clear_if_owner("session-c", 61, "owner-old").unwrap());
        assert!(!mark_completed_if_owner("session-c", 61, "owner-old").unwrap());
        assert!(matches!(
            admit("session-c", 61),
            OperationAdmission::Ambiguous(_)
        ));
        assert!(clear_if_owner("session-c", 61, "owner-new").unwrap());
        assert_eq!(admit("session-c", 61), OperationAdmission::Allowed);
    }

    #[test]
    fn prepared_record_from_dead_process_is_retryable() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        assert_eq!(
            claim("session-d", 71, "owner-d"),
            OperationAdmission::Allowed
        );
        let dir = storage_dir().unwrap().unwrap();
        let path = record_path(&dir, "session-d");
        let mut payload = read_record(&path, "session-d").unwrap().unwrap();
        payload.active.as_mut().unwrap().process_instance_id = "dead-process".into();
        write_record(&dir, &payload).unwrap();

        assert_eq!(admit("session-d", 71), OperationAdmission::Allowed);
    }

    #[test]
    fn expired_dispatched_stage_expires_to_retryable_instead_of_tombstoning() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        dispatch("session-e", 81, "owner-e");
        let dir = storage_dir().unwrap().unwrap();
        let path = record_path(&dir, "session-e");
        let mut payload = read_record(&path, "session-e").unwrap().unwrap();
        payload.active.as_mut().unwrap().expires_at_ms = 0;
        write_record(&dir, &payload).unwrap();

        // Before expiry the dispatched stage blocks replays as ambiguous; the
        // expiry bound must release the client instead of refusing the same
        // turn forever as a synthetic "completed" tombstone.
        assert_eq!(admit("session-e", 81), OperationAdmission::Allowed);
        assert_eq!(admit("session-e", 82), OperationAdmission::Allowed);
    }

    #[test]
    fn accepted_stage_rotation_retains_previous_stage_history() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        dispatch("session-f", 91, "run-f");
        assert!(prepare_stage_if_owner("session-f", "run-f", 91, 92).unwrap());
        assert!(mark_dispatched_if_owner("session-f", 92, "run-f").unwrap());
        assert!(mark_completed_if_owner("session-f", 92, "run-f").unwrap());

        assert_eq!(
            admit("session-f", 91),
            OperationAdmission::DuplicateCompleted
        );
        assert_eq!(
            admit("session-f", 92),
            OperationAdmission::DuplicateCompleted
        );
    }

    #[test]
    fn checksum_detects_valid_json_mutation() {
        let _guard = OPERATION_LEDGER_TEST_LOCK
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let _env = LedgerEnv::new(dir.path());
        dispatch("session-g", 101, "owner-g");
        let dir = storage_dir().unwrap().unwrap();
        let path = record_path(&dir, "session-g");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["payload"]["active"]["fingerprint"] = serde_json::json!(102);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();

        assert!(matches!(
            admit("session-g", 101),
            OperationAdmission::Unavailable(message)
                if message.contains("checksum")
        ));
    }
}
