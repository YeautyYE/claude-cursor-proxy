//! Persist Cursor conversation_id + checkpoint + KV blobs across Claude Code turns.
//!
//! Official CLI keeps a ConversationStateStructure (blob-ID form) plus a content-
//! addressed blob store between Run streams. Without this, each Claude turn is a
//! fresh Cursor run that re-uploads the entire Anthropic history + tools schema.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use sha2::{Digest, Sha256};

const IDLE_TTL_MS: u64 = 30 * 60 * 1000;
const MAX_CONVERSATIONS: usize = 10_000;
const PERSISTED_SWEEP_INTERVAL_MS: u64 = 60_000;
static LAST_PERSISTED_SWEEP_MS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static PERSISTED_SWEEP_RUNS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default)]
pub struct CursorConversation {
    /// Cursor conversation_id (stable UUID for this Claude session).
    pub conversation_id: String,
    /// Latest `conversation_checkpoint_update` payload (ConversationStateStructure bytes).
    pub checkpoint: Option<Vec<u8>>,
    /// KV blob store shared across Runs for this conversation.
    pub blobs: HashMap<Vec<u8>, Vec<u8>>,
    pub last_seen: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConditionalPersist {
    Saved,
    StaleBinding,
    Failed(String),
}

#[derive(Default)]
struct Store {
    map: HashMap<String, CursorConversation>,
    order: VecDeque<String>,
    pins: HashMap<String, usize>,
}

static STORE: LazyLock<Mutex<Store>> = LazyLock::new(|| Mutex::new(Store::default()));

fn store_lock() -> std::sync::MutexGuard<'static, Store> {
    STORE.lock().unwrap_or_else(|e| e.into_inner())
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn persist_dir() -> Option<PathBuf> {
    if let Ok(dir) = std::env::var("CCP_CURSOR_CONV_DIR") {
        let path = PathBuf::from(dir);
        if !path.as_os_str().is_empty() {
            return Some(path);
        }
    }
    if cfg!(test) {
        return None;
    }
    Some(
        crate::paths::state_dir()
            .join("cursor")
            .join("conversations"),
    )
}

fn persist_path(session_id: &str) -> Option<PathBuf> {
    let dir = persist_dir()?;
    let digest = Sha256::digest(session_id.as_bytes());
    Some(dir.join(format!("{digest:x}.json")))
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedConversation {
    session_id: String,
    conversation_id: String,
    checkpoint_b64: Option<String>,
    blobs_b64: Vec<(String, String)>,
    last_seen: u64,
}

fn b64_encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn b64_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

fn persist_conversation(session_id: &str, conv: &CursorConversation) -> io::Result<()> {
    let Some(path) = persist_path(session_id) else {
        return Ok(());
    };
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    let dto = PersistedConversation {
        session_id: session_id.to_string(),
        conversation_id: conv.conversation_id.clone(),
        checkpoint_b64: conv.checkpoint.as_deref().map(b64_encode),
        blobs_b64: conv
            .blobs
            .iter()
            .map(|(k, v)| (b64_encode(k), b64_encode(v)))
            .collect(),
        last_seen: conv.last_seen,
    };
    let json = serde_json::to_vec(&dto).map_err(io::Error::other)?;
    let tmp = path.with_extension(format!("json.{}.tmp", uuid::Uuid::new_v4()));
    let write_result = (|| -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
        }
        file.write_all(&json)?;
        file.sync_all()?;
        std::fs::rename(&tmp, &path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            if let Some(parent) = path.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    write_result
}

fn log_persist_failure(session_id: &str, error: &io::Error) {
    crate::logging::create_logger("cursor").warn(
        "conversation_persist_failed",
        Some(serde_json::Map::from_iter([
            ("sessionId".into(), serde_json::json!(session_id)),
            ("error".into(), serde_json::json!(error.to_string())),
        ])),
    );
}

fn expire_abandoned_persisted(now: u64) {
    #[cfg(test)]
    PERSISTED_SWEEP_RUNS.fetch_add(1, Ordering::Relaxed);
    let Some(dir) = persist_dir() else {
        return;
    };
    let pinned: HashSet<String> = {
        let store = store_lock();
        store.pins.keys().cloned().collect()
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(dto) = serde_json::from_slice::<PersistedConversation>(&bytes) else {
            continue;
        };
        if !pinned.contains(&dto.session_id) && now.saturating_sub(dto.last_seen) > IDLE_TTL_MS {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn maybe_expire_abandoned_persisted(now: u64) {
    let mut observed = LAST_PERSISTED_SWEEP_MS.load(Ordering::Acquire);
    loop {
        if observed != 0 && now.saturating_sub(observed) < PERSISTED_SWEEP_INTERVAL_MS {
            return;
        }
        match LAST_PERSISTED_SWEEP_MS.compare_exchange_weak(
            observed,
            now,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(actual) => observed = actual,
        }
    }
    expire_abandoned_persisted(now);
}

fn load_persisted(session_id: &str) -> Option<CursorConversation> {
    let path = persist_path(session_id)?;
    let bytes = std::fs::read(path).ok()?;
    let dto: PersistedConversation = serde_json::from_slice(&bytes).ok()?;
    let mut blobs = HashMap::new();
    for (k, v) in dto.blobs_b64 {
        blobs.insert(b64_decode(&k)?, b64_decode(&v)?);
    }
    Some(CursorConversation {
        conversation_id: dto.conversation_id,
        checkpoint: dto.checkpoint_b64.as_deref().and_then(b64_decode),
        blobs,
        last_seen: dto.last_seen,
    })
}

fn delete_persisted(session_id: &str) {
    if let Some(path) = persist_path(session_id) {
        let _ = std::fs::remove_file(path);
    }
}

fn touch_and_evict(store: &mut Store, session_id: &str, now: u64) {
    if let Some(entry) = store.map.get_mut(session_id) {
        entry.last_seen = now;
    }
    let mut remaining = store.order.len();
    while store.map.len() > MAX_CONVERSATIONS && remaining > 0 {
        remaining -= 1;
        if let Some(evict) = store.order.pop_front() {
            if store.pins.contains_key(&evict) {
                store.order.push_back(evict);
            } else {
                store.map.remove(&evict);
                delete_persisted(&evict);
            }
        } else {
            break;
        }
    }
    // Drop idle entries opportunistically when touched.
    let stale: Vec<String> = store
        .map
        .iter()
        .filter(|(key, v)| {
            !store.pins.contains_key(*key) && now.saturating_sub(v.last_seen) > IDLE_TTL_MS
        })
        .map(|(k, _)| k.clone())
        .collect();
    for key in stale {
        if key == session_id {
            continue;
        }
        store.map.remove(&key);
        store.order.retain(|item| item != &key);
        delete_persisted(&key);
    }
}

/// Get or create the Cursor conversation binding for a Claude session id.
pub fn get_or_create(session_id: &str) -> CursorConversation {
    let now = now_millis();
    maybe_expire_abandoned_persisted(now);
    let mut store = store_lock();
    if let Some(existing) = store.map.get(session_id).cloned() {
        if store.pins.contains_key(session_id)
            || now.saturating_sub(existing.last_seen) <= IDLE_TTL_MS
        {
            touch_and_evict(&mut store, session_id, now);
            return existing;
        }
        store.map.remove(session_id);
        store.order.retain(|item| item != session_id);
        delete_persisted(session_id);
    }
    if let Some(disk) = load_persisted(session_id) {
        if now.saturating_sub(disk.last_seen) <= IDLE_TTL_MS {
            store.order.push_back(session_id.to_string());
            store.map.insert(session_id.to_string(), disk.clone());
            touch_and_evict(&mut store, session_id, now);
            return disk;
        }
        delete_persisted(session_id);
    }
    let created = CursorConversation {
        conversation_id: uuid::Uuid::new_v4().to_string(),
        checkpoint: None,
        blobs: HashMap::new(),
        last_seen: now,
    };
    store.order.push_back(session_id.to_string());
    store.map.insert(session_id.to_string(), created.clone());
    touch_and_evict(&mut store, session_id, now);
    if let Err(error) = persist_conversation(session_id, &created) {
        log_persist_failure(session_id, &error);
    }
    created
}

pub fn get(session_id: &str) -> Option<CursorConversation> {
    let now = now_millis();
    let mut store = store_lock();
    if let Some(existing) = store.map.get(session_id).cloned() {
        if !store.pins.contains_key(session_id)
            && now.saturating_sub(existing.last_seen) > IDLE_TTL_MS
        {
            store.map.remove(session_id);
            store.order.retain(|item| item != session_id);
            delete_persisted(session_id);
            return None;
        }
        touch_and_evict(&mut store, session_id, now);
        return Some(existing);
    }
    let disk = load_persisted(session_id)?;
    if now.saturating_sub(disk.last_seen) > IDLE_TTL_MS {
        delete_persisted(session_id);
        return None;
    }
    store.order.push_back(session_id.to_string());
    store.map.insert(session_id.to_string(), disk.clone());
    touch_and_evict(&mut store, session_id, now);
    Some(disk)
}

/// Keep one live Cursor conversation binding resident until its driver exits.
///
/// The expected id prevents a stale starter from pinning a replacement binding
/// created after a reset.
pub(crate) fn pin(session_id: &str, expected_conversation_id: &str) -> Option<ConversationLease> {
    let now = now_millis();
    let mut store = store_lock();
    let entry = store.map.get_mut(session_id)?;
    if entry.conversation_id != expected_conversation_id {
        return None;
    }
    entry.last_seen = now;
    *store.pins.entry(session_id.to_string()).or_default() += 1;
    Some(ConversationLease {
        session_id: session_id.to_string(),
    })
}

#[derive(Debug)]
pub(crate) struct ConversationLease {
    session_id: String,
}

impl Drop for ConversationLease {
    fn drop(&mut self) {
        let mut store = store_lock();
        let Some(count) = store.pins.get_mut(&self.session_id) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            store.pins.remove(&self.session_id);
        }
    }
}

fn ensure_entry<'a>(
    store: &'a mut Store,
    session_id: &str,
    now: u64,
) -> &'a mut CursorConversation {
    if !store.map.contains_key(session_id) {
        store.order.push_back(session_id.to_string());
        store.map.insert(
            session_id.to_string(),
            CursorConversation {
                conversation_id: uuid::Uuid::new_v4().to_string(),
                checkpoint: None,
                blobs: HashMap::new(),
                last_seen: now,
            },
        );
    }
    store.map.get_mut(session_id).expect("just inserted")
}

/// Persist the latest checkpoint bytes for a Claude session.
pub fn save_checkpoint(session_id: &str, checkpoint: Vec<u8>) {
    if checkpoint.is_empty() {
        return;
    }
    let now = now_millis();
    let snapshot = {
        let mut store = store_lock();
        let entry = ensure_entry(&mut store, session_id, now);
        entry.checkpoint = Some(checkpoint);
        entry.last_seen = now;
        let snapshot = entry.clone();
        touch_and_evict(&mut store, session_id, now);
        snapshot
    };
    if let Err(error) = persist_conversation(session_id, &snapshot) {
        log_persist_failure(session_id, &error);
    }
}

/// Persist only if the live driver's original conversation binding is current.
pub(crate) fn save_checkpoint_if_current(
    session_id: &str,
    expected_conversation_id: &str,
    checkpoint: Vec<u8>,
) -> ConditionalPersist {
    if checkpoint.is_empty() {
        return ConditionalPersist::Failed("checkpoint was empty".into());
    }
    let now = now_millis();
    let (snapshot, previous) = {
        let mut store = store_lock();
        let Some(entry) = store.map.get_mut(session_id) else {
            return ConditionalPersist::StaleBinding;
        };
        if entry.conversation_id != expected_conversation_id {
            return ConditionalPersist::StaleBinding;
        }
        let previous = entry.checkpoint.replace(checkpoint);
        entry.last_seen = now;
        let snapshot = entry.clone();
        touch_and_evict(&mut store, session_id, now);
        (snapshot, previous)
    };
    match persist_conversation(session_id, &snapshot) {
        Ok(()) => ConditionalPersist::Saved,
        Err(error) => {
            roll_back_checkpoint(
                session_id,
                expected_conversation_id,
                &snapshot.checkpoint,
                previous,
            );
            log_persist_failure(session_id, &error);
            ConditionalPersist::Failed(error.to_string())
        }
    }
}

/// A failed durable write must not leave memory ahead of disk: a later
/// continuation would silently serve state that never survived a restart.
/// Restores the pre-write value only while our own write is still in place.
fn roll_back_checkpoint(
    session_id: &str,
    expected_conversation_id: &str,
    written: &Option<Vec<u8>>,
    previous: Option<Vec<u8>>,
) {
    let mut store = store_lock();
    let Some(entry) = store.map.get_mut(session_id) else {
        return;
    };
    if entry.conversation_id == expected_conversation_id && entry.checkpoint == *written {
        entry.checkpoint = previous;
    }
}

/// Drop a stored checkpoint while keeping `conversation_id` and KV blobs.
///
/// ClientOnly (Workflow/Skill) teardown must not resume an in-flight MCP exec
/// on the next Anthropic turn. Native BiDi runs still call [`save_checkpoint`].
pub fn clear_checkpoint(session_id: &str) {
    let now = now_millis();
    let snapshot = {
        let mut store = store_lock();
        let Some(entry) = store.map.get_mut(session_id) else {
            return;
        };
        entry.checkpoint = None;
        entry.last_seen = now;
        let snapshot = entry.clone();
        touch_and_evict(&mut store, session_id, now);
        snapshot
    };
    if let Err(error) = persist_conversation(session_id, &snapshot) {
        log_persist_failure(session_id, &error);
    }
}

pub(crate) fn clear_checkpoint_if_current(
    session_id: &str,
    expected_conversation_id: &str,
) -> ConditionalPersist {
    let now = now_millis();
    let (snapshot, previous) = {
        let mut store = store_lock();
        let Some(entry) = store.map.get_mut(session_id) else {
            return ConditionalPersist::StaleBinding;
        };
        if entry.conversation_id != expected_conversation_id {
            return ConditionalPersist::StaleBinding;
        }
        let previous = entry.checkpoint.take();
        entry.last_seen = now;
        let snapshot = entry.clone();
        touch_and_evict(&mut store, session_id, now);
        (snapshot, previous)
    };
    match persist_conversation(session_id, &snapshot) {
        Ok(()) => ConditionalPersist::Saved,
        Err(error) => {
            roll_back_checkpoint(session_id, expected_conversation_id, &None, previous);
            log_persist_failure(session_id, &error);
            ConditionalPersist::Failed(error.to_string())
        }
    }
}

/// Forget an unrecoverable Cursor conversation binding.
///
/// The next request for this Claude session gets a new Cursor conversation id
/// and replays its full Anthropic history instead of reusing poisoned state.
pub fn reset(session_id: &str) {
    let now = now_millis();
    let fresh = CursorConversation {
        conversation_id: uuid::Uuid::new_v4().to_string(),
        checkpoint: None,
        blobs: HashMap::new(),
        last_seen: now,
    };
    {
        let mut store = store_lock();
        if !store.order.iter().any(|item| item == session_id) {
            store.order.push_back(session_id.to_string());
        }
        store.map.insert(session_id.to_string(), fresh.clone());
    }
    if let Err(error) = persist_conversation(session_id, &fresh) {
        log_persist_failure(session_id, &error);
    }
}

/// Merge KV blobs into the conversation store (set_blob wins).
pub fn merge_blobs(session_id: &str, blobs: &HashMap<Vec<u8>, Vec<u8>>) {
    if blobs.is_empty() {
        return;
    }
    let now = now_millis();
    let snapshot = {
        let mut store = store_lock();
        let entry = ensure_entry(&mut store, session_id, now);
        for (id, data) in blobs {
            entry.blobs.insert(id.clone(), data.clone());
        }
        entry.last_seen = now;
        let snapshot = entry.clone();
        touch_and_evict(&mut store, session_id, now);
        snapshot
    };
    if let Err(error) = persist_conversation(session_id, &snapshot) {
        log_persist_failure(session_id, &error);
    }
}

/// Merge blobs only while the live driver's original binding is current.
pub(crate) fn merge_blobs_if_current(
    session_id: &str,
    expected_conversation_id: &str,
    blobs: &HashMap<Vec<u8>, Vec<u8>>,
) -> ConditionalPersist {
    if blobs.is_empty() {
        return ConditionalPersist::Saved;
    }
    let now = now_millis();
    let (snapshot, replaced) = {
        let mut store = store_lock();
        let Some(entry) = store.map.get_mut(session_id) else {
            return ConditionalPersist::StaleBinding;
        };
        if entry.conversation_id != expected_conversation_id {
            return ConditionalPersist::StaleBinding;
        }
        let mut replaced = Vec::with_capacity(blobs.len());
        for (id, data) in blobs {
            let previous = entry.blobs.insert(id.clone(), data.clone());
            replaced.push((id.clone(), previous));
        }
        entry.last_seen = now;
        let snapshot = entry.clone();
        touch_and_evict(&mut store, session_id, now);
        (snapshot, replaced)
    };
    match persist_conversation(session_id, &snapshot) {
        Ok(()) => ConditionalPersist::Saved,
        Err(error) => {
            roll_back_blobs(session_id, expected_conversation_id, blobs, replaced);
            log_persist_failure(session_id, &error);
            ConditionalPersist::Failed(error.to_string())
        }
    }
}

/// See [`roll_back_checkpoint`]: per touched key, restore the previous value
/// only while our own write is still the visible one.
fn roll_back_blobs(
    session_id: &str,
    expected_conversation_id: &str,
    written: &HashMap<Vec<u8>, Vec<u8>>,
    replaced: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) {
    let mut store = store_lock();
    let Some(entry) = store.map.get_mut(session_id) else {
        return;
    };
    if entry.conversation_id != expected_conversation_id {
        return;
    }
    for (id, previous) in replaced {
        let still_ours = written
            .get(&id)
            .is_some_and(|data| entry.blobs.get(&id) == Some(data));
        if !still_ours {
            continue;
        }
        match previous {
            Some(previous) => {
                entry.blobs.insert(id, previous);
            }
            None => {
                entry.blobs.remove(&id);
            }
        }
    }
}

/// Snapshot used when opening a new Cursor Run.
#[derive(Debug, Clone, Default)]
pub struct RunContinuation {
    pub conversation_id: Option<String>,
    /// Opaque ConversationStateStructure protobuf bytes (empty = fresh turn).
    pub conversation_state: Vec<u8>,
    pub pre_fetched_blobs: Vec<(Vec<u8>, Vec<u8>)>,
    /// True when we have a prior checkpoint — prompt should be delta-only.
    pub has_checkpoint: bool,
}

pub fn continuation_for(session_id: Option<&str>) -> RunContinuation {
    let Some(session_id) = session_id.filter(|s| !s.is_empty()) else {
        return RunContinuation::default();
    };
    let conv = get_or_create(session_id);
    let has_checkpoint = conv.checkpoint.as_ref().is_some_and(|c| !c.is_empty());
    RunContinuation {
        conversation_id: Some(conv.conversation_id),
        conversation_state: conv.checkpoint.unwrap_or_default(),
        pre_fetched_blobs: conv.blobs.into_iter().collect(),
        has_checkpoint,
    }
}

#[cfg(test)]
pub fn reset_for_test() {
    let mut store = store_lock();
    store.map.clear();
    store.order.clear();
    store.pins.clear();
    LAST_PERSISTED_SWEEP_MS.store(0, Ordering::Relaxed);
    PERSISTED_SWEEP_RUNS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) static STORE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_or_create_reuses_conversation_id() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        let a = get_or_create("sess-1");
        let b = get_or_create("sess-1");
        assert_eq!(a.conversation_id, b.conversation_id);
        assert!(a.checkpoint.is_none());
    }

    #[test]
    fn checkpoint_and_blobs_round_trip() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        let _ = get_or_create("sess-2");
        save_checkpoint("sess-2", vec![0x0a, 0x02, 0x01, 0x02]);
        let mut blobs = HashMap::new();
        blobs.insert(vec![1, 2, 3], vec![9, 9]);
        merge_blobs("sess-2", &blobs);

        let cont = continuation_for(Some("sess-2"));
        assert!(cont.has_checkpoint);
        assert_eq!(cont.conversation_state, vec![0x0a, 0x02, 0x01, 0x02]);
        assert_eq!(cont.pre_fetched_blobs.len(), 1);
        assert_eq!(cont.pre_fetched_blobs[0].0, vec![1, 2, 3]);
        assert_eq!(cont.pre_fetched_blobs[0].1, vec![9, 9]);
        assert!(cont.conversation_id.is_some());
    }

    #[test]
    fn clear_checkpoint_drops_state_but_keeps_conversation_id() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        let created = get_or_create("sess-clear");
        save_checkpoint("sess-clear", vec![0x0a, 0x02, 0x01, 0x02]);
        assert!(continuation_for(Some("sess-clear")).has_checkpoint);
        clear_checkpoint("sess-clear");
        let cont = continuation_for(Some("sess-clear"));
        assert!(!cont.has_checkpoint);
        assert!(cont.conversation_state.is_empty());
        assert_eq!(
            cont.conversation_id.as_deref(),
            Some(created.conversation_id.as_str())
        );
    }

    #[test]
    fn continuation_without_session_is_empty() {
        assert!(!continuation_for(None).has_checkpoint);
        assert!(continuation_for(Some("")).conversation_id.is_none());
    }

    #[test]
    fn build_run_request_replays_checkpoint_and_blobs() {
        use crate::providers::cursor::client::build_run_request_with_continuation;
        use crate::providers::cursor::model::resolve_cursor_model;

        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        save_checkpoint("sess-build", vec![0x08, 0x01]);
        let mut blobs = HashMap::new();
        blobs.insert(vec![0xaa], vec![0xbb]);
        merge_blobs("sess-build", &blobs);

        let cont = continuation_for(Some("sess-build"));
        assert!(cont.has_checkpoint);
        let resolved = resolve_cursor_model("fable").unwrap();
        let req = build_run_request_with_continuation(
            "only new user text",
            &resolved,
            &[],
            "req-1",
            None,
            &cont,
            None,
        );
        assert_eq!(req.conversation_id, cont.conversation_id);
        assert_eq!(req.conversation_state.as_deref(), Some(&[0x08, 0x01][..]));
        assert_eq!(req.pre_fetched_blobs.len(), 1);
        assert!(!req.requested_model.as_ref().unwrap().parameters.is_empty());
    }

    #[test]
    fn checkpoint_reloads_from_disk_after_memory_drop() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_CONV_DIR", dir.path());
        }
        struct ClearEnv;
        impl Drop for ClearEnv {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("CCP_CURSOR_CONV_DIR");
                }
            }
        }
        let _clear = ClearEnv;
        reset_for_test();
        save_checkpoint("sess-persist", vec![0x0a, 0x03]);
        {
            let mut store = store_lock();
            store.map.clear();
            store.order.clear();
        }
        let cont = continuation_for(Some("sess-persist"));
        assert!(cont.has_checkpoint, "checkpoint must reload from disk");
        assert_eq!(cont.conversation_state, vec![0x0a, 0x03]);
    }

    #[test]
    fn reset_overwrites_poisoned_binding_instead_of_only_deleting() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_CONV_DIR", dir.path());
        }
        struct ClearEnv;
        impl Drop for ClearEnv {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("CCP_CURSOR_CONV_DIR");
                }
            }
        }
        let _clear = ClearEnv;
        reset_for_test();
        save_checkpoint("sess-reset", vec![0xaa]);
        merge_blobs("sess-reset", &HashMap::from([(vec![0x01], vec![0x02])]));
        let original = continuation_for(Some("sess-reset")).conversation_id.clone();
        reset("sess-reset");
        {
            let mut store = store_lock();
            store.map.clear();
            store.order.clear();
        }
        let recovered = continuation_for(Some("sess-reset"));
        assert_ne!(recovered.conversation_id, original);
        assert!(!recovered.has_checkpoint);
        assert!(recovered.pre_fetched_blobs.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn persisted_conversation_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_CONV_DIR", dir.path());
        }
        struct ClearEnv;
        impl Drop for ClearEnv {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("CCP_CURSOR_CONV_DIR");
                }
            }
        }
        let _clear = ClearEnv;
        reset_for_test();
        save_checkpoint("sess-mode", vec![0x01]);
        let meta = std::fs::metadata(dir.path()).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o700);
        let file = std::fs::read_dir(dir.path())
            .unwrap()
            .find_map(|e| {
                let e = e.ok()?;
                let name = e.file_name();
                name.to_str()?.ends_with(".json").then_some(e.path())
            })
            .expect("persisted json");
        let file_mode = std::fs::metadata(file).unwrap().permissions().mode() & 0o777;
        assert_eq!(file_mode, 0o600);
    }

    #[test]
    fn expire_abandoned_disk_conversations_ignores_memory_map() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_CONV_DIR", dir.path());
        }
        struct ClearEnv;
        impl Drop for ClearEnv {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("CCP_CURSOR_CONV_DIR");
                }
            }
        }
        let _clear = ClearEnv;
        reset_for_test();
        save_checkpoint("sess-stale-disk", vec![0x02]);
        let path = persist_path("sess-stale-disk").expect("path");
        let mut dto: PersistedConversation =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        dto.last_seen = 1;
        std::fs::write(&path, serde_json::to_vec(&dto).unwrap()).unwrap();
        {
            let mut store = store_lock();
            store.map.clear();
            store.order.clear();
        }
        expire_abandoned_persisted(now_millis());
        assert!(
            !path.exists(),
            "TTL must delete abandoned files from previous processes"
        );
    }

    #[test]
    fn persisted_conversation_sweep_is_not_run_for_every_request() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_CONV_DIR", dir.path());
        }
        struct ClearEnv;
        impl Drop for ClearEnv {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("CCP_CURSOR_CONV_DIR");
                }
            }
        }
        let _clear = ClearEnv;
        reset_for_test();

        let _ = get_or_create("sweep-a");
        let _ = get_or_create("sweep-b");

        assert_eq!(
            PERSISTED_SWEEP_RUNS.load(Ordering::Relaxed),
            1,
            "directory-wide cleanup must be rate-limited off the request hot path"
        );
    }

    #[test]
    fn pinned_live_conversation_survives_idle_eviction() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        let original = get_or_create("sess-pinned");
        let lease =
            pin("sess-pinned", &original.conversation_id).expect("current binding must pin");
        {
            let mut store = store_lock();
            store
                .map
                .get_mut("sess-pinned")
                .expect("pinned entry")
                .last_seen = 1;
        }

        let _ = get_or_create("sess-other");

        assert_eq!(
            get("sess-pinned")
                .expect("active live binding must not expire")
                .conversation_id,
            original.conversation_id
        );
        drop(lease);
    }

    #[test]
    fn checkpoint_save_rejects_a_rebound_conversation() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        let original = get_or_create("sess-rebound");
        reset("sess-rebound");

        assert_eq!(
            save_checkpoint_if_current("sess-rebound", &original.conversation_id, vec![0x08, 0x01],),
            ConditionalPersist::StaleBinding,
            "an old driver must not attach its checkpoint to a replacement conversation"
        );
        assert!(
            !continuation_for(Some("sess-rebound")).has_checkpoint,
            "the replacement binding must remain clean"
        );
    }

    #[test]
    fn conditional_checkpoint_save_reports_disk_failure() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let blocked = dir.path().join("not-a-directory");
        std::fs::write(&blocked, b"block persistence directory").unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_CONV_DIR", &blocked);
        }
        struct ClearEnv;
        impl Drop for ClearEnv {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("CCP_CURSOR_CONV_DIR");
                }
            }
        }
        let _clear = ClearEnv;
        reset_for_test();
        let created = get_or_create("sess-disk-failure");

        assert!(
            matches!(
                save_checkpoint_if_current(
                    "sess-disk-failure",
                    &created.conversation_id,
                    vec![0x08, 0x01],
                ),
                ConditionalPersist::Failed(_)
            ),
            "a failed atomic write must not be reported as durable success"
        );
        assert!(
            !continuation_for(Some("sess-disk-failure")).has_checkpoint,
            "a failed checkpoint write must roll back memory so later turns cannot \
             continue from state that never reached disk"
        );

        assert!(matches!(
            merge_blobs_if_current(
                "sess-disk-failure",
                &created.conversation_id,
                &HashMap::from([(vec![0x01], vec![0x02])]),
            ),
            ConditionalPersist::Failed(_)
        ));
        assert!(
            continuation_for(Some("sess-disk-failure"))
                .pre_fetched_blobs
                .is_empty(),
            "failed blob writes must roll back memory"
        );
    }

    #[test]
    fn conditional_clear_rolls_back_memory_when_disk_write_fails() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_CONV_DIR", dir.path());
        }
        struct ClearEnv;
        impl Drop for ClearEnv {
            fn drop(&mut self) {
                unsafe {
                    std::env::remove_var("CCP_CURSOR_CONV_DIR");
                }
            }
        }
        let _clear = ClearEnv;
        reset_for_test();
        save_checkpoint("sess-clear-failure", vec![0x0a, 0x01]);
        let binding = continuation_for(Some("sess-clear-failure"))
            .conversation_id
            .expect("binding");

        // Break the persistence directory only after the checkpoint exists.
        let blocked = dir.path().join("blocked");
        std::fs::write(&blocked, b"block persistence directory").unwrap();
        unsafe {
            std::env::set_var("CCP_CURSOR_CONV_DIR", &blocked);
        }

        assert!(matches!(
            clear_checkpoint_if_current("sess-clear-failure", &binding),
            ConditionalPersist::Failed(_)
        ));
        assert!(
            continuation_for(Some("sess-clear-failure")).has_checkpoint,
            "a failed clear must keep the durable checkpoint visible in memory"
        );
    }
}
