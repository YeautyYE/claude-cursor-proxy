//! Persist Cursor conversation_id + checkpoint + KV blobs across Claude Code turns.
//!
//! Official CLI keeps a ConversationStateStructure (blob-ID form) plus a content-
//! addressed blob store between Run streams. Without this, each Claude turn is a
//! fresh Cursor run that re-uploads the entire Anthropic history + tools schema.

use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use sha2::{Digest, Sha256};

const IDLE_TTL_MS: u64 = 30 * 60 * 1000;
const MAX_CONVERSATIONS: usize = 10_000;
const PERSISTED_SWEEP_INTERVAL_MS: u64 = 60_000;

// Keep persisted-state decoding bounded even when a stale/corrupt JSON file
// was left by an older build. These mirror the local live KV reply ceilings;
// snapshots at or below the limits are still loaded and then proactively
// rotated by `normalize_kv_store` when they cross the soft threshold.
const CURSOR_KV_HARD_MAX_BLOBS: usize = 4_096;
const CURSOR_KV_HARD_MAX_BYTES: usize = 64 * 1024 * 1024;
// Live frame handling rejects checkpoints above this size. Apply the same
// bound while loading a persisted snapshot so a hand-edited/corrupt file
// cannot allocate an unbounded opaque protobuf before the next request.
const CURSOR_CHECKPOINT_MAX_BYTES: usize = 32 * 1024 * 1024;
// Base64 expands the 64 MiB blob budget and 32 MiB checkpoint budget to just
// over 170 MiB once JSON keys/separators are included. Keep a bounded decoder
// guard, but leave enough room for a valid near-ceiling snapshot.
const MAX_PERSISTED_JSON_BYTES: usize = 192 * 1024 * 1024;

// Cursor's server-side KV store currently accepts at most 4096 blobs and
// roughly 64 MiB per conversation.  A conversation that gets that close to
// either ceiling is not recoverable by dropping entries locally: the remote
// store keeps the old blobs as well.  Rotate a binding before the hard limit
// so the next request can replay the Anthropic history on a clean Cursor
// conversation.  Leave enough headroom for a long tool-heavy turn between
// requests (and for small server-side accounting differences).
pub(crate) const CURSOR_KV_SOFT_MAX_BLOBS: usize = 3_840;
pub(crate) const CURSOR_KV_SOFT_MAX_BYTES: usize = 60 * 1024 * 1024;
static LAST_PERSISTED_SWEEP_MS: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static PERSISTED_SWEEP_RUNS: AtomicU64 = AtomicU64::new(0);

// Snapshot construction happens under `STORE`, while the durable write is
// intentionally performed after releasing that mutex (blob values can be
// large).  Serialize the filesystem side of those writes and re-read the
// current in-memory binding before writing so an older, slower snapshot can
// never overwrite a newer checkpoint/blob update or a rotated conversation.
// Keep this lock separate from `STORE`: callers must not hold `STORE` while
// acquiring it, otherwise a concurrent conditional writer could deadlock.
static PERSIST_WRITE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// Files that should be removed once the filesystem write lock is available.
///
/// Conversation eviction runs while `STORE` is held.  Taking
/// `PERSIST_WRITE_LOCK` from that critical section would invert the lock order
/// used by snapshot writers (`PERSIST_WRITE_LOCK` -> `STORE`) and can deadlock
/// an in-flight writer.  Eviction therefore only queues a conditional delete;
/// the queue is drained after the store guard is released.  The binding
/// metadata captured at eviction time prevents a newly-created/rebound
/// conversation with the same session id from losing its file.
#[derive(Debug, Clone)]
struct PendingPersistDelete {
    session_id: String,
    conversation_id: Option<String>,
}

static PENDING_PERSIST_DELETES: LazyLock<Mutex<Vec<PendingPersistDelete>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));
// Most continuation lookups have no deferred unlink to flush.  Keep a cheap
// lock-free hint so those hot paths do not contend on `PERSIST_WRITE_LOCK`
// (which may be held while a large snapshot is serialized and fsynced).  The
// queue mutex remains authoritative; this flag is only an optimization.
static PENDING_PERSIST_DELETES_READY: AtomicBool = AtomicBool::new(false);

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

fn persist_conversation_unlocked(session_id: &str, conv: &CursorConversation) -> io::Result<()> {
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

fn persist_write_lock() -> std::sync::MutexGuard<'static, ()> {
    PERSIST_WRITE_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn queue_persisted_delete(session_id: &str, conversation: Option<&CursorConversation>) {
    let pending = PendingPersistDelete {
        session_id: session_id.to_string(),
        conversation_id: conversation.map(|entry| entry.conversation_id.clone()),
    };
    let mut queue = PENDING_PERSIST_DELETES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    // Keep one entry per `(session, binding)`; repeated eviction passes should
    // not grow an unbounded queue while the persistence directory is blocked.
    if !queue.iter().any(|existing| {
        existing.session_id == pending.session_id
            && existing.conversation_id == pending.conversation_id
    }) {
        queue.push(pending);
    }
    // Store the hint while holding the queue mutex.  The drainer clears it
    // under that same mutex, so a producer racing a drain cannot have its
    // newly queued entry hidden by a later `false` store.
    PENDING_PERSIST_DELETES_READY.store(!queue.is_empty(), Ordering::Release);
}

/// Drain conditional deletes while the caller owns `PERSIST_WRITE_LOCK`.
/// `STORE` is acquired only after that lock, matching all snapshot writers.
fn flush_pending_persisted_deletes_locked() {
    let pending = {
        let mut queue = PENDING_PERSIST_DELETES
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let pending = std::mem::take(&mut *queue);
        // Clear the fast-path hint before releasing the queue mutex.  Any
        // producer that queues after this point acquires the mutex later and
        // publishes `true` again, preventing a lost flush notification.
        PENDING_PERSIST_DELETES_READY.store(false, Ordering::Release);
        pending
    };
    for pending in pending {
        let store = store_lock();
        // A reload of the *same* binding still owns this file, so preserve it.
        // A replacement binding, however, must not inherit an evicted
        // conversation's durable snapshot: remove the stale file before the
        // replacement gets persisted.  Compare the on-disk owner before the
        // unlink as well: a replacement may already have written its fresh
        // snapshot after this delete was queued, and deleting by session id
        // alone would erase that newer state.
        let current = store.map.get(&pending.session_id);
        let same_binding = pending
            .conversation_id
            .as_deref()
            .zip(current)
            .is_some_and(|(expected, current)| current.conversation_id == expected);
        let pinned = store.pins.contains_key(&pending.session_id);
        let should_delete = if pinned || same_binding {
            false
        } else if let Some(path) = persist_path(&pending.session_id) {
            // The path is deterministic for this session. A malformed or
            // concurrently replaced file is left untouched rather than
            // turning a bookkeeping race into data loss.
            let on_disk = std::fs::read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<PersistedConversation>(&bytes).ok());
            match (
                pending.conversation_id.as_deref(),
                current,
                on_disk.as_ref(),
            ) {
                // An evicted in-memory binding: unlink only the exact old
                // snapshot. If a replacement already won the file race, keep
                // its checkpoint for the next continuation.
                (Some(expected), _, Some(dto)) => {
                    dto.session_id == pending.session_id && dto.conversation_id == expected
                }
                // An expired disk-only binding has no owner metadata. Delete
                // it when the file is not the current resident binding. If a
                // replacement already persisted its own snapshot, preserve
                // that newer DTO; otherwise remove the stale bytes before a
                // process crash can resurrect them on restart.
                (None, None, Some(dto)) => dto.session_id == pending.session_id,
                (None, Some(current), Some(dto)) => {
                    dto.session_id != pending.session_id
                        || dto.conversation_id != current.conversation_id
                }
                (None, Some(_), None) => false,
                // Unknown/corrupt bytes are retained for a later TTL sweep.
                (_, _, None) => false,
            }
        } else {
            false
        };
        if should_delete {
            if let Some(path) = persist_path(&pending.session_id) {
                let _ = std::fs::remove_file(path);
            }
        }
        // Keep the filesystem operation in the same `PERSIST_WRITE_LOCK` /
        // `STORE` order as writers. It is a single unlink and avoids a race
        // with a concurrent load/rebind between the check and removal.
        drop(store);
    }
}

fn flush_pending_persisted_deletes() {
    if !PENDING_PERSIST_DELETES_READY.load(Ordering::Acquire) {
        return;
    }
    let _write_guard = persist_write_lock();
    // Another flusher may have drained the queue while we waited for the
    // serialized filesystem lock.  Avoid taking `STORE` in that case.
    if !PENDING_PERSIST_DELETES_READY.load(Ordering::Acquire) {
        return;
    }
    flush_pending_persisted_deletes_locked();
}

/// Persist the latest state currently bound to `session_id`.
///
/// The caller may have captured an older snapshot before another thread
/// updated the same conversation.  Re-reading the map while the serialized
/// write lock is held makes the latest state win; if the entry was evicted,
/// there is no reason to resurrect its disk file.
fn persist_conversation(session_id: &str, _proposed: &CursorConversation) -> io::Result<()> {
    let _write_guard = persist_write_lock();
    flush_pending_persisted_deletes_locked();
    let snapshot = {
        let store = store_lock();
        store.map.get(session_id).cloned()
    };
    let Some(snapshot) = snapshot else {
        return Ok(());
    };
    persist_conversation_unlocked(session_id, &snapshot)
}

/// Persist only while the live driver's conversation binding is still the
/// expected one.  If another writer changed checkpoint/blob contents without
/// rotating the binding, persist that newer in-memory snapshot instead of the
/// stale proposal.  A changed conversation id is a genuine stale binding and
/// must not be written by the old driver.
fn persist_conversation_if_current(
    session_id: &str,
    expected_conversation_id: &str,
    _proposed: &CursorConversation,
) -> io::Result<bool> {
    let _write_guard = persist_write_lock();
    flush_pending_persisted_deletes_locked();
    let snapshot = {
        let store = store_lock();
        let Some(current) = store.map.get(session_id).cloned() else {
            return Ok(false);
        };
        if current.conversation_id != expected_conversation_id {
            return Ok(false);
        }
        current
    };
    persist_conversation_unlocked(session_id, &snapshot).map(|()| true)
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
    flush_pending_persisted_deletes();
    let Some(dir) = persist_dir() else {
        return;
    };
    // Directory enumeration and JSON decoding can be expensive when a prior
    // process left thousands of large blob snapshots. Do that work without the
    // global write lock; each candidate is re-read and checked while the lock
    // is held immediately before unlinking.
    let Ok(paths) = std::fs::read_dir(dir).map(|entries| {
        entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            .collect::<Vec<_>>()
    }) else {
        return;
    };
    for path in paths {
        let _write_guard = persist_write_lock();
        // A writer may have replaced this file after enumeration. Re-read the
        // current bytes under the same lock so an old parsed DTO can never
        // cause a fresh snapshot to be removed.
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(dto) = serde_json::from_slice::<PersistedConversation>(&bytes) else {
            continue;
        };
        if now.saturating_sub(dto.last_seen) <= IDLE_TTL_MS {
            continue;
        }
        // Re-check the in-memory binding while the write lock is held. A
        // request may have loaded/pinned/touched this session after the
        // directory scan began; deleting it based on an old snapshot would
        // erase a live continuation or race a subsequent rename.
        let store = store_lock();
        let should_delete = match store.map.get(&dto.session_id) {
            Some(current) => {
                !store.pins.contains_key(&dto.session_id)
                    && now.saturating_sub(current.last_seen) > IDLE_TTL_MS
            }
            None => !store.pins.contains_key(&dto.session_id),
        };
        if should_delete {
            let _ = std::fs::remove_file(path);
        }
        drop(store);
    }
}

fn maybe_expire_abandoned_persisted(now: u64) {
    // Evictions can happen on a read-only path that does not otherwise write a
    // snapshot. Flush their deferred unlinks before rate-limiting the sweep.
    flush_pending_persisted_deletes();
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
    // Reject an unexpectedly large file before serde or the decoder allocates
    // thousands of untrusted strings. The bound is deliberately above the
    // base64-expanded KV + checkpoint ceilings so valid near-limit snapshots
    // remain recoverable.
    if bytes.len() > MAX_PERSISTED_JSON_BYTES {
        return None;
    }
    let dto: PersistedConversation = serde_json::from_slice(&bytes).ok()?;
    // The filename is derived from the requested session id, but the JSON
    // carries its own owner. Validate both fields before accepting state so a
    // stale/corrupt file can never leak another Claude session's checkpoint or
    // blobs into this continuation.
    if dto.session_id != session_id || dto.conversation_id.trim().is_empty() {
        return None;
    }
    let checkpoint = match dto.checkpoint_b64.as_deref() {
        Some(encoded) => {
            let checkpoint = b64_decode(encoded)?;
            if checkpoint.len() > CURSOR_CHECKPOINT_MAX_BYTES {
                return None;
            }
            Some(checkpoint)
        }
        None => None,
    };
    let mut blobs = HashMap::new();
    // Keep the aggregate while decoding instead of summing every previously
    // decoded value for each entry.  A near-limit snapshot can contain 4,096
    // blobs; repeatedly walking the map would turn restart recovery into an
    // O(n²) scan of tens of GiB even though the final payload is only ~64 MiB.
    let mut total_blob_bytes = 0usize;
    for (k, v) in dto.blobs_b64 {
        let key = b64_decode(&k)?;
        let value = b64_decode(&v)?;
        let existing_len = blobs.get(&key).map(Vec::len).unwrap_or_default();
        let is_new = !blobs.contains_key(&key);
        let next_count = blobs.len() + usize::from(is_new);
        let next_total = total_blob_bytes
            .saturating_sub(existing_len)
            .saturating_add(value.len());
        if next_count > CURSOR_KV_HARD_MAX_BLOBS || next_total > CURSOR_KV_HARD_MAX_BYTES {
            return None;
        }
        blobs.insert(key, value);
        total_blob_bytes = next_total;
    }
    Some(CursorConversation {
        conversation_id: dto.conversation_id,
        checkpoint,
        blobs,
        last_seen: dto.last_seen,
    })
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
                let removed = store.map.remove(&evict);
                queue_persisted_delete(&evict, removed.as_ref());
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
        let removed = store.map.remove(&key);
        store.order.retain(|item| item != &key);
        queue_persisted_delete(&key, removed.as_ref());
    }
}

fn blob_total_bytes(blobs: &HashMap<Vec<u8>, Vec<u8>>) -> usize {
    blobs
        .values()
        .fold(0usize, |total, value| total.saturating_add(value.len()))
}

fn kv_store_near_limit(conv: &CursorConversation) -> bool {
    conv.blobs.len() >= CURSOR_KV_SOFT_MAX_BLOBS
        || blob_total_bytes(&conv.blobs) >= CURSOR_KV_SOFT_MAX_BYTES
}

/// Return diagnostic counts when a local snapshot approaches the server KV
/// ceiling. Cursor's remote KV store is append-only for a conversation id, so
/// deleting local entries cannot release the server-side quota; the caller
/// always rotates the binding.
fn kv_store_stats_for_rotation(conv: &CursorConversation) -> (usize, usize) {
    if !kv_store_near_limit(conv) {
        return (0, 0);
    }
    // Avoid scanning opaque checkpoint bytes here. A checkpoint can be tens
    // of MiB and the blob map can contain thousands of entries; a byte-pattern
    // scan would turn recovery into an accidental O(N*M) latency spike.
    // Rotation is unconditional, so reachability is deliberately unknown.
    (conv.blobs.len(), 0)
}

/// Rotate an oversized binding atomically with respect to live drivers. A
/// pinned conversation is never rebound under an active stream; its driver
/// surfaces the bounded KV error and the normal late-retry path rotates it once
/// the stream has closed.
fn normalize_kv_store(
    session_id: &str,
    expected_conversation_id: &str,
) -> Option<CursorConversation> {
    let now = now_millis();
    let (snapshot, removed, reachable, rotated) = {
        let mut store = store_lock();
        let is_pinned = store.pins.contains_key(session_id);
        let entry = store.map.get_mut(session_id)?;
        if entry.conversation_id != expected_conversation_id
            || is_pinned
            || !kv_store_near_limit(entry)
        {
            return Some(entry.clone());
        }

        // Either the checkpoint references too many values or has no
        // recognizable ids.  A fresh conversation is the only reliable way
        // to clear Cursor's remote blob store; a local GC would leave those
        // remote values counted against the same conversation id. The caller
        // replays the complete Anthropic history because checkpoint is empty.
        let (removed, reachable) = kv_store_stats_for_rotation(entry);
        let fresh = CursorConversation {
            conversation_id: uuid::Uuid::new_v4().to_string(),
            checkpoint: None,
            blobs: HashMap::new(),
            last_seen: now,
        };
        entry.clone_from(&fresh);
        let snapshot = fresh;
        (snapshot, removed, reachable, true)
    };

    if let Err(error) = persist_conversation(session_id, &snapshot) {
        log_persist_failure(session_id, &error);
    }
    // The durable write is intentionally outside `STORE` because a large blob
    // snapshot can take noticeable time.  A concurrent reset may therefore
    // have replaced this binding while the file was being written.  Never
    // hand that stale UUID to the caller: fetch the current binding after the
    // write and let the caller build its request from the winner.
    let snapshot = {
        let store = store_lock();
        let current = store.map.get(session_id)?;
        current.clone()
    };
    let mut fields = serde_json::Map::from_iter([
        ("sessionId".into(), serde_json::json!(session_id)),
        (
            "conversationId".into(),
            serde_json::json!(expected_conversation_id),
        ),
        ("removed".into(), serde_json::json!(removed)),
        ("reachable".into(), serde_json::json!(reachable)),
        ("remaining".into(), serde_json::json!(snapshot.blobs.len())),
        ("rotated".into(), serde_json::json!(rotated)),
    ]);
    if rotated {
        fields.insert(
            "newConversationId".into(),
            serde_json::json!(&snapshot.conversation_id),
        );
    }
    crate::logging::create_logger("cursor").warn("cursor_kv_store_normalized", Some(fields));
    Some(snapshot)
}

/// Get or create the Cursor conversation binding for a Claude session id.
pub fn get_or_create(session_id: &str) -> CursorConversation {
    let now = now_millis();
    maybe_expire_abandoned_persisted(now);
    let mut store = store_lock();
    // If an in-memory binding crossed its idle TTL, it is authoritative for
    // this process and must not be resurrected from the older JSON snapshot
    // below.  The deferred unlink is flushed after releasing `STORE`; skip
    // the disk lookup in this branch so the old checkpoint cannot win the
    // race before that flush runs.
    let mut discard_persisted = false;
    if let Some(existing) = store.map.get(session_id).cloned() {
        if store.pins.contains_key(session_id)
            || now.saturating_sub(existing.last_seen) <= IDLE_TTL_MS
        {
            touch_and_evict(&mut store, session_id, now);
            drop(store);
            flush_pending_persisted_deletes();
            return existing;
        }
        let removed = store.map.remove(session_id);
        store.order.retain(|item| item != session_id);
        queue_persisted_delete(session_id, removed.as_ref());
        discard_persisted = true;
    }
    if !discard_persisted {
        if let Some(disk) = load_persisted(session_id) {
            if now.saturating_sub(disk.last_seen) <= IDLE_TTL_MS {
                store.order.push_back(session_id.to_string());
                store.map.insert(session_id.to_string(), disk.clone());
                touch_and_evict(&mut store, session_id, now);
                drop(store);
                flush_pending_persisted_deletes();
                return disk;
            }
            queue_persisted_delete(session_id, None);
        }
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
    // Filesystem persistence acquires `PERSIST_WRITE_LOCK` and re-reads the
    // store. Release `STORE` first to keep the lock order consistent with all
    // other snapshot writers.
    drop(store);
    flush_pending_persisted_deletes();
    if let Err(error) = persist_conversation(session_id, &created) {
        log_persist_failure(session_id, &error);
    }
    created
}

pub fn get(session_id: &str) -> Option<CursorConversation> {
    let now = now_millis();
    flush_pending_persisted_deletes();
    let mut store = store_lock();
    if let Some(existing) = store.map.get(session_id).cloned() {
        if !store.pins.contains_key(session_id)
            && now.saturating_sub(existing.last_seen) > IDLE_TTL_MS
        {
            let removed = store.map.remove(session_id);
            store.order.retain(|item| item != session_id);
            queue_persisted_delete(session_id, removed.as_ref());
            drop(store);
            flush_pending_persisted_deletes();
            return None;
        }
        touch_and_evict(&mut store, session_id, now);
        drop(store);
        flush_pending_persisted_deletes();
        return Some(existing);
    }
    let Some(disk) = load_persisted(session_id) else {
        drop(store);
        flush_pending_persisted_deletes();
        return None;
    };
    if now.saturating_sub(disk.last_seen) > IDLE_TTL_MS {
        queue_persisted_delete(session_id, None);
        drop(store);
        flush_pending_persisted_deletes();
        return None;
    }
    store.order.push_back(session_id.to_string());
    store.map.insert(session_id.to_string(), disk.clone());
    touch_and_evict(&mut store, session_id, now);
    drop(store);
    flush_pending_persisted_deletes();
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
    match persist_conversation_if_current(session_id, expected_conversation_id, &snapshot) {
        Ok(true) => ConditionalPersist::Saved,
        Ok(false) => ConditionalPersist::StaleBinding,
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
    match persist_conversation_if_current(session_id, expected_conversation_id, &snapshot) {
        Ok(true) => ConditionalPersist::Saved,
        Ok(false) => ConditionalPersist::StaleBinding,
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
    match persist_conversation_if_current(session_id, expected_conversation_id, &snapshot) {
        Ok(true) => ConditionalPersist::Saved,
        Ok(false) => ConditionalPersist::StaleBinding,
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
    let initial = get_or_create(session_id);
    // A completed run is no longer pinned, so this is the safe point to
    // compact stale checkpoint-unreachable blobs or rotate the remote Cursor
    // conversation before constructing the next request.  If a concurrent
    // reset won the binding race, fetch its replacement rather than sending
    // the stale snapshot we first observed.
    let conv = normalize_kv_store(session_id, &initial.conversation_id)
        .unwrap_or_else(|| get_or_create(session_id));
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
    // Preserve pinned sessions: a concurrent driver test holds a lease, and a
    // global wipe would sever its conversation binding mid-run (the
    // "binding changed while a live Run was active" test flake).
    let pinned: std::collections::HashSet<String> = store.pins.keys().cloned().collect();
    store.map.retain(|key, _| pinned.contains(key));
    store.order.retain(|key| pinned.contains(key));
    drop(store);
    // Test cases frequently swap `CCP_CURSOR_CONV_DIR` to a fresh temporary
    // directory. Do not let a deferred unlink from a previous case target the
    // next case's directory.
    let mut pending = PENDING_PERSIST_DELETES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    pending.clear();
    PENDING_PERSIST_DELETES_READY.store(false, Ordering::Release);
    LAST_PERSISTED_SWEEP_MS.store(0, Ordering::Relaxed);
    PERSISTED_SWEEP_RUNS.store(0, Ordering::Relaxed);
}

/// Test-only restart simulation: drop in-memory state so the next lookup must
/// reload from disk — without severing concurrently pinned sessions.
#[cfg(test)]
fn drop_unpinned_in_memory_for_test() {
    let mut store = store_lock();
    let pinned: std::collections::HashSet<String> = store.pins.keys().cloned().collect();
    store.map.retain(|key, _| pinned.contains(key));
    store.order.retain(|key| pinned.contains(key));
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
        drop_unpinned_in_memory_for_test();
        let cont = continuation_for(Some("sess-persist"));
        assert!(cont.has_checkpoint, "checkpoint must reload from disk");
        assert_eq!(cont.conversation_state, vec![0x0a, 0x03]);
    }

    #[test]
    fn expired_in_memory_binding_is_not_resurrected_from_disk_snapshot() {
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

        let session = "sess-expired-memory-authoritative";
        let original = get_or_create(session);
        save_checkpoint(session, vec![0xde, 0xad]);
        let path = persist_path(session).expect("path");
        assert!(path.exists());

        // Leave a recent JSON snapshot in place while making the resident
        // binding idle.  `get_or_create` must create a fresh conversation;
        // loading the old file here would resurrect the expired checkpoint.
        {
            let mut store = store_lock();
            let entry = store.map.get_mut(session).expect("resident binding");
            entry.last_seen = 1;
        }
        let replacement = get_or_create(session);
        assert_ne!(replacement.conversation_id, original.conversation_id);
        assert!(replacement.checkpoint.is_none());

        // The replacement is what survives a simulated process restart.
        drop_unpinned_in_memory_for_test();
        let recovered = continuation_for(Some(session));
        assert_eq!(
            recovered.conversation_id.as_deref(),
            Some(replacement.conversation_id.as_str())
        );
        assert!(!recovered.has_checkpoint);
    }

    #[test]
    fn persisted_snapshot_owner_must_match_requested_session() {
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

        let requested = "sess-owner-requested";
        let path = persist_path(requested).expect("path");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            serde_json::to_vec(&PersistedConversation {
                session_id: "sess-other".into(),
                conversation_id: uuid::Uuid::new_v4().to_string(),
                checkpoint_b64: Some(b64_encode(b"private")),
                blobs_b64: Vec::new(),
                last_seen: now_millis(),
            })
            .unwrap(),
        )
        .unwrap();
        assert!(
            load_persisted(requested).is_none(),
            "a deterministic filename is not sufficient ownership proof"
        );
    }

    #[test]
    fn persisted_snapshot_rejects_kv_hard_limit_before_replay() {
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

        let requested = "sess-persist-limit";
        let path = persist_path(requested).expect("path");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let blobs_b64 = (0..=CURSOR_KV_HARD_MAX_BLOBS)
            .map(|index| {
                (
                    b64_encode(&(index as u32).to_be_bytes()),
                    b64_encode(b"blob"),
                )
            })
            .collect();
        std::fs::write(
            &path,
            serde_json::to_vec(&PersistedConversation {
                session_id: requested.into(),
                conversation_id: uuid::Uuid::new_v4().to_string(),
                checkpoint_b64: None,
                blobs_b64,
                last_seen: now_millis(),
            })
            .unwrap(),
        )
        .unwrap();
        assert!(
            load_persisted(requested).is_none(),
            "a snapshot beyond Cursor's hard blob count must not be replayed"
        );
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
        drop_unpinned_in_memory_for_test();
        let recovered = continuation_for(Some("sess-reset"));
        assert_ne!(recovered.conversation_id, original);
        assert!(!recovered.has_checkpoint);
        assert!(recovered.pre_fetched_blobs.is_empty());
    }

    #[test]
    fn continuation_rotates_oversized_store_even_with_referenced_blobs() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        let session = "sess-kv-gc";
        let original = get_or_create(session).conversation_id;
        let referenced = vec![0x7a, 0x7b, 0x7c, 0x7d];
        {
            let mut store = store_lock();
            let entry = store.map.get_mut(session).expect("conversation");
            entry.checkpoint = Some(referenced.clone());
            for index in 0..CURSOR_KV_SOFT_MAX_BLOBS {
                let id = (index as u32).to_be_bytes().to_vec();
                entry.blobs.insert(id, vec![0xaa]);
            }
            entry.blobs.insert(referenced.clone(), vec![0xbb]);
        }

        let continuation = continuation_for(Some(session));
        assert_ne!(
            continuation.conversation_id.as_deref(),
            Some(original.as_str())
        );
        assert!(continuation.pre_fetched_blobs.is_empty());
        assert!(!continuation.has_checkpoint);
    }

    #[test]
    fn oversized_store_rotation_does_not_replay_nested_blob_payloads() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        let session = "sess-kv-nested-gc";
        let original = get_or_create(session).conversation_id;
        let root = b"root-blob-id-0000000000000001".to_vec();
        let child = b"child-blob-id-0000000000000002".to_vec();
        {
            let mut store = store_lock();
            let entry = store.map.get_mut(session).expect("conversation");
            entry.checkpoint = Some(root.clone());
            entry.blobs.insert(root.clone(), child.clone());
            entry.blobs.insert(child.clone(), vec![0xbb]);
            for index in 0..(CURSOR_KV_SOFT_MAX_BLOBS - 2) {
                let id = format!("stale-{index:04}").into_bytes();
                entry.blobs.insert(id, vec![0xaa]);
            }
        }

        let continuation = continuation_for(Some(session));
        assert_ne!(
            continuation.conversation_id.as_deref(),
            Some(original.as_str())
        );
        assert!(continuation.pre_fetched_blobs.is_empty());
        assert!(!continuation.has_checkpoint);
    }

    #[test]
    fn continuation_rotates_when_oversized_checkpoint_has_no_known_references() {
        let _guard = STORE_TEST_LOCK.lock().unwrap();
        reset_for_test();
        let session = "sess-kv-rotate";
        let original = get_or_create(session).conversation_id;
        {
            let mut store = store_lock();
            let entry = store.map.get_mut(session).expect("conversation");
            entry.checkpoint = Some(vec![0xff, 0xfe, 0xfd]);
            for index in 0..CURSOR_KV_SOFT_MAX_BLOBS {
                let id = (index as u32).to_be_bytes().to_vec();
                entry.blobs.insert(id, vec![0xaa]);
            }
        }

        let continuation = continuation_for(Some(session));
        assert_ne!(
            continuation.conversation_id.as_deref(),
            Some(original.as_str())
        );
        assert!(!continuation.has_checkpoint);
        assert!(continuation.conversation_state.is_empty());
        assert!(continuation.pre_fetched_blobs.is_empty());
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
    fn deferred_eviction_delete_does_not_remove_reloaded_binding() {
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

        let session = "sess-deferred-delete";
        let original = get_or_create(session);
        let path = persist_path(session).expect("path");
        assert!(path.exists());

        // Simulate an eviction that queues its unlink, then a concurrent
        // request reloading the same on-disk binding before the queue drains.
        let removed = {
            let mut store = store_lock();
            let removed = store.map.remove(session).expect("binding");
            store.order.retain(|item| item != session);
            removed
        };
        queue_persisted_delete(session, Some(&removed));
        let reloaded = load_persisted(session).expect("reload");
        assert_eq!(reloaded.conversation_id, original.conversation_id);
        {
            let mut store = store_lock();
            store.order.push_back(session.to_string());
            store.map.insert(session.to_string(), reloaded);
        }
        flush_pending_persisted_deletes();
        assert!(
            path.exists(),
            "a reloaded binding must not lose its persistence file"
        );

        // If the session is rebound before the deferred unlink drains and the
        // replacement has already persisted, the newer file must survive. A
        // delete keyed only by session id would erase this replacement.
        let replacement = CursorConversation {
            conversation_id: uuid::Uuid::new_v4().to_string(),
            checkpoint: None,
            blobs: HashMap::new(),
            last_seen: now_millis(),
        };
        let removed = {
            let mut store = store_lock();
            let removed = store.map.remove(session).expect("binding");
            store.order.retain(|item| item != session);
            removed
        };
        queue_persisted_delete(session, Some(&removed));
        {
            let mut store = store_lock();
            store.order.push_back(session.to_string());
            store.map.insert(session.to_string(), replacement.clone());
        }
        std::fs::write(
            &path,
            serde_json::to_vec(&PersistedConversation {
                session_id: session.to_string(),
                conversation_id: replacement.conversation_id.clone(),
                checkpoint_b64: None,
                blobs_b64: Vec::new(),
                last_seen: replacement.last_seen,
            })
            .unwrap(),
        )
        .unwrap();
        flush_pending_persisted_deletes();
        assert!(
            path.exists(),
            "a replacement snapshot already on disk must not be removed"
        );
        let persisted: PersistedConversation =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(persisted.conversation_id, replacement.conversation_id);

        // Conversely, if the queued file still belongs to the evicted
        // binding, a different replacement must not inherit it. The flush
        // removes the old file; the replacement can then be persisted on the
        // deterministic path without a stale checkpoint race.
        let replacement2 = CursorConversation {
            conversation_id: uuid::Uuid::new_v4().to_string(),
            checkpoint: None,
            blobs: HashMap::new(),
            last_seen: now_millis(),
        };
        let removed = {
            let mut store = store_lock();
            let removed = store.map.remove(session).expect("replacement binding");
            store.order.retain(|item| item != session);
            removed
        };
        queue_persisted_delete(session, Some(&removed));
        {
            let mut store = store_lock();
            store.order.push_back(session.to_string());
            store.map.insert(session.to_string(), replacement2.clone());
        }
        flush_pending_persisted_deletes();
        assert!(
            !path.exists(),
            "a rebound binding must not retain an evicted snapshot"
        );
        persist_conversation(session, &replacement2).expect("persist replacement");
        let persisted: PersistedConversation =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(persisted.conversation_id, replacement2.conversation_id);

        // Once the replacement is actually absent, the deferred delete is
        // allowed to remove its persisted file as well.
        let removed = {
            let mut store = store_lock();
            let removed = store.map.remove(session).expect("replacement binding");
            store.order.retain(|item| item != session);
            removed
        };
        queue_persisted_delete(session, Some(&removed));
        flush_pending_persisted_deletes();
        assert!(!path.exists(), "unbound eviction should unlink the file");

        // Disk-only expiry uses `None` metadata; it must not be treated as a
        // matching empty binding and left behind indefinitely.
        std::fs::write(
            &path,
            serde_json::to_vec(&PersistedConversation {
                session_id: session.to_string(),
                conversation_id: uuid::Uuid::new_v4().to_string(),
                checkpoint_b64: None,
                blobs_b64: Vec::new(),
                last_seen: 1,
            })
            .unwrap(),
        )
        .unwrap();
        queue_persisted_delete(session, None);
        flush_pending_persisted_deletes();
        assert!(
            !path.exists(),
            "pending disk-only expiry must unlink the file"
        );
    }

    #[test]
    fn persisted_sweep_preserves_a_recent_in_memory_binding() {
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

        let session = "sess-live-sweep";
        let _ = get_or_create(session);
        let path = persist_path(session).expect("path");
        let mut dto: PersistedConversation =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        dto.last_seen = 1;
        std::fs::write(&path, serde_json::to_vec(&dto).unwrap()).unwrap();

        // The disk copy looks abandoned, but the in-memory binding was just
        // touched. The sweep must leave it for the next snapshot write.
        expire_abandoned_persisted(now_millis());
        assert!(
            path.exists(),
            "sweep must not delete a recent in-memory conversation"
        );
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
        drop_unpinned_in_memory_for_test();
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

    #[test]
    fn stale_snapshot_writer_cannot_overwrite_newer_checkpoint() {
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

        let session = "sess-persist-order";
        let _ = get_or_create(session);
        save_checkpoint(session, vec![0x01]);
        // Simulate a slow writer that captured this older snapshot before a
        // newer checkpoint was committed in memory.
        let stale = get(session).expect("checkpoint binding");
        save_checkpoint(session, vec![0x02]);
        persist_conversation(session, &stale).expect("stale write is harmless");

        drop_unpinned_in_memory_for_test();
        let recovered = continuation_for(Some(session));
        assert_eq!(recovered.conversation_state, vec![0x02]);
    }

    #[test]
    fn conditional_stale_binding_cannot_overwrite_rotated_disk_state() {
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

        let session = "sess-persist-rotation-order";
        let original = get_or_create(session);
        save_checkpoint(session, vec![0x03]);
        let stale = get(session).expect("stale binding");
        reset(session);
        assert!(matches!(
            persist_conversation_if_current(session, &original.conversation_id, &stale),
            Ok(false)
        ));

        drop_unpinned_in_memory_for_test();
        let recovered = continuation_for(Some(session));
        assert_ne!(
            recovered.conversation_id.as_deref(),
            Some(original.conversation_id.as_str())
        );
        assert!(!recovered.has_checkpoint);
    }
}
