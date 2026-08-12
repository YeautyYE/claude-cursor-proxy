//! Persist Cursor conversation_id + checkpoint + KV blobs across Claude Code turns.
//!
//! Official CLI keeps a ConversationStateStructure (blob-ID form) plus a content-
//! addressed blob store between Run streams. Without this, each Claude turn is a
//! fresh Cursor run that re-uploads the entire Anthropic history + tools schema.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use sha2::{Digest, Sha256};

const IDLE_TTL_MS: u64 = 30 * 60 * 1000;
const MAX_CONVERSATIONS: usize = 10_000;

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

#[derive(Default)]
struct Store {
    map: HashMap<String, CursorConversation>,
    order: VecDeque<String>,
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

fn persist_conversation(session_id: &str, conv: &CursorConversation) {
    let Some(path) = persist_path(session_id) else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
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
    let Ok(json) = serde_json::to_vec(&dto) else {
        return;
    };
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, json).is_ok() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
        }
        if std::fs::rename(&tmp, &path).is_ok() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            }
        }
    }
}

fn expire_abandoned_persisted(now: u64) {
    let Some(dir) = persist_dir() else {
        return;
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
        if now.saturating_sub(dto.last_seen) > IDLE_TTL_MS {
            let _ = std::fs::remove_file(path);
        }
    }
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
    while store.order.len() > MAX_CONVERSATIONS {
        if let Some(evict) = store.order.pop_front() {
            store.map.remove(&evict);
            delete_persisted(&evict);
        } else {
            break;
        }
    }
    // Drop idle entries opportunistically when touched.
    let stale: Vec<String> = store
        .map
        .iter()
        .filter(|(_, v)| now.saturating_sub(v.last_seen) > IDLE_TTL_MS)
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
    expire_abandoned_persisted(now);
    let mut store = store_lock();
    if let Some(existing) = store.map.get(session_id).cloned() {
        if now.saturating_sub(existing.last_seen) <= IDLE_TTL_MS {
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
    persist_conversation(session_id, &created);
    created
}

pub fn get(session_id: &str) -> Option<CursorConversation> {
    let now = now_millis();
    let mut store = store_lock();
    if let Some(existing) = store.map.get(session_id).cloned() {
        if now.saturating_sub(existing.last_seen) > IDLE_TTL_MS {
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
    persist_conversation(session_id, &snapshot);
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
    persist_conversation(session_id, &snapshot);
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
    persist_conversation(session_id, &snapshot);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use std::sync::Mutex;

    static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    #[test]
    fn get_or_create_reuses_conversation_id() {
        let _guard = TEST_LOCK.lock().unwrap();
        reset_for_test();
        let a = get_or_create("sess-1");
        let b = get_or_create("sess-1");
        assert_eq!(a.conversation_id, b.conversation_id);
        assert!(a.checkpoint.is_none());
    }

    #[test]
    fn checkpoint_and_blobs_round_trip() {
        let _guard = TEST_LOCK.lock().unwrap();
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
        let _guard = TEST_LOCK.lock().unwrap();
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

        let _guard = TEST_LOCK.lock().unwrap();
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
        let _guard = TEST_LOCK.lock().unwrap();
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

    #[cfg(unix)]
    #[test]
    fn persisted_conversation_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let _guard = TEST_LOCK.lock().unwrap();
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
        let _guard = TEST_LOCK.lock().unwrap();
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
}
