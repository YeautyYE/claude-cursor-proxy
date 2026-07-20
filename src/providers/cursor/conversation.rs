//! Persist Cursor conversation_id + checkpoint + KV blobs across Claude Code turns.
//!
//! Official CLI keeps a ConversationStateStructure (blob-ID form) plus a content-
//! addressed blob store between Run streams. Without this, each Claude turn is a
//! fresh Cursor run that re-uploads the entire Anthropic history + tools schema.

use std::collections::{HashMap, VecDeque};
use std::sync::{LazyLock, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn touch_and_evict(store: &mut Store, session_id: &str, now: u64) {
    if let Some(entry) = store.map.get_mut(session_id) {
        entry.last_seen = now;
    }
    while store.order.len() > MAX_CONVERSATIONS {
        if let Some(evict) = store.order.pop_front() {
            store.map.remove(&evict);
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
    }
}

/// Get or create the Cursor conversation binding for a Claude session id.
pub fn get_or_create(session_id: &str) -> CursorConversation {
    let now = now_millis();
    let mut store = STORE.lock().expect("cursor conversation lock");
    if let Some(existing) = store.map.get(session_id).cloned() {
        if now.saturating_sub(existing.last_seen) <= IDLE_TTL_MS {
            touch_and_evict(&mut store, session_id, now);
            return existing;
        }
        store.map.remove(session_id);
        store.order.retain(|item| item != session_id);
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
    created
}

pub fn get(session_id: &str) -> Option<CursorConversation> {
    let now = now_millis();
    let mut store = STORE.lock().expect("cursor conversation lock");
    let existing = store.map.get(session_id).cloned()?;
    if now.saturating_sub(existing.last_seen) > IDLE_TTL_MS {
        store.map.remove(session_id);
        store.order.retain(|item| item != session_id);
        return None;
    }
    touch_and_evict(&mut store, session_id, now);
    Some(existing)
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
    let mut store = STORE.lock().expect("cursor conversation lock");
    let entry = ensure_entry(&mut store, session_id, now);
    entry.checkpoint = Some(checkpoint);
    entry.last_seen = now;
    touch_and_evict(&mut store, session_id, now);
}

/// Merge KV blobs into the conversation store (set_blob wins).
pub fn merge_blobs(session_id: &str, blobs: &HashMap<Vec<u8>, Vec<u8>>) {
    if blobs.is_empty() {
        return;
    }
    let now = now_millis();
    let mut store = STORE.lock().expect("cursor conversation lock");
    let entry = ensure_entry(&mut store, session_id, now);
    for (id, data) in blobs {
        entry.blobs.insert(id.clone(), data.clone());
    }
    entry.last_seen = now;
    touch_and_evict(&mut store, session_id, now);
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
    let mut store = STORE.lock().expect("cursor conversation lock");
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
}
