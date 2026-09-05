//! In-process ownership and replay for one logical Sand operation.
//!
//! Sand requests are full-history and UUID-scoped, which makes replaying a
//! *failed* invocation valid.  It does not make duplicate HTTP requests from
//! a client valid: a retry that arrives while the original request is opening
//! used to create another InferenceService invocation.  Cursor then reports
//! that one of the invocations is already active.  This registry gives a
//! stable client operation one owner and lets retries subscribe to that
//! owner's event stream instead.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use super::live::{LiveEventResult, LiveRunEvent};

const DEFAULT_EVENT_REPLAY_CAPACITY: usize = 16_384;
const DEFAULT_TERMINAL_REPLAY_TTL_SECS: u64 = 30;
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 512;
const MAX_SUBSCRIBER_REPLAY_QUEUE: usize = 16_384;
// A single Claude Code/Grok turn can have the documented 512-way fan-out.
// Keep the duplicate-operation registry at least that wide as well: retries
// carrying one stable request id are subscribers, and rejecting subscribers
// at 128 turns a healthy upstream operation into proxy-generated 503s.
const DEFAULT_SUBSCRIBER_LIMIT: usize = 512;
const MAX_SUBSCRIBER_LIMIT: usize = 1_024;
const DEFAULT_REPLAY_BYTE_CAPACITY: usize = 16 * 1024 * 1024;
const MAX_REPLAY_BYTE_CAPACITY: usize = 128 * 1024 * 1024;
const DEFAULT_OPERATION_ENTRY_LIMIT: usize = 2_048;
const MAX_OPERATION_ENTRY_LIMIT: usize = 65_536;
const MAX_OPERATION_KEY_BYTES: usize = 512;
const OWNER_SEND_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SandOperationKey(String);

impl SandOperationKey {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        let mut value = value.into();
        if value.len() > MAX_OPERATION_KEY_BYTES {
            // Client/session identifiers are normally ASCII, but model and
            // agent labels may contain UTF-8. `String::truncate` requires a
            // character boundary; walk back from the byte cap so a malformed
            // key cannot panic the request handler.
            // Retain a short deterministic digest suffix so two oversized
            // client ids sharing the same prefix cannot alias one operation.
            let digest = value.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
            });
            let suffix = format!("-{digest:016x}");
            let mut boundary = MAX_OPERATION_KEY_BYTES.saturating_sub(suffix.len());
            while boundary > 0 && !value.is_char_boundary(boundary) {
                boundary -= 1;
            }
            value.truncate(boundary);
            value.push_str(&suffix);
        }
        Self(value)
    }
}

pub(crate) enum SandOperationAdmission {
    Owner(SandOperationOwner),
    Subscriber(SandOperationSubscription),
    /// The owner remains authoritative, but its bounded replay no longer has
    /// the stream prefix needed to attach another Anthropic response safely.
    /// The caller must surface backpressure rather than create a second
    /// upstream invocation.
    ReplayUnavailable,
    /// The process already tracks the configured number of logical Sand
    /// operations. Creating a second upstream owner would defeat the bound;
    /// callers should retry after an existing operation finishes.
    CapacityExceeded,
    /// The operation is valid, but its attached HTTP consumers have reached
    /// the bounded fan-out limit. Do not create an untracked sender.
    SubscriberLimit,
}

pub(crate) struct SandOperationOwner {
    key: SandOperationKey,
    entry: Arc<Mutex<SandOperationEntry>>,
    /// The owner is subscribed before opening upstream, so no first frame can
    /// race ahead of its downstream response.
    subscription: SandOperationSubscription,
    forward_started: bool,
}

impl SandOperationOwner {
    pub(crate) fn take_subscription(&mut self) -> SandOperationSubscription {
        std::mem::replace(&mut self.subscription, SandOperationSubscription::closed())
    }

    /// Return a clone of the owner's downstream sender so the upstream driver
    /// can observe HTTP cancellation even while the operation forwarder keeps
    /// the source channel alive.  `Sender::closed()` resolves as soon as the
    /// response body drops its receiver; keeping this clone does not retain
    /// that receiver or prevent the operation from finishing.
    pub(crate) fn owner_sender(&self) -> Option<mpsc::Sender<LiveEventResult>> {
        self.entry
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .subscribers
            .iter()
            .find(|subscriber| subscriber.owner)
            .map(|subscriber| subscriber.sender.clone())
    }

    /// Fan out the owner driver's events to the original HTTP request and all
    /// retry subscribers.  This task owns terminal state so every exit path
    /// closes subscribers and makes the bounded replay immediately visible.
    pub(crate) fn forward_from(&mut self, mut source: mpsc::Receiver<LiveEventResult>) {
        self.forward_started = true;
        let key = self.key.clone();
        let entry = Arc::clone(&self.entry);
        tokio::spawn(async move {
            while let Some(event) = source.recv().await {
                publish(&entry, event).await;
            }
            finish(&key, &entry);
        });
    }

    pub(crate) async fn fail(&self, message: String) {
        publish(&self.entry, Err(message)).await;
        finish(&self.key, &self.entry);
    }
}

impl Drop for SandOperationOwner {
    fn drop(&mut self) {
        // Every normal success starts the forwarder, and every normal error
        // calls `fail`. If an early return or cancellation slips between
        // admission and either transition, do not leave an immortal active
        // entry retaining its replay buffer and key forever.
        if self.forward_started {
            return;
        }
        let mut entry = self
            .entry
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if entry.finished_at.is_none() {
            entry.finished_at = Some(Instant::now());
            // No source/terminal event was published. A duplicate must not
            // receive an empty channel and mistake an abandoned owner for a
            // successful empty completion.
            entry.replay_truncated = true;
            entry.subscribers.clear();
        }
    }
}

pub(crate) struct SandOperationSubscription {
    receiver: mpsc::Receiver<LiveEventResult>,
}

impl SandOperationSubscription {
    fn closed() -> Self {
        let (_sender, receiver) = mpsc::channel(1);
        Self { receiver }
    }

    pub(crate) fn into_receiver(self) -> mpsc::Receiver<LiveEventResult> {
        self.receiver
    }
}

struct SandOperationEntry {
    replay: VecDeque<LiveEventResult>,
    replay_capacity: usize,
    replay_bytes: usize,
    replay_byte_capacity: usize,
    replay_truncated: bool,
    subscribers: Vec<SandOperationSubscriber>,
    finished_at: Option<Instant>,
    publish_lock: Arc<tokio::sync::Mutex<()>>,
}

struct SandOperationSubscriber {
    id: u64,
    sender: mpsc::Sender<LiveEventResult>,
    /// The original HTTP response is authoritative and must receive every
    /// event. Duplicate/retry HTTP responses are bounded best-effort
    /// subscribers and may be evicted when they stop draining.
    owner: bool,
}

static NEXT_SUBSCRIBER_ID: AtomicU64 = AtomicU64::new(1);

impl SandOperationEntry {
    fn new() -> Self {
        Self {
            replay: VecDeque::new(),
            replay_capacity: replay_capacity(),
            replay_bytes: 0,
            replay_byte_capacity: replay_byte_capacity(),
            replay_truncated: false,
            subscribers: Vec::new(),
            finished_at: None,
            publish_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    fn subscribe(&mut self, owner: bool) -> Result<SandOperationSubscription, SubscribeError> {
        // Replaying a truncated prefix would turn an ordinary transport retry
        // into a syntactically valid but semantically partial Claude answer.
        // Keep the single-owner invariant and let the caller retry after the
        // operation's short terminal retention instead.
        if self.replay_truncated {
            return Err(SubscribeError::ReplayUnavailable);
        }
        // Receivers can disappear between source events. Remove their senders
        // before enforcing the configured fan-out limit so a burst of canceled
        // HTTP retries cannot permanently consume all subscriber slots.
        self.subscribers
            .retain(|subscriber| !subscriber.sender.is_closed());
        if self.finished_at.is_none() && self.subscribers.len() >= subscriber_limit() {
            return Err(SubscribeError::SubscriberLimit);
        }
        // A channel must hold the complete replay before live events begin.
        // Bound that allocation independently of the event/byte replay caps;
        // once the history is larger, a fresh subscriber waits for the
        // terminal replay window instead of retaining an oversized queue.
        if self.replay.len() >= MAX_SUBSCRIBER_REPLAY_QUEUE {
            return Err(SubscribeError::ReplayUnavailable);
        }
        // The owner uses the same bounded queue as retry subscribers. Owner
        // delivery is awaited with a timeout below, so a larger queue would
        // only multiply per-operation memory without improving correctness.
        let capacity = SUBSCRIBER_CHANNEL_CAPACITY
            .max(self.replay.len().saturating_add(1))
            .min(MAX_SUBSCRIBER_REPLAY_QUEUE);
        let (sender, receiver) = mpsc::channel(capacity);
        for event in &self.replay {
            // The channel is deliberately sized above replay length.
            let _ = sender.try_send(event.clone());
        }
        if self.finished_at.is_none() {
            self.subscribers.push(SandOperationSubscriber {
                id: NEXT_SUBSCRIBER_ID.fetch_add(1, Ordering::Relaxed),
                sender,
                owner,
            });
        }
        Ok(SandOperationSubscription { receiver })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscribeError {
    ReplayUnavailable,
    SubscriberLimit,
}

static SAND_OPERATIONS: LazyLock<Mutex<HashMap<SandOperationKey, Arc<Mutex<SandOperationEntry>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Admit a stable Sand operation. Callers without a safe stable identity
/// should skip this registry rather than accidentally merging unrelated
/// stateless requests.
pub(crate) fn admit_sand_operation(key: SandOperationKey) -> SandOperationAdmission {
    let mut operations = SAND_OPERATIONS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    prune_expired(&mut operations);

    if let Some(entry) = operations.get(&key) {
        return match entry
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .subscribe(false)
        {
            Ok(subscription) => SandOperationAdmission::Subscriber(subscription),
            Err(SubscribeError::ReplayUnavailable) => SandOperationAdmission::ReplayUnavailable,
            Err(SubscribeError::SubscriberLimit) => SandOperationAdmission::SubscriberLimit,
        };
    }

    if operations.len() >= operation_entry_limit() && !evict_finished_entry(&mut operations) {
        return SandOperationAdmission::CapacityExceeded;
    }

    let entry = Arc::new(Mutex::new(SandOperationEntry::new()));
    let subscription = entry
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .subscribe(true)
        .expect("fresh Sand operation accepts its owner subscription");
    operations.insert(key.clone(), Arc::clone(&entry));
    SandOperationAdmission::Owner(SandOperationOwner {
        key,
        entry,
        subscription,
        forward_started: false,
    })
}

async fn publish(entry: &Arc<Mutex<SandOperationEntry>>, event: LiveEventResult) {
    let event_bytes = replay_event_bytes(&event);
    let publish_lock = entry
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .publish_lock
        .clone();
    // Serialize source events and terminal failures. A subscriber admitted
    // while one event is waiting on the owner must see either the complete
    // replay prefix or subsequent live events, never both copies of the same
    // event.
    let _publish_guard = publish_lock.lock().await;
    // Update replay state and snapshot only authoritative owner senders while
    // holding the mutex. Never hold an operation lock over an async send: a
    // stalled owner must not block duplicate admission, terminal pruning, or
    // another lifecycle transition from observing the operation.
    let (owner_senders, retry_senders) = {
        let mut state = entry.lock().unwrap_or_else(|poison| poison.into_inner());
        if event_bytes > state.replay_byte_capacity {
            // A single giant tool payload cannot fit the replay budget.
            // Preserve live owner delivery, but mark future subscriptions
            // unavailable rather than retaining an over-limit event.
            state.replay.clear();
            state.replay_bytes = 0;
            state.replay_truncated = true;
        } else {
            while state.replay.len() >= state.replay_capacity
                || state.replay_bytes.saturating_add(event_bytes) > state.replay_byte_capacity
            {
                let Some(oldest) = state.replay.pop_front() else {
                    break;
                };
                state.replay_bytes = state
                    .replay_bytes
                    .saturating_sub(replay_event_bytes(&oldest));
                state.replay_truncated = true;
            }
            state.replay.push_back(event.clone());
            state.replay_bytes = state.replay_bytes.saturating_add(event_bytes);
        }
        let owner_senders = state
            .subscribers
            .iter()
            .filter(|subscriber| subscriber.owner)
            .map(|subscriber| (subscriber.id, subscriber.sender.clone()))
            .collect::<Vec<_>>();
        let retry_senders = state
            .subscribers
            .iter()
            .filter(|subscriber| !subscriber.owner)
            .map(|subscriber| (subscriber.id, subscriber.sender.clone()))
            .collect::<Vec<_>>();
        (owner_senders, retry_senders)
    };

    // The original HTTP response is authoritative: await only that one
    // bounded channel so text/tool deltas cannot be silently dropped for the
    // caller that owns the upstream invocation. Retry subscribers are
    // explicitly best-effort and use `try_send`, so a duplicate request that
    // stopped reading can never stall the upstream driver.
    // Failed subscriber ids are looked up once for every retained subscriber.
    // A set keeps a 512-way fan-out at linear cost per event instead of
    // repeatedly scanning a vector (which becomes quadratic for large retry
    // waves).
    let mut failed_ids = HashSet::new();
    let mut owner_closed = false;
    for (id, sender) in owner_senders {
        let delivered = tokio::time::timeout(OWNER_SEND_TIMEOUT, sender.send(event.clone()))
            .await
            .is_ok_and(|result| result.is_ok());
        if !delivered {
            owner_closed = true;
            failed_ids.insert(id);
        }
    }

    for (id, sender) in retry_senders {
        if sender.try_send(event.clone()).is_err() {
            failed_ids.insert(id);
        }
    }

    let mut state = entry.lock().unwrap_or_else(|poison| poison.into_inner());
    // Subscribers admitted after the replay snapshot already received this
    // event as part of their replay prefix. Do not send it a second time here;
    // only remove senders that were part of the snapshot and failed.
    state.subscribers.retain(|subscriber| {
        !(failed_ids.contains(&subscriber.id)
            || subscriber.sender.is_closed()
            || (subscriber.owner && owner_closed))
    });
}

fn replay_event_bytes(event: &LiveEventResult) -> usize {
    const EVENT_OVERHEAD: usize = 64;
    match event {
        Err(message) => EVENT_OVERHEAD.saturating_add(message.len()),
        Ok(LiveRunEvent::Cursor(cursor)) => match cursor {
            super::response::CursorStreamEvent::Session { session_id } => {
                EVENT_OVERHEAD.saturating_add(session_id.len())
            }
            super::response::CursorStreamEvent::ThinkingDelta { text }
            | super::response::CursorStreamEvent::TextDelta { text } => {
                EVENT_OVERHEAD.saturating_add(text.len())
            }
            super::response::CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            } => EVENT_OVERHEAD
                .saturating_add(tool_use_id.len())
                .saturating_add(name.len())
                .saturating_add(
                    serde_json::to_string(input)
                        .map(|value| value.len())
                        .unwrap_or(256),
                ),
            super::response::CursorStreamEvent::Usage { .. }
            | super::response::CursorStreamEvent::OutputTokenDelta { .. }
            | super::response::CursorStreamEvent::End => EVENT_OVERHEAD,
        },
        Ok(LiveRunEvent::NativeToolBatch(tools)) => tools
            .iter()
            .map(|tool| {
                EVENT_OVERHEAD
                    .saturating_add(tool.tool_use_id.len())
                    .saturating_add(tool.name.len())
                    .saturating_add(
                        serde_json::to_string(&tool.input)
                            .map(|value| value.len())
                            .unwrap_or(256),
                    )
            })
            .sum::<usize>()
            .max(EVENT_OVERHEAD),
    }
}

fn finish(key: &SandOperationKey, entry: &Arc<Mutex<SandOperationEntry>>) {
    let mut state = entry.lock().unwrap_or_else(|poison| poison.into_inner());
    state.finished_at = Some(Instant::now());
    state.subscribers.clear();

    // Avoid holding a per-operation mutex while taking the global registry
    // lock on the hot publish path. `prune_expired` removes this completed
    // entry after its replay TTL.
    let _ = key;
}

fn replay_capacity() -> usize {
    std::env::var("CCP_CURSOR_SAND_OPERATION_REPLAY_EVENTS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_EVENT_REPLAY_CAPACITY)
        .clamp(128, 65_536)
}

fn replay_byte_capacity() -> usize {
    std::env::var("CCP_CURSOR_SAND_OPERATION_REPLAY_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_REPLAY_BYTE_CAPACITY)
        .clamp(64 * 1024, MAX_REPLAY_BYTE_CAPACITY)
}

fn subscriber_limit() -> usize {
    std::env::var("CCP_CURSOR_SAND_OPERATION_SUBSCRIBERS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_SUBSCRIBER_LIMIT)
        .clamp(1, MAX_SUBSCRIBER_LIMIT)
}

fn operation_entry_limit() -> usize {
    std::env::var("CCP_CURSOR_SAND_OPERATION_ENTRIES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_OPERATION_ENTRY_LIMIT)
        .clamp(128, MAX_OPERATION_ENTRY_LIMIT)
}

fn evict_finished_entry(
    operations: &mut HashMap<SandOperationKey, Arc<Mutex<SandOperationEntry>>>,
) -> bool {
    let oldest = operations
        .iter()
        .filter_map(|(key, entry)| {
            let finished_at = entry
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .finished_at?;
            // Never evict a still-replayable terminal operation merely to
            // admit an unrelated owner. Doing so would let a client retry
            // during the retention window and accidentally create a second
            // upstream invocation. `prune_expired` normally handles this;
            // the age check also covers a finish racing with that sweep.
            if finished_at.elapsed() < terminal_replay_ttl() {
                return None;
            }
            Some((key.clone(), finished_at))
        })
        .min_by_key(|(_, finished_at)| *finished_at)
        .map(|(key, _)| key);
    oldest.and_then(|key| operations.remove(&key)).is_some()
}

fn terminal_replay_ttl() -> Duration {
    Duration::from_secs(
        std::env::var("CCP_CURSOR_SAND_OPERATION_REPLAY_TTL_SECS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_TERMINAL_REPLAY_TTL_SECS)
            .clamp(1, 300),
    )
}

fn prune_expired(operations: &mut HashMap<SandOperationKey, Arc<Mutex<SandOperationEntry>>>) {
    let ttl = terminal_replay_ttl();
    operations.retain(|_, entry| {
        let entry = entry.lock().unwrap_or_else(|poison| poison.into_inner());
        entry
            .finished_at
            .is_none_or(|finished| finished.elapsed() < ttl)
    });
}

#[cfg(test)]
pub(crate) fn clear_sand_operations_for_test() {
    SAND_OPERATIONS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::live::LiveRunEvent;
    use crate::providers::cursor::response::CursorStreamEvent;

    fn event(text: &str) -> LiveEventResult {
        Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: text.to_string(),
        }))
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn concurrent_duplicate_subscribes_without_second_owner() {
        let _serial = test_lock();
        clear_sand_operations_for_test();
        let key = SandOperationKey::new("same-request");
        let mut owner = match admit_sand_operation(key.clone()) {
            SandOperationAdmission::Owner(owner) => owner,
            SandOperationAdmission::Subscriber(_) => panic!("first caller must own operation"),
            SandOperationAdmission::ReplayUnavailable => panic!("fresh operation has full replay"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("fresh operation admission unexpectedly bounded")
            }
        };
        let duplicate = match admit_sand_operation(key) {
            SandOperationAdmission::Subscriber(subscription) => subscription,
            SandOperationAdmission::Owner(_) => panic!("duplicate must attach"),
            SandOperationAdmission::ReplayUnavailable => panic!("fresh duplicate has full replay"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("duplicate admission unexpectedly bounded")
            }
        };

        let (source_tx, source_rx) = mpsc::channel(4);
        owner.forward_from(source_rx);
        source_tx.send(event("hello")).await.expect("publish event");
        drop(source_tx);

        let mut original = owner.take_subscription().into_receiver();
        let mut duplicate = duplicate.into_receiver();
        assert!(
            matches!(original.recv().await, Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }))) if text == "hello")
        );
        assert!(
            matches!(duplicate.recv().await, Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }))) if text == "hello")
        );
        clear_sand_operations_for_test();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn terminal_retry_replays_without_reopening() {
        let _serial = test_lock();
        clear_sand_operations_for_test();
        let key = SandOperationKey::new("terminal-replay");
        let mut owner = match admit_sand_operation(key.clone()) {
            SandOperationAdmission::Owner(owner) => owner,
            SandOperationAdmission::Subscriber(_) => panic!("first caller must own operation"),
            SandOperationAdmission::ReplayUnavailable => panic!("fresh operation has full replay"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("fresh operation admission unexpectedly bounded")
            }
        };
        let (source_tx, source_rx) = mpsc::channel(4);
        owner.forward_from(source_rx);
        source_tx.send(event("done")).await.expect("publish event");
        drop(source_tx);
        let mut original = owner.take_subscription().into_receiver();
        let _ = original.recv().await;
        while original.recv().await.is_some() {}
        tokio::task::yield_now().await;

        let retry = match admit_sand_operation(key) {
            SandOperationAdmission::Subscriber(subscription) => subscription,
            SandOperationAdmission::Owner(_) => {
                panic!("completed operation must replay during ttl")
            }
            SandOperationAdmission::ReplayUnavailable => {
                panic!("small replay must remain available")
            }
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("terminal replay admission unexpectedly bounded")
            }
        };
        let mut retry = retry.into_receiver();
        assert!(
            matches!(retry.recv().await, Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }))) if text == "done")
        );
        assert!(retry.recv().await.is_none());
        clear_sand_operations_for_test();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn owner_open_failure_finishes_and_replays_terminal_error() {
        let _serial = test_lock();
        clear_sand_operations_for_test();
        let key = SandOperationKey::new("open-failure");
        let mut owner = match admit_sand_operation(key.clone()) {
            SandOperationAdmission::Owner(owner) => owner,
            SandOperationAdmission::Subscriber(_) => panic!("first caller must own operation"),
            SandOperationAdmission::ReplayUnavailable => panic!("fresh operation has full replay"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("fresh operation admission unexpectedly bounded")
            }
        };
        owner.fail("upstream open failed".to_string()).await;
        let mut original = owner.take_subscription().into_receiver();
        assert!(
            matches!(original.recv().await, Some(Err(message)) if message == "upstream open failed")
        );
        assert!(original.recv().await.is_none());

        let retry = match admit_sand_operation(key) {
            SandOperationAdmission::Subscriber(subscription) => subscription,
            SandOperationAdmission::Owner(_) => {
                panic!("failed operation must stay replayable during ttl")
            }
            SandOperationAdmission::ReplayUnavailable => panic!("single error stays replayable"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("terminal replay admission unexpectedly bounded")
            }
        };
        let mut retry = retry.into_receiver();
        assert!(
            matches!(retry.recv().await, Some(Err(message)) if message == "upstream open failed")
        );
        assert!(retry.recv().await.is_none());
        clear_sand_operations_for_test();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn slow_subscriber_is_evicted_without_blocking_owner_publish() {
        let _serial = test_lock();
        clear_sand_operations_for_test();
        let key = SandOperationKey::new("slow-subscriber");
        let mut owner = match admit_sand_operation(key.clone()) {
            SandOperationAdmission::Owner(owner) => owner,
            SandOperationAdmission::Subscriber(_) => panic!("first caller must own operation"),
            SandOperationAdmission::ReplayUnavailable => panic!("fresh operation has full replay"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("fresh operation admission unexpectedly bounded")
            }
        };
        let duplicate = match admit_sand_operation(key) {
            SandOperationAdmission::Subscriber(subscription) => subscription,
            SandOperationAdmission::Owner(_) => panic!("duplicate must attach"),
            SandOperationAdmission::ReplayUnavailable => panic!("fresh duplicate has full replay"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("duplicate admission unexpectedly bounded")
            }
        };
        let mut owner_rx = owner.take_subscription().into_receiver();
        let entry = Arc::clone(&owner.entry);

        // Keep the duplicate receiver unread. Once its bounded channel fills,
        // publishing must evict it and continue delivering to the owner.
        for index in 0..=SUBSCRIBER_CHANNEL_CAPACITY {
            publish(&entry, event(&format!("{index}"))).await;
            while owner_rx.try_recv().is_ok() {}
        }
        let subscribers = entry
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .subscribers
            .len();
        assert_eq!(subscribers, 1, "only the healthy owner should remain");

        drop(duplicate);
        drop(owner_rx);
        publish(&entry, event("cleanup")).await;
        assert!(
            entry
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .subscribers
                .is_empty()
        );
        clear_sand_operations_for_test();
    }

    #[tokio::test]
    async fn replay_byte_budget_marks_history_unavailable_before_growth() {
        let entry = Arc::new(Mutex::new(SandOperationEntry::new()));
        {
            let mut state = entry.lock().unwrap_or_else(|poison| poison.into_inner());
            state.replay_capacity = 100;
            state.replay_byte_capacity = 100;
        }

        publish(&entry, event("first")).await;
        publish(&entry, event("second")).await;

        let state = entry.lock().unwrap_or_else(|poison| poison.into_inner());
        assert!(
            state.replay_truncated,
            "byte eviction must disable replay attach"
        );
        assert!(state.replay_bytes <= state.replay_byte_capacity);
        assert_eq!(state.replay.len(), 1);
    }

    #[tokio::test]
    async fn giant_event_cannot_exceed_replay_byte_budget() {
        let entry = Arc::new(Mutex::new(SandOperationEntry::new()));
        entry
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .replay_byte_capacity = 64;

        publish(&entry, event("payload")).await;

        let state = entry.lock().unwrap_or_else(|poison| poison.into_inner());
        assert!(state.replay_truncated);
        assert!(state.replay.is_empty());
        assert_eq!(state.replay_bytes, 0);
    }

    #[test]
    fn owner_drop_marks_unstarted_operation_terminal_for_pruning() {
        let _serial = test_lock();
        clear_sand_operations_for_test();
        let owner = match admit_sand_operation(SandOperationKey::new("dropped-owner")) {
            SandOperationAdmission::Owner(owner) => owner,
            SandOperationAdmission::Subscriber(_) => panic!("fresh operation must own"),
            SandOperationAdmission::ReplayUnavailable => panic!("fresh replay unavailable"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("fresh operation admission unexpectedly bounded")
            }
        };
        let entry = Arc::clone(&owner.entry);
        drop(owner);
        assert!(
            entry
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .finished_at
                .is_some()
        );
        clear_sand_operations_for_test();
    }

    #[test]
    fn finished_entry_eviction_keeps_active_registry_entries() {
        let mut operations = HashMap::new();
        let finished = Arc::new(Mutex::new(SandOperationEntry::new()));
        finished
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .finished_at = Some(Instant::now() - terminal_replay_ttl() - Duration::from_secs(1));
        let active = Arc::new(Mutex::new(SandOperationEntry::new()));
        operations.insert(SandOperationKey::new("finished"), finished);
        operations.insert(SandOperationKey::new("active"), active);

        assert!(evict_finished_entry(&mut operations));
        assert!(!operations.contains_key(&SandOperationKey::new("finished")));
        assert!(operations.contains_key(&SandOperationKey::new("active")));
        assert!(!evict_finished_entry(&mut operations));
    }

    #[test]
    fn operation_key_truncation_preserves_utf8_boundary() {
        let key = SandOperationKey::new("界".repeat(300));
        assert!(key.0.len() <= MAX_OPERATION_KEY_BYTES);
        assert!(std::str::from_utf8(key.0.as_bytes()).is_ok());

        let first = SandOperationKey::new(format!("{}a", "x".repeat(600)));
        let second = SandOperationKey::new(format!("{}b", "x".repeat(600)));
        assert_ne!(first, second, "oversized ids must not alias by prefix");
    }

    #[test]
    fn stale_subscribers_are_removed_before_limit_check() {
        let mut entry = SandOperationEntry::new();
        for _ in 0..subscriber_limit() {
            let (sender, receiver) = mpsc::channel(1);
            drop(receiver);
            entry.subscribers.push(SandOperationSubscriber {
                id: NEXT_SUBSCRIBER_ID.fetch_add(1, Ordering::Relaxed),
                sender,
                owner: false,
            });
        }
        assert!(entry.subscribe(false).is_ok());
        assert_eq!(entry.subscribers.len(), 1);
    }

    #[test]
    fn subscriber_capacity_covers_documented_512_way_retry_fanout() {
        // The owner occupies one slot. Every remaining slot must still be
        // attachable so a burst of same-operation HTTP retries does not emit
        // a local 503 while the original upstream stream is healthy.
        // Keep this deterministic when an operator deliberately overrides
        // the limit for a process-wide deployment test.
        if std::env::var_os("CCP_CURSOR_SAND_OPERATION_SUBSCRIBERS").is_some() {
            return;
        }
        assert!(subscriber_limit() >= 512);
        let mut entry = SandOperationEntry::new();
        let mut subscriptions = Vec::with_capacity(subscriber_limit());
        subscriptions.push(entry.subscribe(true).expect("owner slot"));
        for _ in 1..subscriber_limit() {
            subscriptions.push(entry.subscribe(false).expect("retry subscriber slot"));
        }
        assert_eq!(entry.subscribers.len(), subscriber_limit());
        assert!(matches!(
            entry.subscribe(false),
            Err(SubscribeError::SubscriberLimit)
        ));
        drop(subscriptions);
    }

    #[tokio::test]
    async fn abandoned_owner_is_not_replayed_as_empty_success() {
        let _serial = test_lock();
        clear_sand_operations_for_test();
        let key = SandOperationKey::new("abandoned-owner");
        let owner = match admit_sand_operation(key.clone()) {
            SandOperationAdmission::Owner(owner) => owner,
            SandOperationAdmission::Subscriber(_) => panic!("fresh operation must own"),
            SandOperationAdmission::ReplayUnavailable => panic!("fresh replay unavailable"),
            SandOperationAdmission::CapacityExceeded | SandOperationAdmission::SubscriberLimit => {
                panic!("fresh operation admission unexpectedly bounded")
            }
        };
        drop(owner);

        assert!(matches!(
            admit_sand_operation(key),
            SandOperationAdmission::ReplayUnavailable
        ));
        clear_sand_operations_for_test();
    }

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|poison| poison.into_inner())
    }
}
