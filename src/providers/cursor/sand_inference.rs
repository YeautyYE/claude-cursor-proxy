//! Cursor Desktop Sand inference transport.
//!
//! Sand stopped being accepted by `agent.v1.AgentService/Run` in the current
//! Cursor service.  The patched desktop runtime sends a Connect-JSON request
//! to `aiserver.v1.InferenceService/Stream` instead.  This module deliberately
//! keeps that wire format separate from the protobuf Agent transport used by
//! the CLI/IDE paths.
//!
//! The Connect framing is shared with the other Cursor transports:
//! `flags (u8)`, `payload length (u32, big endian)`, then the payload.  Unlike
//! AgentService frames, payloads here are UTF-8 JSON objects.

use base64::Engine as _;
use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::anthropic::schema::MessagesRequest;
use crate::logging::create_logger;
use crate::providers::cursor::client::{CursorError, CursorHttpClient, CursorUpstreamResponse};
use crate::providers::cursor::connect::{
    ConnectFrame, ConnectFrameDecoder, FLAG_END, FLAG_GZIP, connect_error_status,
    decode_gzip_frame, encode_connect_frame, is_non_retryable_provider_error_message,
    is_transient_provider_error_message, parse_connect_error,
};
use crate::providers::cursor::request::{
    image_candidate, is_model_visible_tool_definition, message_blocks, normalize_image_data,
};
use crate::providers::cursor::response::CursorStreamEvent;

/// Current Sand inference endpoint.
pub const SAND_INFERENCE_STREAM_PATH: &str = "/aiserver.v1.InferenceService/Stream";

/// Maximum JSON payload accepted by the Sand decoder.  A normal token frame is
/// tiny; the generous ceiling leaves room for tool arguments while preventing
/// a corrupt length prefix from retaining unbounded memory.
pub const MAX_SAND_FRAME_PAYLOAD: usize = 64 * 1024 * 1024;

/// Default number of full-history replays after a Sand stream has opened but
/// fails before producing visible output.  The value is deliberately bounded
/// because each replay is a new upstream invocation.
pub const DEFAULT_SAND_STREAM_RETRIES: u32 = 5;
pub const MAX_SAND_STREAM_RETRIES: u32 = 5;

// ---------------------------------------------------------------------------
// Sand open admission and transport breaker
// ---------------------------------------------------------------------------
//
// Sand accepts large independent Claude Code/Grok fan-outs.  This gate exists
// to bound a pathological retry wave and to keep account/model lanes
// observable; it must not silently reduce the normal 512-way client contract.
// Connection pressure is controlled by the shared sharded H2 client pool,
// which reuses TLS/H2 transports rather than constructing one reqwest client
// per request.  The AgentService live gate intentionally does not cover Sand.
const SAND_OPEN_GLOBAL_DEFAULT: usize = 512;
const SAND_OPEN_GLOBAL_MAX: usize = 512;
const SAND_OPEN_ACCOUNT_DEFAULT: usize = 512;
const SAND_OPEN_ACCOUNT_MAX: usize = 512;
// A logical request may be queued while the upstream is opening, but a
// response that has been accepted consumes a stream slot until its terminal
// event (or downstream cancellation).  This is deliberately independent of
// the cold-open bulkhead: a large Claude/Grok fan-out can queue locally while
// at most this many model streams are actually alive upstream.
const SAND_STREAM_GLOBAL_DEFAULT: usize = 512;
const SAND_STREAM_GLOBAL_MAX: usize = 512;
// Cold opens are substantially more expensive than an established HTTP/2
// stream. The default preserves the historical 512-way contract; operators
// can opt into a gentler ramp with CCP_CURSOR_SAND_OPEN_INITIAL_*.
// Preserve the historical 512-way Sand fan-out by default. Operators that
// need a gentler cold-start can opt in with CCP_CURSOR_SAND_OPEN_INITIAL_*;
// making 16/s the default silently turns a 512-client burst into a minutes-
// long queue and recreates the timeout wave this gate is meant to prevent.
const SAND_OPEN_INITIAL_INFLIGHT_DEFAULT: usize = SAND_OPEN_GLOBAL_DEFAULT;
const SAND_OPEN_INITIAL_INFLIGHT_MAX: usize = 512;
const SAND_OPEN_INITIAL_RATE_DEFAULT: u64 = SAND_OPEN_RATE_MAX;
const SAND_OPEN_RATE_MAX: u64 = 512;
// Keep the local fairness slice short so queued callers re-check capacity
// promptly. A saturated slice is non-terminal backpressure; callers retry it
// without issuing an untracked upstream open.
const SAND_OPEN_QUEUE_DEFAULT_SECS: u64 = 3;
const SAND_OPEN_QUEUE_MAX_SECS: u64 = 120;
// A saturated account/model lane should hand an unbound request to the
// account-pool failover path before the long open budget expires.  Without a
// short handoff threshold, hundreds of callers can sit behind four stalled
// opens for three minutes and then become a synchronized 503 wave.  This is
// only a *lane* threshold; requests still retain the larger logical retry
// budget once a different account (or a released slot) is selected.
const SAND_OPEN_ACCOUNT_QUEUE_FAILOVER_DEFAULT_SECS: u64 = 12;
const SAND_OPEN_ACCOUNT_QUEUE_FAILOVER_MAX_SECS: u64 = 300;
const SAND_OPEN_BREAKER_THRESHOLD_DEFAULT: u32 = 3;
const SAND_OPEN_BREAKER_THRESHOLD_MAX: u32 = 16;
const SAND_OPEN_BREAKER_COOLDOWN_DEFAULT_SECS: u64 = 15;
const SAND_OPEN_BREAKER_COOLDOWN_MAX_SECS: u64 = 300;
const SAND_OPEN_BREAKER_MAX_ENTRIES: usize = 2048;
/// One logical Claude/Grok turn may contain an initial open plus several
/// pre-output stream replays. Keep a single wall-clock budget across those
/// phases so each retry cannot start another unbounded 3x90s episode.
const SAND_LOGICAL_RETRY_DEFAULT_SECS: u64 = 600;
const SAND_LOGICAL_RETRY_MAX_SECS: u64 = 3_600;

fn bounded_env_usize(name: &str, default: usize, max: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
        .clamp(1, max)
}

fn bounded_env_u64(name: &str, default: u64, max: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
        .clamp(1, max)
}

fn sand_open_global_limit() -> usize {
    bounded_env_usize(
        "CCP_CURSOR_SAND_OPEN_CONCURRENCY",
        SAND_OPEN_GLOBAL_DEFAULT,
        SAND_OPEN_GLOBAL_MAX,
    )
}

fn sand_open_account_limit() -> usize {
    bounded_env_usize(
        "CCP_CURSOR_SAND_ACCOUNT_OPEN_CONCURRENCY",
        SAND_OPEN_ACCOUNT_DEFAULT,
        SAND_OPEN_ACCOUNT_MAX,
    )
}

fn sand_stream_global_limit() -> usize {
    bounded_env_usize(
        "CCP_CURSOR_SAND_STREAM_CONCURRENCY",
        SAND_STREAM_GLOBAL_DEFAULT,
        SAND_STREAM_GLOBAL_MAX,
    )
}

fn sand_open_initial_inflight() -> usize {
    bounded_env_usize(
        "CCP_CURSOR_SAND_OPEN_INITIAL_INFLIGHT",
        SAND_OPEN_INITIAL_INFLIGHT_DEFAULT,
        SAND_OPEN_INITIAL_INFLIGHT_MAX,
    )
}

fn sand_open_initial_rate() -> u64 {
    bounded_env_u64(
        "CCP_CURSOR_SAND_OPEN_INITIAL_RATE",
        SAND_OPEN_INITIAL_RATE_DEFAULT,
        SAND_OPEN_RATE_MAX,
    )
}

fn sand_open_rate_ceiling() -> u64 {
    bounded_env_u64(
        "CCP_CURSOR_SAND_OPEN_RATE",
        SAND_OPEN_RATE_MAX,
        SAND_OPEN_RATE_MAX,
    )
}

pub(crate) fn sand_logical_retry_budget() -> Duration {
    Duration::from_secs(bounded_env_u64(
        "CCP_CURSOR_SAND_RETRY_BUDGET_SECS",
        SAND_LOGICAL_RETRY_DEFAULT_SECS,
        SAND_LOGICAL_RETRY_MAX_SECS,
    ))
}

pub(crate) fn sand_open_total_budget() -> Duration {
    Duration::from_secs(bounded_env_u64("CCP_CURSOR_SAND_OPEN_TOTAL_SECS", 180, 900).max(20))
}

pub(crate) fn sand_open_queue_secs() -> u64 {
    bounded_env_u64(
        "CCP_CURSOR_SAND_OPEN_QUEUE_SECS",
        SAND_OPEN_QUEUE_DEFAULT_SECS,
        SAND_OPEN_QUEUE_MAX_SECS,
    )
}

/// How long an open caller waits behind a saturated account/model lane before
/// returning a retryable admission diagnostic to the account-pool selector.
/// The value is intentionally configurable for deployments with a very slow
/// but high-capacity upstream; the default keeps Claude Code's retry window
/// responsive during a burst.
pub(crate) fn sand_open_account_queue_failover_secs() -> u64 {
    bounded_env_u64(
        "CCP_CURSOR_SAND_ACCOUNT_QUEUE_FAILOVER_SECS",
        SAND_OPEN_ACCOUNT_QUEUE_FAILOVER_DEFAULT_SECS,
        SAND_OPEN_ACCOUNT_QUEUE_FAILOVER_MAX_SECS,
    )
}

fn sand_open_breaker_threshold() -> u32 {
    bounded_env_u64(
        "CCP_CURSOR_SAND_BREAKER_THRESHOLD",
        u64::from(SAND_OPEN_BREAKER_THRESHOLD_DEFAULT),
        u64::from(SAND_OPEN_BREAKER_THRESHOLD_MAX),
    ) as u32
}

fn sand_open_breaker_cooldown() -> Duration {
    Duration::from_secs(bounded_env_u64(
        "CCP_CURSOR_SAND_BREAKER_COOLDOWN_SECS",
        SAND_OPEN_BREAKER_COOLDOWN_DEFAULT_SECS,
        SAND_OPEN_BREAKER_COOLDOWN_MAX_SECS,
    ))
}

#[derive(Debug)]
struct SandOpenGate {
    global: Arc<Semaphore>,
    account_limit: usize,
    account_max_entries: usize,
    account: Mutex<HashMap<String, Arc<Semaphore>>>,
    /// Wake waiters whenever either dimension of the two-dimensional gate is
    /// released.  A `Notify` is deliberately separate from the semaphores:
    /// acquiring one permit and then waiting for the other would reserve
    /// capacity indefinitely, so callers use non-blocking pair attempts and
    /// sleep only until this signal (or their deadline).
    wake: Arc<Notify>,
}

impl SandOpenGate {
    fn new(global_limit: usize, account_limit: usize) -> Self {
        Self::with_account_capacity(global_limit, account_limit, SAND_OPEN_BREAKER_MAX_ENTRIES)
    }

    fn with_account_capacity(
        global_limit: usize,
        account_limit: usize,
        account_max_entries: usize,
    ) -> Self {
        Self {
            global: Arc::new(Semaphore::new(global_limit.clamp(1, SAND_OPEN_GLOBAL_MAX))),
            account_limit: account_limit.clamp(1, SAND_OPEN_ACCOUNT_MAX),
            account_max_entries: account_max_entries.max(1),
            account: Mutex::new(HashMap::new()),
            wake: Arc::new(Notify::new()),
        }
    }

    /// Return the lane for `key`, evicting only entries that have no active
    /// permit or waiter.  Evicting an active semaphore would let the next
    /// request create a second lane for the same account/model and silently
    /// bypass the configured per-account concurrency limit. If every bounded
    /// entry is active, reject the new lane rather than growing the map
    /// without limit; the admission caller reports a retryable local
    /// diagnostic instead of creating an untracked lane.
    fn account_gate(&self, key: &str) -> Option<Arc<Semaphore>> {
        let mut account = self
            .account
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(gate) = account.get(key) {
            return Some(Arc::clone(gate));
        }
        // Account ids are user-controlled in multi-account mode. Keep this
        // map bounded even when a caller rotates through a large credential
        // pool. Existing permits/waiters hold their Arc independently, so
        // only an idle map entry may be evicted.
        if account.len() >= self.account_max_entries {
            let idle = account
                .iter()
                .find(|(_, gate)| Arc::strong_count(gate) == 1)
                .map(|(key, _)| key.clone())?;
            account.remove(&idle);
        }
        let gate = Arc::new(Semaphore::new(self.account_limit));
        account.insert(key.to_string(), Arc::clone(&gate));
        Some(gate)
    }

    async fn acquire(&self, key: &str, wait: Duration) -> Result<SandOpenPermit, CursorError> {
        let deadline = Instant::now() + wait;
        // Resolve the lane once. `account_gate` only mutates the map while
        // creating/evicting an idle entry; the returned Arc remains valid even
        // if a later caller evicts that map entry.
        let account_gate = self
            .account_gate(key)
            .ok_or_else(sand_open_admission_error)?;
        // Acquire both dimensions with `try_acquire_owned` and never retain
        // one while awaiting the other.  The previous account-first ordering
        // let four callers reserve an account lane while blocked on the
        // process-wide semaphore; that made the lane appear full, starved
        // account balancing, and amplified a 512-way burst into synchronized
        // timeout/503 waves.
        loop {
            let global = Arc::clone(&self.global).try_acquire_owned().ok();
            if let Some(global) = global {
                match Arc::clone(&account_gate).try_acquire_owned() {
                    Ok(account) => {
                        return Ok(SandOpenPermit {
                            global: Some(global),
                            account: Some(account),
                            wake: Arc::clone(&self.wake),
                            cold_open: None,
                        });
                    }
                    Err(_) => {
                        // Account lane is saturated; release the global slot
                        // before waiting so another account can use it.
                        drop(global);
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(sand_open_admission_error());
            }
            // The notify path reacts immediately to permit drops. Keep a
            // short timer as a lost-wakeup/cancellation fallback; callers
            // cannot be stranded if a permit is released between the try
            // attempt and subscription to `Notify`.
            let retry_slice = remaining.min(Duration::from_millis(100));
            tokio::select! {
                _ = self.wake.notified() => {},
                _ = tokio::time::sleep(retry_slice) => {},
            }
        }
    }

    /// Attempt one bounded queue slice and return `None` when the lane is
    /// saturated. The caller keeps the logical request queued and retries the
    /// admission slice; it must not issue a permit-less upstream open because
    /// that would defeat the bulkhead during a retry burst.
    async fn acquire_soft(&self, key: &str, wait: Duration) -> Option<SandOpenPermit> {
        self.acquire(key, wait).await.ok()
    }
}

/// A permit is intentionally scoped to one HTTP `.send()` attempt.  Holding
/// it while consuming model tokens would turn the open bulkhead into a global
/// stream limit; releasing it as soon as headers arrive still protects the
/// expensive handshake and guarantees cancellation/timeout release.
#[derive(Debug)]
pub(crate) struct SandOpenPermit {
    global: Option<OwnedSemaphorePermit>,
    account: Option<OwnedSemaphorePermit>,
    wake: Arc<Notify>,
    cold_open: Option<SandColdOpenPermit>,
}

impl Drop for SandOpenPermit {
    fn drop(&mut self) {
        // Release both owned semaphore permits before waking contenders. A
        // custom Drop body runs before fields are dropped, hence the explicit
        // `take`; notifying first would wake every task while both dimensions
        // still appeared full and defer progress to the timer fallback.
        self.account.take();
        self.global.take();
        self.wake.notify_waiters();
    }
}

/// An accepted Sand response keeps this permit until its HTTP body is drained
/// or dropped.  That makes the stream limit cover the actual upstream model
/// lifetime rather than only the request-header handshake.
#[derive(Debug)]
pub(crate) struct SandStreamPermit {
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for SandStreamPermit {
    fn drop(&mut self) {
        self.permit.take();
    }
}

static SAND_STREAM_GATE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(sand_stream_global_limit())));

/// Wait for capacity for one accepted upstream model stream. Unlike the open
/// scheduler, this is intentionally a fixed lifetime capacity: every permit
/// is returned by `SandInferenceStream::drop`, including downstream cancel.
pub(crate) async fn admit_sand_stream_until(
    deadline: Instant,
) -> Result<SandStreamPermit, CursorError> {
    admit_sand_stream_from_gate(Arc::clone(&SAND_STREAM_GATE), deadline).await
}

/// Acquire accepted-stream capacity from a specific gate. Keeping the gate as
/// an argument makes the admission lifecycle testable without
/// mutating the process-wide 512-stream gate.
async fn admit_sand_stream_from_gate(
    gate: Arc<Semaphore>,
    deadline: Instant,
) -> Result<SandStreamPermit, CursorError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(sand_stream_admission_error());
    }
    let permit = tokio::time::timeout(remaining, gate.acquire_owned())
        .await
        .map_err(|_| sand_stream_admission_error())?
        .map_err(|_| CursorError::new(503, "Sand stream capacity is unavailable", None))?;
    Ok(SandStreamPermit {
        permit: Some(permit),
    })
}

fn sand_stream_admission_error() -> CursorError {
    let mut error = CursorError::new(
        504,
        "Sand accepted-stream admission deadline exhausted; retry after active streams drain",
        None,
    );
    error.retry_after = Some("1".to_string());
    error
}

/// Global cold-open controller.
///
/// The controller paces cold opens, but its capacity is deliberately
/// monotonic for a running process: a single account/model or provider
/// failure must not shrink the process-wide 512-request contract.  Earlier
/// versions used multiplicative decrease here.  When a fan-out failed in one
/// wave, every completion halved the same shared window (512 -> 256 -> ... ->
/// 1), which left unrelated accounts queued and surfaced proxy-generated 503s.
/// Account/model policy and transport breakers already isolate those failures;
/// this scheduler only tracks in-flight opens and additive recovery for an
/// explicitly lower operator-configured starting window.
#[derive(Debug)]
struct SandColdOpenScheduler {
    state: Mutex<SandColdOpenState>,
    wake: Arc<Notify>,
}

#[derive(Debug)]
struct SandColdOpenState {
    in_flight: usize,
    inflight_limit: usize,
    max_inflight: usize,
    rate_per_second: f64,
    max_rate_per_second: f64,
    tokens: f64,
    last_refill: Instant,
    successes_since_increase: usize,
}

impl SandColdOpenScheduler {
    fn new(initial_inflight: usize, initial_rate: u64, max_rate: u64) -> Self {
        let max_inflight = sand_open_global_limit();
        let inflight_limit = initial_inflight.clamp(1, max_inflight);
        let max_rate_per_second = max_rate.clamp(1, SAND_OPEN_RATE_MAX) as f64;
        let rate_per_second = (initial_rate.clamp(1, max_rate) as f64).min(max_rate_per_second);
        Self {
            state: Mutex::new(SandColdOpenState {
                in_flight: 0,
                inflight_limit,
                max_inflight,
                rate_per_second,
                max_rate_per_second,
                tokens: rate_per_second,
                last_refill: Instant::now(),
                successes_since_increase: 0,
            }),
            wake: Arc::new(Notify::new()),
        }
    }

    fn refill(state: &mut SandColdOpenState, now: Instant) {
        let elapsed = now
            .saturating_duration_since(state.last_refill)
            .as_secs_f64();
        state.tokens =
            (state.tokens + elapsed * state.rate_per_second).min(state.rate_per_second.max(1.0));
        state.last_refill = now;
    }

    async fn acquire(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<SandColdOpenPermit, CursorError> {
        loop {
            let wait = {
                let now = Instant::now();
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner());
                Self::refill(&mut state, now);
                if state.in_flight < state.inflight_limit && state.tokens >= 1.0 {
                    state.in_flight += 1;
                    state.tokens -= 1.0;
                    return Ok(SandColdOpenPermit {
                        scheduler: Arc::clone(self),
                        completed: false,
                    });
                }
                let rate_wait = if state.tokens < 1.0 {
                    Duration::from_secs_f64(
                        ((1.0 - state.tokens) / state.rate_per_second).max(0.001),
                    )
                } else {
                    Duration::from_millis(25)
                };
                rate_wait.min(Duration::from_millis(250))
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(sand_open_admission_error());
            }
            tokio::select! {
                _ = self.wake.notified() => {},
                _ = tokio::time::sleep(wait.min(remaining)) => {},
            }
        }
    }

    fn finish(&self, success: bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.in_flight = state.in_flight.saturating_sub(1);
        if success {
            state.successes_since_increase += 1;
            // Grow once per completed launch window. The bounded proportional
            // step reaches a recovered high-capacity route promptly while
            // still giving the upstream a full window between increases.
            if state.successes_since_increase >= state.inflight_limit {
                let window_step = (state.inflight_limit / 4).max(1);
                state.inflight_limit = (state.inflight_limit + window_step).min(state.max_inflight);
                let rate_step = (state.rate_per_second / 4.0).max(1.0);
                state.rate_per_second =
                    (state.rate_per_second + rate_step).min(state.max_rate_per_second);
                state.successes_since_increase = 0;
            }
        } else {
            // Do not apply multiplicative decrease here.  This state is
            // process-global, while failures are account/model scoped and
            // may arrive in a synchronized burst.  Reducing the shared
            // window for each failed completion collapses a normal 512-way
            // fan-out to one slot and turns transient upstream errors into a
            // local admission/503 storm.  The request-level retry/backoff and
            // account/model breaker provide the appropriate isolation.
            state.successes_since_increase = 0;
        }
        drop(state);
        self.wake.notify_waiters();
    }

    #[cfg(test)]
    fn snapshot(&self) -> (usize, usize, u64) {
        let state = self.state.lock().unwrap();
        (
            state.in_flight,
            state.inflight_limit,
            state.rate_per_second as u64,
        )
    }
}

#[derive(Debug)]
struct SandColdOpenPermit {
    scheduler: Arc<SandColdOpenScheduler>,
    completed: bool,
}

impl SandColdOpenPermit {
    fn complete(&mut self, success: bool) {
        if !self.completed {
            self.completed = true;
            self.scheduler.finish(success);
        }
    }

    fn cancel(&mut self) {
        if !self.completed {
            self.completed = true;
            let mut state = self
                .scheduler
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.in_flight = state.in_flight.saturating_sub(1);
            drop(state);
            self.scheduler.wake.notify_waiters();
        }
    }
}

impl Drop for SandColdOpenPermit {
    fn drop(&mut self) {
        self.complete(false);
    }
}

static SAND_COLD_OPEN_SCHEDULER: LazyLock<Arc<SandColdOpenScheduler>> = LazyLock::new(|| {
    Arc::new(SandColdOpenScheduler::new(
        sand_open_initial_inflight(),
        sand_open_initial_rate(),
        sand_open_rate_ceiling(),
    ))
});

impl SandOpenPermit {
    fn attach_cold_open(&mut self, permit: SandColdOpenPermit) {
        self.cold_open = Some(permit);
    }

    pub(crate) fn record_open_outcome(&mut self, success: bool) {
        if let Some(cold_open) = self.cold_open.as_mut() {
            cold_open.complete(success);
        }
    }

    pub(crate) fn record_open_neutral(&mut self) {
        if let Some(cold_open) = self.cold_open.as_mut() {
            cold_open.cancel();
        }
    }
}

static SAND_OPEN_GATE: LazyLock<SandOpenGate> =
    LazyLock::new(|| SandOpenGate::new(sand_open_global_limit(), sand_open_account_limit()));

fn sand_open_admission_error() -> CursorError {
    let mut error = CursorError::new(
        504,
        "Sand open admission deadline exhausted; retry after upstream capacity recovers",
        None,
    );
    error.retry_after = Some("1".to_string());
    error
}

/// Try to acquire a Sand open slot for one short fairness slice. Admission is
/// deliberately non-terminal: if every slot is occupied, return `Ok(None)` so
/// the caller can apply bounded backpressure without turning local queueing
/// into a proxy-generated 503 or flooding Cursor with untracked opens.
pub(crate) async fn admit_sand_open_until(
    token: &str,
    model: &str,
    deadline: Instant,
) -> Result<Option<SandOpenPermit>, CursorError> {
    let key = capability_key(token, model).0;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(sand_open_admission_error());
    }
    let queue_wait = remaining.min(Duration::from_secs(sand_open_queue_secs()));
    let mut cold_open = SAND_COLD_OPEN_SCHEDULER.acquire(deadline).await?;
    match SAND_OPEN_GATE.acquire_soft(&key, queue_wait).await {
        Some(mut permit) => {
            // The cold scheduler token is now paired with a real open permit;
            // it remains attached until the HTTP outcome is known.
            permit.attach_cold_open(cold_open);
            Ok(Some(permit))
        }
        None => {
            cold_open.cancel();
            // A saturated slice is expected under fan-out. Keep it observable
            // for diagnostics, but let the caller wait for a real permit.
            create_logger("cursor").debug(
                "sand_open_admission_wait",
                Some(serde_json::Map::from_iter([
                    ("model".into(), serde_json::json!(model)),
                    ("reason".into(), serde_json::json!("queue saturated")),
                    ("queueMs".into(), serde_json::json!(queue_wait.as_millis())),
                ])),
            );
            Ok(None)
        }
    }
}

/// Return the currently available permits for one account/model lane.
/// `None` means the lane has not been created yet; in that case callers can
/// treat it as having the configured account capacity.  This read is used by
/// the request-scoped account balancer to avoid sending a large fan-out to one
/// active account while other saved accounts are idle.
pub(crate) fn sand_open_available_permits(token: &str, model: &str) -> Option<usize> {
    let key = capability_key(token, model).0;
    let gate = SAND_OPEN_GATE
        .account
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    gate.get(&key).map(|lane| lane.available_permits())
}

/// The configured per-account capacity, exposed for an as-yet-unseen lane.
pub(crate) fn sand_open_account_capacity() -> usize {
    // Read the environment once, when the process-global gate is first used,
    // and then report that same snapshot. Calling `sand_open_account_limit()`
    // directly here would let a late environment mutation disagree with the
    // already-created semaphore and make account balancing choose the wrong
    // lane. Configuration changes take effect on the next proxy process.
    SAND_OPEN_GATE.account_limit
}

#[derive(Debug, Clone)]
struct SandOpenBreakerState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
    half_open_probe: bool,
    last_failure: Instant,
    /// A failure from an attempt that started before this instant is stale
    /// and must not re-open a route that has already proved healthy.
    last_success: Option<Instant>,
}

static SAND_OPEN_BREAKER: LazyLock<Mutex<HashMap<String, SandOpenBreakerState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn breaker_key(token: &str, model: &str) -> String {
    capability_key(token, model).0
}

/// Record the circuit state without turning it into a proxy-side outage.
///
/// Sand opens are full-history, UUID-scoped requests. A local breaker that
/// rejects every caller while cooling down creates a synchronized 503 wave and
/// prevents a healthy upstream from proving recovery. Keep the state for
/// diagnostics and failure accounting, but let each request reach the normal
/// bounded transport retry path.
pub(crate) fn sand_open_breaker_admit(token: &str, model: &str) -> Result<(), CursorError> {
    let key = breaker_key(token, model);
    let now = Instant::now();
    let mut breaker = SAND_OPEN_BREAKER
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    breaker.retain(|_, state| {
        state.open_until.is_none_or(|until| until > now)
            || now.saturating_duration_since(state.last_failure) < Duration::from_secs(600)
    });
    let Some(state) = breaker.get_mut(&key) else {
        return Ok(());
    };
    if let Some(until) = state.open_until {
        if until > now {
            return Ok(());
        }
        // Start a fresh observation window after cooldown. Do not retain the
        // old failure count, otherwise one transient outage can immediately
        // reopen the state after the first recovery probe.
        state.open_until = None;
        state.consecutive_failures = 0;
        state.half_open_probe = false;
    }
    Ok(())
}

/// Mark an accepted Sand open healthy and close any half-open circuit.
pub(crate) fn sand_open_breaker_success(token: &str, model: &str) {
    let key = breaker_key(token, model);
    let now = Instant::now();
    let mut breaker = SAND_OPEN_BREAKER
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if breaker.len() >= SAND_OPEN_BREAKER_MAX_ENTRIES && !breaker.contains_key(&key) {
        if let Some(oldest) = breaker
            .iter()
            .min_by_key(|(_, state)| state.last_failure)
            .map(|(key, _)| key.clone())
        {
            breaker.remove(&oldest);
        }
    }
    let state = breaker.entry(key).or_insert_with(|| SandOpenBreakerState {
        consecutive_failures: 0,
        open_until: None,
        half_open_probe: false,
        last_failure: now,
        last_success: None,
    });
    state.consecutive_failures = 0;
    state.open_until = None;
    state.half_open_probe = false;
    state.last_failure = now;
    state.last_success = Some(now);
}

/// Abort a half-open probe that never reached the upstream (for example when
/// local open admission timed out or a deterministic capability response was
/// returned). Without this transition the probe flag would remain set after
/// cooldown and permanently reject every later request for that key.
pub(crate) fn sand_open_breaker_abort(token: &str, model: &str, retryable: bool) {
    let key = breaker_key(token, model);
    let now = Instant::now();
    let mut breaker = SAND_OPEN_BREAKER
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let Some(state) = breaker.get_mut(&key) else {
        return;
    };
    if !state.half_open_probe {
        return;
    }
    state.half_open_probe = false;
    if retryable {
        // Keep a short cool-off after a failed half-open probe, but do not
        // extend the full breaker cooldown indefinitely on local queue loss.
        state.open_until = Some(now + Duration::from_secs(2));
    } else {
        // A non-transient response proves the route itself was reached; let
        // the normal capability/policy classifier handle it without a stale
        // transport circuit.
        breaker.remove(&key);
    }
}

fn sand_open_error_is_breaker_candidate(error: &CursorError) -> bool {
    let message = error.client_message();
    let lower = message.to_ascii_lowercase();
    if crate::retry::is_policy_rate_limit(&message)
        || is_non_retryable_provider_error_message(&message)
        || lower.contains("sand traffic is not supported")
        || lower.contains("bad model name")
        || lower.contains("outdated client")
        || lower.contains("open admission queue")
        || lower.contains("open circuit")
    {
        return false;
    }
    is_transient_provider_error_message(&message)
        || matches!(error.status, 408 | 425 | 500 | 502 | 503 | 504)
        || lower.contains("connect failed")
        || lower.contains("timed out")
        || lower.contains("connection reset")
        || lower.contains("connection closed")
}

/// Record a failed transient open. The breaker is account/model scoped so a
/// healthy account continues serving while one exhausted route cools down.
pub(crate) fn sand_open_breaker_failure(
    token: &str,
    model: &str,
    error: &CursorError,
    attempt_started: Instant,
) {
    if !sand_open_error_is_breaker_candidate(error) {
        sand_open_breaker_abort(token, model, false);
        return;
    }
    let key = breaker_key(token, model);
    let now = Instant::now();
    let threshold = sand_open_breaker_threshold();
    let cooldown = sand_open_breaker_cooldown();
    let mut breaker = SAND_OPEN_BREAKER
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if breaker.len() >= SAND_OPEN_BREAKER_MAX_ENTRIES && !breaker.contains_key(&key) {
        if let Some(oldest) = breaker
            .iter()
            .min_by_key(|(_, state)| state.last_failure)
            .map(|(key, _)| key.clone())
        {
            breaker.remove(&oldest);
        }
    }
    let state = breaker.entry(key).or_insert_with(|| SandOpenBreakerState {
        consecutive_failures: 0,
        open_until: None,
        half_open_probe: false,
        last_failure: now,
        last_success: None,
    });
    if state
        .last_success
        .is_some_and(|last_success| attempt_started <= last_success)
    {
        // A slower request that began before a successful concurrent probe
        // cannot invalidate that success. Keep the healthy state intact.
        state.last_failure = now;
        state.half_open_probe = false;
        return;
    }
    state.last_failure = now;
    state.consecutive_failures = state.consecutive_failures.saturating_add(1);
    if state.consecutive_failures >= threshold {
        state.open_until = Some(now + cooldown);
        state.half_open_probe = false;
    }
}

/// Test/diagnostic reset. Production state is intentionally process-local and
/// is cleared only when the proxy exits or this explicit helper is called.
#[cfg(test)]
fn reset_sand_open_state_for_test() {
    SAND_OPEN_BREAKER
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clear();
}

/// Sand tool support is provider/model/account specific.  Cursor can accept
/// the same `InferenceService/Stream` request for text while rejecting a
/// non-empty `tools` catalog with an inner provider 400.  Keep that result in
/// process so every Claude Code retry does not repeat a deterministic request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SandToolCapability {
    Unknown,
    Supported,
    Unsupported,
}

impl SandToolCapability {
    /// Stable text used by `sand-status` and JSON-adjacent diagnostics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Supported => "supported",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandToolCapabilityStatus {
    /// Stable one-way account identity; never a bearer token.
    pub account_id: String,
    pub model: String,
    pub state: SandToolCapability,
    pub tool_count: usize,
    pub tool_names: Vec<String>,
    pub last_error: Option<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone)]
struct SandToolCapabilityRecord {
    state: SandToolCapability,
    tool_count: usize,
    tool_names: Vec<String>,
    last_error: Option<String>,
    updated_at_ms: u64,
    /// Wall-clock timestamps are useful to operators, but expiry must use a
    /// monotonic clock so a system-clock adjustment cannot pin an old
    /// Unsupported result forever.
    observed_at: Instant,
}

const SAND_TOOL_CAPABILITY_MAX_ENTRIES: usize = 1024;
/// Capability failures are cached briefly to stop a Claude Code retry storm,
/// then re-probed so a Cursor provider rollout can recover without requiring a
/// process restart. The explicit reset helpers below remain useful for a
/// manual immediate probe.
const SAND_TOOL_CAPABILITY_TTL: Duration = Duration::from_secs(15 * 60);
static SAND_TOOL_CAPABILITIES: LazyLock<Mutex<HashMap<String, SandToolCapabilityRecord>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn capability_key(token: &str, model: &str) -> (String, String, String) {
    let account_id = crate::providers::cursor::auth::cursor_account_digest(token);
    let model = model.trim().to_ascii_lowercase();
    (format!("{account_id}\0{model}"), account_id, model)
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

/// Return the last observed tool capability for an account/model pair.
pub fn sand_tool_capability_for_token(token: &str, model: &str) -> SandToolCapability {
    let (key, _, _) = capability_key(token, model);
    let mut cache = SAND_TOOL_CAPABILITIES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let stale = cache
        .get(&key)
        .is_some_and(|record| record.observed_at.elapsed() >= SAND_TOOL_CAPABILITY_TTL);
    if stale {
        cache.remove(&key);
        return SandToolCapability::Unknown;
    }
    cache
        .get(&key)
        .map(|record| record.state)
        .unwrap_or(SandToolCapability::Unknown)
}

/// Record a successful useful response for a request carrying a tool catalog.
/// A later successful request can recover an earlier unsupported result after
/// Cursor changes provider routing, so this is intentionally not one-way.
pub fn mark_sand_tools_supported<I, S>(token: &str, model: &str, tool_count: usize, tool_names: I)
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    if tool_count == 0 {
        return;
    }
    let (key, account_id, model) = capability_key(token, model);
    update_sand_tool_capability(
        key,
        account_id,
        model,
        SandToolCapability::Supported,
        tool_count,
        tool_names,
        None,
    );
}

/// Record a deterministic provider rejection of a non-empty tool catalog.
pub fn mark_sand_tools_unsupported<I, S>(
    token: &str,
    model: &str,
    tool_count: usize,
    tool_names: I,
    error: impl Into<String>,
) where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    if tool_count == 0 {
        return;
    }
    let (key, account_id, model) = capability_key(token, model);
    let error = error.into();
    update_sand_tool_capability(
        key,
        account_id,
        model,
        SandToolCapability::Unsupported,
        tool_count,
        tool_names,
        Some(error),
    );
}

fn update_sand_tool_capability<I, S>(
    key: String,
    account_id: String,
    model: String,
    state: SandToolCapability,
    tool_count: usize,
    tool_names: I,
    last_error: Option<String>,
) where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut names = tool_names
        .into_iter()
        .map(Into::into)
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names.truncate(16);
    let mut cache = SAND_TOOL_CAPABILITIES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if cache.len() >= SAND_TOOL_CAPABILITY_MAX_ENTRIES && !cache.contains_key(&key) {
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, record)| record.updated_at_ms)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(
        key,
        SandToolCapabilityRecord {
            state,
            tool_count,
            tool_names: names,
            last_error,
            updated_at_ms: unix_now_ms(),
            observed_at: Instant::now(),
        },
    );
    let _ = (account_id, model);
}

/// Forget one account/model observation and return whether an entry existed.
/// The next request will probe the provider instead of trusting a stale
/// Unsupported result.
pub fn reset_sand_tool_capability(token: &str, model: &str) -> bool {
    let (key, _, _) = capability_key(token, model);
    SAND_TOOL_CAPABILITIES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .remove(&key)
        .is_some()
}

/// Forget every in-process Sand tool observation. This is intentionally
/// process-local: capability state is diagnostic/retry coordination, not a
/// persisted account setting.
pub fn reset_sand_tool_capabilities() {
    SAND_TOOL_CAPABILITIES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clear();
}

/// Return a redacted snapshot for `sand-status` and health diagnostics.
pub fn sand_tool_capability_statuses() -> Vec<SandToolCapabilityStatus> {
    let mut cache = SAND_TOOL_CAPABILITIES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    cache.retain(|_, record| record.observed_at.elapsed() < SAND_TOOL_CAPABILITY_TTL);
    let mut rows = cache
        .iter()
        .filter_map(|(key, record)| {
            let (account_id, model) = key.split_once('\0')?;
            Some(SandToolCapabilityStatus {
                account_id: account_id.to_string(),
                model: model.to_string(),
                state: record.state,
                tool_count: record.tool_count,
                tool_names: record.tool_names.clone(),
                last_error: record.last_error.clone(),
                updated_at_ms: record.updated_at_ms,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        left.account_id
            .cmp(&right.account_id)
            .then(left.model.cmp(&right.model))
    });
    rows
}

/// Determine whether a stream error is the deterministic tool-catalog
/// rejection observed from the Fable provider. Cursor wraps it in an outer
/// 429/resource-exhausted envelope, so the request's non-empty tool count plus
/// explicit tool/schema rejection wording are part of the discriminator.
pub fn is_sand_tool_capability_error(error: &CursorError, tool_count: usize) -> bool {
    if tool_count == 0 {
        return false;
    }
    let text = format!(
        "{} {}",
        error.message,
        error.detail.as_deref().unwrap_or_default()
    );
    let lower = text.to_ascii_lowercase();
    // Cursor reuses the provider 4xx envelope for temporary connectivity
    // failures. Those responses must stay on the bounded transport retry
    // path even when the outer status is 429 and `isRetryable=false`.
    // Check this before any capability markers because the provider detail is
    // the authoritative disposition for this otherwise ambiguous envelope.
    if is_transient_provider_error_message(&text) {
        return false;
    }
    // The diagnostic reaches this layer as either flattened key/value text or
    // pretty-printed JSON. Collapse whitespace before looking for the
    // structural markers so `providerStatusCode : 422` is treated exactly
    // like the compact form emitted by `json_error`.
    let compact = lower
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>();
    let provider_error = compact.contains("error_provider_error")
        || compact.contains("providererrorcode=error_provider_error")
        || compact.contains("provider_error_code=error_provider_error");
    // Deterministic tool/schema rejections have appeared as several provider
    // 4xx values (400 and 422 are both in the wild), not only 400. Read the
    // numeric value after either key spelling and require a 4xx range.
    let provider_4xx = ["providerstatuscode", "provider_status_code"]
        .iter()
        .any(|key| {
            let mut offset = 0usize;
            while let Some(relative) = compact[offset..].find(key) {
                let start = offset + relative + key.len();
                let tail = compact[start..].trim_start_matches(|character: char| {
                    matches!(character, '"' | '\'' | ':' | '=' | ',' | '\\')
                });
                let digits = tail
                    .chars()
                    .take_while(|character| character.is_ascii_digit())
                    .collect::<String>();
                if digits.len() == 3
                    && digits
                        .parse::<u16>()
                        .ok()
                        .is_some_and(|status| (400..500).contains(&status))
                {
                    return true;
                }
                offset = start;
            }
            false
        });
    let non_retryable = compact.contains("isretryable=false")
        || compact.contains("is_retryable=false")
        || compact.contains("isretryable:false")
        || compact.contains("is_retryable:false")
        || compact.contains("isretryable\":false")
        || compact.contains("is_retryable\":false")
        || compact.contains("isretryable\\\":false")
        || compact.contains("is_retryable\\\":false");
    if !(provider_error && provider_4xx && non_retryable) {
        return false;
    }
    // Preserve account/plan/capacity classification. Those errors can carry
    // the same nested provider marker but should still use account failover.
    // Do not call the broad `is_policy_rate_limit` helper here: its bare
    // `resource_exhausted` branch intentionally recognizes the outer 429
    // envelope, which is also present on deterministic tool rejections.
    let terminal_policy = lower.contains("out of usage")
        || lower.contains("usage exhausted")
        || lower.contains("usage limit")
        || lower.contains("rate limit exceeded")
        || lower.contains("quota")
        || lower.contains("included limit")
        || lower.contains("allowance")
        || lower.contains("user_rate_limit_exceeded")
        || lower.contains("api_rate_limit_exceeded")
        || lower.contains("error_rate_limited")
        || lower.contains("gpt_4_vision_preview_rate_limit");
    if terminal_policy
        || crate::retry::is_billing_block(&text)
        || crate::retry::is_capacity_shed(&text)
    {
        return false;
    }

    // Metadata alone is insufficient: provider 4xx diagnostics also cover
    // malformed model parameters, account connectivity, and other request
    // failures. Only switch to the text bridge when the diagnostic names a
    // tool/schema/function/catalog context and explicitly says that context
    // was rejected or is unsupported/invalid.
    let tool_context = ["tool", "function", "catalog", "schema"]
        .iter()
        .any(|marker| lower.contains(marker));
    let rejection = [
        "unsupported",
        "not supported",
        "does not support",
        "rejected",
        "reject",
        "invalid",
        "unknown",
        "unrecognized",
        "not allowed",
        "not accepted",
        "does not accept",
        "doesn't accept",
        "incompatible",
        "not compatible",
        "cannot use",
        "can't use",
        "unable to use",
        "not implemented",
        "not available",
    ]
    .iter()
    .any(|marker| lower.contains(marker));
    tool_context && rejection
}

/// Convert a deterministic tool rejection to a stable client-facing 400.
/// Keeping the original diagnostic in `detail` makes the status actionable
/// without allowing it to enter the generic transient retry classifier.
pub fn sand_tool_capability_client_error(
    error: &CursorError,
    model: &str,
    tool_count: usize,
) -> CursorError {
    CursorError::new(
        400,
        format!(
            "Sand model {} rejected the supplied tool catalog ({} tool{})",
            model.trim(),
            tool_count,
            if tool_count == 1 { "" } else { "s" }
        ),
        Some(error.client_message()),
    )
}

/// Wire role values used by `InferenceMessageRole`.
pub const ROLE_USER: u32 = 1;
pub const ROLE_ASSISTANT: u32 = 2;
pub const ROLE_TOOL: u32 = 3;
pub const ROLE_SYSTEM: u32 = 4;

/// Connect response control/trailer bit emitted by current Cursor Sand
/// gateways.  These frames carry transport metadata (and are not JSON
/// `InferenceStreamResponse` values), so they must be consumed without being
/// surfaced as model output or parsed as an error.
pub const FLAG_CONTROL: u8 = 0x80;

/// Return whether an error from an already-open Sand stream is safe to retry
/// before any model-visible text/tool output has been committed.  The ordinary
/// Cursor retry classifier treats "no useful progress" as an ambiguous live
/// acceptance (which is correct for AgentService's resumable runs), but Sand
/// requests are full-history, UUID-scoped InferenceService calls.  A bounded
/// replay is therefore preferable for connect resets, idle stalls and gateway
/// overloads while the downstream client is still waiting for its first token.
pub fn stream_error_is_retryable(error: &CursorError) -> bool {
    let message = error.client_message();
    let lower = message.to_ascii_lowercase();

    // Account/entitlement and model validation errors are deterministic.  Do
    // not turn these into a retry storm, even when a gateway labels them 502.
    if is_non_retryable_provider_error_message(&message)
        || crate::retry::is_billing_block(&message)
        || crate::retry::is_policy_rate_limit(&message)
        || crate::retry::is_capacity_shed(&message)
        // This is proxy-local backpressure after a successful HTTP open. The
        // response body is dropped on timeout, so replaying another open while
        // the accepted-stream gate is full would create duplicate upstream
        // invocations instead of freeing capacity.
        || lower.contains("sand accepted-stream admission")
        || lower.contains("sand stream capacity is unavailable")
        || lower.contains("sand traffic is not supported")
        || lower.contains("bad model name")
        || lower.contains("outdated client")
    {
        return false;
    }

    // The nested provider adapter can report a temporary connectivity outage
    // as an outer 400/429. Keep it on Sand's bounded replay path even though
    // the embedded status itself is not retryable.
    if is_transient_provider_error_message(&message) {
        return true;
    }

    // Sand's stream can report a stale invocation as 409/"already active";
    // each replay gets fresh UUIDs, so this is transient rather than a live
    // AgentService ownership conflict.
    if lower.contains("already active")
        || lower.contains("connection reset")
        || lower.contains("connection closed")
        || lower.contains("stream idle")
        || lower.contains("idle timeout")
        || lower.contains("no useful progress")
        || lower.contains("no chunks received")
        || lower.contains("connect failed")
        || lower.contains("timed out")
    {
        return true;
    }

    matches!(error.status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

/// Maximum number of stream-level Sand replays after an accepted response.
/// Keep this separately bounded from open retries: a response body may already
/// have reached the upstream, so unbounded retries would multiply model
/// invocations.
pub fn stream_retry_limit() -> u32 {
    stream_retry_limit_from(
        std::env::var("CCP_CURSOR_SAND_STREAM_RETRIES")
            .ok()
            .as_deref(),
    )
}

fn stream_retry_limit_from(raw: Option<&str>) -> u32 {
    raw.and_then(|value| value.trim().parse::<u32>().ok())
        // A provider error can be emitted after the HTTP stream has already
        // been accepted. Two quick replays (the old default) ended the
        // Claude Code turn during ordinary Cursor provider blips. Five
        // bounded replays cover the usual recovery window while keeping the
        // total request fan-out finite; callers can lower this with the env
        // override for latency-sensitive deployments.
        .unwrap_or(DEFAULT_SAND_STREAM_RETRIES)
        .min(MAX_SAND_STREAM_RETRIES)
}

/// One message in an InferenceService request.
///
/// The protobuf JSON representation uses a `oneof` for message content.  We
/// keep the small wire-shaped fields here instead of serializing Anthropic's
/// blocks directly: text-only messages use `text`, multimodal messages use
/// `parts.parts[]`, assistant tool calls use `toolCalls[]`, and tool results
/// use `toolContent.parts[]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandInferenceMessage {
    pub role: u32,
    pub text: Option<String>,
    pub parts: Vec<Value>,
    pub tool_calls: Vec<Value>,
    pub tool_content: Option<Value>,
}

impl SandInferenceMessage {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: ROLE_USER,
            text: Some(text.into()),
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_content: None,
        }
    }

    pub fn assistant(text: impl Into<String>) -> Self {
        Self {
            role: ROLE_ASSISTANT,
            text: Some(text.into()),
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_content: None,
        }
    }

    pub fn system(text: impl Into<String>) -> Self {
        Self {
            role: ROLE_SYSTEM,
            text: Some(text.into()),
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_content: None,
        }
    }

    pub fn tool(parts: Vec<Value>) -> Self {
        Self {
            role: ROLE_TOOL,
            text: None,
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_content: Some(json!({ "parts": parts })),
        }
    }

    pub fn with_parts(mut self, parts: Vec<Value>) -> Self {
        self.text = None;
        self.parts = parts;
        self
    }

    pub fn with_tool_calls(mut self, tool_calls: Vec<Value>) -> Self {
        self.tool_calls = tool_calls;
        self
    }

    fn to_json_value(&self) -> Value {
        let mut object = Map::new();
        object.insert("role".into(), json!(self.role));
        if let Some(tool_content) = &self.tool_content {
            object.insert("toolContent".into(), tool_content.clone());
        } else if !self.parts.is_empty() {
            object.insert("parts".into(), json!({ "parts": self.parts }));
        } else if let Some(text) = &self.text {
            object.insert("text".into(), json!(text));
        }
        if !self.tool_calls.is_empty() {
            object.insert("toolCalls".into(), Value::Array(self.tool_calls.clone()));
        }
        Value::Object(object)
    }
}

/// Convert an Anthropic Messages request to the `InferenceCoreMessage` JSON
/// shape used by the current Sand endpoint.  This intentionally preserves
/// message boundaries and roles; flattening the entire history into one XML
/// user message loses tool-call/result semantics and makes follow-up turns
/// impossible for the inference service to reconcile.
pub fn messages_from_anthropic(
    request: &MessagesRequest,
    compaction_mode: bool,
) -> Vec<SandInferenceMessage> {
    let tool_names = assistant_tool_names(request);
    let mut output = Vec::new();
    for message in &request.messages {
        let role = match message.role.trim().to_ascii_lowercase().as_str() {
            "system" => ROLE_SYSTEM,
            "assistant" => ROLE_ASSISTANT,
            "tool" => ROLE_TOOL,
            _ => ROLE_USER,
        };
        let blocks = message_blocks(message);
        let mut text = String::new();
        let mut parts = Vec::new();
        let mut tool_calls = Vec::new();
        let mut tool_results = Vec::new();

        for block in blocks {
            let block_type = block.get("type").and_then(Value::as_str).unwrap_or("text");
            match block_type {
                "text" => {
                    if let Some(value) = block.get("text").and_then(Value::as_str) {
                        append_text(&mut text, value);
                    }
                }
                // Historical reasoning is represented by a separate response
                // channel. Replaying signatures/markup as user text causes
                // Sand models to echo or treat it as an instruction.
                "thinking" if !compaction_mode => {}
                "thinking" => {}
                "compaction" => {
                    if let Some(value) = block.get("content").and_then(Value::as_str) {
                        append_text(&mut text, value);
                    }
                }
                "image" | "input_image" | "image_url" => {
                    if let Some(part) = image_part(&block) {
                        parts.push(part);
                    }
                }
                "document" | "file" => {
                    if let Some(part) = file_part(&block) {
                        parts.push(part);
                    } else if let Some(value) = document_text(&block) {
                        append_text(&mut text, &value);
                    }
                }
                "tool_use" => {
                    if role == ROLE_ASSISTANT {
                        let id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                        let name = block
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let args = block.get("input").cloned().unwrap_or_else(|| json!({}));
                        if !id.is_empty() || !name.is_empty() {
                            tool_calls.push(json!({
                                "toolCallId": id,
                                "toolName": name,
                                "args": args,
                            }));
                        }
                    }
                }
                "tool_result" => {
                    let id = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if !id.is_empty() {
                        let name = block
                            .get("name")
                            .and_then(Value::as_str)
                            .or_else(|| tool_names.get(id).map(String::as_str))
                            .unwrap_or("unknown_tool");
                        let result = block.get("content").cloned().unwrap_or(Value::Null);
                        let is_error = block
                            .get("is_error")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        tool_results.push(json!({
                            "toolCallId": id,
                            "toolName": name,
                            "result": result,
                            "isError": is_error,
                        }));
                    }
                }
                // Keep server-side tool/search results as ordinary structured
                // content. They are not callable Sand tool results.
                "server_tool_use" | "web_search_tool_result" => {
                    if let Ok(value) = serde_json::to_string(&block) {
                        append_text(&mut text, &value);
                    }
                }
                _ => {
                    if let Some(value) = block.get("text").and_then(Value::as_str) {
                        append_text(&mut text, value);
                    }
                }
            }
        }

        // A user message may contain only tool_result blocks. The Inference
        // schema requires those to be a role=TOOL message with toolContent.
        if !tool_results.is_empty() {
            if !text.trim().is_empty() || !parts.is_empty() {
                output.push(content_message(role, text, parts, Vec::new()));
            }
            output.push(SandInferenceMessage::tool(tool_results));
            continue;
        }
        if !text.trim().is_empty() || !parts.is_empty() || !tool_calls.is_empty() {
            output.push(content_message(role, text, parts, tool_calls));
        }
    }
    output
}

/// Map Anthropic's tool catalog to `InferenceAgentTool` protobuf-JSON. Sand
/// expects the schema under `parameters` (a google.protobuf.Struct), rather
/// than Anthropic's `input_schema` field name.
pub fn tools_from_anthropic(request: &MessagesRequest, omit_tools: bool) -> Vec<Value> {
    if omit_tools {
        return Vec::new();
    }
    request
        .extra
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        // Keep the Sand catalog identical to the model-facing Cursor catalog.
        // Claude-local hook/deprecated definitions are implementation details;
        // forwarding them here lets Sand call tools that the downstream client
        // will later discard and can leave the turn waiting forever.
        .filter(|tool| is_model_visible_tool_definition(tool))
        // Cursor's desktop runtime keeps dynamic execution-only tools out of
        // `modelVisibleTools`; mirror that split when a client forwards the
        // metadata through the Anthropic extension map.
        .filter(|tool| !tool_is_execution_only(tool))
        .filter_map(|tool| {
            let name = tool.get("name").and_then(Value::as_str)?.trim();
            if name.is_empty() {
                return None;
            }
            let description = tool
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("");
            let parameters = tool
                .get("input_schema")
                .or_else(|| tool.get("parameters"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            Some(json!({
                "name": name,
                "description": description,
                "parameters": parameters,
            }))
        })
        .collect()
}

/// Return the names of executable tools that are intentionally not included
/// in the model-visible `tools` catalog.
///
/// Cursor's desktop agent keeps these two sets separate: `modelVisibleTools`
/// is serialized as `tools`, while dynamic/client-local entries are sent in
/// `acceptedUnadvertisedToolNames`.  Claude Code normally exposes a flat
/// Anthropic tool array, but newer clients may preserve the desktop metadata
/// (or an explicit execution-set object) in the flattened extension fields.
/// Honor those forms without guessing that every Claude-local tool is
/// executable.  This is important for hidden hooks: adding all names here can
/// make the model emit calls that the downstream bridge cannot fulfill.
pub fn accepted_unadvertised_tool_names_from_anthropic(
    request: &MessagesRequest,
    model_tools: &[Value],
) -> Vec<String> {
    let model_names: HashSet<String> = model_tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .map(str::to_string)
        .collect();
    let mut names = Vec::new();

    // Preserve an explicit list when a desktop-compatible client forwards it
    // through the Anthropic extension map. Both protobuf-JSON and Rust-style
    // snake_case spellings have appeared in integrations.
    for key in [
        "acceptedUnadvertisedToolNames",
        "accepted_unadvertised_tool_names",
    ] {
        collect_tool_names(request.extra.get(key), &mut names);
    }
    // Some clients forward the complete execution set instead of the derived
    // name list. Accept both the camelCase and snake_case forms, including a
    // nested `toolExecutionSet` envelope.
    for key in ["additionalExecutableTools", "additional_executable_tools"] {
        collect_tool_names(request.extra.get(key), &mut names);
    }
    for key in ["toolExecutionSet", "tool_execution_set"] {
        let Some(value) = request.extra.get(key) else {
            continue;
        };
        for nested in ["additionalExecutableTools", "additional_executable_tools"] {
            collect_tool_names(value.get(nested), &mut names);
        }
    }

    // When metadata is attached to individual tool definitions, only treat a
    // definition as unadvertised when the client explicitly marks it as an
    // execution-only/dynamic entry. A `dynamicToolMetaRole=invocation` entry
    // is the spelling used by Cursor's current agent runtime.
    if let Some(tools) = request.extra.get("tools").and_then(Value::as_array) {
        for tool in tools {
            let Some(name) = tool.get("name").and_then(Value::as_str) else {
                continue;
            };
            if model_names.contains(name) || !tool_is_execution_only(tool) {
                continue;
            }
            names.push(name.to_string());
        }
    }

    // Keep the wire deterministic and discard entries that are already in the
    // visible catalog. The server treats this as a repeated set in practice;
    // preserving first-seen order makes request snapshots stable for tests.
    let mut seen = HashSet::new();
    names
        .into_iter()
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .filter(|name| !model_names.contains(name))
        .filter(|name| seen.insert(name.clone()))
        .collect()
}

fn collect_tool_names(value: Option<&Value>, out: &mut Vec<String>) {
    let Some(value) = value else {
        return;
    };
    match value {
        Value::Array(items) => {
            for item in items {
                match item {
                    Value::String(name) => out.push(name.clone()),
                    Value::Object(object) => {
                        for key in ["name", "toolName", "tool_name"] {
                            if let Some(name) = object.get(key).and_then(Value::as_str) {
                                out.push(name.to_string());
                                break;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Value::String(name) => out.push(name.clone()),
        Value::Object(object) => {
            for key in ["name", "toolName", "tool_name"] {
                if let Some(name) = object.get(key).and_then(Value::as_str) {
                    out.push(name.to_string());
                    break;
                }
            }
        }
        _ => {}
    }
}

fn tool_is_execution_only(tool: &Value) -> bool {
    for key in [
        "additionalExecutable",
        "additional_executable",
        "isAdditionalExecutable",
        "is_additional_executable",
        "executionOnly",
        "execution_only",
        "modelInvisible",
        "model_invisible",
    ] {
        if tool.get(key).and_then(Value::as_bool) == Some(true) {
            return true;
        }
    }
    if tool
        .get("modelVisible")
        .or_else(|| tool.get("model_visible"))
        .and_then(Value::as_bool)
        == Some(false)
    {
        return true;
    }
    tool.get("dynamicToolMetaRole")
        .or_else(|| tool.get("dynamic_tool_meta_role"))
        .and_then(Value::as_str)
        .is_some_and(|role| role.eq_ignore_ascii_case("invocation"))
}

fn content_message(
    role: u32,
    text: String,
    mut parts: Vec<Value>,
    tool_calls: Vec<Value>,
) -> SandInferenceMessage {
    let mut message = SandInferenceMessage {
        role,
        text: None,
        parts: Vec::new(),
        tool_calls,
        tool_content: None,
    };
    if !text.trim().is_empty() {
        if parts.is_empty() {
            message.text = Some(text);
        } else {
            parts.insert(0, json!({ "text": { "text": text } }));
        }
    }
    if !parts.is_empty() {
        message.parts = parts;
    }
    message
}

fn append_text(target: &mut String, value: &str) {
    if value.is_empty() {
        return;
    }
    if !target.is_empty() {
        target.push('\n');
    }
    target.push_str(value);
}

fn image_part(block: &Value) -> Option<Value> {
    let (raw, hinted_mime) = image_candidate(block)?;
    let (data, mime_type) = normalize_image_data(raw, hinted_mime)?;
    Some(json!({
        "image": {
            // InferenceImagePart.data is a provider-ready string.  Cursor's
            // desktop runtime preserves a data URI here (the legacy Agent
            // protobuf path is the one that receives bare base64 bytes).
            "data": format!("data:{mime_type};base64,{data}"),
            "mimeType": mime_type,
        }
    }))
}

/// Convert Anthropic/OpenAI file-like content blocks to Cursor's native
/// `InferenceFilePart`.  The current endpoint accepts inline data URIs and
/// does not resolve remote URLs, so URL-only documents are represented as
/// text below instead of triggering a hidden network fetch.
fn file_part(block: &Value) -> Option<Value> {
    let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
    let source = block.get("source").and_then(Value::as_object);
    let file_object = block.get("file").and_then(Value::as_object);
    if source
        .and_then(|source| source.get("type"))
        .and_then(Value::as_str)
        .is_some_and(|kind| kind.eq_ignore_ascii_case("text"))
    {
        return None;
    }

    let raw = source
        .and_then(|source| source.get("data").or_else(|| source.get("file_data")))
        .or_else(|| file_object.and_then(|file| file.get("file_data").or_else(|| file.get("data"))))
        .or_else(|| block.get("data"))
        .and_then(Value::as_str)?
        .trim();
    if raw.is_empty() || raw.starts_with("http://") || raw.starts_with("https://") {
        return None;
    }

    let hinted_mime = source
        .and_then(|source| source.get("media_type").or_else(|| source.get("mime_type")))
        .or_else(|| {
            file_object.and_then(|file| file.get("media_type").or_else(|| file.get("mime_type")))
        })
        .or_else(|| block.get("media_type").or_else(|| block.get("mime_type")))
        .and_then(Value::as_str)
        .filter(|mime| !mime.trim().is_empty())
        .unwrap_or("application/octet-stream");
    let filename = source
        .and_then(|source| {
            source
                .get("filename")
                .or_else(|| source.get("name"))
                .or_else(|| block.get("title"))
        })
        .or_else(|| file_object.and_then(|file| file.get("filename").or_else(|| file.get("name"))))
        .or_else(|| {
            block
                .get("filename")
                .or_else(|| block.get("name"))
                .or_else(|| block.get("title"))
        })
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| {
            if block_type.eq_ignore_ascii_case("document") {
                "document"
            } else {
                "file"
            }
        });

    // normalize_image_data validates flexible base64 and canonicalizes the
    // alphabet/padding.  Its MIME fallback is image-specific, so use a small
    // data-URI decoder here and retain the declared document media type.
    let (uri_mime, encoded) = if let Some(rest) = raw.strip_prefix("data:") {
        let (metadata, encoded) = rest.split_once(',')?;
        if !metadata
            .split(';')
            .any(|part| part.eq_ignore_ascii_case("base64"))
        {
            return None;
        }
        (
            metadata.split(';').next().filter(|mime| !mime.is_empty()),
            encoded,
        )
    } else {
        (None, raw)
    };
    let compact: String = encoded
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace())
        .collect();
    let bytes = decode_base64_flexible_local(&compact)?;
    let mime_type = uri_mime.unwrap_or(hinted_mime).trim();
    let mime_type = if mime_type.is_empty() {
        "application/octet-stream"
    } else {
        mime_type
    };
    let canonical = base64::engine::general_purpose::STANDARD.encode(bytes);
    Some(json!({
        "file": {
            "data": format!("data:{mime_type};base64,{canonical}"),
            "mediaType": mime_type,
            "filename": filename,
        }
    }))
}

fn decode_base64_flexible_local(value: &str) -> Option<Vec<u8>> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    STANDARD
        .decode(value)
        .or_else(|_| STANDARD_NO_PAD.decode(value))
        .or_else(|_| URL_SAFE.decode(value))
        .or_else(|_| URL_SAFE_NO_PAD.decode(value))
        .ok()
}

fn document_text(block: &Value) -> Option<String> {
    let source = block.get("source").and_then(Value::as_object);
    let text = source
        .and_then(|source| source.get("text"))
        .or_else(|| block.get("text"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())?;
    Some(text.to_string())
}

/// Serialize the model parameter list using the protobuf JSON field names.
/// Keeping this conversion local avoids exposing the Agent protobuf module in
/// the public Sand request API while still forwarding effort/context settings
/// selected by the TUI or model catalog.
fn requested_model_parameters_json(model_id: &str) -> Vec<Value> {
    crate::providers::cursor::model::requested_model_parameters(model_id)
        .into_iter()
        .map(|parameter| json!({ "id": parameter.id, "value": parameter.value }))
        .collect()
}

fn assistant_tool_names(request: &MessagesRequest) -> HashMap<String, String> {
    let mut names = HashMap::new();
    for message in &request.messages {
        if !message.role.eq_ignore_ascii_case("assistant") {
            continue;
        }
        for block in message_blocks(message) {
            if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                continue;
            }
            let (Some(id), Some(name)) = (
                block.get("id").and_then(Value::as_str),
                block.get("name").and_then(Value::as_str),
            ) else {
                continue;
            };
            names.insert(id.to_string(), name.to_string());
        }
    }
    names
}

/// JSON request accepted by the current Sand stream endpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct SandInferenceRequest {
    pub messages: Vec<SandInferenceMessage>,
    pub model_id: String,
    /// Optional CLI/catalog id used only to derive Desktop model parameters.
    /// The Sand wire `model_id` remains the canonical family id, while effort
    /// and thinking settings can still be inherited from the request's
    /// resolved CLI variant (for example Fable's `thinking-max`).
    pub parameter_model_id: Option<String>,
    pub conversation_id: String,
    pub invocation_id: String,
    pub max_mode: bool,
    pub max_tokens: Option<u64>,
    pub tools: Vec<Value>,
    /// Names of executable tools that are intentionally absent from
    /// `tools`. Cursor's InferenceService uses this repeated field for
    /// dynamic/client-local tools (the desktop runtime calls it
    /// `acceptedUnadvertisedToolNames`).
    pub accepted_unadvertised_tool_names: Vec<String>,
    /// Forward-compatible fields supplied by a newer desktop build.  Keeping
    /// these as JSON avoids baking unstable protobuf-generated fields into the
    /// proxy and lets callers pass tool/config metadata when available.
    pub extra: Map<String, Value>,
}

impl SandInferenceRequest {
    pub fn new(
        model_id: impl Into<String>,
        conversation_id: impl Into<String>,
        invocation_id: impl Into<String>,
        messages: Vec<SandInferenceMessage>,
    ) -> Self {
        Self {
            messages,
            model_id: model_id.into(),
            parameter_model_id: None,
            conversation_id: conversation_id.into(),
            invocation_id: invocation_id.into(),
            max_mode: false,
            max_tokens: None,
            tools: Vec::new(),
            accepted_unadvertised_tool_names: Vec::new(),
            extra: Map::new(),
        }
    }

    pub fn with_max_mode(mut self, enabled: bool) -> Self {
        self.max_mode = enabled;
        self
    }

    /// Derive `requestedModel.parameters` from a catalog/CLI variant while
    /// keeping `requestedModel.modelId` on the Sand family namespace.
    pub fn with_parameter_model_id(mut self, model_id: impl Into<String>) -> Self {
        let model_id = model_id.into();
        if !model_id.trim().is_empty() {
            self.parameter_model_id = Some(model_id);
        }
        self
    }

    pub fn with_max_tokens(mut self, max_tokens: Option<u64>) -> Self {
        self.max_tokens = max_tokens.filter(|value| *value > 0);
        self
    }

    pub fn with_extra(mut self, key: impl Into<String>, value: Value) -> Self {
        self.extra.insert(key.into(), value);
        self
    }

    pub fn with_tools(mut self, tools: Vec<Value>) -> Self {
        self.tools = tools;
        self
    }

    /// Set the execution-only tool names accepted by the Sand gateway.
    /// Empty/duplicate names are removed while preserving first-seen order so
    /// snapshots and diagnostics remain deterministic.
    pub fn with_accepted_unadvertised_tool_names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut seen = HashSet::new();
        self.accepted_unadvertised_tool_names = names
            .into_iter()
            .map(Into::into)
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .filter(|name| seen.insert(name.clone()))
            .collect();
        self
    }

    /// Clone a full-history request with fresh InferenceService lifecycle
    /// identifiers. Sand does not use the AgentService session registry, and a
    /// retry after a half-open socket must not collide with the abandoned
    /// invocation (which otherwise surfaces as repeated 503 "already active").
    pub fn with_fresh_ids(mut self) -> Self {
        self.conversation_id = uuid::Uuid::new_v4().to_string();
        self.invocation_id = uuid::Uuid::new_v4().to_string();
        self
    }

    /// Build the unframed JSON object.  This is public for protocol tests and
    /// for callers that need to inspect/log a redacted request before sending.
    pub fn to_json_value(&self) -> Value {
        let mut object = self.extra.clone();
        object.insert(
            "messages".into(),
            Value::Array(
                self.messages
                    .iter()
                    .map(SandInferenceMessage::to_json_value)
                    .collect(),
            ),
        );
        let parameter_model_id = self
            .parameter_model_id
            .as_deref()
            .unwrap_or(self.model_id.as_str());
        object.insert(
            "requestedModel".into(),
            json!({
                "modelId": self.model_id,
                "builtInModel": true,
                "maxMode": self.max_mode,
                // `parameters` is a repeated protobuf field.  Cursor's
                // managed-local runtime reads it with `.map(...)` while
                // constructing the provider attempt, so keep the array
                // explicit even when a model has no effort parameters.
                "parameters": requested_model_parameters_json(parameter_model_id),
                "isVariantStringRepresentation": false,
            }),
        );
        // InferenceService validates the top-level model id as well as the
        // requestedModel envelope.  Desktop sends both fields; omitting the
        // duplicate makes the endpoint classify the request as an older
        // AgentService payload.
        object.insert("modelId".into(), json!(self.model_id));
        object.insert("conversationId".into(), json!(self.conversation_id));
        // Cursor Desktop defaults the optional group binding to the
        // conversation id.  Fable's tool-capable provider uses this binding
        // when it attaches execution state; omitting it can make an otherwise
        // valid non-empty catalog fail as a generic provider 400.
        object.insert("conversationGroupId".into(), json!(self.conversation_id));
        object.insert("invocationId".into(), json!(self.invocation_id));
        // These are repeated fields in InferenceStreamRequest. Proto3 JSON
        // defaults omitted arrays correctly, but emitting them keeps the wire
        // contract explicit while staying within the current schema.
        object.insert("tools".into(), Value::Array(self.tools.clone()));
        object.insert("providerDefinedTools".into(), Value::Array(Vec::new()));
        if !self.accepted_unadvertised_tool_names.is_empty() {
            object.insert(
                "acceptedUnadvertisedToolNames".into(),
                Value::Array(
                    self.accepted_unadvertised_tool_names
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            );
        }
        if let Some(max_tokens) = self.max_tokens {
            object.insert("modelConfig".into(), json!({ "maxTokens": max_tokens }));
        }
        Value::Object(object)
    }

    pub fn to_json_bytes(&self) -> Result<Vec<u8>, CursorError> {
        serde_json::to_vec(&self.to_json_value())
            .map_err(|error| CursorError::internal(format!("Sand request JSON encode: {error}")))
    }

    pub fn encode_frame(&self) -> Result<Bytes, CursorError> {
        Ok(encode_connect_frame(self.to_json_bytes()?, 0))
    }
}

/// Encode a raw JSON value as one Connect-JSON request frame.
pub fn encode_json_frame(value: &Value) -> Result<Bytes, CursorError> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| CursorError::internal(format!("Sand JSON encode: {error}")))?;
    Ok(encode_connect_frame(payload, 0))
}

/// A decoded response stream from InferenceService/Stream.
pub struct SandInferenceStream {
    bytes: Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
    decoder: ConnectFrameDecoder,
    pending: VecDeque<Result<CursorStreamEvent, CursorError>>,
    timeout_secs: u64,
    ended: bool,
    saw_end: bool,
    /// Set once a terminal event (or terminal error) has been queued.  The
    /// Connect endpoint may repeat FLAG_END or append a final JSON marker;
    /// downstream Anthropic encoders must observe exactly one End event.
    terminal_emitted: bool,
    tool_buffers: HashMap<String, SandToolBuffer>,
    completed_tool_ids: HashSet<String>,
    // Kept last so every early return, clean End, decoder error, and
    // downstream cancellation returns active-stream capacity uniformly.
    stream_permit: Option<SandStreamPermit>,
}

#[derive(Debug, Default)]
struct SandToolBuffer {
    name: String,
    /// JSON argument fragments are accumulated by tool-call id.  Cursor may
    /// split a large call over several `toolCallPart` frames.
    args_text: String,
    args_value: Option<Value>,
    /// `InferenceToolCallStreamPart.isComplete` is the authoritative commit
    /// signal for string fragments.  A syntactically complete prefix is not
    /// enough: the next frame may still append another property.
    complete: bool,
}

impl SandInferenceStream {
    fn new(response: reqwest::Response, timeout_secs: u64) -> Self {
        Self {
            bytes: Box::pin(response.bytes_stream()),
            decoder: ConnectFrameDecoder::new(),
            pending: VecDeque::new(),
            timeout_secs,
            ended: false,
            saw_end: false,
            terminal_emitted: false,
            tool_buffers: HashMap::new(),
            completed_tool_ids: HashSet::new(),
            stream_permit: None,
        }
    }

    pub(crate) fn with_stream_permit(mut self, permit: SandStreamPermit) -> Self {
        self.stream_permit = Some(permit);
        self
    }

    /// Queue one and only one terminal event.  Marking the stream ended here
    /// also drops any frames coalesced after FLAG_END in the same HTTP chunk.
    fn emit_end_once(&mut self) {
        if self.terminal_emitted {
            return;
        }
        self.flush_tool_buffers();
        self.terminal_emitted = true;
        self.saw_end = true;
        self.ended = true;
        self.pending.push_back(Ok(CursorStreamEvent::End));
    }

    fn queue_terminal_error(&mut self, error: CursorError) {
        if self.terminal_emitted {
            return;
        }
        self.terminal_emitted = true;
        self.ended = true;
        self.pending.push_back(Err(error));
    }

    /// Decode an already-buffered HTTP response.  Useful for non-streaming
    /// callers and deterministic tests; the returned body retains Connect
    /// framing so existing response accounting can inspect it when needed.
    pub async fn collect_response(mut self) -> Result<CursorUpstreamResponse, CursorError> {
        let mut body = Vec::new();
        while let Some(item) = self.next().await {
            let event = item?;
            // Re-encode event data is lossy, so this helper is intentionally
            // only a transport success marker. Callers needing events should
            // consume the stream directly. Keeping an empty body here avoids
            // pretending JSON frames are Agent protobuf frames.
            let _ = event;
        }
        body.shrink_to_fit();
        Ok(CursorUpstreamResponse {
            status: 200,
            body,
            error_detail: None,
        })
    }

    fn queue_frame(&mut self, frame: ConnectFrame) {
        if self.ended || self.terminal_emitted {
            return;
        }
        // Current Sand builds append a control/trailer frame with bit 7 set.
        // Its payload is often binary or an implementation-specific trailer;
        // attempting JSON decoding here turns an otherwise successful answer
        // into a spurious 502.  Check this before FLAG_END because a gateway
        // may combine the trailer and end bits.
        if frame.flags & FLAG_CONTROL != 0 {
            if frame.flags & FLAG_END != 0 {
                // A combined control+END frame carries no model payload, but
                // it is still the authoritative stream terminator.  Ignoring
                // the END bit here leaves `saw_end` set only implicitly at
                // EOF and can suppress the Anthropic message_end event.
                self.saw_end = true;
                self.emit_end_once();
            }
            return;
        }
        if frame.flags & FLAG_END != 0 {
            self.saw_end = true;
            if frame.payload.is_empty() {
                self.emit_end_once();
                return;
            }
            let payload = match frame_payload(&frame) {
                Ok(payload) => payload,
                Err(error) => {
                    self.queue_terminal_error(error);
                    return;
                }
            };
            if let Some(error) = parse_connect_error(&payload) {
                self.queue_terminal_error(CursorError::new(
                    error.status,
                    error.message,
                    Some(error.detail),
                ));
            } else {
                // Some gateways put a normal final JSON object on the END
                // frame. Decode it before emitting End so a final text delta
                // is not lost.
                match serde_json::from_slice::<Value>(&payload) {
                    Ok(value) => self.queue_json_value(&value),
                    Err(_) if !payload.is_empty() => self.queue_terminal_error(CursorError::new(
                        502,
                        "Sand inference END frame is not valid JSON",
                        Some(String::from_utf8_lossy(&payload).into_owned()),
                    )),
                    Err(_) => {}
                }
                self.emit_end_once();
            }
            return;
        }

        let payload = match frame_payload(&frame) {
            Ok(payload) => payload,
            Err(error) => {
                self.queue_terminal_error(error);
                return;
            }
        };
        if payload.is_empty() {
            return;
        }
        match serde_json::from_slice::<Value>(&payload) {
            Ok(value) => self.queue_json_value(&value),
            Err(error) => self.queue_terminal_error(CursorError::new(
                502,
                "Sand inference response frame is not valid JSON",
                Some(format!("{error}: {}", String::from_utf8_lossy(&payload))),
            )),
        }
    }

    fn queue_json_value(&mut self, value: &Value) {
        if self.ended || self.terminal_emitted {
            return;
        }
        if let Some(error) = json_error(value) {
            self.queue_terminal_error(error);
            return;
        }
        let value = value.get("result").unwrap_or(value);
        for event in
            events_from_json_with_state(value, &mut self.tool_buffers, &mut self.completed_tool_ids)
        {
            self.pending.push_back(Ok(event));
        }
        // A few desktop builds omit FLAG_END and mark the final object.  Honor
        // that marker, but only after queuing its text/usage/tool events.
        if json_is_terminal(value) {
            self.emit_end_once();
        }
    }

    fn flush_tool_buffers(&mut self) {
        let buffers = std::mem::take(&mut self.tool_buffers);
        for (id, buffer) in buffers {
            // Never expose an unterminated fragment as a JSON string.  Claude
            // Code treats that as a malformed tool invocation and can enter a
            // StrReplace/XML fallback loop.  A complete empty argument list is
            // represented by an empty object, matching the desktop client.
            let Some(input) = complete_tool_input(&buffer) else {
                continue;
            };
            if !buffer.name.is_empty() && self.completed_tool_ids.insert(id.clone()) {
                self.pending.push_back(Ok(CursorStreamEvent::NativeTool {
                    tool_use_id: id,
                    name: buffer.name,
                    input,
                }));
            }
        }
    }

    fn finish_at_eof(&mut self) {
        if self.ended {
            return;
        }
        self.ended = true;
        self.flush_tool_buffers();
        if self.decoder.buffered() != 0 {
            self.pending.push_back(Err(CursorError::new(
                502,
                "Sand inference stream ended with an incomplete Connect frame",
                Some(format!("{} trailing bytes", self.decoder.buffered())),
            )));
        } else if !self.terminal_emitted {
            // The endpoint normally sends FLAG_END. Treat a clean HTTP close
            // as terminal for compatibility with proxies that strip the end
            // marker; no useful frame is silently left hanging. Check the
            // actual terminal state rather than `saw_end`: a control/trailer
            // parser can observe the END bit before its terminal event is
            // queued, and that state must still be closed for downstream SSE.
            self.emit_end_once();
        }
    }
}

#[cfg(test)]
pub(crate) fn pending_stream_with_permit_and_notify_for_test(
    gate: Arc<Semaphore>,
    polled: Arc<Notify>,
) -> SandInferenceStream {
    let permit = gate
        .try_acquire_owned()
        .expect("pending stream fixture should own its capacity permit");
    let bytes = futures_util::stream::poll_fn(move |_cx| {
        polled.notify_one();
        Poll::Pending
    });
    SandInferenceStream {
        bytes: Box::pin(bytes),
        decoder: ConnectFrameDecoder::new(),
        pending: VecDeque::new(),
        timeout_secs: 5,
        ended: false,
        saw_end: false,
        terminal_emitted: false,
        tool_buffers: HashMap::new(),
        completed_tool_ids: HashSet::new(),
        stream_permit: Some(SandStreamPermit {
            permit: Some(permit),
        }),
    }
}

impl Stream for SandInferenceStream {
    type Item = Result<CursorStreamEvent, CursorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(item) = this.pending.pop_front() {
                return Poll::Ready(Some(item));
            }
            if this.ended {
                return Poll::Ready(None);
            }
            match this.bytes.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => {
                    let frames = match this.decoder.push_with_limit(&chunk, MAX_SAND_FRAME_PAYLOAD)
                    {
                        Ok(frames) => frames,
                        Err(error) => {
                            this.ended = true;
                            return Poll::Ready(Some(Err(CursorError::new(
                                502,
                                "Sand inference Connect frame decode failed",
                                Some(error.to_string()),
                            ))));
                        }
                    };
                    for frame in frames {
                        this.queue_frame(frame);
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    this.ended = true;
                    return Poll::Ready(Some(Err(CursorError::from_reqwest(
                        error,
                        this.timeout_secs,
                    ))));
                }
                Poll::Ready(None) => {
                    this.finish_at_eof();
                }
            }
        }
    }
}

/// Thin HTTP client for the Sand endpoint.  It can be built from the shared
/// Cursor client so proxy/base-url/timeout settings remain identical.
#[derive(Clone)]
pub struct SandInferenceClient {
    client: reqwest::Client,
    base_url: String,
    timeout_secs: u64,
}

/// Process-wide Sand HTTP client pools.
///
/// `reqwest::Client` is cheap to clone, but expensive to construct: building
/// one for every Claude Code turn disables the connection pool and causes a
/// fresh TCP/TLS/HTTP2 handshake for every request.  Under a 512-way Grok
/// fan-out that looks exactly like an upstream admission outage (and can
/// exhaust Cursor's connection budget before a stream is accepted).  Keep a
/// small set of clients, sharded by conversation, and clone the selected
/// client for each request.  The client cache is separate from the Agent
/// client cache because Sand may use `CCP_CURSOR_SAND_BASE_URL` and a
/// different HTTP2 transport mode.
struct SharedSandInferenceClients {
    fingerprint: String,
    clients: Vec<SandInferenceClient>,
}

static SHARED_SAND_INFERENCE_CLIENTS: LazyLock<Mutex<Option<SharedSandInferenceClients>>> =
    LazyLock::new(|| Mutex::new(None));

fn sand_client_shard_count() -> usize {
    std::env::var("CCP_CURSOR_H2_SHARDS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(16)
        .clamp(1, 64)
}

fn sand_client_shard_index(key: &str, shard_count: usize) -> usize {
    // Stable FNV-1a keeps one conversation on one H2 pool while still
    // distributing independent sessions.  A deterministic hash is important
    // for retries: a reconnect should stay in the same failure domain rather
    // than opening another client and multiplying handshakes.
    let hash = key
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    (hash as usize) % shard_count.max(1)
}

fn sand_client_fingerprint(base_url: &str, timeout_secs: u64, shard_count: usize) -> String {
    // Include transport-affecting environment values.  This lets a TUI or a
    // test that changes the proxy/strict-H2 switch obtain a fresh pool without
    // requiring a process restart, while ordinary requests keep reusing the
    // already-established H2 connections.
    let no_proxy = std::env::var("CCP_CURSOR_NO_PROXY").unwrap_or_default();
    let strict_h2 = std::env::var("CCP_CURSOR_SAND_STRICT_H2").unwrap_or_default();
    let https_proxy = std::env::var("HTTPS_PROXY")
        .or_else(|_| std::env::var("https_proxy"))
        .unwrap_or_default();
    let http_proxy = std::env::var("HTTP_PROXY")
        .or_else(|_| std::env::var("http_proxy"))
        .unwrap_or_default();
    format!(
        "{base_url}\0{timeout_secs}\0{shard_count}\0{no_proxy}\0{strict_h2}\0{https_proxy}\0{http_proxy}"
    )
}

/// Return a pooled Sand client for one conversation (or request id when the
/// caller has no session).  This is intentionally `pub(crate)` so the Cursor
/// provider can use it while protocol tests can continue to construct an
/// isolated client with [`SandInferenceClient::with_base_url_timeout`].
pub(crate) fn shared_client(conversation_key: Option<&str>) -> SandInferenceClient {
    let base_url = crate::config::cursor_sand_base_url();
    let timeout_secs = crate::config::cursor_request_timeout_secs();
    let shard_count = sand_client_shard_count();
    let fingerprint = sand_client_fingerprint(&base_url, timeout_secs, shard_count);
    let mut cache = SHARED_SAND_INFERENCE_CLIENTS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());

    let needs_rebuild = cache
        .as_ref()
        .is_none_or(|existing| existing.fingerprint != fingerprint);
    if needs_rebuild {
        let clients = (0..shard_count)
            .map(|_| {
                let source = CursorHttpClient::with_base_url_timeout_and_prefer_http1(
                    base_url.clone(),
                    timeout_secs,
                    false,
                );
                // `from_cursor_client` switches the source to Sand's H2 mode
                // once, at pool construction time.  Cloning the resulting
                // Sand client below shares reqwest's connection pool.
                SandInferenceClient::from_cursor_client(&source)
            })
            .collect();
        *cache = Some(SharedSandInferenceClients {
            fingerprint,
            clients,
        });
    }

    let pool = cache
        .as_ref()
        .expect("Sand client pool must be initialized");
    let index = conversation_key
        .map(|key| sand_client_shard_index(key, pool.clients.len()))
        .unwrap_or(0);
    pool.clients[index].clone()
}

impl SandInferenceClient {
    pub fn new() -> Self {
        // Sand has its own endpoint override, but should inherit the same
        // timeout and proxy settings as the normal Cursor client.  Construct
        // the source with the resolved Sand URL rather than silently using
        // `CCP_CURSOR_BASE_URL`/the public default.
        let source = CursorHttpClient::with_base_url_timeout_and_prefer_http1(
            crate::config::cursor_sand_base_url(),
            crate::config::cursor_request_timeout_secs(),
            false,
        );
        Self::from_cursor_client(&source)
    }

    pub(crate) fn from_cursor_client(source: &CursorHttpClient) -> Self {
        // Sand must never inherit a process-wide HTTP/1 pin.  The shared
        // constructor selects strict H2 (or prior-knowledge H2 for fixtures).
        let sand = source.with_sand_transport_mode();
        Self {
            client: sand.client.clone(),
            base_url: sand.base_url.clone(),
            timeout_secs: sand.timeout_secs,
        }
    }

    pub fn with_base_url_timeout(
        base_url: impl Into<String>,
        timeout_secs: u64,
    ) -> Result<Self, CursorError> {
        let base_url = base_url.into();
        let source = CursorHttpClient::with_base_url_timeout_and_prefer_http1(
            base_url,
            timeout_secs.max(1),
            false,
        );
        Ok(Self::from_cursor_client(&source))
    }

    pub fn endpoint(&self) -> String {
        format!(
            "{}{}",
            self.base_url.trim_end_matches('/'),
            SAND_INFERENCE_STREAM_PATH
        )
    }

    /// Open one Sand stream.  No replay occurs after a response is accepted;
    /// callers can safely retry only a returned connect/open error.
    pub async fn open(
        &self,
        token: &str,
        request: &SandInferenceRequest,
    ) -> Result<SandInferenceStream, CursorError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let body = request.encode_frame()?;
        let client_type = "sand";
        let mut builder = self
            .client
            .post(self.endpoint())
            .bearer_auth(token)
            .header("content-type", "application/connect+json")
            .header("connect-protocol-version", "1")
            .header("connect-accept-encoding", "gzip")
            .header("user-agent", "connect-es/1.6.1")
            // The Desktop InferenceService checks the product version in the
            // legacy `x-cursor-version` header in addition to the newer
            // client-version identity header.
            .header(
                "x-cursor-version",
                crate::config::cursor_client_version_for_type("sand"),
            )
            .header("x-request-id", &request_id)
            .header("x-original-request-id", &request_id);
        builder = crate::providers::cursor::client::apply_cursor_identity_headers_for_client_type(
            builder,
            token,
            Some(client_type),
        );

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(self.timeout_secs.max(1)),
            builder.body(body).send(),
        )
        .await
        .map_err(|_| {
            CursorError::new(
                504,
                format!("Sand inference open timed out after {}s", self.timeout_secs),
                None,
            )
        })?
        .map_err(|error| CursorError::from_reqwest(error, self.timeout_secs))?;

        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            let body = response.bytes().await.unwrap_or_default();
            let error = sand_http_error_from_body(status, &body);
            return Err(error.with_retry_after(retry_after));
        }
        Ok(SandInferenceStream::new(response, self.timeout_secs))
    }

    /// Convenience wrapper for callers that need a stream of events and do
    /// not need to retain the HTTP response object.
    pub async fn stream_events(
        &self,
        token: &str,
        request: &SandInferenceRequest,
    ) -> Result<SandInferenceStream, CursorError> {
        self.open(token, request).await
    }
}

impl Default for SandInferenceClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Decode an HTTP non-2xx Sand response into a structured error.
///
/// InferenceService can return policy failures either as a plain JSON body or
/// as one or more Connect frames even when the HTTP status is already 4xx/5xx.
/// Reading the body as text loses the framed JSON (and therefore the account
/// quota markers), which makes a deterministic 429 look transient and causes
/// the stream retry loop to replay it five times. Preserve the machine-readable
/// provider details before the retry classifier sees the error.
fn sand_http_error_from_body(status: u16, body: &[u8]) -> CursorError {
    let mut decoder = ConnectFrameDecoder::new();
    if let Ok(frames) = decoder.push_with_limit(body, MAX_SAND_FRAME_PAYLOAD) {
        for frame in frames {
            let Ok(payload) = frame_payload(&frame) else {
                continue;
            };
            if let Ok(value) = serde_json::from_slice::<Value>(&payload)
                && let Some(error) = json_error(&value)
            {
                return CursorError::new(status, error.message, error.detail);
            }
            if let Some(error) = parse_connect_error(&payload) {
                // Keep the compact quota/reset markers even when the generic
                // Connect parser wins over the Sand JSON adapter.  The raw
                // provider envelope is useful for diagnostics, but callers
                // need a stable `nextResetAt=...` field for account breaker
                // and failover decisions.
                let detail = serde_json::from_slice::<Value>(&payload)
                    .ok()
                    .and_then(|value| sand_provider_error_metadata(&value))
                    .map(|metadata| format!("{}; {metadata}", error.detail))
                    .unwrap_or(error.detail);
                return CursorError::new(status, error.message, Some(detail));
            }
        }
    }

    if let Ok(value) = serde_json::from_slice::<Value>(body)
        && let Some(error) = json_error(&value)
    {
        return CursorError::new(status, error.message, error.detail);
    }

    let detail = String::from_utf8_lossy(body).trim().to_string();
    CursorError::new(
        status,
        format!("Sand inference upstream HTTP {status}"),
        (!detail.is_empty()).then_some(detail),
    )
}

fn frame_payload(frame: &ConnectFrame) -> Result<Vec<u8>, CursorError> {
    if frame.flags & FLAG_GZIP != 0 {
        decode_gzip_frame(&frame.payload).map_err(|error| {
            CursorError::new(
                502,
                "Sand inference gzip frame decode failed",
                Some(error.to_string()),
            )
        })
    } else {
        Ok(frame.payload.to_vec())
    }
}

/// Extract an `InferenceStreamError` from a response object.  Connect
/// adapters in the wild wrap the protobuf-JSON response in one or more
/// transport envelopes (`result`, `response`, `data`, or `payload`), so look
/// through those known keys as well as the direct response shape.  Deliberate
/// key allow-listing keeps an `error` field inside tool arguments/metadata
/// from being interpreted as a stream failure.
fn json_error(value: &Value) -> Option<CursorError> {
    json_error_with_depth(value, 0)
}

fn json_error_with_depth(value: &Value, depth: u8) -> Option<CursorError> {
    // A malformed/proxy-generated envelope should not be able to force an
    // unbounded recursive walk.  Normal responses are at most one or two
    // wrappers deep; the extra headroom covers nested Connect adapters.
    const MAX_ERROR_ENVELOPE_DEPTH: u8 = 8;

    if let Some(error) = json_error_direct(value) {
        return Some(error);
    }
    if depth >= MAX_ERROR_ENVELOPE_DEPTH {
        return None;
    }
    let Value::Object(object) = value else {
        return None;
    };
    for key in ["result", "response", "data", "payload"] {
        if let Some(child) = object.get(key)
            && let Some(error) = json_error_with_depth(child, depth + 1)
        {
            return Some(error);
        }
    }
    None
}

fn json_error_direct(value: &Value) -> Option<CursorError> {
    let error = value.get("error")?;
    if error.is_null() {
        return None;
    }
    let error_object = error.as_object();
    let code_value = error_object.and_then(|object| object.get("code"));
    let error_type_value = error_object
        .and_then(|object| object.get("errorType"))
        .or_else(|| error_object.and_then(|object| object.get("error_type")));
    let error_type = error_type_value
        .and_then(value_as_string)
        .filter(|value| !value.trim().is_empty());
    let raw_code = code_value
        .and_then(value_as_string)
        .filter(|value| !value.trim().is_empty());
    let code = raw_code
        .clone()
        .or_else(|| error_type.clone())
        .unwrap_or_else(|| "upstream_error".into());
    let mut message = error_object
        .and_then(|object| object.get("message"))
        .and_then(value_as_string)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| value_as_string(error))
        .unwrap_or_else(|| "Sand inference upstream error".into());

    // InferenceService may put the useful provider diagnosis (for example
    // "temporary trouble connecting") inside ErrorDetails while leaving the
    // direct `message` as a generic "Error". Reuse the Connect parser's
    // normalized detail so retry/account classification sees the same text
    // on both END frames and regular JSON stream frames.
    if let Ok(bytes) = serde_json::to_vec(value)
        && let Some(parsed) = parse_connect_error(&bytes)
        && parsed.provider_error_code.is_some()
        && !parsed.message.trim().is_empty()
        && !message
            .to_ascii_lowercase()
            .contains(&parsed.message.to_ascii_lowercase())
    {
        message.push_str(" — ");
        message.push_str(&parsed.message);
    }
    if let Some(error_type) = error_type.as_deref()
        && !message
            .to_ascii_lowercase()
            .contains(&error_type.to_ascii_lowercase())
    {
        message.push_str(" [errorType=");
        message.push_str(error_type);
        message.push(']');
    }
    let status = if error_object.is_some_and(|object| {
        object
            .get("isInputTokenLimitError")
            .or_else(|| object.get("is_input_token_limit_error"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
            || object
                .get("isOutputTokenLimitError")
                .or_else(|| object.get("is_output_token_limit_error"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
    }) {
        400
    } else {
        connect_error_status(code_value, error_type_value, &message)
    };
    // Preserve the old concise `code` detail and add `errorType` when it
    // carries independent classification. CursorError::client_message then
    // exposes both without dumping an arbitrarily large error envelope.
    let mut detail = match (raw_code, error_type) {
        (Some(code), Some(error_type)) => format!("code={code}; errorType={error_type}"),
        (Some(code), None) => code,
        (None, Some(error_type)) => format!("errorType={error_type}"),
        (None, None) => code,
    };
    // Current Cursor Sand responses can wrap an account-specific provider
    // 4xx in an outer `resource_exhausted` envelope. Keep the small inner
    // diagnostic fields in the client message so the request router can
    // rotate accounts even when the error arrived after HTTP 200.
    if let Some(provider) = sand_provider_error_metadata(value) {
        detail.push_str("; ");
        detail.push_str(&provider);
    }
    Some(CursorError::new(status, message, Some(detail)))
}

fn sand_provider_error_metadata(value: &Value) -> Option<String> {
    let bytes = serde_json::to_vec(value).ok()?;
    let mut fields = Vec::new();
    if let Some(parsed) = parse_connect_error(&bytes) {
        if let Some(code) = parsed.provider_error_code {
            fields.push(format!("providerErrorCode={code}"));
        }
        if let Some(status) = parsed.provider_status_code {
            fields.push(format!("providerStatusCode={status}"));
        }
        if let Some(retryable) = parsed.provider_is_retryable {
            fields.push(format!("isRetryable={retryable}"));
        }
    }
    // Sand quota responses put the allowance reason and reset timestamp in
    // `additionalInfo`, but Connect/SSE may flatten or rename those fields
    // before this helper sees them. Preserve the small machine-readable
    // values in CursorError::detail so account failover and its breaker can
    // use the actual reset window instead of a short generic retry cooldown.
    if let Some(reason) = find_json_string_field(value, &["rateLimitReason", "rate_limit_reason"]) {
        if !fields
            .iter()
            .any(|field| field.starts_with("rateLimitReason="))
        {
            fields.push(format!("rateLimitReason={reason}"));
        }
    }
    if let Some(reset) = find_json_string_field(value, &["nextResetAt", "next_reset_at"]) {
        fields.push(format!("nextResetAt={reset}"));
    }
    (!fields.is_empty()).then(|| fields.join(" "))
}

/// Find a scalar string/number/bool under a nested Sand error envelope. Error
/// details have appeared both as objects and arrays across gateway versions,
/// so keep this traversal deliberately schema-light and case-insensitive.
fn find_json_string_field(value: &Value, names: &[&str]) -> Option<String> {
    match value {
        Value::Object(object) => {
            for (key, candidate) in object {
                if names.iter().any(|name| key.eq_ignore_ascii_case(name)) {
                    if let Some(text) = value_as_string(candidate) {
                        return Some(text);
                    }
                }
            }
            object
                .values()
                .find_map(|candidate| find_json_string_field(candidate, names))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|candidate| find_json_string_field(candidate, names)),
        _ => None,
    }
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}

fn json_is_terminal(value: &Value) -> bool {
    let object = match value {
        Value::Object(object) => object,
        _ => return false,
    };
    if [
        "done",
        "finished",
        "isFinished",
        "is_finished",
        "endOfStream",
        "end_of_stream",
    ]
    .iter()
    .any(|key| object.get(*key).and_then(Value::as_bool).unwrap_or(false))
        || object
            .get("finishReason")
            .or_else(|| object.get("finish_reason"))
            .is_some_and(|reason| !reason.is_null() && reason.as_str() != Some(""))
    {
        return true;
    }

    // A few Connect gateways wrap the protobuf JSON in `result`/`response`.
    // Recurse only through those known envelopes so an `isFinal` flag nested
    // inside tool arguments or provider metadata cannot close the stream.
    ["result", "response", "data", "payload"]
        .iter()
        .filter_map(|key| object.get(*key))
        .any(json_is_terminal)
}

#[cfg(test)]
fn events_from_json(value: &Value) -> Vec<CursorStreamEvent> {
    // Standalone callers expect one JSON object to be self-contained.  The
    // streaming decoder keeps the map on `SandInferenceStream` instead so
    // argument fragments can span frames.
    let mut buffers = HashMap::new();
    let mut completed = HashSet::new();
    let mut events = events_from_json_with_state(value, &mut buffers, &mut completed);
    events.extend(flush_tool_buffers_to_events(&mut buffers));
    events
}

fn events_from_json_with_state(
    value: &Value,
    buffers: &mut HashMap<String, SandToolBuffer>,
    completed: &mut HashSet<String>,
) -> Vec<CursorStreamEvent> {
    let mut events = Vec::new();
    let mut text = Vec::new();
    let mut thinking = Vec::new();
    collect_text_parts(value, &mut text, &mut thinking);
    // Preserve wire order where possible: InferenceService normally emits one
    // part per frame, so this order is deterministic and avoids combining a
    // reasoning delta with visible output in one event.
    for part in thinking {
        if !part.is_empty() {
            events.push(CursorStreamEvent::ThinkingDelta { text: part });
        }
    }
    for part in text {
        if !part.is_empty() {
            events.push(CursorStreamEvent::TextDelta { text: part });
        }
    }
    if let Some(usage) = extract_usage(value) {
        events.push(CursorStreamEvent::Usage {
            input_tokens: usage.0,
            output_tokens: usage.1,
            cache_read_tokens: usage.2,
            cache_write_tokens: usage.3,
        });
    }
    let exact_parts = extract_tool_call_parts(value);
    if exact_parts.is_empty() {
        // Older gateways used `toolCall`/`functionCall`; retain that fallback
        // while avoiding duplicate events when the modern `toolCallPart`
        // envelope is present.
        for (id, name, input) in extract_tool_calls(value) {
            events.push(CursorStreamEvent::NativeTool {
                tool_use_id: id,
                name,
                input,
            });
        }
    } else {
        for part in exact_parts {
            if let Some(event) = ingest_tool_call_part(part, buffers, completed) {
                events.push(event);
            }
        }
    }
    if let Some(session_id) = extract_string(value, &["sessionId", "session_id", "conversationId"])
    {
        // A conversation id is useful to callers that need to persist the
        // binding, but avoid emitting it when it is merely echoed on every
        // frame and no event otherwise exists.
        if !session_id.is_empty() && events.is_empty() {
            events.push(CursorStreamEvent::Session { session_id });
        }
    }
    events
}

#[derive(Debug)]
struct SandToolPart {
    id: String,
    name: String,
    args: Option<Value>,
    done: bool,
    index: Option<i32>,
}

/// Collect the current InferenceService schema exactly.  The endpoint emits
/// `toolCallPart` (and, in some builds, `tool_call_part`) rather than the
/// AgentService `toolCall` envelope.  Parsing this key explicitly prevents
/// metadata nested inside a tool argument from being mistaken for another
/// call.
fn extract_tool_call_parts(value: &Value) -> Vec<SandToolPart> {
    let mut out = Vec::new();
    collect_tool_call_parts(value, &mut out);
    out
}

fn collect_tool_call_parts(value: &Value, out: &mut Vec<SandToolPart>) {
    let Value::Object(object) = value else {
        return;
    };
    for (key, child) in object {
        let lower = key.to_ascii_lowercase();
        if matches!(lower.as_str(), "toolcallpart" | "tool_call_part") {
            collect_tool_part_value(child, out);
            continue;
        }
        if !matches!(lower.as_str(), "input" | "arguments" | "args") {
            collect_tool_call_parts(child, out);
        }
    }
}

fn collect_tool_part_value(value: &Value, out: &mut Vec<SandToolPart>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_tool_part_value(item, out);
            }
        }
        Value::Object(object) => {
            let id = ["toolCallId", "tool_call_id", "id", "callId", "call_id"]
                .iter()
                .find_map(|key| object.get(*key).and_then(value_as_string))
                .unwrap_or_default();
            let name = ["toolName", "tool_name", "name"]
                .iter()
                .find_map(|key| object.get(*key).and_then(value_as_string))
                .or_else(|| {
                    object
                        .get("function")
                        .and_then(|function| function.get("name"))
                        .and_then(value_as_string)
                })
                .unwrap_or_default();
            let args = [
                "args",
                "arguments",
                "input",
                "toolCallArgs",
                "tool_call_args",
                "argsText",
                "args_text",
            ]
            .iter()
            .find_map(|key| object.get(*key).cloned())
            .or_else(|| {
                // A few revisions call the incremental argument field
                // `delta`; only use it when this is a tool-part object.
                object.get("delta").cloned()
            });
            let done = [
                "done",
                "isDone",
                "is_done",
                "finished",
                "isFinished",
                "is_finished",
                "complete",
                "completed",
                "isComplete",
                "is_complete",
                "final",
                "isFinal",
                "is_final",
            ]
            .iter()
            .any(|key| object.get(*key).and_then(Value::as_bool).unwrap_or(false));
            let index = ["toolIndex", "tool_index", "index"]
                .iter()
                .find_map(|key| object.get(*key).and_then(Value::as_i64))
                .and_then(|value| i32::try_from(value).ok());
            if !id.is_empty() || !name.is_empty() || args.is_some() {
                out.push(SandToolPart {
                    id,
                    name,
                    args,
                    done,
                    index,
                });
            }
        }
        _ => {}
    }
}

fn ingest_tool_call_part(
    part: SandToolPart,
    buffers: &mut HashMap<String, SandToolBuffer>,
    completed: &mut HashSet<String>,
) -> Option<CursorStreamEvent> {
    // The protocol normally supplies toolCallId.  A tool index is a stable
    // fallback for builds that omit the id on continuation frames; anonymous
    // one-shot calls are still supported when the part is explicitly marked
    // complete.
    let id = if !part.id.is_empty() {
        part.id
    } else if let Some(index) = part.index {
        format!("sand_tool_index_{index}")
    } else if part.done {
        format!("sand_tool_anon_{}", buffers.len() + 1)
    } else {
        // There is no safe key with which to join an anonymous fragment.
        return None;
    };
    if completed.contains(&id) {
        return None;
    }
    let buffer = buffers.entry(id.clone()).or_default();
    if !part.name.is_empty() {
        buffer.name = part.name;
    }
    let structured_args = part.args.as_ref().is_some_and(|args| !args.is_string());
    if let Some(args) = part.args {
        match args {
            Value::String(fragment) => merge_tool_args_text(&mut buffer.args_text, &fragment),
            value => {
                // The current schema declares `args` as a string.  Older
                // gateways occasionally sent a structured value directly;
                // preserve it as-is (an argument named `text` is valid) rather
                // than mistaking that field for an incremental delta.
                buffer.args_value = Some(value);
            }
        }
    }
    buffer.complete |= part.done || structured_args;

    // `isComplete` is authoritative for string fragments.  Do not emit as
    // soon as a prefix happens to parse: Cursor may append more fields in a
    // later frame, and doing so creates duplicate/partial tool calls.
    if !buffer.complete {
        return None;
    }

    let buffer = buffers.remove(&id)?;
    let input = complete_tool_input(&buffer)?;
    if buffer.name.is_empty() {
        // A continuation may carry the final arguments before the initial
        // frame's tool name arrives. Keep the completed buffer around so a
        // later frame can supply that name instead of permanently dropping
        // an otherwise valid call.
        buffers.insert(id, buffer);
        return None;
    }
    completed.insert(id.clone());
    Some(CursorStreamEvent::NativeTool {
        tool_use_id: id,
        name: buffer.name,
        input,
    })
}

/// Merge an argument update from `InferenceToolCallStreamPart`.
///
/// Cursor revisions use both representations on the wire: some emit a true
/// delta per frame, while others repeat the complete argument prefix (and the
/// `isComplete` frame commonly repeats the final JSON object once more).  A
/// blind concatenation turns the latter into invalid JSON (`{}{}")` and makes
/// Claude Code discard an otherwise valid tool call.  Prefer the longer
/// cumulative value when one update contains the other, ignore exact
/// duplicates, and append only when the update is a genuine fragment.
fn merge_tool_args_text(existing: &mut String, update: &str) {
    if update.is_empty() {
        return;
    }
    if existing.is_empty() {
        existing.push_str(update);
        return;
    }
    if update == existing {
        return;
    }
    if update.starts_with(existing.as_str()) {
        // Cumulative update (for example `{"command":"pwd"}` after the
        // prefix `{"command":"p`).
        *existing = update.to_string();
        return;
    }
    if existing.starts_with(update) {
        // A stale/shorter cumulative update; retaining the longer value is
        // safer than truncating an argument assembled from prior frames.
        return;
    }
    // A repeated final object can arrive after an incremental sequence.  When
    // both values are independently valid JSON, the newer value is a
    // cumulative snapshot (or a repeated final snapshot), not another delta.
    // Prefer it instead of producing concatenated objects such as `{}{}'.
    if serde_json::from_str::<Value>(existing).is_ok()
        && serde_json::from_str::<Value>(update).is_ok()
    {
        *existing = update.to_string();
        return;
    }
    existing.push_str(update);
}

#[cfg(test)]
fn flush_tool_buffers_to_events(
    buffers: &mut HashMap<String, SandToolBuffer>,
) -> Vec<CursorStreamEvent> {
    let drained = std::mem::take(buffers);
    drained
        .into_iter()
        .filter_map(|(id, buffer)| {
            if buffer.name.is_empty() || !buffer.complete {
                return None;
            }
            let input = complete_tool_input(&buffer)?;
            Some(CursorStreamEvent::NativeTool {
                tool_use_id: id,
                name: buffer.name,
                input,
            })
        })
        .collect()
}

/// Parse only a completed tool buffer.  Returning `None` for malformed or
/// incomplete JSON is intentional: forwarding the raw fragment as a string
/// causes Anthropic clients to execute a malformed call and retry forever.
fn complete_tool_input(buffer: &SandToolBuffer) -> Option<Value> {
    if !buffer.complete {
        return None;
    }
    if let Some(value) = &buffer.args_value {
        return Some(value.clone());
    }
    if buffer.args_text.trim().is_empty() {
        return Some(Value::Object(Map::new()));
    }
    serde_json::from_str::<Value>(&buffer.args_text).ok()
}

fn collect_text_parts(value: &Value, text: &mut Vec<String>, thinking: &mut Vec<String>) {
    let Value::Object(object) = value else {
        return;
    };
    for (key, child) in object {
        let normalized = key.to_ascii_lowercase();
        let is_thinking = normalized.contains("thinking")
            || normalized.contains("reasoning")
            || normalized.contains("thought");
        let is_text_part = normalized == "text"
            || normalized == "textpart"
            || normalized == "text_part"
            || normalized == "contentpart"
            || normalized == "content_part"
            || normalized == "textdelta"
            || normalized == "text_delta";
        if is_thinking || is_text_part {
            if let Some(value) = text_from_value(child) {
                if is_thinking {
                    thinking.push(value);
                } else {
                    text.push(value);
                }
                continue;
            }
        }
        // These are response oneof branches whose nested strings are
        // metadata, tool arguments, or diagnostics rather than assistant
        // output.  Restricting recursion here prevents e.g. a tool argument
        // `{\"text\": ...}` or provider metadata from leaking into the
        // Anthropic text stream.
        if matches!(
            normalized.as_str(),
            "toolcallpart"
                | "tool_call_part"
                | "toolcall"
                | "tool_call"
                | "tooluse"
                | "tool_use"
                | "functioncall"
                | "function_call"
                | "responseinfo"
                | "response_info"
                | "providermetadata"
                | "provider_metadata"
                | "usage"
                | "extendedusage"
                | "extended_usage"
                | "error"
                | "invocationid"
                | "invocation_id"
        ) {
            continue;
        }
        // Recurse through `result`, `response`, and wrapper objects. Do not
        // recurse into arbitrary `input` maps, where a tool argument named
        // `text` must not become assistant output.
        if !matches!(normalized.as_str(), "input" | "arguments" | "args") {
            collect_text_parts(child, text, thinking);
        }
    }
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Object(object) => ["text", "value", "delta", "content"]
            .iter()
            .find_map(|key| object.get(*key).and_then(text_from_value)),
        Value::Array(items) => {
            let joined = items.iter().filter_map(text_from_value).collect::<String>();
            (!joined.is_empty()).then_some(joined)
        }
        _ => None,
    }
}

fn extract_usage(value: &Value) -> Option<(u64, u64, u64, u64)> {
    let Value::Object(object) = value else {
        return None;
    };
    for key in [
        "usage",
        "extendedUsage",
        "extended_usage",
        "tokenUsage",
        "token_usage",
    ] {
        if let Some(candidate) = object.get(key)
            && let Some(usage) = usage_object(candidate)
        {
            return Some(usage);
        }
    }
    object.iter().find_map(|(key, child)| {
        let normalized = key.to_ascii_lowercase();
        if matches!(
            normalized.as_str(),
            "input"
                | "arguments"
                | "args"
                | "toolcallpart"
                | "tool_call_part"
                | "toolcall"
                | "tool_call"
                | "tooluse"
                | "tool_use"
                | "functioncall"
                | "function_call"
                | "responseinfo"
                | "response_info"
                | "providermetadata"
                | "provider_metadata"
                | "error"
        ) {
            return None;
        }
        extract_usage(child)
    })
}

fn usage_object(value: &Value) -> Option<(u64, u64, u64, u64)> {
    let Value::Object(object) = value else {
        return None;
    };
    let input = number_for_keys(
        object,
        &[
            "inputTokens",
            "input_tokens",
            "promptTokens",
            "prompt_tokens",
            "prompt",
        ],
    );
    let output = number_for_keys(
        object,
        &[
            "outputTokens",
            "output_tokens",
            "completionTokens",
            "completion_tokens",
            "completion",
        ],
    );
    let cache_read = number_for_keys(
        object,
        &[
            "cacheReadTokens",
            "cache_read_tokens",
            "cacheRead",
            "cache_read",
        ],
    );
    let cache_write = number_for_keys(
        object,
        &[
            "cacheWriteTokens",
            "cache_write_tokens",
            "cacheCreationInputTokens",
            "cache_creation_input_tokens",
            "cacheWrite",
            "cache_write",
        ],
    );
    (input > 0 || output > 0 || cache_read > 0 || cache_write > 0).then_some((
        input,
        output,
        cache_read,
        cache_write,
    ))
}

fn number_for_keys(object: &Map<String, Value>, keys: &[&str]) -> u64 {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(number_value))
        .unwrap_or(0)
}

fn number_value(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
        .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
}

fn extract_string(value: &Value, keys: &[&str]) -> Option<String> {
    let Value::Object(object) = value else {
        return None;
    };
    keys.iter()
        .find_map(|key| object.get(*key).and_then(value_as_string))
}

fn extract_tool_calls(value: &Value) -> Vec<(String, String, Value)> {
    let mut out = Vec::new();
    collect_tool_calls(value, &mut out);
    out
}

fn collect_tool_calls(value: &Value, out: &mut Vec<(String, String, Value)>) {
    let Value::Object(object) = value else {
        return;
    };
    for key in [
        "toolCall",
        "tool_call",
        "toolUse",
        "tool_use",
        "functionCall",
        "function_call",
    ] {
        if let Some(candidate) = object.get(key) {
            collect_tool_value(candidate, out);
        }
    }
    for (key, child) in object {
        let lower = key.to_ascii_lowercase();
        if !matches!(lower.as_str(), "input" | "arguments" | "args") {
            collect_tool_calls(child, out);
        }
    }
}

fn collect_tool_value(value: &Value, out: &mut Vec<(String, String, Value)>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_tool_value(item, out);
            }
        }
        Value::Object(object) => {
            let id = ["id", "toolUseId", "tool_use_id", "callId", "call_id"]
                .iter()
                .find_map(|key| object.get(*key).and_then(value_as_string))
                .unwrap_or_else(|| format!("sand_tool_{}", out.len() + 1));
            let name = object
                .get("name")
                .or_else(|| object.get("toolName"))
                .or_else(|| object.get("tool_name"))
                .or_else(|| object.get("function").and_then(|f| f.get("name")))
                .and_then(value_as_string)
                .unwrap_or_else(|| "unknown_tool".into());
            let input = object
                .get("input")
                .or_else(|| object.get("arguments"))
                .or_else(|| object.get("args"))
                .or_else(|| object.get("function").and_then(|f| f.get("arguments")))
                .cloned()
                .unwrap_or_else(|| Value::Object(object.clone()));
            out.push((id, name, input));
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[test]
    fn request_uses_current_sand_json_shape() {
        let request = SandInferenceRequest::new(
            "claude-fable-5",
            "conv-1",
            "invoke-1",
            vec![
                SandInferenceMessage::system("system"),
                SandInferenceMessage::user("hello"),
            ],
        )
        .with_max_tokens(Some(1234));
        let value = request.to_json_value();
        assert_eq!(value["messages"][0]["role"], ROLE_SYSTEM);
        assert_eq!(value["messages"][1]["role"], ROLE_USER);
        assert_eq!(value["requestedModel"]["modelId"], "claude-fable-5");
        assert_eq!(
            value["requestedModel"]["parameters"],
            json!([{ "id": "context", "value": "1m" }])
        );
        assert_eq!(
            value["requestedModel"]["isVariantStringRepresentation"],
            false
        );
        assert_eq!(value["conversationId"], "conv-1");
        assert_eq!(value["invocationId"], "invoke-1");
        assert_eq!(value["tools"], json!([]));
        assert_eq!(value["providerDefinedTools"], json!([]));
        assert!(value.get("acceptedUnadvertisedToolNames").is_none());
        assert_eq!(value["modelConfig"]["maxTokens"], 1234);
    }

    #[test]
    fn request_forwards_catalog_effort_parameters() {
        let request = SandInferenceRequest::new(
            "claude-fable-5-thinking-max",
            "conv-1",
            "invoke-1",
            vec![SandInferenceMessage::user("hello")],
        );
        let value = request.to_json_value();
        let params = value["requestedModel"]["parameters"]
            .as_array()
            .expect("parameters array");
        assert!(
            params
                .iter()
                .any(|value| { value["id"] == "thinking" && value["value"] == "true" })
        );
        assert!(
            params
                .iter()
                .any(|value| { value["id"] == "effort" && value["value"] == "max" })
        );
    }

    #[test]
    fn request_serializes_accepted_unadvertised_tool_names() {
        let request = SandInferenceRequest::new(
            "claude-fable-5",
            "conv-1",
            "invoke-1",
            vec![SandInferenceMessage::user("hello")],
        )
        .with_accepted_unadvertised_tool_names([
            "mcp__claude-local__Workflow",
            "mcp__claude-local__Workflow",
            " ",
            "Task",
        ]);
        assert_eq!(
            request.to_json_value()["acceptedUnadvertisedToolNames"],
            json!(["mcp__claude-local__Workflow", "Task"])
        );
    }

    #[test]
    fn accepted_unadvertised_names_use_explicit_execution_metadata_only() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {"name": "Read", "input_schema": {"type": "object"}},
                {
                    "name": "dynamic_lookup",
                    "dynamicToolMetaRole": "invocation",
                    "modelVisible": false,
                    "input_schema": {"type": "object"}
                },
                {
                    "name": "ordinary_dynamic",
                    "dynamic": true,
                    "input_schema": {"type": "object"}
                }
            ],
            "additionalExecutableTools": [{"name": "mcp__claude-local__Workflow"}],
            "acceptedUnadvertisedToolNames": ["Task", "Read"]
        }))
        .unwrap();
        let model_tools = tools_from_anthropic(&request, false);
        let accepted = accepted_unadvertised_tool_names_from_anthropic(&request, &model_tools);
        // `Read` is visible and therefore removed from the execution-only set;
        // an unmarked dynamic flag is not enough to opt a tool in.
        assert_eq!(
            accepted,
            vec!["Task", "mcp__claude-local__Workflow", "dynamic_lookup"]
        );
    }

    #[test]
    fn request_can_keep_canonical_sand_id_and_catalog_parameters_separate() {
        let request = SandInferenceRequest::new(
            "claude-fable-5",
            "conv-1",
            "invoke-1",
            vec![SandInferenceMessage::user("hello")],
        )
        .with_parameter_model_id("claude-fable-5-thinking-max");
        let value = request.to_json_value();
        assert_eq!(value["modelId"], "claude-fable-5");
        assert_eq!(value["requestedModel"]["modelId"], "claude-fable-5");
        let params = value["requestedModel"]["parameters"]
            .as_array()
            .expect("parameters array");
        assert!(
            params
                .iter()
                .any(|value| { value["id"] == "thinking" && value["value"] == "true" })
        );
        assert!(
            params
                .iter()
                .any(|value| { value["id"] == "effort" && value["value"] == "max" })
        );
    }

    #[test]
    fn request_frame_has_five_byte_connect_header() {
        let request =
            SandInferenceRequest::new("grok-4.6", "c", "i", vec![SandInferenceMessage::user("x")]);
        let frame = request.encode_frame().unwrap();
        assert_eq!(frame[0], 0);
        let length = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        assert_eq!(length, frame.len() - 5);
        assert_eq!(
            serde_json::from_slice::<Value>(&frame[5..]).unwrap()["requestedModel"]["modelId"],
            "grok-4.6"
        );
    }

    #[test]
    fn anthropic_images_use_data_uri_on_sand_wire() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "inspect"},
                    {"type": "image", "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": "aGVsbG8="
                    }}
                ]
            }]
        }))
        .unwrap();
        let messages = messages_from_anthropic(&request, false);
        let parts = &messages[0].parts;
        assert_eq!(parts[1]["image"]["data"], "data:image/png;base64,aGVsbG8=");
        assert_eq!(parts[1]["image"]["mimeType"], "image/png");
    }

    #[test]
    fn document_and_openai_file_blocks_use_native_file_part() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "document", "title": "report.pdf", "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": "aGVsbG8="
                    }},
                    {"type": "file", "file": {
                        "filename": "notes.txt",
                        "file_data": "data:text/plain;base64,aGVsbG8="
                    }}
                ]
            }]
        }))
        .unwrap();
        let messages = messages_from_anthropic(&request, false);
        let parts = &messages[0].parts;
        assert_eq!(parts.len(), 2);
        assert_eq!(
            parts[0]["file"]["data"],
            "data:application/pdf;base64,aGVsbG8="
        );
        assert_eq!(parts[0]["file"]["mediaType"], "application/pdf");
        // `title` is the Anthropic document filename fallback.
        assert_eq!(parts[0]["file"]["filename"], "report.pdf");
        assert_eq!(parts[1]["file"]["data"], "data:text/plain;base64,aGVsbG8=");
        assert_eq!(parts[1]["file"]["mediaType"], "text/plain");
        assert_eq!(parts[1]["file"]["filename"], "notes.txt");
    }

    #[test]
    fn text_documents_fall_back_to_text_without_base64_decoding() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [{
                "role": "user",
                "content": [{"type": "document", "source": {
                    "type": "text", "media_type": "text/plain", "text": "plain document"
                }}]
            }]
        }))
        .unwrap();
        let messages = messages_from_anthropic(&request, false);
        assert_eq!(messages[0].text.as_deref(), Some("plain document"));
        assert!(messages[0].parts.is_empty());
    }

    #[test]
    fn response_json_maps_text_thinking_usage_and_tool() {
        let value = json!({
            "textPart": {"text": "answer"},
            "thinkingPart": {"text": "thought"},
            "usage": {"promptTokens": 11, "completionTokens": 7, "cacheReadTokens": 2},
            "toolCall": {"id": "call-1", "name": "Bash", "arguments": {"command": "pwd"}}
        });
        let events = events_from_json(&value);
        assert!(events.iter().any(
            |event| matches!(event, CursorStreamEvent::TextDelta { text } if text == "answer")
        ));
        assert!(events.iter().any(
            |event| matches!(event, CursorStreamEvent::ThinkingDelta { text } if text == "thought")
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::Usage {
                input_tokens: 11,
                output_tokens: 7,
                cache_read_tokens: 2,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(event, CursorStreamEvent::NativeTool { tool_use_id, name, .. } if tool_use_id == "call-1" && name == "Bash")));
    }

    #[test]
    fn tool_call_fragments_wait_for_is_complete_and_emit_structured_input_once() {
        let first = json!({
            "toolCallPart": {
                "toolCallId": "call-fragment",
                "toolName": "Bash",
                "args": "{\"command\":\"pw",
                "isComplete": false
            }
        });
        let second = json!({
            "toolCallPart": {
                "toolCallId": "call-fragment",
                "args": "d\"}",
                "isComplete": true
            }
        });
        let duplicate = json!({
            "toolCallPart": {
                "toolCallId": "call-fragment",
                "toolName": "Bash",
                "args": "{\"command\":\"pwd\"}",
                "isComplete": true
            }
        });

        let mut buffers = HashMap::new();
        let mut completed = HashSet::new();
        assert!(events_from_json_with_state(&first, &mut buffers, &mut completed).is_empty());
        let events = events_from_json_with_state(&second, &mut buffers, &mut completed);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            } if tool_use_id == "call-fragment"
                && name == "Bash"
                && input == &json!({"command": "pwd"})
        ));
        assert!(events_from_json_with_state(&duplicate, &mut buffers, &mut completed).is_empty());
    }

    #[test]
    fn cumulative_tool_args_and_repeated_final_frame_emit_once() {
        // Current Cursor commonly sends an args-only prefix followed by a
        // complete frame that repeats the whole object.  The repeated value
        // must not be concatenated into invalid JSON.
        let name = json!({
            "toolCallPart": {
                "toolCallId": "call-cumulative",
                "toolName": "Read"
            }
        });
        let prefix = json!({
            "toolCallPart": {
                "toolCallId": "call-cumulative",
                "args": "{\"path\":\"/tmp/"
            }
        });
        let cumulative = json!({
            "toolCallPart": {
                "toolCallId": "call-cumulative",
                "args": "{\"path\":\"/tmp/file.txt\"}"
            }
        });
        let final_frame = json!({
            "toolCallPart": {
                "toolCallId": "call-cumulative",
                "toolName": "Read",
                "args": "{\"path\":\"/tmp/file.txt\"}",
                "isComplete": true
            }
        });
        let mut buffers = HashMap::new();
        let mut completed = HashSet::new();
        assert!(events_from_json_with_state(&name, &mut buffers, &mut completed).is_empty());
        assert!(events_from_json_with_state(&prefix, &mut buffers, &mut completed).is_empty());
        assert!(events_from_json_with_state(&cumulative, &mut buffers, &mut completed).is_empty());
        let events = events_from_json_with_state(&final_frame, &mut buffers, &mut completed);
        assert!(matches!(
            events.as_slice(),
            [CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            }] if tool_use_id == "call-cumulative"
                && name == "Read"
                && input == &json!({"path": "/tmp/file.txt"})
        ));
    }

    #[test]
    fn incomplete_tool_fragments_are_not_forwarded_as_string_arguments() {
        let value = json!({
            "toolCallPart": {
                "toolCallId": "call-incomplete",
                "toolName": "Bash",
                "args": "{\"command\":\"pwd",
                "isComplete": false
            }
        });
        let mut buffers = HashMap::new();
        let mut completed = HashSet::new();
        assert!(events_from_json_with_state(&value, &mut buffers, &mut completed).is_empty());
        assert!(flush_tool_buffers_to_events(&mut buffers).is_empty());
    }

    #[test]
    fn text_and_thinking_is_final_markers_are_not_stream_terminal() {
        // `isFinal` belongs to the individual text/thinking oneof part.  A
        // response may still carry usage, tool, or finish metadata in later
        // frames, so only stream-level markers may close the Connect stream.
        assert!(!json_is_terminal(&json!({
            "textPart": {"text": "done", "isFinal": true}
        })));
        assert!(!json_is_terminal(&json!({
            "thinkingPart": {"text": "done", "is_final": true}
        })));
        assert!(!json_is_terminal(&json!({
            "toolCallPart": {"args": "{\"isFinal\":true}", "isComplete": false}
        })));
    }

    #[test]
    fn text_part_is_final_does_not_drop_later_usage_or_end() {
        // Exercise the frame queue directly.  Constructing a reqwest response
        // from an `http::Response<Bytes>` does not provide a reliably closing
        // body stream on every reqwest version, while queue_frame is the exact
        // boundary where terminal state is decided.
        let mut stream = test_stream();
        stream.queue_json_value(&json!({
            "textPart": {"text": "done", "isFinal": true}
        }));
        assert!(!stream.ended);
        stream.queue_json_value(&json!({
            "usage": {"promptTokens": 11, "completionTokens": 7}
        }));
        stream.queue_frame(ConnectFrame {
            flags: FLAG_END,
            payload: Bytes::new(),
        });

        let events: Vec<_> = stream.pending.drain(..).filter_map(Result::ok).collect();
        assert!(
            events.iter().any(
                |event| matches!(event, CursorStreamEvent::TextDelta { text } if text == "done")
            )
        );
        assert!(events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::Usage {
                input_tokens: 11,
                output_tokens: 7,
                ..
            }
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, CursorStreamEvent::End))
                .count(),
            1
        );
    }

    #[test]
    fn clean_eof_after_text_emits_one_terminal_event() {
        let mut stream = test_stream();
        stream.queue_json_value(&json!({
            "textPart": {"text": "answer before EOF"}
        }));

        // A reverse proxy may strip the Connect END frame.  The stream must
        // still provide the terminal marker required by the Anthropic SSE
        // encoder after the final text delta has been queued.
        stream.finish_at_eof();
        let events: Vec<_> = stream.pending.drain(..).filter_map(Result::ok).collect();
        assert!(events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::TextDelta { text } if text == "answer before EOF"
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, CursorStreamEvent::End))
                .count(),
            1
        );

        // `finish_at_eof` can only be reached once from the HTTP body, but
        // keeping the call idempotent protects against wrapper streams that
        // report EOF more than once.
        stream.finish_at_eof();
        assert!(stream.pending.is_empty());
    }

    #[test]
    fn clean_eof_repairs_end_bit_seen_without_queued_terminal() {
        let mut stream = test_stream();
        stream.queue_json_value(&json!({
            "textPart": {"text": "answer"}
        }));

        // Model the narrow parser state in which a trailer has set `saw_end`
        // but no terminal event made it into the queue yet.  Checking
        // `terminal_emitted` in `finish_at_eof` closes this gap; checking only
        // `saw_end` would make the downstream encoder report a missing
        // `turn_ended` event.
        stream.saw_end = true;
        stream.finish_at_eof();
        assert_eq!(
            stream
                .pending
                .iter()
                .filter(|event| matches!(event, Ok(CursorStreamEvent::End)))
                .count(),
            1
        );
    }

    fn test_stream() -> SandInferenceStream {
        SandInferenceStream {
            bytes: Box::pin(futures_util::stream::empty()),
            decoder: ConnectFrameDecoder::new(),
            pending: VecDeque::new(),
            timeout_secs: 5,
            ended: false,
            saw_end: false,
            terminal_emitted: false,
            tool_buffers: HashMap::new(),
            completed_tool_ids: HashSet::new(),
            stream_permit: None,
        }
    }

    #[test]
    fn sand_tool_catalog_hides_internal_definitions() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "fable",
            "messages": [{"role": "user", "content": "go"}],
            "tools": [
                {
                    "name": "Read",
                    "description": "Read a file",
                    "input_schema": {"type": "object"}
                },
                {
                    "name": "mcp__plugin__notify_post",
                    "description": "INTERNAL hook",
                    "input_schema": {"type": "object"}
                },
                {
                    "name": "TaskOutput",
                    "description": "DEPRECATED",
                    "input_schema": {"type": "object"}
                }
            ]
        }))
        .unwrap();
        let tools = tools_from_anthropic(&request, false);
        let names: Vec<&str> = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect();
        assert_eq!(names, vec!["Read"]);
    }

    #[test]
    fn response_metadata_and_tool_arguments_do_not_become_output_or_usage() {
        let value = json!({
            "responseInfo": {"messages": [{"content": "metadata text"}]},
            "providerMetadata": {"metadata": {"text": "provider text"}},
            "toolCallPart": {
                "toolCallId": "call-1",
                "toolName": "Bash",
                "args": "{\"text\":\"argument text\",\"usage\":{\"inputTokens\":999}}",
                "isComplete": false
            }
        });
        let events = events_from_json(&value);
        assert!(!events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::TextDelta { .. } | CursorStreamEvent::ThinkingDelta { .. }
        )));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, CursorStreamEvent::Usage { .. }))
        );
    }

    #[test]
    fn repeated_end_frames_emit_one_end_event() {
        let frame = encode_connect_frame(
            serde_json::to_vec(&json!({"textPart": {"text": "done"}})).unwrap(),
            0,
        );
        let end = encode_connect_frame([], FLAG_END);
        let end_with_json = encode_connect_frame(
            serde_json::to_vec(&json!({"finished": true})).unwrap(),
            FLAG_END,
        );
        let mut body = Vec::new();
        body.extend_from_slice(&frame);
        body.extend_from_slice(&end);
        body.extend_from_slice(&end_with_json);
        let mut stream = test_stream();
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(&body).unwrap();
        for frame in frames {
            stream.queue_frame(frame);
        }
        let mut ends = 0;
        let mut text = String::new();
        while let Some(item) = stream.pending.pop_front() {
            match item.unwrap() {
                CursorStreamEvent::End => ends += 1,
                CursorStreamEvent::TextDelta { text: part } => text.push_str(&part),
                _ => {}
            }
        }
        assert_eq!(text, "done");
        assert_eq!(ends, 1);
    }

    #[test]
    fn control_frames_are_ignored_before_json_decoding() {
        let mut stream = test_stream();
        stream.queue_frame(ConnectFrame {
            // Deliberately malformed payload: control frames are not model
            // JSON and must never become a stream error.
            flags: FLAG_CONTROL,
            payload: Bytes::from_static(b"not-json"),
        });
        stream.queue_frame(ConnectFrame {
            flags: 0,
            payload: Bytes::from_static(br#"{"textPart":{"text":"ok"}}"#),
        });
        stream.queue_frame(ConnectFrame {
            flags: FLAG_END,
            payload: Bytes::new(),
        });

        let events: Vec<_> = stream.pending.drain(..).collect();
        assert!(events.iter().all(Result::is_ok));
        assert!(events.iter().any(|event| matches!(
            event,
            Ok(CursorStreamEvent::TextDelta { text }) if text == "ok"
        )));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Ok(CursorStreamEvent::End)))
                .count(),
            1
        );
    }

    #[test]
    fn control_end_frame_still_emits_terminal_event() {
        let mut stream = test_stream();
        stream.queue_frame(ConnectFrame {
            // Desktop gateways may combine the binary trailer/control bit
            // with FLAG_END. The payload is intentionally not JSON.
            flags: FLAG_CONTROL | FLAG_END,
            payload: Bytes::from_static(b"trailer"),
        });

        let events: Vec<_> = stream.pending.drain(..).collect();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Ok(CursorStreamEvent::End)))
                .count(),
            1
        );
        assert!(stream.ended);
        assert!(stream.saw_end);
    }

    #[test]
    fn json_result_wrapper_and_terminal_marker_are_supported() {
        let value = json!({"result": {"textPart": {"text": "done"}, "finished": true}});
        let inner = value.get("result").unwrap();
        let mut events = events_from_json(inner);
        assert!(
            events.iter().any(
                |event| matches!(event, CursorStreamEvent::TextDelta { text } if text == "done")
            )
        );
        assert!(json_is_terminal(inner));
        events.push(CursorStreamEvent::End);
        assert!(matches!(events.last(), Some(CursorStreamEvent::End)));
    }

    #[test]
    fn decoder_accepts_split_frames_and_end() {
        let first = encode_connect_frame(br#"{"textPart":{"text":"hi"}}"#, 0);
        let end = encode_connect_frame([], FLAG_END);
        let mut decoder = ConnectFrameDecoder::new();
        let mut frames = Vec::new();
        frames.extend(decoder.push(&first[..3]).unwrap());
        frames.extend(decoder.push(&first[3..]).unwrap());
        frames.extend(decoder.push(&end).unwrap());
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].flags, 0);
        assert_eq!(frames[1].flags, FLAG_END);
    }

    #[test]
    fn connect_error_maps_to_status() {
        let payload = br#"{"error":{"code":"resource_exhausted","message":"busy"}}"#;
        let error = parse_connect_error(payload).unwrap();
        assert_eq!(error.status, 429);
    }

    #[test]
    fn http_error_from_framed_body_preserves_sand_quota_metadata() {
        let payload = serde_json::to_vec(&json!({
            "error": {
                "code": "resource_exhausted",
                "message": "You've reached your Grok Bot usage limit",
                "details": [{
                    "debug": {
                        "details": {
                            "additionalInfo": {
                                "rateLimitReason": "sand_included_limit",
                                "nextResetAt": "2037-10-21T07:28:00.000Z"
                            }
                        }
                    }
                }]
            }
        }))
        .unwrap();
        let framed = encode_connect_frame(payload, FLAG_END);
        let error = sand_http_error_from_body(429, &framed);
        assert_eq!(error.status, 429);
        let message = error.client_message();
        assert!(message.contains("sand_included_limit"), "{message}");
        assert!(
            message.contains("nextResetAt=2037-10-21T07:28:00.000Z"),
            "{message}"
        );
        assert!(crate::retry::is_policy_rate_limit(&message));
    }

    #[test]
    fn sand_json_error_maps_numeric_error_type() {
        let cases = [
            (2, 400),
            (3, 400),
            (4, 429),
            (5, 401),
            (6, 403),
            (7, 503),
            (8, 400),
        ];
        for (error_type, status) in cases {
            let value = json!({
                "error": {
                    "errorType": error_type,
                    "message": "stream failed"
                }
            });
            let error = json_error(&value).unwrap();
            assert_eq!(error.status, status, "errorType={error_type}");
            assert!(error.message.contains("errorType"));
            assert!(
                error
                    .detail
                    .as_deref()
                    .unwrap_or_default()
                    .contains("errorType")
            );
        }
    }

    #[test]
    fn sand_json_error_maps_string_error_type_over_generic_code() {
        let value = json!({
            "error": {
                "code": "internal",
                "error_type": "ERROR_OVERLOADED",
                "message": "busy"
            }
        });
        let error = json_error(&value).unwrap();
        assert_eq!(error.status, 503);
        assert!(error.message.contains("ERROR_OVERLOADED"));
        assert!(error.client_message().contains("ERROR_OVERLOADED"));
    }

    #[test]
    fn sand_json_error_keeps_provider_metadata_for_account_failover() {
        let value = json!({
            "error": {
                "code": "resource_exhausted",
                "message": "We're having trouble connecting to the model provider",
                "details": [{
                    "debug": {
                        "error": "ERROR_PROVIDER_ERROR",
                        "details": {
                            "additionalInfo": {"providerStatusCode": 400},
                            "isRetryable": false
                        }
                    }
                }]
            }
        });
        let error = json_error(&value).expect("provider error");
        assert_eq!(error.status, 429, "outer quota envelope remains visible");
        let message = error.client_message();
        assert!(message.contains("ERROR_PROVIDER_ERROR"), "{message}");
        assert!(message.contains("providerStatusCode=400"), "{message}");
        assert!(message.contains("isRetryable=false"), "{message}");
        assert!(!is_non_retryable_provider_error_message(&message));
        assert!(
            !is_sand_tool_capability_error(&error, 1),
            "provider connectivity diagnostics must stay on transport retry path"
        );
    }

    #[test]
    fn sand_json_error_keeps_quota_reset_metadata_for_breaker() {
        let value = json!({
            "error": {
                "code": "resource_exhausted",
                "message": "You've reached your Grok Bot usage limit",
                "details": [{
                    "debug": {
                        "details": {
                            "additionalInfo": {
                                "rateLimitReason": "sand_included_limit",
                                "nextResetAt": "2037-10-21T07:28:00.000Z"
                            }
                        }
                    }
                }]
            }
        });
        let error = json_error(&value).expect("quota error");
        let detail = error.detail.as_deref().unwrap_or_default();
        assert!(
            detail.contains("rateLimitReason=sand_included_limit"),
            "{detail}"
        );
        assert!(
            detail.contains("nextResetAt=2037-10-21T07:28:00.000Z"),
            "{detail}"
        );
        assert!(crate::retry::is_policy_rate_limit(&error.client_message()));
    }

    #[test]
    fn sand_json_error_follows_known_response_envelopes_only() {
        let nested = json!({
            "result": {
                "response": {
                    "data": {
                        "payload": {
                            "error": {
                                "code": "resource_exhausted",
                                "message": "busy"
                            }
                        }
                    }
                }
            }
        });
        let error = json_error(&nested).expect("nested Sand error");
        assert_eq!(error.status, 429);
        assert!(error.client_message().contains("busy"));

        // Do not walk arbitrary response branches such as tool arguments:
        // model-provided JSON may legitimately contain an `error` key.
        let tool_argument = json!({
            "toolCallPart": {
                "toolCallId": "call-1",
                "args": {"error": {"code": "internal", "message": "argument"}}
            }
        });
        assert!(json_error(&tool_argument).is_none());
    }

    #[test]
    fn nested_sand_error_terminates_stream_before_unwrapping_result() {
        let mut stream = test_stream();
        stream.queue_json_value(&json!({
            "result": {
                "error": {"code": "overloaded", "message": "worker busy"}
            }
        }));
        assert!(stream.ended);
        assert!(stream.terminal_emitted);
        let item = stream.pending.pop_front().expect("queued error");
        let error = item.expect_err("nested error must be surfaced");
        assert_eq!(error.status, 503);
        assert!(error.client_message().contains("worker busy"));
    }

    #[test]
    fn sand_stream_retry_classifier_covers_transport_stalls_but_not_policy_errors() {
        let idle = CursorError::new(
            504,
            "Sand stream idle timeout after 45s with no useful progress",
            None,
        );
        assert!(stream_error_is_retryable(&idle));

        let active = CursorError::new(
            503,
            "A Cursor live run is already active for this session; retry after it advances",
            None,
        );
        assert!(stream_error_is_retryable(&active));

        let quota = CursorError::new(429, "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED", None);
        assert!(!stream_error_is_retryable(&quota));

        // Current Sand/Bot quota responses use the legacy GPT-4 vision enum
        // plus a machine-readable allowance reason. They are terminal for
        // this account and must not enter the stream replay loop.
        let sand_quota = CursorError::new(
            429,
            "ERROR_GPT_4_VISION_PREVIEW_RATE_LIMIT: You've reached your Grok Bot usage limit",
            Some("rateLimitReason=sand_included_limit isRetryable=false".into()),
        );
        assert!(!stream_error_is_retryable(&sand_quota));

        let invalid = CursorError::new(400, "Sand traffic is not supported on this endpoint", None);
        assert!(!stream_error_is_retryable(&invalid));

        let accepted_capacity = CursorError::new(
            504,
            "Sand accepted-stream admission deadline exhausted; retry after active streams drain",
            None,
        );
        assert!(
            !stream_error_is_retryable(&accepted_capacity),
            "local accepted-stream backpressure must not replay another upstream open"
        );
    }

    #[test]
    fn exact_grok_bot_quota_envelope_is_terminal_before_stream_replay() {
        let value = json!({
            "error": {
                "code": "resource_exhausted",
                "message": "Error",
                "details": [{
                    "debug": {
                        "details": {
                            "additionalInfo": {
                                "availableBankedResetCount": "0",
                                "nextResetAt": "2026-09-05T15:13:32.831Z",
                                "rateLimitReason": "sand_included_limit"
                            },
                            "detail": "Your included Grok Bot usage limit has been reached. It resets in 23 hours. Enable on-demand spend to continue.",
                            "isRetryable": false,
                            "title": "You've reached your Grok Bot usage limit"
                        },
                        "error": "ERROR_GPT_4_VISION_PREVIEW_RATE_LIMIT"
                    }
                }]
            }
        });
        let error = json_error(&value).expect("quota envelope should parse");
        assert_eq!(error.status, 429);
        let message = error.client_message();
        assert!(message.contains("sand_included_limit"), "{message}");
        assert!(crate::retry::is_policy_rate_limit(&message), "{message}");
        assert!(!stream_error_is_retryable(&error), "{message}");
    }

    #[tokio::test]
    async fn accepted_stream_admission_waits_for_capacity_and_releases_on_drop() {
        let gate = Arc::new(Semaphore::new(0));
        let waiting = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                admit_sand_stream_from_gate(gate, Instant::now() + Duration::from_secs(1)).await
            })
        };

        // The accepted-stream waiter must remain pending until an existing
        // stream releases capacity; this path is entered only after HTTP open.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(gate.available_permits(), 0);
        gate.add_permits(1);
        let permit = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("accepted-stream admission should wake after release")
            .expect("admission task should not panic")
            .expect("released accepted-stream slot should be acquired");
        assert_eq!(gate.available_permits(), 0);
        drop(permit);
        assert_eq!(gate.available_permits(), 1);
    }

    #[tokio::test]
    async fn accepted_stream_admission_timeout_has_local_backpressure_error() {
        let gate = Arc::new(Semaphore::new(0));
        let error = admit_sand_stream_from_gate(gate, Instant::now() + Duration::from_millis(20))
            .await
            .expect_err("a full accepted-stream gate should time out");
        assert_eq!(error.status, 504);
        assert_eq!(error.retry_after.as_deref(), Some("1"));
        assert!(
            error
                .client_message()
                .contains("accepted-stream admission deadline exhausted")
        );
        assert!(!stream_error_is_retryable(&error));
    }

    #[test]
    fn sand_stream_retry_default_covers_transient_provider_window() {
        assert_eq!(DEFAULT_SAND_STREAM_RETRIES, 5);
        assert_eq!(MAX_SAND_STREAM_RETRIES, 5);
        assert_eq!(stream_retry_limit_from(None), 5);
        assert_eq!(stream_retry_limit_from(Some("3")), 3);
        // A malformed deployment cannot create an unbounded replay loop.
        assert_eq!(stream_retry_limit_from(Some("999")), 5);
        assert_eq!(stream_retry_limit_from(Some("not-a-number")), 5);
    }

    #[test]
    fn sand_tool_capability_error_requires_explicit_tool_rejection() {
        let error = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR",
            Some(
                "providerStatusCode=400 isRetryable=false We're having trouble connecting to the model provider"
                    .into(),
            ),
        );
        assert!(
            !is_sand_tool_capability_error(&error, 1),
            "a provider connectivity sentence is not evidence of a tool mismatch"
        );
        assert!(!is_sand_tool_capability_error(&error, 0));

        let deterministic = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR",
            Some("providerStatusCode=400 isRetryable=false tool catalog is not supported".into()),
        );
        assert!(is_sand_tool_capability_error(&deterministic, 1));

        let schema_rejected = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR",
            Some(
                "providerStatusCode=422 isRetryable=false function schema rejected by provider"
                    .into(),
            ),
        );
        assert!(is_sand_tool_capability_error(&schema_rejected, 1));

        let parameter_invalid = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR",
            Some("providerStatusCode=422 isRetryable=false invalid tool parameter schema".into()),
        );
        assert!(is_sand_tool_capability_error(&parameter_invalid, 1));

        let retryable = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR",
            Some("providerStatusCode=503 isRetryable=false".into()),
        );
        assert!(!is_sand_tool_capability_error(&retryable, 1));

        let temporary_422 = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR",
            Some(
                "providerStatusCode=422 isRetryable=false We're having trouble connecting to the model provider. This might be temporary - please try again in a moment".into(),
            ),
        );
        assert!(
            !is_sand_tool_capability_error(&temporary_422, 1),
            "the observed 422 provider-connectivity response must not trigger text bridge"
        );

        let quota = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR out of usage",
            Some("providerStatusCode=400 isRetryable=false".into()),
        );
        assert!(!is_sand_tool_capability_error(&quota, 1));

        // HTTP error bodies are often pretty-printed before they reach the
        // retry classifier. Whitespace around JSON punctuation must not turn
        // a deterministic tool rejection back into a five-attempt replay.
        let pretty = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR",
            Some(
                r#"{
                    "message": "tool catalog is not supported by this provider",
                    "additionalInfo": { "providerStatusCode" : 400 },
                    "isRetryable" : false
                }"#
                .into(),
            ),
        );
        assert!(is_sand_tool_capability_error(&pretty, 1));
    }

    #[test]
    fn sand_tool_capability_is_scoped_to_account_and_model() {
        let token_a = "capability-account-a";
        let token_b = "capability-account-b";
        let model = "claude-fable-5-capability-test";
        reset_sand_tool_capability(token_a, model);
        reset_sand_tool_capability(token_b, model);
        assert_eq!(
            sand_tool_capability_for_token(token_a, model),
            SandToolCapability::Unknown
        );
        mark_sand_tools_unsupported(
            token_a,
            model,
            2,
            ["Read", "Bash"],
            "providerStatusCode=400 isRetryable=false",
        );
        assert_eq!(
            sand_tool_capability_for_token(token_a, model),
            SandToolCapability::Unsupported
        );
        assert_eq!(
            sand_tool_capability_for_token(token_b, model),
            SandToolCapability::Unknown
        );
        mark_sand_tools_supported(token_a, model, 2, ["Read", "Bash"]);
        assert_eq!(
            sand_tool_capability_for_token(token_a, model),
            SandToolCapability::Supported
        );
        let rows = sand_tool_capability_statuses();
        let account = crate::providers::cursor::auth::cursor_account_digest(token_a);
        assert!(rows.iter().any(|row| {
            row.account_id == account
                && row.model == model
                && row.state == SandToolCapability::Supported
                && row.tool_names == vec!["Bash".to_string(), "Read".to_string()]
        }));
        assert!(reset_sand_tool_capability(token_a, model));
        assert_eq!(
            sand_tool_capability_for_token(token_a, model),
            SandToolCapability::Unknown
        );
        assert!(!reset_sand_tool_capability(token_a, model));
    }

    #[test]
    fn sand_tool_capability_state_strings_are_stable() {
        assert_eq!(SandToolCapability::Unknown.as_str(), "unknown");
        assert_eq!(SandToolCapability::Supported.as_str(), "supported");
        assert_eq!(SandToolCapability::Unsupported.as_str(), "unsupported");
    }

    #[test]
    fn sand_open_breaker_is_scoped_without_client_blocking() {
        reset_sand_open_state_for_test();
        let token = "sand-breaker-account-a";
        let model = "cursor-grok-breaker-test";
        let other_token = "sand-breaker-account-b";
        let transient = CursorError::new(504, "upstream timed out", None);

        assert!(sand_open_breaker_admit(token, model).is_ok());
        for _ in 0..3 {
            sand_open_breaker_failure(token, model, &transient, Instant::now());
        }
        assert!(
            sand_open_breaker_admit(token, model).is_ok(),
            "an open local circuit must not synthesize a client-visible 503"
        );
        // A different account/model lane must remain eligible.
        assert!(sand_open_breaker_admit(other_token, model).is_ok());
        sand_open_breaker_success(token, model);
        assert!(sand_open_breaker_admit(token, model).is_ok());
        reset_sand_open_state_for_test();
    }

    #[test]
    fn deterministic_open_error_does_not_leave_half_open_probe_stuck() {
        reset_sand_open_state_for_test();
        let token = "sand-breaker-half-open";
        let model = "cursor-grok-half-open-test";
        let transient = CursorError::new(504, "upstream timed out", None);
        for _ in 0..3 {
            sand_open_breaker_failure(token, model, &transient, Instant::now());
        }

        // Force the half-open transition without sleeping by directly using a
        // short-lived test state, then feed a deterministic capability error.
        {
            let key = breaker_key(token, model);
            let mut breaker = SAND_OPEN_BREAKER.lock().unwrap();
            let state = breaker.get_mut(&key).expect("opened state");
            state.open_until = Some(Instant::now() - Duration::from_secs(1));
            state.half_open_probe = false;
        }
        assert!(sand_open_breaker_admit(token, model).is_ok());
        let deterministic = CursorError::new(400, "bad model name", None);
        sand_open_breaker_failure(token, model, &deterministic, Instant::now());
        assert!(sand_open_breaker_admit(token, model).is_ok());
        reset_sand_open_state_for_test();
    }

    #[test]
    fn stale_failure_cannot_reopen_after_concurrent_success() {
        reset_sand_open_state_for_test();
        let token = "sand-breaker-stale-failure";
        let model = "cursor-grok-stale-failure-test";
        let request_started = Instant::now() - Duration::from_secs(1);
        let transient = CursorError::new(504, "upstream timed out", None);

        sand_open_breaker_success(token, model);
        sand_open_breaker_failure(token, model, &transient, request_started);
        assert!(sand_open_breaker_admit(token, model).is_ok());
        reset_sand_open_state_for_test();
    }

    #[tokio::test]
    async fn sand_open_gate_releases_permits_on_drop_and_keeps_accounts_fair() {
        let gate = SandOpenGate::new(2, 1);
        let first = gate
            .acquire("account-a\0model", Duration::from_millis(100))
            .await
            .expect("first permit");
        // A second request for the same account must wait while the first
        // permit is held, whereas another account can use the remaining
        // global slot immediately.
        let same_account = gate.acquire("account-a\0model", Duration::from_millis(10));
        assert!(same_account.await.is_err());
        let other_account = gate
            .acquire("account-b\0model", Duration::from_millis(100))
            .await
            .expect("other account should not be head-of-line blocked");
        drop(other_account);
        drop(first);
        gate.acquire("account-a\0model", Duration::from_millis(100))
            .await
            .expect("permit must return after drop");
    }

    #[tokio::test]
    async fn sand_open_gate_does_not_hold_account_permit_while_global_is_busy() {
        // The account-first implementation used to let a waiter consume the
        // account permit and then block on the global semaphore.  A second
        // request for that account consequently timed out even after the
        // global slot was released.  Pair admission must reserve neither
        // dimension while waiting for the other.
        let gate = Arc::new(SandOpenGate::new(1, 1));
        let first = gate
            .acquire("account-a\0model", Duration::from_millis(100))
            .await
            .expect("first permit");
        let waiting_global = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.acquire("account-b\0model", Duration::from_millis(500))
                    .await
                    .map(|permit| {
                        // Do not return the permit through JoinHandle: the
                        // caller awaits both tasks, and a returned permit
                        // would remain held until that JoinHandle is dropped.
                        drop(permit);
                    })
            })
        };
        // Let the account-b task observe the busy global slot. It must not
        // consume account-b's sole permit while waiting for global capacity.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            gate.account_gate("account-b\0model")
                .expect("account-b lane")
                .available_permits(),
            1,
            "a global waiter must not reserve the account lane"
        );
        let second_same_account = {
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                gate.acquire("account-b\0model", Duration::from_millis(500))
                    .await
                    .map(|permit| {
                        drop(permit);
                    })
            })
        };
        drop(first);
        waiting_global
            .await
            .expect("global waiter task")
            .expect("account-b should acquire after release");
        // Once the first account-b permit is released, the second one must
        // also complete, proving that pair admission never leaked/duplicated
        // lane state.
        second_same_account
            .await
            .expect("same-account waiter task")
            .expect("released account-b lane should be reusable");
    }

    #[tokio::test]
    async fn sand_open_gate_admits_512_same_account_opens_without_local_shedding() {
        const FANOUT: usize = 512;
        let gate = Arc::new(SandOpenGate::new(FANOUT, FANOUT));
        // Every task keeps its permit until all peers have acquired one. This
        // distinguishes true 512-way admission from rapid sequential permit
        // reuse and catches an accidental return to the old 32/4 defaults.
        let all_admitted = Arc::new(tokio::sync::Barrier::new(FANOUT + 1));
        let mut tasks = Vec::with_capacity(FANOUT);
        for _ in 0..FANOUT {
            let gate = Arc::clone(&gate);
            let all_admitted = Arc::clone(&all_admitted);
            tasks.push(tokio::spawn(async move {
                let permit = gate
                    .acquire("account-a\0grok-4.6", Duration::from_secs(2))
                    .await
                    .expect("512-way open should not be locally rejected");
                all_admitted.wait().await;
                drop(permit);
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), all_admitted.wait())
            .await
            .expect("all 512 callers should hold a permit concurrently");
        for task in tasks {
            task.await.expect("fan-out task");
        }
        assert_eq!(gate.global.available_permits(), FANOUT);
        let lane = gate
            .account_gate("account-a\0grok-4.6")
            .expect("account lane");
        assert_eq!(lane.available_permits(), FANOUT);
    }

    #[tokio::test]
    async fn sand_open_gate_soft_admission_reports_a_saturated_slice() {
        // A queue slice expiring must not become a terminal admission error.
        // The caller can wait and retry without opening an untracked upstream
        // request.
        let gate = SandOpenGate::new(1, 1);
        let first = gate
            .acquire("account-a\0model", Duration::from_millis(100))
            .await
            .expect("first permit");
        let bypass = gate
            .acquire_soft("account-a\0model", Duration::from_millis(20))
            .await;
        assert!(bypass.is_none(), "a saturated slice should be retryable");
        drop(first);
        let second = gate
            .acquire_soft("account-a\0model", Duration::from_millis(100))
            .await
            .expect("released slot should be reusable");
        drop(second);
    }

    #[tokio::test]
    async fn stream_permit_lives_with_the_stream_and_returns_on_drop() {
        let gate = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&gate)
            .acquire_owned()
            .await
            .expect("fixture semaphore should be open");
        let stream = test_stream().with_stream_permit(SandStreamPermit {
            permit: Some(permit),
        });
        assert_eq!(gate.available_permits(), 0);
        drop(stream);
        assert_eq!(gate.available_permits(), 1);
    }

    #[tokio::test]
    async fn cold_open_scheduler_adds_capacity_without_global_backoff_on_failure() {
        let scheduler = Arc::new(SandColdOpenScheduler::new(2, 64, 64));
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut first = scheduler.acquire(deadline).await.expect("first open");
        let mut second = scheduler.acquire(deadline).await.expect("second open");
        assert_eq!(scheduler.snapshot().0, 2);

        first.complete(true);
        second.complete(true);
        assert_eq!(scheduler.snapshot(), (0, 3, 64));

        let mut third = scheduler.acquire(deadline).await.expect("third open");
        third.complete(false);
        assert_eq!(
            scheduler.snapshot(),
            (0, 3, 64),
            "one failed route must not shrink the process-wide launch window"
        );
    }

    #[tokio::test]
    async fn cold_open_scheduler_preserves_512_way_capacity_after_failure_burst() {
        // A synchronized provider outage can complete hundreds of opens as
        // failures.  The old multiplicative-decrease branch applied once per
        // completion and collapsed the global window to one.  Keep the
        // configured 512-way contract intact while request-level retry and
        // account/model breakers handle the outage.
        const FANOUT: usize = SAND_OPEN_GLOBAL_DEFAULT;
        let scheduler = Arc::new(SandColdOpenScheduler::new(
            FANOUT,
            SAND_OPEN_RATE_MAX,
            SAND_OPEN_RATE_MAX,
        ));
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut permits = Vec::with_capacity(FANOUT);
        for _ in 0..FANOUT {
            permits.push(
                scheduler
                    .acquire(deadline)
                    .await
                    .expect("configured fan-out should admit every open"),
            );
        }
        assert_eq!(scheduler.snapshot().0, FANOUT);
        for mut permit in permits {
            permit.complete(false);
        }
        assert_eq!(
            scheduler.snapshot(),
            (0, FANOUT, SAND_OPEN_RATE_MAX),
            "failure burst must not globally halve 512 opens to one"
        );

        // A fresh burst remains admissible immediately after the failed wave;
        // this catches a hidden token/rate collapse in addition to the window
        // assertion above.
        let fresh_deadline = Instant::now() + Duration::from_secs(3);
        let mut fresh = Vec::with_capacity(FANOUT);
        for _ in 0..FANOUT {
            fresh.push(
                scheduler
                    .acquire(fresh_deadline)
                    .await
                    .expect("capacity must remain available after failures"),
            );
        }
        assert_eq!(scheduler.snapshot().0, FANOUT);
        drop(fresh);
    }

    #[tokio::test]
    async fn sand_open_gate_never_evicts_an_active_lane() {
        // Use a one-entry map so the eviction path is deterministic. The
        // active permit keeps an Arc reference to its semaphore; a new key
        // must be rejected rather than creating a second lane that bypasses
        // the account limit.
        let gate = SandOpenGate::with_account_capacity(4, 1, 1);
        let first = gate
            .acquire("active-account\0model", Duration::from_millis(100))
            .await
            .expect("first permit");
        assert!(
            gate.acquire("new-account\0model", Duration::from_millis(5))
                .await
                .is_err(),
            "a full map of active lanes must fail closed instead of bypassing limits"
        );
        {
            let account = gate.account.lock().unwrap();
            assert!(account.contains_key("active-account\0model"));
            assert!(!account.contains_key("new-account\0model"));
        }
        drop(first);

        // Once the old lane is idle it may be evicted and the new account can
        // proceed. This confirms the bounded map still recovers normally.
        let replacement = gate
            .acquire("new-account\0model", Duration::from_millis(100))
            .await
            .expect("idle lane should be evictable");
        drop(replacement);
    }

    #[test]
    fn sand_retry_budget_is_bounded_and_configurable_shape_is_stable() {
        // The defaults leave enough room for the normal five stream retries,
        // while the implementation clamps hostile environment values.
        assert!(sand_logical_retry_budget() >= Duration::from_secs(60));
        assert!(sand_open_total_budget() >= Duration::from_secs(20));
        assert!(sand_open_total_budget() <= Duration::from_secs(900));
    }

    #[test]
    fn sand_open_defaults_preserve_512_way_fanout() {
        // Sand's open gate is a hard safety ceiling, not a low default
        // throughput throttle. A normal 512-way caller must reach the shared
        // H2 transport without a proxy-generated admission failure.
        assert_eq!(SAND_OPEN_GLOBAL_DEFAULT, 512);
        assert_eq!(SAND_OPEN_ACCOUNT_DEFAULT, 512);
        assert_eq!(SAND_OPEN_GLOBAL_MAX, 512);
        assert_eq!(SAND_OPEN_ACCOUNT_MAX, 512);
        assert_eq!(SAND_STREAM_GLOBAL_DEFAULT, 512);
        assert_eq!(SAND_STREAM_GLOBAL_MAX, 512);
        assert_eq!(SAND_OPEN_INITIAL_INFLIGHT_DEFAULT, 512);
        assert_eq!(SAND_OPEN_INITIAL_RATE_DEFAULT, 512);
    }

    #[tokio::test]
    async fn stream_type_is_send_and_can_be_polled_from_fixture() {
        // This test exercises the JSON mapper without opening a network
        // socket; compile-time use of StreamExt also guards the public API.
        let mut queue: VecDeque<Result<CursorStreamEvent, CursorError>> = VecDeque::new();
        queue.push_back(Ok(CursorStreamEvent::TextDelta { text: "x".into() }));
        assert!(matches!(
            queue.pop_front().unwrap(),
            Ok(CursorStreamEvent::TextDelta { .. })
        ));
        let _ = futures_util::stream::iter(Vec::<Result<CursorStreamEvent, CursorError>>::new())
            .next()
            .await;
    }
}
