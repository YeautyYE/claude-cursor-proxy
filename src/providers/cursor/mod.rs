pub mod auth;
pub mod catalog;
pub mod client;
pub mod connect;
pub mod conversation;
pub mod exec_results;
pub(crate) mod fetch_gate;
pub mod hosted_web_search;
pub mod http1;
pub(crate) mod identity;
pub mod live;
pub mod model;
pub mod native_tools;
pub(crate) mod operation_ledger;
pub mod proto;
pub mod request;
pub mod response;
pub mod sand_inference;
pub(crate) mod sand_operation;
pub mod sand_status;
pub mod sse;
#[cfg(test)]
pub(crate) mod test_frames;
pub mod tool_bridge;
pub mod tool_use_xml;
pub mod usage;

use async_trait::async_trait;
use axum::Json;
use axum::response::{IntoResponse, Response};
use futures_util::{FutureExt, StreamExt};
use http::StatusCode;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::anthropic::error::json_error;
use crate::anthropic::schema::{CountTokensResponse, MessagesRequest};
use crate::logging::create_logger;
use crate::monitor::usage_from_anthropic_sse;
use crate::provider::{CliHandlers, Provider, RequestContext};
use crate::providers::cursor::auth::{
    CursorAccountProfile, CursorAuthSelection, clear_cursor_auth, cursor_account_digest,
    expired_auth_message, force_refresh_cursor_auth, list_cursor_accounts, load_cursor_auth,
    load_cursor_auth_for_model, missing_auth_message, refresh_cursor_account_for_usage,
    run_cursor_login,
};
use crate::providers::cursor::client::{CursorError, CursorHttpClient, CursorRunOptions};
use crate::providers::cursor::connect::{
    cursor_connect_error_is_missing_image, is_non_retryable_provider_error_message,
    is_transient_provider_error_message,
};
use crate::providers::cursor::exec_results::PendingCursorExec;
use crate::providers::cursor::hosted_web_search::{
    extract_web_search_query, hosted_web_search_json_response, hosted_web_search_sse_response,
    is_hosted_web_search_request, maybe_handle_hosted_web_fetch, search_web,
};
use crate::providers::cursor::live::{
    CursorLiveRunHandle, EMPTY_TURN_CHECKPOINT_CONTINUE_PROMPT, EMPTY_TURN_RETRY_NOTE,
    LIVE_AMBIGUOUS_OPEN_TTL, LIVE_H2_OPEN_ATTEMPT, LiveEventResult, LiveReplacementClaim,
    LiveRunEvent, LiveRunIdentity, LiveRunProbe, LiveRunRegistry, LiveRunReservation,
    LiveSlotClaim, LiveStartRecovery, account_scoped_agent_id, cursor_error_is_kv_blob_overflow,
    cursor_start_error_is_same_request_retryable, exhausted_live_start_error,
    finish_replacement_after_cancel, live_error_is_agent_looping_detected,
    live_error_is_empty_turn_retry, live_error_is_kv_blob_overflow_replayable,
    live_error_is_same_request_retryable, live_error_is_upstream_already_active,
    live_error_needs_checkpoint_continue, live_pending_must_supersede,
    live_probe_error_blocks_new_run, live_request_fingerprint, live_resume_error_is_dead_driver,
    live_run_key_for, live_sse_response, live_start_error_seals_tombstone,
    local_overload_retry_after, same_request_retry_wait_ms,
};
use crate::providers::cursor::model::{
    anthropic_wire_model, resolve_cursor_model, resolve_sand_model_id,
};
use crate::providers::cursor::request::{
    CursorPromptOptions, CursorSelectedImage, claude_local_mcp_tools, current_user_blocks,
    cursor_request_context, is_reactive_compact_prompt, latest_user_is_only_tool_results,
    refresh_image_uuids, reject_orphaned_native_results_when_live_slot_is_free,
    render_cursor_prompt, render_cursor_prompt_parts_with, render_sand_text_tool_bridge_prompt,
    request_has_client_only_tool_results,
};
use crate::providers::cursor::response::{
    AnthropicJsonAcc, CursorDecodeError, CursorStreamEvent, decode_cursor_upstream_compaction,
    decode_cursor_upstream_with_allowed, decode_upstream_response_with_allowed,
    estimate_rendered_prompt_tokens, estimate_request_input_tokens,
};
use crate::providers::cursor::sand_inference::{
    SandInferenceClient, SandInferenceMessage, SandInferenceRequest, SandInferenceStream,
    accepted_unadvertised_tool_names_from_anthropic, admit_sand_open_until,
    admit_sand_stream_until, is_sand_tool_capability_error, mark_sand_tools_supported,
    mark_sand_tools_unsupported, messages_from_anthropic, sand_logical_retry_budget,
    sand_open_breaker_abort, sand_open_breaker_admit, sand_open_breaker_failure,
    sand_open_breaker_success, sand_open_total_budget, sand_tool_capability_client_error,
    sand_tool_capability_for_token, stream_error_is_retryable, stream_retry_limit,
    tools_from_anthropic,
};
use crate::providers::cursor::sand_operation::{
    SandOperationAdmission, SandOperationKey, SandOperationSubscription, admit_sand_operation,
};
use crate::providers::cursor::tool_bridge::{
    BridgeRegistry, advertised_tool_names, bridge_cursor_events_to_sse,
    can_bridge_cursor_native_tools, find_tool_result, resolve_advertised_name,
    start_cursor_tool_bridge,
};

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

// Sized for ~2000 concurrent live runs (4 windows x 512 agents): 16 shards
// keep per-connection H2 stream depth near typical server limits while
// bounding TLS handshakes. Tune with CCP_CURSOR_H2_SHARDS.
const CURSOR_HTTP_SHARDS_DEFAULT: usize = 16;
const CURSOR_HTTP_SHARDS_MAX: usize = 64;

/// Credential selection touches the account registry and can refresh a near-
/// expiry token through a blocking HTTP client.  Keep that work off Tokio's
/// request workers and cap the time a single request is willing to wait for a
/// locked Keychain/registry.  A timed-out blocking task is detached by Tokio;
/// the request itself fails closed and the next request can try again.
const CURSOR_AUTH_LOAD_TIMEOUT: Duration = Duration::from_secs(8);

/// A Sand turn can be replayed after a pre-output transport failure, but every
/// replay is a new InferenceService invocation. Keep independent budgets for
/// transport retries, account failover, and tool-catalog fallback so a burst
/// of deterministic quota errors cannot consume the only slot that would
/// have allowed a healthy account to take over.
const SAND_TOTAL_ATTEMPTS_DEFAULT: usize = 3;
const SAND_TOTAL_ATTEMPTS_MAX: usize = 8;
const SAND_ACCOUNT_FAILOVER_ATTEMPTS_DEFAULT: usize = 16;
const SAND_ACCOUNT_FAILOVER_ATTEMPTS_MAX: usize = 16;
const SAND_TOOL_FALLBACK_ATTEMPTS_DEFAULT: usize = 1;

fn sand_total_attempts() -> usize {
    std::env::var("CCP_CURSOR_SAND_TOTAL_ATTEMPTS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(SAND_TOTAL_ATTEMPTS_DEFAULT)
        .clamp(1, SAND_TOTAL_ATTEMPTS_MAX)
}

fn sand_account_failover_attempts() -> usize {
    std::env::var("CCP_CURSOR_SAND_ACCOUNT_FAILOVER_ATTEMPTS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(SAND_ACCOUNT_FAILOVER_ATTEMPTS_DEFAULT)
        .clamp(1, SAND_ACCOUNT_FAILOVER_ATTEMPTS_MAX)
}

#[derive(Debug, Clone, Copy)]
enum SandOpenAttemptKind {
    /// Initial opens and fresh transport replays share one logical-turn
    /// budget. Their retry count is governed by `CCP_CURSOR_SAND_TOTAL_ATTEMPTS`.
    Transport,
    /// A deterministic account-local error has already been observed. One
    /// fresh open is allowed per saved account, independent of transport
    /// retries consumed by the previous account.
    AccountFailover,
    /// One native-tool catalog fallback is allowed for the logical turn.
    ToolFallback,
}

#[derive(Debug, Clone)]
struct SandAttemptBudget {
    transport_remaining: Arc<AtomicUsize>,
    account_failover_remaining: Arc<AtomicUsize>,
    tool_fallback_remaining: Arc<AtomicUsize>,
}

impl SandAttemptBudget {
    fn new() -> Self {
        Self {
            transport_remaining: Arc::new(AtomicUsize::new(sand_total_attempts())),
            account_failover_remaining: Arc::new(
                AtomicUsize::new(sand_account_failover_attempts()),
            ),
            tool_fallback_remaining: Arc::new(AtomicUsize::new(
                SAND_TOOL_FALLBACK_ATTEMPTS_DEFAULT,
            )),
        }
    }

    /// Consume one slot immediately before an upstream invocation. Waiting
    /// for local admission does not consume budget; an actual HTTP open does.
    fn try_consume(&self, kind: SandOpenAttemptKind) -> bool {
        let remaining = match kind {
            SandOpenAttemptKind::Transport => &self.transport_remaining,
            SandOpenAttemptKind::AccountFailover => &self.account_failover_remaining,
            SandOpenAttemptKind::ToolFallback => &self.tool_fallback_remaining,
        };
        let mut current = remaining.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return false;
            }
            match remaining.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }
}

fn sand_attempt_budget_exhausted(error: &CursorError) -> bool {
    error
        .client_message()
        .to_ascii_lowercase()
        .contains("sand upstream attempt budget exhausted")
}

/// Bound the process-local policy breaker so a long-lived proxy cannot retain
/// one entry forever for every historical model/account combination. Expired
/// entries are swept on every read/write; this cap covers a burst of account
/// rotations before the sweep gets a chance to run.
const POLICY_RATE_LIMIT_BREAKER_MAX_ENTRIES: usize = 1024;
/// A cold account/model key is single-flighted long enough to cover Cursor's
/// delayed policy decisions. Production traces put the median policy 429 near
/// 6s and have observed it after 16s; the former 1.5s window released most
/// retry waves before the decisive error arrived. A useful model/tool/End
/// event still marks the key healthy immediately, so healthy traffic normally
/// pays only the first Run's time-to-first-useful-event rather than this cap.
const POLICY_RATE_LIMIT_PROBE_WINDOW_DEFAULT_MS: u64 = 30_000;
const POLICY_RATE_LIMIT_PROBE_WINDOW_MIN_MS: u64 = 25;
const POLICY_RATE_LIMIT_PROBE_WINDOW_MAX_MS: u64 = 120_000;

/// The two Cursor allowance buckets exposed by the dashboard.  The labels are
/// intentionally aligned with the TUI so a policy 429 can be diagnosed from
/// logs without inspecting a request payload or bearer credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorQuotaLane {
    CliApi,
    SandBot,
}

impl CursorQuotaLane {
    fn as_str(self) -> &'static str {
        match self {
            Self::CliApi => "CLI/API",
            Self::SandBot => "Sand/Bot",
        }
    }
}

/// Map the effective Cursor transport to the dashboard allowance bucket.  A
/// missing/unknown client type follows the historical AgentService path and
/// therefore belongs to the named-model CLI/API meter.
fn quota_lane_for_client_type(client_type: &str) -> CursorQuotaLane {
    if client_type.trim().eq_ignore_ascii_case("sand") {
        CursorQuotaLane::SandBot
    } else {
        CursorQuotaLane::CliApi
    }
}

/// Keep account identifiers useful for correlating logs while avoiding a full
/// bearer-derived digest (or a hand-authored account id) in every line.  IDs
/// shorter than the display budget are retained verbatim; long IDs keep both
/// ends so adjacent account rows remain easy to distinguish.
fn truncate_account_id(account_id: &str) -> String {
    const PREFIX_CHARS: usize = 8;
    const SUFFIX_CHARS: usize = 4;
    let account_id = account_id.trim();
    if account_id.is_empty() {
        return "unknown".to_string();
    }
    let chars: Vec<char> = account_id.chars().collect();
    let keep = PREFIX_CHARS + SUFFIX_CHARS;
    if chars.len() <= keep {
        return account_id.to_string();
    }
    let prefix: String = chars[..PREFIX_CHARS].iter().collect();
    let suffix: String = chars[chars.len() - SUFFIX_CHARS..].iter().collect();
    format!("{prefix}…{suffix}")
}

/// Build account/route/quota diagnostics shared by account-selection and
/// policy-limit events.  The raw token is deliberately accepted only to look
/// up in-memory evidence; it is never inserted into the returned fields.
fn quota_diagnostic_fields(
    account_id: &str,
    token: &str,
    model: &str,
    client_type: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let lane = quota_lane_for_client_type(client_type);
    let mut fields = serde_json::Map::from_iter([
        (
            "accountId".to_string(),
            serde_json::json!(truncate_account_id(account_id)),
        ),
        ("model".to_string(), serde_json::json!(model)),
        ("clientType".to_string(), serde_json::json!(client_type)),
        ("quotaLane".to_string(), serde_json::json!(lane.as_str())),
    ]);

    // Include both meters when available.  `quotaPercent` is the meter that
    // corresponds to the selected lane and makes the diagnosis obvious even
    // when the other bucket happens to be exhausted as well.
    let api_percent = crate::providers::cursor::usage::cached_api_usage_evidence(token)
        .map(|evidence| evidence.usage_percent);
    let bot_percent = crate::providers::cursor::usage::cached_sand_usage_evidence(token)
        .map(|evidence| evidence.usage_percent);
    if let Some(value) = api_percent {
        fields.insert("apiPercent".to_string(), serde_json::json!(value));
    }
    if let Some(value) = bot_percent {
        fields.insert("grokBotPercent".to_string(), serde_json::json!(value));
    }
    let selected_percent = match lane {
        CursorQuotaLane::CliApi => api_percent,
        CursorQuotaLane::SandBot => bot_percent,
    };
    if let Some(value) = selected_percent {
        fields.insert("quotaPercent".to_string(), serde_json::json!(value));
    }
    fields
}

// Cursor account/model policy 429s are deterministic for the current login.
// Without a local breaker, a Claude Code retry wave can open hundreds of
// identical Runs before the first response reaches the client; each rejected
// start then consumes a connection and turns into a visible 503/429 storm.
// Keep the breaker keyed by a stable one-way account digest and request-scoped
// client route. Access-token refreshes retain the cooldown, while a hot account
// switch or Sand/CLI route switch remains immediately eligible.
#[derive(Clone, Debug)]
struct PolicyRateLimitState {
    until: Instant,
    retry_after_secs: u64,
    message: String,
}

static POLICY_RATE_LIMIT_BREAKER: LazyLock<Mutex<HashMap<String, PolicyRateLimitState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// Sand account routes can fail independently of the subscription-wide policy
// meter. Keep their short-lived cooldowns in a *separate* keyed store: putting
// an admission/auth route failure in POLICY_RATE_LIMIT_BREAKER would make
// policy_rate_limit_preflight synthesize a client-visible 429 for a healthy
// single-account login merely because one lane was briefly saturated. The
// route map is process-local and expires automatically; no account is removed
// from the persistent registry as a side effect of a transient response.
#[derive(Clone, Debug)]
struct SandRouteCooldownState {
    until: Instant,
}

static SAND_ROUTE_COOLDOWNS: LazyLock<Mutex<HashMap<String, SandRouteCooldownState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const SAND_ROUTE_ADMISSION_COOLDOWN_SECS: u64 = 15;
const SAND_ROUTE_AUTH_COOLDOWN_SECS: u64 = 15 * 60;
static SAND_ACCOUNT_FAILOVER_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// Cold/half-open account+model keys use bounded single-flight. This closes
/// the gap where a large retry wave could pass an empty breaker: actual output
/// releases the wave, while a quiet key ramps by only one probe per window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyRateLimitProbeState {
    Unknown,
    Probing { lease: u64, started: Instant },
    Healthy,
}

#[derive(Debug)]
struct PolicyRateLimitProbeGateState {
    /// Incremented whenever a policy result opens the breaker. Probes from a
    /// previous epoch may finish later, but must not mark a post-cooldown key
    /// healthy and bypass its next half-open probe.
    epoch: u64,
    phase: PolicyRateLimitProbeState,
}

#[derive(Debug)]
struct PolicyRateLimitProbeGate {
    state: Mutex<PolicyRateLimitProbeGateState>,
    changed: tokio::sync::Notify,
}

impl PolicyRateLimitProbeGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(PolicyRateLimitProbeGateState {
                epoch: 0,
                phase: PolicyRateLimitProbeState::Unknown,
            }),
            changed: tokio::sync::Notify::new(),
        }
    }

    fn reset_after_policy_limit(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.epoch = state.epoch.wrapping_add(1);
        state.phase = PolicyRateLimitProbeState::Unknown;
        drop(state);
        self.changed.notify_waiters();
    }
}

static POLICY_RATE_LIMIT_PROBE_GATES: LazyLock<
    Mutex<HashMap<String, Arc<PolicyRateLimitProbeGate>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));
static POLICY_RATE_LIMIT_PROBE_SEQUENCE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

#[derive(Debug)]
struct PolicyRateLimitProbeLease {
    gate: Arc<PolicyRateLimitProbeGate>,
    epoch: u64,
    lease: u64,
    started: Instant,
    active: bool,
}

impl PolicyRateLimitProbeLease {
    fn remaining_until(&self, window: Duration) -> Duration {
        window.saturating_sub(self.started.elapsed())
    }

    fn mark_healthy(mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // A quiet key may have admitted a newer bounded probe by the time an
        // older Run emits its first useful event. Same-epoch evidence can
        // release the whole wave. A probe predating a policy result cannot:
        // after cooldown it must leave the key half-open for a fresh probe.
        if state.epoch == self.epoch {
            state.phase = PolicyRateLimitProbeState::Healthy;
        }
        self.active = false;
        drop(state);
        self.gate.changed.notify_waiters();
    }
}

impl Drop for PolicyRateLimitProbeLease {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if state.epoch == self.epoch
            && matches!(
                state.phase,
                PolicyRateLimitProbeState::Probing { lease, .. } if lease == self.lease
            )
        {
            state.phase = PolicyRateLimitProbeState::Unknown;
        }
        drop(state);
        self.gate.changed.notify_waiters();
    }
}

#[derive(Debug)]
enum PolicyRateLimitAdmission {
    KnownHealthy,
    Probe(PolicyRateLimitProbeLease),
}

impl PolicyRateLimitAdmission {
    fn mark_healthy(self) {
        if let Self::Probe(lease) = self {
            lease.mark_healthy();
        }
    }

    fn into_probe(self) -> Option<PolicyRateLimitProbeLease> {
        match self {
            Self::KnownHealthy => None,
            Self::Probe(lease) => Some(lease),
        }
    }

    /// Publish the breaker before releasing a cold-probe lease. Reversing
    /// this order briefly changes the gate to Unknown and wakes a waiter that
    /// can dispatch another upstream Run before the policy 429 is visible.
    fn mark_policy_limited(
        self,
        model: &str,
        client_type: &str,
        token: &str,
        message: &str,
        retry_after: Option<&str>,
    ) {
        note_policy_rate_limit(model, client_type, token, message, retry_after);
        drop(self);
    }
}

fn policy_rate_limit_key(model: &str, client_type: &str, token: &str) -> String {
    let route = match client_type.trim().to_ascii_lowercase() {
        route if !route.is_empty() => route,
        _ => "cli".to_string(),
    };
    // Sand's InferenceService keys account state by the family id while the
    // Agent catalog/CLI route carries an effort suffix (for example
    // `cursor-grok-4.6-xhigh-fast`). Keep policy cooldowns on the same key as
    // the Sand open gate; otherwise a 429/closed-account observation on one
    // spelling is invisible to the other and the retry wave hammers it again.
    let resolved_model = if route.eq_ignore_ascii_case("sand") {
        resolve_sand_model_id(model)
    } else {
        resolve_cursor_model(model)
            .map(|resolved| resolved.model_id)
            .unwrap_or_else(|_| model.trim().to_ascii_lowercase())
    };
    format!("{route}:{resolved_model}:{}", cursor_account_digest(token))
}

fn policy_rate_limit_probe_gate(key: &str) -> Arc<PolicyRateLimitProbeGate> {
    let mut gates = POLICY_RATE_LIMIT_PROBE_GATES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    if let Some(gate) = gates.get(key) {
        return Arc::clone(gate);
    }
    if gates.len() >= POLICY_RATE_LIMIT_BREAKER_MAX_ENTRIES {
        let removable = gates
            .iter()
            .find_map(|(key, gate)| (Arc::strong_count(gate) == 1).then(|| key.clone()));
        if let Some(removable) = removable {
            gates.remove(&removable);
        }
    }
    let gate = Arc::new(PolicyRateLimitProbeGate::new());
    gates.insert(key.to_string(), Arc::clone(&gate));
    gate
}

fn policy_rate_limit_probe_window() -> Duration {
    Duration::from_millis(
        std::env::var("CCP_CURSOR_POLICY_429_PROBE_WINDOW_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(POLICY_RATE_LIMIT_PROBE_WINDOW_DEFAULT_MS)
            .clamp(
                POLICY_RATE_LIMIT_PROBE_WINDOW_MIN_MS,
                POLICY_RATE_LIMIT_PROBE_WINDOW_MAX_MS,
            ),
    )
}

async fn policy_rate_limit_admit_fresh_open(
    model: &str,
    client_type: &str,
    token: &str,
) -> Result<PolicyRateLimitAdmission, CursorError> {
    policy_rate_limit_admit_fresh_open_with_window(
        model,
        client_type,
        token,
        policy_rate_limit_probe_window(),
    )
    .await
}

async fn policy_rate_limit_admit_fresh_open_with_window(
    model: &str,
    client_type: &str,
    token: &str,
    probe_window: Duration,
) -> Result<PolicyRateLimitAdmission, CursorError> {
    let key = policy_rate_limit_key(model, client_type, token);
    let gate = policy_rate_limit_probe_gate(&key);
    // Sand/Bot policy decisions are delivered by the full-history
    // InferenceService stream and can arrive well after the short live-start
    // peek.  A second Sand open while the first one is still active is not a
    // harmless health probe: it consumes another allowance and may produce a
    // duplicate 429 several minutes later.  The Sand stream owner already
    // releases this lease on a decisive event, terminal error, or its bounded
    // logical deadline, so keep the probe single-flight for the whole owner
    // lifetime.  CLI/Agent keeps the quiet-window ramp because its live probe
    // path does not retain the lease through the complete stream.
    let sand_probe = client_type.trim().eq_ignore_ascii_case("sand");
    loop {
        policy_rate_limit_preflight(model, client_type, token)?;
        enum ProbeDecision {
            Healthy,
            Acquire {
                epoch: u64,
                lease: u64,
                started: Instant,
            },
            Wait(Duration),
        }
        let decision = {
            let mut state = gate
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            match state.phase {
                PolicyRateLimitProbeState::Healthy => ProbeDecision::Healthy,
                PolicyRateLimitProbeState::Unknown => {
                    let lease = POLICY_RATE_LIMIT_PROBE_SEQUENCE
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let started = Instant::now();
                    state.phase = PolicyRateLimitProbeState::Probing { lease, started };
                    ProbeDecision::Acquire {
                        epoch: state.epoch,
                        lease,
                        started,
                    }
                }
                PolicyRateLimitProbeState::Probing { started, .. } => {
                    let elapsed = started.elapsed();
                    if !sand_probe && elapsed >= probe_window {
                        // The current probe remained quiet for one window. Do
                        // not call the key healthy and release the whole retry
                        // wave: delayed policy decisions have arrived well
                        // after 30s. Admit exactly one additional probe and
                        // rotate the lease; older probes keep observing their
                        // Runs and may still prove health or open the breaker.
                        let lease = POLICY_RATE_LIMIT_PROBE_SEQUENCE
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let started = Instant::now();
                        state.phase = PolicyRateLimitProbeState::Probing { lease, started };
                        ProbeDecision::Acquire {
                            epoch: state.epoch,
                            lease,
                            started,
                        }
                    } else {
                        // For Sand, wait for the owner to settle the lease.
                        // A bounded wake/timer keeps cancellation and stale
                        // state recoverable without opening another upstream
                        // request after the probe window has elapsed.
                        let remaining = if sand_probe {
                            Duration::from_millis(250)
                        } else {
                            probe_window - elapsed
                        };
                        ProbeDecision::Wait(remaining)
                    }
                }
            }
        };
        match decision {
            ProbeDecision::Healthy => {
                // Close the small race with a policy result published between
                // the first breaker read and the gate-state read.
                policy_rate_limit_preflight(model, client_type, token)?;
                return Ok(PolicyRateLimitAdmission::KnownHealthy);
            }
            ProbeDecision::Acquire {
                epoch,
                lease,
                started,
            } => {
                // A concurrent policy result may have won immediately after
                // the first read. Do not let this newly acquired lease pass it.
                if let Err(error) = policy_rate_limit_preflight(model, client_type, token) {
                    drop(PolicyRateLimitProbeLease {
                        gate: Arc::clone(&gate),
                        epoch,
                        lease,
                        started,
                        active: true,
                    });
                    return Err(error);
                }
                return Ok(PolicyRateLimitAdmission::Probe(PolicyRateLimitProbeLease {
                    gate: Arc::clone(&gate),
                    epoch,
                    lease,
                    started,
                    active: true,
                }));
            }
            ProbeDecision::Wait(remaining) => {
                // Result publication wakes the coalesced wave. Short sleeps
                // let one waiter rotate the probe at the window boundary;
                // every other waiter observes that new lease and remains
                // queued instead of fanning out.
                tokio::select! {
                    _ = gate.changed.notified() => {}
                    // The notify path handles normal lease settlement.  Keep
                    // a coarse timer only as a lost-wakeup/cancellation
                    // fallback; a 25ms poll across a 512-way retry wave
                    // creates avoidable mutex/CPU pressure while the owner
                    // is waiting on a delayed Sand response.
                    _ = tokio::time::sleep(remaining.min(Duration::from_millis(250))) => {}
                }
            }
        }
    }
}

fn retry_after_delta_secs(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    if let Ok(seconds) = raw.parse::<u64>() {
        return Some(seconds);
    }
    // Retry-After is normally an integer or RFC 2822 HTTP-date.  Cursor's
    // Sand allowance envelope also exposes an RFC 3339 `nextResetAt`; accept
    // both formats through the same delta helper so a quota breaker can stay
    // closed until the account's actual reset instead of probing every 30s.
    let deadline = time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
        .or_else(|_| {
            time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc2822)
        })
        .ok()?;
    Some(
        (deadline - time::OffsetDateTime::now_utc())
            .whole_seconds()
            .max(0) as u64,
    )
}

/// Extract Cursor's account-local quota reset hint from a flattened error
/// detail.  Sand currently sends `nextResetAt` inside a nested JSON
/// `additionalInfo` object; later Connect/SSE layers flatten the payload into
/// `nextResetAt=...`, so support either naming/casing and common separators.
fn next_reset_at_delta_secs(message: &str) -> Option<u64> {
    let lower = message.to_ascii_lowercase();
    for marker in ["nextresetat", "next_reset_at"] {
        let mut offset = 0usize;
        while let Some(relative) = lower[offset..].find(marker) {
            let start = offset + relative + marker.len();
            let tail = message[start..].trim_start_matches(|ch: char| {
                matches!(ch, '"' | '\'' | ':' | '=' | ',' | ' ' | '\t')
            });
            let value = tail
                .chars()
                .take_while(|ch| {
                    !matches!(ch, '"' | '\'' | ',' | '}' | ']' | ';' | ' ' | '\t' | '\n')
                })
                .collect::<String>();
            if !value.is_empty()
                && let Some(delta) = retry_after_delta_secs(&value)
            {
                return Some(delta);
            }
            offset = start;
        }
    }
    None
}

fn policy_rate_limit_cooldown_secs(message: &str, retry_after: Option<&str>) -> u64 {
    // Cursor usually omits Retry-After from Connect END frames. Accept a
    // small set of human-readable hints when present, otherwise use a short
    // local cooldown that smooths retries without making account changes feel
    // sticky. The value is deliberately bounded so a stale error cannot brick
    // a model for hours.
    // A Sand/Bot allowance response carries an account-local reset timestamp.
    // Prefer it over a generic Retry-After/header and allow a longer bounded
    // hold, otherwise the exhausted account is re-probed every few seconds
    // for the entire reset window and produces another 429 wave.
    if let Some(next_reset) = next_reset_at_delta_secs(message) {
        const MAX_QUOTA_RESET_COOLDOWN_SECS: u64 = 7 * 24 * 60 * 60;
        return next_reset.clamp(5, MAX_QUOTA_RESET_COOLDOWN_SECS);
    }
    let lower = message.to_ascii_lowercase();
    let message_hint = ["retry after", "try again in", "wait "]
        .iter()
        .find_map(|marker| {
            let start = lower.find(marker)? + marker.len();
            let tail = lower[start..].trim_start_matches(|ch: char| !ch.is_ascii_digit());
            let digits: String = tail.chars().take_while(|ch| ch.is_ascii_digit()).collect();
            let amount = digits.parse::<u64>().ok()?;
            let unit = tail[digits.len()..].trim_start();
            Some(if unit.starts_with("minute") {
                amount.saturating_mul(60)
            } else {
                amount
            })
        });
    retry_after
        .and_then(retry_after_delta_secs)
        .or(message_hint)
        .or_else(|| {
            std::env::var("CCP_CURSOR_POLICY_429_COOLDOWN_SECS")
                .ok()
                .and_then(|raw| raw.trim().parse::<u64>().ok())
        })
        .unwrap_or(30)
        .clamp(5, 600)
}

fn note_policy_rate_limit(
    model: &str,
    client_type: &str,
    token: &str,
    message: &str,
    retry_after: Option<&str>,
) {
    if !crate::retry::is_policy_rate_limit(message)
        && !is_non_retryable_provider_error_message(message)
    {
        return;
    }
    let retry_after_secs = policy_rate_limit_cooldown_secs(message, retry_after);
    let key = policy_rate_limit_key(model, client_type, token);
    // This helper is called directly from async stream/error paths, often by
    // hundreds of concurrent retries.  Do not take the account-registry file
    // lock here: `cursor_account_key_for_token` performs synchronous I/O and
    // can stall every Tokio worker while a 503 wave is being handled.  The
    // stable bearer digest is sufficient for correlation and never exposes a
    // credential; selection logs retain the persisted profile id separately.
    let account_id = cursor_account_digest(token);
    let now = Instant::now();
    let state = PolicyRateLimitState {
        until: now + Duration::from_secs(retry_after_secs),
        retry_after_secs,
        message: message.to_string(),
    };
    {
        let mut breaker = POLICY_RATE_LIMIT_BREAKER
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // Prune all expired keys, not only the key being opened. Account
        // switches can otherwise leave a stale digest per model in memory for
        // the lifetime of the process.
        breaker.retain(|_, previous| previous.until > now);
        if let Some(previous) = breaker.get_mut(&key) {
            // Extend an existing window, never shorten it during a burst.
            if state.until > previous.until {
                *previous = state;
            }
        } else {
            if breaker.len() >= POLICY_RATE_LIMIT_BREAKER_MAX_ENTRIES {
                // Remove the entry with the nearest expiry first. This keeps
                // the longest-lived cooldowns while making room for the
                // current account/model that just produced a policy error.
                let oldest = breaker
                    .iter()
                    .min_by_key(|(_, previous)| previous.until)
                    .map(|(key, _)| key.clone());
                if let Some(oldest) = oldest {
                    breaker.remove(&oldest);
                }
            }
            breaker.insert(key, state);
        }
    }
    if let Some(gate) = POLICY_RATE_LIMIT_PROBE_GATES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .get(&policy_rate_limit_key(model, client_type, token))
        .cloned()
    {
        gate.reset_after_policy_limit();
    }
    let mut fields = quota_diagnostic_fields(&account_id, token, model, client_type);
    fields.insert(
        "retryAfterSecs".to_string(),
        serde_json::json!(retry_after_secs),
    );
    create_logger("cursor").warn("policy_rate_limit_breaker_open", Some(fields));
}

/// Return whether a Sand account/model route is in its short-lived transport
/// cooldown. This deliberately does not consult the policy breaker: route
/// pressure and subscription allowance are independent signals.
fn sand_account_route_is_cooled(model: &str, token: &str) -> bool {
    let key = policy_rate_limit_key(model, "sand", token);
    let now = Instant::now();
    let mut routes = SAND_ROUTE_COOLDOWNS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    routes.retain(|_, state| state.until > now);
    routes.get(&key).is_some_and(|state| state.until > now)
}

/// Mark one Sand account/model route unavailable for a bounded interval.
///
/// A subsequent unbound request sees the route as cooled and immediately tries
/// another saved account instead of waiting behind the same stalled lane. The
/// route marker is intentionally independent from subscription policy state;
/// a longer existing route cooldown is never shortened by a shorter admission
/// observation.
fn note_sand_account_route_cooldown(model: &str, token: &str, reason: &str, cooldown: Duration) {
    let key = policy_rate_limit_key(model, "sand", token);
    let now = Instant::now();
    let state = SandRouteCooldownState {
        until: now + cooldown,
    };
    {
        let mut routes = SAND_ROUTE_COOLDOWNS
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        routes.retain(|_, previous| previous.until > now);
        match routes.get_mut(&key) {
            Some(previous) if previous.until >= state.until => {}
            Some(previous) => *previous = state,
            None => {
                if routes.len() >= POLICY_RATE_LIMIT_BREAKER_MAX_ENTRIES {
                    if let Some(oldest) = routes
                        .iter()
                        .min_by_key(|(_, previous)| previous.until)
                        .map(|(key, _)| key.clone())
                    {
                        routes.remove(&oldest);
                    }
                }
                routes.insert(key.clone(), state);
            }
        }
    }
    // This helper runs on the async Sand stream/error path and can be called
    // by hundreds of concurrent retries. Do not consult the on-disk account
    // registry here: `list_cursor_accounts()` may wait on an inter-process
    // lock and would block a Tokio worker while a 503 wave is being logged.
    // The bearer digest is stable for the lifetime of this route and is the
    // same fallback used for environment-backed credentials.
    let account_id = cursor_account_digest(token);
    let mut fields = quota_diagnostic_fields(&account_id, token, model, "sand");
    fields.insert("reason".to_string(), serde_json::json!(reason));
    fields.insert(
        "cooldownSecs".to_string(),
        serde_json::json!(cooldown.as_secs().max(1)),
    );
    create_logger("cursor").warn("sand_account_route_cooldown", Some(fields));
}

/// Return whether a structured Cursor error represents an account-scoped
/// policy result.  The useful marker can live in any `CursorError` field, so
/// keep this decision in one helper instead of letting the initial-open and
/// late-stream failover paths drift apart.
fn cursor_error_is_account_failover_policy(error: &CursorError) -> bool {
    is_account_failover_policy_error(&error.client_message())
        || is_account_failover_policy_error(&error.message)
        || error
            .detail
            .as_deref()
            .is_some_and(is_account_failover_policy_error)
}

/// Whether an error should open the Sand allowance breaker. Generic provider
/// 4xx diagnostics are intentionally excluded: malformed tool schemas,
/// unsupported parameters, and context validation failures are request-local
/// and must not poison the account or trigger account rotation.
fn cursor_error_is_policy_limited(error: &CursorError) -> bool {
    // Tool/schema capability rejection is request-local even when Cursor
    // wraps it in `resource_exhausted`/429.  Keep this guard here as a final
    // backstop for every live path that settles a probe; the Sand-specific
    // callers also short-circuit before settlement so the fallback request
    // cannot inherit an account cooldown.
    if is_sand_tool_capability_error(error, 1) {
        return false;
    }
    let texts = [
        error.client_message(),
        error.message.clone(),
        error.detail.clone().unwrap_or_default(),
    ];
    texts.iter().any(|text| {
        crate::retry::is_policy_rate_limit(text) || is_account_failover_policy_error(text)
    })
}

/// Record only the cooldown implied by the observed Sand error.
///
/// Admission/open failures are transport pressure, not quota exhaustion. They
/// may receive the short route cooldown below, but must never be promoted into
/// the longer policy-429 breaker merely because they share the account
/// failover path. Conversely, a genuine policy diagnostic keeps the existing
/// policy cooldown and `Retry-After` semantics.
fn note_sand_account_failover_error(model: &str, token: &str, error: &CursorError) {
    if let Some((cooldown, reason)) = sand_account_route_cooldown(error) {
        note_sand_account_route_cooldown(model, token, reason, cooldown);
    }
    if cursor_error_is_account_failover_policy(error) {
        let diagnostic = error.client_message();
        note_policy_rate_limit(
            model,
            "sand",
            token,
            &diagnostic,
            error.retry_after.as_deref(),
        );
    }
}

/// Return the route cooldown that should be applied to a Sand open error.
/// Admission saturation is short-lived; an explicit closed/session-invalid
/// response is held longer so a retry wave does not keep hammering the same
/// credential.  Generic provider 5xx errors remain on the transport retry
/// path and do not poison an account route.
fn sand_account_route_cooldown(error: &CursorError) -> Option<(Duration, &'static str)> {
    let message = error.client_message();
    let lower = message.to_ascii_lowercase();
    if lower.contains("error_account_closed")
        || lower.contains("error_not_logged_in")
        || (error.status == 401 && lower.contains("authentication error"))
    {
        return Some((
            Duration::from_secs(SAND_ROUTE_AUTH_COOLDOWN_SECS),
            "authentication_or_account_closed",
        ));
    }
    if lower.contains("sand inference open admission deadline exhausted")
        || lower.contains("sand open admission queue timed out")
    {
        return Some((
            Duration::from_secs(SAND_ROUTE_ADMISSION_COOLDOWN_SECS),
            "admission_saturated",
        ));
    }
    None
}

fn policy_rate_limit_breaker_state(
    model: &str,
    client_type: &str,
    token: &str,
) -> Option<PolicyRateLimitState> {
    let key = policy_rate_limit_key(model, client_type, token);
    let mut breaker = POLICY_RATE_LIMIT_BREAKER
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let now = Instant::now();
    // Keep the map bounded even when no new policy errors arrive. A read is
    // the hot path for retries, so doing this under the same short mutex lock
    // avoids a background task and keeps account-switch cleanup deterministic.
    breaker.retain(|_, state| state.until > now);
    let mut state = breaker.get(&key).cloned();
    if let Some(state) = state.as_mut() {
        state.retry_after_secs = state.until.saturating_duration_since(now).as_secs().max(1);
    }
    state
}

fn policy_rate_limit_breaker_error(model: &str, state: &PolicyRateLimitState) -> CursorError {
    let mut error = CursorError::new(
        429,
        format!(
            "{} (local rate-limit cooldown for {model}; retry after {}s)",
            state.message, state.retry_after_secs
        ),
        None,
    );
    error.retry_after = Some(state.retry_after_secs.to_string());
    error
}

fn policy_rate_limit_preflight(
    model: &str,
    client_type: &str,
    token: &str,
) -> Result<(), CursorError> {
    match policy_rate_limit_breaker_state(model, client_type, token) {
        Some(state) => Err(policy_rate_limit_breaker_error(model, &state)),
        None => Ok(()),
    }
}

#[cfg(test)]
fn reset_policy_rate_limit_breaker_for_test() {
    POLICY_RATE_LIMIT_BREAKER
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clear();
    SAND_ROUTE_COOLDOWNS
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clear();
    POLICY_RATE_LIMIT_PROBE_GATES
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clear();
}

struct SharedCursorHttpClients {
    base_url: String,
    clients: Vec<CursorHttpClient>,
}

fn cursor_http_shard_count(raw: Option<&str>) -> usize {
    raw.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(CURSOR_HTTP_SHARDS_DEFAULT)
        .min(CURSOR_HTTP_SHARDS_MAX)
}

fn cursor_http_shard_index(key: &str, shard_count: usize) -> usize {
    // Stable FNV-1a keeps every conversation on one pool while UUIDv7 siblings
    // spread by their random suffix. Avoid RandomState so retries cannot move
    // between H2 failure domains within one process.
    let hash = key
        .as_bytes()
        .iter()
        .fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
    (hash as usize) % shard_count.max(1)
}

/// Process-wide sharded HTTP clients reuse TLS/H2 connections without putting
/// every concurrent Grok conversation on one H2 failure domain. Rebuilds when
/// the base URL or configured shard count changes.
fn shared_cursor_http_client(conversation_key: Option<&str>) -> CursorHttpClient {
    use std::sync::{Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<Option<SharedCursorHttpClients>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(None));
    let base = crate::config::cursor_base_url();
    let shard_count =
        cursor_http_shard_count(std::env::var("CCP_CURSOR_H2_SHARDS").ok().as_deref());
    let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(existing) = guard.as_ref()
        && existing.base_url == base
        && existing.clients.len() == shard_count
    {
        let index = conversation_key
            .map(|key| cursor_http_shard_index(key, shard_count))
            .unwrap_or(0);
        return existing.clients[index].clone();
    }
    let clients: Vec<_> = (0..shard_count).map(|_| CursorHttpClient::new()).collect();
    let index = conversation_key
        .map(|key| cursor_http_shard_index(key, shard_count))
        .unwrap_or(0);
    let selected = clients[index].clone();
    *guard = Some(SharedCursorHttpClients {
        base_url: base,
        clients,
    });
    selected
}

const MAX_SESSION_USAGE: usize = 10_000;
const LIVE_USAGE_TAP_CAP: usize = 512;
const LIVE_EMPTY_TURN_MAX_RETRIES: u32 = 1;
// Gemini's Cursor route can acknowledge a Run with a clean FLAG_END while
// the model worker is still warming up.  This happens on both the CLI and
// Sand identities; limiting the larger recovery budget to Sand made CLI
// Gemini requests fail after the first fresh-conversation retry and produced
// a 502 storm when Claude Code fanned out subagents. Keep the extra attempts
// bounded and scoped to Gemini only so other models retain their stricter
// duplicate-suppression policy.
const LIVE_GEMINI_EMPTY_TURN_MAX_RETRIES: u32 = 3;
const LIVE_EMPTY_TURN_MAX_RETRIES_LIMIT: u32 = 8;
const LIVE_EMPTY_TURN_EPISODE_MS: u64 = 300_000;
const CURSOR_RESOURCE_RETRIES_DEFAULT: u32 = 6;
const CURSOR_RESOURCE_RETRIES_MAX: u32 = 12;
const CURSOR_STEP_FAILURE_RETRIES_DEFAULT: u32 = 4;
const CURSOR_STEP_FAILURE_RETRIES_MAX: u32 = 8;

#[derive(Debug, Clone, Copy)]
struct LiveLateRetryPolicy {
    transient_max_retries: u32,
    empty_turn_max_retries: u32,
    empty_turn_episode: Duration,
}

impl Default for LiveLateRetryPolicy {
    fn default() -> Self {
        Self {
            transient_max_retries: crate::retry::MAX_RATE_LIMIT_RETRIES,
            empty_turn_max_retries: LIVE_EMPTY_TURN_MAX_RETRIES,
            empty_turn_episode: Duration::from_millis(LIVE_EMPTY_TURN_EPISODE_MS),
        }
    }
}

impl LiveLateRetryPolicy {
    fn for_request_with_override(
        model: &str,
        _client_type: &str,
        empty_turn_max_retries: Option<&str>,
    ) -> Self {
        let request_default = if is_gemini_request(model) {
            LIVE_GEMINI_EMPTY_TURN_MAX_RETRIES
        } else {
            LIVE_EMPTY_TURN_MAX_RETRIES
        };
        Self {
            empty_turn_max_retries: empty_turn_max_retries
                .and_then(|raw| raw.trim().parse::<u32>().ok())
                .map(|value| value.min(LIVE_EMPTY_TURN_MAX_RETRIES_LIMIT))
                .unwrap_or(request_default),
            empty_turn_episode: Duration::from_millis(
                env_u64_millis(
                    "CCP_CURSOR_EMPTY_TURN_EPISODE_MS",
                    LIVE_EMPTY_TURN_EPISODE_MS,
                )
                .min(3_600_000),
            ),
            ..Self::default()
        }
    }

    fn for_request(model: &str, client_type: &str) -> Self {
        Self::for_request_with_override(
            model,
            client_type,
            std::env::var("CCP_CURSOR_EMPTY_TURN_MAX_RETRIES")
                .ok()
                .as_deref(),
        )
    }
}

fn is_gemini_request(model: &str) -> bool {
    resolve_cursor_model(model)
        .map(|resolved| resolved.model_id.to_ascii_lowercase())
        .unwrap_or_else(|_| model.trim().to_ascii_lowercase())
        .starts_with("gemini-")
}

#[derive(Debug, Clone)]
struct LiveLateRetryContext {
    model: String,
    client_type: String,
    effective_token: Arc<Mutex<String>>,
    /// Stable account partition for registry/conversation state.  This is
    /// shared with the retry starter so an account-pool failover can move the
    /// late-retry observer to the replacement account without reusing the
    /// exhausted account's pending generation.
    account_key: Arc<Mutex<String>>,
    compaction_mode: bool,
}

impl LiveLateRetryContext {
    fn effective_token(&self) -> String {
        self.effective_token
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn account_key(&self) -> String {
        self.account_key
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

struct SessionUsageStore {
    map: HashMap<String, u64>,
    order: VecDeque<String>,
}

static SESSION_USAGE: LazyLock<Mutex<SessionUsageStore>> = LazyLock::new(|| {
    Mutex::new(SessionUsageStore {
        map: HashMap::new(),
        order: VecDeque::new(),
    })
});

fn record_session_input_tokens(session_id: &str, input_tokens: u64) {
    if session_id.is_empty() || input_tokens == 0 {
        return;
    }
    let mut store = SESSION_USAGE.lock().unwrap_or_else(|e| e.into_inner());
    if store
        .map
        .insert(session_id.to_string(), input_tokens)
        .is_none()
    {
        store.order.push_back(session_id.to_string());
    } else {
        store.order.retain(|item| item != session_id);
        store.order.push_back(session_id.to_string());
    }
    while store.order.len() > MAX_SESSION_USAGE {
        if let Some(evict) = store.order.pop_front() {
            store.map.remove(&evict);
        } else {
            break;
        }
    }
}

fn count_tokens_for_request(_session_id: Option<&str>, body: &MessagesRequest) -> u64 {
    (render_cursor_prompt(body).len() / 4).max(1) as u64
}

fn remember_input_tokens(session_id: Option<&str>, input_tokens: Option<u64>) {
    let Some(session_id) = session_id.filter(|id| !id.is_empty()) else {
        return;
    };
    let Some(input_tokens) = input_tokens.filter(|tokens| *tokens > 0) else {
        return;
    };
    record_session_input_tokens(session_id, input_tokens);
}

/// Mirror live `turn_ended` usage into the session store without editing live.rs.
fn tap_session_usage(
    session_id: String,
    mut events: mpsc::Receiver<LiveEventResult>,
) -> mpsc::Receiver<LiveEventResult> {
    let (tx, rx) = mpsc::channel(LIVE_USAGE_TAP_CAP);
    tokio::spawn(async move {
        while let Some(item) = events.recv().await {
            if let Ok(LiveRunEvent::Cursor(CursorStreamEvent::Usage { input_tokens, .. })) = &item
                && *input_tokens > 0
            {
                record_session_input_tokens(&session_id, *input_tokens);
            }
            if tx.send(item).await.is_err() {
                break;
            }
        }
    });
    rx
}

fn live_path_eligible(_want_stream: bool, has_session: bool, bidi_enabled: bool) -> bool {
    has_session && bidi_enabled
}

/// Grok Build uses a stable `xai-compact-*` client request id for server-side
/// context compaction. Compaction is a separate operation: it must not join
/// the ordinary Cursor live-run slot or reuse the conversation checkpoint that
/// the preceding turn is still draining.
pub(crate) fn is_xai_compact_request(client_request_id: Option<&str>) -> bool {
    client_request_id.map(str::trim).is_some_and(|id| {
        id.strip_prefix("xai-compact-")
            .is_some_and(|suffix| !suffix.is_empty())
    })
}

/// Anthropic's context-management extension can request compaction without
/// the Grok-specific `x-grok-req-id` marker.  Such a request carries the full
/// history and must be isolated from the ordinary Cursor live slot just like
/// the xAI compact operation; otherwise it can race the preceding generation
/// and re-enter its checkpoint.
fn is_context_management_compact_request(body: &MessagesRequest) -> bool {
    body.extra
        .get("context_management")
        .and_then(|value| value.get("edits"))
        .and_then(serde_json::Value::as_array)
        .is_some_and(|edits| {
            edits.iter().any(|edit| {
                edit.get("type")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|kind| kind.eq_ignore_ascii_case("compact_20260112"))
            })
        })
}

#[cfg(test)]
fn is_compact_request(body: &MessagesRequest, client_request_id: Option<&str>) -> bool {
    is_compact_request_with_helper(body, client_request_id, None)
}

/// Detect every Claude/Cursor compaction transport variant.  Grok Build marks
/// its operation with `xai-compact-*`; Anthropic's server-side extension uses
/// `context_management.edits`; Claude Code's local `/compact` and reactive
/// compaction use a strict summary prompt (and newer SDK helper calls add
/// `x-stainless-helper: compaction`).
fn is_compact_request_with_helper(
    body: &MessagesRequest,
    client_request_id: Option<&str>,
    stainless_helper: Option<&str>,
) -> bool {
    is_xai_compact_request(client_request_id)
        || is_context_management_compact_request(body)
        || is_stainless_compaction_helper(stainless_helper)
        || is_reactive_compact_prompt(body)
}

fn is_stainless_compaction_helper(value: Option<&str>) -> bool {
    value.is_some_and(|raw| {
        raw.split(',')
            .map(str::trim)
            .any(|part| part.eq_ignore_ascii_case("compaction"))
    })
}

/// Give a context-compaction operation its own live-run/conversation lane
/// while keeping the key stable across transport retries.  Grok Build supplies
/// `xai-compact-*` for this purpose; Anthropic's `compact_20260112` extension
/// does not, so its canonical request payload is the fallback identity.
fn compact_agent_id(body: &MessagesRequest, ctx: &RequestContext) -> String {
    let mut payload = b"ccp-compact-agent\0".to_vec();
    if let Some(request_id) = ctx
        .client_request_id
        .as_deref()
        .map(str::trim)
        .filter(|id| is_xai_compact_request(Some(id)))
    {
        // The xAI id is explicitly retry-stable. Prefer it over the complete
        // body because clients may rewrite stream-only fields on a retry.
        payload.extend_from_slice(request_id.as_bytes());
    } else {
        payload.extend_from_slice(&live_operation_fingerprint_payload(body, None));
    }
    // Nested Claude agents can share a session and occasionally reuse a
    // compaction payload. Keep their isolated lanes distinct as well.
    for value in [
        ctx.claude_code.agent_id.as_deref(),
        ctx.claude_code.parent_agent_id.as_deref(),
    ] {
        payload.push(0);
        if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
            payload.extend_from_slice(value.as_bytes());
        }
    }
    format!("ccp-compact-{:016x}", live_request_fingerprint(&payload))
}

fn live_path_skip_reason(
    _want_stream: bool,
    has_session: bool,
    bidi_enabled: bool,
) -> Option<&'static str> {
    if !bidi_enabled {
        return Some("bidi_disabled");
    }
    if !has_session {
        return Some("no_session");
    }
    None
}

fn live_sse_recording_usage(
    session_id: &str,
    events: mpsc::Receiver<LiveEventResult>,
    message_id: String,
    wire_model: String,
    estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
    compaction_mode: bool,
) -> Response {
    live_sse_response(
        tap_session_usage(session_id.to_string(), events),
        message_id,
        wire_model,
        estimated_input,
        monitor,
        compaction_mode,
    )
}

/// Dispatch a request through Cursor Desktop's current Sand transport.
///
/// Sand is intentionally stateless from the proxy's point of view: every
/// request carries the complete rendered Anthropic history and gets fresh
/// conversation/invocation ids.  This keeps retries and Claude Code's
/// `/compact` requests independent from the AgentService live-run registry,
/// whose resumable stream semantics do not apply to InferenceService.
#[allow(clippy::too_many_arguments)]
async fn sand_direct_response(
    body: &MessagesRequest,
    ctx: &RequestContext,
    token: &str,
    message_id: String,
    wire_model: String,
    model: &str,
    parameter_model: &str,
    compaction_mode: bool,
    account_failover_state: SharedAccountFailoverState,
) -> Response {
    // Keep the local hosted search/fetch shortcuts available on Sand.  These
    // handlers are provider-agnostic and must run after auth (the caller has
    // already selected/validated the Cursor account).
    if is_hosted_web_search_request(body) {
        let query = extract_web_search_query(body).unwrap_or_default();
        if query.trim().is_empty() {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "web_search requires a non-empty query",
            );
        }
        let (hits, error) = match search_web(&query).await {
            Ok(hits) => (hits, None),
            Err(err) => (Vec::new(), Some(err)),
        };
        if body.stream {
            return hosted_web_search_sse_response(message_id, wire_model, query, hits, error);
        }
        return hosted_web_search_json_response(message_id, wire_model, query, hits, error);
    }
    if let Some(resp) = maybe_handle_hosted_web_fetch(body, &message_id, &wire_model).await {
        return resp;
    }

    let parts = render_cursor_prompt_parts_with(
        body,
        CursorPromptOptions {
            // Compaction is summary-only.  Omitting tool schemas also prevents
            // a Sand model from emitting a tool call while Claude is waiting
            // for a compacted context response.
            omit_tools: compaction_mode,
            delta_only: false,
        },
    );
    // Use the native InferenceCoreMessage representation for Sand.  The
    // renderer above remains useful for system-field policy and token
    // estimates, but its XML/text form is only a fallback for old Agent runs.
    // In particular, preserve every Anthropic role and tool/image block here.
    let mut messages = messages_from_anthropic(body, compaction_mode);
    if let Some(system) = parts
        .custom_system_prompt
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        && !messages
            .iter()
            .any(|message| message.role == sand_inference::ROLE_SYSTEM)
    {
        messages.insert(0, SandInferenceMessage::system(system));
    }
    // Requests with no representable blocks still need one valid message.
    // InferenceService expects at least one message.  An empty Anthropic
    // payload is unusual but can be produced by a client-side retry; preserve
    // the request rather than turning it into a malformed Connect body.
    if messages.is_empty() {
        messages.push(SandInferenceMessage::user(parts.user_text.clone()));
    }

    // The new endpoint accepts UUID identifiers and does not use the
    // AgentService session registry.  Fresh ids also prevent an abandoned
    // Desktop invocation from poisoning the next Claude Code turn.
    let sand_tools = tools_from_anthropic(body, compaction_mode);
    let accepted_unadvertised_tool_names =
        accepted_unadvertised_tool_names_from_anthropic(body, &sand_tools);
    let tool_count = sand_tools.len();
    let tool_names = sand_tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let estimated_input = estimate_rendered_prompt_tokens(&parts);
    if let Some(monitor) = ctx.monitor.as_ref() {
        // The request is now in the Sand supervisor. Keep this phase visible
        // while operation dedupe and local admission are deciding who opens
        // the single upstream invocation.
        monitor.queued(&ctx.req_id);
    }

    // Claim before the first upstream open. A retry can arrive while the
    // owner is still waiting for HTTP headers, which is precisely the window
    // where two otherwise identical Sand invocations used to create Cursor's
    // "already active" 503. The key deliberately excludes bearer identity:
    // unbound account selection may rotate between transport retries, but the
    // client operation is still one logical turn.
    let mut sand_operation_owner = match sand_operation_key(body, ctx, model, compaction_mode) {
        Some(key) => match admit_sand_operation(key) {
            SandOperationAdmission::Owner(owner) => Some(owner),
            SandOperationAdmission::Subscriber(subscription) => {
                return sand_operation_subscription_response(
                    subscription,
                    body,
                    ctx,
                    token,
                    message_id,
                    wire_model,
                    compaction_mode,
                    tool_count,
                    estimated_input,
                )
                .await;
            }
            SandOperationAdmission::ReplayUnavailable => {
                return json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "api_error",
                    "Sand operation is still active but its replay window is unavailable; retry shortly",
                );
            }
            SandOperationAdmission::CapacityExceeded => {
                return json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "api_error",
                    "Sand operation registry is at capacity; retry shortly",
                );
            }
            SandOperationAdmission::SubscriberLimit => {
                return json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "api_error",
                    "Sand operation subscriber capacity is full; retry shortly",
                );
            }
        },
        None => None,
    };
    let capability = sand_tool_capability_for_token(token, model);
    // Fable currently accepts the InferenceService text path but rejects the
    // ordinary `InferenceAgentTool` catalog with an inner provider 400.  Use
    // the text bridge for that family (and for a cached account/model
    // rejection) from the first request, avoiding a deterministic 400 retry
    // loop. Other Sand models retain the native catalog and can fall back to
    // the same bridge if the provider reports the rejection at stream time.
    let text_bridge_initial = tool_count > 0
        && !compaction_mode
        && (sand_model_prefers_text_tool_bridge(model)
            || matches!(capability, sand_inference::SandToolCapability::Unsupported));
    let bridge_prompt = if tool_count > 0 && !compaction_mode {
        render_sand_text_tool_bridge_prompt(body)
    } else {
        None
    };
    let mut request_messages = messages.clone();
    if text_bridge_initial {
        if let Some(prompt) = bridge_prompt.as_deref() {
            prepend_sand_text_tool_bridge_prompt(&mut request_messages, prompt);
        }
    }
    let request = SandInferenceRequest::new(
        model.to_string(),
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
        request_messages,
    )
    // `maxMode` belongs to the selected model configuration, not the
    // canonical family id sent on the Sand wire.  Keep deriving it from the
    // resolved CLI/catalog variant so Fable/Opus aliases retain their default
    // mode after canonicalization.
    .with_max_mode(parameter_model.to_ascii_lowercase().contains("max"))
    .with_parameter_model_id(parameter_model)
    .with_max_tokens(body.max_tokens.map(u64::from))
    .with_tools(if text_bridge_initial {
        Vec::new()
    } else {
        sand_tools.clone()
    })
    .with_accepted_unadvertised_tool_names(accepted_unadvertised_tool_names);

    // Keep a complete no-catalog request ready. If an otherwise native Sand
    // model rejects the catalog after HTTP 200, the stream driver can switch
    // once to this request and continue through the text bridge without
    // asking Claude Code to resend the turn.
    let fallback_request = if tool_count > 0 && !text_bridge_initial {
        bridge_prompt.as_deref().map(|prompt| {
            let mut fallback_messages = messages.clone();
            prepend_sand_text_tool_bridge_prompt(&mut fallback_messages, prompt);
            SandInferenceRequest::new(
                model.to_string(),
                uuid::Uuid::new_v4().to_string(),
                uuid::Uuid::new_v4().to_string(),
                fallback_messages,
            )
            .with_max_mode(parameter_model.to_ascii_lowercase().contains("max"))
            .with_parameter_model_id(parameter_model)
            .with_max_tokens(body.max_tokens.map(u64::from))
            .with_tools(Vec::new())
            .with_accepted_unadvertised_tool_names(Vec::<String>::new())
        })
    } else {
        None
    };

    // Reuse a process-wide Sand H2 client pool. Constructing a fresh reqwest
    // client for every turn disables connection reuse and turns a 512-way
    // Claude Code/Grok fan-out into a synchronized TLS/H2 handshake burst,
    // which Cursor reports as 502/503 admission failures. The shard key keeps
    // one session on one connection pool while independent sessions spread
    // across the configured H2 shards.
    // Shard by the logical request id first. Claude Code's nested agents
    // intentionally share one session id, so using the session alone would
    // funnel a 512-way fan-out onto one H2 connection pool and recreate the
    // upstream's stream/admission pressure. `x-grok-req-id` is stable across
    // retries of one logical turn, preserving its failure domain while
    // distributing sibling requests. Older clients may omit that header; in
    // that case include the agent id and finally the proxy request id so each
    // independent turn still gets a deterministic shard.
    let sand_client_key = ctx
        .client_request_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .or_else(|| {
            ctx.claude_code
                .agent_id
                .as_deref()
                .filter(|id| !id.is_empty())
        })
        .unwrap_or(ctx.req_id.as_str());
    let client = sand_inference::shared_client(Some(sand_client_key));
    if let Some(monitor) = ctx.monitor.as_ref() {
        monitor.upstream_started(&ctx.req_id);
    }

    // A Sand policy/allowance error can arrive as an outer 429 or as Cursor's
    // newer `ERROR_PROVIDER_ERROR providerStatusCode=400 ...` diagnostic. The
    // selected account is only the first candidate; before exposing a
    // pre-output error, rotate through the saved account pool using fresh
    // request ids. This keeps all logged accounts usable without changing a
    // model's explicit account binding.
    let mut effective_token = token.to_string();
    let mut effective_request = request;
    let mut fallback_request = fallback_request;
    let sand_attempt_budget = SandAttemptBudget::new();
    let mut open_attempt_kind = SandOpenAttemptKind::Transport;
    // Share one deadline between the first open and all pre-output stream
    // replays. This prevents a retrying client from multiplying the open
    // timeout for every transport attempt.
    let sand_retry_deadline = Instant::now() + sand_logical_retry_budget();
    if let Some(monitor) = ctx.monitor.as_ref() {
        monitor.opening(&ctx.req_id);
    }
    let (stream, policy_admission) = loop {
        match open_sand_with_policy_probe_until(
            &client,
            &effective_token,
            &effective_request,
            sand_retry_deadline,
            &account_failover_state,
            &sand_attempt_budget,
            open_attempt_kind,
        )
        .await
        {
            Ok(stream) => break stream,
            Err(error)
                if !sand_attempt_budget_exhausted(&error)
                    && !is_sand_tool_capability_error(&error, tool_count)
                    && (is_account_failover_open_error(model, &error)
                        || cursor_error_is_account_failover_policy(&error)) =>
            {
                let diagnostic = error.client_message();
                note_sand_account_failover_error(model, &effective_token, &error);
                let replacement = account_failover_replacement_token_async(
                    effective_token.clone(),
                    model.to_string(),
                    "sand".to_string(),
                    Arc::clone(&account_failover_state),
                )
                .await;
                let Some(replacement) = replacement else {
                    // The candidate snapshot and the final claim are
                    // intentionally separate (the registry is read under a
                    // blocking lock). Another request may win the candidate
                    // between them. Keep waiting on this lane while the
                    // logical deadline remains instead of surfacing a local
                    // 504 that causes Claude Code to retry in a storm.
                    if is_sand_admission_capacity_error(&error)
                        && wait_for_sand_admission_capacity(sand_retry_deadline).await
                    {
                        open_attempt_kind = SandOpenAttemptKind::Transport;
                        continue;
                    }
                    if let Some(owner) = sand_operation_owner.as_ref() {
                        owner.fail(error.client_message()).await;
                    }
                    return map_cursor_error_to_response(&error);
                };
                effective_token = replacement;
                effective_request = effective_request.with_fresh_ids();
                open_attempt_kind = SandOpenAttemptKind::AccountFailover;
                create_logger("cursor").warn(
                    "sand_open_account_failover",
                    Some(serde_json::Map::from_iter([
                        ("model".into(), serde_json::json!(model)),
                        ("clientType".into(), serde_json::json!("sand")),
                        (
                            "reason".into(),
                            serde_json::json!(if is_account_failover_open_error(model, &error) {
                                if diagnostic.to_ascii_lowercase().contains("admission") {
                                    "admission_saturated"
                                } else if error.status == 401 {
                                    "account_auth"
                                } else {
                                    "open_timeout"
                                }
                            } else {
                                "policy"
                            }),
                        ),
                        ("recovery".into(), serde_json::json!("fresh_request")),
                    ])),
                );
            }
            Err(error) if is_sand_tool_capability_error(&error, tool_count) => {
                mark_sand_tools_unsupported(
                    &effective_token,
                    model,
                    tool_count,
                    tool_names.clone(),
                    error.client_message(),
                );
                if let Some(replacement) = fallback_request.take() {
                    create_logger("cursor").warn(
                        "sand_tools_catalog_fallback",
                        Some(serde_json::Map::from_iter([
                            ("model".into(), serde_json::json!(model)),
                            ("clientType".into(), serde_json::json!("sand")),
                            ("toolCount".into(), serde_json::json!(tool_count)),
                            ("recovery".into(), serde_json::json!("text_bridge")),
                        ])),
                    );
                    effective_request = replacement.with_fresh_ids();
                    open_attempt_kind = SandOpenAttemptKind::ToolFallback;
                    continue;
                }
                let client_error = sand_tool_capability_client_error(&error, model, tool_count);
                if let Some(owner) = sand_operation_owner.as_ref() {
                    owner.fail(client_error.client_message()).await;
                }
                return map_cursor_error_to_response(&client_error);
            }
            Err(error) => {
                if let Some(owner) = sand_operation_owner.as_ref() {
                    owner.fail(error.client_message()).await;
                }
                return map_cursor_error_to_response(&error);
            }
        }
    };

    let allowed = if compaction_mode {
        Some(BTreeSet::new())
    } else {
        advertised_tool_names(body)
    };
    let (tx, source_rx) = mpsc::channel::<LiveEventResult>(128);
    // The driver keeps its existing single-consumer channel. The operation
    // forwarder is the only added hop and fans those events to every attached
    // HTTP retry, including the original owner subscription.
    let rx = if let Some(owner) = sand_operation_owner.as_mut() {
        let receiver = owner.take_subscription().into_receiver();
        owner.forward_from(source_rx);
        receiver
    } else {
        source_rx
    };
    let idle_secs = sand_stream_idle_timeout_secs();
    let retry_client = client.clone();
    let retry_token = effective_token.clone();
    let retry_request = effective_request;
    let retry_model = model.to_string();
    let retry_tool_count = tool_count;
    let retry_tool_names = tool_names;
    let retry_account_failover_state = Arc::clone(&account_failover_state);
    let retry_attempt_budget = sand_attempt_budget.clone();
    let retry_monitor = ctx.monitor.clone();
    let retry_monitor_request_id = ctx.req_id.clone();
    let retry_policy_admission = policy_admission;
    let effective_account_key = cursor_account_key_for_token_async(effective_token.clone()).await;
    let bridge_key = ctx
        .session_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .map(|sid| {
            live_run_key_for(live_run_identity_with_account(
                sid,
                ctx,
                Some(&effective_account_key),
            ))
        });
    let bridge_allowed = if compaction_mode {
        Some(BTreeSet::new())
    } else {
        advertised_tool_names(body)
    };
    // Any Sand request that carries Claude tools is framed through the same
    // buffered bridge, even when the provider accepts its native catalog. It
    // keeps native and XML calls on one downstream lifecycle and prevents raw
    // `<tool_use>` text from leaking into Claude Code if a provider changes
    // formats mid-rollout.
    let use_tool_bridge = tool_count > 0 && !compaction_mode && bridge_key.is_some();
    tokio::spawn(async move {
        drive_sand_stream_with_retries(
            retry_client,
            retry_token,
            retry_request,
            stream,
            allowed,
            compaction_mode,
            idle_secs,
            tx,
            retry_model,
            "sand".to_string(),
            retry_account_failover_state,
            retry_tool_count,
            retry_tool_names,
            fallback_request,
            sand_retry_deadline,
            retry_attempt_budget,
            retry_monitor,
            retry_monitor_request_id,
            Some(retry_policy_admission),
        )
        .await;
    });

    if body.stream {
        if use_tool_bridge {
            let Some(session_key) = bridge_key.as_deref() else {
                unreachable!("tool bridge requires a session key")
            };
            return sand_tool_bridge_stream_response(
                rx,
                session_key.to_string(),
                message_id,
                wire_model,
                body,
                bridge_allowed,
                estimated_input,
                ctx.monitor
                    .clone()
                    .map(|monitor| (monitor, ctx.req_id.clone())),
            )
            .await;
        }
        let events = if let Some(session_id) = ctx.session_id.as_deref() {
            tap_session_usage(session_id.to_string(), rx)
        } else {
            rx
        };
        return live_sse_response(
            events,
            message_id,
            wire_model,
            estimated_input,
            ctx.monitor
                .clone()
                .map(|monitor| (monitor, ctx.req_id.clone())),
            compaction_mode,
        );
    }

    if use_tool_bridge {
        let Some(session_key) = bridge_key.as_deref() else {
            unreachable!("tool bridge requires a session key")
        };
        return sand_tool_bridge_json_response(
            rx,
            session_key,
            &message_id,
            &wire_model,
            body,
            bridge_allowed,
            estimated_input,
            ctx.monitor.as_ref().map(|monitor| (&ctx.req_id, monitor)),
        )
        .await;
    }

    match collect_live_events_to_json(
        rx,
        &message_id,
        &wire_model,
        estimated_input,
        compaction_mode,
    )
    .await
    {
        Ok(json) => {
            let input_tokens = json.pointer("/usage/input_tokens").and_then(|v| v.as_u64());
            remember_input_tokens(ctx.session_id.as_deref(), input_tokens);
            if let Some(monitor) = ctx.monitor.as_ref() {
                monitor.usage_updated(
                    &ctx.req_id,
                    input_tokens,
                    json.pointer("/usage/output_tokens")
                        .and_then(|v| v.as_u64()),
                );
            }
            (StatusCode::OK, Json(json)).into_response()
        }
        Err(error) => json_error_from_cursor_message(error),
    }
}

/// Build a retry-stable identity for Sand only when the client supplied an
/// operation id that survives HTTP retries. `req_id` is intentionally not a
/// fallback: it is generated per HTTP request and would merge unrelated turns
/// or miss the duplicate this registry is meant to catch.
fn sand_operation_key(
    body: &MessagesRequest,
    ctx: &RequestContext,
    model: &str,
    compaction_mode: bool,
) -> Option<SandOperationKey> {
    let client_id = ctx
        .client_request_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let mut payload = b"ccp-sand-operation\0".to_vec();
    payload.extend_from_slice(model.trim().as_bytes());
    payload.push(u8::from(compaction_mode));
    if let Some(session) = ctx.session_id.as_deref().filter(|value| !value.is_empty()) {
        payload.extend_from_slice(session.as_bytes());
    }
    for value in [
        ctx.claude_code.agent_id.as_deref(),
        ctx.claude_code.parent_agent_id.as_deref(),
    ] {
        payload.push(0);
        if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
            payload.extend_from_slice(value.as_bytes());
        }
    }
    payload.extend_from_slice(&live_operation_fingerprint_payload(body, Some(client_id)));
    let digest = Sha256::digest(payload);
    // Keep the untrusted client header out of the registry key.  The digest
    // already binds the header into the operation identity, while omitting
    // its raw value gives the in-process registry a fixed-size key even when
    // a client sends an unusually large request id.
    Some(SandOperationKey::new(format!("sand-{digest:x}")))
}

/// Attach a duplicate HTTP request to an existing Sand operation. The owner
/// has already started (or is about to start) the upstream driver; this path
/// never calls `SandInferenceClient::open` itself.
#[allow(clippy::too_many_arguments)]
async fn sand_operation_subscription_response(
    subscription: SandOperationSubscription,
    body: &MessagesRequest,
    ctx: &RequestContext,
    token: &str,
    message_id: String,
    wire_model: String,
    compaction_mode: bool,
    tool_count: usize,
    estimated_input: u64,
) -> Response {
    let rx = subscription.into_receiver();
    let account_key = cursor_account_key_for_token_async(token.to_string()).await;
    let bridge_key = ctx
        .session_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .map(|sid| live_run_key_for(live_run_identity_with_account(sid, ctx, Some(&account_key))));
    let allowed = if compaction_mode {
        Some(BTreeSet::new())
    } else {
        advertised_tool_names(body)
    };
    let use_tool_bridge = tool_count > 0 && !compaction_mode && bridge_key.is_some();
    if body.stream {
        if use_tool_bridge {
            let Some(session_key) = bridge_key else {
                unreachable!("tool bridge requires a session key")
            };
            return sand_tool_bridge_stream_response(
                rx,
                session_key,
                message_id,
                wire_model,
                body,
                allowed,
                estimated_input,
                ctx.monitor
                    .clone()
                    .map(|monitor| (monitor, ctx.req_id.clone())),
            )
            .await;
        }
        let events = if let Some(session_id) = ctx.session_id.as_deref() {
            tap_session_usage(session_id.to_string(), rx)
        } else {
            rx
        };
        return live_sse_response(
            events,
            message_id,
            wire_model,
            estimated_input,
            ctx.monitor
                .clone()
                .map(|monitor| (monitor, ctx.req_id.clone())),
            compaction_mode,
        );
    }
    if use_tool_bridge {
        let Some(session_key) = bridge_key.as_deref() else {
            unreachable!("tool bridge requires a session key")
        };
        return sand_tool_bridge_json_response(
            rx,
            session_key,
            &message_id,
            &wire_model,
            body,
            allowed,
            estimated_input,
            ctx.monitor.as_ref().map(|monitor| (&ctx.req_id, monitor)),
        )
        .await;
    }
    match collect_live_events_to_json(
        rx,
        &message_id,
        &wire_model,
        estimated_input,
        compaction_mode,
    )
    .await
    {
        Ok(json) => (StatusCode::OK, Json(json)).into_response(),
        Err(error) => json_error_from_cursor_message(error),
    }
}

/// Open Sand with bounded retries without extending the caller's logical
/// request deadline. The local per-call cap still applies, so a stream retry
/// cannot spend the entire remaining turn budget on one stalled connection.
async fn open_sand_with_retries_until(
    client: &SandInferenceClient,
    token: &str,
    request: &SandInferenceRequest,
    logical_deadline: Instant,
    account_failover_state: &SharedAccountFailoverState,
    attempt_budget: &SandAttemptBudget,
    attempt_kind: SandOpenAttemptKind,
) -> Result<crate::providers::cursor::sand_inference::SandInferenceStream, CursorError> {
    let max_retries = std::env::var("CCP_CURSOR_SAND_OPEN_RETRIES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .unwrap_or(3)
        .min(8);
    // Bound each invocation as well as the shared logical deadline. This
    // keeps account-failover and stream-replay opens from monopolizing a
    // single request after the caller has already spent most of its budget.
    let local_deadline = logical_deadline.min(Instant::now() + sand_open_total_budget());
    // Reserve accepted-stream capacity before contacting the upstream.  A
    // successful HTTP open represents a live model Run; waiting for the stream
    // gate after headers would create unowned upstream Runs while the local
    // 512-stream budget is full.  This permit is held across transport retries
    // for this logical open and moves into the returned stream on success.
    let stream_permit = admit_sand_stream_until(local_deadline).await?;
    let attempt_secs = std::env::var("CCP_CURSOR_SAND_OPEN_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(crate::config::cursor_request_timeout_secs)
        .clamp(10, 180);
    let mut attempt = 0;
    // The first invocation after an account/tool handoff consumes that
    // handoff's dedicated budget. Any retries of the same selected account
    // are ordinary transport attempts and use the transport budget instead.
    let mut current_attempt_kind = attempt_kind;
    // A lane timeout is only useful as an account-pool handoff when there is
    // an unattempted, healthy profile to receive the request.  Snapshot that
    // fact once per open invocation; when the registry contains no viable
    // replacement (the common single-account case), keep waiting until the
    // normal local deadline instead of manufacturing a 504 after 12 seconds.
    let mut admission_failover_checked = false;
    loop {
        let remaining = local_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(CursorError::new(
                504,
                "Sand inference open retry budget exhausted",
                None,
            ));
        }
        // A prior Sand response may have already established an account/model
        // allowance breaker while this logical request was waiting for open
        // capacity.  Check it immediately before acquiring a transport slot;
        // otherwise queued callers keep issuing identical upstream requests
        // for the known-exhausted account and recreate the 429 wave.  The
        // caller's existing account-failover branch handles this typed policy
        // error and can rotate an unbound request to another saved account.
        if let Err(error) = policy_rate_limit_preflight(&request.model_id, "sand", token) {
            sand_open_breaker_abort(token, &request.model_id, false);
            return Err(error);
        }
        // Keep account/model failure accounting for diagnostics and retry
        // backoff. It never rejects a caller locally: the upstream transport
        // must remain able to prove that a cooled-down route recovered.
        sand_open_breaker_admit(token, &request.model_id)?;
        // A busy account/model lane is backpressure, not an upstream failure.
        // Keep the request queued until a real open slot is available instead
        // of bypassing the gate and creating the 503 wave seen with the old
        // 512-open default.  The sleep is deliberately short so a released
        // permit is observed quickly, while the logical deadline remains the
        // final bound for a genuinely wedged upstream.
        let admission_started = Instant::now();
        let open_permit = loop {
            match admit_sand_open_until(token, &request.model_id, local_deadline).await {
                Ok(Some(permit)) => break permit,
                Ok(None) => {
                    // Do not leave a large fan-out parked behind one
                    // account/model lane until the full 180s open budget.
                    // The caller can safely rotate an *unbound* request to a
                    // saved account because Sand carries complete history and
                    // fresh UUIDs. Explicit model-account bindings remain on
                    // their selected lane and will receive the original
                    // retryable diagnostic if no slot becomes available.
                    if admission_started.elapsed()
                        >= Duration::from_secs(
                            crate::providers::cursor::sand_inference::sand_open_account_queue_failover_secs(),
                        )
                        && !admission_failover_checked
                    {
                        admission_failover_checked = true;
                        // Do the registry read off Tokio workers.  The helper
                        // excludes the current/attempted accounts and routes
                        // already in policy cooldown; a single-account pool
                        // therefore keeps its request queued rather than
                        // turning a brief lane burst into a client-visible
                        // 504/503.  If another request wins the candidate
                        // concurrently, the caller still falls back to the
                        // same-account wait on its next logical attempt.
                        if sand_admission_failover_candidate_available(
                            token,
                            &request.model_id,
                            account_failover_state,
                        )
                        .await
                        {
                            create_logger("cursor").warn(
                                "sand_open_admission_failover",
                                Some(serde_json::Map::from_iter([
                                    ("model".into(), serde_json::json!(&request.model_id)),
                                    (
                                        "waitMs".into(),
                                        serde_json::json!(admission_started.elapsed().as_millis()),
                                    ),
                                    ("recovery".into(), serde_json::json!("account_pool")),
                                ])),
                            );
                            return Err(CursorError::new(
                                504,
                                "Sand inference open admission deadline exhausted",
                                None,
                            ));
                        }
                    }
                    let remaining = local_deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(CursorError::new(
                            504,
                            "Sand inference open admission deadline exhausted",
                            None,
                        ));
                    }
                    let wait = remaining.min(Duration::from_millis(250));
                    crate::retry::sleep(wait.as_millis().max(1) as u64).await;
                }
                Err(error) => {
                    // A half-open probe can lose the local queue race before
                    // the HTTP request starts. Clear its in-flight marker so
                    // the next caller can probe after a short cool-off.
                    sand_open_breaker_abort(token, &request.model_id, true);
                    return Err(error);
                }
            }
        };
        // The fairness slice may have consumed most of the logical budget.
        // Recompute it after admission so queue time cannot extend this HTTP
        // attempt beyond the caller's deadline.
        let remaining = local_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            sand_open_breaker_abort(token, &request.model_id, true);
            return Err(CursorError::new(
                504,
                "Sand inference open retry budget exhausted",
                None,
            ));
        }
        // A connect/open failure is ambiguous: the gateway may have accepted
        // the frame before the local socket failed. Replaying the same
        // lifecycle ids then looks like a duplicate live invocation and can
        // produce a persistent 503 "already active" response. Keep the first
        // attempt's ids for normal diagnostics, but rotate both ids before
        // every subsequent open.
        let attempt_request = if attempt == 0 {
            request.clone()
        } else {
            request.clone().with_fresh_ids()
        };
        if !attempt_budget.try_consume(current_attempt_kind) {
            sand_open_breaker_abort(token, &request.model_id, true);
            return Err(CursorError::new(
                503,
                "Sand upstream attempt budget exhausted",
                None,
            ));
        }
        let per_attempt = remaining.min(Duration::from_secs(attempt_secs));
        let attempt_started = Instant::now();
        // The admission permit protects only the expensive HTTP open/headers
        // phase.  Do not keep it in the returned stream's scope: a previous
        // version bound `_open_permit` to this whole function, effectively
        // turning the 512-open gate into a 512-*stream* gate.  Once those
        // streams stayed alive, every later Claude Code turn waited for the
        // 180s deadline and surfaced a proxy-generated 503 even though the
        // upstream connection had already been established.
        let mut open_permit = open_permit;
        let result = tokio::time::timeout(per_attempt, client.open(token, &attempt_request)).await;
        let result = match result {
            Ok(result) => result,
            Err(_) => Err(CursorError::new(
                504,
                format!(
                    "Sand inference open timed out after {}s",
                    per_attempt.as_secs()
                ),
                None,
            )),
        };
        match result {
            Ok(stream) => {
                // The HTTP handshake succeeded. Release both open gates before
                // consuming model tokens; the independent stream slot was
                // reserved before the request and remains attached to the
                // response body until it drains or is cancelled.
                open_permit.record_open_outcome(true);
                drop(open_permit);
                sand_open_breaker_success(token, &request.model_id);
                return Ok(stream.with_stream_permit(stream_permit));
            }
            Err(error) if is_sand_tool_capability_error(&error, request.tools.len()) => {
                // A deterministic catalog mismatch says nothing about route
                // capacity; do not force the cold-open controller to shrink.
                open_permit.record_open_neutral();
                drop(open_permit);
                sand_open_breaker_abort(token, &request.model_id, false);
                return Err(error);
            }
            Err(error) => {
                // Record transport failures for diagnostics, but do not
                // multiplicatively reduce the process-wide launch window.
                // Admission capacity is shared by every account/model route;
                // a synchronized failure burst on one route must not collapse
                // unrelated healthy lanes to a single slot. Account/model
                // breakers and the per-request retry budget provide the
                // isolation and backoff instead.
                if sand_open_failure_is_retryable(&error) {
                    open_permit.record_open_outcome(false);
                } else {
                    open_permit.record_open_neutral();
                }
                drop(open_permit);
                sand_open_breaker_failure(token, &request.model_id, &error, attempt_started);
                if attempt < max_retries
                    && crate::retry::should_retry_upstream(error.status, &error.client_message())
                {
                    let delay =
                        crate::retry::compute_backoff_delay(attempt, error.retry_after.as_deref())
                            .wait_ms;
                    create_logger("cursor").warn(
                        "sand_open_retry",
                        Some(serde_json::Map::from_iter([
                            ("attempt".into(), serde_json::json!(attempt + 1)),
                            ("status".into(), serde_json::json!(error.status)),
                            ("delayMs".into(), serde_json::json!(delay)),
                        ])),
                    );
                    let remaining = local_deadline.saturating_duration_since(Instant::now());
                    if remaining <= Duration::from_millis(delay) {
                        return Err(error);
                    }
                    crate::retry::sleep(delay).await;
                    attempt += 1;
                    current_attempt_kind = SandOpenAttemptKind::Transport;
                    continue;
                }
                return Err(error);
            }
        }
    }
}

/// Couple a Sand upstream open with the account/model policy single-flight
/// probe.  The probe is intentionally acquired outside the transport gate:
/// the latter protects connection establishment, while this lease prevents a
/// cold account/model route from sending a 512-request wave before the first
/// allowance result is known.  The lease is handed to the stream driver and
/// settled on the first useful event or terminal policy result.
async fn open_sand_with_policy_probe_until(
    client: &SandInferenceClient,
    token: &str,
    request: &SandInferenceRequest,
    logical_deadline: Instant,
    account_failover_state: &SharedAccountFailoverState,
    attempt_budget: &SandAttemptBudget,
    attempt_kind: SandOpenAttemptKind,
) -> Result<(SandInferenceStream, PolicyRateLimitAdmission), CursorError> {
    let admission = policy_rate_limit_admit_fresh_open(&request.model_id, "sand", token).await?;
    match open_sand_with_retries_until(
        client,
        token,
        request,
        logical_deadline,
        account_failover_state,
        attempt_budget,
        attempt_kind,
    )
    .await
    {
        Ok(stream) => Ok((stream, admission)),
        Err(error) => {
            // A deterministic tool/schema rejection is request-local.  The
            // provider wraps it in the same outer resource_exhausted/429
            // envelope used for account quota, but opening the policy breaker
            // here would poison the account before the text bridge fallback
            // gets a chance to replay the turn.  Release the probe without
            // publishing policy state for this capability-only failure.
            if is_sand_tool_capability_error(&error, request.tools.len()) {
                drop(admission);
            } else {
                // A policy result can arrive before an HTTP body is accepted.
                // Open the breaker while the cold probe is still owned so
                // queued Sand callers observe the typed 429 instead of
                // dispatching another identical request.
                settle_sand_policy_probe(
                    Some(admission),
                    &request.model_id,
                    "sand",
                    token,
                    Some(&error),
                    false,
                );
            }
            Err(error)
        }
    }
}

/// Settle a Sand policy probe exactly once. Returns `true` when this lease (or
/// a late terminal error after the lease was released) published an
/// account/model policy result; callers that also perform account failover can
/// then avoid logging/publishing the same breaker twice.
fn settle_sand_policy_probe(
    admission: Option<PolicyRateLimitAdmission>,
    model: &str,
    client_type: &str,
    token: &str,
    error: Option<&CursorError>,
    committed: bool,
) -> bool {
    // A stream can commit visible text/tool output and still terminate with
    // an account-local allowance error.  Do not let the `committed` bit turn
    // that late 429 into a health signal: the lease may already have been
    // consumed at the first output, so record the breaker even when no lease
    // remains.  Otherwise every subsequent request re-probes the exhausted
    // account and recreates a 429 wave.
    if let Some(error) = error {
        let diagnostic = error.client_message();
        if cursor_error_is_policy_limited(error) {
            if let Some(admission) = admission {
                admission.mark_policy_limited(
                    model,
                    client_type,
                    token,
                    &diagnostic,
                    error.retry_after.as_deref(),
                );
            } else {
                note_policy_rate_limit(
                    model,
                    client_type,
                    token,
                    &diagnostic,
                    error.retry_after.as_deref(),
                );
            }
            return true;
        }
    }

    let Some(admission) = admission else {
        return false;
    };
    if committed {
        admission.mark_healthy();
    } else {
        drop(admission);
    }
    false
}

/// Drain one Sand response and replay it when the transport dies before any
/// client-visible output.  InferenceService requests carry the complete
/// history and do not share AgentService's live-run registry, so a fresh UUID
/// request is safe at this point.  Once text or a tool call is committed, the
/// stream is fail-closed to avoid duplicating partial output in Claude Code.
#[allow(clippy::too_many_arguments)]
async fn drive_sand_stream_with_retries(
    client: SandInferenceClient,
    mut token: String,
    mut request: SandInferenceRequest,
    initial_stream: SandInferenceStream,
    allowed: Option<BTreeSet<String>>,
    compaction_mode: bool,
    idle_secs: u64,
    tx: mpsc::Sender<LiveEventResult>,
    model: String,
    client_type: String,
    account_failover_state: SharedAccountFailoverState,
    tool_count: usize,
    tool_names: Vec<String>,
    mut fallback_request: Option<SandInferenceRequest>,
    retry_deadline: Instant,
    attempt_budget: SandAttemptBudget,
    monitor: Option<crate::monitor::MonitorHandle>,
    monitor_request_id: String,
    mut policy_admission: Option<PolicyRateLimitAdmission>,
) {
    let max_retries = stream_retry_limit();
    let mut retries = 0u32;
    let mut next_stream = Some(initial_stream);
    let mut buffered = Vec::new();
    let mut committed = false;
    let mut catalog_active = !request.tools.is_empty();
    let mut next_open_kind = SandOpenAttemptKind::Transport;

    loop {
        let mut stream = if let Some(stream) = next_stream.take() {
            stream
        } else {
            if Instant::now() >= retry_deadline {
                discard_sand_replay_buffer(&mut buffered);
                send_sand_buffered_error(
                    &tx,
                    &mut buffered,
                    CursorError::new(
                        504,
                        "Sand logical retry deadline exhausted before useful progress",
                        None,
                    ),
                )
                .await;
                return;
            }
            if let Some(monitor) = monitor.as_ref() {
                monitor.retrying(&monitor_request_id);
            }
            let retry_request = request.clone().with_fresh_ids();
            match open_sand_with_policy_probe_until(
                &client,
                &token,
                &retry_request,
                retry_deadline,
                &account_failover_state,
                &attempt_budget,
                next_open_kind,
            )
            .await
            {
                Ok((stream, admission)) => {
                    policy_admission = Some(admission);
                    // The handoff/fallback budget is consumed by the first
                    // open on the new route. Any later pre-output replay of
                    // that same route is ordinary transport recovery.
                    next_open_kind = SandOpenAttemptKind::Transport;
                    stream
                }
                Err(error) => {
                    if sand_attempt_budget_exhausted(&error) {
                        discard_sand_replay_buffer(&mut buffered);
                        send_sand_buffered_error(&tx, &mut buffered, error).await;
                        return;
                    }
                    if !committed && is_sand_tool_capability_error(&error, tool_count) {
                        mark_sand_tools_unsupported(
                            &token,
                            &model,
                            tool_count,
                            tool_names.clone(),
                            error.client_message(),
                        );
                        if let Some(replacement) = fallback_request.take() {
                            request = replacement.with_fresh_ids();
                            next_open_kind = SandOpenAttemptKind::ToolFallback;
                            catalog_active = false;
                            retries = 0;
                            discard_sand_replay_buffer(&mut buffered);
                            create_logger("cursor").warn(
                                "sand_stream_catalog_fallback",
                                Some(serde_json::Map::from_iter([
                                    ("model".into(), serde_json::json!(&model)),
                                    ("clientType".into(), serde_json::json!(&client_type)),
                                    ("toolCount".into(), serde_json::json!(tool_count)),
                                    ("recovery".into(), serde_json::json!("text_bridge")),
                                ])),
                            );
                            continue;
                        }
                        discard_sand_replay_buffer(&mut buffered);
                        let client_error =
                            sand_tool_capability_client_error(&error, &model, tool_count);
                        send_sand_buffered_error(&tx, &mut buffered, client_error).await;
                        create_logger("cursor").warn(
                            "sand_tools_unsupported",
                            Some(serde_json::Map::from_iter([
                                (
                                    "accountId".into(),
                                    serde_json::json!(cursor_account_digest(&token)),
                                ),
                                ("model".into(), serde_json::json!(&model)),
                                ("clientType".into(), serde_json::json!(&client_type)),
                                ("toolCount".into(), serde_json::json!(tool_count)),
                                ("tools".into(), serde_json::json!(&tool_names)),
                            ])),
                        );
                        return;
                    }
                    if !committed && retries < max_retries && stream_error_is_retryable(&error) {
                        // The failed attempt may have queued thinking/usage
                        // control events without exposing visible output.
                        // A replay carries the full history and will produce
                        // a fresh set; retaining these frames would duplicate
                        // usage accounting and hidden reasoning on the client.
                        discard_sand_replay_buffer(&mut buffered);
                        let delay = crate::retry::compute_backoff_delay(
                            retries,
                            error.retry_after.as_deref(),
                        )
                        .wait_ms;
                        create_logger("cursor").warn(
                            "sand_stream_retry_open",
                            Some(serde_json::Map::from_iter([
                                ("attempt".into(), serde_json::json!(retries + 1)),
                                ("status".into(), serde_json::json!(error.status)),
                                ("delayMs".into(), serde_json::json!(delay)),
                            ])),
                        );
                        let remaining = retry_deadline.saturating_duration_since(Instant::now());
                        if remaining <= Duration::from_millis(delay) {
                            discard_sand_replay_buffer(&mut buffered);
                            send_sand_buffered_error(&tx, &mut buffered, error).await;
                            return;
                        }
                        crate::retry::sleep(delay).await;
                        retries += 1;
                        continue;
                    }
                    if !committed {
                        discard_sand_replay_buffer(&mut buffered);
                    }
                    send_sand_buffered_error(&tx, &mut buffered, error).await;
                    return;
                }
            }
        };

        let stream_error: Option<CursorError>;
        // Compaction's downstream contract allows a reasoning-only summary,
        // but it is only authoritative once the stream reaches End. Keep
        // that signal separate from `committed`: ordinary reasoning/usage is
        // speculative and must remain replayable until visible text or a
        // native tool has been observed.
        let mut compaction_thinking = false;
        loop {
            // A pre-output idle wait is part of the shared replay budget. Do
            // not allow its final timer to outlive that budget; once output
            // is committed, the normal per-stream idle timer remains valid.
            let idle_wait = if committed {
                Duration::from_secs(idle_secs)
            } else {
                let remaining = retry_deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    stream_error = Some(CursorError::new(
                        504,
                        "Sand logical retry deadline exhausted before useful progress",
                        None,
                    ));
                    break;
                }
                Duration::from_secs(idle_secs).min(remaining)
            };
            let next = tokio::time::timeout(idle_wait, stream.next()).await;
            match next {
                Ok(Some(Ok(event))) => {
                    let Some(event) = normalize_sand_stream_event(event, allowed.as_ref()) else {
                        continue;
                    };
                    if matches!(
                        &event,
                        CursorStreamEvent::ThinkingDelta { text } if compaction_mode && !text.is_empty()
                    ) {
                        compaction_thinking = true;
                    }
                    match classify_sand_stream_event(
                        &event,
                        committed,
                        compaction_mode,
                        compaction_thinking,
                    ) {
                        SandStreamEventAction::Buffer => buffered.push(event),
                        SandStreamEventAction::Commit => {
                            buffered.push(event);
                            committed = true;
                            settle_sand_policy_probe(
                                policy_admission.take(),
                                &model,
                                &client_type,
                                &token,
                                None,
                                true,
                            );
                            if catalog_active {
                                mark_sand_tools_supported(
                                    &token,
                                    &model,
                                    tool_count,
                                    tool_names.clone(),
                                );
                            }
                            if !send_sand_buffered_events(&tx, &mut buffered).await {
                                return;
                            }
                        }
                        SandStreamEventAction::Complete => {
                            buffered.push(event);
                            settle_sand_policy_probe(
                                policy_admission.take(),
                                &model,
                                &client_type,
                                &token,
                                None,
                                committed || (compaction_mode && compaction_thinking),
                            );
                            if !send_sand_buffered_events(&tx, &mut buffered).await {
                                return;
                            }
                            return;
                        }
                        SandStreamEventAction::HollowEnd => {
                            // A payload-less END (or an END carrying only
                            // thinking/usage/session metadata) is not a
                            // successful assistant turn. Treat it like a
                            // transport hollow response so the bounded fresh
                            // UUID replay can recover a delayed model worker.
                            stream_error = Some(CursorError::new(
                                502,
                                "Sand stream ended without useful progress",
                                None,
                            ));
                            break;
                        }
                    }
                }
                Ok(Some(Err(error))) => {
                    stream_error = Some(error);
                    break;
                }
                Ok(None) => {
                    // A clean EOF without text/tool/end is treated as a
                    // retryable hollow response.  Sand gateways occasionally
                    // close after a control frame without FLAG_END.
                    if committed {
                        // SandInferenceStream normally synthesizes End at
                        // EOF, but keep this guard at the retry boundary as
                        // well.  A wrapper/proxy can consume the synthetic
                        // marker or report EOF after a committed frame; in
                        // that case closing the downstream channel without
                        // End makes the Anthropic SSE adapter emit
                        // "stream ended without turn_ended".  Flush any
                        // post-commit usage/metadata before the local End.
                        buffered.push(CursorStreamEvent::End);
                        if !send_sand_buffered_events(&tx, &mut buffered).await {
                            return;
                        }
                        return;
                    } else {
                        stream_error = Some(CursorError::new(
                            502,
                            "Sand stream ended without useful progress",
                            None,
                        ));
                    }
                    break;
                }
                Err(_) => {
                    let exhausted = !committed
                        && retry_deadline
                            .saturating_duration_since(Instant::now())
                            .is_zero();
                    stream_error = Some(CursorError::new(
                        504,
                        if exhausted {
                            "Sand logical retry deadline exhausted before useful progress"
                                .to_string()
                        } else {
                            format!(
                                "Sand stream idle timeout after {idle_secs}s with no useful progress"
                            )
                        },
                        None,
                    ));
                    break;
                }
            }
        }

        let Some(error) = stream_error else {
            // A committed stream that cleanly EOF'd has already flushed its
            // events.  No synthetic error is needed here.
            return;
        };
        // Capability failures are handled immediately below by the text
        // bridge.  They share the outer resource_exhausted/429 envelope with
        // quota responses, so settling the probe first would incorrectly
        // open an account breaker and make the fallback request fail locally.
        let capability_error = !committed && is_sand_tool_capability_error(&error, tool_count);
        if capability_error {
            drop(policy_admission.take());
        } else {
            settle_sand_policy_probe(
                policy_admission.take(),
                &model,
                &client_type,
                &token,
                Some(&error),
                committed,
            );
        }
        if sand_attempt_budget_exhausted(&error) {
            discard_sand_replay_buffer(&mut buffered);
            send_sand_buffered_error(&tx, &mut buffered, error).await;
            return;
        }
        if capability_error {
            mark_sand_tools_unsupported(
                &token,
                &model,
                tool_count,
                tool_names.clone(),
                error.client_message(),
            );
            if let Some(replacement) = fallback_request.take() {
                // The catalog rejection is deterministic for this
                // account/model, but the text-only provider path remains
                // usable. Replace the request once and let the same bounded
                // replay machinery deliver its XML calls through the bridge.
                request = replacement.with_fresh_ids();
                catalog_active = false;
                retries = 0;
                discard_sand_replay_buffer(&mut buffered);
                create_logger("cursor").warn(
                    "sand_stream_catalog_fallback",
                    Some(serde_json::Map::from_iter([
                        ("model".into(), serde_json::json!(&model)),
                        ("clientType".into(), serde_json::json!(&client_type)),
                        ("toolCount".into(), serde_json::json!(tool_count)),
                        ("recovery".into(), serde_json::json!("text_bridge")),
                    ])),
                );
                continue;
            }
            discard_sand_replay_buffer(&mut buffered);
            let client_error = sand_tool_capability_client_error(&error, &model, tool_count);
            send_sand_buffered_error(&tx, &mut buffered, client_error).await;
            create_logger("cursor").warn(
                "sand_tools_unsupported",
                Some(serde_json::Map::from_iter([
                    (
                        "accountId".into(),
                        serde_json::json!(cursor_account_digest(&token)),
                    ),
                    ("model".into(), serde_json::json!(&model)),
                    ("clientType".into(), serde_json::json!(&client_type)),
                    ("toolCount".into(), serde_json::json!(tool_count)),
                    ("tools".into(), serde_json::json!(tool_names)),
                ])),
            );
            return;
        }
        if !committed
            && !sand_attempt_budget_exhausted(&error)
            && (is_account_failover_open_error(&model, &error)
                || cursor_error_is_account_failover_policy(&error))
        {
            // Sand's InferenceService call is full-history and UUID-scoped, so
            // an account swap can safely replay the request while no visible
            // text/tool has reached Claude Code. Mark the old account's
            // cooldown before selecting a replacement; otherwise concurrent
            // retries could keep rediscovering the same exhausted login.
            note_sand_account_failover_error(&model, &token, &error);
            let Some(replacement) = account_failover_replacement_token_async(
                token.clone(),
                model.clone(),
                client_type.clone(),
                Arc::clone(&account_failover_state),
            )
            .await
            else {
                // The admission candidate check is only a snapshot. A
                // concurrent request may claim the alternate account before
                // this late retry reaches the registry, so a missing
                // replacement is ordinary backpressure rather than a
                // terminal request error. Stay on the current lane until the
                // shared logical deadline; this avoids a visible 503 retry
                // storm while preserving a hard upper bound.
                if is_sand_admission_capacity_error(&error)
                    && wait_for_sand_admission_capacity(retry_deadline).await
                {
                    next_open_kind = SandOpenAttemptKind::Transport;
                    continue;
                }
                discard_sand_replay_buffer(&mut buffered);
                send_sand_buffered_error(&tx, &mut buffered, error).await;
                return;
            };
            token = replacement;
            request = request.with_fresh_ids();
            next_open_kind = SandOpenAttemptKind::AccountFailover;
            retries = 0;
            discard_sand_replay_buffer(&mut buffered);
            create_logger("cursor").warn(
                "sand_stream_account_failover",
                Some(serde_json::Map::from_iter([
                    ("model".into(), serde_json::json!(&model)),
                    ("clientType".into(), serde_json::json!(&client_type)),
                    ("recovery".into(), serde_json::json!("fresh_request")),
                ])),
            );
            continue;
        }
        if !committed && retries < max_retries && stream_error_is_retryable(&error) {
            // Do not carry pre-commit events from the abandoned stream into a
            // full-history replay. In particular, duplicate Usage events can
            // make Claude Code's context meter jump after a transient reset.
            discard_sand_replay_buffer(&mut buffered);
            let delay =
                crate::retry::compute_backoff_delay(retries, error.retry_after.as_deref()).wait_ms;
            create_logger("cursor").warn(
                "sand_stream_retry",
                Some(serde_json::Map::from_iter([
                    ("attempt".into(), serde_json::json!(retries + 1)),
                    ("status".into(), serde_json::json!(error.status)),
                    ("delayMs".into(), serde_json::json!(delay)),
                ])),
            );
            let remaining = retry_deadline.saturating_duration_since(Instant::now());
            if remaining <= Duration::from_millis(delay) {
                send_sand_buffered_error(&tx, &mut buffered, error).await;
                return;
            }
            crate::retry::sleep(delay).await;
            retries += 1;
            next_open_kind = SandOpenAttemptKind::Transport;
            continue;
        }
        // Thinking/usage/session frames are speculative when no visible text
        // or tool committed. Do not leak them alongside the terminal error
        // after the replay budget is exhausted; the client should see one
        // coherent retryable API error rather than a partial answer.
        if !committed {
            discard_sand_replay_buffer(&mut buffered);
        }
        send_sand_buffered_error(&tx, &mut buffered, error).await;
        return;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandStreamEventAction {
    /// Keep transport metadata/reasoning buffered until a commit boundary.
    Buffer,
    /// A visible text/tool event commits the attempt, but the stream may
    /// continue until its terminal End marker.
    Commit,
    /// A valid terminal boundary was observed and can be delivered now.
    Complete,
    /// End arrived without a client-visible result and must be replayed.
    HollowEnd,
}

/// Classify one normalized Sand event without conflating a transport End with
/// useful model output. Compaction is the one intentional exception: a
/// reasoning-only summary becomes authoritative at End because its SSE/JSON
/// adapters promote it to text only at that boundary.
fn classify_sand_stream_event(
    event: &CursorStreamEvent,
    committed: bool,
    compaction_mode: bool,
    compaction_thinking: bool,
) -> SandStreamEventAction {
    match event {
        CursorStreamEvent::End => {
            if committed || (compaction_mode && compaction_thinking) {
                SandStreamEventAction::Complete
            } else {
                SandStreamEventAction::HollowEnd
            }
        }
        CursorStreamEvent::TextDelta { text } if !text.is_empty() => SandStreamEventAction::Commit,
        CursorStreamEvent::NativeTool { .. } => SandStreamEventAction::Commit,
        _ => SandStreamEventAction::Buffer,
    }
}

fn normalize_sand_stream_event(
    event: CursorStreamEvent,
    allowed: Option<&BTreeSet<String>>,
) -> Option<CursorStreamEvent> {
    match event {
        CursorStreamEvent::NativeTool {
            tool_use_id,
            name,
            input,
        } => {
            // Never expose a native tool that Claude Code did not advertise.
            // Resolve aliases before applying the strict client schema adapter.
            let name = resolve_advertised_name(&name, allowed)?;
            let input =
                crate::providers::cursor::native_tools::adapt_tool_input_for_client(&name, input);
            Some(CursorStreamEvent::NativeTool {
                tool_use_id,
                name,
                input,
            })
        }
        other => Some(other),
    }
}

async fn send_sand_buffered_events(
    tx: &mpsc::Sender<LiveEventResult>,
    buffered: &mut Vec<CursorStreamEvent>,
) -> bool {
    for event in buffered.drain(..) {
        if tx.send(Ok(LiveRunEvent::Cursor(event))).await.is_err() {
            return false;
        }
    }
    true
}

async fn send_sand_buffered_error(
    tx: &mpsc::Sender<LiveEventResult>,
    buffered: &mut Vec<CursorStreamEvent>,
    error: CursorError,
) {
    let _ = send_sand_buffered_events(tx, buffered).await;
    let _ = tx.send(Err(error.client_message())).await;
}

/// Drop events observed before a Sand replay becomes necessary. Sand retries
/// carry the complete history, so the replacement stream will regenerate
/// those control events (thinking/usage) and forwarding both copies would
/// double-count them downstream.
#[inline]
fn discard_sand_replay_buffer(buffered: &mut Vec<CursorStreamEvent>) {
    buffered.clear();
}

fn sand_stream_idle_timeout_secs() -> u64 {
    std::env::var("CCP_CURSOR_SAND_IDLE_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(crate::config::cursor_request_timeout_secs)
        .clamp(15, 3600)
}

fn sand_model_prefers_text_tool_bridge(model: &str) -> bool {
    let model = model.trim().to_ascii_lowercase();
    model.contains("fable")
}

/// Add the text-tool contract as a single system message while retaining the
/// structured Anthropic history. Sand's Fable provider accepts this role on
/// its text path; unlike flattening render_cursor_prompt, it preserves image
/// parts and assistant/tool-result boundaries for the next full-history turn.
fn prepend_sand_text_tool_bridge_prompt(messages: &mut Vec<SandInferenceMessage>, prompt: &str) {
    let prompt = prompt.trim();
    if prompt.is_empty() {
        return;
    }
    if let Some(system) = messages
        .iter_mut()
        .find(|message| message.role == sand_inference::ROLE_SYSTEM)
    {
        let existing = system.text.take().unwrap_or_default();
        system.parts.clear();
        system.tool_calls.clear();
        system.tool_content = None;
        system.text = Some(if existing.trim().is_empty() {
            prompt.to_string()
        } else {
            format!("{prompt}\n\n{existing}")
        });
    } else {
        messages.insert(0, SandInferenceMessage::system(prompt));
    }
}

async fn collect_sand_bridge_events(
    mut events: mpsc::Receiver<LiveEventResult>,
) -> Result<Vec<CursorStreamEvent>, String> {
    let mut output = Vec::new();
    while let Some(item) = events.recv().await {
        match item {
            Ok(LiveRunEvent::Cursor(event)) => output.push(event),
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                for tool in tools {
                    output.push(CursorStreamEvent::NativeTool {
                        tool_use_id: tool.tool_use_id,
                        name: tool.name,
                        input: tool.input,
                    });
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(output)
}

fn sand_response_headers() -> [(http::header::HeaderName, &'static str); 3] {
    [
        (http::header::CONTENT_TYPE, "text/event-stream"),
        (http::header::CACHE_CONTROL, "no-cache, no-transform"),
        (http::header::CONNECTION, "keep-alive"),
    ]
}

#[allow(clippy::too_many_arguments)]
async fn sand_tool_bridge_stream_response(
    events: mpsc::Receiver<LiveEventResult>,
    session_key: String,
    message_id: String,
    wire_model: String,
    _body: &MessagesRequest,
    allowed: Option<BTreeSet<String>>,
    _estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
) -> Response {
    let events = match collect_sand_bridge_events(events).await {
        Ok(events) => events,
        Err(error) => return json_error_from_cursor_message(error),
    };
    let (sse_bytes, _paused) =
        bridge_cursor_events_to_sse(&message_id, &wire_model, &session_key, &events, allowed);
    if let Some((handle, req_id)) = monitor {
        let (input_tokens, output_tokens) = usage_from_anthropic_sse(&sse_bytes);
        handle.stream_progress(
            &req_id,
            sse_bytes.len() as u64,
            count_sse_events(&sse_bytes),
            input_tokens,
            output_tokens,
        );
        handle.usage_updated(
            &req_id,
            input_tokens.filter(|value| *value > 0),
            output_tokens.filter(|value| *value > 0),
        );
    }
    (sand_response_headers(), sse_bytes).into_response()
}

#[allow(clippy::too_many_arguments)]
async fn sand_tool_bridge_json_response(
    events: mpsc::Receiver<LiveEventResult>,
    session_key: &str,
    message_id: &str,
    wire_model: &str,
    _body: &MessagesRequest,
    allowed: Option<BTreeSet<String>>,
    estimated_input: u64,
    monitor: Option<(&String, &crate::monitor::MonitorHandle)>,
) -> Response {
    let events = match collect_sand_bridge_events(events).await {
        Ok(events) => events,
        Err(error) => return json_error_from_cursor_message(error),
    };
    let (sse_bytes, _paused) =
        bridge_cursor_events_to_sse(message_id, wire_model, session_key, &events, allowed);
    let json = match anthropic_json_from_sse(&sse_bytes, message_id, wire_model, estimated_input) {
        Ok(json) => json,
        Err(error) => return json_error_from_cursor_message(error),
    };
    let input_tokens = json
        .pointer("/usage/input_tokens")
        .and_then(|value| value.as_u64());
    if let Some((req_id, handle)) = monitor {
        handle.usage_updated(
            req_id,
            input_tokens,
            json.pointer("/usage/output_tokens")
                .and_then(|value| value.as_u64()),
        );
    }
    (StatusCode::OK, Json(json)).into_response()
}

/// Decode the small Anthropic SSE segment generated by the collected bridge
/// back into the non-streaming Messages response. This keeps stream=false on
/// the same XML parser and pending-tool registry path as stream=true.
fn anthropic_json_from_sse(
    bytes: &[u8],
    message_id: &str,
    model: &str,
    estimated_input: u64,
) -> Result<serde_json::Value, String> {
    let parsed = crate::providers::cursor::sse::parse_sse_events(&String::from_utf8_lossy(bytes));
    let mut acc = AnthropicJsonAcc::new(estimated_input);
    let mut tool_blocks: HashMap<i64, (String, String, String)> = HashMap::new();
    let mut ended = false;
    for (_, data) in parsed {
        let kind = data
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        match kind {
            "message_start" => {
                if let Some(usage) = data.pointer("/message/usage") {
                    acc.push(&CursorStreamEvent::Usage {
                        input_tokens: json_u64_field(usage, "input_tokens"),
                        output_tokens: json_u64_field(usage, "output_tokens"),
                        cache_read_tokens: json_u64_field(usage, "cache_read_input_tokens"),
                        cache_write_tokens: json_u64_field(usage, "cache_creation_input_tokens"),
                    });
                }
            }
            "content_block_start" => {
                let index = data
                    .get("index")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                let Some(block) = data.get("content_block") else {
                    continue;
                };
                if block.get("type").and_then(serde_json::Value::as_str) == Some("tool_use") {
                    let id = block
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    let name = block
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    tool_blocks.insert(index, (id, name, String::new()));
                }
            }
            "content_block_delta" => {
                let index = data
                    .get("index")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                let Some(delta) = data.get("delta") else {
                    continue;
                };
                match delta.get("type").and_then(serde_json::Value::as_str) {
                    Some("text_delta") => {
                        if let Some(text) = delta.get("text").and_then(serde_json::Value::as_str) {
                            acc.push(&CursorStreamEvent::TextDelta {
                                text: text.to_string(),
                            });
                        }
                    }
                    Some("thinking_delta") => {
                        if let Some(text) =
                            delta.get("thinking").and_then(serde_json::Value::as_str)
                        {
                            acc.push(&CursorStreamEvent::ThinkingDelta {
                                text: text.to_string(),
                            });
                        }
                    }
                    Some("input_json_delta") => {
                        if let Some((_, _, input)) = tool_blocks.get_mut(&index) {
                            input.push_str(
                                delta
                                    .get("partial_json")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or(""),
                            );
                        }
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                let index = data
                    .get("index")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0);
                if let Some((id, name, input)) = tool_blocks.remove(&index) {
                    let input =
                        serde_json::from_str(&input).unwrap_or_else(|_| serde_json::json!({}));
                    acc.push_native_tool(id, name, input);
                }
            }
            "message_delta" => {
                if let Some(usage) = data.get("usage") {
                    acc.push(&CursorStreamEvent::Usage {
                        input_tokens: json_u64_field(usage, "input_tokens"),
                        output_tokens: json_u64_field(usage, "output_tokens"),
                        cache_read_tokens: json_u64_field(usage, "cache_read_input_tokens"),
                        cache_write_tokens: json_u64_field(usage, "cache_creation_input_tokens"),
                    });
                }
            }
            "message_stop" => ended = true,
            "error" => {
                let message = data
                    .pointer("/error/message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("Sand tool bridge returned an error");
                return Err(message.to_string());
            }
            _ => {}
        }
    }
    for (_, (id, name, input)) in tool_blocks {
        let input = serde_json::from_str(&input).unwrap_or_else(|_| serde_json::json!({}));
        acc.push_native_tool(id, name, input);
    }
    if !acc.has_useful() {
        return Err("Sand tool bridge produced no useful progress".into());
    }
    if !ended {
        create_logger("cursor").warn(
            "sand_tool_bridge_missing_message_stop",
            Some(serde_json::Map::from_iter([
                ("messageId".into(), serde_json::json!(message_id)),
                ("model".into(), serde_json::json!(model)),
            ])),
        );
    }
    Ok(acc.into_message_json(message_id, model))
}

fn json_u64_field(value: &serde_json::Value, key: &str) -> u64 {
    value.get(key).and_then(|value| value.as_u64()).unwrap_or(0)
}

enum LiveStartPeek {
    Retryable(String),
    Ready {
        events: mpsc::Receiver<LiveEventResult>,
        /// The peek observed client-committing model output/tool completion.
        /// Session/usage/thinking metadata is deliberately not health proof:
        /// Cursor can emit it immediately before a delayed policy 429.
        observed_healthy_event: bool,
    },
}

#[derive(Clone)]
struct LiveRetryStart {
    client: CursorHttpClient,
    /// The bearer actually used by the current generation. A start can refresh
    /// or hot-switch accounts internally; subsequent empty-turn/transport
    /// retries and policy attribution must continue with that effective token.
    effective_token: Arc<Mutex<String>>,
    user_text: String,
    /// Full Anthropic-history prompt retained for a one-shot recovery that
    /// rotates a poisoned Cursor conversation (KV overflow, stale assets,
    /// etc.). `user_text` may be a checkpoint delta and is therefore not
    /// sufficient after the conversation id changes.
    reset_user_text: String,
    /// Conversation binding observed while rendering this request.  The live
    /// client compares it with its just-in-time continuation snapshot and
    /// switches to `reset_user_text` when another task rotated the binding.
    ///
    /// This is shared with late retries. A recovery can intentionally rotate
    /// the Cursor conversation; retaining the original immutable UUID would
    /// make every subsequent checkpoint continuation look like another
    /// rotation and replay the complete history (including completed tools).
    expected_conversation_id: Arc<Mutex<Option<String>>>,
    model: String,
    /// Images appropriate for the currently persisted Cursor continuation.
    /// With a checkpoint this is limited to the current user turn.
    images: Vec<CursorSelectedImage>,
    /// Full-history images retained for the one recovery path that clears the
    /// Cursor conversation and replays the original Anthropic history.
    reset_retry_images: Vec<CursorSelectedImage>,
    /// Shared one-shot fence for stale selected-image recovery. It spans the
    /// initial open, late stream pump, and any internal restart so one request
    /// cannot create an unbounded fresh-UUID wave.
    image_recovery_attempted: Arc<AtomicBool>,
    /// The exact refreshed image metadata used by the first stale-image
    /// recovery. A late KV rotation can happen after that recovery; retaining
    /// the wave prevents it from falling back to stale UUIDs (or minting a
    /// second, unrelated set while a queued asset upload is still settling).
    image_recovery_images: Arc<Mutex<Option<Vec<CursorSelectedImage>>>>,
    /// Shared one-shot fence for KV blob-store overflow recovery.  It spans
    /// initial-open peeks and late stream-pump retries so one logical request
    /// cannot repeatedly rotate conversations when the upstream keeps
    /// rejecting the same oversized state.
    kv_recovery_attempted: Arc<AtomicBool>,
    /// Shared one-shot fence for Claude Code compaction loop-detection
    /// recovery. It spans the initial open and late stream pump so a malformed
    /// compact request can trigger at most one fresh-conversation replay.
    compaction_recovery_attempted: Arc<AtomicBool>,
    /// Shared one-shot fence for an upstream Cursor response that says the
    /// conversation already has an active Run.  This is distinct from the
    /// local live registry's busy state: the upstream response means our
    /// previous open likely survived after its HTTP stream was lost, so the
    /// only useful recovery is one fresh conversation replay.  Keeping the
    /// fence on the logical request prevents the late retry pump from
    /// repeatedly rotating UUIDs when the remote Run remains stuck.
    upstream_active_recovery_attempted: Arc<AtomicBool>,
    custom_system: Option<String>,
    session_id: String,
    agent_id: Option<String>,
    parent_agent_id: Option<String>,
    /// Stable account partition shared by the initial opener and all late
    /// retries.  The value is updated when bounded account failover swaps the
    /// effective bearer so state keys follow the replacement account.
    account_key: Arc<Mutex<String>>,
    allowed: Option<BTreeSet<String>>,
    mcp_tools: Option<crate::providers::cursor::proto::McpTools>,
    request_context: crate::providers::cursor::proto::RequestContext,
    fingerprint: Vec<u8>,
    has_refresh: bool,
    /// Fresh Anthropic streaming requests have already committed their SSE
    /// response (and therefore emit heartbeats while they wait). Keep their
    /// same-session admission wait open until the observed generation reaches
    /// a terminal/replacement state. Pre-response callers leave this false so
    /// `/v1/responses` and tool-result continuations retain bounded waits.
    unbounded_conflict_wait: bool,
    /// Captured when the logical request starts; retries must keep the same
    /// client identity even if the live routing config is edited meanwhile.
    client_type: String,
    /// Stable identity for this logical request. Internal fresh-conversation
    /// retries reuse it so quota-sentinel observations cannot merge with a
    /// different request for the same account/model.
    request_sequence_id: String,
    /// Shared across the initial start and all late retries so account-pool
    /// failover remains bounded for one logical request.
    account_failover_state: SharedAccountFailoverState,
    /// Context compaction uses output-text framing even when Cursor emits
    /// reasoning deltas. Keep this bit stable across transport retries.
    compaction_mode: bool,
}

fn live_retry_user_text<'a>(original: &'a str, error: &str) -> &'a str {
    if live_error_needs_checkpoint_continue(error) {
        EMPTY_TURN_CHECKPOINT_CONTINUE_PROMPT
    } else {
        original
    }
}

fn prepare_live_retry_conversation_for_account(
    session_id: &str,
    agent_id: Option<&str>,
    account_key: Option<&str>,
    error: &str,
) -> bool {
    if live_error_is_empty_turn_retry(error)
        && !live_error_needs_checkpoint_continue(error)
        // `recover_empty_turn_if_needed` already reset this conversation when
        // it appended the stale-reset note. Avoid replacing it a second time,
        // which would unnecessarily discard a just-created binding.
        && !error.contains("stale Cursor conversation reset")
    {
        let key = live_retry_conversation_key_for_account(session_id, agent_id, account_key);
        conversation::reset(&key);
        true
    } else {
        false
    }
}

/// Compatibility wrapper for callers that predate account-partitioned
/// conversation state. New request paths should use the account-aware helper
/// above so retries cannot cross account lanes.
#[cfg(test)]
fn prepare_live_retry_conversation(session_id: &str, error: &str) -> bool {
    // Legacy unit-test callers pass the already-composed conversation key.
    // Keep this compatibility shim byte-for-byte equivalent to the old
    // unpartitioned helper; production request paths use the account-aware
    // variant above.
    if live_error_is_empty_turn_retry(error)
        && !live_error_needs_checkpoint_continue(error)
        && !error.contains("stale Cursor conversation reset")
    {
        conversation::reset(session_id);
        true
    } else {
        false
    }
}

fn live_retry_conversation_key_for_account(
    session_id: &str,
    agent_id: Option<&str>,
    account_key: Option<&str>,
) -> String {
    live_run_key_for(LiveRunIdentity {
        session_id,
        agent_id,
        parent_agent_id: None,
        account_key,
    })
}

#[cfg(test)]
fn live_retry_conversation_key(session_id: &str, agent_id: Option<&str>) -> String {
    live_retry_conversation_key_for_account(session_id, agent_id, None)
}

fn live_retry_needs_fresh_history(error: &str) -> bool {
    live_error_is_empty_turn_retry(error) && !live_error_needs_checkpoint_continue(error)
}

fn live_request_image_sets(
    body: &MessagesRequest,
    has_checkpoint: bool,
) -> (Vec<CursorSelectedImage>, Vec<CursorSelectedImage>) {
    let images = request::cursor_selected_images_for_continuation(body, has_checkpoint);
    let reset_retry_images = if has_checkpoint {
        request::cursor_selected_images_for_continuation(body, false)
    } else {
        images.clone()
    };
    (images, reset_retry_images)
}

/// Select image metadata for a one-shot KV conversation rotation.
///
/// A stale-image recovery may already have rebuilt the selected-image entries
/// for this request.  Re-generating UUIDs when the subsequent KV error arrives
/// creates a second asset identity wave and can make Cursor associate queued
/// image bytes with the wrong turn.  Keep the first refreshed set; otherwise
/// issue fresh UUIDs from the original full-history payload.
fn kv_recovery_images(
    current_images: &[CursorSelectedImage],
    reset_images: &[CursorSelectedImage],
    image_recovery_attempted: bool,
) -> Vec<CursorSelectedImage> {
    if image_recovery_attempted {
        current_images.to_vec()
    } else {
        refresh_image_uuids(reset_images)
    }
}

/// Return the request-scoped image recovery wave, minting it exactly once.
///
/// `image_recovery_attempted` is an atomic fence, but it intentionally carries
/// no payload. The late stream retry therefore needs this small side channel
/// to reuse the UUIDs selected by an earlier initial-open recovery. Keeping the
/// bytes and MIME/path metadata cloned here is cheap (the base64 payload is
/// already reference-owned by the request) and avoids a second UUID wave.
fn cached_image_recovery_images(
    shared: &Arc<Mutex<Option<Vec<CursorSelectedImage>>>>,
    reset_images: &[CursorSelectedImage],
) -> Vec<CursorSelectedImage> {
    let mut slot = shared.lock().unwrap_or_else(|poison| poison.into_inner());
    if let Some(images) = slot.as_ref() {
        return images.clone();
    }
    let refreshed = refresh_image_uuids(reset_images);
    *slot = Some(refreshed.clone());
    refreshed
}

/// Read an already-minted image recovery wave without creating a new one.
/// Generic late retries (for example an empty turn after a successful stale
/// image recovery) must reuse that wave; minting another UUID set there would
/// make Cursor's asset index observe two competing identities for one turn.
fn cached_image_recovery_snapshot(
    shared: &Arc<Mutex<Option<Vec<CursorSelectedImage>>>>,
) -> Option<Vec<CursorSelectedImage>> {
    shared
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

/// Account failover creates a conversation in a different Cursor account.
/// Image UUIDs are account-scoped in Cursor's asset index, so a wave cached
/// for the exhausted account must not be reused by the replacement account.
/// Replace the request-scoped cache atomically and use the new metadata for
/// every subsequent retry in that account.
fn fresh_image_recovery_images_for_account(
    shared: &Arc<Mutex<Option<Vec<CursorSelectedImage>>>>,
    reset_images: &[CursorSelectedImage],
) -> Vec<CursorSelectedImage> {
    let refreshed = refresh_image_uuids(reset_images);
    *shared.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(refreshed.clone());
    refreshed
}

/// Match image lookup failures across the structured Cursor error fields.
/// Connect END errors often put the useful text in `detail`, while HTTP/SSE
/// failures expose it through `client_message`; checking all three keeps the
/// one-shot re-upload recovery consistent for live and buffered paths.
fn cursor_error_is_missing_image(error: &CursorError) -> bool {
    let client_message = error.client_message();
    cursor_connect_error_is_missing_image(&client_message)
        || cursor_connect_error_is_missing_image(&error.message)
        || error
            .detail
            .as_deref()
            .is_some_and(cursor_connect_error_is_missing_image)
}

impl LiveRetryStart {
    fn expected_conversation_snapshot(&self) -> Option<String> {
        self.expected_conversation_id
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn effective_token(&self) -> String {
        self.effective_token
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn account_key(&self) -> String {
        self.account_key
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn retry_user_text(&self, error: &str) -> &str {
        live_retry_user_text(&self.user_text, error)
    }

    async fn start(
        &self,
        reservation: Option<LiveRunReservation>,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        self.start_with_user_text(&self.user_text, reservation)
            .await
    }

    async fn start_after_error(
        &self,
        error: &str,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        let mut account_key = self.account_key();
        // An upstream "already active" response is different from the local
        // registry busy diagnostic.  It usually means the prior Cursor open
        // was accepted but its response channel disappeared before a handle
        // could be published.  Retrying the same conversation reproduces the
        // conflict forever; rotate the account-scoped conversation exactly
        // once and replay the complete Anthropic history instead.  A second
        // conflict is surfaced as-is so Claude Code gets a bounded terminal
        // error rather than an unbounded 503 loop.
        if live_error_is_upstream_already_active(error) && !is_local_live_busy_text(error) {
            if !claim_upstream_active_recovery(&self.upstream_active_recovery_attempted) {
                return Err(CursorError::new(503, error, None));
            }
            let conversation_key = live_retry_conversation_key_for_account(
                &self.session_id,
                self.agent_id.as_deref(),
                Some(&account_key),
            );
            conversation::reset(&conversation_key);
            *self
                .expected_conversation_id
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = None;
            // Preserve a UUID wave already refreshed by image recovery.  If
            // none exists, the original account-scoped image ids remain valid
            // and can be replayed on the fresh conversation.
            let images = cached_image_recovery_snapshot(&self.image_recovery_images)
                .unwrap_or_else(|| self.reset_retry_images.clone());
            create_logger("cursor").warn(
                "upstream_live_run_conflict_recovery",
                Some(serde_json::Map::from_iter([
                    ("sessionId".into(), serde_json::json!(&self.session_id)),
                    ("model".into(), serde_json::json!(&self.model)),
                    ("clientType".into(), serde_json::json!(&self.client_type)),
                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                    ("attempt".into(), serde_json::json!(1)),
                ])),
            );
            return self
                .start_with_user_text_and_images(&self.reset_user_text, &images, None)
                .await;
        }
        // A policy response can arrive after the initial live-open peek (for
        // example Cursor accepts the Run, then emits a delayed Sand
        // quota/error frame).  The normal late-retry classifier keeps this
        // path terminal so it does not replay an already accepted operation,
        // but no client-visible text/tool event has committed at this point.
        // Move the logical request to one unused account and replay the full
        // Anthropic history on a fresh account-scoped conversation.  The
        // The shared state bounds this to the number of other saved accounts
        // (with a hard ceiling), including retries started by replacements.
        if is_account_failover_policy_error(error) {
            let current_token = self.effective_token();
            let Some(replacement) = account_failover_replacement_token_async(
                current_token.clone(),
                self.model.clone(),
                self.client_type.clone(),
                Arc::clone(&self.account_failover_state),
            )
            .await
            else {
                let mut terminal = CursorError::new(429, error, None);
                terminal.retry_after =
                    policy_rate_limit_breaker_state(&self.model, &self.client_type, &current_token)
                        .map(|state| state.retry_after_secs.to_string());
                return Err(terminal);
            };
            *self
                .effective_token
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = replacement.clone();
            account_key = cursor_account_key_for_token_async(replacement.clone()).await;
            *self
                .account_key
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = account_key.clone();
            let conversation_key = live_retry_conversation_key_for_account(
                &self.session_id,
                self.agent_id.as_deref(),
                Some(&account_key),
            );
            conversation::reset(&conversation_key);
            *self
                .expected_conversation_id
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = None;
            // Image assets are account-scoped. Allow one bounded upload retry
            // for the replacement account even when the exhausted account
            // already consumed the request's image-recovery fence.
            self.image_recovery_attempted
                .store(false, Ordering::Release);
            let images = fresh_image_recovery_images_for_account(
                &self.image_recovery_images,
                &self.reset_retry_images,
            );
            create_logger("cursor").warn(
                "late_policy_account_failover",
                Some(serde_json::Map::from_iter([
                    ("sessionId".into(), serde_json::json!(&self.session_id)),
                    ("model".into(), serde_json::json!(&self.model)),
                    ("clientType".into(), serde_json::json!(&self.client_type)),
                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                ])),
            );
            return self
                .start_with_user_text_and_images(&self.reset_user_text, &images, None)
                .await;
        }
        if self.compaction_mode && live_error_is_agent_looping_detected(error) {
            // Cursor's loop detector can leave the compact lane bound to a
            // poisoned checkpoint. Rotate that lane once and replay the
            // summary prompt; a second detector hit is terminal for this
            // request and must not create an endless Claude Code retry wave.
            if self
                .compaction_recovery_attempted
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(CursorError::new(400, error, None));
            }
            let conversation_key = live_retry_conversation_key_for_account(
                &self.session_id,
                self.agent_id.as_deref(),
                Some(&account_key),
            );
            conversation::reset(&conversation_key);
            create_logger("cursor").warn(
                "compact_loop_recovery",
                Some(serde_json::Map::from_iter([
                    ("sessionId".into(), serde_json::json!(&self.session_id)),
                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                    ("attempt".into(), serde_json::json!(1)),
                ])),
            );
            return self
                .start_with_user_text_and_images(
                    &self.reset_user_text,
                    &self.reset_retry_images,
                    None,
                )
                .await;
        }
        // Ordinary hollow turns are safe to replay because no client-visible
        // text/tool committed. Fence every retry onto a fresh Cursor
        // conversation even if an unusual upstream termination path failed to
        // perform the driver's normal empty-turn reset. A post-tool checkpoint
        // continuation is different: clearing it could replay completed tools.
        let conversation_key = live_retry_conversation_key_for_account(
            &self.session_id,
            self.agent_id.as_deref(),
            Some(&account_key),
        );
        if live_error_is_kv_blob_overflow_replayable(error) {
            // Cursor's KV store is append-only for the lifetime of a remote
            // conversation. Replaying the delta against the same id cannot
            // make progress, so rotate once and replay complete history.
            if self
                .kv_recovery_attempted
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(CursorError::new(413, error, None));
            }
            conversation::reset(&conversation_key);
            create_logger("cursor").warn(
                "kv_blob_overflow_recovery",
                Some(serde_json::Map::from_iter([
                    ("sessionId".into(), serde_json::json!(&self.session_id)),
                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                    ("replay".into(), serde_json::json!("full_history")),
                ])),
            );
            // A prior image-recovery attempt may have left the original
            // selected-image UUIDs stale in Cursor's asset index.  The fresh
            // conversation must receive new metadata even when KV recovery
            // happens later in the same logical request.
            let images =
                cached_image_recovery_images(&self.image_recovery_images, &self.reset_retry_images);
            return self
                .start_with_user_text_and_images(&self.reset_user_text, &images, None)
                .await;
        }
        if cursor_connect_error_is_missing_image(error) {
            // The same request can encounter the stale asset during the
            // initial open *and* again when a late stream pump is restarted.
            // Share one atomic fence across both paths so a persistent
            // upstream Image-not-found response cannot create a fresh UUID /
            // conversation wave on every retry.
            if self
                .image_recovery_attempted
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(CursorError::new(502, error, None));
            }
            // A stale selected-image id can survive checkpoint clearing in the
            // upstream asset index. Rotate the binding and replay the original
            // Anthropic bytes with fresh UUIDs for one bounded recovery.
            conversation::reset(&conversation_key);
            let images =
                cached_image_recovery_images(&self.image_recovery_images, &self.reset_retry_images);
            create_logger("cursor").warn(
                "image_checkpoint_recovery",
                Some(serde_json::Map::from_iter([
                    ("sessionId".into(), serde_json::json!(&self.session_id)),
                    ("imageCount".into(), serde_json::json!(images.len())),
                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                ])),
            );
            self.start_with_user_text_and_images(&self.reset_user_text, &images, None)
                .await
        } else {
            prepare_live_retry_conversation_for_account(
                &self.session_id,
                self.agent_id.as_deref(),
                Some(&account_key),
                error,
            );
            let cached_images = cached_image_recovery_snapshot(&self.image_recovery_images);
            let images = if live_retry_needs_fresh_history(error) {
                cached_images.as_deref().unwrap_or(&self.reset_retry_images)
            } else {
                &self.images
            };
            self.start_with_user_text_and_images(self.retry_user_text(error), images, None)
                .await
        }
    }

    async fn start_with_user_text(
        &self,
        user_text: &str,
        reservation: Option<LiveRunReservation>,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        self.start_with_user_text_and_images(user_text, &self.images, reservation)
            .await
    }

    async fn start_with_user_text_and_images(
        &self,
        user_text: &str,
        images: &[CursorSelectedImage],
        reservation: Option<LiveRunReservation>,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        // Do not preflight before registry reconciliation: this path also
        // serves identical attach/replay and late tool-result retries, which
        // create no new upstream load. The start loop checks the breaker after
        // it has a reservation and immediately before the first Cursor open.
        let expected_conversation_id = self.expected_conversation_snapshot();
        // Keep the opener's actual binding in a separate slot.  The session
        // map may be reset by another recovery task as soon as the upstream
        // open succeeds; publishing from a post-await map lookup would then
        // associate this generation with that unrelated replacement.
        let opened_conversation_id = Arc::new(Mutex::new(None));
        let account_key = self.account_key();
        let result = start_live_events_with_retries_with_client_type(
            self.client.clone(),
            self.effective_token(),
            user_text,
            &self.model,
            images,
            // Keep the full-history slice available to the initial-open
            // recovery path.  A checkpoint-backed tool-result continuation
            // intentionally has an empty `images` slice, but a stale Cursor
            // image error still needs the original bytes to rebuild a fresh
            // conversation.
            Some(&self.reset_retry_images),
            Some(&self.reset_user_text),
            self.custom_system.as_deref(),
            LiveRunIdentity {
                session_id: &self.session_id,
                agent_id: self.agent_id.as_deref(),
                parent_agent_id: self.parent_agent_id.as_deref(),
                account_key: Some(&account_key),
            },
            self.allowed.clone(),
            self.mcp_tools.clone(),
            self.request_context.clone(),
            self.fingerprint.clone(),
            reservation,
            self.has_refresh,
            &self.client_type,
            self.unbounded_conflict_wait,
            self.compaction_mode,
            Some(&self.request_sequence_id),
            Some(Arc::clone(&self.effective_token)),
            Some(Arc::clone(&self.image_recovery_attempted)),
            Some(Arc::clone(&self.image_recovery_images)),
            Some(Arc::clone(&self.kv_recovery_attempted)),
            Some(Arc::clone(&self.compaction_recovery_attempted)),
            Some(Arc::clone(&self.upstream_active_recovery_attempted)),
            Some(Arc::clone(&self.account_failover_state)),
            Some(Arc::clone(&opened_conversation_id)),
            LiveStartRecovery {
                expected_conversation_id: expected_conversation_id.as_deref(),
                reset_user_text: Some(&self.reset_user_text),
                reset_images: Some(&self.reset_retry_images),
            },
        )
        .await;
        // Only a successful start owns a generation whose binding should be
        // used by subsequent late retries.  If every attempt failed, retain
        // the old snapshot so the next caller can detect a concurrent reset
        // and request a full-history replay.
        if result.is_ok()
            && let Some(conversation_id) = opened_conversation_id
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
        {
            *self
                .expected_conversation_id
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(conversation_id);
        }
        result
    }
}

/// Decide whether a streaming request should receive its SSE envelope before
/// live admission finishes.
///
/// The Responses adapter historically held HTTP headers for every request so
/// a pre-output Cursor error could be returned as JSON.  Holding a *fresh*
/// stream, however, routes it through the nested-resume probe and produces a
/// local 503 after `LIVE_NESTED_WAIT_DEFAULT_MS` whenever another generation
/// owns the session.  Fresh streams now get the envelope immediately (the
/// adapter still peeks the translated SSE for pre-output 4xx classification);
/// tool-result/non-streaming requests retain the held status path.
fn commit_streaming_live_sse_before_start_live(
    want_stream: bool,
    hold_http: bool,
    defer_fresh_stream: bool,
) -> bool {
    want_stream && (!hold_http || defer_fresh_stream)
}

const LIVE_RUN_BUSY_MESSAGE: &str =
    "A Cursor live run is already active for this session; retry after it advances";

/// The local registry emits [`LIVE_RUN_BUSY_MESSAGE`] when no upstream open
/// was attempted.  It must continue to use the normal attach/wait path rather
/// than resetting a healthy conversation merely because a caller timed out.
fn is_local_live_busy_text(message: &str) -> bool {
    let trimmed = message.trim();
    trimmed.eq_ignore_ascii_case(LIVE_RUN_BUSY_MESSAGE)
        || trimmed
            .strip_prefix("Cursor error 503:")
            .is_some_and(|rest| rest.trim().eq_ignore_ascii_case(LIVE_RUN_BUSY_MESSAGE))
}

/// Claim the one recovery slot for an upstream session-conflict response.
/// Keeping this tiny atomic operation in one helper makes the no-loop
/// guarantee explicit and straightforward to regression-test.
fn claim_upstream_active_recovery(fence: &AtomicBool) -> bool {
    fence
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}
// A Claude Code retry can arrive while the previous HTTP segment is being
// detached, replayed, or finishing its last Cursor turn.  Short waits turn
// that normal handoff into a repeated client-visible 503 storm.  Keep the
// waits bounded, but long enough to cover the common Cursor step/reconnect
// window; SSE callers continue receiving heartbeats while this task waits.
const LIVE_ATTACH_WAIT_DEFAULT_MS: u64 = 15_000;
const LIVE_ATTACH_WAIT_MAX_MS: u64 = 60_000;
// A normal Claude Code turn can legitimately overlap the tail of the prior
// turn (for example an edited prompt arriving while Fable is still thinking).
// Streaming callers get an SSE heartbeat while this bounded single-flight
// handoff runs, so a short pre-admission timeout only creates a retry storm.
const LIVE_CONFLICT_WAIT_DEFAULT_MS: u64 = 180_000;
const LIVE_CONFLICT_WAIT_MAX_MS: u64 = 600_000;
// This wait runs before the Anthropic response has been committed, so it must
// stay below Claude Code's stream watchdog.  The longer conflict wait above is
// used only after the streaming response has been handed to the client.
const LIVE_RESUME_WAIT_DEFAULT_MS: u64 = 5_000;
// Both tool-result and nested-request admission happen before the downstream
// response is committed. Do not allow an environment override to hold a
// streaming Claude Code request silent beyond its event watchdog.
const LIVE_RESUME_WAIT_MAX_MS: u64 = 5_000;
// `await_live_run_resume_for_operation` runs before an HTTP response is
// committed. Keep its attach probe below the downstream client's idle
// watchdog; the post-SSE start path uses the longer attach budget above.
const LIVE_RESUME_ATTACH_WAIT_DEFAULT_MS: u64 = 4_000;
const LIVE_RESUME_ATTACH_WAIT_MAX_MS: u64 = 5_000;
const LIVE_NESTED_WAIT_DEFAULT_MS: u64 = 1_500;
const LIVE_NESTED_WAIT_MAX_MS: u64 = 5_000;

fn live_run_busy_error() -> CursorError {
    let mut error = CursorError::new(503, LIVE_RUN_BUSY_MESSAGE, None);
    error.retry_after = Some(local_overload_retry_after());
    error
}

/// Return the deadline used while waiting for a different operation in the
/// same session. Once an SSE response is committed, the downstream heartbeat
/// keeps the connection alive and there is no useful reason to fail a healthy
/// long-running generation merely because it crossed an arbitrary wall clock
/// limit. `None` is therefore intentional for fresh streaming starts.
fn live_conflict_wait_deadline(unbounded: bool) -> Option<Instant> {
    if unbounded {
        None
    } else {
        Some(
            Instant::now()
                + Duration::from_millis(
                    env_u64_millis(
                        "CCP_CURSOR_LIVE_CONFLICT_WAIT_MS",
                        LIVE_CONFLICT_WAIT_DEFAULT_MS,
                    )
                    .clamp(500, LIVE_CONFLICT_WAIT_MAX_MS),
                ),
        )
    }
}

fn conflict_wait_expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|deadline| Instant::now() >= deadline)
}

fn conflict_wait_active(deadline: Option<Instant>) -> bool {
    !conflict_wait_expired(deadline)
}

/// Whether an occupied live slot may be handed to the asynchronous streaming
/// admission path.  The path emits Anthropic heartbeats immediately, while
/// tool-result continuations and `/v1/responses` keep their pre-response
/// semantics so they can return a structured JSON error when needed.
fn defer_fresh_stream_admission(
    want_stream: bool,
    hold_http_until_live_open: bool,
    has_tool_results: bool,
) -> bool {
    want_stream && !hold_http_until_live_open && !has_tool_results
}

/// A fresh streamed turn must not be sent through the nested-resume probe.
///
/// `/v1/responses` deliberately keeps its HTTP status uncommitted until the
/// live run opens (`hold_http_until_live_open = true`) so a pre-output Cursor
/// error can be returned as JSON.  That flag used to make Responses requests
/// take the 1.5s nested-resume waiter, which then surfaced a local 503 for a
/// perfectly healthy *different* generation.  The normal start path already
/// has generation-bound conflict/replacement handling; let both streaming
/// surfaces use it.  Tool-result continuations remain on the bounded resume
/// path because their payload must be matched to the exact pending batch.
fn fresh_stream_can_skip_resume_probe(_want_stream: bool, has_tool_results: bool) -> bool {
    // A non-streaming Responses caller has no SSE envelope/heartbeat while it
    // waits in the pre-admission phase.  Sending such requests through the
    // short nested-resume probe (1.5s) turns an otherwise normal queued turn
    // into a client-visible 503 storm whenever the preceding Grok generation
    // is still running.  Fresh requests without tool results are safe to let
    // the normal single-flight start loop queue; that loop has the longer
    // conflict deadline and can attach/replay an identical operation.  Tool
    // result requests stay on the exact bounded probe because their ids are
    // generation-scoped.
    !has_tool_results
}

fn live_probe_cursor_error(message: String) -> CursorError {
    let status = crate::retry::classify_proxy_error_status(502, &message);
    CursorError::new(status, message, None)
}

/// A retry of the same logical operation can race the driver while it is
/// handing the downstream channel from the original request to the retry.
/// Wait briefly for that handoff instead of immediately returning local 503.
/// The fingerprint check and the existing attach protocol preserve exactly-once
/// execution; a different operation never enters this path.
async fn attach_live_run_with_bounded_wait(
    run: Arc<CursorLiveRunHandle>,
    fingerprint: u64,
) -> Option<mpsc::Receiver<LiveEventResult>> {
    let wait_ms = env_u64_millis(
        "CCP_CURSOR_LIVE_ATTACH_WAIT_MS",
        LIVE_ATTACH_WAIT_DEFAULT_MS,
    )
    .clamp(500, LIVE_ATTACH_WAIT_MAX_MS);
    attach_live_run_with_wait(run, fingerprint, wait_ms).await
}

async fn attach_live_run_with_pre_response_wait(
    run: Arc<CursorLiveRunHandle>,
    fingerprint: u64,
) -> Option<mpsc::Receiver<LiveEventResult>> {
    let wait_ms = env_u64_millis(
        "CCP_CURSOR_LIVE_RESUME_ATTACH_WAIT_MS",
        LIVE_RESUME_ATTACH_WAIT_DEFAULT_MS,
    )
    .clamp(500, LIVE_RESUME_ATTACH_WAIT_MAX_MS);
    attach_live_run_with_wait(run, fingerprint, wait_ms).await
}

async fn attach_live_run_with_wait(
    run: Arc<CursorLiveRunHandle>,
    fingerprint: u64,
    wait_ms: u64,
) -> Option<mpsc::Receiver<LiveEventResult>> {
    let deadline = Instant::now() + Duration::from_millis(wait_ms);
    loop {
        if run.request_fingerprint() != fingerprint || run.is_completed() || run.is_command_closed()
        {
            return None;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let attempt = remaining.min(Duration::from_millis(500));
        if let Ok(Ok(events)) =
            tokio::time::timeout(attempt, run.attach_for_operation(fingerprint)).await
        {
            return Some(events);
        }
        if !remaining.is_zero() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Serve the retained final segment of an already-completed identical
/// operation: exactly-once delivery for a client that never received the
/// original response (crash, timeout, dropped connection).
///
/// Replays lazily through a small bounded channel: cloning the whole segment
/// upfront multiplied a large replay by every concurrent duplicate (a 113-wide
/// retry burst of an 8 MiB replay would allocate ~900 MiB).
fn replay_completed_turn_channel(
    session_id: &str,
    events: &Arc<Vec<LiveRunEvent>>,
) -> mpsc::Receiver<LiveEventResult> {
    const REPLAY_CHANNEL_CAP: usize = 32;
    // A completed snapshot normally ends in `CursorStreamEvent::End` (or a
    // NativeToolBatch).  A narrow race at clean upstream EOF can seal a
    // useful text segment just before that marker is recorded, however.  Do
    // not replay such a segment into `live_sse_response` without a terminal:
    // the adapter quite correctly turns a closed, non-finalized stream into a
    // 502, which then makes every identical Grok retry fail the same way.
    // Append one local End only when the snapshot contains client-visible text
    // or a native tool; metadata/thinking-only snapshots remain hollow and are
    // left untouched for the normal retry classifier.
    let synthesized_terminal =
        crate::providers::cursor::live::replay_needs_synthetic_terminal(events);
    let replay_events = if synthesized_terminal {
        let mut copy = (**events).clone();
        copy.push(LiveRunEvent::Cursor(
            crate::providers::cursor::response::CursorStreamEvent::End,
        ));
        Arc::new(copy)
    } else {
        Arc::clone(events)
    };
    let (tx, rx) = mpsc::channel(REPLAY_CHANNEL_CAP.min(replay_events.len().max(1)));
    create_logger("cursor").info(
        "live_replay_completed_turn",
        Some(serde_json::Map::from_iter([
            ("sessionId".into(), serde_json::json!(session_id)),
            (
                "replayedEvents".into(),
                serde_json::json!(replay_events.len()),
            ),
            (
                "synthesizedTerminal".into(),
                serde_json::json!(synthesized_terminal),
            ),
        ])),
    );
    let events = replay_events;
    tokio::spawn(async move {
        for event in events.iter() {
            if tx.send(Ok(event.clone())).await.is_err() {
                // Receiver dropped mid-replay; the tombstone still holds the
                // segment for the next identical retry.
                return;
            }
        }
    });
    rx
}

fn live_ambiguous_accept_error() -> CursorError {
    CursorError::new(
        409,
        "Cursor live run acceptance is ambiguous; retrying could duplicate execution",
        None,
    )
}

/// Hard ceiling for account failover swaps in one logical request. The actual
/// budget is derived from the saved account pool, so a pool of twelve accounts
/// can try all eleven alternatives without allowing an unbounded retry storm.
const MAX_ACCOUNT_FAILOVER_SWAPS: u32 = 16;

/// Decide whether an unbound Sand request should inspect the saved-account
/// pool before entering the open gate. `None` means this account/model lane
/// has not been created yet, so the active account is still the least
/// surprising destination. A lane with one or more free permits likewise has
/// no reason to incur registry I/O or alter the user's selected account; only
/// a fully saturated lane, or a route already in cooldown, needs a replacement
/// candidate.
fn sand_account_rebalance_needed(
    current_available: Option<usize>,
    current_route_healthy: bool,
) -> bool {
    !current_route_healthy || current_available == Some(0)
}

fn sand_usage_evidence_is_exhausted(token: &str) -> bool {
    crate::providers::cursor::usage::cached_sand_usage_evidence(token).is_some_and(|evidence| {
        evidence.usage_percent >= 100.0 || evidence.has_available_usage == Some(false)
    })
}

/// Rebalance an unbound Sand request when its currently selected account lane
/// has no open capacity (or is already in a route cooldown).
///
/// Normal, low-volume traffic keeps the active account semantics users expect.
/// Once that lane is full, choosing among idle saved accounts before entering
/// the admission queue lets a 512-request fan-out use the account pool rather
/// than parking every request behind four permits on one account. Explicit
/// `cursor.modelAccounts` routes are never rebalanced.
async fn maybe_rebalance_sand_account(
    route_model: &str,
    sand_model: &str,
    client_type: &str,
    selection: CursorAuthSelection,
) -> CursorAuthSelection {
    let sand_model = resolve_sand_model_id(sand_model);
    if !client_type.trim().eq_ignore_ascii_case("sand")
        || crate::config::cursor_account_for_model(route_model).is_some()
        || crate::config::cursor_account_for_model(&sand_model).is_some()
        || selection.account_id.is_none()
    {
        return selection;
    }

    let current_token = selection.auth.access_token.clone();
    let current_available_opt =
        crate::providers::cursor::sand_inference::sand_open_available_permits(
            &current_token,
            &sand_model,
        );
    let current_available = current_available_opt
        .unwrap_or_else(crate::providers::cursor::sand_inference::sand_open_account_capacity);
    // A route cooldown is represented by the Sand-only route map used by the
    // failover selector. Rebalance even if its semaphore happens to be idle.
    let current_healthy = policy_rate_limit_preflight(&sand_model, "sand", &current_token).is_ok()
        && !sand_account_route_is_cooled(&sand_model, &current_token)
        && !sand_usage_evidence_is_exhausted(&current_token);
    // Preserve the active-account default while its lane still has capacity.
    // A previous implementation treated *any* occupied lane as a reason to
    // rebalance.  In a normal 512-way burst that made every request after the
    // first perform a blocking account-registry read and, more importantly,
    // changed the user's active-account routing even though that account was
    // perfectly capable of accepting the request.  Only rotate when all open
    // slots are consumed, or when the current route is in cooldown.  An
    // unobserved lane (`None`) is treated as idle and remains on the active
    // account until the gate itself proves saturation.
    if !sand_account_rebalance_needed(current_available_opt, current_healthy) {
        return selection;
    }

    let profiles = match tokio::task::spawn_blocking(list_cursor_accounts)
        .await
        .ok()
        .and_then(Result::ok)
    {
        Some(profiles) => profiles,
        None => return selection,
    };
    let current_digest = cursor_account_digest(&current_token);
    let current_id = selection.account_id.as_deref();
    let mut candidates: Vec<(usize, usize)> = profiles
        .iter()
        .enumerate()
        .filter_map(|(index, profile)| {
            let token = profile.auth.access_token.trim();
            if token.is_empty()
                || cursor_account_digest(token) == current_digest
                || current_id == Some(profile.id.as_str())
                || policy_rate_limit_preflight(&sand_model, "sand", token).is_err()
                || sand_account_route_is_cooled(&sand_model, token)
            {
                return None;
            }
            // A recent dashboard meter is stronger than a semaphore snapshot:
            // avoid deliberately routing a burst to an account already known
            // to have exhausted its Sand/Bot allowance. Missing or stale
            // evidence is ignored; typed provider errors still drive normal
            // account failover when the request is opened.
            if let Some(evidence) =
                crate::providers::cursor::usage::cached_sand_usage_evidence(token)
                && (evidence.usage_percent >= 100.0 || evidence.has_available_usage == Some(false))
            {
                return None;
            }
            let available = crate::providers::cursor::sand_inference::sand_open_available_permits(
                token,
                &sand_model,
            )
            .unwrap_or_else(crate::providers::cursor::sand_inference::sand_open_account_capacity);
            Some((index, available))
        })
        .collect();
    if candidates.is_empty() {
        return selection;
    }

    // Prefer lanes with the most free permits. Rotate ties so concurrent
    // callers do not all select the first alphabetically sorted profile.
    let best_available = candidates.iter().map(|(_, available)| *available).max();
    if let Some(best_available) = best_available {
        candidates.retain(|(_, available)| *available == best_available);
    }
    candidates.sort_by_key(|(index, _)| *index);
    let offset = SAND_ACCOUNT_FAILOVER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let (profile_index, _) = candidates[offset % candidates.len()];
    let profile = profiles[profile_index].clone();
    let profile_active = profile.active;
    let refresh_profile = profile.clone();
    let refreshed = if profile
        .auth
        .expires
        .is_some_and(|expires| expires <= now_ms() + 60_000)
    {
        match tokio::task::spawn_blocking(move || {
            refresh_cursor_account_for_usage(&refresh_profile)
        })
        .await
        .ok()
        .and_then(Result::ok)
        {
            Some(auth) => auth,
            None => return selection,
        }
    } else {
        profile.auth.clone()
    };
    let replacement_id = profile.id.clone();
    let replacement_available =
        crate::providers::cursor::sand_inference::sand_open_available_permits(
            &refreshed.access_token,
            &sand_model,
        )
        .unwrap_or_else(crate::providers::cursor::sand_inference::sand_open_account_capacity);
    create_logger("cursor").info(
        "sand_account_rebalanced",
        Some(serde_json::Map::from_iter([
            (
                "fromAccount".into(),
                serde_json::json!(truncate_account_id(
                    selection.account_id.as_deref().unwrap_or("unknown")
                )),
            ),
            (
                "toAccount".into(),
                serde_json::json!(truncate_account_id(&replacement_id)),
            ),
            ("model".into(), serde_json::json!(route_model)),
            ("sandModel".into(), serde_json::json!(&sand_model)),
            ("fromAvailable".into(), serde_json::json!(current_available)),
            (
                "toAvailable".into(),
                serde_json::json!(replacement_available),
            ),
        ])),
    );
    CursorAuthSelection {
        auth: refreshed,
        account_id: Some(replacement_id),
        active: profile_active,
    }
}

fn account_failover_swap_limit(profiles: &[CursorAccountProfile]) -> u32 {
    let account_count = profiles
        .iter()
        .filter(|profile| !profile.auth.access_token.trim().is_empty())
        .count();
    account_count
        .saturating_sub(1)
        .min(MAX_ACCOUNT_FAILOVER_SWAPS as usize) as u32
}

/// Only account-scoped allowance failures are candidates for pool failover.
/// Billing blocks and generic policy responses apply to the subscription (or
/// deployment), so rotating credentials cannot make them succeed and would
/// needlessly fan one client error across every stored account.
fn is_account_failover_policy_error(message: &str) -> bool {
    // A temporary provider outage can carry an outer rate-limit/resource
    // marker. It must stay on the bounded transport retry path and never
    // rotate or cool down an otherwise healthy account.
    if is_transient_provider_error_message(message) {
        return false;
    }
    if crate::retry::is_billing_block(message) {
        return false;
    }
    // Cursor sometimes wraps an account allowance failure in an outer
    // resource_exhausted/429. Only rotate when the provider diagnostic also
    // carries an explicit quota/rate/usage marker. A generic non-retryable
    // provider 4xx can instead be a malformed request, unsupported schema,
    // model parameter, or context overflow; cycling every saved account for
    // those deterministic errors multiplies traffic and creates a 429 wave.
    if is_non_retryable_provider_error_message(message) {
        let lower = message.to_ascii_lowercase();
        let account_allowance_marker = [
            "resource_exhausted",
            "resource exhausted",
            "rate_limit",
            "rate limit",
            "quota",
            "usage limit",
            "out of usage",
            "included limit",
            "allowance",
            "account closed",
            "account_closed",
            "not_logged_in",
            "not logged in",
            "entitlement",
        ]
        .iter()
        .any(|marker| lower.contains(marker));
        // The nested provider adapter may omit the outer
        // `resource_exhausted`/quota text when it serializes the JSON error.
        // In that shape the enclosing Cursor 429 is the only account-scoped
        // signal left (for example:
        // `Cursor error 429: {"error":{"details":[...]}}`).  Preserve the
        // explicit provider 4xx classification so an exhausted account can
        // fail over instead of being retried indefinitely.  Tool-catalog
        // rejections are handled before this branch by
        // `is_sand_tool_capability_error`, so this does not turn the known
        // catalog fallback into a broad retry loop.
        let outer_rate_limit = lower.contains("connect error 429")
            || lower.contains("cursor error 429")
            || lower.contains("cursor upstream http 429");
        if account_allowance_marker || outer_rate_limit {
            return true;
        }
    }
    let lower = message.to_ascii_lowercase();
    // Authentication/account-terminal responses are account-local for the
    // Sand pool. Treat them like a policy failure so the next saved profile
    // can be tried immediately instead of replaying a closed bearer.
    let account_auth_failure = (lower.contains("cursor error 401")
        || lower.contains("upstream http 401"))
        && (lower.contains("error_account_closed")
            || lower.contains("error_not_logged_in")
            || lower.contains("authentication error")
            || lower.contains("unauthenticated"));
    lower.contains("user_rate_limit_exceeded")
        || lower.contains("api_rate_limit_exceeded")
        || lower.contains("error_rate_limited_changeable")
        || crate::retry::is_grok_bot_vision_quota(&lower)
        || lower.contains("sand account route temporarily unavailable")
        || account_auth_failure
}

/// A pre-output Sand open timeout can be account-local when an unbound model
/// is concentrated on the active profile. Rotate only explicit open timeout
/// diagnostics, and only when the model has no account binding; generic 5xx
/// provider outages stay on the normal transport retry path.
fn is_account_failover_open_error(model: &str, error: &CursorError) -> bool {
    if crate::config::cursor_account_for_model(model).is_some()
        || crate::retry::is_billing_block(&error.client_message())
        || crate::retry::is_policy_rate_limit(&error.client_message())
    {
        return false;
    }
    let message = error.client_message();
    let lower = message.to_ascii_lowercase();
    let admission = lower.contains("sand inference open admission deadline exhausted")
        || lower.contains("sand open admission queue timed out");
    let account_auth = matches!(error.status, 401)
        && (lower.contains("error_account_closed")
            || lower.contains("error_not_logged_in")
            || lower.contains("authentication error")
            || lower.contains("unauthenticated"));
    (matches!(error.status, 502..=504)
        && (lower.contains("sand inference open timed out")
            || lower.contains("sand inference open retry budget exhausted")
            || lower.contains("sand inference upstream http 502")
            || admission))
        || account_auth
}

/// Admission handoff is an optimization for a saturated Sand lane, not proof
/// that the request itself is invalid. A concurrent caller can consume the
/// candidate account between the registry probe and the failover claim. When
/// that race leaves no replacement, keep the logical request queued until its
/// existing Sand deadline instead of turning the race into a client-visible
/// 503/504 retry storm.
fn is_sand_admission_capacity_error(error: &CursorError) -> bool {
    let text = error.client_message().to_ascii_lowercase();
    text.contains("sand inference open admission deadline exhausted")
        || text.contains("sand open admission queue timed out")
}

async fn wait_for_sand_admission_capacity(deadline: Instant) -> bool {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return false;
    }
    tokio::time::sleep(remaining.min(Duration::from_millis(250))).await;
    true
}

/// Account failover is state for one logical request, not one transport
/// attempt. Late stream retries clone `LiveRetryStart`, so keeping this in an
/// `Arc` prevents every reconnect from restarting at the first account and
/// amplifying a Sand quota response into an account-rotation storm.
#[derive(Debug, Default)]
struct AccountFailoverState {
    swaps: u32,
    attempted_accounts: BTreeSet<String>,
}

impl AccountFailoverState {
    fn new(current_token: &str) -> Self {
        let mut state = Self::default();
        state
            .attempted_accounts
            .insert(cursor_account_digest(current_token));
        state
    }
}

type SharedAccountFailoverState = Arc<Mutex<AccountFailoverState>>;

/// Preserve the profile that was displaced by opportunistic Sand balancing
/// in the request-local attempted set. Without this marker a later transport
/// failure on the replacement account could immediately rotate back to the
/// account whose lane was already observed as saturated.
fn mark_pre_rebalance_account_attempted(
    state: &mut AccountFailoverState,
    previous_account_id: Option<&str>,
    previous_account_digest: &str,
    account_was_rebalanced: bool,
) {
    if !account_was_rebalanced {
        return;
    }
    state
        .attempted_accounts
        .insert(previous_account_digest.to_string());
    if let Some(account_id) = previous_account_id {
        state.attempted_accounts.insert(account_id.to_string());
    }
}

/// Return whether a Sand admission handoff has at least one viable saved
/// account.  This is intentionally a pure registry-snapshot helper so the
/// async caller can perform filesystem/Keychain work on a blocking worker and
/// unit tests can exercise the single-account behavior without opening a
/// network connection.
fn sand_admission_candidate_from_profiles(
    profiles: &[CursorAccountProfile],
    current_token: &str,
    model: &str,
    attempted_accounts: &BTreeSet<String>,
) -> bool {
    // Explicit model-account routes are fail-closed: a saturated pinned lane
    // must wait for that account rather than silently consuming another
    // account's allowance.
    if crate::config::cursor_account_for_model(model).is_some() {
        return false;
    }
    let current_digest = cursor_account_digest(current_token);
    profiles.iter().any(|profile| {
        let token = profile.auth.access_token.trim();
        if token.is_empty() {
            return false;
        }
        let digest = cursor_account_digest(token);
        if digest == current_digest
            || attempted_accounts.contains(&digest)
            || attempted_accounts.contains(&profile.id)
        {
            return false;
        }
        // Do not hand off to a route that a recent policy response already
        // cooled.  A stale/missing usage snapshot is deliberately treated as
        // unknown; the actual Sand open remains the source of truth.
        if policy_rate_limit_preflight(model, "sand", token).is_err()
            || sand_account_route_is_cooled(model, token)
        {
            return false;
        }
        if let Some(evidence) = crate::providers::cursor::usage::cached_sand_usage_evidence(token)
            && (evidence.usage_percent >= 100.0 || evidence.has_available_usage == Some(false))
        {
            return false;
        }
        true
    })
}

/// Check for an unattempted Sand account without blocking a Tokio worker on
/// the account registry lock.  Returning `false` on a transient registry read
/// failure is deliberate: the open path then waits on the current account's
/// normal deadline instead of producing a premature client-visible 503.
async fn sand_admission_failover_candidate_available(
    current_token: &str,
    model: &str,
    state: &SharedAccountFailoverState,
) -> bool {
    let attempted = state
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .attempted_accounts
        .clone();
    let current_token = current_token.to_string();
    let model = model.to_string();
    let task = tokio::task::spawn_blocking(move || {
        let profiles = list_cursor_accounts().ok()?;
        Some(sand_admission_candidate_from_profiles(
            &profiles,
            &current_token,
            &model,
            &attempted,
        ))
    });
    // Account-registry mutations use an inter-process file lock.  A crashed
    // companion process or a network filesystem must not hold the inference
    // request forever while we merely probe for a handoff candidate.
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .ok()
        .and_then(Result::ok)
        .flatten()
        .unwrap_or(false)
}

/// Return deterministic account candidates for a policy/quota failure.
///
/// Account ids are stable across access-token rotation, so the breaker and
/// request-local attempt set continue to identify the same account when its
/// JWT changes. This helper is deliberately pure with respect to the account
/// source, which keeps selection behavior unit-testable without touching the
/// user's registry.
fn account_failover_candidates_from_profiles(
    profiles: &[CursorAccountProfile],
    current_token: &str,
    model: &str,
    client_type: &str,
    attempted_accounts: &BTreeSet<String>,
) -> Vec<String> {
    let current_digest = cursor_account_digest(current_token);
    // A model pinned to one account must not silently consume another
    // account's unrelated allowance during automatic policy failover. With
    // no explicit route the historical pool-wide failover behavior remains.
    let pinned_account = crate::config::cursor_account_for_model(model);
    let mut candidates: Vec<(String, String)> = profiles
        .iter()
        .filter_map(|profile| {
            let token = profile.auth.access_token.trim();
            if token.is_empty() {
                return None;
            }
            let digest = cursor_account_digest(token);
            if digest == current_digest
                || attempted_accounts.contains(&digest)
                || attempted_accounts.contains(&profile.id)
            {
                return None;
            }
            if let Some(selector) = pinned_account.as_deref()
                && !crate::config::account_selector_matches(
                    selector,
                    &profile.id,
                    profile.label.as_deref(),
                    profile.auth.email.as_deref(),
                )
            {
                return None;
            }
            if policy_rate_limit_breaker_state(model, client_type, token).is_some()
                || (client_type.trim().eq_ignore_ascii_case("sand")
                    && sand_account_route_is_cooled(model, token))
            {
                return None;
            }
            // A recent dashboard snapshot is account-local quota evidence.
            // Do not immediately fail over onto a Sand/Bot account that is
            // already exhausted; doing so only repeats the same terminal 429
            // and burns the request's retry budget. Unknown or stale meters
            // remain eligible and are checked by the actual Sand open.
            if client_type.trim().eq_ignore_ascii_case("sand")
                && let Some(evidence) =
                    crate::providers::cursor::usage::cached_sand_usage_evidence(token)
                && (evidence.usage_percent >= 100.0 || evidence.has_available_usage == Some(false))
            {
                return None;
            }
            Some((profile.id.clone(), token.to_string()))
        })
        .collect();
    // Registry reads are normally sorted already, but sort again here so a
    // concurrent account-management write cannot change failover order.
    candidates.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    candidates.into_iter().map(|(_, token)| token).collect()
}

/// Atomically reserve the next account for one logical request. Candidate
/// selection happens outside the mutex because listing accounts may perform a
/// filesystem read; the final check under the mutex prevents two concurrent
/// late-retry paths sharing this state from selecting the same account.
fn take_account_failover_candidate_from_profiles(
    profiles: &[CursorAccountProfile],
    current_token: &str,
    model: &str,
    client_type: &str,
    state: &SharedAccountFailoverState,
) -> Option<String> {
    take_account_failover_candidate_from_profiles_with_offset(
        profiles,
        current_token,
        model,
        client_type,
        state,
        0,
    )
}

/// Variant used by Sand's concurrent account pool. `offset` rotates the
/// candidate walk while retaining the same request-local attempted-account
/// guarantees as the deterministic helper above.
fn take_account_failover_candidate_from_profiles_with_offset(
    profiles: &[CursorAccountProfile],
    current_token: &str,
    model: &str,
    client_type: &str,
    state: &SharedAccountFailoverState,
    offset: usize,
) -> Option<String> {
    // Record the registry id for the current bearer as well as its digest.
    // Opaque Cursor tokens can rotate without a stable JWT subject; the
    // profile id still prevents that same account from re-entering the
    // candidate set after a refresh.
    if let Ok(mut state) = state.lock() {
        for profile in profiles {
            if profile.auth.access_token == current_token {
                state.attempted_accounts.insert(profile.id.clone());
            }
        }
    }
    let (swaps, attempted) = {
        let state = state.lock().unwrap_or_else(|poison| poison.into_inner());
        (state.swaps, state.attempted_accounts.clone())
    };
    if swaps >= account_failover_swap_limit(profiles) {
        return None;
    }
    let candidates = account_failover_candidates_from_profiles(
        profiles,
        current_token,
        model,
        client_type,
        &attempted,
    );
    let mut state = state.lock().unwrap_or_else(|poison| poison.into_inner());
    if state.swaps >= account_failover_swap_limit(profiles) {
        return None;
    }
    let mut selected = None;
    for step in 0..candidates.len() {
        let candidate = &candidates[(offset + step) % candidates.len()];
        let digest = cursor_account_digest(candidate);
        let profile_id = profiles
            .iter()
            .find(|profile| profile.auth.access_token == *candidate)
            .map(|profile| profile.id.clone());
        if state.attempted_accounts.contains(&digest)
            || profile_id
                .as_deref()
                .is_some_and(|id| state.attempted_accounts.contains(id))
        {
            continue;
        }
        selected = Some((candidate.clone(), digest, profile_id));
        break;
    }
    let (candidate, digest, profile_id) = selected?;
    state.attempted_accounts.insert(digest);
    if let Some(profile_id) = profile_id {
        state.attempted_accounts.insert(profile_id);
    }
    state.swaps += 1;
    Some(candidate)
}

/// Select an unused, non-cooled account from the persistent pool. A missing
/// or malformed registry simply means there is no local failover candidate;
/// the caller then returns the original policy error with its Retry-After.
fn account_failover_replacement_token(
    current_token: &str,
    model: &str,
    client_type: &str,
    state: &SharedAccountFailoverState,
) -> Option<String> {
    let profiles = list_cursor_accounts().ok()?;
    let replacement = if client_type.trim().eq_ignore_ascii_case("sand") {
        let offset = SAND_ACCOUNT_FAILOVER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        take_account_failover_candidate_from_profiles_with_offset(
            &profiles,
            current_token,
            model,
            client_type,
            state,
            offset,
        )
    } else {
        take_account_failover_candidate_from_profiles(
            &profiles,
            current_token,
            model,
            client_type,
            state,
        )
    };
    if let Some(token) = replacement.as_deref() {
        let email = profiles
            .iter()
            .find(|profile| profile.auth.access_token == token)
            .and_then(|profile| profile.email())
            .unwrap_or("unknown");
        let replacement_id = profiles
            .iter()
            .find(|profile| profile.auth.access_token == token)
            .map(|profile| profile.id.clone())
            .unwrap_or_else(|| cursor_account_key_for_token(token));
        let mut fields = quota_diagnostic_fields(&replacement_id, token, model, client_type);
        // Keep the existing email correlation for operators who already use
        // it, while making the stable/truncated id and selected quota lane the
        // primary account-routing diagnostics.
        fields.insert("email".to_string(), serde_json::json!(email));
        fields.insert("selection".to_string(), serde_json::json!("failover"));
        create_logger("cursor").info("live_account_failover", Some(fields));
    }
    replacement
}

/// Account failover reads the persistent registry under an inter-process file
/// lock. Keep that lookup off Tokio workers; a concurrent TUI mutation or
/// refresh must not stall unrelated inference streams.
async fn account_failover_replacement_token_async(
    current_token: String,
    model: String,
    client_type: String,
    state: SharedAccountFailoverState,
) -> Option<String> {
    tokio::task::spawn_blocking(move || {
        account_failover_replacement_token(&current_token, &model, &client_type, &state)
    })
    .await
    .ok()
    .flatten()
}

/// Resolve the stable registry id for a bearer selected from the account pool.
/// Access tokens may rotate while an account keeps the same profile id; use
/// that id whenever the registry can be read and fall back to the digest for
/// environment/legacy credentials that have no persisted profile.
fn cursor_account_key_for_token(token: &str) -> String {
    list_cursor_accounts()
        .ok()
        .and_then(|profiles| {
            profiles
                .into_iter()
                .find(|profile| profile.auth.access_token == token)
                .map(|profile| profile.id)
        })
        .unwrap_or_else(|| cursor_account_digest(token))
}

/// Async counterpart for request paths. The synchronous lookup may take the
/// account-registry file lock while another process is switching or refreshing
/// a profile, so never run it on a Tokio worker.
async fn cursor_account_key_for_token_async(token: String) -> String {
    let fallback = cursor_account_digest(&token);
    tokio::task::spawn_blocking(move || cursor_account_key_for_token(&token))
        .await
        .ok()
        .unwrap_or(fallback)
}

/// 401-recovery refresh off the async workers: the refresh HTTP call is
/// blocking (single-flighted in auth.rs), so run it on the blocking pool.
async fn force_refresh_cursor_auth_async(
    failed_access_token: String,
) -> anyhow::Result<Option<crate::providers::cursor::auth::CursorAuth>> {
    match tokio::task::spawn_blocking(move || force_refresh_cursor_auth(Some(&failed_access_token)))
        .await
    {
        Ok(result) => result,
        Err(join) => Err(anyhow::anyhow!("Cursor auth refresh task failed: {join}")),
    }
}

/// Resolve the request-scoped account without running registry/Keychain work
/// on a Tokio worker.  `load_cursor_auth_for_model` also performs an
/// account-aware refresh when a profile is near expiry, so this wrapper covers
/// both the local file lock and the blocking refresh HTTP call.
async fn load_cursor_auth_for_model_async(
    model: String,
) -> anyhow::Result<Option<crate::providers::cursor::auth::CursorAuthSelection>> {
    let task = tokio::task::spawn_blocking(move || load_cursor_auth_for_model(&model));
    match tokio::time::timeout(CURSOR_AUTH_LOAD_TIMEOUT, task).await {
        Ok(Ok(result)) => result,
        Ok(Err(join)) => Err(anyhow::anyhow!("Cursor auth selection task failed: {join}")),
        Err(_) => Err(anyhow::anyhow!(
            "Cursor auth selection timed out after {}s",
            CURSOR_AUTH_LOAD_TIMEOUT.as_secs()
        )),
    }
}

fn live_replacement_conflict_error(has_tool_results: bool) -> CursorError {
    if has_tool_results {
        // Tool ids are scoped to one Run generation. Retrying after the slot
        // changed could deliver stale results to a replacement Run.
        live_ambiguous_accept_error()
    } else {
        live_run_busy_error()
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code)]
async fn start_live_events_with_retries(
    client: CursorHttpClient,
    token: String,
    user_text: &str,
    model: &str,
    images: &[CursorSelectedImage],
    custom_system: Option<&str>,
    identity: LiveRunIdentity<'_>,
    allowed: Option<BTreeSet<String>>,
    mcp_tools: Option<crate::providers::cursor::proto::McpTools>,
    request_context: crate::providers::cursor::proto::RequestContext,
    fingerprint: Vec<u8>,
    initial_reservation: Option<LiveRunReservation>,
    has_refresh: bool,
) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
    let client_type = crate::config::cursor_client_type_for_model(model);
    start_live_events_with_retries_with_client_type(
        client,
        token,
        user_text,
        model,
        images,
        None,
        None,
        custom_system,
        identity,
        allowed,
        mcp_tools,
        request_context,
        fingerprint,
        initial_reservation,
        has_refresh,
        &client_type,
        false,
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        LiveStartRecovery::default(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_live_events_with_retries_with_client_type(
    client: CursorHttpClient,
    mut token: String,
    user_text: &str,
    model: &str,
    images: &[CursorSelectedImage],
    reset_retry_images: Option<&[CursorSelectedImage]>,
    reset_user_text: Option<&str>,
    custom_system: Option<&str>,
    identity: LiveRunIdentity<'_>,
    allowed: Option<BTreeSet<String>>,
    mcp_tools: Option<crate::providers::cursor::proto::McpTools>,
    request_context: crate::providers::cursor::proto::RequestContext,
    fingerprint: Vec<u8>,
    mut initial_reservation: Option<LiveRunReservation>,
    has_refresh: bool,
    client_type: &str,
    unbounded_conflict_wait: bool,
    compaction_mode: bool,
    request_sequence_id: Option<&str>,
    effective_token: Option<Arc<Mutex<String>>>,
    image_recovery_attempted: Option<Arc<AtomicBool>>,
    image_recovery_images: Option<Arc<Mutex<Option<Vec<CursorSelectedImage>>>>>,
    kv_recovery_attempted: Option<Arc<AtomicBool>>,
    compaction_recovery_attempted: Option<Arc<AtomicBool>>,
    upstream_active_recovery_attempted: Option<Arc<AtomicBool>>,
    account_failover_state: Option<SharedAccountFailoverState>,
    // Receives the exact Cursor conversation binding used by the generation
    // that successfully opened.  This avoids re-reading the session map after
    // start, where a concurrent reset could make a different generation look
    // like the one we just opened.
    opened_conversation_id: Option<Arc<Mutex<Option<String>>>>,
    recovery: LiveStartRecovery<'_>,
) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
    let publish_effective_token = |token: &str| {
        if let Some(shared) = effective_token.as_ref() {
            *shared.lock().unwrap_or_else(|poison| poison.into_inner()) = token.to_string();
        }
    };
    publish_effective_token(&token);
    let operation_conflict_deadline = live_conflict_wait_deadline(unbounded_conflict_wait);
    // Keep Cursor's original-request identity stable across the bounded
    // fresh-conversation retries of one logical Anthropic request. This is
    // also used to partition empty-END quota evidence; independent requests
    // must never satisfy one another's consecutive-observation threshold.
    let original_request_id = request_sequence_id
        .map(str::to_owned)
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let operation_fingerprint = live_request_fingerprint(&fingerprint);
    let mut transient_retries = 0_u32;
    // `force_refresh_cursor_auth` refreshes the process-wide active account.
    // Once this request moves to an inactive pool account, using that helper
    // would silently switch the request back to the exhausted bearer. Keep
    // refresh recovery enabled only for the account that opened the request.
    let swapped_account = account_failover_state.as_ref().is_some_and(|state| {
        state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .swaps
            > 0
    });
    let mut can_refresh_current_account = has_refresh && !swapped_account;
    let image_recovery_attempted =
        image_recovery_attempted.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let image_recovery_images = image_recovery_images.unwrap_or_else(|| Arc::new(Mutex::new(None)));
    let kv_recovery_attempted =
        kv_recovery_attempted.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let compaction_recovery_attempted =
        compaction_recovery_attempted.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let upstream_active_recovery_attempted =
        upstream_active_recovery_attempted.unwrap_or_else(|| Arc::new(AtomicBool::new(false)));
    let account_failover_state = account_failover_state
        .unwrap_or_else(|| Arc::new(Mutex::new(AccountFailoverState::new(&token))));
    // Keep registry/conversation state account-scoped even when callers use
    // the legacy `(session_id, agent_id)` registry API.  The effective bearer
    // is the source of truth for retries and account-pool failover; a stable
    // JWT subject/email digest remains unchanged across token refreshes.
    // An explicit account key is required for account-partitioned state. Keep
    // legacy callers that omit it on the historical `(session, agent)` lane;
    // production request handlers always provide the selected profile id (or
    // a bearer digest for environment-backed credentials).
    let mut account_key = identity.account_key.map(str::to_owned);
    // Keep the caller-supplied agent component separate from the scoped
    // `identity` value built at the top of each retry iteration.  Recovery
    // paths can swap accounts in the middle of an iteration; using the
    // already-scoped agent there would reset the old account's checkpoint and
    // leave the replacement account attached to stale conversation state.
    let base_agent_id = identity.agent_id;
    // Normally the original request slice is reused across transport retries.
    // A stale Cursor asset requires a new UUID and a fresh conversation; keep
    // that replacement owned by this start loop so both initial-open and
    // Connect END errors share the same bounded recovery state.
    // Carry a UUID wave minted by an earlier segment/recovery into this start
    // invocation.  Late KV/image errors can call back into the same helper
    // after the first stream has already been torn down; starting from `None`
    // here would make the binding-race recovery path reintroduce stale image
    // ids even though the request-scoped cache already has fresh metadata.
    let mut image_retry_images = image_recovery_images
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    // `images` is normally the current checkpoint delta.  On a stale-image
    // response the conversation is reset and the original Anthropic history
    // must be replayed; callers that have no separate history slice (the
    // compatibility wrapper) simply use the current slice for both roles.
    let reset_retry_images = reset_retry_images.unwrap_or(images);
    let reset_user_text = reset_user_text.unwrap_or(user_text);
    // The initial continuation is only a snapshot.  Once Cursor accepts an
    // open, carry the binding returned by that exact generation through every
    // internal retry instead of comparing against the stale caller snapshot.
    let mut expected_conversation_id = recovery.expected_conversation_id.map(str::to_owned);
    let mut retry_user_text: Option<String> = None;
    loop {
        // Fold the current account into the agent component for all registry
        // calls in this iteration.  On failover the next iteration rebuilds
        // this value from the replacement bearer, so no late retry can attach
        // to the exhausted account's generation.
        let scoped_agent_id = account_scoped_agent_id(account_key.as_deref(), base_agent_id);
        let identity = LiveRunIdentity {
            session_id: identity.session_id,
            agent_id: scoped_agent_id.as_deref(),
            parent_agent_id: identity.parent_agent_id,
            account_key: None,
        };
        // Local admission strictly precedes the session-slot claim. A start
        // that is only queued for local capacity must stay invisible to
        // concurrent duplicates; otherwise a 15s admission queue turns into
        // "already active for this session" for every overlapping retry.
        let mut admission = Some(live::admit_live_start(model).await?);
        let mut reservation = if let Some(reservation) = initial_reservation.take() {
            reservation
        } else {
            let claimed = loop {
                if admission.is_none() {
                    // A healthy different-operation Run still owns the
                    // session. Poll its registry state without repeatedly
                    // reacquiring scarce generation capacity; tombstones and
                    // replaceable generations fall through to a real claim.
                    if live_start_should_wait_without_admission(
                        identity.session_id,
                        identity.agent_id,
                        operation_fingerprint,
                    ) {
                        if conflict_wait_expired(operation_conflict_deadline) {
                            break None;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    admission = Some(live::admit_live_start(model).await?);
                }
                match LiveRunRegistry::try_claim_run_for_operation(
                    identity.session_id,
                    identity.agent_id,
                    operation_fingerprint,
                ) {
                    LiveSlotClaim::Reserved(reservation) => break Some(reservation),
                    LiveSlotClaim::Starting => {
                        // Do not let a queued same-session request consume a
                        // scarce generation permit while it waits for the
                        // existing starter to publish or release its slot.
                        // The permit is reacquired at the next outer-loop
                        // iteration after the slot transition is observed.
                        admission.take();
                        if conflict_wait_expired(operation_conflict_deadline) {
                            break None;
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    LiveSlotClaim::Ambiguous => {
                        return Err(live_ambiguous_accept_error());
                    }
                    LiveSlotClaim::CompletedReplay(events) => {
                        return Ok(replay_completed_turn_channel(identity.session_id, &events));
                    }
                    LiveSlotClaim::CompletedNoReplay => {
                        // The identical operation already completed but its
                        // response is no longer replayable. Retryable-busy
                        // here would invite the client to retry until the
                        // tombstone expires and then silently re-execute the
                        // turn. Fail closed instead.
                        return Err(CursorError::new(
                            409,
                            "Cursor operation already completed; response no longer replayable. Refusing duplicate execution",
                            None,
                        ));
                    }
                    LiveSlotClaim::Running => {
                        // `get_run` hides cancel-requested/terminal-error
                        // handles so attach/resume callers cannot race their
                        // teardown. A fresh streamed start still needs to
                        // take over a *different* replaceable generation;
                        // otherwise the unbounded SSE wait would observe an
                        // opaque Occupied slot forever.
                        if unbounded_conflict_wait
                            && let Some(hidden) = LiveRunRegistry::replaceable_run_for_fresh_request(
                                identity.session_id,
                                identity.agent_id,
                                operation_fingerprint,
                            )
                        {
                            match claim_hidden_fresh_replacement(
                                identity.session_id,
                                identity.agent_id,
                                hidden.run_id(),
                            )
                            .await
                            {
                                Ok(Some(reservation)) => break Some(reservation),
                                Ok(None) => {
                                    // Another claimant won the generation
                                    // fence; do not hold scarce capacity while
                                    // the registry settles.
                                    admission.take();
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        if let Some(run) =
                            LiveRunRegistry::get_run(identity.session_id, identity.agent_id)
                        {
                            if run.request_fingerprint() == operation_fingerprint {
                                // Attaching waits for the current segment's
                                // handoff and does not consume a new
                                // generation. Release the start permit while
                                // that bounded wait runs.
                                admission.take();
                                if let Some(events) = attach_live_run_with_bounded_wait(
                                    Arc::clone(&run),
                                    operation_fingerprint,
                                )
                                .await
                                {
                                    return Ok(events);
                                }
                                // The generation may have completed between
                                // get_run() and attach_for_operation().
                                // Reconcile the terminal state before
                                // surfacing 503, otherwise an identical retry
                                // loops on "already active" until the
                                // tombstone expires and loses the replay.
                                if let Some(events) = LiveRunRegistry::completed_replay_for(
                                    identity.session_id,
                                    identity.agent_id,
                                    operation_fingerprint,
                                ) {
                                    return Ok(replay_completed_turn_channel(
                                        identity.session_id,
                                        &events,
                                    ));
                                }
                                match LiveRunRegistry::probe_run(
                                    identity.session_id,
                                    identity.agent_id,
                                ) {
                                    LiveRunProbe::Free => continue,
                                    LiveRunProbe::TerminalError(error)
                                        if live_probe_error_blocks_new_run(&error) =>
                                    {
                                        return Err(live_probe_cursor_error(error));
                                    }
                                    LiveRunProbe::TerminalError(_) => continue,
                                    LiveRunProbe::Occupied => {}
                                }
                            }
                            // A different operation while the previous run's
                            // consumer vanished: the client moved on (ESC +
                            // new message). Supersede the orphan instead of
                            // busy-bouncing until it finishes generating.
                            if run.request_fingerprint() != operation_fingerprint
                                && run.is_replaceable_for_fresh_request()
                            {
                                // A fresh streamed turn may arrive while the
                                // previous downstream has already gone away,
                                // or while a client-only tool batch has made
                                // that generation non-resumable. Claim the
                                // exact generation under the registry lock;
                                // connected, resumable runs remain protected.
                                match LiveRunRegistry::claim_replacement_for_fresh_request(
                                    identity.session_id,
                                    identity.agent_id,
                                    run.run_id(),
                                ) {
                                    LiveReplacementClaim::Reserved {
                                        mut reservation,
                                        superseded: Some(handle),
                                    } => {
                                        reservation.set_operation_fingerprint(
                                            handle.request_fingerprint(),
                                        );
                                        reservation.protect_on_drop();
                                        match handle.cancel_and_wait().await {
                                            Ok(()) => {
                                                match finish_replacement_after_cancel(
                                                    reservation,
                                                    handle,
                                                    false,
                                                    Ok(()),
                                                ) {
                                                    Ok(reservation) => break Some(reservation),
                                                    Err(error) => return Err(error),
                                                }
                                            }
                                            Err(error) => {
                                                match finish_replacement_after_cancel(
                                                    reservation,
                                                    handle,
                                                    false,
                                                    Err(error),
                                                ) {
                                                    Ok(_) => unreachable!(
                                                        "failed cancellation must not authorize replacement"
                                                    ),
                                                    Err(error) => return Err(error),
                                                }
                                            }
                                        }
                                    }
                                    LiveReplacementClaim::Reserved {
                                        reservation,
                                        superseded: None,
                                    } => break Some(reservation),
                                    LiveReplacementClaim::Conflict => {}
                                }
                            }
                            // A different operation must never attach to or
                            // cancel a live generation. Give the old request
                            // a short chance to finish before surfacing local
                            // backpressure to a client retry.
                            if !run.is_command_closed()
                                && conflict_wait_active(operation_conflict_deadline)
                            {
                                admission.take();
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        }
                        // A completed handle can be hidden by get_run(). Give
                        // the registry one final chance to expose its replay
                        // or clear a retryable terminal outcome before
                        // returning local backpressure.
                        if let Some(events) = LiveRunRegistry::completed_replay_for(
                            identity.session_id,
                            identity.agent_id,
                            operation_fingerprint,
                        ) {
                            return Ok(replay_completed_turn_channel(identity.session_id, &events));
                        }
                        match LiveRunRegistry::probe_run(identity.session_id, identity.agent_id) {
                            LiveRunProbe::Free => continue,
                            LiveRunProbe::TerminalError(error)
                                if live_probe_error_blocks_new_run(&error) =>
                            {
                                return Err(live_probe_cursor_error(error));
                            }
                            LiveRunProbe::TerminalError(_) => continue,
                            LiveRunProbe::Occupied => {}
                        }
                        // A hidden Running handle is no longer attachable. If
                        // it was not replaceable above, do not let an
                        // already-committed SSE spin forever behind it. A
                        // later client retry can observe the terminal state;
                        // healthy visible generations still take the normal
                        // heartbeat-backed wait path above.
                        if LiveRunRegistry::running_generation(
                            identity.session_id,
                            identity.agent_id,
                        )
                        .is_some()
                        {
                            if unbounded_conflict_wait {
                                // A hidden cancel-requested/terminaling Run
                                // cannot accept AttachReplay yet. Keep the
                                // already-committed SSE alive while its driver
                                // publishes the terminal transition; the
                                // next probe can then replay or reserve it.
                                // Returning local 503 here recreates the
                                // duplicate retry storm this path is meant to
                                // absorb.
                                admission.take();
                                // Drop the permit and let the next loop
                                // iteration either wait on an incomplete
                                // hidden driver or reacquire and probe a
                                // completed/terminal one.
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                            return Err(live_run_busy_error());
                        }
                        // `get_run` intentionally hides a cancel-requested or
                        // terminal handle. If that state changes between the
                        // claim and terminal probe, keep the same bounded
                        // handoff wait instead of leaking an early busy error
                        // from this narrow registry transition window.
                        if conflict_wait_active(operation_conflict_deadline) {
                            admission.take();
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        return Err(live_run_busy_error());
                    }
                }
            };
            match claimed {
                Some(reservation) => reservation,
                None => break,
            }
        };
        let admission = admission.expect("live admission is present for a claimed slot");
        reservation.set_operation_fingerprint(operation_fingerprint);
        match reservation.begin_durable_operation() {
            operation_ledger::OperationAdmission::Allowed => {}
            operation_ledger::OperationAdmission::DuplicateCompleted => {
                reservation.release();
                return Err(CursorError::new(
                    409,
                    "Cursor operation already completed; refusing duplicate replay",
                    None,
                ));
            }
            operation_ledger::OperationAdmission::Ambiguous(message) => {
                reservation.release();
                return Err(CursorError::new(
                    409,
                    format!("Cursor operation is unresolved; completion is ambiguous: {message}"),
                    None,
                ));
            }
            operation_ledger::OperationAdmission::Unavailable(error) => {
                create_logger("cursor").error(
                    "operation_ledger_begin_failed",
                    Some(serde_json::Map::from_iter([(
                        "error".into(),
                        serde_json::json!(error),
                    )])),
                );
                reservation.release();
                return Err(CursorError::new(
                    503,
                    "Cursor operation ledger is unavailable; request was not dispatched",
                    None,
                ));
            }
        }

        // A request may have waited behind another generation while a policy
        // 429 arrived and opened the account/model breaker. Re-check directly
        // before the first Cursor operation so queued callers do not dispatch
        // into the same deterministic limit. This is intentionally after the
        // session claim but before `start_live_agent...`; releasing here is a
        // definitive pre-acceptance path and does not leave a 503 tombstone.
        let mut probe_admission = Some(
            match policy_rate_limit_admit_fresh_open(model, client_type, &token).await {
                Ok(admission) => admission,
                Err(error) => {
                    reservation.release();
                    return Err(error);
                }
            },
        );

        let upstream_open_guard = reservation.upstream_open_guard();
        let attempt_images = image_retry_images.as_deref().unwrap_or(images);
        let attempt_prompt = retry_user_text.as_deref().unwrap_or(user_text);
        // If a prior stale-image/KV recovery already minted fresh asset
        // identities, keep that exact wave when the continuation-binding race
        // below is detected.  Falling back to the original full-history slice
        // here would silently reintroduce the stale UUIDs (and create a second
        // refresh wave on the next error).
        let attempt_recovery = LiveStartRecovery {
            expected_conversation_id: expected_conversation_id.as_deref(),
            reset_user_text: recovery.reset_user_text,
            reset_images: Some(image_retry_images.as_deref().unwrap_or(reset_retry_images)),
        };
        let start = match client
            .start_live_agent_with_identity_guarded_profile_mode_with_recovery(
                &token,
                attempt_prompt,
                model,
                attempt_images,
                custom_system,
                identity,
                allowed.clone(),
                mcp_tools.clone(),
                request_context.clone(),
                Some(&original_request_id),
                Some(reservation.cancelled()),
                Some(Arc::clone(&upstream_open_guard)),
                Some(admission),
                Some(client_type),
                compaction_mode,
                attempt_recovery,
            )
            .await
        {
            Ok(start) => Ok(start),
            Err(error) if error.status == 401 && can_refresh_current_account => {
                // Release this attempt's probe while refreshing. JWT rotation
                // for the same subject intentionally resolves to the same
                // stable account key; a genuine account switch resolves to a
                // different key.
                drop(probe_admission.take());
                match force_refresh_cursor_auth_async(token.clone()).await {
                    Ok(Some(refreshed)) => {
                        token = refreshed.access_token;
                        publish_effective_token(&token);
                        probe_admission = Some(
                            match policy_rate_limit_admit_fresh_open(model, client_type, &token)
                                .await
                            {
                                Ok(admission) => admission,
                                Err(policy_error) => {
                                    reservation.release();
                                    return Err(policy_error);
                                }
                            },
                        );
                        client
                            .start_live_agent_with_identity_guarded_profile_mode_with_recovery(
                                &token,
                                attempt_prompt,
                                model,
                                attempt_images,
                                custom_system,
                                identity,
                                allowed.clone(),
                                mcp_tools.clone(),
                                request_context.clone(),
                                Some(&original_request_id),
                                Some(reservation.cancelled()),
                                Some(upstream_open_guard),
                                None,
                                Some(client_type),
                                compaction_mode,
                                attempt_recovery,
                            )
                            .await
                    }
                    _ => Err(error),
                }
            }
            Err(error) => Err(error),
        };

        match start {
            Ok(start) => {
                // `LiveRunStart` carries the binding pinned by the opener. It
                // is authoritative even if another task rotates the same
                // session immediately after this point; late retries use the
                // shared snapshot and the generation fence to avoid blindly
                // reading whichever conversation happens to be current.
                expected_conversation_id = Some(start.conversation_id.clone());
                if let Some(shared) = opened_conversation_id.as_ref() {
                    *shared.lock().unwrap_or_else(|poison| poison.into_inner()) =
                        expected_conversation_id.clone();
                }
                start.handle.set_request_fingerprint(operation_fingerprint);
                if let Err(orphaned) = reservation.insert(Arc::clone(&start.handle)) {
                    drop(probe_admission.take());
                    let _ = orphaned.cancel_and_wait().await;
                    break;
                }
                match peek_live_start_for_stale_reset(start.events).await {
                    LiveStartPeek::Ready {
                        events,
                        observed_healthy_event,
                    } => {
                        let admission = probe_admission
                            .take()
                            .expect("policy admission remains owned by the live start");
                        if observed_healthy_event {
                            // Client-committing model output/tool completion
                            // proves this key is not in an immediate
                            // deterministic policy shed.
                            // Subsequent starts need no cold-probe serialization
                            // until a policy error resets the gate.
                            admission.mark_healthy();
                            return Ok(events);
                        }
                        // The short stale-conversation peek is a downstream
                        // latency optimization, not evidence that Cursor
                        // accepted the model. Keep a cold probe single-flight
                        // after that timeout and classify its eventual first
                        // event before releasing the account/model gate.
                        if let Some(lease) = admission.into_probe() {
                            return Ok(hold_policy_probe_until_decisive_event(
                                events,
                                lease,
                                model.to_string(),
                                client_type.to_string(),
                                token.clone(),
                                policy_rate_limit_probe_window(),
                            ));
                        }
                        return Ok(events);
                    }
                    LiveStartPeek::Retryable(error) => {
                        let image_error = cursor_connect_error_is_missing_image(&error);
                        let kv_error = live_error_is_kv_blob_overflow_replayable(&error);
                        let policy_limited = crate::retry::is_policy_rate_limit(&error)
                            || is_account_failover_policy_error(&error);
                        if policy_limited {
                            probe_admission
                                .take()
                                .expect("policy admission remains owned by the live start")
                                .mark_policy_limited(model, client_type, &token, &error, None);
                        } else {
                            drop(probe_admission.take());
                        }
                        let _ = start.handle.cancel_and_wait().await;
                        let _ = LiveRunRegistry::probe_run(identity.session_id, identity.agent_id);
                        if live_error_is_upstream_already_active(&error)
                            && !is_local_live_busy_text(&error)
                        {
                            if !claim_upstream_active_recovery(&upstream_active_recovery_attempted)
                            {
                                return Err(CursorError::new(503, error, None));
                            }
                            let key = live_retry_conversation_key_for_account(
                                identity.session_id,
                                base_agent_id,
                                account_key.as_deref(),
                            );
                            conversation::reset(&key);
                            expected_conversation_id = None;
                            retry_user_text = Some(reset_user_text.to_string());
                            image_retry_images = Some(
                                cached_image_recovery_snapshot(&image_recovery_images)
                                    .unwrap_or_else(|| reset_retry_images.to_vec()),
                            );
                            create_logger("cursor").warn(
                                "upstream_live_run_conflict_recovery",
                                Some(serde_json::Map::from_iter([
                                    ("sessionId".into(), serde_json::json!(identity.session_id)),
                                    ("model".into(), serde_json::json!(model)),
                                    ("clientType".into(), serde_json::json!(client_type)),
                                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                                    ("attempt".into(), serde_json::json!(1)),
                                ])),
                            );
                            continue;
                        }
                        if compaction_mode && live_error_is_agent_looping_detected(&error) {
                            if compaction_recovery_attempted
                                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                                .is_err()
                            {
                                return Err(CursorError::new(400, error, None));
                            }
                            let key = live_retry_conversation_key_for_account(
                                identity.session_id,
                                base_agent_id,
                                account_key.as_deref(),
                            );
                            conversation::reset(&key);
                            retry_user_text = Some(reset_user_text.to_string());
                            image_retry_images = Some(cached_image_recovery_images(
                                &image_recovery_images,
                                reset_retry_images,
                            ));
                            create_logger("cursor").warn(
                                "compact_loop_recovery",
                                Some(serde_json::Map::from_iter([
                                    ("sessionId".into(), serde_json::json!(identity.session_id)),
                                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                                    ("attempt".into(), serde_json::json!(1)),
                                ])),
                            );
                            continue;
                        }
                        if kv_error {
                            if kv_recovery_attempted
                                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                                .is_err()
                            {
                                return Err(CursorError::new(413, error, None));
                            }
                            let key = live_retry_conversation_key_for_account(
                                identity.session_id,
                                base_agent_id,
                                account_key.as_deref(),
                            );
                            conversation::reset(&key);
                            // A checkpoint delta only carries images from the
                            // current turn. A fresh conversation needs the
                            // complete image set as well; preserve an already
                            // refreshed set if the same request hit the image
                            // recovery path first.
                            image_retry_images = Some(cached_image_recovery_images(
                                &image_recovery_images,
                                reset_retry_images,
                            ));
                            retry_user_text = Some(reset_user_text.to_string());
                            create_logger("cursor").warn(
                                "kv_blob_start_recovery",
                                Some(serde_json::Map::from_iter([
                                    ("sessionId".into(), serde_json::json!(identity.session_id)),
                                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                                    ("replay".into(), serde_json::json!("full_history")),
                                ])),
                            );
                            continue;
                        }
                        if image_error {
                            if image_recovery_attempted
                                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                                .is_err()
                            {
                                return Err(CursorError::new(502, error, None));
                            }
                            let key = live_retry_conversation_key_for_account(
                                identity.session_id,
                                base_agent_id,
                                account_key.as_deref(),
                            );
                            conversation::reset(&key);
                            image_retry_images = Some(cached_image_recovery_images(
                                &image_recovery_images,
                                reset_retry_images,
                            ));
                            retry_user_text = Some(reset_user_text.to_string());
                            create_logger("cursor").warn(
                                "image_start_recovery",
                                Some(serde_json::Map::from_iter([
                                    ("sessionId".into(), serde_json::json!(identity.session_id)),
                                    (
                                        "imageCount".into(),
                                        serde_json::json!(
                                            image_retry_images.as_ref().map_or(0, Vec::len)
                                        ),
                                    ),
                                    ("recovery".into(), serde_json::json!("fresh_conversation")),
                                ])),
                            );
                            continue;
                        }
                        if is_account_failover_policy_error(&error) {
                            // Account-bound 429: same-login retries cannot
                            // succeed. Fail over to newly stored credentials
                            // after a hot account switch, else pass through.
                            if let Some(replacement) = account_failover_replacement_token_async(
                                token.clone(),
                                model.to_string(),
                                client_type.to_string(),
                                Arc::clone(&account_failover_state),
                            )
                            .await
                            {
                                token = replacement;
                                account_key =
                                    Some(cursor_account_key_for_token_async(token.clone()).await);
                                publish_effective_token(&token);
                                can_refresh_current_account = false;
                                // Cursor conversation/checkpoint state is
                                // account-scoped. A replacement bearer must
                                // start from a fresh binding and replay the
                                // complete logical request, otherwise the new
                                // account can reject the old conversation id.
                                let key = live_retry_conversation_key_for_account(
                                    identity.session_id,
                                    base_agent_id,
                                    account_key.as_deref(),
                                );
                                conversation::reset(&key);
                                expected_conversation_id = None;
                                image_recovery_attempted.store(false, Ordering::Release);
                                image_retry_images = Some(fresh_image_recovery_images_for_account(
                                    &image_recovery_images,
                                    reset_retry_images,
                                ));
                                retry_user_text = Some(reset_user_text.to_string());
                                transient_retries = 0;
                                continue;
                            }
                            let status = crate::retry::classify_proxy_error_status(502, &error);
                            let mut surfaced = CursorError::new(status, error, None);
                            surfaced.retry_after =
                                policy_rate_limit_breaker_state(model, client_type, &token)
                                    .map(|state| state.retry_after_secs.to_string());
                            return Err(surfaced);
                        }
                        if transient_retries >= cursor_transient_retry_limit(&error) {
                            return Err(CursorError::internal(error));
                        }
                        crate::retry::sleep(same_request_retry_wait_ms(transient_retries, &error))
                            .await;
                        transient_retries += 1;
                        continue;
                    }
                }
            }
            Err(error) => {
                if live_error_is_upstream_already_active(&error.client_message())
                    && !is_local_live_busy_text(&error.client_message())
                {
                    if claim_upstream_active_recovery(&upstream_active_recovery_attempted) {
                        drop(probe_admission.take());
                        reservation.release();
                        let key = live_retry_conversation_key_for_account(
                            identity.session_id,
                            base_agent_id,
                            account_key.as_deref(),
                        );
                        conversation::reset(&key);
                        expected_conversation_id = None;
                        image_retry_images = Some(
                            cached_image_recovery_snapshot(&image_recovery_images)
                                .unwrap_or_else(|| reset_retry_images.to_vec()),
                        );
                        retry_user_text = Some(reset_user_text.to_string());
                        create_logger("cursor").warn(
                            "upstream_live_run_conflict_recovery",
                            Some(serde_json::Map::from_iter([
                                ("sessionId".into(), serde_json::json!(identity.session_id)),
                                ("model".into(), serde_json::json!(model)),
                                ("clientType".into(), serde_json::json!(client_type)),
                                ("recovery".into(), serde_json::json!("fresh_conversation")),
                                ("attempt".into(), serde_json::json!(1)),
                            ])),
                        );
                        continue;
                    }
                    drop(probe_admission.take());
                    reservation.release();
                    return Err(error);
                }
                if compaction_mode && live_error_is_agent_looping_detected(&error.client_message())
                {
                    if compaction_recovery_attempted
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        drop(probe_admission.take());
                        reservation.release();
                        let key = live_retry_conversation_key_for_account(
                            identity.session_id,
                            base_agent_id,
                            account_key.as_deref(),
                        );
                        conversation::reset(&key);
                        image_retry_images = Some(cached_image_recovery_images(
                            &image_recovery_images,
                            reset_retry_images,
                        ));
                        retry_user_text = Some(reset_user_text.to_string());
                        create_logger("cursor").warn(
                            "compact_loop_recovery",
                            Some(serde_json::Map::from_iter([
                                ("sessionId".into(), serde_json::json!(identity.session_id)),
                                ("recovery".into(), serde_json::json!("fresh_conversation")),
                                ("attempt".into(), serde_json::json!(1)),
                            ])),
                        );
                        continue;
                    }
                    drop(probe_admission.take());
                    reservation.release();
                    return Err(error);
                }
                let kv_error = cursor_error_is_kv_blob_overflow(&error)
                    && live_error_is_kv_blob_overflow_replayable(&error.client_message());
                if kv_error {
                    // A start-level 413 can arrive before a live event exists
                    // (for example Cursor rejects the first SetBlob while
                    // opening the stream).  Rotate the composite binding and
                    // replay the full prompt once; a second 413 is terminal
                    // for this logical request rather than a same-id retry.
                    if kv_recovery_attempted
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        drop(probe_admission.take());
                        reservation.release();
                        let key = live_retry_conversation_key_for_account(
                            identity.session_id,
                            base_agent_id,
                            account_key.as_deref(),
                        );
                        conversation::reset(&key);
                        image_retry_images = Some(cached_image_recovery_images(
                            &image_recovery_images,
                            reset_retry_images,
                        ));
                        retry_user_text = Some(reset_user_text.to_string());
                        create_logger("cursor").warn(
                            "kv_blob_start_recovery",
                            Some(serde_json::Map::from_iter([
                                ("sessionId".into(), serde_json::json!(identity.session_id)),
                                ("recovery".into(), serde_json::json!("fresh_conversation")),
                                ("replay".into(), serde_json::json!("full_history")),
                            ])),
                        );
                        continue;
                    }
                    drop(probe_admission.take());
                    reservation.release();
                    return Err(error);
                }
                let image_error = cursor_connect_error_is_missing_image(&error.client_message())
                    || cursor_connect_error_is_missing_image(&error.message)
                    || error
                        .detail
                        .as_deref()
                        .is_some_and(cursor_connect_error_is_missing_image);
                if image_error
                    && image_recovery_attempted
                        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                {
                    drop(probe_admission.take());
                    reservation.release();
                    let key = live_retry_conversation_key_for_account(
                        identity.session_id,
                        base_agent_id,
                        account_key.as_deref(),
                    );
                    conversation::reset(&key);
                    image_retry_images = Some(cached_image_recovery_images(
                        &image_recovery_images,
                        reset_retry_images,
                    ));
                    retry_user_text = Some(reset_user_text.to_string());
                    create_logger("cursor").warn(
                        "image_start_recovery",
                        Some(serde_json::Map::from_iter([
                            ("sessionId".into(), serde_json::json!(identity.session_id)),
                            (
                                "imageCount".into(),
                                serde_json::json!(image_retry_images.as_ref().map_or(0, Vec::len)),
                            ),
                            ("recovery".into(), serde_json::json!("fresh_conversation")),
                        ])),
                    );
                    continue;
                }
                let policy_limited = cursor_error_is_policy_limited(&error);
                if policy_limited {
                    probe_admission
                        .take()
                        .expect("policy admission remains owned by the live start")
                        .mark_policy_limited(
                            model,
                            client_type,
                            &token,
                            &error.client_message(),
                            error.retry_after.as_deref(),
                        );
                } else {
                    drop(probe_admission.take());
                }
                if (is_account_failover_policy_error(&error.client_message())
                    || is_account_failover_policy_error(&error.message))
                    && let Some(replacement) = account_failover_replacement_token_async(
                        token.clone(),
                        model.to_string(),
                        client_type.to_string(),
                        Arc::clone(&account_failover_state),
                    )
                    .await
                {
                    reservation.release();
                    token = replacement;
                    account_key = Some(cursor_account_key_for_token_async(token.clone()).await);
                    publish_effective_token(&token);
                    can_refresh_current_account = false;
                    let key = live_retry_conversation_key_for_account(
                        identity.session_id,
                        base_agent_id,
                        account_key.as_deref(),
                    );
                    conversation::reset(&key);
                    expected_conversation_id = None;
                    image_recovery_attempted.store(false, Ordering::Release);
                    image_retry_images = Some(fresh_image_recovery_images_for_account(
                        &image_recovery_images,
                        reset_retry_images,
                    ));
                    retry_user_text = Some(reset_user_text.to_string());
                    transient_retries = 0;
                    continue;
                }
                let retryable = !image_error
                    && !kv_error
                    && transient_retries < cursor_transient_retry_limit(&error.client_message())
                    && cursor_start_error_is_same_request_retryable(&error);
                if retryable {
                    reservation.release();
                    crate::retry::sleep(same_request_retry_wait_ms(
                        transient_retries,
                        &error.client_message(),
                    ))
                    .await;
                    transient_retries += 1;
                    continue;
                }
                if live_start_error_seals_tombstone(&error) {
                    reservation.seal_ambiguous(Instant::now() + LIVE_AMBIGUOUS_OPEN_TTL);
                } else {
                    reservation.release();
                }
                let mut error = exhausted_live_start_error(error, transient_retries);
                if policy_limited && error.retry_after.is_none() {
                    error.retry_after = policy_rate_limit_breaker_state(model, client_type, &token)
                        .map(|state| state.retry_after_secs.to_string());
                }
                return Err(error);
            }
        }
    }

    Err(live_run_busy_error())
}

#[allow(clippy::too_many_arguments)]
fn spawn_streaming_live_sse(
    client: CursorHttpClient,
    token: String,
    user_text: String,
    reset_user_text: String,
    expected_conversation_id: Option<String>,
    model: String,
    images: Vec<CursorSelectedImage>,
    reset_retry_images: Vec<CursorSelectedImage>,
    custom_system: Option<String>,
    sid: String,
    account_key: String,
    agent_id: Option<String>,
    parent_agent_id: Option<String>,
    allowed: Option<BTreeSet<String>>,
    mcp_tools: Option<crate::providers::cursor::proto::McpTools>,
    request_context: crate::providers::cursor::proto::RequestContext,
    fingerprint: Vec<u8>,
    initial_reservation: Option<LiveRunReservation>,
    has_refresh: bool,
    unbounded_conflict_wait: bool,
    compaction_mode: bool,
    client_type: String,
    message_id: String,
    wire_model: String,
    estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
) -> Response {
    let sid_for_sse = sid.clone();
    let account_failover_state = Arc::new(Mutex::new(AccountFailoverState::new(&token)));
    let rx = spawn_live_events_with_late_retries(
        LiveRetryStart {
            client,
            effective_token: Arc::new(Mutex::new(token)),
            user_text,
            reset_user_text,
            expected_conversation_id: Arc::new(Mutex::new(expected_conversation_id)),
            model,
            images,
            reset_retry_images,
            image_recovery_attempted: Arc::new(AtomicBool::new(false)),
            image_recovery_images: Arc::new(Mutex::new(None)),
            kv_recovery_attempted: Arc::new(AtomicBool::new(false)),
            compaction_recovery_attempted: Arc::new(AtomicBool::new(false)),
            upstream_active_recovery_attempted: Arc::new(AtomicBool::new(false)),
            account_failover_state,
            custom_system,
            session_id: sid,
            agent_id,
            parent_agent_id,
            account_key: Arc::new(Mutex::new(account_key)),
            allowed,
            mcp_tools,
            request_context,
            fingerprint,
            has_refresh,
            client_type,
            request_sequence_id: uuid::Uuid::new_v4().to_string(),
            // Only a fresh request may wait indefinitely. Tool-result
            // continuations keep the bounded, generation-specific handoff
            // semantics even though their SSE lifecycle is already open.
            unbounded_conflict_wait,
            compaction_mode,
        },
        initial_reservation,
        None,
    );
    live_sse_recording_usage(
        &sid_for_sse,
        rx,
        message_id,
        wire_model,
        estimated_input,
        monitor,
        compaction_mode,
    )
}

/// Wait briefly for the first live event before committing Anthropic SSE.
/// A missing-conversation reset on that first event can start a fresh Run
/// on this same request; grok-build will not retry the 502 itself.
async fn peek_live_start_for_stale_reset(events: mpsc::Receiver<LiveEventResult>) -> LiveStartPeek {
    let wait = Duration::from_millis(env_u64_millis("CCP_CURSOR_STALE_CONV_PEEK_MS", 2_000));
    peek_live_start_for_stale_reset_with_wait(events, wait).await
}

async fn peek_live_start_for_stale_reset_with_wait(
    mut events: mpsc::Receiver<LiveEventResult>,
    wait: Duration,
) -> LiveStartPeek {
    match tokio::time::timeout(wait, events.recv()).await {
        // Policy 429s are routed as Retryable so the start loop can fail over
        // to newly stored credentials after a hot account switch; without a
        // switch the loop passes them through verbatim instead of retrying.
        Ok(Some(Err(error)))
            if (live_error_is_same_request_retryable(&error)
                || crate::retry::is_policy_rate_limit(&error)
                // A Cursor gateway can wrap the session-conflict text in a
                // 503/Connect envelope that the generic classifier correctly
                // keeps out of same-request retries. It still needs the
                // coordinator's one-shot fresh-conversation recovery here;
                // preserve the local registry busy wording for its attach/
                // wait path instead of rotating a healthy conversation.
                || (live_error_is_upstream_already_active(&error)
                    && !is_local_live_busy_text(&error)))
                && !live_error_is_empty_turn_retry(&error) =>
        {
            LiveStartPeek::Retryable(error)
        }
        Ok(Some(first)) => LiveStartPeek::Ready {
            observed_healthy_event: first.as_ref().is_ok_and(live_event_commits_client_output),
            events: prepend_live_event(first, events),
        },
        Ok(None) => LiveStartPeek::Ready {
            events,
            observed_healthy_event: false,
        },
        Err(_) => LiveStartPeek::Ready {
            events,
            observed_healthy_event: false,
        },
    }
}

/// Keep a cold account/model policy probe owned after the short live-start
/// peek expires. A late policy 429 opens the breaker before any waiting
/// session can dispatch; a decisive model event marks the key healthy.
///
/// The task deliberately keeps observing the upstream even if the original
/// downstream receiver is dropped. Otherwise a client disconnect during the
/// quiet start window could release the only probe while its accepted Cursor
/// Run was still capable of returning the decisive policy result.
fn hold_policy_probe_until_decisive_event(
    mut events: mpsc::Receiver<LiveEventResult>,
    lease: PolicyRateLimitProbeLease,
    model: String,
    client_type: String,
    token: String,
    probe_window: Duration,
) -> mpsc::Receiver<LiveEventResult> {
    let (tx, rx) = mpsc::channel(512);
    tokio::spawn(async move {
        let mut lease = Some(lease);
        let mut empty_turn_deadline: Option<Instant> = None;
        let mut forwarding = true;
        loop {
            // A hollow Cursor turn is not evidence that the account/model is
            // healthy. Keep the single-flight lease through the original
            // probe window even when the upstream closes or goes quiet, then
            // let exactly one waiter start the next bounded probe.
            let item = if let Some(deadline) = empty_turn_deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    drop(lease.take());
                    empty_turn_deadline = None;
                    continue;
                }
                tokio::select! {
                    item = events.recv() => item,
                    _ = tokio::time::sleep(remaining) => {
                        drop(lease.take());
                        empty_turn_deadline = None;
                        continue;
                    }
                }
            } else {
                events.recv().await
            };
            let Some(item) = item else {
                // The live pump synthesizes EMPTY_TURN_RETRY_NOTE when the
                // upstream closes after metadata (or with no frames at all).
                // Preserve the probe lease for the same bounded window here,
                // otherwise an EOF-only hollow turn can reopen a retry wave
                // before the downstream classifier gets a chance to observe
                // it.
                if let Some(probe_lease) = lease.as_ref() {
                    let remaining = probe_lease.remaining_until(probe_window);
                    if !remaining.is_zero() {
                        empty_turn_deadline = Some(Instant::now() + remaining);
                        tokio::time::sleep(remaining).await;
                    }
                }
                break;
            };

            if lease.is_some() {
                match &item {
                    Ok(event) if live_event_commits_client_output(event) => {
                        lease
                            .take()
                            .expect("policy probe lease is present")
                            .mark_healthy();
                        empty_turn_deadline = None;
                    }
                    Err(error) if crate::retry::is_policy_rate_limit(error) => {
                        note_policy_rate_limit(&model, &client_type, &token, error, None);
                        drop(lease.take());
                        empty_turn_deadline = None;
                    }
                    Err(error) if live_error_is_empty_turn_retry(error) => {
                        if let Some(probe_lease) = lease.as_ref() {
                            let remaining = probe_lease.remaining_until(probe_window);
                            if remaining.is_zero() {
                                drop(lease.take());
                            } else {
                                empty_turn_deadline
                                    .get_or_insert_with(|| Instant::now() + remaining);
                            }
                        }
                    }
                    Err(_) => drop(lease.take()),
                    Ok(_) => {
                        // Session/usage/thinking metadata is not proof that
                        // Cursor admitted the requested model. Keep the probe
                        // owned until text, a tool, End, or an error arrives.
                    }
                }
            }

            if forwarding && tx.send(item).await.is_err() {
                forwarding = false;
            }
            if !forwarding && lease.is_none() {
                return;
            }
        }
        if let Some(deadline) = empty_turn_deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() && lease.is_some() {
                tokio::time::sleep(remaining).await;
            }
        }
        drop(lease.take());
    });
    rx
}

fn prepend_live_event(
    first: LiveEventResult,
    mut rest: mpsc::Receiver<LiveEventResult>,
) -> mpsc::Receiver<LiveEventResult> {
    let (tx, rx) = mpsc::channel(512);
    tokio::spawn(async move {
        if tx.send(first).await.is_err() {
            return;
        }
        while let Some(item) = rest.recv().await {
            if tx.send(item).await.is_err() {
                return;
            }
        }
    });
    rx
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LivePumpOutcome {
    Done,
    Retry(String),
    PolicyLimited(String),
    ClientGone,
}

/// Thinking deltas are speculative and remain buffered until an answer,
/// native tool, or terminal event commits the turn. This lets a heartbeat-only
/// stall restart without exposing a terminal error after hidden reasoning.
fn live_event_commits_client_output(event: &LiveRunEvent) -> bool {
    match event {
        // `emit_tool_batch` finalizes only when at least one tool is
        // present; an empty batch is a protocol keep-alive/malformed event,
        // not client-visible progress.
        LiveRunEvent::NativeToolBatch(tools) => !tools.is_empty(),
        LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }) => !text.is_empty(),
        LiveRunEvent::Cursor(CursorStreamEvent::NativeTool { .. } | CursorStreamEvent::End) => true,
        LiveRunEvent::Cursor(
            CursorStreamEvent::ThinkingDelta { .. }
            | CursorStreamEvent::Session { .. }
            | CursorStreamEvent::Usage { .. }
            | CursorStreamEvent::OutputTokenDelta { .. },
        ) => false,
    }
}

#[cfg(test)]
fn classify_live_pump_item(committed: bool, item: &LiveEventResult) -> LivePumpAction {
    classify_live_pump_item_with_mode(committed, item, false)
}

fn classify_live_pump_item_with_mode(
    committed: bool,
    item: &LiveEventResult,
    compaction_mode: bool,
) -> LivePumpAction {
    if compaction_mode
        && !committed
        && let Err(error) = item
        && live_error_is_agent_looping_detected(error)
    {
        // A compact summary has no client-visible output yet.  Allow the
        // bounded fresh-lane recovery below to rotate its poisoned checkpoint;
        // ordinary turns deliberately keep this 400 terminal.
        return LivePumpAction::Retry;
    }
    // Cursor asset lookup failures are recoverable before any client-visible
    // output: rotate the conversation and resend the original inline bytes.
    // Keep this ahead of generic 502 classification, which intentionally
    // excludes image errors so stale assets do not loop indefinitely.
    if let Err(error) = item
        && !committed
        && !crate::retry::is_policy_rate_limit(error)
        && cursor_connect_error_is_missing_image(error)
    {
        return LivePumpAction::Retry;
    }
    match item {
        // Keep upstream session conflicts on the dedicated one-shot recovery
        // path. `live_error_is_same_request_retryable` intentionally returns
        // false for this semantic error so lower transport layers cannot
        // replay the same conversation indefinitely. A local registry busy
        // diagnostic is deliberately excluded and retains its attach/wait
        // semantics.
        Err(error)
            if !committed
                && live_error_is_upstream_already_active(error)
                && !is_local_live_busy_text(error) =>
        {
            LivePumpAction::Retry
        }
        Err(error) if !committed && crate::retry::is_policy_rate_limit(error) => {
            LivePumpAction::PolicyLimit
        }
        Err(error) if live_error_is_empty_turn_retry(error) => LivePumpAction::Retry,
        Err(error) if !committed && live_error_is_same_request_retryable(error) => {
            LivePumpAction::Retry
        }
        Ok(event) if !committed && !live_event_commits_client_output(event) => {
            LivePumpAction::Buffer
        }
        _ => LivePumpAction::Forward,
    }
}

fn live_probe_error_blocks_new_run_for_mode(error: &str, compaction_mode: bool) -> bool {
    live_probe_error_blocks_new_run(error)
        && !(compaction_mode && live_error_is_agent_looping_detected(error))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LivePumpAction {
    Buffer,
    Forward,
    Retry,
    PolicyLimit,
}

#[cfg(test)]
async fn pump_live_events_until_commit_or_retry(
    tx: &mpsc::Sender<LiveEventResult>,
    events: mpsc::Receiver<LiveEventResult>,
) -> LivePumpOutcome {
    pump_live_events_until_commit_or_retry_with_mode(tx, events, false).await
}

async fn pump_live_events_until_commit_or_retry_with_mode(
    tx: &mpsc::Sender<LiveEventResult>,
    mut events: mpsc::Receiver<LiveEventResult>,
    compaction_mode: bool,
) -> LivePumpOutcome {
    let mut committed = false;
    let mut buffered = Vec::new();
    while let Some(item) = events.recv().await {
        match classify_live_pump_item_with_mode(committed, &item, compaction_mode) {
            LivePumpAction::Retry => {
                let Err(error) = item else {
                    continue;
                };
                return LivePumpOutcome::Retry(error);
            }
            LivePumpAction::PolicyLimit => {
                let Err(error) = item else {
                    continue;
                };
                return LivePumpOutcome::PolicyLimited(error);
            }
            LivePumpAction::Buffer => buffered.push(item),
            LivePumpAction::Forward => {
                committed = true;
                for pending in buffered.drain(..) {
                    if tx.send(pending).await.is_err() {
                        return LivePumpOutcome::ClientGone;
                    }
                }
                if tx.send(item).await.is_err() {
                    return LivePumpOutcome::ClientGone;
                }
            }
        }
    }
    // A driver can close its event channel cleanly after emitting only
    // session/thinking/usage metadata.  There is no client-visible result to
    // replay in that case, so treat the close like the existing hollow-turn
    // marker instead of letting live_sse_response manufacture a bare 502.
    // Once text, a native tool, or End has committed, EOF remains terminal and
    // must stay fail-closed.
    if !committed {
        return LivePumpOutcome::Retry(
            "Cursor upstream finished this turn without text or tool calls; retry this turn (upstream stream ended before a client-visible response; stale Cursor conversation reset; retry this message to continue)"
                .into(),
        );
    }
    for pending in buffered {
        if tx.send(pending).await.is_err() {
            return LivePumpOutcome::ClientGone;
        }
    }
    LivePumpOutcome::Done
}

async fn wait_for_optional_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

async fn forward_empty_turn_deadline(
    tx: &mpsc::Sender<LiveEventResult>,
    session_id: &str,
    agent_id: Option<&str>,
    expected_run_id: Option<&str>,
    last_error: &str,
) {
    // A late retry task may still be alive after its Run was superseded. Do
    // not let its deadline cancel whichever generation now occupies the slot.
    if let Some(run_id) = expected_run_id {
        LiveRunRegistry::cancel_run_if_generation(session_id, agent_id, run_id);
    }
    let _ = tx
        .send(Err(format!(
            "{last_error} (empty-turn recovery deadline exhausted)"
        )))
        .await;
}

fn live_late_retry_limit(error: &str, policy: LiveLateRetryPolicy) -> u32 {
    if live_error_is_kv_blob_overflow_replayable(error) {
        // A KV overflow can only be repaired by changing the Cursor
        // conversation id. The shared recovery fence makes this one attempt
        // per logical request; keep the explicit limit here as a second guard
        // against malformed/repeated upstream frames.
        1
    } else if live_error_is_empty_turn_retry(error) {
        policy.empty_turn_max_retries
    } else if is_transient_step_failure(error) {
        cursor_step_failure_retry_limit(error)
    } else if is_transient_resource_exhausted(error) {
        cursor_transient_retry_limit(error)
    } else {
        policy.transient_max_retries
    }
}

#[cfg(test)]
async fn forward_live_events_with_retries<F, Fut>(
    tx: &mpsc::Sender<LiveEventResult>,
    events: mpsc::Receiver<LiveEventResult>,
    session_id: &str,
    agent_id: Option<&str>,
    restart: F,
    policy: LiveLateRetryPolicy,
) where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<mpsc::Receiver<LiveEventResult>, CursorError>>,
{
    let context = LiveLateRetryContext {
        model: "unknown".into(),
        client_type: "unknown".into(),
        effective_token: Arc::new(Mutex::new(String::new())),
        account_key: Arc::new(Mutex::new(String::new())),
        compaction_mode: false,
    };
    forward_live_events_with_retries_context(
        tx, events, session_id, agent_id, restart, policy, &context,
    )
    .await;
}

async fn forward_live_events_with_retries_context<F, Fut>(
    tx: &mpsc::Sender<LiveEventResult>,
    mut events: mpsc::Receiver<LiveEventResult>,
    session_id: &str,
    agent_id: Option<&str>,
    mut restart: F,
    policy: LiveLateRetryPolicy,
    context: &LiveLateRetryContext,
) where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<mpsc::Receiver<LiveEventResult>, CursorError>>,
{
    let episode_started = tokio::time::Instant::now();
    let mut transient_retries = 0_u32;
    let mut empty_turn_retries = 0_u32;
    let mut image_retries = 0_u32;
    let mut kv_retries = 0_u32;
    let mut empty_turn_deadline = None;
    let mut last_empty_turn_error = None::<String>;
    // Event receivers do not carry their owning Run id. Capture the current
    // generation and refresh it after each internal restart so deadline
    // cancellation remains generation-bound.
    let mut registry_agent_id = account_scoped_agent_id(Some(&context.account_key()), agent_id);
    let mut expected_run_id =
        LiveRunRegistry::running_generation(session_id, registry_agent_id.as_deref());
    loop {
        let pump =
            pump_live_events_until_commit_or_retry_with_mode(tx, events, context.compaction_mode);
        tokio::pin!(pump);
        let outcome = tokio::select! {
            _ = tx.closed() => {
                // Downstream disconnected. Do NOT destructively cancel: that
                // frees the slot before the upstream Run acknowledges and
                // seals ambiguous tombstones that 409 the client's retry.
                // Dropping the receiver lets the driver observe the closed
                // sink and run the orphan path — an identical retry attaches
                // or gets the completed turn replayed; a different request
                // supersedes the orphan.
                return;
            }
            _ = wait_for_optional_deadline(empty_turn_deadline) => {
                forward_empty_turn_deadline(
                    tx,
                    session_id,
                    registry_agent_id.as_deref(),
                    expected_run_id.as_deref(),
                    last_empty_turn_error
                        .as_deref()
                        .unwrap_or("Cursor empty-turn recovery timed out"),
                )
                .await;
                return;
            }
            outcome = &mut pump => outcome,
        };
        match outcome {
            LivePumpOutcome::ClientGone => {
                // Same as tx.closed above: leave the run to the orphan path.
                return;
            }
            LivePumpOutcome::Retry(error) => {
                let empty_turn = live_error_is_empty_turn_retry(&error);
                let image_error = cursor_connect_error_is_missing_image(&error);
                let kv_error = live_error_is_kv_blob_overflow_replayable(&error);
                let retry_index = if empty_turn {
                    empty_turn_retries
                } else if image_error {
                    image_retries
                } else if kv_error {
                    kv_retries
                } else {
                    transient_retries
                };
                // Re-upload a stale inline image at most once.  A second
                // Image-not-found response is generally an upstream asset
                // outage and should reach the client instead of generating a
                // fresh-conversation storm.
                let retry_limit = if image_error {
                    1
                } else {
                    live_late_retry_limit(&error, policy)
                };
                if retry_index >= retry_limit {
                    create_logger("cursor").warn(
                        "live_retry_exhausted",
                        Some(serde_json::Map::from_iter([
                            ("attempt".into(), serde_json::json!(retry_index + 1)),
                            ("maxRetries".into(), serde_json::json!(retry_limit)),
                            ("model".into(), serde_json::json!(&context.model)),
                            ("clientType".into(), serde_json::json!(&context.client_type)),
                            ("sessionId".into(), serde_json::json!(session_id)),
                            (
                                "recovery".into(),
                                serde_json::json!(if empty_turn {
                                    "empty_turn_exhausted"
                                } else if kv_error {
                                    "kv_blob_overflow_exhausted"
                                } else {
                                    "transient_exhausted"
                                }),
                            ),
                        ])),
                    );
                    let _ = tx.send(Err(error)).await;
                    return;
                }
                if empty_turn {
                    last_empty_turn_error = Some(error.clone());
                    let deadline = *empty_turn_deadline
                        .get_or_insert(episode_started + policy.empty_turn_episode);
                    if tokio::time::Instant::now() >= deadline {
                        forward_empty_turn_deadline(
                            tx,
                            session_id,
                            registry_agent_id.as_deref(),
                            expected_run_id.as_deref(),
                            last_empty_turn_error.as_deref().unwrap_or(&error),
                        )
                        .await;
                        return;
                    }
                }
                let slot_deadline = Instant::now() + LIVE_H2_OPEN_ATTEMPT;
                loop {
                    match LiveRunRegistry::probe_run(session_id, registry_agent_id.as_deref()) {
                        LiveRunProbe::Free => break,
                        LiveRunProbe::TerminalError(terminal)
                            if live_probe_error_blocks_new_run_for_mode(
                                &terminal,
                                context.compaction_mode,
                            ) =>
                        {
                            let _ = tx.send(Err(terminal)).await;
                            return;
                        }
                        LiveRunProbe::TerminalError(_) => break,
                        LiveRunProbe::Occupied if Instant::now() < slot_deadline => {
                            tokio::select! {
                                _ = tx.closed() => {
                                    // Disconnect: leave the run to the orphan path.
                                    return;
                                }
                                _ = wait_for_optional_deadline(empty_turn_deadline) => {
                                    forward_empty_turn_deadline(
                                        tx,
                                        session_id,
                                        registry_agent_id.as_deref(),
                                        expected_run_id.as_deref(),
                                        last_empty_turn_error
                                            .as_deref()
                                            .unwrap_or(&error),
                                    )
                                    .await;
                                    return;
                                }
                                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                            }
                        }
                        LiveRunProbe::Occupied => {
                            let _ = tx.send(Err(error)).await;
                            return;
                        }
                    }
                }
                let wait = same_request_retry_wait_ms(retry_index, &error);
                tokio::select! {
                    _ = tx.closed() => {
                        // Disconnect: leave the run to the orphan path.
                        return;
                    }
                    _ = wait_for_optional_deadline(empty_turn_deadline) => {
                        forward_empty_turn_deadline(
                            tx,
                            session_id,
                            registry_agent_id.as_deref(),
                            expected_run_id.as_deref(),
                            last_empty_turn_error.as_deref().unwrap_or(&error),
                        )
                        .await;
                        return;
                    }
                    _ = crate::retry::sleep(wait) => {}
                }
                if empty_turn {
                    empty_turn_retries += 1;
                } else if image_error {
                    image_retries += 1;
                } else if kv_error {
                    kv_retries += 1;
                } else {
                    transient_retries += 1;
                }
                create_logger("cursor").warn(
                    "live_internal_retry",
                    Some(serde_json::Map::from_iter([
                        ("attempt".into(), serde_json::json!(retry_index + 1)),
                        ("maxRetries".into(), serde_json::json!(retry_limit)),
                        ("model".into(), serde_json::json!(&context.model)),
                        ("clientType".into(), serde_json::json!(&context.client_type)),
                        ("sessionId".into(), serde_json::json!(session_id)),
                        (
                            "recovery".into(),
                            serde_json::json!(if live_error_needs_checkpoint_continue(&error) {
                                "checkpoint_continue"
                            } else if image_error {
                                "image_fresh_conversation"
                            } else if kv_error {
                                "kv_blob_fresh_conversation"
                            } else if empty_turn {
                                "fresh_conversation"
                            } else {
                                "same_request"
                            }),
                        ),
                    ])),
                );
                let start = restart(error);
                tokio::pin!(start);
                events = match tokio::select! {
                    _ = tx.closed() => {
                        // Disconnect: leave the run to the orphan path.
                        return;
                    }
                    _ = wait_for_optional_deadline(empty_turn_deadline) => {
                        forward_empty_turn_deadline(
                            tx,
                            session_id,
                            registry_agent_id.as_deref(),
                            expected_run_id.as_deref(),
                            last_empty_turn_error
                                .as_deref()
                                .unwrap_or("Cursor empty-turn recovery timed out"),
                        )
                        .await;
                        return;
                    }
                    result = &mut start => result,
                } {
                    Ok(events) => events,
                    Err(error) => {
                        let _ = tx.send(Err(error.client_message())).await;
                        return;
                    }
                };
                // `start_after_error` publishes the new handle before it
                // returns its receiver. Fence future deadline cancellation to
                // that newly accepted generation.
                registry_agent_id = account_scoped_agent_id(Some(&context.account_key()), agent_id);
                expected_run_id =
                    LiveRunRegistry::running_generation(session_id, registry_agent_id.as_deref());
            }
            LivePumpOutcome::PolicyLimited(error) => {
                // A Sand clean-END may be reclassified by the live driver once
                // dashboard quota evidence is available. Treat it exactly like
                // an explicit Connect policy 429.  The late-start helper can
                // safely fail over here because this branch is only selected
                // before any client-visible text/tool event is committed.  It
                // resets the account-scoped conversation and replays complete
                // history; if the account pool is exhausted, the original
                // typed policy error is surfaced with no retry amplification.
                let token = context.effective_token();
                if !token.is_empty() {
                    note_policy_rate_limit(
                        &context.model,
                        &context.client_type,
                        &token,
                        &error,
                        None,
                    );
                }
                if is_account_failover_policy_error(&error) {
                    let start = restart(error.clone());
                    tokio::pin!(start);
                    events = match tokio::select! {
                        _ = tx.closed() => return,
                        result = &mut start => result,
                    } {
                        Ok(events) => events,
                        Err(error) => {
                            let _ = tx.send(Err(error.client_message())).await;
                            return;
                        }
                    };
                    registry_agent_id =
                        account_scoped_agent_id(Some(&context.account_key()), agent_id);
                    expected_run_id = LiveRunRegistry::running_generation(
                        session_id,
                        registry_agent_id.as_deref(),
                    );
                } else {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
            LivePumpOutcome::Done => return,
        }
    }
}

fn spawn_live_events_with_late_retries(
    start: LiveRetryStart,
    initial_reservation: Option<LiveRunReservation>,
    initial_events: Option<mpsc::Receiver<LiveEventResult>>,
) -> mpsc::Receiver<LiveEventResult> {
    let (tx, rx) = mpsc::channel(512);
    let session_id = start.session_id.clone();
    let agent_id = start.agent_id.clone();
    let panic_tx = tx.clone();
    let panic_session_id = session_id.clone();
    let panic_agent_id = agent_id.clone();
    tokio::spawn(async move {
        // A panic in the late-retry coordinator otherwise closes `rx` without
        // an event. The SSE adapter then has no typed cause and manufactures a
        // misleading bare 502. Keep client disconnects as ordinary early
        // returns, but turn every coordinator panic into an explicit retryable
        // terminal event so the client can recover instead of stalling.
        let coordinator = async move {
            let retry_context = LiveLateRetryContext {
                model: start.model.clone(),
                client_type: start.client_type.clone(),
                effective_token: Arc::clone(&start.effective_token),
                account_key: Arc::clone(&start.account_key),
                compaction_mode: start.compaction_mode,
            };
            let retry_policy =
                LiveLateRetryPolicy::for_request(&retry_context.model, &retry_context.client_type);
            let events = match initial_events {
                Some(events) => events,
                None => {
                    let first = start.start(initial_reservation);
                    tokio::pin!(first);
                    match tokio::select! {
                        _ = tx.closed() => {
                            // Disconnect while starting: dropping the start
                            // future aborts a pre-accept open; an accepted Run
                            // stays owned by its driver/orphan path.
                            return;
                        }
                        result = &mut first => result,
                    } {
                        Ok(events) => events,
                        Err(error) => {
                            let _ = tx.send(Err(error.client_message())).await;
                            return;
                        }
                    }
                }
            };
            let retry_start = start.clone();
            forward_live_events_with_retries_context(
                &tx,
                events,
                &session_id,
                agent_id.as_deref(),
                move |error| {
                    let retry_start = retry_start.clone();
                    async move { retry_start.start_after_error(&error).await }
                },
                retry_policy,
                &retry_context,
            )
            .await;
        };
        run_live_retry_coordinator_with_panic_guard(
            panic_tx,
            panic_session_id,
            panic_agent_id,
            coordinator,
        )
        .await;
    });
    rx
}

/// Keep an unexpected coordinator exit observable to the downstream adapter.
///
/// A panic otherwise drops the event sender and is indistinguishable from an
/// empty upstream stream, which is surfaced as a misleading bare 502. The
/// empty-turn marker deliberately stays in this message so the normal bounded
/// late-retry classifier can recover the request without exposing the panic.
async fn run_live_retry_coordinator_with_panic_guard<F>(
    tx: mpsc::Sender<LiveEventResult>,
    session_id: String,
    agent_id: Option<String>,
    coordinator: F,
) where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    if AssertUnwindSafe(coordinator).catch_unwind().await.is_err() {
        create_logger("cursor").error(
            "live_retry_coordinator_panic",
            Some(serde_json::Map::from_iter([
                ("sessionId".into(), serde_json::json!(&session_id)),
                (
                    "agentId".into(),
                    serde_json::json!(agent_id.as_deref().unwrap_or("")),
                ),
                ("recovery".into(), serde_json::json!("coordinator_panic")),
            ])),
        );
        let message = format!(
            "Cursor live retry coordinator failed unexpectedly; {EMPTY_TURN_RETRY_NOTE} (coordinator panic)"
        );
        let send = tx.send(Err(message));
        let _ = tokio::time::timeout(
            Duration::from_millis(env_u64_millis(
                "CCP_CURSOR_DOWNSTREAM_SEND_TIMEOUT_MS",
                5_000,
            )),
            send,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn live_downstream_response(
    want_stream: bool,
    session_id: &str,
    events: mpsc::Receiver<LiveEventResult>,
    message_id: String,
    wire_model: String,
    estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
    compaction_mode: bool,
) -> Response {
    if want_stream {
        live_sse_recording_usage(
            session_id,
            events,
            message_id,
            wire_model,
            estimated_input,
            monitor,
            compaction_mode,
        )
    } else {
        live_json_recording_usage(
            session_id,
            events,
            message_id,
            wire_model,
            estimated_input,
            monitor,
            compaction_mode,
        )
        .await
    }
}

async fn collect_live_events_to_json(
    mut events: mpsc::Receiver<LiveEventResult>,
    message_id: &str,
    model: &str,
    estimated_input: u64,
    compaction_mode: bool,
) -> Result<serde_json::Value, String> {
    let mut acc = AnthropicJsonAcc::new_mode(estimated_input, compaction_mode);
    let mut saw_end = false;
    let mut tool_handoff = false;
    // Keep this bit separate from `acc.has_useful()`: a compaction
    // reasoning fallback is useful for diagnostics, but it is not evidence
    // that an ordinary non-streaming answer reached a clean terminal.  A
    // visible text delta is the same commit signal used by the live replay
    // path and is the only payload that may close a clean EOF into an
    // `end_turn` response.
    let mut saw_text = false;
    while let Some(item) = events.recv().await {
        match item {
            Ok(LiveRunEvent::Cursor(event)) => {
                let ended = matches!(event, CursorStreamEvent::End);
                if let CursorStreamEvent::TextDelta { text } = &event {
                    saw_text |= !text.is_empty();
                }
                // `NativeTool` is already a finalized Anthropic segment in
                // the incremental encoder (equivalent to NativeToolBatch).
                // Treat it as a handoff here too, otherwise a valid
                // non-streaming tool response that closes without a separate
                // End marker is misreported as "without turn_ended".
                if matches!(event, CursorStreamEvent::NativeTool { .. }) {
                    tool_handoff = true;
                }
                acc.push(&event);
                if ended {
                    saw_end = true;
                    break;
                }
                if tool_handoff {
                    // A single Cursor NativeTool is encoded as a complete
                    // Anthropic tool-use handoff. Ignore any metadata/text
                    // that may arrive after it on the channel, just as the
                    // streaming encoder does after finalization.
                    break;
                }
            }
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                let has_tools = !tools.is_empty();
                for tool in tools {
                    acc.push_native_tool(tool.tool_use_id, tool.name, tool.input);
                }
                // An empty batch is not client-visible progress.  Keep the
                // handoff bit tied to an actual tool so a malformed empty
                // event cannot turn into a successful `tool_use` response.
                if has_tools {
                    tool_handoff = true;
                }
                break;
            }
            Err(error) => return Err(error),
        }
    }
    if !acc.has_useful() {
        return Err("Cursor stream produced no useful progress".into());
    }
    if !saw_end && !tool_handoff {
        if clean_json_eof_can_synthesize_terminal(saw_text) {
            // JSON has no event channel on which to append FLAG_END.  The
            // accumulator's `end_turn` stop reason is the equivalent local
            // terminal for a clean text-bearing EOF.  This mirrors replay's
            // bounded synthetic End while preserving fail-closed behavior
            // for metadata/thinking-only or empty responses.
            create_logger("cursor").info(
                "live_json_clean_eof_terminal",
                Some(serde_json::Map::from_iter([
                    ("messageId".into(), serde_json::json!(message_id)),
                    ("model".into(), serde_json::json!(model)),
                ])),
            );
        } else {
            return Err("Cursor stream ended without turn_ended".into());
        }
    }
    Ok(acc.into_message_json(message_id, model))
}

/// A non-streaming live channel can close without carrying a separate End
/// event when the upstream emitted its final text and then cleanly EOF'd.  Do
/// not apply this to thinking/metadata-only channels: those remain retryable
/// hollow turns, matching `replay_needs_synthetic_terminal`.
#[inline]
fn clean_json_eof_can_synthesize_terminal(saw_text: bool) -> bool {
    saw_text
}

async fn live_json_recording_usage(
    session_id: &str,
    events: mpsc::Receiver<LiveEventResult>,
    message_id: String,
    wire_model: String,
    estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
    compaction_mode: bool,
) -> Response {
    match collect_live_events_to_json(
        tap_session_usage(session_id.to_string(), events),
        &message_id,
        &wire_model,
        estimated_input,
        compaction_mode,
    )
    .await
    {
        Ok(json) => {
            let input_tokens = json.pointer("/usage/input_tokens").and_then(|v| v.as_u64());
            remember_input_tokens(Some(session_id), input_tokens);
            if let Some((handle, req_id)) = monitor.as_ref() {
                handle.usage_updated(
                    req_id,
                    input_tokens,
                    json.pointer("/usage/output_tokens")
                        .and_then(|v| v.as_u64()),
                );
            }
            (StatusCode::OK, Json(json)).into_response()
        }
        Err(error) => json_error_from_cursor_message(error),
    }
}

#[cfg(test)]
fn reset_session_usage_for_test() {
    let mut store = SESSION_USAGE.lock().unwrap_or_else(|e| e.into_inner());
    store.map.clear();
    store.order.clear();
}

#[cfg(test)]
static SESSION_USAGE_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn log_live_start_claude_headers(
    ctx: &RequestContext,
    session_id: &str,
    model: &str,
    client_type: &str,
    compaction: bool,
) {
    create_logger("cursor").info(
        "live_start_identity",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), serde_json::json!(&ctx.req_id)),
            ("sessionId".to_string(), serde_json::json!(session_id)),
            // Keep the effective route alongside the Claude headers. The
            // incoming `app=cli` identifies Claude Code, not Sand versus CLI.
            ("model".to_string(), serde_json::json!(model)),
            ("clientType".to_string(), serde_json::json!(client_type)),
            ("compaction".to_string(), serde_json::json!(compaction)),
            (
                "agentId".to_string(),
                serde_json::json!(&ctx.claude_code.agent_id),
            ),
            (
                "parentAgentId".to_string(),
                serde_json::json!(&ctx.claude_code.parent_agent_id),
            ),
            ("app".to_string(), serde_json::json!(&ctx.claude_code.app)),
        ])),
    );
}

fn claude_agent_id(ctx: &RequestContext) -> Option<&str> {
    ctx.claude_code
        .agent_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
}

#[cfg(test)]
fn live_run_identity<'a>(session_id: &'a str, ctx: &'a RequestContext) -> LiveRunIdentity<'a> {
    live_run_identity_with_account(session_id, ctx, None)
}

fn live_run_identity_with_account<'a>(
    session_id: &'a str,
    ctx: &'a RequestContext,
    account_key: Option<&'a str>,
) -> LiveRunIdentity<'a> {
    LiveRunIdentity {
        session_id,
        agent_id: claude_agent_id(ctx),
        parent_agent_id: ctx
            .claude_code
            .parent_agent_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty()),
        account_key,
    }
}

/// Tool-bridge state must use the same composite identity as live runs and
/// conversation checkpoints. Claude Code nested agents reuse the parent
/// `X-Claude-Code-Session-Id`, so the raw session id alone is not an isolation
/// boundary.
fn bridge_registry_key_for_account(
    ctx: &RequestContext,
    account_key: Option<&str>,
) -> Option<String> {
    let session_id = ctx.session_id.as_deref().filter(|id| !id.is_empty())?;
    Some(live_run_key_for(live_run_identity_with_account(
        session_id,
        ctx,
        account_key,
    )))
}

#[cfg(test)]
fn bridge_registry_key(ctx: &RequestContext) -> Option<String> {
    bridge_registry_key_for_account(ctx, None)
}

fn live_operation_fingerprint_payload(
    body: &MessagesRequest,
    client_request_id: Option<&str>,
) -> Vec<u8> {
    // The sampler keeps `x-grok-req-id` stable across transport retries, but
    // it may rebuild the JSON body along the way.  Hash a canonical semantic
    // projection rather than serde's insertion order or the wire's transient
    // image/trace metadata.  This is deliberately done before the request is
    // rendered for Cursor: the same projection is needed by the registry,
    // retry worker, and preflight attach checks.
    let body_value = serde_json::to_value(body).unwrap_or(serde_json::Value::Null);
    if let Some(request_id) = client_request_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let mut payload = b"x-grok-req-id\0".to_vec();
        payload.extend_from_slice(request_id.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&canonical_live_fingerprint_json(&body_value));
        return payload;
    }
    // Clients predating x-grok-req-id use message history as their fallback
    // identity.  Keep that compatibility boundary, while applying the same
    // canonicalization so image re-encoding and object key order do not open
    // a second live Run.
    // Preserve the exact legacy bytes for ordinary text/tool messages. The
    // in-memory live registry can contain a generation admitted by an older
    // code path (which used serde's insertion order), and changing those bytes
    // would make a concurrent tool-result waiter look like a different turn.
    // Image/thinking blocks are the exceptional wire shapes that may be
    // re-encoded between retries, so use the canonical traversal only when
    // one is actually present.
    if let Ok(raw_messages) = serde_json::to_vec(&body.messages) {
        let messages = serde_json::to_value(&body.messages).unwrap_or(serde_json::Value::Null);
        if !legacy_fingerprint_needs_normalization(&messages) {
            return raw_messages;
        }
        return canonical_live_fingerprint_json_with_context(
            &messages,
            LiveFingerprintContext::LegacyMessages,
        );
    }
    Vec::new()
}

fn legacy_fingerprint_needs_normalization(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(object) => {
            is_live_fingerprint_image_block(object, LiveFingerprintContext::LegacyContent)
                || object.values().any(legacy_fingerprint_needs_normalization)
        }
        serde_json::Value::Array(values) => values.iter().any(|value| {
            is_live_internal_thinking_block(value) || legacy_fingerprint_needs_normalization(value)
        }),
        _ => false,
    }
}

/// Context used while constructing the operation fingerprint.  Message
/// content needs a little more normalization than arbitrary request objects:
/// historical thinking blocks and transport-only cache metadata do not alter
/// the prompt that reaches Cursor, while tool-result ids and ordering do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LiveFingerprintContext {
    Root,
    Messages,
    Message,
    Content,
    /// Legacy message-history fingerprinting used when a client does not
    /// provide an explicit request id.  Keep the old scalar/content wire
    /// shape so an in-flight generation created by an older proxy remains
    /// attachable, while still normalizing image bytes and thinking blocks.
    LegacyMessages,
    LegacyMessage,
    LegacyContent,
    Metadata,
    Generic,
}

/// Serialize a JSON value with deterministic object ordering and the small
/// set of retry-only normalizations needed by Grok/Claude clients.
fn canonical_live_fingerprint_json(value: &serde_json::Value) -> Vec<u8> {
    canonical_live_fingerprint_json_with_context(value, LiveFingerprintContext::Root)
}

fn canonical_live_fingerprint_json_with_context(
    value: &serde_json::Value,
    context: LiveFingerprintContext,
) -> Vec<u8> {
    let normalized = normalize_live_fingerprint_value(value, context);
    serde_json::to_vec(&normalized).unwrap_or_default()
}

fn normalize_live_fingerprint_value(
    value: &serde_json::Value,
    context: LiveFingerprintContext,
) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            // Inline images are carried in several equivalent Anthropic/OpenAI
            // shapes.  Their bytes may be omitted, wrapped, or re-encoded on a
            // retry; the block's position/type is the stable operation signal.
            if is_live_fingerprint_image_block(object, context) {
                return normalized_live_fingerprint_image();
            }

            let mut keys: Vec<&String> = object.keys().collect();
            keys.sort_unstable();
            let mut normalized = serde_json::Map::new();
            for key in keys {
                if should_drop_live_fingerprint_field(context, key) {
                    continue;
                }
                let raw_value = object
                    .get(key)
                    .expect("key collected from the same JSON object");
                let child_context = match (context, key.as_str()) {
                    (LiveFingerprintContext::Root, "messages") => LiveFingerprintContext::Messages,
                    (LiveFingerprintContext::Root, "metadata") => LiveFingerprintContext::Metadata,
                    (LiveFingerprintContext::Messages, _) => LiveFingerprintContext::Message,
                    (LiveFingerprintContext::LegacyMessages, _) => {
                        LiveFingerprintContext::LegacyMessage
                    }
                    (LiveFingerprintContext::Message, "content")
                    | (LiveFingerprintContext::Content, "content") => {
                        LiveFingerprintContext::Content
                    }
                    (LiveFingerprintContext::LegacyMessage, "content")
                    | (LiveFingerprintContext::LegacyContent, "content") => {
                        LiveFingerprintContext::LegacyContent
                    }
                    (LiveFingerprintContext::Metadata, _) => LiveFingerprintContext::Metadata,
                    _ => LiveFingerprintContext::Generic,
                };

                // A string message and a one-block text message are wire
                // equivalents.  Normalize both to the block form used by the
                // renderer; this also makes a reconstructed retry stable.
                let normalized_value = if matches!(
                    (context, key.as_str()),
                    (LiveFingerprintContext::Message, "content")
                        | (LiveFingerprintContext::Content, "content")
                ) {
                    normalize_live_message_content(raw_value)
                } else {
                    normalize_live_fingerprint_value(raw_value, child_context)
                };
                normalized.insert(key.clone(), normalized_value);
            }
            serde_json::Value::Object(normalized)
        }
        serde_json::Value::Array(values) => {
            let mut normalized = Vec::with_capacity(values.len());
            for value in values {
                // Thinking is an internal history artifact and is omitted by
                // render_cursor_prompt.  Keeping it in the idempotency body
                // would make a retry that gained/lost a signature look like a
                // new operation.  Preserve every other block and its order.
                if matches!(
                    context,
                    LiveFingerprintContext::Content | LiveFingerprintContext::LegacyContent
                ) && is_live_internal_thinking_block(value)
                {
                    continue;
                }
                let child_context = match context {
                    LiveFingerprintContext::Messages => LiveFingerprintContext::Message,
                    LiveFingerprintContext::LegacyMessages => LiveFingerprintContext::LegacyMessage,
                    LiveFingerprintContext::Content => LiveFingerprintContext::Content,
                    LiveFingerprintContext::LegacyContent => LiveFingerprintContext::LegacyContent,
                    LiveFingerprintContext::Metadata => LiveFingerprintContext::Metadata,
                    _ => LiveFingerprintContext::Generic,
                };
                normalized.push(normalize_live_fingerprint_value(value, child_context));
            }
            serde_json::Value::Array(normalized)
        }
        _ => value.clone(),
    }
}

fn normalize_live_message_content(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => serde_json::json!([
            {"type": "text", "text": text}
        ]),
        serde_json::Value::Object(object)
            if object
                .get("type")
                .and_then(serde_json::Value::as_str)
                .is_some() =>
        {
            serde_json::Value::Array(vec![normalize_live_fingerprint_value(
                value,
                LiveFingerprintContext::Content,
            )])
        }
        _ => normalize_live_fingerprint_value(value, LiveFingerprintContext::Content),
    }
}

fn normalized_live_fingerprint_image() -> serde_json::Value {
    // Keep one compact marker instead of base64 bytes, signed URLs, UUIDs, or
    // local paths.  Array position still distinguishes multiple images in a
    // turn, while retries with stripped/re-encoded payloads remain identical.
    serde_json::json!({"type": "image"})
}

fn is_live_fingerprint_image_block(
    object: &serde_json::Map<String, serde_json::Value>,
    context: LiveFingerprintContext,
) -> bool {
    let block_type = object
        .get("type")
        .and_then(serde_json::Value::as_str)
        .map(|value| value.to_ascii_lowercase());
    if matches!(
        context,
        LiveFingerprintContext::Content | LiveFingerprintContext::LegacyContent
    ) && matches!(
        block_type.as_deref(),
        Some("image" | "input_image" | "image_url")
    ) {
        return true;
    }
    // A few OpenAI-compatible retries omit `type` while retaining a sole
    // `image_url` wrapper.  Restrict this compatibility branch to message
    // content and a single key so a tool input containing an `image_url`
    // parameter is never collapsed into an image marker.
    context == LiveFingerprintContext::Content
        && object.len() == 1
        && (object.contains_key("image_url") || object.contains_key("imageUrl"))
}

fn is_live_internal_thinking_block(value: &serde_json::Value) -> bool {
    value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|kind| {
            kind.eq_ignore_ascii_case("thinking") || kind.eq_ignore_ascii_case("redacted_thinking")
        })
}

fn normalized_live_fingerprint_key(key: &str) -> String {
    key.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn should_drop_live_fingerprint_field(context: LiveFingerprintContext, key: &str) -> bool {
    let normalized = normalized_live_fingerprint_key(key);
    match context {
        LiveFingerprintContext::Root => matches!(
            normalized.as_str(),
            // Streaming and telemetry envelopes can change when a client
            // retries through a different HTTP path; they do not change the
            // generated turn.
            "stream"
                | "streamoptions"
                | "requestid"
                | "requestuuid"
                | "requestnonce"
                | "nonce"
                | "timestamp"
                | "createdat"
                | "updatedat"
                | "traceid"
                | "spanid"
                | "correlationid"
                | "xrequestid"
                | "xoriginalrequestid"
                | "xgrokreqid"
                | "xgrokagentid"
                | "xgroksessionid"
        ),
        LiveFingerprintContext::Metadata => matches!(
            normalized.as_str(),
            "requestid"
                | "requestuuid"
                | "requestnonce"
                | "nonce"
                | "timestamp"
                | "createdat"
                | "updatedat"
                | "traceid"
                | "spanid"
                | "correlationid"
                | "xrequestid"
                | "xoriginalrequestid"
                | "xgrokreqid"
                | "xgrokagentid"
                | "xgroksessionid"
        ),
        LiveFingerprintContext::Content => {
            // Prompt-cache directives are transport hints and are frequently
            // regenerated by Claude Code.  Tool ids, text, and all semantic
            // fields remain part of the fingerprint.
            normalized == "cachecontrol"
        }
        _ => false,
    }
}

/// Cursor conversation key for prompt compaction (`delta_only` / checkpoint).
///
/// Must match [`live_run_key_for`] used by the BiDi worker. Nested agents share
/// `X-Claude-Code-Session-Id` with the parent; using the raw session id here
/// would compact the nested prompt against the parent's checkpoint while the
/// nested Run is a fresh conversation.
fn continuation_for_request_for_account(
    session_id: Option<&str>,
    ctx: &RequestContext,
    account_key: Option<&str>,
) -> crate::providers::cursor::conversation::RunContinuation {
    let Some(sid) = session_id.filter(|s| !s.is_empty()) else {
        return crate::providers::cursor::conversation::continuation_for(None);
    };
    let key = live_run_key_for(live_run_identity_with_account(sid, ctx, account_key));
    crate::providers::cursor::conversation::continuation_for(Some(&key))
}

#[cfg(test)]
fn continuation_for_request(
    session_id: Option<&str>,
    ctx: &RequestContext,
) -> crate::providers::cursor::conversation::RunContinuation {
    continuation_for_request_for_account(session_id, ctx, None)
}

enum LiveResumeOutcome {
    Resumed(mpsc::Receiver<LiveEventResult>),
    TerminalError(String),
    MissingTools(Vec<String>),
    ResumeError(CursorError),
    SupersedeRunning(String),
    Conflict,
    /// Slot is free for a fresh start. Carries the reservation when a
    /// Succeeded tombstone was atomically released+reclaimed, so no other
    /// request can slip into the gap.
    Free(Option<LiveRunReservation>),
}

fn unresolved_live_tools_outcome(
    has_current_tool_results: bool,
    missing: Vec<String>,
    observed_run_id: Option<&str>,
) -> LiveResumeOutcome {
    if let Some(run_id) = observed_run_id {
        LiveResumeOutcome::SupersedeRunning(run_id.to_string())
    } else if has_current_tool_results {
        LiveResumeOutcome::MissingTools(missing)
    } else {
        LiveResumeOutcome::Conflict
    }
}

/// A fresh request may only take over a different live generation when the
/// driver has already established that generation is no longer serving its
/// downstream consumer.  Without one of these signals, cancelling the old
/// run can turn a normal overlapping request into a spurious 502 and lose the
/// original response.
fn fresh_request_can_supersede(run: &CursorLiveRunHandle, pending: &[PendingCursorExec]) -> bool {
    run.is_consumer_gone()
        || run.is_cancel_requested()
        || run.is_command_closed()
        || live_pending_must_supersede(pending)
}

/// A live start permit represents scarce generation capacity.  Once a
/// claimant has observed a healthy, different-operation generation, release
/// that permit and wait for the registry transition before reacquiring it;
/// otherwise every 50ms conflict probe repeatedly takes and drops a semaphore
/// permit, starving unrelated starts while the same session is still busy.
///
/// Succeeded/Ambiguous tombstones are deliberately not included: a different
/// fingerprint can atomically rotate those entries on the next claim.  A
/// replaceable Running generation is also left to the claimant so its
/// generation-bound cancellation can happen without another polling round.
fn live_start_should_wait_without_admission(
    session_id: &str,
    agent_id: Option<&str>,
    fingerprint: u64,
) -> bool {
    if LiveRunRegistry::is_starting_run(session_id, agent_id) {
        return true;
    }
    let Some(run) = LiveRunRegistry::get_run(session_id, agent_id) else {
        // `get_run` intentionally hides cancel-requested/terminaling handles.
        // A different operation may claim a hidden generation only when the
        // replacement predicate is true; an identical retry must instead
        // leave capacity released while it waits for replay/terminal cleanup.
        if LiveRunRegistry::replaceable_run_for_fresh_request(session_id, agent_id, fingerprint)
            .is_some()
        {
            return false;
        }
        return LiveRunRegistry::hidden_running_requires_wait(session_id, agent_id);
    };
    if run.request_fingerprint() == fingerprint {
        return false;
    }
    !run.is_replaceable_for_fresh_request()
}

/// Atomically take over a generation that has already been observed as
/// replaceable. The registry re-checks the run id and predicate under its
/// lock, then `cancel_and_wait` fences the old upstream before the caller
/// opens a replacement. Returning `None` means another transition won the
/// race and the caller should probe again.
async fn claim_hidden_fresh_replacement(
    session_id: &str,
    agent_id: Option<&str>,
    expected_run_id: &str,
) -> Result<Option<LiveRunReservation>, CursorError> {
    match LiveRunRegistry::claim_replacement_for_fresh_request(
        session_id,
        agent_id,
        expected_run_id,
    ) {
        LiveReplacementClaim::Conflict => Ok(None),
        LiveReplacementClaim::Reserved {
            mut reservation,
            superseded: Some(handle),
        } => {
            reservation.set_operation_fingerprint(handle.request_fingerprint());
            reservation.protect_on_drop();
            let cancel_result = handle.cancel_and_wait().await;
            match finish_replacement_after_cancel(reservation, handle, false, cancel_result) {
                Ok(reservation) => Ok(Some(reservation)),
                Err(error) => Err(error),
            }
        }
        LiveReplacementClaim::Reserved {
            reservation,
            superseded: None,
        } => Ok(Some(reservation)),
    }
}

/// Classify a slot that `get_run` hides. A dying Running generation is
/// superseded. Ambiguous stays occupied until its TTL and fails closed because
/// retrying cannot prove whether the prior Run was accepted. A Succeeded
/// tombstone with a *new* fingerprint is released so compact/next-turn can
/// start. Starting and same-fingerprint Succeeded stay occupied.
fn resume_when_slot_has_no_runnable_handle(
    session_id: &str,
    agent_id: Option<&str>,
    fingerprint: u64,
    observed_run_id: Option<&str>,
    has_tool_results: bool,
) -> Option<LiveResumeOutcome> {
    // A hidden Running handle is only safe to replace for a fresh request when
    // its own state proves that it is dying. Tool-result retries retain their
    // historical stale-generation takeover behavior because the result ids
    // cannot be delivered to an unknown replacement generation.
    let replaceable_generation = if has_tool_results {
        LiveRunRegistry::running_generation(session_id, agent_id)
    } else {
        LiveRunRegistry::replaceable_running_generation(session_id, agent_id)
    };
    if let Some(run_id) = replaceable_generation {
        // get_run hides cancel-requested / terminal handles. Compact and the
        // next grok turn close the previous SSE first; the dying generation
        // must be superseded, not 409'd.
        return Some(LiveResumeOutcome::SupersedeRunning(run_id));
    }
    if LiveRunRegistry::is_ambiguous_for_operation(session_id, agent_id, fingerprint) {
        return Some(LiveResumeOutcome::ResumeError(live_ambiguous_accept_error()));
    }
    if let Some(reservation) = LiveRunRegistry::claim_ambiguous_release_for_new_operation(
        session_id,
        agent_id,
        fingerprint,
    ) {
        return Some(LiveResumeOutcome::Free(Some(reservation)));
    }
    // Identical retry of a completed turn: deliver the retained response.
    if let Some(events) = LiveRunRegistry::completed_replay_for(session_id, agent_id, fingerprint) {
        return Some(LiveResumeOutcome::Resumed(replay_completed_turn_channel(
            session_id, &events,
        )));
    }
    // Atomic release+reserve of a different-operation Succeeded tombstone:
    // the freed slot is claimed under the same lock, so a concurrent identical
    // retry of the OLD operation can never start a duplicate Run in between.
    if let Some(reservation) =
        LiveRunRegistry::claim_success_release_for_new_operation(session_id, agent_id, fingerprint)
    {
        return Some(LiveResumeOutcome::Free(Some(reservation)));
    }
    if !LiveRunRegistry::is_occupied_run(session_id, agent_id) {
        if let Some(run_id) = observed_run_id {
            if has_tool_results {
                return Some(LiveResumeOutcome::SupersedeRunning(run_id.to_string()));
            }
            return Some(LiveResumeOutcome::ResumeError(live_run_busy_error()));
        }
        return Some(LiveResumeOutcome::Free(None));
    }
    match LiveRunRegistry::probe_run(session_id, agent_id) {
        LiveRunProbe::TerminalError(error) if live_probe_error_blocks_new_run(&error) => {
            Some(LiveResumeOutcome::TerminalError(error))
        }
        LiveRunProbe::TerminalError(_) => {
            if let Some(run_id) = observed_run_id {
                if has_tool_results {
                    Some(LiveResumeOutcome::SupersedeRunning(run_id.to_string()))
                } else {
                    Some(LiveResumeOutcome::Free(None))
                }
            } else {
                Some(LiveResumeOutcome::Free(None))
            }
        }
        LiveRunProbe::Free => {
            if let Some(run_id) = observed_run_id {
                if has_tool_results {
                    Some(LiveResumeOutcome::SupersedeRunning(run_id.to_string()))
                } else {
                    Some(LiveResumeOutcome::ResumeError(live_run_busy_error()))
                }
            } else {
                Some(LiveResumeOutcome::Free(None))
            }
        }
        LiveRunProbe::Occupied => None,
    }
}

/// Wait for an in-flight BiDi run to expose pending tools (and resume), finish,
/// or fail — instead of immediately 409'ing concurrent same-session POSTs.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn await_live_run_resume(
    session_id: &str,
    agent_id: Option<&str>,
    body: &MessagesRequest,
    _message_id: String,
    _wire_model: String,
    _estimated_input: u64,
    _monitor: Option<(crate::monitor::MonitorHandle, String)>,
    _want_stream: bool,
) -> LiveResumeOutcome {
    await_live_run_resume_for_operation(
        session_id,
        agent_id,
        body,
        _message_id,
        _wire_model,
        _estimated_input,
        _monitor,
        _want_stream,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn await_live_run_resume_for_operation(
    session_id: &str,
    agent_id: Option<&str>,
    body: &MessagesRequest,
    _message_id: String,
    _wire_model: String,
    _estimated_input: u64,
    _monitor: Option<(crate::monitor::MonitorHandle, String)>,
    _want_stream: bool,
    client_request_id: Option<&str>,
) -> LiveResumeOutcome {
    let has_tool_results = request_has_current_tool_result(body);
    let fingerprint =
        live_request_fingerprint(&live_operation_fingerprint_payload(body, client_request_id));
    // Tool-result resumes: wait for pending tools to appear (race with expose).
    // Keep this below downstream stream-idle: no Anthropic response exists yet,
    // so SSE pings cannot protect this pre-response window.
    let wait_ms = if has_tool_results {
        env_u64_millis(
            "CCP_CURSOR_LIVE_RESUME_WAIT_MS",
            LIVE_RESUME_WAIT_DEFAULT_MS,
        )
        .clamp(500, LIVE_RESUME_WAIT_MAX_MS)
    } else {
        env_u64_millis(
            "CCP_CURSOR_LIVE_NESTED_WAIT_MS",
            LIVE_NESTED_WAIT_DEFAULT_MS,
        )
        .clamp(500, LIVE_NESTED_WAIT_MAX_MS)
    };
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
    let mut observed_run_id: Option<String> = None;
    let mut observed_non_running_slot = false;

    while tokio::time::Instant::now() < deadline {
        let Some(run) = LiveRunRegistry::get_run(session_id, agent_id) else {
            if let Some(outcome) = resume_when_slot_has_no_runnable_handle(
                session_id,
                agent_id,
                fingerprint,
                observed_run_id.as_deref(),
                has_tool_results,
            ) {
                return outcome;
            }
            // Starting / Succeeded: wait for Running or Free.
            observed_non_running_slot = true;
            tokio::time::sleep(Duration::from_millis(25)).await;
            continue;
        };
        if run.request_fingerprint() == fingerprint {
            // The active segment IS this operation. Attach (and steal a
            // half-open original SSE) instead of 503-bouncing until the run
            // dies — grok-build retries 503 as transient overload and that
            // is the "already active" storm.
            if let Some(events) =
                attach_live_run_with_pre_response_wait(Arc::clone(&run), fingerprint).await
            {
                return LiveResumeOutcome::Resumed(events);
            }
            // The run can seal its terminal state after the attach waiter
            // observes the old handle. Reconcile that transition before
            // returning 503, so an identical retry receives the retained
            // segment (or the real terminal error) instead of entering an
            // "already active" retry loop.
            if let Some(events) =
                LiveRunRegistry::completed_replay_for(session_id, agent_id, fingerprint)
            {
                return LiveResumeOutcome::Resumed(replay_completed_turn_channel(
                    session_id, &events,
                ));
            }
            match LiveRunRegistry::probe_run(session_id, agent_id) {
                LiveRunProbe::Free => return LiveResumeOutcome::Free(None),
                LiveRunProbe::TerminalError(error) if live_probe_error_blocks_new_run(&error) => {
                    return LiveResumeOutcome::TerminalError(error);
                }
                LiveRunProbe::TerminalError(_) => return LiveResumeOutcome::Free(None),
                LiveRunProbe::Occupied => {}
            }
            return LiveResumeOutcome::ResumeError(live_run_busy_error());
        }
        if observed_non_running_slot {
            // A Starting slot became Running. Tool-result waiters must not
            // attach to an unobserved generation. This waiter was never sent
            // upstream, so shed it as retryable busy rather than fatal 409.
            if has_tool_results {
                return LiveResumeOutcome::ResumeError(live_replacement_conflict_error(true));
            }
            let pending = run.pending_tools();
            if fresh_request_can_supersede(&run, &pending) {
                return LiveResumeOutcome::SupersedeRunning(run.run_id().to_string());
            }
            return LiveResumeOutcome::ResumeError(live_run_busy_error());
        }
        match observed_run_id.as_deref() {
            Some(observed) if observed != run.run_id() => {
                return LiveResumeOutcome::ResumeError(live_replacement_conflict_error(
                    has_tool_results,
                ));
            }
            None => observed_run_id = Some(run.run_id().to_string()),
            _ => {}
        }
        if let Some(error) =
            LiveRunRegistry::take_terminal_error_for_run(session_id, agent_id, run.run_id())
        {
            return LiveResumeOutcome::TerminalError(error);
        }
        let pending = run.pending_tools();
        if !pending.is_empty() {
            if live_pending_must_supersede(&pending) {
                return LiveResumeOutcome::SupersedeRunning(run.run_id().to_string());
            }
            if !has_tool_results {
                // This is a fresh/steering request, not a result for the
                // exposed batch. Give the owning request a short chance to
                // resume, then supersede it below.
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            }
            match collect_live_tool_results(body, &pending) {
                Ok(tool_results) => match run
                    .resume_batch_for_operation(tool_results, fingerprint)
                    .await
                {
                    Ok(events) => {
                        return LiveResumeOutcome::Resumed(events);
                    }
                    Err(error) if live_resume_error_is_dead_driver(&error) => {
                        return LiveResumeOutcome::SupersedeRunning(run.run_id().to_string());
                    }
                    Err(error) => return LiveResumeOutcome::ResumeError(error),
                },
                Err(_missing) => {
                    // grok-build will not synthesize the missing Cursor ids.
                    // Partial, leftover, dual-id, and extra batches are all
                    // unrecoverable as a resume — take over this generation.
                    return LiveResumeOutcome::SupersedeRunning(run.run_id().to_string());
                }
            }
        }
        // Still generating with empty pending — wait for tools or completion.
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let Some(run) = LiveRunRegistry::get_run(session_id, agent_id) else {
        if let Some(outcome) = resume_when_slot_has_no_runnable_handle(
            session_id,
            agent_id,
            fingerprint,
            observed_run_id.as_deref(),
            has_tool_results,
        ) {
            return outcome;
        }
        return LiveResumeOutcome::ResumeError(live_run_busy_error());
    };
    if run.request_fingerprint() == fingerprint {
        if let Some(events) =
            attach_live_run_with_pre_response_wait(Arc::clone(&run), fingerprint).await
        {
            return LiveResumeOutcome::Resumed(events);
        }
        if let Some(events) =
            LiveRunRegistry::completed_replay_for(session_id, agent_id, fingerprint)
        {
            return LiveResumeOutcome::Resumed(replay_completed_turn_channel(session_id, &events));
        }
        match LiveRunRegistry::probe_run(session_id, agent_id) {
            LiveRunProbe::Free => return LiveResumeOutcome::Free(None),
            LiveRunProbe::TerminalError(error) if live_probe_error_blocks_new_run(&error) => {
                return LiveResumeOutcome::TerminalError(error);
            }
            LiveRunProbe::TerminalError(_) => return LiveResumeOutcome::Free(None),
            LiveRunProbe::Occupied => {}
        }
        return LiveResumeOutcome::ResumeError(live_run_busy_error());
    }
    if observed_non_running_slot {
        if has_tool_results {
            return LiveResumeOutcome::ResumeError(live_replacement_conflict_error(true));
        }
        let pending = run.pending_tools();
        if fresh_request_can_supersede(&run, &pending) {
            return LiveResumeOutcome::SupersedeRunning(run.run_id().to_string());
        }
        return LiveResumeOutcome::ResumeError(live_run_busy_error());
    }
    match observed_run_id.as_deref() {
        Some(observed) if observed != run.run_id() => {
            return LiveResumeOutcome::ResumeError(live_replacement_conflict_error(
                has_tool_results,
            ));
        }
        None => observed_run_id = Some(run.run_id().to_string()),
        _ => {}
    }
    if let Some(error) =
        LiveRunRegistry::take_terminal_error_for_run(session_id, agent_id, run.run_id())
    {
        return LiveResumeOutcome::TerminalError(error);
    }
    let pending = run.pending_tools();
    if !pending.is_empty() {
        if live_pending_must_supersede(&pending) {
            return LiveResumeOutcome::SupersedeRunning(
                observed_run_id
                    .clone()
                    .unwrap_or_else(|| run.run_id().to_string()),
            );
        }
        if !has_tool_results {
            let missing = pending
                .iter()
                .map(|exec| exec.tool_use_id.clone())
                .collect();
            if fresh_request_can_supersede(&run, &pending) {
                return unresolved_live_tools_outcome(false, missing, observed_run_id.as_deref());
            }
            return LiveResumeOutcome::ResumeError(live_run_busy_error());
        }
        match collect_live_tool_results(body, &pending) {
            Ok(tool_results) => match run
                .resume_batch_for_operation(tool_results, fingerprint)
                .await
            {
                Ok(events) => {
                    return LiveResumeOutcome::Resumed(events);
                }
                Err(error) if live_resume_error_is_dead_driver(&error) => {
                    return LiveResumeOutcome::SupersedeRunning(
                        observed_run_id
                            .clone()
                            .unwrap_or_else(|| run.run_id().to_string()),
                    );
                }
                Err(error) => return LiveResumeOutcome::ResumeError(error),
            },
            Err(_missing) => {
                return LiveResumeOutcome::SupersedeRunning(
                    observed_run_id
                        .clone()
                        .unwrap_or_else(|| run.run_id().to_string()),
                );
            }
        }
    }
    if has_tool_results {
        return LiveResumeOutcome::SupersedeRunning(
            observed_run_id.expect("a live handle established the observed generation"),
        );
    }
    if fresh_request_can_supersede(&run, &pending) {
        return LiveResumeOutcome::SupersedeRunning(run.run_id().to_string());
    }
    LiveResumeOutcome::ResumeError(live_run_busy_error())
}

fn env_u64_millis(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Distinguish a short-lived provider-pool failure from account or capacity
/// policy errors. The former can recover while the request is still open;
/// retrying the latter only delays a useful error and may amplify a shed.
fn is_transient_resource_exhausted(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    (is_transient_provider_error_message(message)
        || lower.contains("error_resource_exhausted")
        || (lower.contains("unable to reach the model provider")
            && lower.contains("resource_exhausted")))
        && !is_non_retryable_provider_error_message(message)
        && !crate::retry::is_billing_block(message)
        && !crate::retry::is_capacity_shed(message)
        && !crate::retry::is_policy_rate_limit(message)
}

/// Cursor sometimes exhausts its own internal step loop while the upstream
/// Run is still safe to replay. Treat this exact provider failure as transient
/// while the downstream response is still pre-output; policy errors remain
/// terminal through the guards below.
fn is_transient_step_failure(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("failed to run step")
        && lower.contains("exceeded max retries")
        && (lower.contains("[internal]")
            || lower.contains("connect error 502")
            || lower.contains("cursor error 502"))
        && !crate::retry::is_billing_block(message)
        && !crate::retry::is_capacity_shed(message)
        && !crate::retry::is_policy_rate_limit(message)
}

fn cursor_step_failure_retry_limit(message: &str) -> u32 {
    if !is_transient_step_failure(message) {
        return crate::retry::MAX_RATE_LIMIT_RETRIES;
    }

    std::env::var("CCP_CURSOR_STEP_FAILURE_RETRIES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(CURSOR_STEP_FAILURE_RETRIES_DEFAULT as u64)
        .clamp(1, CURSOR_STEP_FAILURE_RETRIES_MAX as u64) as u32
}

fn cursor_transient_retry_limit(message: &str) -> u32 {
    if is_transient_step_failure(message) {
        return cursor_step_failure_retry_limit(message);
    }
    if !is_transient_resource_exhausted(message) {
        return crate::retry::MAX_RATE_LIMIT_RETRIES;
    }

    env_u64_millis(
        "CCP_CURSOR_RESOURCE_RETRIES",
        CURSOR_RESOURCE_RETRIES_DEFAULT as u64,
    )
    .clamp(1, CURSOR_RESOURCE_RETRIES_MAX as u64) as u32
}

fn strip_cursor_run_generation(id: &str) -> &str {
    id.split("__cursor_run_").next().unwrap_or(id)
}

fn cursor_run_generation(id: &str) -> Option<&str> {
    id.rsplit_once("__cursor_run_")
        .map(|(_, generation)| generation)
        .and_then(|generation| {
            generation
                .split(|c: char| c.is_whitespace() || c == ',')
                .next()
        })
        .filter(|generation| !generation.is_empty())
}

fn tool_id_match_tokens(id: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    for raw in id.split(|c: char| c.is_whitespace() || c == ',') {
        if raw.is_empty() {
            continue;
        }
        let stripped = strip_cursor_run_generation(raw);
        tokens.push(stripped);
        if let Some(idx) = stripped.find("_fc_") {
            tokens.push(&stripped[..idx]);
            tokens.push(&stripped[idx + 1..]);
        }
    }
    tokens
}

fn tool_use_ids_equivalent(left: &str, right: &str) -> bool {
    match (cursor_run_generation(left), cursor_run_generation(right)) {
        (Some(left), Some(right)) if left == right => {}
        (None, None) => {}
        _ => return false,
    }
    let left_tokens = tool_id_match_tokens(left);
    let right_tokens = tool_id_match_tokens(right);
    left_tokens.iter().any(|left| {
        right_tokens.iter().any(|right| {
            left == right
                || left.strip_prefix("fc_").is_some_and(|rest| rest == *right)
                || right.strip_prefix("fc_").is_some_and(|rest| rest == *left)
        })
    })
}

fn collect_live_tool_results(
    body: &MessagesRequest,
    pending: &[PendingCursorExec],
) -> Result<Vec<(String, serde_json::Value)>, Vec<String>> {
    let mut claimed = vec![false; pending.len()];
    let mut results_by_pending: Vec<Option<&serde_json::Value>> = vec![None; pending.len()];
    let mut invalid_current = Vec::new();

    for block in current_user_blocks(body) {
        if block.get("type").and_then(|value| value.as_str()) != Some("tool_result") {
            invalid_current.push("<non-tool_result content>".to_string());
            continue;
        }
        let Some(tool_use_id) = block.get("tool_use_id").and_then(|value| value.as_str()) else {
            invalid_current.push("<missing tool_use_id>".to_string());
            continue;
        };
        let Some(index) = pending.iter().enumerate().position(|(index, exec)| {
            !claimed[index] && tool_use_ids_equivalent(exec.tool_use_id.as_str(), tool_use_id)
        }) else {
            invalid_current.push(tool_use_id.to_string());
            continue;
        };
        claimed[index] = true;
        results_by_pending[index] = Some(block);
    }

    let missing: Vec<String> = pending
        .iter()
        .zip(claimed.iter())
        .filter(|(_, claimed)| !**claimed)
        .map(|(exec, _)| exec.tool_use_id.clone())
        .collect();
    if !missing.is_empty() {
        return Err(missing);
    }
    if !invalid_current.is_empty() {
        return Err(invalid_current);
    }

    Ok(pending
        .iter()
        .zip(results_by_pending)
        .map(|(exec, block)| {
            (
                exec.tool_use_id.clone(),
                block.expect("validated current tool result").clone(),
            )
        })
        .collect())
}

fn request_has_current_tool_result(body: &MessagesRequest) -> bool {
    current_user_blocks(body).iter().any(|block| {
        block.get("type").and_then(|value| value.as_str()) == Some("tool_result")
            && block
                .get("tool_use_id")
                .and_then(|value| value.as_str())
                .is_some_and(|id| !id.is_empty())
    })
}

/// An open account/model breaker blocks only work that could create a new
/// Cursor Run. Matching tool-result continuations and identical retries can
/// still attach to a Run that is already accepted upstream; rejecting those
/// here would strand native tools or turn a recoverable dropped SSE into a
/// new client retry storm without reducing Cursor load.
fn policy_preflight_can_attach_existing_run_for_account(
    body: &MessagesRequest,
    ctx: &RequestContext,
    account_key: Option<&str>,
) -> bool {
    if request_has_current_tool_result(body) {
        // Route tool results through the registry first. A matching accepted
        // Run can resume even while its account/model breaker is open; a stale
        // result is rejected by the registry, and every true fresh start still
        // hits the final pre-open breaker below.
        return true;
    }
    let Some(session_id) = ctx.session_id.as_deref().filter(|id| !id.is_empty()) else {
        return false;
    };
    let fingerprint = live_request_fingerprint(&live_operation_fingerprint_payload(
        body,
        ctx.client_request_id.as_deref(),
    ));
    let agent_id = account_scoped_agent_id(account_key, claude_agent_id(ctx));
    LiveRunRegistry::get_run(session_id, agent_id.as_deref())
        .is_some_and(|run| run.request_fingerprint() == fingerprint)
        || LiveRunRegistry::completed_replay_for(session_id, agent_id.as_deref(), fingerprint)
            .is_some()
}

#[cfg(test)]
fn policy_preflight_can_attach_existing_run(body: &MessagesRequest, ctx: &RequestContext) -> bool {
    policy_preflight_can_attach_existing_run_for_account(body, ctx, None)
}

#[cfg(test)]
fn live_current_batch_is_unrelated(body: &MessagesRequest, pending: &[PendingCursorExec]) -> bool {
    if pending.is_empty() {
        return false;
    }
    let mut saw_tool_result = false;
    for block in current_user_blocks(body) {
        if block.get("type").and_then(|value| value.as_str()) != Some("tool_result") {
            continue;
        }
        let Some(tool_use_id) = block
            .get("tool_use_id")
            .and_then(|value| value.as_str())
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        saw_tool_result = true;
        if pending
            .iter()
            .any(|exec| tool_use_ids_equivalent(exec.tool_use_id.as_str(), tool_use_id))
        {
            return false;
        }
    }
    saw_tool_result
}

pub struct CursorProvider;

impl Default for CursorProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Provider for CursorProvider {
    fn name(&self) -> &'static str {
        "cursor"
    }

    fn supported_models(&self) -> Vec<String> {
        model::cursor_supported_models()
    }

    fn cli(&self) -> &'static dyn CliHandlers {
        &CURSOR_CLI
    }

    async fn handle_messages(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let mut ctx = ctx;
        let message_id = format!("msg_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
        let want_stream = body.stream;
        let xai_compact = is_compact_request_with_helper(
            &body,
            ctx.client_request_id.as_deref(),
            ctx.stainless_helper.as_deref(),
        );
        if xai_compact {
            // Context compaction is a distinct operation on the same Claude
            // session.  Keep the session for Cursor's long-lived transport,
            // but isolate registry/checkpoint/bridge state with a stable lane
            // identity so retries never collide with the preceding turn.
            let compact_id = compact_agent_id(&body, &ctx);
            ctx.claude_code.agent_id = Some(compact_id);
            ctx.claude_code.parent_agent_id = None;
        }
        let requested_model = body.model.as_deref().unwrap_or("cursor");
        let wire_model = anthropic_wire_model(requested_model);
        let effort = match crate::providers::translate_shared::read_effort(&body) {
            Ok(effort) => effort,
            Err(error) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    error.to_string(),
                );
            }
        };
        let routed_model =
            crate::providers::cursor::model::apply_effort_to_cursor_model(requested_model, effort);
        let model = routed_model.as_str();
        // Resolve Sand from the same request-scoped helper used by the direct
        // Cursor client.  It checks both the public alias and the concrete
        // catalog id, which is important for Fable's `[1m]`/thinking aliases.
        let client_type = crate::config::cursor_client_type_for_model(model);
        let uses_sand = client_type.trim().eq_ignore_ascii_case("sand");
        // The server records the incoming model before provider-specific effort
        // aliases are applied. Publish the final request-scoped route here as
        // well so the TUI reflects the identity actually sent to Cursor.
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.client_type_resolved(&ctx.req_id, &client_type);
        }

        let resolved = match resolve_cursor_model(model) {
            Ok(resolved) => resolved,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    format!("Model \"{model}\" is not supported: {e}"),
                );
            }
        };

        // Read auth before the live-slot/SSE path so an account/model policy
        // breaker can return a real HTTP 429. If this check runs after
        // `live_sse_response`, Claude Code sees HTTP 200 plus an error event
        // and immediately fans out another retry wave.
        let auth_selection = match load_cursor_auth_for_model_async(model.to_string()).await {
            Ok(Some(selection)) => selection,
            Ok(None) => {
                return json_error(
                    StatusCode::UNAUTHORIZED,
                    "authentication_error",
                    missing_auth_message(),
                );
            }
            Err(err) => {
                return json_error(
                    StatusCode::UNAUTHORIZED,
                    "authentication_error",
                    format!("Cursor auth failed: {err}"),
                );
            }
        };
        // Keep the account that was selected before opportunistic Sand
        // rebalancing. If the active lane is saturated, the request may be
        // moved to another saved profile below; that original profile should
        // remain marked as attempted for this logical request so a later
        // transport failover does not immediately rotate back into the lane
        // we just observed as full.
        let pre_rebalance_account_id = auth_selection.account_id.clone();
        let pre_rebalance_account_digest = cursor_account_digest(&auth_selection.auth.access_token);
        // Keep the active-account default for ordinary traffic, but when a
        // Sand lane is already full choose an idle saved account before the
        // request enters the admission queue. This is the first line of
        // defence for large Claude Code/Grok fan-outs; later transport errors
        // still use the bounded failover path below.
        let auth_selection = if uses_sand {
            // Account balancing must inspect the same canonical Sand family
            // that keys the InferenceService open gate. The routed CLI id may
            // carry an effort suffix (for example
            // `cursor-grok-4.6-xhigh-fast`), while the Sand wire uses
            // `grok-4.6`; passing only the former made every lane look unseen
            // and concentrated the whole fan-out on the active account.
            let sand_model_for_balance = resolve_sand_model_id(&resolved.model_id);
            maybe_rebalance_sand_account(
                model,
                &sand_model_for_balance,
                &client_type,
                auth_selection,
            )
            .await
        } else {
            auth_selection
        };
        let selected_account_is_active = auth_selection.active;
        // Prefer the persisted profile id so labels/email aliases and JWT
        // refreshes remain in the same state partition. Environment-backed
        // credentials have no profile id; their stable bearer digest is the
        // appropriate fallback.
        let mut account_key = auth_selection
            .account_id
            .clone()
            .unwrap_or_else(|| cursor_account_digest(&auth_selection.auth.access_token));
        let mut selection_fields = quota_diagnostic_fields(
            &account_key,
            &auth_selection.auth.access_token,
            model,
            &client_type,
        );
        selection_fields.insert(
            "accountSource".to_string(),
            serde_json::json!(if auth_selection.account_id.is_some() {
                "registry"
            } else {
                "legacy-or-environment"
            }),
        );
        selection_fields.insert(
            "accountBinding".to_string(),
            serde_json::json!(
                if crate::config::cursor_account_for_model(model).is_some() {
                    "model"
                } else if auth_selection.account_id.is_some() {
                    "active"
                } else {
                    "environment-or-legacy"
                }
            ),
        );
        selection_fields.insert(
            "active".to_string(),
            serde_json::json!(auth_selection.active),
        );
        create_logger("cursor").info("cursor_account_selected", Some(selection_fields));
        let mut auth = auth_selection.auth;

        // Near expiry: refresh first. A rotated token represents a fresh
        // account credential and must not inherit the old token's breaker. A
        // model-pinned inactive account was refreshed by the selector above;
        // only the active compatibility account can use this global helper.
        if selected_account_is_active
            && matches!(auth.expires, Some(expires) if expires <= now_ms() + 60_000)
        {
            match force_refresh_cursor_auth_async(auth.access_token.clone()).await {
                Ok(Some(refreshed)) => auth = refreshed,
                Ok(None) | Err(_) => {
                    return json_error(
                        StatusCode::UNAUTHORIZED,
                        "authentication_error",
                        expired_auth_message(&auth),
                    );
                }
            }
        }
        let has_tool_results = request_has_current_tool_result(&body);
        // Keep one failover state for the whole Sand request, including the
        // preflight phase.  If the active account is already in cooldown, pick
        // an unused saved account before opening the InferenceService stream;
        // otherwise every Claude Code retry would fail fast on the same stale
        // account and never reach the healthy pool.
        let account_was_rebalanced = uses_sand
            && (auth_selection.account_id != pre_rebalance_account_id
                || cursor_account_digest(&auth.access_token) != pre_rebalance_account_digest);
        let sand_account_failover_state = Arc::new(Mutex::new({
            let mut state = AccountFailoverState::new(&auth.access_token);
            // Preserve both stable registry identity and the prior bearer
            // digest. A refreshed token can change the latter while the
            // profile id remains the same; storing both prevents either
            // spelling from re-entering the candidate walk.
            mark_pre_rebalance_account_attempted(
                &mut state,
                pre_rebalance_account_id.as_deref(),
                &pre_rebalance_account_digest,
                account_was_rebalanced,
            );
            state
        }));
        if uses_sand {
            while let Err(error) =
                policy_rate_limit_preflight(model, &client_type, &auth.access_token)
            {
                if !is_account_failover_policy_error(&error.client_message()) {
                    return map_cursor_error_to_response(&error);
                }
                let Some(replacement) = account_failover_replacement_token_async(
                    auth.access_token.clone(),
                    model.to_string(),
                    client_type.clone(),
                    Arc::clone(&sand_account_failover_state),
                )
                .await
                else {
                    return map_cursor_error_to_response(&error);
                };
                auth.access_token = replacement;
                account_key = cursor_account_key_for_token_async(auth.access_token.clone()).await;
                create_logger("cursor").info(
                    "sand_preflight_account_failover",
                    Some(serde_json::Map::from_iter([
                        ("model".into(), serde_json::json!(model)),
                        ("clientType".into(), serde_json::json!(&client_type)),
                        ("recovery".into(), serde_json::json!("fresh_request")),
                    ])),
                );
            }
        } else if !policy_preflight_can_attach_existing_run_for_account(
            &body,
            &ctx,
            Some(&account_key),
        ) && let Err(error) =
            policy_rate_limit_preflight(model, &client_type, &auth.access_token)
        {
            return map_cursor_error_to_response(&error);
        }

        // Sand is stateless and replays the complete Anthropic history on
        // every turn. A previous text/native bridge pause therefore only
        // needs to be acknowledged and removed before the next Sand request;
        // the supplied tool_result is carried in that full history. Keeping
        // the stale buffered events would replay an old tool call instead of
        // asking the model to continue.
        if uses_sand
            && !xai_compact
            && let Some(bridge_key) = bridge_registry_key_for_account(&ctx, Some(&account_key))
            && let Some(pending) = BridgeRegistry::pending_tool(&bridge_key)
            && find_tool_result(&body, pending.tool_use_id()).is_some()
        {
            BridgeRegistry::remove(&bridge_key);
            create_logger("cursor").info(
                "sand_tool_bridge_resumed_by_history",
                Some(serde_json::Map::from_iter([
                    ("model".into(), serde_json::json!(model)),
                    ("tool".into(), serde_json::json!(pending.name())),
                    ("recovery".into(), serde_json::json!("full_history")),
                ])),
            );
        }

        // Sand is a Desktop InferenceService transport.  It is deliberately
        // dispatched before any LiveRunRegistry admission or AgentService
        // continuation logic: the latter endpoint now rejects
        // `x-cursor-client-type: sand` with a deterministic 400.  Sand sends
        // the complete rendered Anthropic history on every turn, so it does
        // not need (and must not share) the resumable AgentService state.
        if uses_sand {
            let sand_model = resolve_sand_model_id(&resolved.model_id);
            return sand_direct_response(
                &body,
                &ctx,
                &auth.access_token,
                message_id,
                wire_model,
                &sand_model,
                &resolved.model_id,
                xai_compact,
                sand_account_failover_state,
            )
            .await;
        }

        // True Cursor BiDi continuation: the preceding Anthropic response ended
        // at tool_use, but the upstream AgentService/Run stream is still alive.
        // Route the matching tool_result back onto that exact request stream
        // instead of replaying the whole conversation as a fresh Cursor run.
        let mut preclaimed_live_reservation = None;
        let mut resumed_live_events = None;
        let defer_fresh_stream = defer_fresh_stream_admission(
            want_stream,
            ctx.hold_http_until_live_open,
            has_tool_results,
        );
        // Responses callers set `hold_http_until_live_open` so pre-output
        // policy errors can be translated to JSON.  That must not force a
        // fresh turn through the nested-resume waiter: once the streaming
        // envelope is committed, the normal generation claim path can wait
        // behind the prior turn and emit heartbeats.  Only a current
        // tool_result needs the exact bounded resume probe.
        let skip_resume_probe = fresh_stream_can_skip_resume_probe(want_stream, has_tool_results);
        if !xai_compact && let Some(session_id) = ctx.session_id.as_deref() {
            // Registry keys must include the selected account. Claude Code
            // reuses a session/agent id across account switches; using the
            // raw agent here would attach a retry to the prior account's
            // generation.
            let registry_agent_id =
                account_scoped_agent_id(Some(&account_key), claude_agent_id(&ctx));
            let agent_id = registry_agent_id.as_deref();
            let fingerprint_payload =
                live_operation_fingerprint_payload(&body, ctx.client_request_id.as_deref());
            let fingerprint = live_request_fingerprint(&fingerprint_payload);
            // Identical retry of a turn that already completed: deliver the
            // retained response instead of stalling on the Succeeded tombstone
            // (client crashed / timed out / dropped before it saw message_end).
            if let Some(events) =
                LiveRunRegistry::completed_replay_for(session_id, agent_id, fingerprint)
            {
                resumed_live_events = Some(replay_completed_turn_channel(session_id, &events));
            }
            if resumed_live_events.is_none()
                && let Some(reservation) = LiveRunRegistry::claim_success_release_for_new_operation(
                    session_id,
                    agent_id,
                    fingerprint,
                )
            {
                // Atomic release+reserve: no window in which a concurrent
                // identical retry of the completed operation could take the
                // freed slot and execute a duplicate.
                preclaimed_live_reservation = Some(reservation);
            }
            if resumed_live_events.is_none()
                && preclaimed_live_reservation.is_none()
                && let Some(reservation) =
                    LiveRunRegistry::claim_ambiguous_release_for_new_operation(
                        session_id,
                        agent_id,
                        fingerprint,
                    )
            {
                preclaimed_live_reservation = Some(reservation);
            }
            if resumed_live_events.is_none() && preclaimed_live_reservation.is_none() {
                match LiveRunRegistry::probe_run(session_id, agent_id) {
                    LiveRunProbe::TerminalError(error)
                        if live_probe_error_blocks_new_run(&error) =>
                    {
                        if let Some(reservation) =
                            LiveRunRegistry::claim_ambiguous_release_for_new_operation(
                                session_id,
                                agent_id,
                                fingerprint,
                            )
                        {
                            preclaimed_live_reservation = Some(reservation);
                        } else {
                            return json_error_from_cursor_message(error);
                        }
                    }
                    LiveRunProbe::TerminalError(_) => {
                        // Retryable terminal failures are removed by probe_run.
                    }
                    LiveRunProbe::Free => {
                        if reject_orphaned_native_results_when_live_slot_is_free(&body) {
                            return json_error(
                                StatusCode::CONFLICT,
                                "invalid_request_error",
                                "Stale Cursor tool_result cannot start a new live run",
                            );
                        }
                    }
                    LiveRunProbe::Occupied if defer_fresh_stream || skip_resume_probe => {
                        // Do not hold the HTTP handler in the pre-response
                        // resume waiter for a fresh streaming turn. The
                        // downstream SSE starts now (and emits heartbeats),
                        // while `start_live_events_with_retries...` performs
                        // the same-session single-flight wait and claims the
                        // slot once the observed generation advances.
                        create_logger("cursor").info(
                            "live_request_queued_behind_active_run",
                            Some(serde_json::Map::from_iter([
                                ("sessionId".into(), serde_json::json!(session_id)),
                                ("agentId".into(), serde_json::json!(agent_id)),
                                ("reqId".into(), serde_json::json!(&ctx.req_id)),
                                (
                                    "clientRequestId".into(),
                                    serde_json::json!(ctx.client_request_id.as_deref()),
                                ),
                            ])),
                        );
                    }
                    LiveRunProbe::Occupied => {
                        let estimated_input = estimate_request_input_tokens(&body);
                        let monitor = ctx
                            .monitor
                            .clone()
                            .map(|handle| (handle, ctx.req_id.clone()));
                        match await_live_run_resume_for_operation(
                            session_id,
                            agent_id,
                            &body,
                            message_id.clone(),
                            wire_model.clone(),
                            estimated_input,
                            monitor,
                            want_stream,
                            ctx.client_request_id.as_deref(),
                        )
                        .await
                        {
                            LiveResumeOutcome::Resumed(events) => {
                                resumed_live_events = Some(events);
                            }
                            LiveResumeOutcome::TerminalError(error)
                                if live_probe_error_blocks_new_run(&error) =>
                            {
                                if let Some(reservation) =
                                    LiveRunRegistry::claim_ambiguous_release_for_new_operation(
                                        session_id,
                                        agent_id,
                                        fingerprint,
                                    )
                                {
                                    preclaimed_live_reservation = Some(reservation);
                                } else {
                                    return json_error_from_cursor_message(error);
                                }
                            }
                            LiveResumeOutcome::TerminalError(_) => {}
                            LiveResumeOutcome::MissingTools(missing) => {
                                return json_error(
                                    StatusCode::BAD_REQUEST,
                                    "invalid_request_error",
                                    format!(
                                        "Missing tool_result blocks for pending tools: {}",
                                        missing.join(", ")
                                    ),
                                );
                            }
                            LiveResumeOutcome::SupersedeRunning(run_id) => {
                                let replacement = if has_tool_results {
                                    LiveRunRegistry::claim_replacement_for_run(
                                        session_id, agent_id, &run_id,
                                    )
                                } else {
                                    LiveRunRegistry::claim_replacement_for_fresh_request(
                                        session_id, agent_id, &run_id,
                                    )
                                };
                                match replacement {
                                    LiveReplacementClaim::Conflict => {
                                        let error =
                                            live_replacement_conflict_error(has_tool_results);
                                        return map_cursor_error_to_response(&error);
                                    }
                                    LiveReplacementClaim::Reserved {
                                        mut reservation,
                                        superseded,
                                    } => {
                                        if let Some(handle) = superseded {
                                            // The replacement reservation starts
                                            // with an unpublished fingerprint.
                                            // Publish the old operation before
                                            // protecting it: if the request is
                                            // cancelled while awaiting the old
                                            // driver's teardown, Drop may seal
                                            // an ambiguous tombstone directly.
                                            reservation.set_operation_fingerprint(
                                                handle.request_fingerprint(),
                                            );
                                            reservation.protect_on_drop();
                                            let cancel_result = handle.cancel_and_wait().await;
                                            match finish_replacement_after_cancel(
                                                reservation,
                                                handle,
                                                has_tool_results,
                                                cancel_result,
                                            ) {
                                                Ok(kept) => {
                                                    preclaimed_live_reservation = Some(kept);
                                                }
                                                Err(error) => {
                                                    return map_cursor_error_to_response(&error);
                                                }
                                            }
                                        } else {
                                            preclaimed_live_reservation = Some(reservation);
                                        }
                                    }
                                }
                            }
                            LiveResumeOutcome::Conflict => {
                                return json_error(
                                    StatusCode::CONFLICT,
                                    "invalid_request_error",
                                    "A Cursor live run is already active for this session",
                                );
                            }
                            LiveResumeOutcome::ResumeError(error) => {
                                return map_cursor_error_to_response(&error);
                            }
                            LiveResumeOutcome::Free(reservation) => {
                                if reject_orphaned_native_results_when_live_slot_is_free(&body) {
                                    return json_error(
                                        StatusCode::CONFLICT,
                                        "invalid_request_error",
                                        "Stale Cursor tool_result cannot start a new live run",
                                    );
                                }
                                if let Some(reservation) = reservation {
                                    preclaimed_live_reservation = Some(reservation);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Claude Code agent mode: after tool_use pause, the next request carries
        // tool_result in `messages` and expects a *new* model turn (full history),
        // not an empty resume of leftover Cursor frames. Clear bridge pending and
        // fall through to run_agent with the complete Anthropic conversation.
        if !xai_compact
            && let Some(bridge_key) = bridge_registry_key_for_account(&ctx, Some(&account_key))
            && let Some(pending) = BridgeRegistry::pending_tool(&bridge_key)
            && find_tool_result(&body, pending.tool_use_id()).is_some()
        {
            BridgeRegistry::remove(&bridge_key);
        }

        // Hosted search/fetch run after Cursor auth so a logged-out proxy is
        // not an open SSRF/search endpoint. Incoming Anthropic tokens are still
        // unused; the gate is the stored Cursor login on this host.
        if is_hosted_web_search_request(&body) {
            let query = extract_web_search_query(&body).unwrap_or_default();
            if query.trim().is_empty() {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    "web_search requires a non-empty query",
                );
            }
            let (hits, error) = match search_web(&query).await {
                Ok(hits) => (hits, None),
                Err(err) => (Vec::new(), Some(err)),
            };
            if want_stream {
                return hosted_web_search_sse_response(message_id, wire_model, query, hits, error);
            }
            return hosted_web_search_json_response(message_id, wire_model, query, hits, error);
        }
        if let Some(resp) = maybe_handle_hosted_web_fetch(&body, &message_id, &wire_model).await {
            return resp;
        }

        // The registry work above may wait for a previous generation to
        // finish. Re-check before constructing any fresh live/buffered Run so
        // a policy 429 observed during that handoff is still surfaced as a
        // real HTTP 429. A resumed/attached stream is already accepted work
        // and deliberately bypasses this new-dispatch guard.
        if resumed_live_events.is_none()
            && !policy_preflight_can_attach_existing_run_for_account(
                &body,
                &ctx,
                Some(&account_key),
            )
            && let Err(error) = policy_rate_limit_preflight(model, &client_type, &auth.access_token)
        {
            if let Some(reservation) = preclaimed_live_reservation.take() {
                reservation.release();
            }
            return map_cursor_error_to_response(&error);
        }

        // Compaction uses the original session with its synthetic agent lane;
        // this keeps the BiDi heartbeat/reconnect path while preventing it from
        // sharing the ordinary turn's live slot or conversation checkpoint.
        let session_id = ctx.session_id.as_deref();
        if xai_compact {
            create_logger("cursor").info(
                "compaction_isolated",
                Some(serde_json::Map::from_iter([
                    ("reqId".into(), serde_json::json!(&ctx.req_id)),
                    (
                        "clientRequestId".into(),
                        serde_json::json!(ctx.client_request_id.as_deref()),
                    ),
                    (
                        "sessionId".into(),
                        serde_json::json!(ctx.session_id.as_deref()),
                    ),
                    (
                        "syntheticAgentId".into(),
                        serde_json::json!(ctx.claude_code.agent_id.as_deref()),
                    ),
                ])),
            );
        }
        // A compaction turn is a summary-only operation. Do not expose native
        // tool bridge state or MCP catalogs to it: a tool call here would make
        // the Responses collector wait for output text and report an empty
        // summary, then retry the same compaction forever.
        let bridge_eligible = !xai_compact && can_bridge_cursor_native_tools(&body, session_id);
        let request_allowed_tools = if xai_compact {
            Some(BTreeSet::new())
        } else {
            advertised_tool_names(&body)
        };
        let request_mcp_tools = if xai_compact {
            None
        } else {
            claude_local_mcp_tools(&body)
        };
        let mut bridge_key = bridge_registry_key_for_account(&ctx, Some(&account_key));
        let mut continuation_key = session_id.filter(|s| !s.is_empty()).map(|sid| {
            live_run_key_for(live_run_identity_with_account(
                sid,
                &ctx,
                Some(&account_key),
            ))
        });
        let continuation =
            continuation_for_request_for_account(session_id, &ctx, Some(&account_key));
        let tool_result_only_turn = latest_user_is_only_tool_results(&body);
        let client_only_continuation =
            request_has_client_only_tool_results(&body) || tool_result_only_turn;
        let parts = render_cursor_prompt_parts_with(
            &body,
            CursorPromptOptions {
                // Native BiDi tools don't need Anthropic schemas in user text;
                // Claude-local tools (Workflow/Skill/mcp__) are still forwarded.
                omit_tools: xai_compact || bridge_eligible || continuation.has_checkpoint,
                // ClientOnly (Workflow/Skill) results arrive after BiDi teardown.
                // delta_only would skip tool_result-only messages and replay the
                // original user text against a stale/zombie MCP checkpoint.
                delta_only: !xai_compact
                    && continuation.has_checkpoint
                    && !client_only_continuation,
            },
        );
        // Normal checkpoint-backed starts send only images from the current
        // user turn. A tool-result-only request additionally retains historical
        // image bytes for the specific recovery that clears the conversation
        // and replays full Anthropic history; confirmed-checkpoint nudges and
        // ordinary transport retries must not re-submit old screenshots.
        let (images, reset_retry_images) =
            live_request_image_sets(&body, continuation.has_checkpoint);
        if !images.is_empty() {
            create_logger("cursor").info(
                "selected_images",
                Some(serde_json::Map::from_iter([
                    ("count".into(), serde_json::json!(images.len())),
                    (
                        "hasCheckpoint".into(),
                        serde_json::json!(continuation.has_checkpoint),
                    ),
                ])),
            );
        }
        // A checkpoint-backed request normally uses a compact delta prompt.
        // If the remote conversation must be rotated (for example after a KV
        // blob-store 413), replay the complete Anthropic history instead of
        // sending that delta to a brand-new Cursor conversation. Native tool
        // schemas remain omitted when the live bridge supplies them directly.
        let reset_user_text = if continuation.has_checkpoint {
            render_cursor_prompt_parts_with(
                &body,
                CursorPromptOptions {
                    omit_tools: xai_compact || bridge_eligible,
                    delta_only: false,
                },
            )
            .user_text
        } else {
            parts.user_text.clone()
        };
        let custom_system = parts.custom_system_prompt.as_deref();
        let user_text = parts.user_text.as_str();

        let client = shared_cursor_http_client(continuation_key.as_deref());
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.upstream_started(&ctx.req_id);
        }
        let mut token = auth.access_token.clone();
        // Keep the selected profile id (when present) as the stable state
        // partition. It survives bearer refreshes and is updated below only
        // when bounded account failover selects a replacement credential.
        let account_failover_state = Arc::new(Mutex::new(AccountFailoverState::new(&token)));

        if let Some(events) = resumed_live_events.take() {
            let sid = session_id.expect("a resumed live run requires a session id");
            let identity = live_run_identity_with_account(sid, &ctx, Some(&account_key));
            let estimated_input = estimate_rendered_prompt_tokens(&parts);
            let monitor = ctx
                .monitor
                .clone()
                .map(|handle| (handle, ctx.req_id.clone()));
            let retry_start = LiveRetryStart {
                client: client.clone(),
                effective_token: Arc::new(Mutex::new(token.clone())),
                user_text: user_text.to_string(),
                reset_user_text: reset_user_text.clone(),
                expected_conversation_id: Arc::new(Mutex::new(
                    continuation.conversation_id.clone(),
                )),
                model: model.to_string(),
                images,
                reset_retry_images,
                image_recovery_attempted: Arc::new(AtomicBool::new(false)),
                image_recovery_images: Arc::new(Mutex::new(None)),
                kv_recovery_attempted: Arc::new(AtomicBool::new(false)),
                compaction_recovery_attempted: Arc::new(AtomicBool::new(false)),
                upstream_active_recovery_attempted: Arc::new(AtomicBool::new(false)),
                account_failover_state: Arc::clone(&account_failover_state),
                custom_system: custom_system.map(str::to_string),
                session_id: sid.to_string(),
                agent_id: identity.agent_id.map(str::to_string),
                parent_agent_id: identity.parent_agent_id.map(str::to_string),
                account_key: Arc::new(Mutex::new(account_key.clone())),
                allowed: request_allowed_tools.clone(),
                mcp_tools: request_mcp_tools.clone(),
                request_context: cursor_request_context(&body),
                fingerprint: live_operation_fingerprint_payload(
                    &body,
                    ctx.client_request_id.as_deref(),
                ),
                has_refresh: auth.refresh_token.is_some(),
                client_type: client_type.clone(),
                request_sequence_id: uuid::Uuid::new_v4().to_string(),
                unbounded_conflict_wait: false,
                compaction_mode: xai_compact,
            };
            let events = spawn_live_events_with_late_retries(retry_start, None, Some(events));
            return live_downstream_response(
                want_stream,
                sid,
                events,
                message_id,
                wire_model,
                estimated_input,
                monitor,
                xai_compact,
            )
            .await;
        }

        // Prefer long-lived BiDi/RunSSE whenever we have a session. Claude Code's
        // non-streaming fallback (`stream=false`) still uses live; we collect the
        // same events into one JSON body instead of SSE.
        let has_session = session_id.is_some_and(|s| !s.is_empty());
        let bidi_enabled = client.live_bidi_enabled_for_client_type(&client_type);
        let live_eligible = live_path_eligible(want_stream, has_session, bidi_enabled);
        if !live_eligible {
            let mut fields = serde_json::Map::new();
            fields.insert("stream".into(), serde_json::json!(want_stream));
            fields.insert("hasSession".into(), serde_json::json!(has_session));
            fields.insert("bidiEnabled".into(), serde_json::json!(bidi_enabled));
            fields.insert(
                "reason".into(),
                serde_json::json!(live_path_skip_reason(
                    want_stream,
                    has_session,
                    bidi_enabled
                )),
            );
            create_logger("cursor").info("live_skipped", Some(fields));
        }
        if live_eligible {
            let sid = session_id.expect("live eligibility requires session id");
            let identity = live_run_identity_with_account(sid, &ctx, Some(&account_key));
            log_live_start_claude_headers(&ctx, sid, model, &client_type, xai_compact);
            let allowed = request_allowed_tools.clone();
            let mcp_tools = request_mcp_tools.clone();
            let estimated_input = estimate_rendered_prompt_tokens(&parts);
            let monitor = ctx
                .monitor
                .clone()
                .map(|handle| (handle, ctx.req_id.clone()));

            // Concurrent same-session POSTs race on Starting→Running. A request
            // may supersede only the exact generation it observed above, where
            // replacement was atomically preclaimed. Never blindly replace a
            // Running/Starting slot discovered here.
            let fingerprint =
                live_operation_fingerprint_payload(&body, ctx.client_request_id.as_deref());
            let request_context = cursor_request_context(&body);
            let has_refresh = auth.refresh_token.is_some();
            let initial_reservation = preclaimed_live_reservation.take();
            if commit_streaming_live_sse_before_start_live(
                want_stream,
                ctx.hold_http_until_live_open,
                defer_fresh_stream || skip_resume_probe,
            ) {
                return spawn_streaming_live_sse(
                    client.clone(),
                    token,
                    user_text.to_string(),
                    reset_user_text.clone(),
                    continuation.conversation_id.clone(),
                    model.to_string(),
                    images,
                    reset_retry_images,
                    custom_system.map(str::to_string),
                    sid.to_string(),
                    account_key.clone(),
                    identity.agent_id.map(str::to_string),
                    identity.parent_agent_id.map(str::to_string),
                    allowed,
                    mcp_tools,
                    request_context,
                    fingerprint,
                    initial_reservation,
                    has_refresh,
                    defer_fresh_stream || skip_resume_probe,
                    xai_compact,
                    client_type.clone(),
                    message_id,
                    wire_model,
                    estimated_input,
                    monitor,
                );
            }
            let retry_start = LiveRetryStart {
                client: client.clone(),
                effective_token: Arc::new(Mutex::new(token.clone())),
                user_text: user_text.to_string(),
                reset_user_text,
                expected_conversation_id: Arc::new(Mutex::new(
                    continuation.conversation_id.clone(),
                )),
                model: model.to_string(),
                images,
                reset_retry_images,
                image_recovery_attempted: Arc::new(AtomicBool::new(false)),
                image_recovery_images: Arc::new(Mutex::new(None)),
                kv_recovery_attempted: Arc::new(AtomicBool::new(false)),
                compaction_recovery_attempted: Arc::new(AtomicBool::new(false)),
                upstream_active_recovery_attempted: Arc::new(AtomicBool::new(false)),
                account_failover_state: Arc::clone(&account_failover_state),
                custom_system: custom_system.map(str::to_string),
                session_id: sid.to_string(),
                agent_id: identity.agent_id.map(str::to_string),
                parent_agent_id: identity.parent_agent_id.map(str::to_string),
                account_key: Arc::new(Mutex::new(account_key.clone())),
                allowed,
                mcp_tools,
                request_context,
                fingerprint,
                has_refresh,
                client_type: client_type.clone(),
                request_sequence_id: uuid::Uuid::new_v4().to_string(),
                unbounded_conflict_wait: false,
                compaction_mode: xai_compact,
            };
            match retry_start.start(initial_reservation).await {
                Ok(events) => {
                    let events =
                        spawn_live_events_with_late_retries(retry_start, None, Some(events));
                    return live_downstream_response(
                        want_stream,
                        sid,
                        events,
                        message_id,
                        wire_model,
                        estimated_input,
                        monitor,
                        xai_compact,
                    )
                    .await;
                }
                Err(error) => return map_cursor_error_to_response(&error),
            }
        }

        let mut transport_retries = 0_u32;
        let mut refreshed_once = false;
        let mut image_recovery_attempted = false;
        let mut kv_recovery_attempted = false;
        // A buffered Run can be accepted by Cursor while returning only an
        // idle/no-progress diagnostic.  Retrying the same continuation then
        // races that still-live Run and produces a 409 wave.  Rotate the
        // conversation once, with the full Anthropic history, before exposing
        // the ambiguity to the client.  The fence is intentionally local to
        // this logical request so repeated 502s cannot create a restart loop.
        let mut idle_recovery_attempted = false;
        // Keep the current continuation delta untouched for ordinary
        // transport retries.  A stale selected-image id is different: after
        // one bounded conversation reset, replay the original history with
        // fresh UUID metadata so Cursor receives the inline bytes again.
        let mut request_images = images.clone();
        // Keep the recovery image slice aligned with the current request
        // images.  When the binding changes after a first stale-image/KV
        // recovery, the client-side continuation guard must not substitute
        // the original (now stale) UUIDs back into the retried request.
        let mut binding_reset_images = reset_retry_images.clone();
        let mut request_prompt = user_text;
        let upstream = loop {
            // The buffered fallback can also wait behind auth/session work;
            // honor a breaker opened in that interval before dispatching, and
            // single-flight a cold account/model key just like the live path.
            let probe_admission =
                match policy_rate_limit_admit_fresh_open(model, &client_type, &token).await {
                    Ok(admission) => admission,
                    Err(error) => return map_cursor_error_to_response(&error),
                };
            match client
                .run_agent_with_session_profile(
                    &token,
                    request_prompt,
                    model,
                    &request_images,
                    custom_system,
                    CursorRunOptions {
                        session_id: continuation_key.as_deref(),
                        client_type: Some(&client_type),
                        expected_conversation_id: continuation.conversation_id.as_deref(),
                        reset_user_text: Some(&reset_user_text),
                        reset_images: Some(&binding_reset_images),
                    },
                )
                .await
            {
                Ok(r) => {
                    probe_admission.mark_healthy();
                    break r;
                }
                Err(e) if e.status == 401 && !refreshed_once && auth.refresh_token.is_some() => {
                    drop(probe_admission);
                    match force_refresh_cursor_auth_async(token.clone()).await {
                        Ok(Some(refreshed)) => {
                            token = refreshed.access_token;
                            refreshed_once = true;
                            continue;
                        }
                        _ => return map_cursor_error_to_response(&e),
                    }
                }
                Err(e) if cursor_error_is_missing_image(&e) && !image_recovery_attempted => {
                    drop(probe_admission);
                    image_recovery_attempted = true;
                    if let Some(key) = continuation_key.as_deref() {
                        conversation::reset(key);
                    }
                    request_images = refresh_image_uuids(&reset_retry_images);
                    binding_reset_images = request_images.clone();
                    request_prompt = &reset_user_text;
                    create_logger("cursor").warn(
                        "image_buffered_recovery",
                        Some(serde_json::Map::from_iter([
                            (
                                "sessionId".into(),
                                serde_json::json!(session_id.unwrap_or_default()),
                            ),
                            ("imageCount".into(), serde_json::json!(request_images.len())),
                            ("recovery".into(), serde_json::json!("fresh_conversation")),
                        ])),
                    );
                    continue;
                }
                Err(e)
                    if cursor_error_is_kv_blob_overflow(&e)
                        && live_error_is_kv_blob_overflow_replayable(&e.client_message()) =>
                {
                    if !kv_recovery_attempted {
                        drop(probe_admission);
                        kv_recovery_attempted = true;
                        if let Some(key) = continuation_key.as_deref() {
                            conversation::reset(key);
                        }
                        request_prompt = &reset_user_text;
                        // A KV rotation is a fresh Cursor conversation. Do
                        // not carry selected-image UUIDs from the poisoned
                        // conversation into it; preserve bytes/MIME while
                        // issuing CLI-style fresh asset identities. If the
                        // same request already performed stale-image
                        // recovery, retain that refreshed set rather than
                        // generating a second UUID wave.
                        request_images = kv_recovery_images(
                            &request_images,
                            &reset_retry_images,
                            image_recovery_attempted,
                        );
                        binding_reset_images = request_images.clone();
                        create_logger("cursor").warn(
                            "kv_blob_buffered_recovery",
                            Some(serde_json::Map::from_iter([
                                (
                                    "sessionId".into(),
                                    serde_json::json!(session_id.unwrap_or_default()),
                                ),
                                ("recovery".into(), serde_json::json!("fresh_conversation")),
                                ("replay".into(), serde_json::json!("full_history")),
                            ])),
                        );
                        continue;
                    }
                    drop(probe_admission);
                    return map_cursor_error_to_response(&e);
                }
                Err(e)
                    if !idle_recovery_attempted
                        && continuation_key.is_some()
                        && crate::retry::is_idle_no_progress(&e.client_message())
                        && live_error_is_empty_turn_retry(&e.client_message()) =>
                {
                    drop(probe_admission);
                    idle_recovery_attempted = true;
                    if let Some(key) = continuation_key.as_deref() {
                        conversation::reset(key);
                    }
                    request_prompt = &reset_user_text;
                    // A fresh conversation cannot consume checkpoint-delta
                    // image ids.  Reuse the same full-history image set (and
                    // any UUID wave already minted for image recovery) so a
                    // transport stall does not create a second asset upload.
                    request_images = kv_recovery_images(
                        &request_images,
                        &reset_retry_images,
                        image_recovery_attempted,
                    );
                    binding_reset_images = request_images.clone();
                    create_logger("cursor").warn(
                        "idle_buffered_recovery",
                        Some(serde_json::Map::from_iter([
                            (
                                "sessionId".into(),
                                serde_json::json!(session_id.unwrap_or_default()),
                            ),
                            ("recovery".into(), serde_json::json!("fresh_conversation")),
                            ("diagnostic".into(), serde_json::json!(e.client_message())),
                        ])),
                    );
                    continue;
                }
                // A second missing-image response means the bounded
                // re-upload did not repair the upstream asset lookup. Surface
                // it directly instead of feeding it into the generic
                // transport retry budget.
                Err(e) if cursor_error_is_missing_image(&e) => {
                    drop(probe_admission);
                    return map_cursor_error_to_response(&e);
                }
                Err(e)
                    if transport_retries < cursor_transient_retry_limit(&e.client_message())
                        && cursor_start_error_is_same_request_retryable(&e) =>
                {
                    drop(probe_admission);
                    crate::retry::sleep(same_request_retry_wait_ms(
                        transport_retries,
                        &e.client_message(),
                    ))
                    .await;
                    transport_retries += 1;
                }
                Err(e) => {
                    let policy_limited = cursor_error_is_policy_limited(&e);
                    if policy_limited {
                        probe_admission.mark_policy_limited(
                            model,
                            &client_type,
                            &token,
                            &e.client_message(),
                            e.retry_after.as_deref(),
                        );
                        if (is_account_failover_policy_error(&e.client_message())
                            || is_account_failover_policy_error(&e.message))
                            && let Some(replacement) = account_failover_replacement_token_async(
                                token.clone(),
                                model.to_string(),
                                client_type.clone(),
                                Arc::clone(&account_failover_state),
                            )
                            .await
                        {
                            token = replacement;
                            account_key = cursor_account_key_for_token_async(token.clone()).await;
                            bridge_key = bridge_registry_key_for_account(&ctx, Some(&account_key));
                            continuation_key = session_id.map(|sid| {
                                live_run_key_for(live_run_identity_with_account(
                                    sid,
                                    &ctx,
                                    Some(&account_key),
                                ))
                            });
                            // The refresh helper targets the active account;
                            // this request now uses an inactive candidate.
                            // Suppress a later 401 refresh that would switch
                            // back to the exhausted active bearer.
                            refreshed_once = true;
                            if let Some(key) = continuation_key.as_deref() {
                                conversation::reset(key);
                            }
                            image_recovery_attempted = false;
                            request_images = refresh_image_uuids(&reset_retry_images);
                            binding_reset_images = request_images.clone();
                            request_prompt = &reset_user_text;
                            transport_retries = 0;
                            continue;
                        }
                    } else {
                        drop(probe_admission);
                    }
                    let mut error = exhausted_live_start_error(e, transport_retries);
                    if policy_limited && error.retry_after.is_none() {
                        error.retry_after =
                            policy_rate_limit_breaker_state(model, &client_type, &token)
                                .map(|state| state.retry_after_secs.to_string());
                    }
                    return map_cursor_error_to_response(&error);
                }
            }
        };

        if want_stream {
            if bridge_eligible {
                // The buffered bridge must apply the same downstream tool
                // allow-list while decoding that the live/SSE paths use.
                // Without this, a multi-replacement PiEdit is first mapped to
                // the legacy `MultiEdit` shape; the bridge then cannot resolve
                // it to Claude Code 2.1+'s single-operation text editor and
                // silently drops the edit.
                let allowed = advertised_tool_names(&body);
                let events =
                    match decode_upstream_response_with_allowed(&upstream.body, allowed.as_ref()) {
                        Ok(e) => e,
                        Err(e) => return map_cursor_decode_error_to_response(&e),
                    };

                // Anthropic surface must echo the wire id (`claude-fable-5[1m]`),
                // not the suffix-stripped request model — Claude Code / ccstatusline
                // derive the 1M window from `[1m]` when the proxy host is not
                // api.anthropic.com (gB/pL first-party path is off).
                let (sse_bytes, _paused) = start_cursor_tool_bridge(
                    &message_id,
                    &wire_model,
                    bridge_key.as_deref().unwrap_or_else(|| {
                        session_id.expect("bridge eligibility requires a session id")
                    }),
                    &events,
                    allowed,
                    Box::new(|| uuid::Uuid::new_v4().to_string().replace('-', "")),
                );
                if let Some(monitor) = ctx.monitor.as_ref() {
                    let (input_tokens, output_tokens) = usage_from_anthropic_sse(&sse_bytes);
                    remember_input_tokens(session_id, input_tokens);
                    monitor.stream_progress(
                        &ctx.req_id,
                        sse_bytes.len() as u64,
                        count_sse_events(&sse_bytes),
                        input_tokens,
                        output_tokens,
                    );
                } else {
                    let (input_tokens, _) = usage_from_anthropic_sse(&sse_bytes);
                    remember_input_tokens(session_id, input_tokens);
                }

                let headers = [
                    (http::header::CONTENT_TYPE, "text/event-stream"),
                    (http::header::CACHE_CONTROL, "no-cache"),
                    (http::header::CONNECTION, "keep-alive"),
                ];
                (headers, sse_bytes).into_response()
            } else {
                // Legacy/non-bridge responses must honor Claude Code's
                // advertised tool set too. Native Cursor exec events are
                // ignored when the request advertises no tools, preventing
                // synthetic tool_use blocks from leaking into plain chats.
                let allowed = request_allowed_tools.clone().unwrap_or_default();
                let sse_bytes = if xai_compact {
                    sse::frame_cursor_stream_compaction(&upstream, &message_id, &wire_model)
                } else {
                    sse::frame_cursor_stream_with_allowed(
                        &upstream,
                        &message_id,
                        &wire_model,
                        Some(&allowed),
                    )
                };
                if let Some(monitor) = ctx.monitor.as_ref() {
                    let (input_tokens, output_tokens) = usage_from_anthropic_sse(&sse_bytes);
                    remember_input_tokens(session_id, input_tokens);
                    monitor.stream_progress(
                        &ctx.req_id,
                        sse_bytes.len() as u64,
                        count_sse_events(&sse_bytes),
                        input_tokens,
                        output_tokens,
                    );
                } else {
                    let (input_tokens, _) = usage_from_anthropic_sse(&sse_bytes);
                    remember_input_tokens(session_id, input_tokens);
                }
                let headers = [
                    (http::header::CONTENT_TYPE, "text/event-stream"),
                    (http::header::CACHE_CONTROL, "no-cache"),
                    (http::header::CONNECTION, "keep-alive"),
                ];
                (headers, sse_bytes).into_response()
            }
        } else {
            let allowed = request_allowed_tools.clone().unwrap_or_default();
            let decoded = if xai_compact {
                decode_cursor_upstream_compaction(&upstream, &message_id, &wire_model)
            } else {
                decode_cursor_upstream_with_allowed(
                    &upstream,
                    &message_id,
                    &wire_model,
                    Some(&allowed),
                )
            };
            match decoded {
                Ok(json) => {
                    let input_tokens = json.pointer("/usage/input_tokens").and_then(|v| v.as_u64());
                    remember_input_tokens(session_id, input_tokens);
                    if let Some(monitor) = ctx.monitor.as_ref() {
                        monitor.usage_updated(
                            &ctx.req_id,
                            input_tokens,
                            json.pointer("/usage/output_tokens")
                                .and_then(|v| v.as_u64()),
                        );
                    }
                    (StatusCode::OK, Json(json)).into_response()
                }
                Err(e) => map_cursor_decode_error_to_response(&e),
            }
        }
    }

    async fn handle_count_tokens(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let tokens = count_tokens_for_request(ctx.session_id.as_deref(), &body);
        if let Some(monitor) = ctx.monitor.as_ref() {
            if let Some(model) = body.model.as_deref() {
                monitor.client_type_resolved(
                    &ctx.req_id,
                    crate::config::cursor_client_type_for_model(model),
                );
            }
            monitor.usage_updated(&ctx.req_id, Some(tokens), None);
        }
        (
            StatusCode::OK,
            Json(CountTokensResponse {
                input_tokens: tokens,
            }),
        )
            .into_response()
    }
}

fn count_sse_events(bytes: &[u8]) -> u64 {
    String::from_utf8_lossy(bytes).matches("event:").count() as u64
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn json_error_from_cursor_message(message: impl Into<String>) -> Response {
    let message = message.into();
    let status = crate::retry::classify_proxy_error_status(502, &message);
    let kind = crate::retry::anthropic_error_kind_for_status(status, &message);
    json_error(
        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
        kind,
        message,
    )
}

/// Whether an open failure is evidence that the upstream launch path is
/// congested. Keep this predicate identical to the same-request retry gate so
/// Sand's cold-open scheduler never backs off the process-wide launch window
/// for terminal account policy/quota responses that are being handed to
/// account failover.
fn sand_open_failure_is_retryable(error: &client::CursorError) -> bool {
    let diagnostic = error.client_message();
    // The retry classifier recognizes the normal flattened policy forms, but
    // the provider adapter can serialize `isRetryable=false` and its quota
    // reason in an order that is not recognized by the compact resource
    // marker parser. Keep the dedicated provider diagnostic predicate as a
    // second terminal-policy guard.
    if is_non_retryable_provider_error_message(&diagnostic) {
        return false;
    }
    crate::retry::should_retry_upstream(error.status, &diagnostic)
}

fn user_facing_cursor_detail(err: &client::CursorError) -> &str {
    let Some(detail) = err.detail.as_deref().filter(|s| !s.is_empty()) else {
        return err.message.as_str();
    };
    let lower = detail.to_ascii_lowercase();
    if lower.contains("error sending request for url")
        || (lower.starts_with("error sending request") && !detail.contains('\n'))
    {
        return err.message.as_str();
    }
    detail
}

fn map_cursor_error_to_response(err: &client::CursorError) -> Response {
    let detail = user_facing_cursor_detail(err);
    let classified = crate::retry::classify_proxy_error_status(err.status, &err.client_message());
    match classified {
        400 => json_error(StatusCode::BAD_REQUEST, "invalid_request_error", detail),
        401 => json_error(StatusCode::UNAUTHORIZED, "authentication_error", detail),
        // permission_denied / OUTDATED_CLIENT are NOT login failures — do not force re-login.
        403 if is_outdated_client_error(detail) => json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!(
                "{detail}. Cursor rejected this client fingerprint (not an expired login). \
Upgrade ~/.local/share/cursor-agent, or set CCP_CURSOR_CLIENT_VERSION to your installed \
cli-* version (e.g. cli-2026.07.16-899851b)."
            ),
        ),
        403 => json_error(
            StatusCode::FORBIDDEN,
            "permission_error",
            format!(
                "{detail}. This is a Cursor permission/policy error, not a missing login. \
Re-running `cursor auth login` usually will not help."
            ),
        ),
        404 => json_error(StatusCode::NOT_FOUND, "not_found_error", detail),
        409 => json_error(StatusCode::CONFLICT, "invalid_request_error", detail),
        429 => {
            let retry_after = err.retry_after.as_deref().unwrap_or("5");
            let resp = json_error(StatusCode::TOO_MANY_REQUESTS, "rate_limit_error", detail);
            let headers = [(http::header::RETRY_AFTER, retry_after)];
            (headers, resp).into_response()
        }
        503 | 504 => {
            let retry_after = err.retry_after.as_deref().unwrap_or("1");
            let resp = json_error(StatusCode::SERVICE_UNAVAILABLE, "api_error", detail);
            let headers = [(http::header::RETRY_AFTER, retry_after)];
            (headers, resp).into_response()
        }
        other if (400..500).contains(&other) => json_error(
            StatusCode::from_u16(other).unwrap_or(StatusCode::BAD_REQUEST),
            crate::retry::anthropic_error_kind_for_status(other, detail),
            detail,
        ),
        _ => json_error(StatusCode::BAD_GATEWAY, "api_error", detail),
    }
}

fn is_outdated_client_error(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("outdated_client")
        || lower.contains("outdated client")
        || lower.contains("update required")
        || lower.contains("error_outdated_client")
}

fn map_cursor_decode_error_to_response(err: &CursorDecodeError) -> Response {
    let msg = err.to_string();
    // Connect END errors already carry a provider-derived status, but the
    // diagnostic can still be wrapped in another status (for example an
    // outer 429/resource_exhausted containing a temporary provider 400). Use
    // the same classifier as the live/HTTP error path so buffered and
    // streaming responses never turn a typed 503/429/409 into a generic 502.
    let classified = crate::retry::classify_proxy_error_status(err.status().unwrap_or(502), &msg);
    match classified {
        400 => json_error(StatusCode::BAD_REQUEST, "invalid_request_error", msg),
        401 => json_error(StatusCode::UNAUTHORIZED, "authentication_error", msg),
        403 if is_outdated_client_error(&msg) => json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!(
                "{msg}. Cursor rejected this client fingerprint (not an expired login). \
Upgrade cursor-agent or set CCP_CURSOR_CLIENT_VERSION."
            ),
        ),
        403 => json_error(
            StatusCode::FORBIDDEN,
            "permission_error",
            format!("{msg}. Permission/policy error — re-login usually will not help."),
        ),
        404 => json_error(StatusCode::NOT_FOUND, "not_found_error", msg),
        409 => json_error(StatusCode::CONFLICT, "invalid_request_error", msg),
        429 => {
            let response = json_error(StatusCode::TOO_MANY_REQUESTS, "rate_limit_error", msg);
            let headers = [(http::header::RETRY_AFTER, "5")];
            (headers, response).into_response()
        }
        503 | 504 => {
            // 504 is a transport timeout rather than a stable client error.
            // Normalize it to 503 with a short Retry-After so Claude Code and
            // Grok clients use their transient retry path consistently.
            let response = json_error(StatusCode::SERVICE_UNAVAILABLE, "api_error", msg);
            let headers = [(http::header::RETRY_AFTER, "1")];
            (headers, response).into_response()
        }
        other if (400..500).contains(&other) => json_error(
            StatusCode::from_u16(other).unwrap_or(StatusCode::BAD_REQUEST),
            crate::retry::anthropic_error_kind_for_status(other, &msg),
            msg,
        ),
        _ => json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            format!("Response decoding error: {err}"),
        ),
    }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

pub(crate) struct CursorCli;

impl CliHandlers for CursorCli {
    fn login(&self) -> Result<(), anyhow::Error> {
        // Capture the account being replaced so a hot swap is explicit.
        let previous_email = load_cursor_auth()
            .ok()
            .flatten()
            .and_then(|auth| auth.email);
        let auth = run_cursor_login()?.ok_or_else(|| anyhow::anyhow!("Cursor login timed out"))?;
        println!("Cursor auth saved in {}", auth.source);
        if let Some(ref user_id) = auth.user_id {
            println!("User: {user_id}");
        }
        if let Some(ref email) = auth.email {
            println!("Email: {email}");
        }
        match (previous_email.as_deref(), auth.email.as_deref()) {
            (Some(old), Some(new)) if old != new => {
                println!();
                println!("Account switched: {old} -> {new}");
                println!(
                    "A running `serve` picks this up immediately: new requests use the new \
                     account, while in-flight runs finish on the previous login. No restart \
                     needed. Existing sessions start a fresh Cursor conversation on their \
                     next turn (client-side history is replayed automatically)."
                );
            }
            _ => {
                println!();
                println!(
                    "A running `serve` picks this login up immediately for new requests; \
                     no restart needed."
                );
            }
        }
        if crate::providers::cursor::auth::env_cursor_token_present() {
            println!();
            println!(
                "WARNING: CCP_CURSOR_AUTH_TOKEN/CURSOR_AUTH_TOKEN is set in this shell. Any \
                 process started with that env (including `serve`) keeps using the env token \
                 and will NOT see this login. Unset the env var to use the stored account."
            );
        }
        Ok(())
    }

    fn device(&self) -> Result<(), anyhow::Error> {
        anyhow::bail!("cursor: device login not yet implemented");
    }

    fn status(&self) -> Result<(), anyhow::Error> {
        match load_cursor_auth()? {
            Some(auth) => {
                println!("Auth source: {}", auth.source);
                if let Some(ref user_id) = auth.user_id {
                    println!("User: {user_id}");
                }
                if let Some(ref email) = auth.email {
                    println!("Email: {email}");
                }
                if let Some(expires) = auth.expires {
                    let remaining = expires.saturating_sub(now_ms()) / 1000;
                    println!("Access token expires in: {remaining}s");
                } else {
                    println!("Access token expiry: unknown");
                }
                Ok(())
            }
            None => {
                anyhow::bail!("Not authenticated");
            }
        }
    }

    fn logout(&self) -> Result<(), anyhow::Error> {
        clear_cursor_auth()?;
        println!(
            "Cursor persistent auth cleared. Unset CCP_CURSOR_AUTH_TOKEN or CURSOR_AUTH_TOKEN if using env auth."
        );
        println!(
            "A running `serve` keeps finishing in-flight runs with the old credentials; new \
             requests will fail with 401 until the next `cursor auth login`."
        );
        Ok(())
    }
}

pub(crate) static CURSOR_CLI: CursorCli = CursorCli;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_selection_timeout_is_bounded() {
        assert!(CURSOR_AUTH_LOAD_TIMEOUT <= Duration::from_secs(10));
        assert!(CURSOR_AUTH_LOAD_TIMEOUT >= Duration::from_secs(1));
    }
    use crate::providers::cursor::live::{
        live_error_allows_fresh_conversation, live_error_is_kv_blob_overflow,
    };

    static POLICY_RATE_LIMIT_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    /// Exercise the complete Fable Sand tool fallback without contacting a
    /// real Cursor account. The fixture deliberately returns the provider
    /// diagnostic inside a successful Connect response, which is where the
    /// production stream driver must classify a deterministic catalog
    /// rejection and replay the same full history without `tools`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sand_tool_catalog_fallback_bridges_xml_and_replays_tool_result_history() {
        use crate::providers::cursor::connect::{
            ConnectFrameDecoder, FLAG_END, encode_connect_frame,
        };
        use crate::providers::cursor::sand_inference::{
            SandInferenceClient, SandInferenceRequest, messages_from_anthropic,
            reset_sand_tool_capability, tools_from_anthropic,
        };
        use crate::providers::cursor::tool_bridge::bridge_cursor_events_to_sse_stateless;
        use axum::{Router, body::Body, extract::Request, routing::post};
        use futures_util::StreamExt;
        use serde_json::{Value, json};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let token = "sand-fixture-token-capability-regression";
        reset_sand_tool_capability(token, "claude-fable-5");
        {
            let _guard = POLICY_RATE_LIMIT_TEST_LOCK
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            reset_policy_rate_limit_breaker_for_test();
        }
        let requests: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let request_log = Arc::clone(&requests);
        let request_count = Arc::new(AtomicUsize::new(0));
        let count_for_handler = Arc::clone(&request_count);

        let app = Router::new().route(
            "/aiserver.v1.InferenceService/Stream",
            post(move |request: Request<Body>| {
                let request_log = Arc::clone(&request_log);
                let count_for_handler = Arc::clone(&count_for_handler);
                async move {
                    let body = axum::body::to_bytes(request.into_body(), 8 * 1024 * 1024)
                        .await
                        .expect("read Sand fixture request");
                    let mut decoder = ConnectFrameDecoder::new();
                    let frames = decoder.push(&body).expect("decode Sand request frame");
                    assert_eq!(frames.len(), 1, "each fixture request has one Connect frame");
                    let value: Value = serde_json::from_slice(&frames[0].payload)
                        .expect("decode Sand request JSON");
                    let attempt = count_for_handler.fetch_add(1, Ordering::SeqCst) + 1;
                    request_log.lock().expect("request log lock").push(value);

                    let payload = match attempt {
                        1 => json!({
                            "error": {
                                "code": "resource_exhausted",
                                "message": "tool catalog is not supported by this provider",
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
                        }),
                        2 => json!({
                            "textPart": {
                                "text": "<tool_use name=\"Read\">{\"file_path\":\"/tmp/fixture.txt\"}</tool_use>"
                            }
                        }),
                        _ => json!({"textPart": {"text": "continued after tool result"}}),
                    };
                    let payload = serde_json::to_vec(&payload).expect("encode fixture response");
                    let frame = encode_connect_frame(payload, 0);
                    let end = encode_connect_frame(Vec::new(), FLAG_END);
                    let response_body = [frame.as_ref(), end.as_ref()].concat();
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/connect+json")],
                        response_body,
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Sand fixture");
        let url = format!("http://{}", listener.local_addr().expect("fixture address"));
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve Sand fixture");
        });

        let anthropic: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [{"role": "user", "content": "read the fixture"}],
            "tools": [{
                "name": "Read",
                "description": "Read a file",
                "input_schema": {
                    "type": "object",
                    "properties": {"file_path": {"type": "string"}},
                    "required": ["file_path"]
                }
            }]
        }))
        .expect("Anthropic fixture request");
        let messages = messages_from_anthropic(&anthropic, false);
        let sand_tools = tools_from_anthropic(&anthropic, false);
        assert_eq!(sand_tools.len(), 1);
        let bridge_prompt =
            render_sand_text_tool_bridge_prompt(&anthropic).expect("tool bridge prompt");

        let initial_request = SandInferenceRequest::new(
            "claude-fable-5",
            "fixture-conversation",
            "fixture-invocation",
            messages.clone(),
        )
        .with_tools(sand_tools.clone());
        let mut fallback_messages = messages;
        prepend_sand_text_tool_bridge_prompt(&mut fallback_messages, &bridge_prompt);
        let fallback_request = SandInferenceRequest::new(
            "claude-fable-5",
            "fixture-fallback-conversation",
            "fixture-fallback-invocation",
            fallback_messages,
        );

        let client = SandInferenceClient::with_base_url_timeout(url, 5).expect("Sand client");
        let initial_stream = client
            .open(token, &initial_request)
            .await
            .expect("initial catalog probe should open");
        let (tx, mut rx) = mpsc::channel::<LiveEventResult>(32);
        let account_state = Arc::new(Mutex::new(AccountFailoverState::new(token)));
        drive_sand_stream_with_retries(
            client.clone(),
            token.to_string(),
            initial_request,
            initial_stream,
            Some(BTreeSet::from(["Read".to_string()])),
            false,
            5,
            tx,
            "claude-fable-5".to_string(),
            "sand".to_string(),
            account_state,
            sand_tools.len(),
            vec!["Read".to_string()],
            Some(fallback_request),
            Instant::now() + Duration::from_secs(60),
            SandAttemptBudget::new(),
            None,
            "fixture-request".to_string(),
            None,
        )
        .await;

        let mut events = Vec::new();
        while let Some(item) = rx.recv().await {
            match item.expect("fallback stream should succeed") {
                LiveRunEvent::Cursor(event) => events.push(event),
                LiveRunEvent::NativeToolBatch(_) => panic!("fixture should use XML bridge"),
            }
        }
        assert!(events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::TextDelta { text } if text.contains("<tool_use")
        )));

        let (sse, paused) = bridge_cursor_events_to_sse_stateless(
            "fixture-message",
            "claude-fable-5",
            "fixture-session",
            &events,
            Some(BTreeSet::from(["Read".to_string()])),
        );
        assert!(paused, "XML tool call must pause for Claude tool_result");
        let sse_text = String::from_utf8(sse).expect("bridge SSE is UTF-8");
        let tool_use = sse_text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find_map(|value| {
                (value["type"] == "content_block_start"
                    && value["content_block"]["type"] == "tool_use")
                    .then(|| value["content_block"].clone())
            })
            .expect("bridge must emit an Anthropic tool_use block");
        assert_eq!(tool_use["name"], "Read");
        let tool_use_id = tool_use["id"].as_str().expect("tool_use id").to_string();

        // Recreate the next Claude Code turn and verify Sand receives the
        // assistant tool call plus a role=TOOL toolContent message rather than
        // a flattened XML transcript.
        let continuation: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-fable-5",
            "messages": [
                {"role": "user", "content": "read the fixture"},
                {"role": "assistant", "content": [{
                    "type": "tool_use",
                    "id": tool_use_id,
                    "name": "Read",
                    "input": {"file_path": "/tmp/fixture.txt"}
                }]},
                {"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": "fixture contents"
                }]}
            ]
        }))
        .expect("continuation request");
        let continuation_messages = messages_from_anthropic(&continuation, false);
        assert_eq!(continuation_messages.len(), 3);
        assert_eq!(
            continuation_messages[1].tool_calls[0]["toolCallId"],
            tool_use_id
        );
        assert_eq!(continuation_messages[2].role, sand_inference::ROLE_TOOL);
        assert_eq!(
            continuation_messages[2].tool_content.as_ref().unwrap()["parts"][0]["result"],
            "fixture contents"
        );

        let continuation_request = SandInferenceRequest::new(
            "claude-fable-5",
            "fixture-continuation-conversation",
            "fixture-continuation-invocation",
            continuation_messages,
        );
        let mut stream = client
            .open(token, &continuation_request)
            .await
            .expect("full-history continuation should open");
        let mut continuation_events = Vec::new();
        while let Some(event) = stream.next().await {
            continuation_events.push(event.expect("continuation event"));
        }
        assert!(continuation_events.iter().any(|event| matches!(
            event,
            CursorStreamEvent::TextDelta { text } if text == "continued after tool result"
        )));

        let observed = requests.lock().expect("request log lock").clone();
        assert_eq!(
            observed.len(),
            3,
            "catalog, fallback, and continuation requests"
        );
        assert_eq!(observed[0]["tools"].as_array().unwrap().len(), 1);
        assert_eq!(observed[1]["tools"], json!([]));
        assert!(
            policy_rate_limit_preflight("claude-fable-5", "sand", token).is_ok(),
            "capability fallback must not publish an account 429 cooldown"
        );
        assert!(
            observed[1]["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["role"] == sand_inference::ROLE_SYSTEM
                    && message["text"].as_str().unwrap_or("").contains("tool_use"))
        );
        assert_eq!(observed[2]["tools"], json!([]));
        assert_eq!(
            observed[2]["messages"][2]["role"],
            sand_inference::ROLE_TOOL
        );
        assert!(
            observed[2]["messages"][2]["toolContent"]["parts"][0]["result"]
                .as_str()
                .is_some_and(|result| result == "fixture contents")
        );

        reset_sand_tool_capability(token, "claude-fable-5");
        {
            let _guard = POLICY_RATE_LIMIT_TEST_LOCK
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            reset_policy_rate_limit_breaker_for_test();
        }
        server.abort();
    }

    #[test]
    fn sand_replay_discards_uncommitted_metadata_events() {
        let mut buffered = vec![
            CursorStreamEvent::ThinkingDelta {
                text: "draft reasoning".into(),
            },
            CursorStreamEvent::Usage {
                input_tokens: 128,
                output_tokens: 3,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
            },
        ];
        discard_sand_replay_buffer(&mut buffered);
        assert!(buffered.is_empty());
    }

    #[test]
    fn sand_end_does_not_commit_thinking_or_usage_only_turns() {
        let thinking = CursorStreamEvent::ThinkingDelta {
            text: "speculative reasoning".into(),
        };
        let usage = CursorStreamEvent::Usage {
            input_tokens: 100,
            output_tokens: 4,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        };
        let end = CursorStreamEvent::End;

        assert_eq!(
            classify_sand_stream_event(&thinking, false, false, false),
            SandStreamEventAction::Buffer
        );
        assert_eq!(
            classify_sand_stream_event(&usage, false, false, false),
            SandStreamEventAction::Buffer
        );
        assert_eq!(
            classify_sand_stream_event(&end, false, false, false),
            SandStreamEventAction::HollowEnd,
            "an empty Sand END must enter the bounded replay path"
        );
        assert_eq!(
            classify_sand_stream_event(
                &CursorStreamEvent::TextDelta {
                    text: String::new()
                },
                false,
                false,
                false,
            ),
            SandStreamEventAction::Buffer,
            "an empty text delta is metadata, not a commit"
        );
    }

    #[test]
    fn sand_end_commits_text_and_native_tools_only_after_progress() {
        let text = CursorStreamEvent::TextDelta {
            text: "answer".into(),
        };
        let tool = CursorStreamEvent::NativeTool {
            tool_use_id: "tool-1".into(),
            name: "Read".into(),
            input: serde_json::json!({"file_path": "/tmp/a"}),
        };
        let end = CursorStreamEvent::End;

        assert_eq!(
            classify_sand_stream_event(&text, false, false, false),
            SandStreamEventAction::Commit
        );
        assert_eq!(
            classify_sand_stream_event(&tool, false, false, false),
            SandStreamEventAction::Commit
        );
        assert_eq!(
            classify_sand_stream_event(&end, true, false, false),
            SandStreamEventAction::Complete
        );
        assert_eq!(
            classify_sand_stream_event(&end, false, false, true),
            SandStreamEventAction::HollowEnd,
            "the compaction reasoning flag must not affect ordinary turns"
        );
    }

    #[test]
    fn sand_compaction_reasoning_only_end_is_a_valid_summary() {
        let thinking = CursorStreamEvent::ThinkingDelta {
            text: "summary from reasoning".into(),
        };
        let end = CursorStreamEvent::End;

        assert_eq!(
            classify_sand_stream_event(&thinking, false, true, true),
            SandStreamEventAction::Buffer,
            "compaction reasoning stays buffered until its terminal boundary"
        );
        assert_eq!(
            classify_sand_stream_event(&end, false, true, true),
            SandStreamEventAction::Complete
        );
        assert_eq!(
            classify_sand_stream_event(&end, false, true, false),
            SandStreamEventAction::HollowEnd,
            "an empty compaction END still needs replay"
        );
    }

    #[test]
    fn xai_compact_request_ids_are_detected_without_matching_other_operations() {
        assert!(is_xai_compact_request(Some("xai-compact-123")));
        assert!(is_xai_compact_request(Some(" xai-compact-123 ")));
        assert!(!is_xai_compact_request(Some("xai-compact")));
        assert!(!is_xai_compact_request(Some("xai-compactible-123")));
        assert!(!is_xai_compact_request(Some("xai-turn-123")));
        assert!(!is_xai_compact_request(None));
    }

    #[test]
    fn anthropic_context_management_compaction_is_detected_without_xai_header() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5[1m]",
            "stream": true,
            "messages": [{"role": "user", "content": "compact"}],
            "context_management": {
                "edits": [{"type": "compact_20260112"}]
            }
        }))
        .expect("valid context-management request");
        assert!(is_context_management_compact_request(&body));
        assert!(is_compact_request(&body, None));
    }

    #[test]
    fn claude_manual_compact_command_marker_uses_isolated_lane() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gemini-3.1-pro",
            "stream": true,
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "<command-name>/compact</command-name>"},
                {"type": "text", "text": "<command-message>compact</command-message>"}
            ]}],
            "tools": [{"name": "Read", "input_schema": {}}]
        }))
        .expect("valid command-marker request");
        assert!(is_compact_request(&body, None));
        let compact_mode = is_compact_request(&body, None);
        let allowed = if compact_mode {
            Some(BTreeSet::new())
        } else {
            advertised_tool_names(&body)
        };
        assert_eq!(allowed.expect("tool set"), BTreeSet::new());
    }

    #[test]
    fn stainless_helper_compaction_is_detected_and_other_helpers_are_ignored() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gemini-3.1-pro",
            "messages": [{"role": "user", "content": "summarize"}]
        }))
        .expect("valid helper request");
        assert!(is_stainless_compaction_helper(Some(
            "BetaToolRunner, compaction"
        )));
        assert!(is_stainless_compaction_helper(Some("COMPACTION")));
        assert!(!is_stainless_compaction_helper(Some("BetaToolRunner")));
        assert!(!is_stainless_compaction_helper(Some("not-compaction")));
        assert!(is_compact_request_with_helper(
            &body,
            None,
            Some("BetaToolRunner, compaction")
        ));
        assert!(!is_compact_request_with_helper(
            &body,
            None,
            Some("BetaToolRunner")
        ));
    }

    #[test]
    fn stainless_compaction_helper_is_case_insensitive_and_supports_lists() {
        assert!(is_stainless_compaction_helper(Some("compaction")));
        assert!(is_stainless_compaction_helper(Some(" Compaction ")));
        assert!(is_stainless_compaction_helper(Some(
            "stream, compaction, retry"
        )));
        assert!(is_stainless_compaction_helper(Some("STREAM,COMPACTION")));
        assert!(!is_stainless_compaction_helper(Some("compact")));
        assert!(!is_stainless_compaction_helper(Some("precompaction")));
        assert!(!is_stainless_compaction_helper(None));
    }

    #[test]
    fn stainless_compaction_helper_routes_gemini_summary_to_isolated_lane() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gemini-3.1-pro",
            "messages": [{"role": "user", "content": "summarize"}],
            "tools": [{"name": "Read", "input_schema": {}}]
        }))
        .expect("valid Gemini request");
        assert!(is_compact_request_with_helper(
            &body,
            None,
            Some("compaction")
        ));
        // The same predicate used by handle_messages must suppress tools for
        // compaction, even if Claude Code included its regular tool catalog.
        let compaction_mode = is_compact_request_with_helper(&body, None, Some("compaction"));
        let allowed = if compaction_mode {
            Some(BTreeSet::new())
        } else {
            advertised_tool_names(&body)
        };
        assert!(compaction_mode);
        assert_eq!(allowed.expect("tool set"), BTreeSet::new());
    }

    #[test]
    fn non_compaction_helper_does_not_change_regular_request_lane() {
        let body = hello_body();
        assert!(!is_compact_request_with_helper(&body, None, Some("stream")));
        assert!(!is_compact_request_with_helper(
            &body,
            None,
            Some("compactible")
        ));
    }

    fn compact_test_context(client_request_id: Option<&str>) -> RequestContext {
        RequestContext {
            req_id: "compact-test-req".into(),
            client_request_id: client_request_id.map(str::to_owned),
            stainless_helper: None,
            session_id: Some("compact-test-session".into()),
            session_seq: None,
            provider: "cursor".into(),
            traffic: None,
            monitor: None,
            claude_code: crate::provider::ClaudeCodeAgentHeaders::default(),
            hold_http_until_live_open: false,
        }
    }

    fn policy_test_context(session_id: Option<&str>) -> RequestContext {
        RequestContext {
            req_id: "policy-test-req".into(),
            client_request_id: None,
            stainless_helper: None,
            session_id: session_id.map(str::to_owned),
            session_seq: None,
            provider: "cursor".into(),
            traffic: None,
            monitor: None,
            claude_code: crate::provider::ClaudeCodeAgentHeaders::default(),
            hold_http_until_live_open: false,
        }
    }

    #[test]
    fn quota_lane_follows_effective_cursor_transport() {
        assert_eq!(quota_lane_for_client_type("sand"), CursorQuotaLane::SandBot);
        assert_eq!(quota_lane_for_client_type("SAND"), CursorQuotaLane::SandBot);
        assert_eq!(quota_lane_for_client_type("cli"), CursorQuotaLane::CliApi);
        assert_eq!(quota_lane_for_client_type("agent"), CursorQuotaLane::CliApi);
        assert_eq!(quota_lane_for_client_type(""), CursorQuotaLane::CliApi);
        assert_eq!(CursorQuotaLane::SandBot.as_str(), "Sand/Bot");
        assert_eq!(CursorQuotaLane::CliApi.as_str(), "CLI/API");
    }

    #[test]
    fn account_id_diagnostic_truncation_is_stable_and_bounded() {
        assert_eq!(truncate_account_id(""), "unknown");
        assert_eq!(truncate_account_id("short-id"), "short-id");
        assert_eq!(truncate_account_id("1234567890abcdef"), "12345678…cdef");
        // Unicode IDs are bounded by characters rather than byte offsets.
        assert_eq!(
            truncate_account_id("账户账户账户账户账户账户账户账户"),
            "账户账户账户账户…账户账户"
        );
    }

    #[test]
    fn quota_diagnostics_include_both_account_lanes_without_bearer() {
        let token = format!("quota-diagnostic-token-{}", uuid::Uuid::new_v4());
        crate::providers::cursor::usage::store_api_usage_evidence_for_test(
            &token,
            100.0,
            Some("2099-09-02T20:12:42Z"),
        );
        crate::providers::cursor::usage::store_sand_usage_evidence_for_test(
            &token,
            2.5,
            Some(true),
            Some("2099-09-02T20:12:42Z"),
        );
        let fields = quota_diagnostic_fields(
            "stable-account-1234567890",
            &token,
            "cursor-grok-4.6-xhigh-fast",
            "cli",
        );
        assert_eq!(fields["accountId"], "stable-a…7890");
        assert_eq!(fields["model"], "cursor-grok-4.6-xhigh-fast");
        assert_eq!(fields["clientType"], "cli");
        assert_eq!(fields["quotaLane"], "CLI/API");
        assert_eq!(fields["apiPercent"], 100.0);
        assert_eq!(fields["grokBotPercent"], 2.5);
        assert_eq!(fields["quotaPercent"], 100.0);
        let serialized = serde_json::Value::Object(fields).to_string();
        assert!(
            !serialized.contains(&token),
            "diagnostic fields must never contain the bearer"
        );
    }

    #[test]
    fn policy_breaker_is_scoped_by_model_account_and_client_route() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        let message = "Connect error 429: ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded";
        note_policy_rate_limit("gemini-3.6-flash-high", "sand", "token-a", message, None);

        let blocked = policy_rate_limit_preflight("gemini-3.6-flash-high", "sand", "token-a")
            .expect_err("the same model/account/route must observe the local cooldown");
        assert_eq!(blocked.status, 429);
        assert!(blocked.retry_after.is_some());
        assert!(policy_rate_limit_preflight("gemini-3.1-pro", "sand", "token-a").is_ok());
        assert!(policy_rate_limit_preflight("gemini-3.6-flash-high", "sand", "token-b").is_ok());
        assert!(
            policy_rate_limit_preflight("gemini-3.6-flash-high", "cli", "token-a").is_ok(),
            "a Sand allowance limit must not block the CLI route"
        );
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn policy_breaker_concurrent_preflights_remain_model_account_and_route_isolated() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        note_policy_rate_limit(
            "gemini-3.6-flash-high",
            "sand",
            "token-a",
            "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED",
            Some("90"),
        );

        std::thread::scope(|scope| {
            let mut checks = Vec::new();
            for index in 0..64 {
                checks.push(scope.spawn(move || {
                    match index % 4 {
                        0 => {
                            policy_rate_limit_preflight("gemini-3.6-flash-high", "sand", "token-a")
                                .is_err()
                        }
                        1 => {
                            policy_rate_limit_preflight("gemini-3.1-pro", "sand", "token-a").is_ok()
                        }
                        2 => {
                            policy_rate_limit_preflight("gemini-3.6-flash-high", "sand", "token-b")
                                .is_ok()
                        }
                        _ => policy_rate_limit_preflight("gemini-3.6-flash-high", "cli", "token-a")
                            .is_ok(),
                    }
                }));
            }
            for check in checks {
                assert!(check.join().expect("policy preflight worker"));
            }
        });
        reset_policy_rate_limit_breaker_for_test();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::await_holding_lock)]
    async fn policy_breaker_single_flights_a_thousand_waiters_until_delayed_429() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        // Preserve the production 5s-policy / 30s-window ratio while keeping
        // this regression sub-second through the injectable window.
        let probe_window = Duration::from_millis(600);
        let owner = policy_rate_limit_admit_fresh_open_with_window(
            "gemini-3.6-flash-high",
            "sand",
            "first-wave-token",
            probe_window,
        )
        .await
        .expect("the first request owns the cold probe");
        assert!(matches!(owner, PolicyRateLimitAdmission::Probe(_)));

        const REQUESTS: usize = 1_000;
        let barrier = Arc::new(tokio::sync::Barrier::new(REQUESTS + 1));
        let unexpected_probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut wave = tokio::task::JoinSet::new();
        for _ in 0..REQUESTS {
            let barrier = Arc::clone(&barrier);
            let unexpected_probes = Arc::clone(&unexpected_probes);
            wave.spawn(async move {
                barrier.wait().await;
                match policy_rate_limit_admit_fresh_open_with_window(
                    "gemini-3.6-flash-high",
                    "sand",
                    "first-wave-token",
                    probe_window,
                )
                .await
                {
                    Ok(PolicyRateLimitAdmission::Probe(lease)) => {
                        unexpected_probes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        drop(lease);
                        None
                    }
                    Ok(PolicyRateLimitAdmission::KnownHealthy) => None,
                    Err(error) => error.retry_after,
                }
            });
        }
        barrier.wait().await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            unexpected_probes.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a policy result delayed by the equivalent of 5s must still precede fanout"
        );
        owner.mark_policy_limited(
            "gemini-3.6-flash-high",
            "sand",
            "first-wave-token",
            "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded",
            Some("120"),
        );
        // Exercise the former note-after-drop race: every awakened waiter must
        // see the breaker even when it is scheduled immediately after release.
        tokio::task::yield_now().await;

        let mut blocked = 0;
        while let Some(result) = wave.join_next().await {
            let retry_after = result.expect("policy first-wave worker");
            if let Some(retry_after) = retry_after {
                let retry_after = retry_after.parse::<u64>().expect("numeric Retry-After");
                assert!((1..=120).contains(&retry_after));
                blocked += 1;
            }
        }
        assert_eq!(
            unexpected_probes.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "only the original cold probe may reach Cursor before its delayed 429"
        );
        assert_eq!(blocked, REQUESTS);
        reset_policy_rate_limit_breaker_for_test();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn sand_policy_probe_does_not_rotate_while_owner_is_still_active() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        // Use a tiny injectable window.  Sand must still keep one owner after
        // that window because the provider can delay its quota decision far
        // beyond the live-start peek.
        let probe_window = Duration::from_millis(25);
        let owner = policy_rate_limit_admit_fresh_open_with_window(
            "grok-build",
            "sand",
            "delayed-sand-policy-token",
            probe_window,
        )
        .await
        .expect("the first Sand request owns the policy probe");
        assert!(matches!(owner, PolicyRateLimitAdmission::Probe(_)));

        let waiter = tokio::spawn(async move {
            policy_rate_limit_admit_fresh_open_with_window(
                "grok-build",
                "sand",
                "delayed-sand-policy-token",
                probe_window,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            !waiter.is_finished(),
            "a Sand waiter must not rotate into a second upstream probe after the quiet window"
        );

        drop(owner);
        let admission = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter should wake when the owner settles")
            .expect("policy waiter task should not panic")
            .expect("the next Sand request should become the new probe");
        assert!(matches!(admission, PolicyRateLimitAdmission::Probe(_)));
        reset_policy_rate_limit_breaker_for_test();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::await_holding_lock)]
    async fn policy_probe_quiet_window_ramps_only_one_of_a_thousand_waiters() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        let probe_window = Duration::from_millis(500);
        let owner = policy_rate_limit_admit_fresh_open_with_window(
            "cursor-grok-4.6-xhigh-fast",
            "cli",
            "healthy-fanout-token",
            probe_window,
        )
        .await
        .expect("the first start owns the coalescing probe");
        assert!(matches!(owner, PolicyRateLimitAdmission::Probe(_)));

        const WAITERS: usize = 1_000;
        let barrier = Arc::new(tokio::sync::Barrier::new(WAITERS + 1));
        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let healthy = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut fanout = tokio::task::JoinSet::new();
        for _ in 0..WAITERS {
            let barrier = Arc::clone(&barrier);
            let probes = Arc::clone(&probes);
            let healthy = Arc::clone(&healthy);
            fanout.spawn(async move {
                barrier.wait().await;
                match policy_rate_limit_admit_fresh_open_with_window(
                    "cursor-grok-4.6-xhigh-fast",
                    "cli",
                    "healthy-fanout-token",
                    probe_window,
                )
                .await
                {
                    Ok(PolicyRateLimitAdmission::Probe(lease)) => {
                        probes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        std::future::pending::<()>().await;
                        drop(lease);
                    }
                    Ok(PolicyRateLimitAdmission::KnownHealthy) => {
                        healthy.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        std::future::pending::<()>().await;
                    }
                    Err(error) => panic!("quiet fanout was rejected: {error}"),
                }
            });
        }
        barrier.wait().await;

        // Once the first quiet window expires, exactly one waiter becomes a
        // second probe. It holds that rotated lease so the remaining 999 stay
        // coalesced until another full window or a decisive event.
        tokio::time::timeout(Duration::from_millis(800), async {
            loop {
                if probes.load(std::sync::atomic::Ordering::SeqCst) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("one bounded ramp probe must be admitted after the quiet window");
        assert_eq!(probes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(healthy.load(std::sync::atomic::Ordering::SeqCst), 0);

        drop(owner);
        fanout.abort_all();
        while fanout.join_next().await.is_some() {}
        reset_policy_rate_limit_breaker_for_test();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn policy_probe_waits_for_a_decisive_result_after_live_peek_timeout() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        let admission = policy_rate_limit_admit_fresh_open(
            "gemini-3.6-flash-high",
            "sand",
            "late-policy-token",
        )
        .await
        .expect("the first request owns the cold probe");
        let lease = admission.into_probe().expect("cold admission is a probe");
        let (upstream_tx, upstream_rx) = mpsc::channel(2);
        let LiveStartPeek::Ready {
            events,
            observed_healthy_event,
        } = peek_live_start_for_stale_reset_with_wait(upstream_rx, Duration::from_millis(1)).await
        else {
            panic!("a quiet live start must remain attached after the short peek");
        };
        assert!(
            !observed_healthy_event,
            "a local peek timeout is not upstream health evidence"
        );
        let mut held = hold_policy_probe_until_decisive_event(
            events,
            lease,
            "gemini-3.6-flash-high".into(),
            "sand".into(),
            "late-policy-token".into(),
            policy_rate_limit_probe_window(),
        );

        let waiter = tokio::spawn(async {
            policy_rate_limit_admit_fresh_open("gemini-3.6-flash-high", "sand", "late-policy-token")
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !waiter.is_finished(),
            "the second session must not dispatch while the first probe is quiet"
        );

        upstream_tx
            .send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::Session {
                session_id: "metadata-only".into(),
            })))
            .await
            .expect("deliver non-decisive metadata");
        assert!(matches!(
            held.recv().await,
            Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::Session { .. })))
        ));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            !waiter.is_finished(),
            "session metadata must not be mistaken for model admission"
        );

        // Continue observing even if the original downstream receiver is
        // dropped; a late policy result still has to open the breaker.
        drop(held);

        let policy_error =
            "Connect error 429: ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded";
        upstream_tx
            .send(Err(policy_error.into()))
            .await
            .expect("deliver the delayed policy result");

        let blocked = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the waiter observes the opened breaker")
            .expect("policy waiter task")
            .expect_err("the delayed first policy result blocks the waiting session");
        assert_eq!(blocked.status, 429);
        assert!(blocked.retry_after.is_some());
        reset_policy_rate_limit_breaker_for_test();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn policy_probe_holds_empty_turn_until_probe_window_expires() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        let probe_window = Duration::from_millis(300);
        let admission = policy_rate_limit_admit_fresh_open_with_window(
            "grok-build",
            "sand",
            "empty-turn-token",
            probe_window,
        )
        .await
        .expect("the first request owns the cold probe");
        let lease = admission.into_probe().expect("cold admission is a probe");
        let (upstream_tx, upstream_rx) = mpsc::channel(2);
        let mut held = hold_policy_probe_until_decisive_event(
            upstream_rx,
            lease,
            "grok-build".into(),
            "sand".into(),
            "empty-turn-token".into(),
            probe_window,
        );

        upstream_tx
            .send(Err(EMPTY_TURN_RETRY_NOTE.to_string()))
            .await
            .expect("deliver the hollow-turn result");
        assert!(matches!(held.recv().await, Some(Err(error)) if error == EMPTY_TURN_RETRY_NOTE));

        let waiter = tokio::spawn(async move {
            policy_rate_limit_admit_fresh_open_with_window(
                "grok-build",
                "sand",
                "empty-turn-token",
                probe_window,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            !waiter.is_finished(),
            "an empty turn must retain the cold probe during its bounded window"
        );

        let next = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the next probe should be admitted after the window")
            .expect("policy waiter task")
            .expect("probe should not be blocked without a policy 429");
        assert!(matches!(next, PolicyRateLimitAdmission::Probe(_)));

        drop(next);
        drop(held);
        drop(upstream_tx);
        reset_policy_rate_limit_breaker_for_test();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::await_holding_lock)]
    async fn policy_probe_holds_metadata_only_eof_until_probe_window_expires() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        let probe_window = Duration::from_millis(250);
        let admission = policy_rate_limit_admit_fresh_open_with_window(
            "grok-build",
            "sand",
            "metadata-eof-token",
            probe_window,
        )
        .await
        .expect("the first request owns the cold probe");
        let lease = admission.into_probe().expect("cold admission is a probe");
        let (upstream_tx, upstream_rx) = mpsc::channel(2);
        let held = hold_policy_probe_until_decisive_event(
            upstream_rx,
            lease,
            "grok-build".into(),
            "sand".into(),
            "metadata-eof-token".into(),
            probe_window,
        );
        upstream_tx
            .send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::Session {
                session_id: "metadata-only".into(),
            })))
            .await
            .expect("deliver metadata");
        drop(upstream_tx);

        let waiter = tokio::spawn(async move {
            policy_rate_limit_admit_fresh_open_with_window(
                "grok-build",
                "sand",
                "metadata-eof-token",
                probe_window,
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(35)).await;
        assert!(
            !waiter.is_finished(),
            "metadata-only EOF must retain the cold probe during its window"
        );
        let next = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("the next probe should be admitted after the window")
            .expect("policy waiter task")
            .expect("probe should not be blocked without a policy 429");
        assert!(matches!(next, PolicyRateLimitAdmission::Probe(_)));
        drop(next);
        drop(held);
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn policy_breaker_key_uses_the_resolved_model_id() {
        assert_eq!(
            policy_rate_limit_key("cursor", "cli", "token-a"),
            policy_rate_limit_key("composer-2.5", "cli", "token-a")
        );
        assert_ne!(
            policy_rate_limit_key("cursor", "sand", "token-a"),
            policy_rate_limit_key("cursor", "cli", "token-a"),
            "Sand and CLI policy state must remain independent"
        );
    }

    fn failover_test_profile(id: &str, token: &str) -> CursorAccountProfile {
        CursorAccountProfile {
            id: id.into(),
            label: Some(id.into()),
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: token.into(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: None,
                email: Some(format!("{id}@example.test")),
                source: "test".into(),
            },
            active: false,
        }
    }

    #[test]
    fn sand_admission_handoff_requires_an_unattempted_viable_account() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        let model = "sand-admission-candidate-unique";
        let profiles = vec![failover_test_profile("only-account", "token-only")];
        let mut attempted = BTreeSet::new();
        attempted.insert(cursor_account_digest("token-only"));
        assert!(
            !sand_admission_candidate_from_profiles(&profiles, "token-only", model, &attempted,),
            "a single-account pool must keep waiting instead of handing off"
        );

        let profiles = vec![
            failover_test_profile("current-account", "token-current"),
            failover_test_profile("alternate-account", "token-alternate"),
        ];
        let mut attempted = BTreeSet::new();
        attempted.insert(cursor_account_digest("token-current"));
        assert!(sand_admission_candidate_from_profiles(
            &profiles,
            "token-current",
            model,
            &attempted,
        ));
        attempted.insert(cursor_account_digest("token-alternate"));
        assert!(
            !sand_admission_candidate_from_profiles(&profiles, "token-current", model, &attempted,),
            "once the alternate was attempted, admission should stay on the current lane"
        );
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn sand_route_cooldown_does_not_become_a_synthetic_policy_429() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        let model = "sand-route-cooldown-isolation";
        let token = "sand-route-cooldown-token";

        note_sand_account_route_cooldown(
            model,
            token,
            "admission_saturated",
            Duration::from_secs(15),
        );

        assert!(
            sand_account_route_is_cooled(model, token),
            "the Sand selector should see the short route cooldown"
        );
        assert!(
            policy_rate_limit_preflight(model, "sand", token).is_ok(),
            "a transport lane cooldown must not synthesize a policy 429"
        );

        let profiles = vec![
            failover_test_profile("cooled", token),
            failover_test_profile("healthy", "sand-route-healthy-token"),
        ];
        let mut attempted = BTreeSet::new();
        attempted.insert(cursor_account_digest(token));
        assert!(sand_admission_candidate_from_profiles(
            &profiles, token, model, &attempted,
        ));
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn account_failover_candidates_skip_current_attempted_and_cooled_accounts() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        let profiles = vec![
            failover_test_profile("z-account", "token-z"),
            failover_test_profile("a-account", "token-a"),
            failover_test_profile("b-account", "token-b"),
            failover_test_profile("c-exhausted", "token-c-exhausted"),
        ];
        note_policy_rate_limit(
            "gemini-3.6-flash-high",
            "sand",
            "token-b",
            "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED",
            Some("60"),
        );
        crate::providers::cursor::usage::store_sand_usage_evidence_for_test(
            "token-c-exhausted",
            100.0,
            Some(false),
            None,
        );
        let mut attempted = BTreeSet::new();
        attempted.insert(cursor_account_digest("token-z"));
        let candidates = account_failover_candidates_from_profiles(
            &profiles,
            "token-current",
            "gemini-3.6-flash-high",
            "sand",
            &attempted,
        );
        assert_eq!(candidates, vec!["token-a"]);
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn sand_open_timeout_is_eligible_for_unbound_account_failover() {
        let timeout = CursorError::new(504, "Sand inference open timed out after 90s", None);
        assert!(is_account_failover_open_error(
            "cursor-grok-account-failover-test",
            &timeout
        ));

        let admission = CursorError::new(
            504,
            "Sand inference open admission deadline exhausted",
            None,
        );
        assert!(is_account_failover_open_error(
            "cursor-grok-account-failover-test",
            &admission
        ));

        let closed = CursorError::new(
            401,
            "Sand inference upstream HTTP 401",
            Some("ERROR_ACCOUNT_CLOSED".into()),
        );
        assert!(is_account_failover_open_error(
            "cursor-grok-account-failover-test",
            &closed
        ));
        assert!(is_account_failover_policy_error(&closed.client_message()));

        let generic = CursorError::new(503, "Cursor upstream temporarily unavailable", None);
        assert!(!is_account_failover_open_error(
            "cursor-grok-account-failover-test",
            &generic
        ));
    }

    #[test]
    fn sand_admission_capacity_error_is_distinguished_from_generic_upstream_5xx() {
        assert!(is_sand_admission_capacity_error(&CursorError::new(
            504,
            "Sand inference open admission deadline exhausted",
            None,
        )));
        assert!(is_sand_admission_capacity_error(&CursorError::new(
            504,
            "Sand open admission queue timed out",
            None,
        )));
        assert!(!is_sand_admission_capacity_error(&CursorError::new(
            503,
            "Cursor upstream temporarily unavailable",
            None,
        )));
    }

    #[test]
    fn sand_account_rebalance_keeps_active_account_for_unseen_lane() {
        assert!(
            !sand_account_rebalance_needed(None, true),
            "an unobserved lane should use the active account first"
        );
    }

    #[test]
    fn sand_account_rebalance_keeps_active_account_while_capacity_remains() {
        assert!(
            !sand_account_rebalance_needed(Some(512), true),
            "a completely idle lane should not rebalance"
        );
        assert!(
            !sand_account_rebalance_needed(Some(511), true),
            "an occupied but non-saturated lane should not rebalance"
        );
    }

    #[test]
    fn sand_account_rebalance_rotates_only_when_lane_is_saturated_or_unhealthy() {
        assert!(sand_account_rebalance_needed(Some(0), true));
        assert!(
            sand_account_rebalance_needed(None, false),
            "a route cooldown should trigger failover even before its lane exists"
        );
        assert!(sand_account_rebalance_needed(Some(128), false));
    }

    #[test]
    fn sand_rebalance_marks_displaced_account_before_late_failover() {
        let mut state = AccountFailoverState::new("token-b");
        let previous_digest = cursor_account_digest("token-a");
        mark_pre_rebalance_account_attempted(&mut state, Some("account-a"), &previous_digest, true);
        assert!(state.attempted_accounts.contains(&previous_digest));
        assert!(state.attempted_accounts.contains("account-a"));

        let before = state.attempted_accounts.clone();
        mark_pre_rebalance_account_attempted(
            &mut state,
            Some("unused-account"),
            "unused-digest",
            false,
        );
        assert_eq!(state.attempted_accounts, before);
    }

    #[test]
    fn account_failover_state_is_bounded_and_never_reuses_an_account() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        let profiles = vec![
            failover_test_profile("a-account", "token-a"),
            failover_test_profile("b-account", "token-b"),
            failover_test_profile("c-account", "token-c"),
            failover_test_profile("d-account", "token-d"),
        ];
        let state = Arc::new(Mutex::new(AccountFailoverState::new("token-current")));
        let first = take_account_failover_candidate_from_profiles(
            &profiles,
            "token-current",
            "gemini-3.6-flash-high",
            "sand",
            &state,
        );
        let second = take_account_failover_candidate_from_profiles(
            &profiles,
            first.as_deref().unwrap_or("token-current"),
            "gemini-3.6-flash-high",
            "sand",
            &state,
        );
        let third = take_account_failover_candidate_from_profiles(
            &profiles,
            second.as_deref().unwrap_or("token-current"),
            "gemini-3.6-flash-high",
            "sand",
            &state,
        );
        let fourth = take_account_failover_candidate_from_profiles(
            &profiles,
            third.as_deref().unwrap_or("token-current"),
            "gemini-3.6-flash-high",
            "sand",
            &state,
        );
        assert_eq!(first.as_deref(), Some("token-a"));
        assert_eq!(second.as_deref(), Some("token-b"));
        assert_eq!(third.as_deref(), Some("token-c"));
        assert!(fourth.is_none(), "all other accounts have been attempted");
        let state = state.lock().unwrap_or_else(|poison| poison.into_inner());
        assert_eq!(state.swaps, account_failover_swap_limit(&profiles));
        assert!(
            state
                .attempted_accounts
                .contains(&cursor_account_digest("token-current"))
        );
        assert!(
            state
                .attempted_accounts
                .contains(&cursor_account_digest("token-a"))
        );
        assert!(
            state
                .attempted_accounts
                .contains(&cursor_account_digest("token-b"))
        );
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn account_failover_budget_covers_every_saved_account_with_hard_cap() {
        let profiles = (0..12)
            .map(|index| {
                failover_test_profile(&format!("account-{index:02}"), &format!("token-{index:02}"))
            })
            .collect::<Vec<_>>();
        assert_eq!(account_failover_swap_limit(&profiles), 11);

        let oversized = (0..32)
            .map(|index| {
                failover_test_profile(&format!("account-{index:02}"), &format!("token-{index:02}"))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            account_failover_swap_limit(&oversized),
            MAX_ACCOUNT_FAILOVER_SWAPS
        );
    }

    #[test]
    fn account_failover_returns_none_when_every_other_account_is_cooled() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        let profiles = vec![
            failover_test_profile("a-account", "token-a"),
            failover_test_profile("b-account", "token-b"),
        ];
        for token in ["token-a", "token-b"] {
            note_policy_rate_limit(
                "gemini-3.6-flash-high",
                "sand",
                token,
                "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED",
                Some("60"),
            );
        }
        let state = Arc::new(Mutex::new(AccountFailoverState::new("token-current")));
        assert!(
            take_account_failover_candidate_from_profiles(
                &profiles,
                "token-current",
                "gemini-3.6-flash-high",
                "sand",
                &state,
            )
            .is_none()
        );
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn concurrent_account_failover_claims_do_not_duplicate_candidates() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        let profiles = Arc::new(vec![
            failover_test_profile("a-account", "token-a"),
            failover_test_profile("b-account", "token-b"),
            failover_test_profile("c-account", "token-c"),
        ]);
        let state = Arc::new(Mutex::new(AccountFailoverState::new("token-current")));
        let selected = Arc::new(Mutex::new(Vec::new()));
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let profiles = Arc::clone(&profiles);
                let state = Arc::clone(&state);
                let selected = Arc::clone(&selected);
                scope.spawn(move || {
                    if let Some(token) = take_account_failover_candidate_from_profiles(
                        &profiles,
                        "token-current",
                        "gemini-3.6-flash-high",
                        "sand",
                        &state,
                    ) {
                        selected
                            .lock()
                            .unwrap_or_else(|poison| poison.into_inner())
                            .push(token);
                    }
                });
            }
        });
        let selected = selected.lock().unwrap_or_else(|poison| poison.into_inner());
        assert_eq!(
            selected.len(),
            account_failover_swap_limit(&profiles) as usize
        );
        assert_ne!(selected[0], selected[1]);
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn account_failover_policy_filter_excludes_subscription_wide_blocks() {
        assert!(is_account_failover_policy_error(
            "Connect error 429: ERROR_SAND_USER_RATE_LIMIT_EXCEEDED: usage meter is 100%"
        ));
        assert!(is_account_failover_policy_error(
            "Connect error 429: ERROR_CURSOR_API_RATE_LIMIT_EXCEEDED: API usage meter is 100%"
        ));
        assert!(is_account_failover_policy_error(
            "Connect error 429: ERROR_RATE_LIMITED_CHANGEABLE: Free plans can only use Auto"
        ));
        assert!(is_account_failover_policy_error(
            "Cursor error 400: ERROR_PROVIDER_ERROR providerStatusCode=400 resource_exhausted isRetryable=false"
        ));
        assert!(is_account_failover_policy_error(
            r#"Cursor error 429: {"error":{"details":[{"debug":{"error":"ERROR_PROVIDER_ERROR","details":{"additionalInfo":{"providerStatusCode":400},"isRetryable":false}}}]}}"#
        ));
        assert!(is_account_failover_policy_error(
            "Connect error 429: ERROR_GPT_4_VISION_PREVIEW_RATE_LIMIT: You are out of usage — Upgrade to a paid plan to use more Grok Bot. [resource_exhausted]"
        ));
        assert!(!is_account_failover_policy_error(
            "Cursor error 429: ERROR_PROVIDER_ERROR provider unavailable; temporary trouble connecting to the model provider [providerStatusCode=400,isRetryable=false]"
        ));
        assert!(!is_account_failover_policy_error(
            "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice"
        ));
        assert!(!is_account_failover_policy_error(
            "Connect error 429: ERROR_RESOURCE_EXHAUSTED: High Load — switch models"
        ));
    }

    #[test]
    fn upstream_already_active_recovery_is_bounded_and_local_busy_is_preserved() {
        let upstream = "Cursor error 503: Cursor upstream HTTP 503 (A Cursor live run is already active for this session; retry after it advances)";
        assert!(live_error_is_upstream_already_active(upstream));
        assert!(
            !is_local_live_busy_text(upstream),
            "an upstream wrapper must enter the one-shot fresh-conversation recovery"
        );

        let local = format!("Cursor error 503: {LIVE_RUN_BUSY_MESSAGE}");
        assert!(live_error_is_upstream_already_active(&local));
        assert!(
            is_local_live_busy_text(&local),
            "the local registry diagnostic must keep its attach/wait semantics"
        );

        let fence = AtomicBool::new(false);
        assert!(claim_upstream_active_recovery(&fence));
        assert!(
            !claim_upstream_active_recovery(&fence),
            "a logical request gets one fresh-conversation recovery, never a loop"
        );
    }

    #[test]
    fn upstream_already_active_pump_uses_dedicated_retry_but_local_busy_does_not() {
        let upstream = Err(
            "Connect error 503: Cursor upstream HTTP 503 (A Cursor live run is already active for this session; retry after it advances)"
                .to_string(),
        );
        assert_eq!(
            classify_live_pump_item(false, &upstream),
            LivePumpAction::Retry,
            "a pre-output upstream conflict must reach LiveRetryStart::start_after_error"
        );
        assert_eq!(
            classify_live_pump_item(true, &upstream),
            LivePumpAction::Forward,
            "a conflict after committed output remains fail-closed"
        );

        let local = Err(format!("Cursor error 503: {LIVE_RUN_BUSY_MESSAGE}"));
        assert_eq!(
            classify_live_pump_item(false, &local),
            LivePumpAction::Forward,
            "the local registry busy response must retain its attach/wait semantics"
        );
    }

    #[test]
    fn kv_blob_store_overflow_rotates_once_and_replays_full_history() {
        let message = "Request too large (413) — invalid_request_error: Cursor error 413: Cursor KV blob store limit exceeded (blob=3559 bytes, blobs=4097, total=62731560 bytes)";
        assert!(live_error_is_kv_blob_overflow(message));
        assert!(live_error_allows_fresh_conversation(message));
        assert!(live_error_is_same_request_retryable(message));
        assert_eq!(same_request_retry_wait_ms(0, message), 0);
        assert_eq!(
            live_late_retry_limit(message, LiveLateRetryPolicy::default()),
            1,
            "KV overflow must have an independent single reset budget"
        );
        assert_eq!(
            classify_live_pump_item(false, &Err(message.to_string())),
            LivePumpAction::Retry
        );
        let start = CursorError::new(413, "Request too large (413)", Some(message.into()));
        assert!(cursor_start_error_is_same_request_retryable(&start));
        assert!(!live_start_error_seals_tombstone(&start));
    }

    #[test]
    fn compact_agent_looping_error_retries_only_in_compaction_mode() {
        let looping =
            "Connect error 400: ERROR_INTERNAL: Agent Looping Detected [failed_precondition]";
        assert_eq!(
            classify_live_pump_item(false, &Err(looping.to_string())),
            LivePumpAction::Forward,
            "ordinary turns must not replay a potentially accepted request"
        );
        assert_eq!(
            classify_live_pump_item_with_mode(false, &Err(looping.to_string()), true),
            LivePumpAction::Retry,
            "compaction has a bounded fresh-lane recovery"
        );
        assert_eq!(
            classify_live_pump_item_with_mode(true, &Err(looping.to_string()), true),
            LivePumpAction::Forward,
            "a committed compact response must not be replayed"
        );
        assert!(live_probe_error_blocks_new_run_for_mode(looping, false));
        assert!(
            !live_probe_error_blocks_new_run_for_mode(looping, true),
            "compaction loop errors must reach the one-shot fresh-lane restart"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn stale_pre_limit_probe_cannot_mark_post_cooldown_epoch_healthy() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        let stale = policy_rate_limit_admit_fresh_open_with_window(
            "gemini-3.6-flash-high",
            "sand",
            "epoch-token",
            Duration::from_millis(100),
        )
        .await
        .expect("the pre-limit request owns a probe");
        assert!(matches!(stale, PolicyRateLimitAdmission::Probe(_)));

        note_policy_rate_limit(
            "gemini-3.6-flash-high",
            "sand",
            "epoch-token",
            "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded",
            Some("30"),
        );
        // Simulate cooldown expiry without a wall-clock sleep. The stale Run
        // then reports useful output, but its older epoch must not turn the
        // newly half-open gate Healthy.
        POLICY_RATE_LIMIT_BREAKER
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clear();
        stale.mark_healthy();

        let after_cooldown = policy_rate_limit_admit_fresh_open_with_window(
            "gemini-3.6-flash-high",
            "sand",
            "epoch-token",
            Duration::from_millis(100),
        )
        .await
        .expect("the post-cooldown request is admitted as half-open");
        assert!(
            matches!(after_cooldown, PolicyRateLimitAdmission::Probe(_)),
            "stale pre-limit health evidence must not skip the fresh half-open probe"
        );
        drop(after_cooldown);
        reset_policy_rate_limit_breaker_for_test();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn committed_sand_policy_error_opens_breaker_even_after_probe_release() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        let model = "gemini-3.6-flash-high";
        let client_type = "sand";
        let token = "late-policy-token";
        let admission = policy_rate_limit_admit_fresh_open_with_window(
            model,
            client_type,
            token,
            Duration::from_millis(100),
        )
        .await
        .expect("the first request owns the cold probe");
        let error = CursorError::new(
            429,
            "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded",
            None,
        );

        // This models a late allowance response after the stream has already
        // committed visible output.  The old committed-first branch marked
        // the probe healthy and let the next request through.
        assert!(settle_sand_policy_probe(
            Some(admission),
            model,
            client_type,
            token,
            Some(&error),
            true,
        ));
        let blocked = policy_rate_limit_preflight(model, client_type, token)
            .expect_err("a late account quota error must open the local breaker");
        assert_eq!(blocked.status, 429);
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn late_sand_policy_error_is_recorded_without_a_live_probe_lease() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        let model = "gemini-3.6-flash-high";
        let client_type = "sand";
        let token = "released-late-policy-token";
        let error = CursorError::new(
            429,
            "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded",
            None,
        );
        assert!(settle_sand_policy_probe(
            None,
            model,
            client_type,
            token,
            Some(&error),
            true,
        ));
        assert!(
            policy_rate_limit_preflight(model, client_type, token).is_err(),
            "the stream driver may have released its probe at first output"
        );
        reset_policy_rate_limit_breaker_for_test();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn sand_tool_capability_rejection_does_not_open_policy_breaker() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();

        let model = "claude-fable-5";
        let client_type = "sand";
        let token = "capability-breaker-regression-token";
        let admission = policy_rate_limit_admit_fresh_open_with_window(
            model,
            client_type,
            token,
            Duration::from_millis(100),
        )
        .await
        .expect("the first request owns the cold probe");
        let error = CursorError::new(
            429,
            "ERROR_PROVIDER_ERROR",
            Some(
                "{\"error\":{\"code\":\"resource_exhausted\",\"details\":[{\"debug\":{\"error\":\"ERROR_PROVIDER_ERROR\",\"details\":{\"additionalInfo\":{\"providerStatusCode\":400},\"isRetryable\":false,\"detail\":\"tool catalog is not supported by this provider\"}}}]}}"
                    .into(),
            ),
        );
        assert!(is_sand_tool_capability_error(&error, 1));
        assert!(
            !settle_sand_policy_probe(
                Some(admission),
                model,
                client_type,
                token,
                Some(&error),
                false,
            ),
            "tool capability rejection is not an account policy result"
        );
        assert!(
            policy_rate_limit_preflight(model, client_type, token).is_ok(),
            "text bridge fallback must not inherit a local 429 cooldown"
        );
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn policy_cooldown_parses_retry_hints_and_bounds_values() {
        assert_eq!(
            policy_rate_limit_cooldown_secs("retry after 2 minutes", None),
            120
        );
        assert_eq!(
            policy_rate_limit_cooldown_secs("try again in 3 seconds", None),
            5
        );
        assert_eq!(
            policy_rate_limit_cooldown_secs("wait 9999 seconds", None),
            600
        );
        assert_eq!(policy_rate_limit_cooldown_secs("quota", Some("120")), 120);
        assert_eq!(
            policy_rate_limit_cooldown_secs("retry after 2 minutes", Some("9")),
            9,
            "the upstream HTTP header takes priority over body prose"
        );
        assert!(
            retry_after_delta_secs("Wed, 21 Oct 2037 07:28:00 GMT").is_some(),
            "HTTP-date Retry-After must be accepted"
        );
    }

    #[test]
    fn policy_cooldown_uses_sand_next_reset_at() {
        let reset_at = (time::OffsetDateTime::now_utc() + time::Duration::seconds(90))
            .format(&time::format_description::well_known::Rfc3339)
            .expect("format reset timestamp");
        let message = format!(
            "Connect error 429: resource_exhausted rateLimitReason=sand_included_limit nextResetAt={reset_at} isRetryable=false"
        );
        let cooldown = policy_rate_limit_cooldown_secs(&message, None);
        assert!(
            (5..=90).contains(&cooldown),
            "nextResetAt should drive the account breaker, got {cooldown}s"
        );
        assert_eq!(
            next_reset_at_delta_secs(&message),
            Some(cooldown),
            "flattened Sand reset metadata must be parsed consistently"
        );
    }

    #[test]
    fn policy_preflight_keeps_tool_result_continuations_attachable() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "gemini-3.6-flash-high",
            "messages": [{"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": "call-1",
                "content": "done"
            }]}]
        }))
        .unwrap();
        assert!(policy_preflight_can_attach_existing_run(
            &body,
            &policy_test_context(Some("policy-tool-session")),
        ));
    }

    #[test]
    fn policy_breaker_map_prunes_expired_entries_and_stays_bounded() {
        let _guard = POLICY_RATE_LIMIT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        reset_policy_rate_limit_breaker_for_test();
        let now = Instant::now();
        {
            let mut breaker = POLICY_RATE_LIMIT_BREAKER
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            breaker.insert(
                "expired".into(),
                PolicyRateLimitState {
                    until: now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
                    retry_after_secs: 1,
                    message: "expired".into(),
                },
            );
            for index in 0..POLICY_RATE_LIMIT_BREAKER_MAX_ENTRIES {
                breaker.insert(
                    format!("seed-{index}"),
                    PolicyRateLimitState {
                        until: now + Duration::from_secs(60 + index as u64),
                        retry_after_secs: 60,
                        message: "seed".into(),
                    },
                );
            }
        }
        note_policy_rate_limit(
            "gemini-3.6-flash-high",
            "sand",
            "fresh-token",
            "ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: retry after 30 seconds",
            None,
        );
        let breaker = POLICY_RATE_LIMIT_BREAKER
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        assert!(breaker.len() <= POLICY_RATE_LIMIT_BREAKER_MAX_ENTRIES);
        assert!(!breaker.contains_key("expired"));
        drop(breaker);
        reset_policy_rate_limit_breaker_for_test();
    }

    #[test]
    fn compact_agent_id_is_stable_for_xai_retries_with_stream_changes() {
        let first = hello_body();
        let mut retry = first.clone();
        retry.stream = !first.stream;
        let ctx = compact_test_context(Some("xai-compact-42"));
        assert_eq!(
            compact_agent_id(&first, &ctx),
            compact_agent_id(&retry, &ctx)
        );
    }

    #[test]
    fn compact_agent_id_separates_xai_operations() {
        let body = hello_body();
        let first = compact_agent_id(&body, &compact_test_context(Some("xai-compact-1")));
        let second = compact_agent_id(&body, &compact_test_context(Some("xai-compact-2")));
        assert_ne!(first, second);
    }

    #[test]
    fn compact_agent_id_without_xai_header_is_stable_for_same_body() {
        let body = hello_body();
        let ctx = compact_test_context(None);
        assert_eq!(compact_agent_id(&body, &ctx), compact_agent_id(&body, &ctx));
    }

    #[test]
    fn compact_agent_id_uses_body_for_non_xai_request_ids() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [{"role": "user", "content": "compact"}],
            "context_management": {
                "edits": [{"type": "compact_20260112"}]
            }
        }))
        .expect("valid compact request");
        let first = compact_agent_id(&body, &compact_test_context(Some("xai-turn-1")));
        let second = compact_agent_id(&body, &compact_test_context(Some("xai-turn-2")));
        assert_eq!(first, second);
    }

    #[test]
    fn compact_identity_does_not_change_regular_agent_identity() {
        let body = hello_body();
        let ctx = compact_test_context(Some("xai-turn-1"));
        let original = ctx.claude_code.agent_id.clone();
        assert!(!is_compact_request(&body, ctx.client_request_id.as_deref()));
        assert_eq!(ctx.claude_code.agent_id, original);
    }

    #[test]
    fn compact_identity_keeps_session_and_live_path_eligible() {
        let body = hello_body();
        let mut ctx = compact_test_context(Some("xai-compact-live"));
        let compact = compact_agent_id(&body, &ctx);
        ctx.claude_code.agent_id = Some(compact);
        ctx.claude_code.parent_agent_id = None;
        let session = ctx.session_id.as_deref().expect("test session");
        let identity = live_run_identity(session, &ctx);
        assert!(identity.agent_id.is_some());
        assert!(live_path_eligible(true, true, true));
    }

    #[test]
    fn unrelated_context_management_edits_do_not_isolate_a_request() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [{"role": "user", "content": "continue"}],
            "context_management": {
                "edits": [{"type": "clear_tool_uses_20250919"}]
            }
        }))
        .expect("valid context-management request");
        assert!(!is_context_management_compact_request(&body));
        assert!(!is_compact_request(&body, Some("xai-turn-123")));
    }

    fn pending(tool_use_id: &str) -> PendingCursorExec {
        PendingCursorExec {
            id: 1,
            exec_id: Some("exec-1".into()),
            tool_use_id: tool_use_id.into(),
            claude_name: "Read".into(),
            claude_input: serde_json::json!({"file_path":"/tmp/one"}),
            kind: exec_results::CursorExecKind::Read {
                path: "/tmp/one".into(),
                range_applied: false,
            },
        }
    }

    #[test]
    fn local_replacement_conflicts_distinguish_fresh_prompts_from_tool_results() {
        assert_eq!(live_replacement_conflict_error(false).status, 503);
        assert_eq!(
            live_replacement_conflict_error(true).status,
            409,
            "a stale tool-result POST must fail closed instead of rebinding to another Run"
        );
    }

    #[test]
    fn h2_shard_index_is_stable_and_spreads_nested_agent_keys() {
        let key = "session::agent::agent-a";
        assert_eq!(
            cursor_http_shard_index(key, 4),
            cursor_http_shard_index(key, 4),
            "retries for one conversation must stay in the same H2 failure domain"
        );

        let shards = (0..32)
            .map(|index| cursor_http_shard_index(&format!("session::agent::agent-{index}"), 4))
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            shards.len(),
            4,
            "nested agents sharing a Claude session must fan out across all configured shards"
        );
    }

    #[test]
    fn supported_models_includes_legacy_and_agent() {
        let provider = CursorProvider::new();
        let models = provider.supported_models();
        assert!(models.contains(&"cursor".to_string()));
        assert!(models.contains(&"cursor-agent".to_string()));
        assert!(models.contains(&"cursor-plan".to_string()));
        assert!(models.contains(&"cursor-ask".to_string()));
    }

    #[test]
    fn live_continuation_rejects_zero_matching_tool_results() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor:gpt-5.5",
            "max_tokens": 128,
            "stream": true,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "different-id",
                    "content": "result"
                }]
            }]
        }))
        .unwrap();
        let missing = collect_live_tool_results(&body, &[pending("expected-id")]).unwrap_err();
        assert_eq!(missing, ["expected-id"]);
    }

    #[test]
    fn live_continuation_rejects_extra_and_duplicate_current_tool_results() {
        let extra: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "expected-id", "content": "ok"},
                {"type": "tool_result", "tool_use_id": "unexpected-id", "content": "wrong batch"}
            ]}]
        }))
        .unwrap();
        assert!(
            collect_live_tool_results(&extra, &[pending("expected-id")]).is_err(),
            "an extra current tool_result must not be silently dropped"
        );

        let duplicate: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "expected-id", "content": "first"},
                {"type": "tool_result", "tool_use_id": "expected-id", "content": "second"}
            ]}]
        }))
        .unwrap();
        assert!(
            collect_live_tool_results(&duplicate, &[pending("expected-id")]).is_err(),
            "a duplicate current tool_result must not be collapsed to one"
        );

        let mixed: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "expected-id", "content": "ok"},
                {"type": "text", "text": "also start unrelated work"}
            ]}]
        }))
        .unwrap();
        assert!(
            collect_live_tool_results(&mixed, &[pending("expected-id")]).is_err(),
            "fresh user content must not be discarded by treating a mixed turn as result-only"
        );
    }

    #[test]
    fn responses_multi_tool_outputs_resume_one_live_batch() {
        let body = crate::openai::responses_to_messages(&serde_json::json!({
            "model": "cursor-grok-4.5-high-fast",
            "input": [
                {"type": "message", "role": "user", "content": "read both files"},
                {"type": "function_call", "call_id": "call-1", "name": "read_file", "arguments": "{}"},
                {"type": "function_call", "call_id": "call-2", "name": "read_file", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "call-1", "output": "first"},
                {"type": "function_call_output", "call_id": "call-2", "output": "second"},
                {
                    "type": "message",
                    "role": "user",
                    "content": "<system-reminder>Background work completed.</system-reminder>"
                }
            ]
        }))
        .unwrap();

        assert!(
            request_has_current_tool_result(&body),
            "split Responses outputs must be classified as a live resume"
        );
        assert!(
            latest_user_is_only_tool_results(&body),
            "the same logical result turn must disable checkpoint delta replay after restart"
        );
        let delta = render_cursor_prompt_parts_with(
            &body,
            CursorPromptOptions {
                omit_tools: true,
                delta_only: true,
            },
        );
        assert!(delta.user_text.contains("first"));
        assert!(delta.user_text.contains("second"));
        assert!(!delta.user_text.contains("Background work completed"));
        let matched = collect_live_tool_results(&body, &[pending("call-1"), pending("call-2")])
            .expect("separate Responses outputs must resume the same live tool batch");
        assert_eq!(
            matched
                .iter()
                .map(|(tool_use_id, _)| tool_use_id.as_str())
                .collect::<Vec<_>>(),
            ["call-1", "call-2"]
        );
    }

    #[test]
    fn live_resume_preserves_multi_message_tool_batch_boundaries() {
        let native: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [{"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "call-1", "content": "first"},
                {"type": "tool_result", "tool_use_id": "call-2", "content": "second"}
            ]}]
        }))
        .unwrap();
        let matched = collect_live_tool_results(&native, &[pending("call-1"), pending("call-2")])
            .expect("one Anthropic user message must retain every tool result");
        assert_eq!(
            matched
                .iter()
                .map(|(tool_use_id, _)| tool_use_id.as_str())
                .collect::<Vec<_>>(),
            ["call-1", "call-2"]
        );

        let interrupted_by_reminder: MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model": "cursor-grok-4.5-high-fast",
                "messages": [
                    {"role": "assistant", "content": [
                        {"type": "tool_use", "id": "call-1", "name": "read_file", "input": {}},
                        {"type": "tool_use", "id": "call-2", "name": "read_file", "input": {}}
                    ]},
                    {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "call-1", "content": "first"}
                    ]},
                    {"role": "user", "content": "<system-reminder>Background work completed.</system-reminder>"},
                    {"role": "user", "content": [
                        {"type": "tool_result", "tool_use_id": "call-2", "content": "second"}
                    ]}
                ]
            }))
            .unwrap();
        let matched = collect_live_tool_results(
            &interrupted_by_reminder,
            &[pending("call-1"), pending("call-2")],
        )
        .expect("a standalone reminder must not split one tool-result batch");
        assert_eq!(matched.len(), 2);

        let interrupted_by_user: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.5-high-fast",
            "messages": [
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "call-1", "name": "read_file", "input": {}},
                    {"type": "tool_use", "id": "call-2", "name": "read_file", "input": {}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "call-1", "content": "first"}
                ]},
                {"role": "user", "content": "start different work"},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "call-2", "content": "second"}
                ]}
            ]
        }))
        .unwrap();
        let missing = collect_live_tool_results(
            &interrupted_by_user,
            &[pending("call-1"), pending("call-2")],
        )
        .expect_err("fresh user content must split adjacent tool-result batches");
        assert_eq!(missing, ["call-1"]);
    }

    #[test]
    fn live_resume_ignores_historical_tool_results_on_a_new_user_turn() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [
                {"role": "user", "content": "old request"},
                {"role": "assistant", "content": [{
                    "type": "tool_use",
                    "id": "old-tool",
                    "name": "Read",
                    "input": {"file_path": "/tmp/old"}
                }]},
                {"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "old-tool",
                    "content": "old result"
                }]},
                {"role": "user", "content": "new request after interrupt"}
            ]
        }))
        .unwrap();

        assert!(
            !request_has_current_tool_result(&body),
            "historical results must not turn a new request into a live resume"
        );
        let missing = collect_live_tool_results(&body, &[pending("old-tool")]).unwrap_err();
        assert_eq!(
            missing,
            ["old-tool"],
            "a historical result must never satisfy the current pending batch"
        );
    }

    #[test]
    fn live_resume_looks_through_trailing_system_reminder_for_current_tool_results() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.6-xhigh-fast",
            "messages": [
                {"role": "user", "content": "read the file"},
                {"role": "assistant", "content": [{
                    "type": "tool_use",
                    "id": "expected-tool",
                    "name": "read_file",
                    "input": {"target_file": "/tmp/input"}
                }]},
                {"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "expected-tool",
                    "content": "file contents"
                }]},
                {"role": "user", "content": [{
                    "type": "text",
                    "text": "<system-reminder>\nBackground work completed.\n</system-reminder>"
                }]}
            ]
        }))
        .unwrap();

        assert!(
            request_has_current_tool_result(&body),
            "a trailing synthetic reminder must not turn a tool continuation into a fresh run"
        );
        let matched = collect_live_tool_results(&body, &[pending("expected-tool")])
            .expect("the immediately preceding tool result is still the current live batch");
        assert_eq!(matched[0].0, "expected-tool");
    }

    #[test]
    fn live_tool_result_fc_alias_and_unrelated_batch() {
        let aliased: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.6-xhigh-fast",
            "messages": [{"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": "fc_call-1",
                "content": "ok"
            }]}]
        }))
        .unwrap();
        let matched = collect_live_tool_results(&aliased, &[pending("call-1")])
            .expect("fc_ prefix is an alias of the exposed tool_use id");
        assert_eq!(matched[0].0, "call-1");

        let reverse: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.6-xhigh-fast",
            "messages": [{"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": "call-1",
                "content": "ok"
            }]}]
        }))
        .unwrap();
        let matched = collect_live_tool_results(&reverse, &[pending("fc_call-1")])
            .expect("bare id matches an fc_ pending id");
        assert_eq!(matched[0].0, "fc_call-1");

        let unrelated: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.6-xhigh-fast",
            "messages": [{"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": "fc_other",
                "content": "stale"
            }]}]
        }))
        .unwrap();
        assert!(
            live_current_batch_is_unrelated(
                &unrelated,
                &[
                    pending("call-f38a5db0-c948-4429-890d-d1113d2c7a36-0"),
                    pending("fc_owSziHw-6jKPYy-a2c1c5de7ba52d13_0"),
                ]
            ),
            "zero-overlap current results are not a resume of this live batch"
        );
        assert!(!live_current_batch_is_unrelated(
            &aliased,
            &[pending("call-1")]
        ));
    }

    #[test]
    fn live_tool_result_matches_dual_id_newline_and_grok_sanitize() {
        let pending_id = "call-72ee1731-4917-4d55-96f6-89841af2f48f-3\nfc_owTHooM-2dTqGa-65a125c0-aws_uw2_0__cursor_run_f7a036cf-3617-41b9";
        let grok_sanitized = pending_id.replace('\n', "_");
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.6-xhigh-fast",
            "messages": [{"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": grok_sanitized,
                "content": "ok"
            }]}]
        }))
        .unwrap();
        let matched = collect_live_tool_results(&body, &[pending(pending_id)])
            .expect("newline dual-id must match grok-build's underscore sanitizer");
        assert_eq!(matched[0].0, pending_id);

        let empty_id: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.6-xhigh-fast",
            "messages": [{"role": "user", "content": [{
                "type": "tool_result",
                "content": "no id"
            }]}]
        }))
        .unwrap();
        assert!(
            !live_current_batch_is_unrelated(&empty_id, &[pending("call-1")]),
            "a tool_result without tool_use_id is not an unrelated resume batch"
        );
        assert!(!request_has_current_tool_result(&empty_id));
    }

    #[test]
    fn live_tool_result_never_crosses_cursor_run_generation() {
        let pending_id = "recycled-id__cursor_run_generation-b";
        let stale: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.6-xhigh-fast",
            "messages": [{"role": "user", "content": [{
                "type": "tool_result",
                "tool_use_id": "recycled-id__cursor_run_generation-a",
                "content": "belongs to generation A"
            }]}]
        }))
        .unwrap();

        assert!(
            collect_live_tool_results(&stale, &[pending(pending_id)]).is_err(),
            "alias normalization must not strip the Run generation fence"
        );
        assert!(!tool_use_ids_equivalent(
            pending_id,
            "recycled-id__cursor_run_generation-a"
        ));
    }

    #[test]
    fn live_resume_without_current_results_supersedes_pending_instead_of_400() {
        match unresolved_live_tools_outcome(
            false,
            vec!["abandoned-tool".into()],
            Some("observed-generation"),
        ) {
            LiveResumeOutcome::SupersedeRunning(run_id) => {
                assert_eq!(run_id, "observed-generation");
            }
            _ => panic!("an abandoned tool turn should supersede only its observed generation"),
        }
        match unresolved_live_tools_outcome(
            true,
            vec!["still-required".into()],
            Some("observed-generation"),
        ) {
            LiveResumeOutcome::SupersedeRunning(run_id) => {
                assert_eq!(run_id, "observed-generation");
            }
            _ => panic!("a mismatched current tool-result batch must supersede, not 400"),
        }
    }

    #[test]
    fn user_facing_missing_blobs_error_allows_fresh_conversation() {
        let message = "Connect error 502: ERROR_CUSTOM_MESSAGE: Conversation data missing - This conversation's data is missing and can't be restored. Start a new chat to continue. (26 missing blobs: 59fb2285cd72) [internal] (stale Cursor conversation reset; retry this message to continue)";
        assert!(
            live_error_allows_fresh_conversation(message),
            "the grok-build 502 after a serve interrupt must be retryable on the same request"
        );
        assert!(!live_error_allows_fresh_conversation(
            "Connect error 502: Cursor stream stalled after partial progress"
        ));
        assert!(
            !live_error_allows_fresh_conversation(
                "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]"
            ),
            "429 is not a conversation-reset; it retries via the transient path"
        );
        assert!(
            live_error_is_same_request_retryable(
                "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]"
            ),
            "transient provider 429 must retry on this same request"
        );
        assert!(
            !live_error_is_same_request_retryable(
                "Connect error 429: ERROR_RESOURCE_EXHAUSTED: High Load — We're experiencing high demand for Cursor Grok 4.5 right now. Please switch to Auto, another model, or try again in a few moments. [resource_exhausted]"
            ),
            "High Load must fail closed as 429, not retry 3 times into the same shed"
        );
        assert!(live_error_is_same_request_retryable(
            "Connect error 502: Conversation data missing (stale Cursor conversation reset; retry this message to continue)"
        ));
        assert!(
            !live_error_is_same_request_retryable(
                "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — pay your invoice in Stripe [resource_exhausted]"
            ),
            "billing 429 must fail closed, not spin"
        );
        assert!(
            !live_error_is_same_request_retryable(
                "Connect error 502: This model is not available in your country or region [internal]"
            ),
            "geo blocks must fail closed, not retry as a 502"
        );
        assert!(live_error_is_same_request_retryable(
            "Connect error 502: Image not found [internal]"
        ));
        assert!(!live_error_is_same_request_retryable(
            "Connect error 502: model slug is not supported [invalid_argument]"
        ));
        assert!(
            live_error_is_same_request_retryable(
                "Connect error 400: Conversation data missing [failed_precondition] (stale Cursor conversation reset; retry this message to continue)"
            ),
            "conversation-missing must still same-request retry after 4xx classification"
        );
        assert!(!live_error_is_same_request_retryable(
            "Cursor error 403: Cursor upstream HTTP 403"
        ));
        assert_eq!(
            same_request_retry_wait_ms(
                0,
                "Connect error 502: Conversation data missing (stale Cursor conversation reset; retry this message to continue)"
            ),
            0
        );
        assert!(
            same_request_retry_wait_ms(
                0,
                "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]"
            ) > 0
        );
    }

    #[tokio::test]
    async fn peek_stale_conversation_reset_does_not_commit_sse() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(Err(
            "Connect error 502: Conversation data missing (stale Cursor conversation reset; retry this message to continue)"
                .into(),
        ))
        .await
        .unwrap();
        drop(tx);
        assert!(
            matches!(
                peek_live_start_for_stale_reset(rx).await,
                LiveStartPeek::Retryable(_)
            ),
            "first-event missing-conversation reset must not be forwarded to grok-build"
        );
    }

    #[tokio::test]
    async fn peek_upstream_active_uses_fresh_conversation_lane_but_local_busy_does_not() {
        let upstream = "Cursor error 503: Cursor upstream HTTP 503 (A Cursor live run is already active for this session; retry after it advances)";
        let (tx, rx) = mpsc::channel(2);
        tx.send(Err(upstream.to_string())).await.unwrap();
        drop(tx);
        assert!(
            matches!(
                peek_live_start_for_stale_reset(rx).await,
                LiveStartPeek::Retryable(error) if error == upstream
            ),
            "the initial error must reach the one-shot fresh-conversation recovery"
        );

        let local = format!("Cursor error 503: {LIVE_RUN_BUSY_MESSAGE}");
        let (tx, rx) = mpsc::channel(2);
        tx.send(Err(local.clone())).await.unwrap();
        drop(tx);
        let LiveStartPeek::Ready { mut events, .. } = peek_live_start_for_stale_reset(rx).await
        else {
            panic!("the local registry busy response must not rotate the conversation");
        };
        assert_eq!(events.recv().await.unwrap().unwrap_err(), local);
    }

    #[tokio::test]
    async fn peek_transient_429_does_not_commit_sse() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(Err(
            "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]"
                .into(),
        ))
        .await
        .unwrap();
        drop(tx);
        assert!(
            matches!(
                peek_live_start_for_stale_reset(rx).await,
                LiveStartPeek::Retryable(_)
            ),
            "first-event 429 must retry on this request instead of reaching grok-build"
        );
    }

    #[tokio::test]
    async fn peek_policy_429_is_retryable_for_account_failover() {
        // Routed as Retryable so the start loop can swap to newly stored
        // credentials after a hot account switch; without a switch the loop
        // passes the 429 through verbatim instead of retrying the same login.
        let (tx, rx) = mpsc::channel(2);
        tx.send(Err(
            "Connect error 429: ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded. [resource_exhausted]"
                .into(),
        ))
        .await
        .unwrap();
        drop(tx);
        assert!(matches!(
            peek_live_start_for_stale_reset(rx).await,
            LiveStartPeek::Retryable(_)
        ));
    }

    #[tokio::test]
    async fn peek_empty_turn_defers_to_the_dedicated_late_retry_policy() {
        let error = "Cursor upstream finished this turn without text or tool calls; retry this turn \
                     (stale Cursor conversation reset; retry this message to continue)";
        let (tx, rx) = mpsc::channel(2);
        tx.send(Err(error.into())).await.unwrap();
        drop(tx);
        let LiveStartPeek::Ready { mut events, .. } = peek_live_start_for_stale_reset(rx).await
        else {
            panic!("empty turns must not consume the generic start retry budget");
        };
        assert_eq!(events.recv().await.unwrap().unwrap_err(), error);
    }

    #[tokio::test]
    async fn peek_metadata_and_thinking_are_not_policy_health_evidence() {
        for event in [
            LiveRunEvent::Cursor(CursorStreamEvent::Session {
                session_id: "metadata-only".into(),
            }),
            LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta {
                text: "speculative reasoning".into(),
            }),
            LiveRunEvent::NativeToolBatch(Vec::new()),
        ] {
            let (tx, rx) = mpsc::channel(2);
            tx.send(Ok(event)).await.unwrap();
            drop(tx);
            let LiveStartPeek::Ready {
                observed_healthy_event,
                ..
            } = peek_live_start_for_stale_reset(rx).await
            else {
                panic!("metadata must stay on the attached live stream");
            };
            assert!(
                !observed_healthy_event,
                "metadata/thinking must not release a cold policy probe"
            );
        }

        let (tx, rx) = mpsc::channel(2);
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: "accepted".into(),
        })))
        .await
        .unwrap();
        drop(tx);
        let LiveStartPeek::Ready {
            observed_healthy_event,
            ..
        } = peek_live_start_for_stale_reset(rx).await
        else {
            panic!("model output must stay on the attached live stream");
        };
        assert!(observed_healthy_event, "model text is health evidence");
    }

    #[test]
    fn empty_turn_retry_budget_is_separate_from_transport_retries() {
        let policy = LiveLateRetryPolicy::default();
        assert_eq!(
            live_late_retry_limit(
                "Cursor upstream finished this turn without text or tool calls; retry this turn",
                policy
            ),
            1
        );
        assert_eq!(
            live_late_retry_limit(
                "Connect error 502: ERROR_OPENAI: Unable to reach the model provider [unavailable]",
                policy
            ),
            crate::retry::MAX_RATE_LIMIT_RETRIES
        );
    }

    #[test]
    fn gemini_gets_a_larger_bounded_empty_turn_budget() {
        assert_eq!(
            LiveLateRetryPolicy::for_request_with_override("gemini-3.6-flash", "sand", None,)
                .empty_turn_max_retries,
            3
        );
        assert_eq!(
            LiveLateRetryPolicy::for_request_with_override("gemini-3.6-flash", "cli", None,)
                .empty_turn_max_retries,
            3
        );
        assert_eq!(
            LiveLateRetryPolicy::for_request_with_override("claude-fable-5", "sand", None,)
                .empty_turn_max_retries,
            1
        );
        assert_eq!(
            LiveLateRetryPolicy::for_request_with_override("gemini-3.6-flash", "sand", Some("99"),)
                .empty_turn_max_retries,
            LIVE_EMPTY_TURN_MAX_RETRIES_LIMIT,
            "operator overrides stay bounded"
        );
    }

    #[test]
    fn empty_turn_retry_resets_only_replay_safe_conversations() {
        let _guard = conversation::STORE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        conversation::reset_for_test();
        let session = "sand-gemini-fresh-conversation-retry";
        let original = conversation::get_or_create(session).conversation_id;
        assert!(!prepare_live_retry_conversation(
            session,
            "Cursor upstream finished this turn without text or tool calls; retry this turn \
             (stale Cursor conversation reset; retry this message to continue)"
        ));
        let replacement = conversation::get_or_create(session).conversation_id;
        assert_eq!(original, replacement);

        assert!(prepare_live_retry_conversation(
            session,
            "Cursor upstream finished this turn without text or tool calls; retry this turn"
        ));
        let replacement = conversation::get_or_create(session).conversation_id;
        assert_ne!(original, replacement);

        assert!(!prepare_live_retry_conversation(
            session,
            "Cursor upstream finished this turn without text or tool calls; retry this turn \
             (completed tool results retained in Cursor checkpoint; continue without replaying tools)"
        ));
        assert_eq!(
            replacement,
            conversation::get_or_create(session).conversation_id,
            "checkpoint continuation must not replay completed tools on a new conversation"
        );
        conversation::reset_for_test();
    }

    #[test]
    fn nested_empty_turn_retry_resets_only_the_composite_agent_conversation() {
        let _guard = conversation::STORE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        conversation::reset_for_test();
        let session = "nested-empty-turn-session";
        let agent = "agent-child";
        let parent_key = live_retry_conversation_key(session, None);
        let nested_key = live_retry_conversation_key(session, Some(agent));
        assert_ne!(parent_key, nested_key);
        assert_eq!(
            nested_key,
            live_run_key_for(LiveRunIdentity {
                session_id: session,
                agent_id: Some(agent),
                parent_agent_id: Some("parent-agent"),
                account_key: None,
            })
        );
        let parent_before = conversation::get_or_create(&parent_key).conversation_id;
        let nested_before = conversation::get_or_create(&nested_key).conversation_id;
        conversation::save_checkpoint(&nested_key, vec![1, 2, 3]);

        assert!(prepare_live_retry_conversation(
            &nested_key,
            "Cursor upstream finished this turn without text or tool calls; retry this turn"
        ));
        assert_eq!(
            conversation::get_or_create(&parent_key).conversation_id,
            parent_before,
            "a nested retry must not reset its parent lane"
        );
        let nested_after = conversation::continuation_for(Some(&nested_key));
        assert_ne!(nested_after.conversation_id, Some(nested_before));
        assert!(!nested_after.has_checkpoint);
        conversation::reset_for_test();
    }

    #[test]
    fn tool_result_reset_retry_alone_retains_full_history_images() {
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "inspect this screenshot"},
                    {"type": "image", "source": {
                        "type": "base64", "media_type": "image/png", "data": "T0xESU1H"
                    }}
                ]},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "read-1", "name": "Read", "input": {"file_path": "/tmp/a"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "read-1", "content": "done"}
                ]}
            ]
        }))
        .unwrap();
        assert!(latest_user_is_only_tool_results(&body));
        let (checkpoint_images, reset_images) = live_request_image_sets(&body, true);
        assert!(
            checkpoint_images.is_empty(),
            "normal BiDi/checkpoint retries must not resubmit an old screenshot"
        );
        assert_eq!(
            reset_images
                .iter()
                .map(|image| image.data.as_str())
                .collect::<Vec<_>>(),
            ["T0xESU1H"]
        );
        assert!(live_retry_needs_fresh_history(
            "Cursor upstream finished this turn without text or tool calls; retry this turn \
             (stale Cursor conversation reset; retry this message to continue)"
        ));
        assert!(!live_retry_needs_fresh_history(
            "Cursor upstream finished this turn without text or tool calls; retry this turn \
             (completed tool results retained in Cursor checkpoint; continue without replaying tools)"
        ));
        assert!(!live_retry_needs_fresh_history(
            "Connect error 502: transient transport failure"
        ));
    }

    #[test]
    fn image_recovery_helper_preserves_detail_and_history_payload() {
        let error = CursorError::new(
            502,
            "Cursor upstream connect failed",
            Some("Image not found [internal]".into()),
        );
        assert!(cursor_error_is_missing_image(&error));

        let images = vec![CursorSelectedImage {
            data: "T0xESU1H".into(),
            uuid: "stale-image-id".into(),
            path: String::new(),
            mime_type: "image/png".into(),
        }];
        let refreshed = refresh_image_uuids(&images);
        assert_eq!(refreshed[0].data, images[0].data);
        assert_eq!(refreshed[0].mime_type, images[0].mime_type);
        assert_ne!(refreshed[0].uuid, images[0].uuid);
    }

    #[test]
    fn kv_recovery_reuses_image_wave_after_stale_image_recovery() {
        let original = vec![CursorSelectedImage {
            data: "aW1hZ2U=".into(),
            uuid: "old-image-id".into(),
            path: String::new(),
            mime_type: "image/png".into(),
        }];
        let refreshed = refresh_image_uuids(&original);
        let selected = kv_recovery_images(&refreshed, &original, true);
        assert_eq!(selected[0].uuid, refreshed[0].uuid);
        assert_eq!(selected[0].data, original[0].data);

        let first_kv_wave = kv_recovery_images(&original, &original, false);
        assert_ne!(first_kv_wave[0].uuid, original[0].uuid);
    }

    #[test]
    fn cached_image_recovery_wave_is_stable_across_late_kv_retry() {
        let original = vec![CursorSelectedImage {
            data: "aW1hZ2U=".into(),
            uuid: "stale-image-id".into(),
            path: String::new(),
            mime_type: "image/png".into(),
        }];
        let shared = Arc::new(Mutex::new(None));
        // Simulate the initial stale-image recovery minting a UUID wave.
        let first = cached_image_recovery_images(&shared, &original);
        // A later KV 413 must reuse that exact wave, not mint another UUID.
        let late = cached_image_recovery_images(&shared, &original);
        assert_eq!(late[0].uuid, first[0].uuid);
        assert_eq!(late[0].data, first[0].data);
        assert_ne!(first[0].uuid, original[0].uuid);
    }

    #[test]
    fn late_retry_binding_snapshot_advances_after_conversation_rotation() {
        let _guard = conversation::STORE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        conversation::reset_for_test();
        let session = format!("late-retry-binding-{}", uuid::Uuid::new_v4());
        let key = live_retry_conversation_key(&session, None);
        let original = conversation::get_or_create(&key).conversation_id;
        let retry = LiveRetryStart {
            client: CursorHttpClient::new(),
            effective_token: Arc::new(Mutex::new(String::new())),
            user_text: "delta".into(),
            reset_user_text: "full history".into(),
            expected_conversation_id: Arc::new(Mutex::new(Some(original.clone()))),
            model: "claude-fable-5".into(),
            images: Vec::new(),
            reset_retry_images: Vec::new(),
            image_recovery_attempted: Arc::new(AtomicBool::new(false)),
            image_recovery_images: Arc::new(Mutex::new(None)),
            kv_recovery_attempted: Arc::new(AtomicBool::new(false)),
            compaction_recovery_attempted: Arc::new(AtomicBool::new(false)),
            upstream_active_recovery_attempted: Arc::new(AtomicBool::new(false)),
            account_failover_state: Arc::new(Mutex::new(AccountFailoverState::default())),
            custom_system: None,
            session_id: session.clone(),
            agent_id: None,
            parent_agent_id: None,
            account_key: Arc::new(Mutex::new(String::new())),
            allowed: None,
            mcp_tools: None,
            request_context: crate::providers::cursor::proto::RequestContext::default(),
            fingerprint: Vec::new(),
            has_refresh: false,
            unbounded_conflict_wait: false,
            client_type: "sand".into(),
            request_sequence_id: "test-request-sequence".into(),
            compaction_mode: false,
        };

        conversation::reset(&key);
        let rotated = conversation::get_or_create(&key).conversation_id;
        assert_ne!(rotated, original);
        // This mirrors the successful recovery path: the next checkpoint
        // continuation must compare against the fresh binding, not the UUID
        // captured before KV/image rotation.
        *retry
            .expected_conversation_id
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(rotated.clone());
        assert_eq!(retry.expected_conversation_snapshot(), Some(rotated));
        conversation::reset_for_test();
    }

    #[test]
    fn transient_resource_exhaustion_gets_extended_retry_budget() {
        let transient = "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]";
        assert!(is_transient_resource_exhausted(transient));
        assert_eq!(cursor_transient_retry_limit(transient), 6);
        assert_eq!(
            live_late_retry_limit(transient, LiveLateRetryPolicy::default()),
            6
        );

        let ordinary =
            "Connect error 502: ERROR_OPENAI: Unable to reach the model provider [unavailable]";
        assert!(!is_transient_resource_exhausted(ordinary));
        assert_eq!(
            cursor_transient_retry_limit(ordinary),
            crate::retry::MAX_RATE_LIMIT_RETRIES
        );
        assert_eq!(
            live_late_retry_limit(ordinary, LiveLateRetryPolicy::default()),
            crate::retry::MAX_RATE_LIMIT_RETRIES
        );
    }

    #[test]
    fn policy_resource_exhaustion_never_uses_extended_retry_budget() {
        for message in [
            "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice [resource_exhausted]",
            "Connect error 429: ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded [resource_exhausted]",
            "Connect error 429: ERROR_RESOURCE_EXHAUSTED: High Load — please switch to another model [resource_exhausted]",
        ] {
            assert!(!is_transient_resource_exhausted(message), "{message}");
            assert_eq!(
                cursor_transient_retry_limit(message),
                crate::retry::MAX_RATE_LIMIT_RETRIES,
                "policy errors retain the normal retry budget"
            );
            assert_eq!(
                live_late_retry_limit(message, LiveLateRetryPolicy::default()),
                crate::retry::MAX_RATE_LIMIT_RETRIES
            );
        }
    }

    #[test]
    fn cursor_step_failure_gets_a_bounded_pre_output_retry_budget() {
        let transient = "Cursor error 502: Failed to run step, exceeded max retries [internal]";
        assert!(is_transient_step_failure(transient));
        assert_eq!(cursor_step_failure_retry_limit(transient), 4);
        assert_eq!(
            live_late_retry_limit(transient, LiveLateRetryPolicy::default()),
            4,
            "step exhaustion gets its own short retry budget"
        );
        assert!(!is_transient_step_failure(
            "Cursor error 502: Failed to run step, exceeded max retries; unpaid invoice"
        ));
        assert_eq!(
            cursor_step_failure_retry_limit("Cursor error 502: upstream unavailable"),
            crate::retry::MAX_RATE_LIMIT_RETRIES
        );
    }

    #[tokio::test]
    async fn pre_output_openai_502_is_retried_not_forwarded() {
        let provider_502 = "Connect error 502: ERROR_OPENAI: Unable to reach the model provider — We're having trouble connecting to the model provider. This might be temporary - please try again in a moment. [unavailable]";
        assert_eq!(
            classify_live_pump_item(false, &Err("Cursor live run cancelled".into())),
            LivePumpAction::Retry,
            "a resolved pre-output cancellation must restart the same SSE request"
        );
        assert_eq!(
            classify_live_pump_item(false, &Err(provider_502.into())),
            LivePumpAction::Retry
        );
        assert_eq!(
            classify_live_pump_item(
                false,
                &Ok(LiveRunEvent::Cursor(CursorStreamEvent::Session {
                    session_id: "s".into()
                }))
            ),
            LivePumpAction::Buffer
        );
        assert_eq!(
            classify_live_pump_item(
                false,
                &Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta {
                    text: "speculative reasoning".into(),
                }))
            ),
            LivePumpAction::Buffer,
            "thinking-only output must remain retryable until text/tool output commits"
        );
        assert_eq!(
            classify_live_pump_item(
                false,
                &Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
                    text: String::new(),
                }))
            ),
            LivePumpAction::Buffer,
            "an empty text delta must not commit output before a stall"
        );

        let (src_tx, src_rx) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        src_tx
            .send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::Session {
                session_id: "s".into(),
            })))
            .await
            .unwrap();
        src_tx.send(Err(provider_502.into())).await.unwrap();
        drop(src_tx);
        let outcome = pump_live_events_until_commit_or_retry(&out_tx, src_rx).await;
        assert!(
            matches!(outcome, LivePumpOutcome::Retry(ref error) if error.contains("Unable to reach the model provider")),
            "{outcome:?}"
        );
        assert!(
            out_rx.try_recv().is_err(),
            "session + provider 502 must not reach grok-build"
        );
    }

    #[tokio::test]
    async fn metadata_only_eof_is_retried_without_leaking_a_502() {
        let (src_tx, src_rx) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        src_tx
            .send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::Session {
                session_id: "s".into(),
            })))
            .await
            .unwrap();
        src_tx
            .send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta {
                text: "speculative reasoning".into(),
            })))
            .await
            .unwrap();
        drop(src_tx);

        let outcome = pump_live_events_until_commit_or_retry(&out_tx, src_rx).await;
        let LivePumpOutcome::Retry(error) = outcome else {
            panic!("metadata-only EOF must enter the hollow-turn retry path: {outcome:?}");
        };
        assert!(live_error_is_empty_turn_retry(&error), "{error}");
        assert!(
            live_error_is_same_request_retryable(&error),
            "the outer retry pump must recognize the EOF marker: {error}"
        );
        assert!(
            out_rx.try_recv().is_err(),
            "speculative metadata must not be exposed before a successful retry"
        );
    }

    #[tokio::test]
    async fn held_http_empty_turn_retries_without_forwarding_failure() {
        let (first_tx, first_rx) = mpsc::channel(8);
        first_tx
            .send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta {
                text: "first attempt".into(),
            })))
            .await
            .unwrap();
        first_tx
            .send(Err(
                "Cursor upstream finished this turn without text or tool calls; retry this turn \
                 (stale Cursor conversation reset; retry this message to continue)"
                    .into(),
            ))
            .await
            .unwrap();
        drop(first_tx);

        let restarts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restart_count = Arc::clone(&restarts);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        forward_live_events_with_retries(
            &out_tx,
            first_rx,
            "sess-held-http-empty-retry",
            None,
            move |_| {
                restart_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (retry_tx, retry_rx) = mpsc::channel(8);
                retry_tx
                    .try_send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
                        text: "recovered".into(),
                    })))
                    .unwrap();
                retry_tx
                    .try_send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::End)))
                    .unwrap();
                drop(retry_tx);
                std::future::ready(Ok::<_, CursorError>(retry_rx))
            },
            LiveLateRetryPolicy::default(),
        )
        .await;

        assert_eq!(restarts.load(std::sync::atomic::Ordering::SeqCst), 1);
        let mut saw_recovered = false;
        while let Ok(item) = out_rx.try_recv() {
            match item {
                // Thinking is speculative and intentionally buffered until a
                // committed answer/tool event. The empty-turn retry therefore
                // drops it rather than exposing stale reasoning from the
                // failed generation.
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta { .. })) => {}
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) => {
                    saw_recovered |= text == "recovered";
                }
                Err(error) => panic!("internal retry leaked to the client: {error}"),
                _ => {}
            }
        }
        assert!(saw_recovered);
    }

    #[tokio::test]
    async fn heartbeat_stall_after_thinking_retries_without_leaking_error() {
        let (first_tx, first_rx) = mpsc::channel(8);
        first_tx
            .send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta {
                text: "speculative reasoning".into(),
            })))
            .await
            .unwrap();
        first_tx
            .send(Err("Cursor recovery exhausted without producing output \
                 (stale Cursor conversation reset; retry this message to continue)"
                .into()))
            .await
            .unwrap();
        drop(first_tx);

        let restarts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restart_count = Arc::clone(&restarts);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        forward_live_events_with_retries(
            &out_tx,
            first_rx,
            "sess-thinking-stall-retry",
            None,
            move |_| {
                restart_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (retry_tx, retry_rx) = mpsc::channel(8);
                retry_tx
                    .try_send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
                        text: "recovered".into(),
                    })))
                    .unwrap();
                retry_tx
                    .try_send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::End)))
                    .unwrap();
                drop(retry_tx);
                std::future::ready(Ok::<_, CursorError>(retry_rx))
            },
            LiveLateRetryPolicy::default(),
        )
        .await;

        assert_eq!(restarts.load(std::sync::atomic::Ordering::SeqCst), 1);
        let mut saw_recovered = false;
        while let Ok(item) = out_rx.try_recv() {
            match item {
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) => {
                    saw_recovered |= text == "recovered";
                }
                Err(error) => panic!("internal heartbeat-stall error leaked: {error}"),
                _ => {}
            }
        }
        assert!(
            saw_recovered,
            "retry output should reach the downstream client"
        );
    }

    #[test]
    fn empty_after_tool_results_retries_with_checkpoint_nudge() {
        let original = "<tool_result tool_use_id=\"read-1\">done</tool_result>";
        let checkpoint_error = "Cursor upstream finished this turn without text or tool calls; retry this turn \
             (completed tool results retained in Cursor checkpoint; continue without replaying tools)";
        assert_eq!(
            live_retry_user_text(original, checkpoint_error),
            EMPTY_TURN_CHECKPOINT_CONTINUE_PROMPT
        );
        assert_eq!(
            live_retry_user_text(
                original,
                "Cursor upstream finished this turn without text or tool calls; retry this turn \
                 (stale Cursor conversation reset; retry this message to continue)"
            ),
            original,
            "a reset conversation still needs the full original retry payload"
        );
    }

    #[tokio::test]
    async fn held_http_empty_turn_gets_one_internal_retry_then_fails() {
        let error = "Cursor upstream finished this turn without text or tool calls; retry this turn \
                     (stale Cursor conversation reset; retry this message to continue)";
        let (first_tx, first_rx) = mpsc::channel(1);
        first_tx.send(Err(error.into())).await.unwrap();
        drop(first_tx);

        let restarts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restart_count = Arc::clone(&restarts);
        let (out_tx, mut out_rx) = mpsc::channel(2);
        forward_live_events_with_retries(
            &out_tx,
            first_rx,
            "sess-held-http-empty-exhausted",
            None,
            move |_| {
                restart_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (retry_tx, retry_rx) = mpsc::channel(1);
                retry_tx.try_send(Err(error.into())).unwrap();
                drop(retry_tx);
                std::future::ready(Ok::<_, CursorError>(retry_rx))
            },
            LiveLateRetryPolicy::default(),
        )
        .await;

        assert_eq!(
            restarts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "empty turns must not reuse the three-attempt transport retry budget"
        );
        let final_error = out_rx
            .try_recv()
            .expect("one exhausted retry error")
            .expect_err("the retry budget must end as a failure");
        assert!(live_error_is_empty_turn_retry(&final_error));
        assert!(
            out_rx.try_recv().is_err(),
            "only the final failure may reach the client"
        );
    }

    #[tokio::test]
    async fn late_policy_error_uses_the_account_failover_restart_before_surface() {
        // A delayed policy frame arrives after the short live-open peek.  It
        // is still pre-output, so the coordinator must give the request's
        // restart closure one chance to move to another account instead of
        // immediately exposing a terminal 429 to Claude Code.
        let policy_error =
            "Connect error 429: ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded";
        let (first_tx, first_rx) = mpsc::channel(2);
        first_tx.send(Err(policy_error.into())).await.unwrap();
        drop(first_tx);

        let restarts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restart_count = Arc::clone(&restarts);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        forward_live_events_with_retries(
            &out_tx,
            first_rx,
            "sess-late-policy-failover",
            None,
            move |_| {
                restart_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (retry_tx, retry_rx) = mpsc::channel(2);
                retry_tx
                    .try_send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
                        text: "recovered on alternate account".into(),
                    })))
                    .unwrap();
                retry_tx
                    .try_send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::End)))
                    .unwrap();
                drop(retry_tx);
                std::future::ready(Ok::<_, CursorError>(retry_rx))
            },
            LiveLateRetryPolicy::default(),
        )
        .await;

        assert_eq!(restarts.load(std::sync::atomic::Ordering::SeqCst), 1);
        let mut recovered = false;
        while let Ok(item) = out_rx.try_recv() {
            match item {
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) => {
                    recovered |= text == "recovered on alternate account";
                }
                Err(error) => panic!("late policy error leaked after failover: {error}"),
                _ => {}
            }
        }
        assert!(recovered, "alternate-account output must reach the client");
    }

    #[tokio::test]
    async fn gemini_empty_turn_recovers_on_its_fourth_upstream_attempt() {
        let error = "Cursor upstream finished this turn without text or tool calls; retry this turn \
                     (stale Cursor conversation reset; retry this message to continue)";
        let (first_tx, first_rx) = mpsc::channel(1);
        first_tx.send(Err(error.into())).await.unwrap();
        drop(first_tx);

        let restarts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restart_count = Arc::clone(&restarts);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        forward_live_events_with_retries(
            &out_tx,
            first_rx,
            "sess-gemini-empty-fourth-success",
            None,
            move |_| {
                let attempt = restart_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let (retry_tx, retry_rx) = mpsc::channel(2);
                if attempt < 3 {
                    retry_tx.try_send(Err(error.into())).unwrap();
                } else {
                    retry_tx
                        .try_send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
                            text: "recovered after transient Gemini hollow turns".into(),
                        })))
                        .unwrap();
                    retry_tx
                        .try_send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::End)))
                        .unwrap();
                }
                drop(retry_tx);
                std::future::ready(Ok::<_, CursorError>(retry_rx))
            },
            LiveLateRetryPolicy::for_request_with_override("gemini-3.6-flash", "cli", None),
        )
        .await;

        assert_eq!(restarts.load(std::sync::atomic::Ordering::SeqCst), 3);
        let mut text = String::new();
        while let Ok(item) = out_rx.try_recv() {
            match item {
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text: delta })) => {
                    text.push_str(&delta);
                }
                Err(error) => panic!("internal retry leaked to downstream: {error}"),
                _ => {}
            }
        }
        assert_eq!(text, "recovered after transient Gemini hollow turns");
    }

    #[tokio::test]
    async fn missing_conversation_retains_the_transport_retry_budget() {
        let error = "Connect error 502: Conversation data missing \
                     (stale Cursor conversation reset; retry this message to continue)";
        let (first_tx, first_rx) = mpsc::channel(1);
        first_tx.send(Err(error.into())).await.unwrap();
        drop(first_tx);

        let restarts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restart_count = Arc::clone(&restarts);
        let (out_tx, mut out_rx) = mpsc::channel(2);
        forward_live_events_with_retries(
            &out_tx,
            first_rx,
            "sess-missing-conversation-budget",
            None,
            move |_| {
                restart_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (retry_tx, retry_rx) = mpsc::channel(1);
                retry_tx.try_send(Err(error.into())).unwrap();
                drop(retry_tx);
                std::future::ready(Ok::<_, CursorError>(retry_rx))
            },
            LiveLateRetryPolicy::default(),
        )
        .await;

        assert_eq!(
            restarts.load(std::sync::atomic::Ordering::SeqCst),
            crate::retry::MAX_RATE_LIMIT_RETRIES as usize
        );
        assert!(out_rx.try_recv().unwrap().is_err());
    }

    #[tokio::test]
    async fn empty_turn_retry_respects_one_cross_attempt_deadline() {
        let error = "Cursor upstream finished this turn without text or tool calls; retry this turn \
                     (stale Cursor conversation reset; retry this message to continue)";
        let (first_tx, first_rx) = mpsc::channel(1);
        first_tx.send(Err(error.into())).await.unwrap();
        drop(first_tx);

        let held_senders = Arc::new(Mutex::new(Vec::new()));
        let restart_senders = Arc::clone(&held_senders);
        let restarts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let restart_count = Arc::clone(&restarts);
        let (out_tx, mut out_rx) = mpsc::channel(2);
        let started = Instant::now();
        forward_live_events_with_retries(
            &out_tx,
            first_rx,
            "sess-empty-turn-episode-deadline",
            None,
            move |_| {
                restart_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let (retry_tx, retry_rx) = mpsc::channel(1);
                restart_senders
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(retry_tx);
                std::future::ready(Ok::<_, CursorError>(retry_rx))
            },
            LiveLateRetryPolicy {
                empty_turn_episode: Duration::from_millis(50),
                ..LiveLateRetryPolicy::default()
            },
        )
        .await;

        assert_eq!(restarts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "the retry inherited a fresh long idle window"
        );
        let final_error = out_rx
            .try_recv()
            .expect("deadline failure")
            .expect_err("the hollow retry must fail");
        assert!(
            final_error.contains("empty-turn recovery deadline exhausted"),
            "{final_error}"
        );
    }

    #[tokio::test]
    async fn committed_or_policy_live_error_is_not_retried() {
        let billing = "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — pay your invoice in Stripe [resource_exhausted]";
        let empty_turn =
            "Cursor upstream finished this turn without text or tool calls; retry this turn";
        let step_failure = "Cursor error 502: Failed to run step, exceeded max retries [internal]";
        assert_eq!(
            classify_live_pump_item(false, &Err(billing.into())),
            LivePumpAction::PolicyLimit,
            "pre-output policy failures must open the local breaker, not restart upstream"
        );
        assert_eq!(
            classify_live_pump_item(
                true,
                &Err(
                    "Connect error 502: ERROR_OPENAI: Unable to reach the model provider [unavailable]"
                        .into()
                )
            ),
            LivePumpAction::Forward
        );
        assert_eq!(
            classify_live_pump_item(true, &Err(empty_turn.into())),
            LivePumpAction::Retry,
            "thinking-only output must not expose a failed empty turn"
        );
        assert_eq!(
            classify_live_pump_item(false, &Err(step_failure.into())),
            LivePumpAction::Retry,
            "step exhaustion must retry before client-visible output"
        );
        assert_eq!(
            classify_live_pump_item(true, &Err(step_failure.into())),
            LivePumpAction::Forward,
            "step exhaustion must not replay after output commits"
        );

        let (src_tx, src_rx) = mpsc::channel(8);
        let (out_tx, mut out_rx) = mpsc::channel(8);
        src_tx
            .send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
                text: "hello".into(),
            })))
            .await
            .unwrap();
        src_tx
            .send(Err(
                "Connect error 502: ERROR_OPENAI: Unable to reach the model provider [unavailable]"
                    .into(),
            ))
            .await
            .unwrap();
        drop(src_tx);
        let outcome = pump_live_events_until_commit_or_retry(&out_tx, src_rx).await;
        assert!(matches!(outcome, LivePumpOutcome::Done), "{outcome:?}");
        let Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) =
            out_rx.recv().await.unwrap()
        else {
            panic!("text must still be forwarded");
        };
        assert_eq!(text, "hello");
        let Err(error) = out_rx.recv().await.unwrap() else {
            panic!("post-output 502 must still be forwarded");
        };
        assert!(
            error.contains("Unable to reach the model provider"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn peek_unpaid_invoice_routes_to_the_policy_failover_arm() {
        // Billing blocks are policy rate limits: peek hands them to the start
        // loop, which fails over to newly stored credentials after an account
        // switch or otherwise returns the verbatim error as a pre-commit 429
        // (never a hidden same-login retry).
        let (tx, rx) = mpsc::channel(2);
        let text = "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — pay your invoice in Stripe [resource_exhausted]";
        tx.send(Err(text.into())).await.unwrap();
        drop(tx);
        let LiveStartPeek::Retryable(error) = peek_live_start_for_stale_reset(rx).await else {
            panic!("billing 429 must reach the policy failover arm");
        };
        assert!(error.contains("unpaid invoice"), "{error}");
        assert_eq!(
            crate::retry::classify_proxy_error_status(502, &error),
            429,
            "pass-through must surface as HTTP 429"
        );
    }

    #[test]
    fn unpaid_invoice_connect_error_is_http_429_not_502() {
        let err = client::CursorError::internal(
            "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — Your team has an unpaid invoice. Please contact your team administrator to pay your invoice and continue using Cursor. [resource_exhausted]",
        );
        assert_eq!(err.status, 502, "internal() still records a gateway status");
        let response = map_cursor_error_to_response(&err);
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "grok-build maps 500/502 to 'our side'; Cursor billing 429 must stay 429"
        );
    }

    #[test]
    fn geo_restriction_connect_error_is_http_403_not_502() {
        let err = client::CursorError::internal(
            "Connect error 502: ERROR_OPENAI: This model is not available in your country or region [internal]",
        );
        assert_eq!(err.status, 502);
        let response = map_cursor_error_to_response(&err);
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "geo blocks must not become grok-build 'our side' 500/502"
        );
    }

    #[test]
    fn live_open_timeout_is_http_409_not_502() {
        let err = client::CursorError::new(504, "Cursor live open timed out after 20s", None);
        let response = map_cursor_error_to_response(&err);
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "ambiguous live open must fail closed as 409 so grok-build does not 5xx-retry"
        );
    }

    #[test]
    fn heartbeat_live_ambiguous_completion_is_http_409_not_502() {
        let response = json_error_from_cursor_message(
            "Cursor stream produced no useful progress; upstream transport remained live, so completion is ambiguous",
        );
        assert_eq!(
            response.status(),
            StatusCode::CONFLICT,
            "the first terminal response must agree with the ambiguous live-run tombstone"
        );
    }

    #[tokio::test]
    async fn bidiappend_connect_failure_is_http_502_not_409() {
        let err = client::CursorError::new(
            502,
            "Cursor BidiAppend send failed; acceptance is ambiguous: Cursor upstream connect failed",
            Some(
                "error sending request for url (https://api2.cursor.sh/aiserver.v1.BidiService/BidiAppend)"
                    .into(),
            ),
        );
        let response = map_cursor_error_to_response(&err);
        assert_eq!(
            response.status(),
            StatusCode::BAD_GATEWAY,
            "a refused BidiAppend must be retryable 502, not grok invalid_request 409"
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let message = body["error"]["message"].as_str().unwrap_or("");
        assert!(
            message.contains("connect failed"),
            "do not leak raw reqwest URL as the 409 body: {message}"
        );
        assert!(
            !message.contains("api2.cursor.sh"),
            "reqwest URL detail must not become the user-facing error: {message}"
        );
    }

    #[test]
    fn classified_451_is_not_rewritten_to_502() {
        let err = client::CursorError::new(
            502,
            "Connect error 451: unavailable for legal reasons",
            None,
        );
        let response = map_cursor_error_to_response(&err);
        assert_eq!(response.status().as_u16(), 451);
    }

    #[tokio::test]
    async fn http_429_response_uses_upstream_detail_and_retry_after() {
        let mut err = client::CursorError::new(
            429,
            "Cursor upstream HTTP 429",
            Some(
                "You have an unpaid invoice — Visit cursor.com/dashboard and pay your invoice"
                    .into(),
            ),
        );
        err.retry_after = Some("9".into());
        let response = map_cursor_error_to_response(&err);
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("9")
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("unpaid invoice"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn local_503_overload_preserves_status_and_retry_after() {
        let mut err =
            client::CursorError::new(503, "Cursor live generation concurrency saturated", None);
        err.retry_after = Some("2".into());

        let response = map_cursor_error_to_response(&err);

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("2")
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "api_error");
    }

    #[tokio::test]
    async fn sand_open_timeout_is_retryable_503_not_502() {
        let err = client::CursorError::new(504, "Sand inference open timed out after 90s", None);
        assert!(sand_open_failure_is_retryable(&err));
        let response = map_cursor_error_to_response(&err);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "api_error");
    }

    #[test]
    fn sand_open_policy_429_does_not_backoff_global_launch_window() {
        // These errors are account/model policy outcomes. The caller's
        // account-failover path handles them, and the cold-open scheduler must
        // leave capacity for healthy Sand accounts unchanged.
        let quota = client::CursorError::new(
            429,
            "Connect error 429: ERROR_GPT_4_VISION_PREVIEW_RATE_LIMIT: You are out of usage",
            Some("rateLimitReason=sand_included_limit isRetryable=false".into()),
        );
        let provider_quota = client::CursorError::new(
            429,
            "Connect error 429: ERROR_PROVIDER_ERROR providerStatusCode=400 isRetryable=false",
            Some("resource_exhausted".into()),
        );
        let capacity_shed = client::CursorError::new(
            429,
            "Connect error 429: ERROR_RESOURCE_EXHAUSTED: High Load - switch to another model",
            None,
        );

        assert!(!sand_open_failure_is_retryable(&quota));
        assert!(!sand_open_failure_is_retryable(&provider_quota));
        assert!(!sand_open_failure_is_retryable(&capacity_shed));
    }

    #[test]
    fn sand_open_transient_429_still_backoffs_and_retries() {
        let transient = client::CursorError::new(
            429,
            "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider",
            Some("[resource_exhausted]".into()),
        );
        assert!(sand_open_failure_is_retryable(&transient));
    }

    fn decode_connect_error(status: u16, message: &str) -> CursorDecodeError {
        CursorDecodeError::ConnectEnd(crate::providers::cursor::connect::ConnectEndError {
            code: "upstream_error".into(),
            message: message.into(),
            detail: format!("{{\"error\":{{\"message\":{message:?}}}}}"),
            status,
            provider_error_code: None,
            provider_status_code: None,
            provider_is_retryable: None,
            provider_rate_limit_reason: None,
            provider_next_reset_at: None,
        })
    }

    #[tokio::test]
    async fn decoded_connect_503_is_service_unavailable() {
        let response = map_cursor_decode_error_to_response(&decode_connect_error(
            503,
            "upstream unavailable while opening Cursor run",
        ));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    #[tokio::test]
    async fn decoded_connect_504_is_normalized_to_retryable_503() {
        let response = map_cursor_decode_error_to_response(&decode_connect_error(
            504,
            "Cursor upstream timed out while opening run",
        ));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
    }

    #[test]
    fn decoded_connect_client_statuses_are_not_collapsed_to_502() {
        for (status, expected) in [(400, 400), (404, 404), (409, 409)] {
            let response = map_cursor_decode_error_to_response(&decode_connect_error(
                status,
                "Cursor request rejected",
            ));
            assert_eq!(response.status().as_u16(), expected, "status={status}");
        }
    }

    #[tokio::test]
    async fn undecorated_decode_rate_limit_message_gets_retry_after() {
        let error = CursorDecodeError::Decode(
            "Connect error 429: temporary capacity exhaustion [resource_exhausted]".into(),
        );
        let response = map_cursor_decode_error_to_response(&error);
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("5")
        );
    }

    #[test]
    fn streaming_live_commits_sse_before_start_live() {
        assert!(
            commit_streaming_live_sse_before_start_live(true, false, true),
            "Claude Code must get message_start before start_live / peek / retry"
        );
        assert!(
            commit_streaming_live_sse_before_start_live(true, true, true),
            "/v1/responses fresh streams must emit heartbeats while waiting behind a live run"
        );
        assert!(
            !commit_streaming_live_sse_before_start_live(false, false, false),
            "non-streaming JSON collection still waits for the live run"
        );
        assert!(
            !commit_streaming_live_sse_before_start_live(true, true, false),
            "Responses tool-result continuations retain JSON-before-open semantics"
        );
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn pre_response_live_waits_stay_below_client_idle_watchdog() {
        // These waits run before `live_sse_response` can emit its heartbeat.
        // Keep every default and environment clamp within the 10s Claude Code
        // event-watchdog budget, with headroom for scheduling and serialization.
        const CLAUDE_CODE_EVENT_WATCHDOG_MS: u64 = 10_000;
        assert!(LIVE_RESUME_WAIT_DEFAULT_MS < CLAUDE_CODE_EVENT_WATCHDOG_MS);
        assert!(LIVE_RESUME_WAIT_MAX_MS < CLAUDE_CODE_EVENT_WATCHDOG_MS);
        assert!(LIVE_NESTED_WAIT_DEFAULT_MS < CLAUDE_CODE_EVENT_WATCHDOG_MS);
        assert!(LIVE_NESTED_WAIT_MAX_MS < CLAUDE_CODE_EVENT_WATCHDOG_MS);
        assert!(LIVE_RESUME_ATTACH_WAIT_DEFAULT_MS < CLAUDE_CODE_EVENT_WATCHDOG_MS);
        assert!(LIVE_RESUME_ATTACH_WAIT_MAX_MS < CLAUDE_CODE_EVENT_WATCHDOG_MS);
    }

    #[test]
    fn fresh_stream_admission_is_deferred_until_sse_is_live() {
        assert!(defer_fresh_stream_admission(true, false, false));
        assert!(
            !defer_fresh_stream_admission(true, false, true),
            "tool-result continuations need the pre-response resume waiter"
        );
        assert!(
            !defer_fresh_stream_admission(true, true, false),
            "Responses callers must retain JSON-before-open semantics"
        );
        assert!(!defer_fresh_stream_admission(false, false, false));
    }

    #[test]
    fn every_fresh_request_without_tool_results_skips_short_nested_probe() {
        // This includes `stream:false` Responses calls. They cannot emit a
        // heartbeat before the response body exists, so the 1.5s nested
        // probe would turn a healthy queue behind a long Grok generation into
        // repeated local 503s.
        assert!(fresh_stream_can_skip_resume_probe(false, false));
        assert!(fresh_stream_can_skip_resume_probe(true, false));
        assert!(!fresh_stream_can_skip_resume_probe(false, true));
        assert!(!fresh_stream_can_skip_resume_probe(true, true));
    }

    #[test]
    fn start_wait_helper_drops_capacity_while_slot_is_starting() {
        let session = format!("admission-wait-{}", uuid::Uuid::new_v4());
        assert!(!live_start_should_wait_without_admission(&session, None, 1));
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve starting slot");
        assert!(live_start_should_wait_without_admission(&session, None, 1));
        reservation.release();
        assert!(!live_start_should_wait_without_admission(&session, None, 1));
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn active_run_conflict_budget_covers_long_fable_turns() {
        assert!(LIVE_CONFLICT_WAIT_DEFAULT_MS >= 120_000);
        assert!(LIVE_CONFLICT_WAIT_MAX_MS >= LIVE_CONFLICT_WAIT_DEFAULT_MS);
    }

    #[test]
    fn fresh_stream_conflict_wait_has_no_wall_clock_deadline() {
        assert!(
            live_conflict_wait_deadline(true).is_none(),
            "an already-committed SSE must keep waiting while the healthy prior Run advances"
        );
        let bounded = live_conflict_wait_deadline(false).expect("pre-response wait is bounded");
        assert!(bounded > Instant::now());
    }

    #[test]
    fn unbounded_conflict_wait_remains_active_until_registry_transition() {
        assert!(!conflict_wait_expired(None));
        assert!(conflict_wait_active(None));
        let past = Instant::now()
            .checked_sub(Duration::from_millis(1))
            .expect("instant subtraction");
        assert!(conflict_wait_expired(Some(past)));
        assert!(!conflict_wait_active(Some(past)));
    }

    #[tokio::test]
    async fn peek_useful_first_event_stays_on_the_same_run() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: "hello".into(),
        })))
        .await
        .unwrap();
        drop(tx);
        let LiveStartPeek::Ready { mut events, .. } = peek_live_start_for_stale_reset(rx).await
        else {
            panic!("a useful first event must keep the original live run");
        };
        let Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) =
            events.recv().await.unwrap()
        else {
            panic!("the peeked text delta must still be delivered");
        };
        assert_eq!(text, "hello");
    }

    #[test]
    fn cursor_cli_handler_is_available_without_touching_real_credentials() {
        let handler: &dyn CliHandlers = &CURSOR_CLI;
        let _ = handler;
    }

    fn hello_body() -> MessagesRequest {
        serde_json::from_value(serde_json::json!({
            "model": "cursor",
            "max_tokens": 128,
            "messages": [{"role": "user", "content": "hello"}]
        }))
        .unwrap()
    }

    #[test]
    fn grok_request_id_is_the_idempotency_boundary() {
        let body = hello_body();
        let mut tool_result_stage = hello_body();
        tool_result_stage.messages.push(
            serde_json::from_value(serde_json::json!({
                "role": "user",
                "content": [{"type": "tool_result", "tool_use_id": "tool-1", "content": "done"}]
            }))
            .unwrap(),
        );
        let retry_a = live_operation_fingerprint_payload(&body, Some("req-a"));
        let retry_a_again = live_operation_fingerprint_payload(&body, Some("req-a"));
        let distinct_turn = live_operation_fingerprint_payload(&body, Some("req-b"));
        assert_eq!(retry_a, retry_a_again);
        assert_ne!(
            retry_a,
            live_operation_fingerprint_payload(&tool_result_stage, Some("req-a")),
            "Grok reuses one request id across initial and tool-result sampling stages"
        );
        assert_ne!(
            retry_a, distinct_turn,
            "identical user text in a later Grok operation must start a new Run"
        );
        assert_ne!(
            retry_a,
            live_operation_fingerprint_payload(&body, None),
            "legacy body fingerprints remain a fallback only"
        );
    }

    #[test]
    fn legacy_fingerprint_preserves_ordinary_message_bytes() {
        // A live generation may have been admitted by an older proxy (or by
        // the preflight path immediately before a hot reload).  Without an
        // explicit request id, its operation key is the serialized message
        // history; scalar content and ordinary tool payloads must remain
        // byte-for-byte compatible so a waiter can still attach to it.
        let body: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-fable-5",
            "messages": [
                {"role": "user", "content": "same turn"},
                {"role": "assistant", "content": [{
                    "type": "tool_use",
                    "id": "tool-1",
                    "name": "Read",
                    "input": {"path": "src/lib.rs"}
                }]},
                {"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "content": "done"
                }]}
            ]
        }))
        .unwrap();
        assert_eq!(
            live_operation_fingerprint_payload(&body, None),
            serde_json::to_vec(&body.messages).unwrap()
        );
    }

    #[test]
    fn live_fingerprint_normalizes_retry_wire_noise_and_images() {
        let first: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "grok-build",
            "max_tokens": 256,
            "stream": true,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "inspect this screenshot"},
                    {"type": "image", "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": "iVBORw0KGgoAAAANSUhEUgAAAAE="
                    }}
                ]
            }],
            "metadata": {"trace_id": "attempt-a", "user_id": "stable-user"},
            "request_id": "attempt-a",
        }))
        .unwrap();
        let retry: MessagesRequest = serde_json::from_value(serde_json::json!({
            "request_id": "attempt-b",
            "metadata": {"user_id": "stable-user", "traceId": "attempt-b"},
            "messages": [{
                "content": [
                    {"text": "inspect this screenshot", "type": "text"},
                    {"source": {"data": "<stripped>", "media_type": "image/png"}, "type": "input_image"}
                ],
                "role": "user"
            }],
            "stream": false,
            "max_tokens": 256,
            "model": "grok-build",
        }))
        .unwrap();

        assert_eq!(
            live_operation_fingerprint_payload(&first, Some(" req-image ")),
            live_operation_fingerprint_payload(&retry, Some("req-image")),
            "retries may reorder keys, switch stream mode, and strip/re-encode image bytes"
        );
    }

    #[test]
    fn live_fingerprint_keeps_tool_result_stage_distinct_after_normalization() {
        let initial: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "grok-build",
            "messages": [{"role": "user", "content": "run the tool"}],
        }))
        .unwrap();
        let mut resumed = initial.clone();
        resumed.messages.push(
            serde_json::from_value(serde_json::json!({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "tool-1",
                    "content": "done"
                }]
            }))
            .unwrap(),
        );
        assert_ne!(
            live_operation_fingerprint_payload(&initial, Some("req-stage")),
            live_operation_fingerprint_payload(&resumed, Some("req-stage")),
            "adding a tool result is a new sampling stage even when request id is reused"
        );
    }

    #[test]
    fn live_fingerprint_does_not_collapse_tool_input_image_url_fields() {
        let first: MessagesRequest = serde_json::from_value(serde_json::json!({
            "messages": [{"role": "assistant", "content": [{
                "type": "tool_use",
                "id": "tool-1",
                "name": "inspect",
                "input": {"image_url": "https://one.example/image.png"}
            }]}]
        }))
        .unwrap();
        let second: MessagesRequest = serde_json::from_value(serde_json::json!({
            "messages": [{"role": "assistant", "content": [{
                "type": "tool_use",
                "id": "tool-1",
                "name": "inspect",
                "input": {"image_url": "https://two.example/image.png"}
            }]}]
        }))
        .unwrap();
        assert_ne!(
            live_operation_fingerprint_payload(&first, None),
            live_operation_fingerprint_payload(&second, None),
            "tool arguments remain semantic even when a key is named image_url"
        );
    }

    #[test]
    fn live_fingerprint_fallback_ignores_image_encoding_but_preserves_image_count() {
        let one_image: MessagesRequest = serde_json::from_value(serde_json::json!({
            "messages": [{"role": "user", "content": [{
                "type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}
            }]}]
        }))
        .unwrap();
        let one_image_retry: MessagesRequest = serde_json::from_value(serde_json::json!({
            "messages": [{"content": [{
                "image_url": {"url": "<omitted>"}, "type": "image"
            }], "role": "user"}]
        }))
        .unwrap();
        let two_images: MessagesRequest = serde_json::from_value(serde_json::json!({
            "messages": [{"role": "user", "content": [
                {"type": "image", "source": {"data": "AAAA"}},
                {"type": "image", "source": {"data": "BBBB"}}
            ]}]
        }))
        .unwrap();
        assert_eq!(
            live_operation_fingerprint_payload(&one_image, None),
            live_operation_fingerprint_payload(&one_image_retry, None)
        );
        assert_ne!(
            live_operation_fingerprint_payload(&one_image, None),
            live_operation_fingerprint_payload(&two_images, None),
            "image count/order remains part of the operation stage"
        );
    }

    #[test]
    fn count_tokens_uses_current_request_body_not_previous_turn() {
        let _guard = SESSION_USAGE_TEST_LOCK.lock().unwrap();
        reset_session_usage_for_test();
        record_session_input_tokens("sess-count-last", 53_037);
        let body = hello_body();
        let expected = (render_cursor_prompt(&body).len() / 4).max(1) as u64;
        let tokens = count_tokens_for_request(Some("sess-count-last"), &body);
        assert_eq!(tokens, expected);
        assert_ne!(tokens, 53_037);
    }

    #[test]
    fn count_tokens_estimates_rendered_prompt_when_session_has_no_usage() {
        let _guard = SESSION_USAGE_TEST_LOCK.lock().unwrap();
        reset_session_usage_for_test();
        let body = hello_body();
        let expected = (render_cursor_prompt(&body).len() / 4).max(1) as u64;
        let tokens = count_tokens_for_request(Some("sess-count-fresh"), &body);
        assert_eq!(tokens, expected);
        assert!(tokens >= 1);
    }

    #[test]
    fn count_tokens_does_not_leak_usage_across_sessions() {
        let _guard = SESSION_USAGE_TEST_LOCK.lock().unwrap();
        reset_session_usage_for_test();
        record_session_input_tokens("sess-other", 99_000);
        let body = hello_body();
        let expected = (render_cursor_prompt(&body).len() / 4).max(1) as u64;
        assert_eq!(
            count_tokens_for_request(Some("sess-count-isolated"), &body),
            expected
        );
    }

    #[test]
    fn nested_agent_headers_keep_parent_session_id_for_live_start() {
        let ctx = RequestContext {
            req_id: "req".into(),
            client_request_id: None,
            stainless_helper: None,
            session_id: Some("parent-session".into()),
            session_seq: None,
            provider: "cursor".into(),
            traffic: None,
            monitor: None,
            claude_code: crate::provider::ClaudeCodeAgentHeaders {
                agent_id: Some("agent%2Fchild".into()),
                parent_agent_id: Some("agent%2Fparent".into()),
                app: Some("cli-bg".into()),
            },
            hold_http_until_live_open: false,
        };
        let identity = live_run_identity("parent-session", &ctx);
        assert_eq!(identity.session_id, "parent-session");
        assert_eq!(identity.agent_id, Some("agent%2Fchild"));
        assert_eq!(identity.parent_agent_id, Some("agent%2Fparent"));
        assert!(identity.is_nested());
    }

    #[test]
    fn bridge_registry_key_isolated_for_nested_agent() {
        let parent = RequestContext {
            req_id: "req-parent".into(),
            client_request_id: None,
            stainless_helper: None,
            session_id: Some("shared-session".into()),
            session_seq: None,
            provider: "cursor".into(),
            traffic: None,
            monitor: None,
            claude_code: crate::provider::ClaudeCodeAgentHeaders::default(),
            hold_http_until_live_open: false,
        };
        let child = RequestContext {
            req_id: "req-child".into(),
            client_request_id: None,
            stainless_helper: None,
            session_id: Some("shared-session".into()),
            session_seq: None,
            provider: "cursor".into(),
            traffic: None,
            monitor: None,
            claude_code: crate::provider::ClaudeCodeAgentHeaders {
                agent_id: Some("agent%2Fchild".into()),
                parent_agent_id: Some("agent%2Fparent".into()),
                app: Some("cli-bg".into()),
            },
            hold_http_until_live_open: false,
        };

        let parent_key = bridge_registry_key(&parent).expect("parent session key");
        let child_key = bridge_registry_key(&child).expect("nested session key");
        assert_eq!(
            parent_key,
            live_run_key_for(live_run_identity("shared-session", &parent))
        );
        assert_eq!(
            child_key,
            live_run_key_for(live_run_identity("shared-session", &child))
        );
        assert_ne!(
            parent_key, child_key,
            "nested agents sharing Claude's session header need separate bridge state"
        );
    }

    #[test]
    fn nested_agent_prompt_continuation_ignores_parent_checkpoint() {
        let _store_guard = crate::providers::cursor::conversation::STORE_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let session = format!("parent-session-{}", uuid::Uuid::new_v4());
        // Conversations are keyed by the live run key, exactly as the live
        // driver persists checkpoints for the parent slot.
        let parent_key = crate::providers::cursor::live::live_run_key(&session, None);
        crate::providers::cursor::conversation::save_checkpoint(
            &parent_key,
            vec![0x0a, 0x02, 0x01, 0x02],
        );
        let nested = RequestContext {
            req_id: "req".into(),
            client_request_id: None,
            stainless_helper: None,
            session_id: Some(session.clone()),
            session_seq: None,
            provider: "cursor".into(),
            traffic: None,
            monitor: None,
            claude_code: crate::provider::ClaudeCodeAgentHeaders {
                agent_id: Some("agent%2Fchild".into()),
                parent_agent_id: Some("agent%2Fparent".into()),
                app: Some("cli-bg".into()),
            },
            hold_http_until_live_open: false,
        };
        let nested_cont = continuation_for_request(Some(&session), &nested);
        assert!(
            !nested_cont.has_checkpoint,
            "nested agent must not compact against the parent Cursor checkpoint"
        );

        let parent = RequestContext {
            req_id: "req".into(),
            client_request_id: None,
            stainless_helper: None,
            session_id: Some(session.clone()),
            session_seq: None,
            provider: "cursor".into(),
            traffic: None,
            monitor: None,
            claude_code: crate::provider::ClaudeCodeAgentHeaders {
                agent_id: None,
                parent_agent_id: None,
                app: Some("cli".into()),
            },
            hold_http_until_live_open: false,
        };
        let parent_cont = continuation_for_request(Some(&session), &parent);
        assert!(parent_cont.has_checkpoint);
        assert_eq!(parent_cont.conversation_state, vec![0x0a, 0x02, 0x01, 0x02]);
    }

    #[test]
    fn live_path_stays_eligible_when_claude_code_disables_stream() {
        assert!(live_path_eligible(false, true, true));
        assert!(live_path_eligible(true, true, true));
        assert!(!live_path_eligible(true, false, true));
        assert!(!live_path_eligible(true, true, false));
        assert_eq!(live_path_skip_reason(false, true, true), None);
        assert_eq!(live_path_skip_reason(true, false, true), Some("no_session"));
        assert_eq!(
            live_path_skip_reason(false, true, false),
            Some("bidi_disabled")
        );
        assert_ne!(
            live_path_skip_reason(false, true, true),
            Some("stream_false"),
            "Claude Code non-streaming fallback must still use live BiDi"
        );
    }

    #[tokio::test]
    async fn collect_live_events_to_json_text_end() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: "hi".into(),
        })))
        .await
        .unwrap();
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::End)))
            .await
            .unwrap();
        drop(tx);
        let json = collect_live_events_to_json(rx, "msg_live", "claude-fable-5", 3, false)
            .await
            .unwrap();
        assert_eq!(json["content"][0]["text"], "hi");
        assert_eq!(json["stop_reason"], "end_turn");
    }

    #[tokio::test]
    async fn collect_live_events_to_json_clean_text_eof_synthesizes_terminal() {
        // Grok/Gemini routes can close a clean live body immediately after
        // the final text delta. JSON has no wire End event to append, so the
        // collector must still return a normal end_turn message.
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: "clean EOF answer".into(),
        })))
        .await
        .unwrap();
        drop(tx);
        let json = collect_live_events_to_json(rx, "msg_clean_eof", "gemini-3.1-pro", 3, false)
            .await
            .expect("text-bearing clean EOF should be finalized locally");
        assert_eq!(json["content"][0]["text"], "clean EOF answer");
        assert_eq!(json["stop_reason"], "end_turn");
    }

    #[tokio::test]
    async fn collect_live_events_to_json_native_tool_is_terminal_without_end() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::NativeTool {
            tool_use_id: "tool-1".into(),
            name: "Read".into(),
            input: serde_json::json!({"file_path": "/tmp/a"}),
        })))
        .await
        .unwrap();
        drop(tx);
        let json = collect_live_events_to_json(rx, "msg_native_tool", "claude-fable-5", 3, false)
            .await
            .expect("native tool is an Anthropic terminal handoff");
        assert_eq!(json["content"][0]["type"], "tool_use");
        assert_eq!(json["stop_reason"], "tool_use");
    }

    #[tokio::test]
    async fn collect_live_events_to_json_stops_after_native_tool_handoff() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::NativeTool {
            tool_use_id: "tool-stop".into(),
            name: "Read".into(),
            input: serde_json::json!({"file_path": "/tmp/a"}),
        })))
        .await
        .unwrap();
        // A finalized tool handoff must not be invalidated by a late stream
        // error that belongs to the next Cursor segment.
        tx.send(Err("late transport diagnostic".into()))
            .await
            .unwrap();
        drop(tx);
        let json = collect_live_events_to_json(rx, "msg_native_stop", "claude-fable-5", 3, false)
            .await
            .expect("late post-handoff errors must not replace tool_use");
        assert_eq!(json["stop_reason"], "tool_use");
    }

    #[tokio::test]
    async fn completed_replay_adds_missing_text_terminal() {
        let events = Arc::new(vec![LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: "replayed answer".into(),
        })]);
        let mut rx = replay_completed_turn_channel("replay-terminal", &events);
        let first = rx.recv().await.expect("replayed text").expect("text event");
        assert!(matches!(
            first,
            LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { .. })
        ));
        let second = rx
            .recv()
            .await
            .expect("synthetic terminal")
            .expect("end event");
        assert!(matches!(
            second,
            LiveRunEvent::Cursor(CursorStreamEvent::End)
        ));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn completed_replay_does_not_invent_terminal_for_thinking_only() {
        let events = Arc::new(vec![LiveRunEvent::Cursor(
            CursorStreamEvent::ThinkingDelta {
                text: "private reasoning".into(),
            },
        )]);
        let mut rx = replay_completed_turn_channel("replay-hollow", &events);
        let item = rx
            .recv()
            .await
            .expect("replayed thinking")
            .expect("thinking event");
        assert!(matches!(
            item,
            LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta { .. })
        ));
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn collect_live_events_to_json_empty_is_error() {
        let (tx, rx) = mpsc::channel::<LiveEventResult>(1);
        drop(tx);
        let err = collect_live_events_to_json(rx, "msg_empty", "claude-fable-5", 1, false)
            .await
            .unwrap_err();
        assert!(err.contains("no useful progress"), "{err}");
    }

    #[tokio::test]
    async fn live_retry_coordinator_panic_is_explicit_retryable_event() {
        let (tx, mut rx) = mpsc::channel::<LiveEventResult>(1);
        let coordinator = async {
            std::panic::resume_unwind(Box::new("test coordinator panic"));
        };

        run_live_retry_coordinator_with_panic_guard(tx, "panic-session".into(), None, coordinator)
            .await;

        let item = rx
            .recv()
            .await
            .expect("coordinator panic must produce a terminal event");
        let Err(error) = item else {
            panic!("coordinator panic must be represented as an error: {item:?}");
        };
        assert!(live_error_is_empty_turn_retry(&error), "{error}");
        assert!(live_error_is_same_request_retryable(&error), "{error}");
        assert!(error.contains("coordinator panic"), "{error}");
        assert!(
            rx.recv().await.is_none(),
            "the explicit error must be followed by normal channel closure"
        );
    }

    #[tokio::test]
    async fn collect_live_events_to_json_thinking_only_eof_is_error() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta {
            text: "speculative".into(),
        })))
        .await
        .unwrap();
        drop(tx);
        let err = collect_live_events_to_json(rx, "msg_thinking_eof", "claude-fable-5", 3, true)
            .await
            .unwrap_err();
        assert!(err.contains("without turn_ended"), "{err}");
    }

    #[test]
    fn clean_json_eof_terminal_requires_visible_text() {
        assert!(clean_json_eof_can_synthesize_terminal(true));
        assert!(!clean_json_eof_can_synthesize_terminal(false));
    }

    #[test]
    fn count_tokens_ignores_zero_recorded_usage() {
        let _guard = SESSION_USAGE_TEST_LOCK.lock().unwrap();
        reset_session_usage_for_test();
        record_session_input_tokens("sess-zero", 0);
        let body = hello_body();
        let expected = (render_cursor_prompt(&body).len() / 4).max(1) as u64;
        assert_eq!(count_tokens_for_request(Some("sess-zero"), &body), expected);
    }
}
