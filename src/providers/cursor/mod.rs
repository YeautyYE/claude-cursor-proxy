pub mod auth;
pub mod client;
pub mod connect;
pub mod conversation;
pub mod exec_results;
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
pub mod sse;
#[cfg(test)]
pub(crate) mod test_frames;
pub mod tool_bridge;
pub mod tool_use_xml;

use async_trait::async_trait;
use axum::Json;
use axum::response::{IntoResponse, Response};
use http::StatusCode;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::anthropic::error::json_error;
use crate::anthropic::schema::{CountTokensResponse, MessagesRequest};
use crate::logging::create_logger;
use crate::monitor::usage_from_anthropic_sse;
use crate::provider::{CliHandlers, Provider, RequestContext};
use crate::providers::cursor::auth::{
    clear_cursor_auth, expired_auth_message, force_refresh_cursor_auth, load_cursor_auth,
    missing_auth_message, run_cursor_login,
};
use crate::providers::cursor::client::{CursorError, CursorHttpClient};
use crate::providers::cursor::exec_results::PendingCursorExec;
use crate::providers::cursor::hosted_web_search::{
    extract_web_search_query, hosted_web_search_json_response, hosted_web_search_sse_response,
    is_hosted_web_search_request, maybe_handle_hosted_web_fetch, search_web,
};
use crate::providers::cursor::live::{
    EMPTY_TURN_CHECKPOINT_CONTINUE_PROMPT, LIVE_AMBIGUOUS_OPEN_TTL, LIVE_H2_OPEN_ATTEMPT,
    LiveEventResult, LiveReplacementClaim, LiveRunEvent, LiveRunIdentity, LiveRunProbe,
    LiveRunRegistry, LiveRunReservation, LiveSlotClaim,
    cursor_start_error_is_same_request_retryable, exhausted_live_start_error,
    finish_replacement_after_cancel, live_error_is_empty_turn_retry,
    live_error_is_same_request_retryable, live_error_needs_checkpoint_continue,
    live_pending_must_supersede, live_probe_error_blocks_new_run, live_request_fingerprint,
    live_resume_error_is_dead_driver, live_run_key_for, live_sse_response,
    live_start_error_seals_tombstone, local_overload_retry_after, same_request_retry_wait_ms,
};
use crate::providers::cursor::model::{anthropic_wire_model, resolve_cursor_model};
use crate::providers::cursor::request::{
    CursorPromptOptions, CursorSelectedImage, claude_local_mcp_tools, current_user_blocks,
    cursor_request_context, latest_user_is_only_tool_results,
    reject_orphaned_native_results_when_live_slot_is_free, render_cursor_prompt,
    render_cursor_prompt_parts_with, request_has_client_only_tool_results,
};
use crate::providers::cursor::response::{
    AnthropicJsonAcc, CursorDecodeError, CursorStreamEvent, decode_cursor_upstream,
    decode_upstream_response, estimate_request_input_tokens,
};
use crate::providers::cursor::tool_bridge::{
    BridgeRegistry, advertised_tool_names, can_bridge_cursor_native_tools, find_tool_result,
    start_cursor_tool_bridge,
};

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

const CURSOR_HTTP_SHARDS_DEFAULT: usize = 4;
const CURSOR_HTTP_SHARDS_MAX: usize = 16;

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
const LIVE_EMPTY_TURN_EPISODE_MS: u64 = 300_000;

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
    fn from_env() -> Self {
        Self {
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
) -> Response {
    live_sse_response(
        tap_session_usage(session_id.to_string(), events),
        message_id,
        wire_model,
        estimated_input,
        monitor,
    )
}

enum LiveStartPeek {
    Retryable(String),
    Ready(mpsc::Receiver<LiveEventResult>),
}

#[derive(Clone)]
struct LiveRetryStart {
    client: CursorHttpClient,
    token: String,
    user_text: String,
    model: String,
    images: Vec<CursorSelectedImage>,
    custom_system: Option<String>,
    session_id: String,
    agent_id: Option<String>,
    parent_agent_id: Option<String>,
    allowed: Option<BTreeSet<String>>,
    mcp_tools: Option<crate::providers::cursor::proto::McpTools>,
    request_context: crate::providers::cursor::proto::RequestContext,
    fingerprint: Vec<u8>,
    has_refresh: bool,
}

fn live_retry_user_text<'a>(original: &'a str, error: &str) -> &'a str {
    if live_error_needs_checkpoint_continue(error) {
        EMPTY_TURN_CHECKPOINT_CONTINUE_PROMPT
    } else {
        original
    }
}

impl LiveRetryStart {
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
        self.start_with_user_text(self.retry_user_text(error), None)
            .await
    }

    async fn start_with_user_text(
        &self,
        user_text: &str,
        reservation: Option<LiveRunReservation>,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        start_live_events_with_retries(
            self.client.clone(),
            self.token.clone(),
            user_text,
            &self.model,
            &self.images,
            self.custom_system.as_deref(),
            LiveRunIdentity {
                session_id: &self.session_id,
                agent_id: self.agent_id.as_deref(),
                parent_agent_id: self.parent_agent_id.as_deref(),
            },
            self.allowed.clone(),
            self.mcp_tools.clone(),
            self.request_context.clone(),
            self.fingerprint.clone(),
            reservation,
            self.has_refresh,
        )
        .await
    }
}

/// Streaming Anthropic clients see "Waiting for response" until SSE starts.
/// `/v1/responses` must wait for live open so a 20s timeout can still be JSON 409.
fn commit_streaming_live_sse_before_start_live(want_stream: bool, hold_http: bool) -> bool {
    want_stream && !hold_http
}

const LIVE_RUN_BUSY_MESSAGE: &str =
    "A Cursor live run is already active for this session; retry after it advances";

fn live_run_busy_error() -> CursorError {
    let mut error = CursorError::new(503, LIVE_RUN_BUSY_MESSAGE, None);
    error.retry_after = Some(local_overload_retry_after());
    error
}

/// Serve the retained final segment of an already-completed identical
/// operation: exactly-once delivery for a client that never received the
/// original response (crash, timeout, dropped connection).
fn replay_completed_turn_channel(
    session_id: &str,
    events: &[LiveRunEvent],
) -> mpsc::Receiver<LiveEventResult> {
    let (tx, rx) = mpsc::channel(events.len().max(1));
    for event in events {
        // Capacity covers every event; we still hold the receiver.
        let _ = tx.try_send(Ok(event.clone()));
    }
    create_logger("cursor").info(
        "live_replay_completed_turn",
        Some(serde_json::Map::from_iter([
            ("sessionId".into(), serde_json::json!(session_id)),
            ("replayedEvents".into(), serde_json::json!(events.len())),
        ])),
    );
    rx
}

fn live_ambiguous_accept_error() -> CursorError {
    CursorError::new(
        409,
        "Cursor live run acceptance is ambiguous; retrying could duplicate execution",
        None,
    )
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
async fn start_live_events_with_retries(
    client: CursorHttpClient,
    mut token: String,
    user_text: &str,
    model: &str,
    images: &[CursorSelectedImage],
    custom_system: Option<&str>,
    identity: LiveRunIdentity<'_>,
    allowed: Option<BTreeSet<String>>,
    mcp_tools: Option<crate::providers::cursor::proto::McpTools>,
    request_context: crate::providers::cursor::proto::RequestContext,
    fingerprint: Vec<u8>,
    mut initial_reservation: Option<LiveRunReservation>,
    has_refresh: bool,
) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
    let conflict_deadline = Instant::now() + LIVE_H2_OPEN_ATTEMPT;
    let original_request_id = uuid::Uuid::new_v4().to_string();
    let operation_fingerprint = live_request_fingerprint(&fingerprint);
    let mut transient_retries = 0_u32;
    loop {
        // Local admission strictly precedes the session-slot claim. A start
        // that is only queued for local capacity must stay invisible to
        // concurrent duplicates; otherwise a 15s admission queue turns into
        // "already active for this session" for every overlapping retry.
        let admission = live::admit_live_start(model).await?;
        let mut reservation = if let Some(reservation) = initial_reservation.take() {
            reservation
        } else {
            let claimed = loop {
                match LiveRunRegistry::try_claim_run_for_operation(
                    identity.session_id,
                    identity.agent_id,
                    operation_fingerprint,
                ) {
                    LiveSlotClaim::Reserved(reservation) => break Some(reservation),
                    LiveSlotClaim::Starting => {
                        if Instant::now() >= conflict_deadline {
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
                    LiveSlotClaim::Running => {
                        if let Some(run) =
                            LiveRunRegistry::get_run(identity.session_id, identity.agent_id)
                            && run.request_fingerprint() == operation_fingerprint
                            && let Ok(events) =
                                run.attach_for_operation(operation_fingerprint).await
                        {
                            return Ok(events);
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

        let upstream_open_guard = reservation.upstream_open_guard();
        let start = match client
            .start_live_agent_with_identity_guarded(
                &token,
                user_text,
                model,
                images,
                custom_system,
                identity,
                allowed.clone(),
                mcp_tools.clone(),
                request_context.clone(),
                Some(&original_request_id),
                Some(reservation.cancelled()),
                Some(Arc::clone(&upstream_open_guard)),
                Some(admission),
            )
            .await
        {
            Ok(start) => Ok(start),
            Err(error) if error.status == 401 && has_refresh => match force_refresh_cursor_auth() {
                Ok(Some(refreshed)) => {
                    token = refreshed.access_token;
                    client
                        .start_live_agent_with_identity_guarded(
                            &token,
                            user_text,
                            model,
                            images,
                            custom_system,
                            identity,
                            allowed.clone(),
                            mcp_tools.clone(),
                            request_context.clone(),
                            Some(&original_request_id),
                            Some(reservation.cancelled()),
                            Some(upstream_open_guard),
                            None,
                        )
                        .await
                }
                _ => Err(error),
            },
            Err(error) => Err(error),
        };

        match start {
            Ok(start) => {
                start.handle.set_request_fingerprint(operation_fingerprint);
                if let Err(orphaned) = reservation.insert(Arc::clone(&start.handle)) {
                    let _ = orphaned.cancel_and_wait().await;
                    break;
                }
                match peek_live_start_for_stale_reset(start.events).await {
                    LiveStartPeek::Ready(events) => return Ok(events),
                    LiveStartPeek::Retryable(error) => {
                        let _ = start.handle.cancel_and_wait().await;
                        let _ = LiveRunRegistry::probe_run(identity.session_id, identity.agent_id);
                        if transient_retries >= crate::retry::MAX_RATE_LIMIT_RETRIES {
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
                let retryable = transient_retries < crate::retry::MAX_RATE_LIMIT_RETRIES
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
                return Err(exhausted_live_start_error(error, transient_retries));
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
    model: String,
    images: Vec<CursorSelectedImage>,
    custom_system: Option<String>,
    sid: String,
    agent_id: Option<String>,
    parent_agent_id: Option<String>,
    allowed: Option<BTreeSet<String>>,
    mcp_tools: Option<crate::providers::cursor::proto::McpTools>,
    request_context: crate::providers::cursor::proto::RequestContext,
    fingerprint: Vec<u8>,
    initial_reservation: Option<LiveRunReservation>,
    has_refresh: bool,
    message_id: String,
    wire_model: String,
    estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
) -> Response {
    let sid_for_sse = sid.clone();
    let rx = spawn_live_events_with_late_retries(
        LiveRetryStart {
            client,
            token,
            user_text,
            model,
            images,
            custom_system,
            session_id: sid,
            agent_id,
            parent_agent_id,
            allowed,
            mcp_tools,
            request_context,
            fingerprint,
            has_refresh,
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
    )
}

/// Wait briefly for the first live event before committing Anthropic SSE.
/// A missing-conversation reset on that first event can start a fresh Run
/// on this same request; grok-build will not retry the 502 itself.
async fn peek_live_start_for_stale_reset(
    mut events: mpsc::Receiver<LiveEventResult>,
) -> LiveStartPeek {
    let wait = Duration::from_millis(env_u64_millis("CCP_CURSOR_STALE_CONV_PEEK_MS", 2_000));
    match tokio::time::timeout(wait, events.recv()).await {
        Ok(Some(Err(error)))
            if live_error_is_same_request_retryable(&error)
                && !live_error_is_empty_turn_retry(&error) =>
        {
            LiveStartPeek::Retryable(error)
        }
        Ok(Some(first)) => LiveStartPeek::Ready(prepend_live_event(first, events)),
        Ok(None) | Err(_) => LiveStartPeek::Ready(events),
    }
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
    ClientGone,
}

fn live_event_commits_client_output(event: &LiveRunEvent) -> bool {
    match event {
        LiveRunEvent::NativeToolBatch(_) => true,
        LiveRunEvent::Cursor(
            CursorStreamEvent::ThinkingDelta { .. }
            | CursorStreamEvent::TextDelta { .. }
            | CursorStreamEvent::NativeTool { .. }
            | CursorStreamEvent::End,
        ) => true,
        LiveRunEvent::Cursor(
            CursorStreamEvent::Session { .. }
            | CursorStreamEvent::Usage { .. }
            | CursorStreamEvent::OutputTokenDelta { .. },
        ) => false,
    }
}

fn classify_live_pump_item(committed: bool, item: &LiveEventResult) -> LivePumpAction {
    match item {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LivePumpAction {
    Buffer,
    Forward,
    Retry,
}

async fn pump_live_events_until_commit_or_retry(
    tx: &mpsc::Sender<LiveEventResult>,
    mut events: mpsc::Receiver<LiveEventResult>,
) -> LivePumpOutcome {
    let mut committed = false;
    let mut buffered = Vec::new();
    while let Some(item) = events.recv().await {
        match classify_live_pump_item(committed, &item) {
            LivePumpAction::Retry => {
                let Err(error) = item else {
                    continue;
                };
                return LivePumpOutcome::Retry(error);
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
    last_error: &str,
) {
    LiveRunRegistry::cancel_run(session_id, agent_id);
    let _ = tx
        .send(Err(format!(
            "{last_error} (empty-turn recovery deadline exhausted)"
        )))
        .await;
}

fn live_late_retry_limit(error: &str, policy: LiveLateRetryPolicy) -> u32 {
    if live_error_is_empty_turn_retry(error) {
        policy.empty_turn_max_retries
    } else {
        policy.transient_max_retries
    }
}

async fn forward_live_events_with_retries<F, Fut>(
    tx: &mpsc::Sender<LiveEventResult>,
    mut events: mpsc::Receiver<LiveEventResult>,
    session_id: &str,
    agent_id: Option<&str>,
    mut restart: F,
    policy: LiveLateRetryPolicy,
) where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<mpsc::Receiver<LiveEventResult>, CursorError>>,
{
    let episode_started = tokio::time::Instant::now();
    let mut transient_retries = 0_u32;
    let mut empty_turn_retries = 0_u32;
    let mut empty_turn_deadline = None;
    let mut last_empty_turn_error = None::<String>;
    loop {
        let pump = pump_live_events_until_commit_or_retry(tx, events);
        tokio::pin!(pump);
        let outcome = tokio::select! {
            _ = tx.closed() => {
                LiveRunRegistry::cancel_run(session_id, agent_id);
                return;
            }
            _ = wait_for_optional_deadline(empty_turn_deadline) => {
                forward_empty_turn_deadline(
                    tx,
                    session_id,
                    agent_id,
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
                LiveRunRegistry::cancel_run(session_id, agent_id);
                return;
            }
            LivePumpOutcome::Retry(error) => {
                let empty_turn = live_error_is_empty_turn_retry(&error);
                let retry_index = if empty_turn {
                    empty_turn_retries
                } else {
                    transient_retries
                };
                let retry_limit = live_late_retry_limit(&error, policy);
                if retry_index >= retry_limit {
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
                            agent_id,
                            last_empty_turn_error.as_deref().unwrap_or(&error),
                        )
                        .await;
                        return;
                    }
                }
                let slot_deadline = Instant::now() + LIVE_H2_OPEN_ATTEMPT;
                loop {
                    match LiveRunRegistry::probe_run(session_id, agent_id) {
                        LiveRunProbe::Free => break,
                        LiveRunProbe::TerminalError(terminal)
                            if live_probe_error_blocks_new_run(&terminal) =>
                        {
                            let _ = tx.send(Err(terminal)).await;
                            return;
                        }
                        LiveRunProbe::TerminalError(_) => break,
                        LiveRunProbe::Occupied if Instant::now() < slot_deadline => {
                            tokio::select! {
                                _ = tx.closed() => {
                                    LiveRunRegistry::cancel_run(session_id, agent_id);
                                    return;
                                }
                                _ = wait_for_optional_deadline(empty_turn_deadline) => {
                                    forward_empty_turn_deadline(
                                        tx,
                                        session_id,
                                        agent_id,
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
                        LiveRunRegistry::cancel_run(session_id, agent_id);
                        return;
                    }
                    _ = wait_for_optional_deadline(empty_turn_deadline) => {
                        forward_empty_turn_deadline(
                            tx,
                            session_id,
                            agent_id,
                            last_empty_turn_error.as_deref().unwrap_or(&error),
                        )
                        .await;
                        return;
                    }
                    _ = crate::retry::sleep(wait) => {}
                }
                if empty_turn {
                    empty_turn_retries += 1;
                } else {
                    transient_retries += 1;
                }
                let start = restart(error);
                tokio::pin!(start);
                events = match tokio::select! {
                    _ = tx.closed() => {
                        LiveRunRegistry::cancel_run(session_id, agent_id);
                        return;
                    }
                    _ = wait_for_optional_deadline(empty_turn_deadline) => {
                        forward_empty_turn_deadline(
                            tx,
                            session_id,
                            agent_id,
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
    tokio::spawn(async move {
        let events = match initial_events {
            Some(events) => events,
            None => {
                let first = start.start(initial_reservation);
                tokio::pin!(first);
                match tokio::select! {
                    _ = tx.closed() => {
                        LiveRunRegistry::cancel_run(&session_id, agent_id.as_deref());
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
        forward_live_events_with_retries(
            &tx,
            events,
            &session_id,
            agent_id.as_deref(),
            move |error| {
                let retry_start = retry_start.clone();
                async move { retry_start.start_after_error(&error).await }
            },
            LiveLateRetryPolicy::from_env(),
        )
        .await;
    });
    rx
}

async fn live_downstream_response(
    want_stream: bool,
    session_id: &str,
    events: mpsc::Receiver<LiveEventResult>,
    message_id: String,
    wire_model: String,
    estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
) -> Response {
    if want_stream {
        live_sse_recording_usage(
            session_id,
            events,
            message_id,
            wire_model,
            estimated_input,
            monitor,
        )
    } else {
        live_json_recording_usage(
            session_id,
            events,
            message_id,
            wire_model,
            estimated_input,
            monitor,
        )
        .await
    }
}

async fn collect_live_events_to_json(
    mut events: mpsc::Receiver<LiveEventResult>,
    message_id: &str,
    model: &str,
    estimated_input: u64,
) -> Result<serde_json::Value, String> {
    let mut acc = AnthropicJsonAcc::new(estimated_input);
    let mut saw_end = false;
    let mut tool_handoff = false;
    while let Some(item) = events.recv().await {
        match item {
            Ok(LiveRunEvent::Cursor(event)) => {
                let ended = matches!(event, CursorStreamEvent::End);
                acc.push(&event);
                if ended {
                    saw_end = true;
                    break;
                }
            }
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                for tool in tools {
                    acc.push_native_tool(tool.tool_use_id, tool.name, tool.input);
                }
                tool_handoff = true;
                break;
            }
            Err(error) => return Err(error),
        }
    }
    if !acc.has_useful() {
        return Err("Cursor stream produced no useful progress".into());
    }
    if !saw_end && !tool_handoff {
        return Err("Cursor stream ended without turn_ended".into());
    }
    Ok(acc.into_message_json(message_id, model))
}

async fn live_json_recording_usage(
    session_id: &str,
    events: mpsc::Receiver<LiveEventResult>,
    message_id: String,
    wire_model: String,
    estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
) -> Response {
    match collect_live_events_to_json(
        tap_session_usage(session_id.to_string(), events),
        &message_id,
        &wire_model,
        estimated_input,
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

fn log_live_start_claude_headers(ctx: &RequestContext, session_id: &str) {
    create_logger("cursor").info(
        "live_start_identity",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), serde_json::json!(&ctx.req_id)),
            ("sessionId".to_string(), serde_json::json!(session_id)),
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

fn live_run_identity<'a>(session_id: &'a str, ctx: &'a RequestContext) -> LiveRunIdentity<'a> {
    LiveRunIdentity {
        session_id,
        agent_id: claude_agent_id(ctx),
        parent_agent_id: ctx
            .claude_code
            .parent_agent_id
            .as_deref()
            .map(str::trim)
            .filter(|id| !id.is_empty()),
    }
}

fn live_operation_fingerprint_payload(
    body: &MessagesRequest,
    client_request_id: Option<&str>,
) -> Vec<u8> {
    if let Some(request_id) = client_request_id.filter(|value| !value.is_empty()) {
        let mut payload = b"x-grok-req-id\0".to_vec();
        payload.extend_from_slice(request_id.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&serde_json::to_vec(body).unwrap_or_default());
        return payload;
    }
    serde_json::to_vec(&body.messages).unwrap_or_default()
}

/// Cursor conversation key for prompt compaction (`delta_only` / checkpoint).
///
/// Must match [`live_run_key_for`] used by the BiDi worker. Nested agents share
/// `X-Claude-Code-Session-Id` with the parent; using the raw session id here
/// would compact the nested prompt against the parent's checkpoint while the
/// nested Run is a fresh conversation.
fn continuation_for_request(
    session_id: Option<&str>,
    ctx: &RequestContext,
) -> crate::providers::cursor::conversation::RunContinuation {
    let Some(sid) = session_id.filter(|s| !s.is_empty()) else {
        return crate::providers::cursor::conversation::continuation_for(None);
    };
    let key = live_run_key_for(live_run_identity(sid, ctx));
    crate::providers::cursor::conversation::continuation_for(Some(&key))
}

enum LiveResumeOutcome {
    Resumed(mpsc::Receiver<LiveEventResult>),
    TerminalError(String),
    MissingTools(Vec<String>),
    ResumeError(CursorError),
    SupersedeRunning(String),
    Conflict,
    Free,
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
) -> Option<LiveResumeOutcome> {
    if let Some(run_id) = LiveRunRegistry::running_generation(session_id, agent_id) {
        // get_run hides cancel-requested / terminal handles. Compact and the
        // next grok turn close the previous SSE first; the dying generation
        // must be superseded, not 409'd.
        return Some(LiveResumeOutcome::SupersedeRunning(run_id));
    }
    if LiveRunRegistry::is_ambiguous_run(session_id, agent_id) {
        return Some(LiveResumeOutcome::ResumeError(live_ambiguous_accept_error()));
    }
    // Identical retry of a completed turn: deliver the retained response.
    if let Some(events) = LiveRunRegistry::completed_replay_for(session_id, agent_id, fingerprint) {
        return Some(LiveResumeOutcome::Resumed(replay_completed_turn_channel(
            session_id, &events,
        )));
    }
    LiveRunRegistry::release_success_if_new_request(session_id, agent_id, fingerprint);
    if !LiveRunRegistry::is_occupied_run(session_id, agent_id) {
        if let Some(run_id) = observed_run_id {
            return Some(LiveResumeOutcome::SupersedeRunning(run_id.to_string()));
        }
        return Some(LiveResumeOutcome::Free);
    }
    match LiveRunRegistry::probe_run(session_id, agent_id) {
        LiveRunProbe::TerminalError(error) if live_probe_error_blocks_new_run(&error) => {
            Some(LiveResumeOutcome::TerminalError(error))
        }
        LiveRunProbe::TerminalError(_) => {
            if let Some(run_id) = observed_run_id {
                Some(LiveResumeOutcome::SupersedeRunning(run_id.to_string()))
            } else {
                Some(LiveResumeOutcome::Free)
            }
        }
        LiveRunProbe::Free => {
            if let Some(run_id) = observed_run_id {
                Some(LiveResumeOutcome::SupersedeRunning(run_id.to_string()))
            } else {
                Some(LiveResumeOutcome::Free)
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
        env_u64_millis("CCP_CURSOR_LIVE_RESUME_WAIT_MS", 5_000)
    } else {
        env_u64_millis("CCP_CURSOR_LIVE_NESTED_WAIT_MS", 1_500)
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
            ) {
                return outcome;
            }
            // Starting / Succeeded: wait for Running or Free.
            observed_non_running_slot = true;
            tokio::time::sleep(Duration::from_millis(25)).await;
            continue;
        };
        if run.request_fingerprint() == fingerprint {
            // The active segment IS this operation. When its original consumer
            // is gone (client retry after disconnect/timeout), attach to the
            // in-flight run and replay the segment instead of 503-bouncing the
            // retry until the run dies — the wedge that killed grok-build
            // sessions with "already active" storms. A still-connected original
            // consumer rejects attach; keep waiting until the deadline in case
            // that consumer is mid-drop.
            match run.attach_for_operation(fingerprint).await {
                Ok(events) => return LiveResumeOutcome::Resumed(events),
                Err(error)
                    if tokio::time::Instant::now() < deadline
                        && error.message.contains(LIVE_RUN_BUSY_MESSAGE) =>
                {
                    // Original consumer is still connected. It may be mid-drop;
                    // keep polling until the deadline instead of 503-storming.
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    continue;
                }
                Err(_) => return LiveResumeOutcome::ResumeError(live_run_busy_error()),
            }
        }
        if observed_non_running_slot {
            // A Starting slot became Running. Tool-result waiters must not
            // attach to an unobserved generation. This waiter was never sent
            // upstream, so shed it as retryable busy rather than fatal 409.
            if has_tool_results {
                return LiveResumeOutcome::ResumeError(live_replacement_conflict_error(true));
            }
            return LiveResumeOutcome::SupersedeRunning(run.run_id().to_string());
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
        ) {
            return outcome;
        }
        if let Some(run_id) = observed_run_id {
            return LiveResumeOutcome::SupersedeRunning(run_id);
        }
        return LiveResumeOutcome::ResumeError(live_run_busy_error());
    };
    if run.request_fingerprint() == fingerprint {
        match run.attach_for_operation(fingerprint).await {
            Ok(events) => return LiveResumeOutcome::Resumed(events),
            Err(_) => return LiveResumeOutcome::ResumeError(live_run_busy_error()),
        }
    }
    if observed_non_running_slot {
        if has_tool_results {
            return LiveResumeOutcome::ResumeError(live_replacement_conflict_error(true));
        }
        return LiveResumeOutcome::SupersedeRunning(run.run_id().to_string());
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
            return unresolved_live_tools_outcome(false, missing, observed_run_id.as_deref());
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
    LiveResumeOutcome::SupersedeRunning(
        observed_run_id.expect("a live handle established the observed generation"),
    )
}

fn env_u64_millis(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
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
        let message_id = format!("msg_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
        let want_stream = body.stream;
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

        let resolved = resolve_cursor_model(model);
        if let Err(e) = resolved {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("Model \"{model}\" is not supported: {e}"),
            );
        }

        // True Cursor BiDi continuation: the preceding Anthropic response ended
        // at tool_use, but the upstream AgentService/Run stream is still alive.
        // Route the matching tool_result back onto that exact request stream
        // instead of replaying the whole conversation as a fresh Cursor run.
        let mut preclaimed_live_reservation = None;
        let mut resumed_live_events = None;
        if let Some(session_id) = ctx.session_id.as_deref() {
            let agent_id = claude_agent_id(&ctx);
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
            if resumed_live_events.is_none() {
                LiveRunRegistry::release_success_if_new_request(session_id, agent_id, fingerprint);
            }
            if resumed_live_events.is_none() {
                match LiveRunRegistry::probe_run(session_id, agent_id) {
                    LiveRunProbe::TerminalError(error)
                        if live_probe_error_blocks_new_run(&error) =>
                    {
                        return json_error_from_cursor_message(error);
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
                                return json_error_from_cursor_message(error);
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
                                match LiveRunRegistry::claim_replacement_for_run(
                                    session_id, agent_id, &run_id,
                                ) {
                                    LiveReplacementClaim::Conflict => {
                                        let error = live_replacement_conflict_error(
                                            request_has_current_tool_result(&body),
                                        );
                                        return map_cursor_error_to_response(&error);
                                    }
                                    LiveReplacementClaim::Reserved {
                                        reservation,
                                        superseded,
                                    } => {
                                        if let Some(handle) = superseded {
                                            reservation.protect_on_drop();
                                            let cancel_result = handle.cancel_and_wait().await;
                                            match finish_replacement_after_cancel(
                                                reservation,
                                                handle,
                                                request_has_current_tool_result(&body),
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
                            LiveResumeOutcome::Free => {
                                if reject_orphaned_native_results_when_live_slot_is_free(&body) {
                                    return json_error(
                                        StatusCode::CONFLICT,
                                        "invalid_request_error",
                                        "Stale Cursor tool_result cannot start a new live run",
                                    );
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
        if let Some(ref session_id) = ctx.session_id
            && let Some(pending) = BridgeRegistry::pending_tool(session_id)
            && find_tool_result(&body, pending.tool_use_id()).is_some()
        {
            BridgeRegistry::remove(session_id);
        }

        let mut auth = match load_cursor_auth() {
            Ok(Some(auth)) => auth,
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

        // Near expiry: force-refresh when possible instead of hard-failing re-login.
        if matches!(auth.expires, Some(expires) if expires <= now_ms() + 60_000) {
            match force_refresh_cursor_auth() {
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

        let session_id = ctx.session_id.as_deref();
        let bridge_eligible = can_bridge_cursor_native_tools(&body, session_id);
        let continuation_key = session_id
            .filter(|s| !s.is_empty())
            .map(|sid| live_run_key_for(live_run_identity(sid, &ctx)));
        let continuation = continuation_for_request(session_id, &ctx);
        let client_only_continuation =
            request_has_client_only_tool_results(&body) || latest_user_is_only_tool_results(&body);
        let parts = render_cursor_prompt_parts_with(
            &body,
            CursorPromptOptions {
                // Native BiDi tools don't need Anthropic schemas in user text;
                // Claude-local tools (Workflow/Skill/mcp__) are still forwarded.
                omit_tools: bridge_eligible || continuation.has_checkpoint,
                // ClientOnly (Workflow/Skill) results arrive after BiDi teardown.
                // delta_only would skip tool_result-only messages and replay the
                // original user text against a stale/zombie MCP checkpoint.
                delta_only: continuation.has_checkpoint && !client_only_continuation,
            },
        );
        let images = request::cursor_selected_images(&body);
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
        let custom_system = parts.custom_system_prompt.as_deref();
        let user_text = parts.user_text.as_str();

        let client = shared_cursor_http_client(continuation_key.as_deref());
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.upstream_started(&ctx.req_id);
        }
        let mut token = auth.access_token.clone();

        if let Some(events) = resumed_live_events.take() {
            let sid = session_id.expect("a resumed live run requires a session id");
            let identity = live_run_identity(sid, &ctx);
            let estimated_input = estimate_request_input_tokens(&body);
            let monitor = ctx
                .monitor
                .clone()
                .map(|handle| (handle, ctx.req_id.clone()));
            let retry_start = LiveRetryStart {
                client: client.clone(),
                token,
                user_text: user_text.to_string(),
                model: model.to_string(),
                images,
                custom_system: custom_system.map(str::to_string),
                session_id: sid.to_string(),
                agent_id: identity.agent_id.map(str::to_string),
                parent_agent_id: identity.parent_agent_id.map(str::to_string),
                allowed: advertised_tool_names(&body),
                mcp_tools: claude_local_mcp_tools(&body),
                request_context: cursor_request_context(&body),
                fingerprint: live_operation_fingerprint_payload(
                    &body,
                    ctx.client_request_id.as_deref(),
                ),
                has_refresh: auth.refresh_token.is_some(),
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
            )
            .await;
        }

        // Prefer long-lived BiDi/RunSSE whenever we have a session. Claude Code's
        // non-streaming fallback (`stream=false`) still uses live; we collect the
        // same events into one JSON body instead of SSE.
        let has_session = session_id.is_some_and(|s| !s.is_empty());
        let live_eligible =
            live_path_eligible(want_stream, has_session, client.live_bidi_enabled());
        if !live_eligible {
            let mut fields = serde_json::Map::new();
            fields.insert("stream".into(), serde_json::json!(want_stream));
            fields.insert("hasSession".into(), serde_json::json!(has_session));
            fields.insert(
                "bidiEnabled".into(),
                serde_json::json!(client.live_bidi_enabled()),
            );
            fields.insert(
                "reason".into(),
                serde_json::json!(live_path_skip_reason(
                    want_stream,
                    has_session,
                    client.live_bidi_enabled()
                )),
            );
            create_logger("cursor").info("live_skipped", Some(fields));
        }
        if live_eligible {
            let sid = session_id.expect("live eligibility requires session id");
            let identity = live_run_identity(sid, &ctx);
            log_live_start_claude_headers(&ctx, sid);
            let allowed = advertised_tool_names(&body);
            let mcp_tools = claude_local_mcp_tools(&body);
            let estimated_input = estimate_request_input_tokens(&body);
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
            ) {
                return spawn_streaming_live_sse(
                    client.clone(),
                    token,
                    user_text.to_string(),
                    model.to_string(),
                    images,
                    custom_system.map(str::to_string),
                    sid.to_string(),
                    identity.agent_id.map(str::to_string),
                    identity.parent_agent_id.map(str::to_string),
                    allowed,
                    mcp_tools,
                    request_context,
                    fingerprint,
                    initial_reservation,
                    has_refresh,
                    message_id,
                    wire_model,
                    estimated_input,
                    monitor,
                );
            }
            let retry_start = LiveRetryStart {
                client: client.clone(),
                token,
                user_text: user_text.to_string(),
                model: model.to_string(),
                images,
                custom_system: custom_system.map(str::to_string),
                session_id: sid.to_string(),
                agent_id: identity.agent_id.map(str::to_string),
                parent_agent_id: identity.parent_agent_id.map(str::to_string),
                allowed,
                mcp_tools,
                request_context,
                fingerprint,
                has_refresh,
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
                    )
                    .await;
                }
                Err(error) => return map_cursor_error_to_response(&error),
            }
        }

        let mut transport_retries = 0_u32;
        let mut refreshed_once = false;
        let upstream = loop {
            match client
                .run_agent_with_session(
                    &token,
                    user_text,
                    model,
                    &images,
                    custom_system,
                    continuation_key.as_deref(),
                )
                .await
            {
                Ok(r) => break r,
                Err(e) if e.status == 401 && !refreshed_once && auth.refresh_token.is_some() => {
                    match force_refresh_cursor_auth() {
                        Ok(Some(refreshed)) => {
                            token = refreshed.access_token;
                            refreshed_once = true;
                            continue;
                        }
                        _ => return map_cursor_error_to_response(&e),
                    }
                }
                Err(e)
                    if transport_retries < crate::retry::MAX_RATE_LIMIT_RETRIES
                        && cursor_start_error_is_same_request_retryable(&e) =>
                {
                    crate::retry::sleep(same_request_retry_wait_ms(
                        transport_retries,
                        &e.client_message(),
                    ))
                    .await;
                    transport_retries += 1;
                }
                Err(e) => {
                    return map_cursor_error_to_response(&exhausted_live_start_error(
                        e,
                        transport_retries,
                    ));
                }
            }
        };

        if want_stream {
            if bridge_eligible {
                let events = match decode_upstream_response(&upstream.body) {
                    Ok(e) => e,
                    Err(e) => return map_cursor_decode_error_to_response(&e),
                };

                let allowed = advertised_tool_names(&body);
                // Anthropic surface must echo the wire id (`claude-fable-5[1m]`),
                // not the suffix-stripped request model — Claude Code / ccstatusline
                // derive the 1M window from `[1m]` when the proxy host is not
                // api.anthropic.com (gB/pL first-party path is off).
                let (sse_bytes, _paused) = start_cursor_tool_bridge(
                    &message_id,
                    &wire_model,
                    session_id.unwrap(),
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
                let sse_bytes = sse::frame_cursor_stream(&upstream, &message_id, &wire_model);
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
            match decode_cursor_upstream(&upstream, &message_id, &wire_model) {
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
        503 => {
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
    match err.status() {
        Some(401) => json_error(StatusCode::UNAUTHORIZED, "authentication_error", msg),
        Some(403) if is_outdated_client_error(&msg) => json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!(
                "{msg}. Cursor rejected this client fingerprint (not an expired login). \
Upgrade cursor-agent or set CCP_CURSOR_CLIENT_VERSION."
            ),
        ),
        Some(403) => json_error(
            StatusCode::FORBIDDEN,
            "permission_error",
            format!("{msg}. Permission/policy error — re-login usually will not help."),
        ),
        Some(429) => json_error(StatusCode::TOO_MANY_REQUESTS, "rate_limit_error", msg),
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
        let auth = run_cursor_login()?.ok_or_else(|| anyhow::anyhow!("Cursor login timed out"))?;
        println!("Cursor auth saved in {}", auth.source);
        if let Some(ref user_id) = auth.user_id {
            println!("User: {user_id}");
        }
        if let Some(ref email) = auth.email {
            println!("Email: {email}");
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
        Ok(())
    }
}

pub(crate) static CURSOR_CLI: CursorCli = CursorCli;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::live::live_error_allows_fresh_conversation;

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
        assert!(!live_error_is_same_request_retryable(
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
    async fn peek_empty_turn_defers_to_the_dedicated_late_retry_policy() {
        let error = "Cursor upstream finished this turn without text or tool calls; retry this turn \
                     (stale Cursor conversation reset; retry this message to continue)";
        let (tx, rx) = mpsc::channel(2);
        tx.send(Err(error.into())).await.unwrap();
        drop(tx);
        let LiveStartPeek::Ready(mut events) = peek_live_start_for_stale_reset(rx).await else {
            panic!("empty turns must not consume the generic start retry budget");
        };
        assert_eq!(events.recv().await.unwrap().unwrap_err(), error);
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

    #[tokio::test]
    async fn pre_output_openai_502_is_retried_not_forwarded() {
        let provider_502 = "Connect error 502: ERROR_OPENAI: Unable to reach the model provider — We're having trouble connecting to the model provider. This might be temporary - please try again in a moment. [unavailable]";
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
        let mut saw_thinking = false;
        let mut saw_recovered = false;
        while let Ok(item) = out_rx.try_recv() {
            match item {
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta { .. })) => {
                    saw_thinking = true;
                }
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) => {
                    saw_recovered |= text == "recovered";
                }
                Err(error) => panic!("internal retry leaked to the client: {error}"),
                _ => {}
            }
        }
        assert!(saw_thinking);
        assert!(saw_recovered);
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
    async fn committed_or_billing_live_error_is_forwarded() {
        let billing = "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — pay your invoice in Stripe [resource_exhausted]";
        let empty_turn =
            "Cursor upstream finished this turn without text or tool calls; retry this turn";
        assert_eq!(
            classify_live_pump_item(false, &Err(billing.into())),
            LivePumpAction::Forward
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
    async fn peek_unpaid_invoice_stays_on_the_same_run() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(Err(
            "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — pay your invoice in Stripe [resource_exhausted]"
                .into(),
        ))
        .await
        .unwrap();
        drop(tx);
        let LiveStartPeek::Ready(mut events) = peek_live_start_for_stale_reset(rx).await else {
            panic!("billing 429 must be forwarded, not retried");
        };
        let Err(error) = events.recv().await.unwrap() else {
            panic!("the billing error must still be delivered");
        };
        assert!(error.contains("unpaid invoice"), "{error}");
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

    #[test]
    fn streaming_live_commits_sse_before_start_live() {
        assert!(
            commit_streaming_live_sse_before_start_live(true, false),
            "Claude Code must get message_start before start_live / peek / retry"
        );
        assert!(
            !commit_streaming_live_sse_before_start_live(true, true),
            "/v1/responses must wait for live open so 409 is JSON, not grok 500"
        );
        assert!(
            !commit_streaming_live_sse_before_start_live(false, false),
            "non-streaming JSON collection still waits for the live run"
        );
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
        let LiveStartPeek::Ready(mut events) = peek_live_start_for_stale_reset(rx).await else {
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
    fn nested_agent_prompt_continuation_ignores_parent_checkpoint() {
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
        let json = collect_live_events_to_json(rx, "msg_live", "claude-fable-5", 3)
            .await
            .unwrap();
        assert_eq!(json["content"][0]["text"], "hi");
        assert_eq!(json["stop_reason"], "end_turn");
    }

    #[tokio::test]
    async fn collect_live_events_to_json_empty_is_error() {
        let (tx, rx) = mpsc::channel::<LiveEventResult>(1);
        drop(tx);
        let err = collect_live_events_to_json(rx, "msg_empty", "claude-fable-5", 1)
            .await
            .unwrap_err();
        assert!(err.contains("no useful progress"), "{err}");
    }

    #[tokio::test]
    async fn collect_live_events_to_json_truncated_useful_is_error() {
        let (tx, rx) = mpsc::channel(4);
        tx.send(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: "partial".into(),
        })))
        .await
        .unwrap();
        drop(tx);
        let err = collect_live_events_to_json(rx, "msg_trunc", "claude-fable-5", 3)
            .await
            .unwrap_err();
        assert!(err.contains("without turn_ended"), "{err}");
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
