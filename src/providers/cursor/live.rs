//! Long-lived Cursor Agent BiDi runs.
//!
//! A Claude Code tool turn spans two Anthropic HTTP requests, while Cursor keeps
//! the model + exec loop on one `AgentService/Run` stream. This module owns that
//! upstream stream between requests and sends native exec results back through
//! the original request-body channel.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::error::Error;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::StatusCode;
use prost::Message;
use rand::Rng;
use tokio::sync::{Notify, mpsc, oneshot, watch};

use super::client::{
    CursorError, CursorHttpClient, build_resume_run_request, build_run_request_with_continuation,
    encode_client_heartbeat_frame,
};
use super::connect::{
    ConnectEndError, ConnectFrame, ConnectFrameDecoder, FLAG_END, FLAG_GZIP,
    anthropic_error_type_from_live_error, cursor_connect_error_is_missing_conversation_data,
    cursor_connect_error_is_missing_image, decode_gzip_frame, encode_connect_frame,
    parse_connect_error,
};
use super::exec_results::{
    CursorExecKind, PendingCursorExec, encode_control_close, encode_control_throw,
    encode_exec_heartbeat, encode_tool_result_frames,
};
use super::http1::{self, BidiAppendSession};
use super::native_tools::{
    accumulate_partial_args_text, adapt_client_tool_input, adapt_native_task_to_spawn_subagent,
    adapt_tool_input_for_client, advertised_name_fallbacks, map_tool_call_started,
    merge_partial_args_json, resolve_glob_client_name,
};
use super::proto::{
    self, AgentClientMessage, AskQuestionArgs, AskQuestionInteractionQuery,
    AskQuestionInteractionResponse, AskQuestionRejected, AskQuestionResult, ClientHeartbeat,
    CreatePlanRequestResponse, CreatePlanResult, CreatePlanSuccess, ExecClientMessage,
    GetBlobResult, InteractionApproved, InteractionQuery, InteractionRejected, InteractionResponse,
    InteractionUpdate, KvClientMessage, KvServerMessage, MAX_TASK_DELTA_NEST,
    McpAuthRequestResponse, RequestContext, RequestContextResult, RequestContextSuccess,
    SetBlobResult, SwitchModeRequestResponse, WebFetchRequestResponse, WebSearchRequestResponse,
};
use super::request::{
    CLAUDE_LOCAL_MCP_PROVIDER, CursorSelectedImage, is_claude_local_tool_name,
    is_grok_build_subagent_lifecycle_tool, normalize_grok_build_lifecycle_name,
    strip_mcp_provider_prefix,
};
use super::response::CursorStreamEvent;
use super::sse::{CursorSseEncoder, EVENT_ERROR, EVENT_PING, format_sse_event_bytes};
use super::tool_use_xml::{CursorToolUseXmlParser, RecoveredCursorEvent};

/// Outbound client messages: BiDi request body stream, or HTTP/1 BidiAppend.
#[derive(Clone)]
enum ClientOutbound {
    Bidi(mpsc::Sender<Result<Bytes, std::io::Error>>),
    Http1(BidiAppendSession),
}

fn ambiguous_http1_append_error(error: CursorError, operation: &str) -> CursorError {
    if is_pre_connect_failure(&error) {
        return error;
    }
    CursorError::new(
        error.status,
        format!(
            "Cursor BidiAppend {operation} failed; acceptance is ambiguous: {}",
            error.message
        ),
        error.detail,
    )
}

impl ClientOutbound {
    async fn send_connect_frame(&self, frame: Bytes) -> Result<(), CursorError> {
        let timeout = Duration::from_secs(env_u64("CCP_CURSOR_SEND_TIMEOUT_SECS", 5));
        match self {
            Self::Bidi(tx) => match tokio::time::timeout(timeout, tx.send(Ok(frame))).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(_)) => Err(CursorError::internal("Cursor BiDi request stream closed")),
                // Tokio's bounded-channel send is cancellation-safe: if this
                // times out, the frame was not queued and may be retried.
                Err(_) => Err(CursorError::new(
                    504,
                    "Cursor BiDi request stream send timed out",
                    None,
                )),
            },
            Self::Http1(session) => {
                match tokio::time::timeout(timeout, session.append_connect_or_raw(&frame)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(ambiguous_http1_append_error(error, "send")),
                    // Dropping a unary HTTP request future cannot prove that
                    // Cursor did not accept it. Never reconnect and replay it.
                    Err(_) => Err(CursorError::new(
                        504,
                        "Cursor BidiAppend timed out; acceptance is ambiguous",
                        None,
                    )),
                }
            }
        }
    }

    /// Best-effort send for keepalives — never block the BiDi read loop.
    /// Full queues drop this heartbeat tick; the next interval still fires.
    /// HTTP/1 BidiAppend is spawned so a slow append cannot stall upstream reads
    /// (CLI's duplex heartbeats never serialize behind unary append RTTs).
    fn try_send_heartbeat_frame(&self, frame: Bytes) -> bool {
        match self {
            Self::Bidi(tx) => matches!(tx.try_send(Ok(frame)), Ok(())),
            Self::Http1(session) => {
                let session = session.clone();
                tokio::spawn(async move {
                    let _ = session.append_connect_or_raw(&frame).await;
                });
                true
            }
        }
    }
}

/// Fan-out from the BiDi driver to Anthropic SSE. Sized for Fable max-effort
/// thinking bursts so we rarely block the upstream read loop.
const LIVE_EVENT_CHANNEL_CAP: usize = 512;

/// Merge consecutive thinking/text deltas only when the fan-out is at least
/// half full. Healthy queues stay 1:1 so Claude Code paints at Cursor cadence.
const COALESCE_WINDOW: Duration = Duration::from_millis(8);
const COALESCE_MAX_CHARS: usize = 4096;
const MAX_CONSECUTIVE_DECODE_FAILURES: u32 = 8;

/// Claude Code 2.1.193 `AskUserQuestion` chip/tag max (`Qzi = 12`).
const ASK_USER_QUESTION_HEADER_MAX: usize = 12;

/// Nested Workflow POSTs share `X-Claude-Code-Session-Id` (`kt()` is not
/// rotated). The nested difference is additive headers
/// `x-claude-code-agent-id` + `x-claude-code-parent-agent-id`. Concurrent live
/// runs are keyed by `(session_id, agent_id)` — never a new session UUID.
const AGENT_RUN_MARK: &str = "::agent::";

/// Identity for a Claude Code Messages POST that may be a nested agent.
///
/// `mod.rs` / `server.rs` should pass `x-claude-code-agent-id` and
/// `x-claude-code-parent-agent-id` here. Absent `agent_id` keeps one run per
/// session (legacy behavior).
#[derive(Debug, Clone, Copy, Default)]
pub struct LiveRunIdentity<'a> {
    pub session_id: &'a str,
    pub agent_id: Option<&'a str>,
    pub parent_agent_id: Option<&'a str>,
}

impl<'a> LiveRunIdentity<'a> {
    pub fn parent(session_id: &'a str) -> Self {
        Self {
            session_id,
            agent_id: None,
            parent_agent_id: None,
        }
    }

    pub fn is_nested(&self) -> bool {
        self.agent_id
            .map(str::trim)
            .is_some_and(|id| !id.is_empty())
            && self
                .parent_agent_id
                .map(str::trim)
                .is_some_and(|id| !id.is_empty())
    }
}

/// Registry / Cursor-conversation key for a live run.
///
/// `None` / empty `agent_id` → `session_id` (one run per Claude session).
/// Nested agent → `{session_id}::agent::{agent_id}` so it cannot supersede
/// the parent that shares `X-Claude-Code-Session-Id`.
pub fn live_run_key(session_id: &str, agent_id: Option<&str>) -> String {
    match agent_id.map(str::trim).filter(|id| !id.is_empty()) {
        Some(agent) => format!("{session_id}{AGENT_RUN_MARK}{agent}"),
        None => session_id.to_string(),
    }
}

pub fn live_run_key_for(identity: LiveRunIdentity<'_>) -> String {
    live_run_key(identity.session_id, identity.agent_id)
}

fn claude_session_of(run_key: &str) -> &str {
    run_key.split(AGENT_RUN_MARK).next().unwrap_or(run_key)
}

fn channel_backpressured(remaining: usize, cap: usize) -> bool {
    cap > 0 && remaining.saturating_mul(2) <= cap
}

#[derive(Debug, Clone)]
pub enum LiveRunEvent {
    Cursor(CursorStreamEvent),
    NativeToolBatch(Vec<LiveNativeTool>),
}

#[derive(Debug, Clone)]
pub struct LiveNativeTool {
    pub(crate) tool_use_id: String,
    pub(crate) name: String,
    pub(crate) input: serde_json::Value,
}

pub type LiveEventResult = Result<LiveRunEvent, String>;

#[derive(Debug, Clone)]
struct TerminalOutcome {
    message: String,
    created_at: Instant,
}

fn terminal_outcome_is_fresh(outcome: &TerminalOutcome) -> bool {
    let configured = Duration::from_secs(env_u64("CCP_CURSOR_TERMINAL_TTL_SECS", 60));
    let ttl = if terminal_error_is_ambiguous_accept(&outcome.message) {
        configured.max(LIVE_AMBIGUOUS_OPEN_TTL)
    } else {
        configured
    };
    outcome.created_at.elapsed() < ttl
}

pub struct LiveRunStart {
    pub handle: Arc<CursorLiveRunHandle>,
    pub events: mpsc::Receiver<LiveEventResult>,
}

struct LiveResumePermit {
    in_flight: Arc<AtomicBool>,
}

impl Drop for LiveResumePermit {
    fn drop(&mut self) {
        self.in_flight.store(false, Ordering::Release);
    }
}

const RESUME_DISPATCH_WAITING: u8 = 0;
const RESUME_DISPATCH_STARTED: u8 = 1;
const RESUME_DISPATCH_CANCELLED: u8 = 2;

struct LiveResumeWaitGuard {
    state: Arc<AtomicU8>,
}

impl Drop for LiveResumeWaitGuard {
    fn drop(&mut self) {
        let _ = self.state.compare_exchange(
            RESUME_DISPATCH_WAITING,
            RESUME_DISPATCH_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

enum RunCommand {
    ResumeBatch {
        tool_results: Vec<(String, serde_json::Value)>,
        sink: mpsc::Sender<LiveEventResult>,
        ack: oneshot::Sender<Result<(), CursorError>>,
        permit: LiveResumePermit,
        generation_permit: LiveGenerationPermit,
        dispatch_state: Arc<AtomicU8>,
    },
    Cancel {
        ack: Option<oneshot::Sender<()>>,
    },
}

#[derive(Debug)]
pub struct CursorLiveRunHandle {
    run_id: String,
    command_tx: mpsc::Sender<RunCommand>,
    pending: Arc<Mutex<Vec<PendingCursorExec>>>,
    terminal_error: Arc<Mutex<Option<TerminalOutcome>>>,
    completed: Arc<AtomicBool>,
    cancel_requested: Arc<AtomicBool>,
    resume_in_flight: Arc<AtomicBool>,
    request_fingerprint: AtomicU64,
}

impl CursorLiveRunHandle {
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn set_request_fingerprint(&self, fingerprint: u64) {
        self.request_fingerprint
            .store(fingerprint, Ordering::Release);
    }

    fn request_fingerprint(&self) -> u64 {
        self.request_fingerprint.load(Ordering::Acquire)
    }

    /// Return the first exposed exec for compatibility with the original
    /// single-tool bridge API.
    pub fn pending(&self) -> Option<PendingCursorExec> {
        self.pending_tools().into_iter().next()
    }

    /// Snapshot all execs exposed in the current Anthropic tool-use segment.
    pub fn pending_tools(&self) -> Vec<PendingCursorExec> {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }

    fn is_cancel_requested(&self) -> bool {
        self.cancel_requested.load(Ordering::Acquire)
    }

    fn take_terminal_error(&self) -> Option<String> {
        self.terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .filter(terminal_outcome_is_fresh)
            .map(|outcome| outcome.message)
    }

    fn has_terminal_error(&self) -> bool {
        self.terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(terminal_outcome_is_fresh)
    }

    fn ensure_replacement_is_safe(&self) -> Result<(), CursorError> {
        let ambiguous = self
            .terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|outcome| {
                terminal_outcome_is_fresh(outcome)
                    && terminal_error_is_ambiguous_accept(&outcome.message)
            })
            .map(|outcome| outcome.message.clone());
        match ambiguous {
            Some(message) => Err(CursorError::new(
                409,
                format!(
                    "Cursor live run ended in an ambiguous upstream state; replacement blocked: {message}"
                ),
                None,
            )),
            None => Ok(()),
        }
    }

    pub async fn resume(
        &self,
        tool_use_id: &str,
        tool_result: serde_json::Value,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        self.resume_batch(vec![(tool_use_id.to_string(), tool_result)])
            .await
    }

    /// Resume one Cursor turn with every `tool_result` produced for the sibling
    /// Anthropic `tool_use` blocks in the preceding response.
    pub async fn resume_batch(
        &self,
        tool_results: Vec<(String, serde_json::Value)>,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        self.resume_batch_within(tool_results, resume_dispatch_timeout())
            .await
    }

    async fn resume_batch_within(
        &self,
        tool_results: Vec<(String, serde_json::Value)>,
        dispatch_timeout: Duration,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        let pending = self.pending_tools();
        validate_tool_result_batch(&pending, &tool_results)
            .map_err(|message| CursorError::new(400, message, None))?;
        self.resume_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                CursorError::new(
                    409,
                    "Another tool-result resume is already in flight for this Cursor run",
                    None,
                )
            })?;
        let permit = LiveResumePermit {
            in_flight: Arc::clone(&self.resume_in_flight),
        };
        let dispatch_state = Arc::new(AtomicU8::new(RESUME_DISPATCH_WAITING));
        let _wait_guard = LiveResumeWaitGuard {
            state: Arc::clone(&dispatch_state),
        };
        let command_tx = self.command_tx.clone();
        let command_dispatch_state = Arc::clone(&dispatch_state);
        let cancel_requested = Arc::clone(&self.cancel_requested);
        let dispatch = async move {
            let generation_permit = acquire_live_generation_resume_permit().await?;
            if cancel_requested.load(Ordering::Acquire) {
                return Err(CursorError::new(
                    409,
                    "Cursor live run was cancelled while waiting for generation capacity",
                    None,
                ));
            }
            // Match start_live_agent capacity — post-tool thinking bursts must not
            // trip the old 64-slot ceiling (silent drop under try_send timeout).
            let (sink, events) = mpsc::channel(LIVE_EVENT_CHANNEL_CAP);
            let (ack, ready) = oneshot::channel();
            command_tx
                .send(RunCommand::ResumeBatch {
                    tool_results,
                    sink,
                    ack,
                    permit,
                    generation_permit,
                    dispatch_state: command_dispatch_state,
                })
                .await
                .map_err(|_| CursorError::internal("Cursor live run already closed"))?;
            ready.await.map_err(|_| {
                CursorError::internal("Cursor live resume acknowledgement dropped")
            })??;
            Ok(events)
        };
        self.await_resume_dispatch(dispatch, dispatch_state, dispatch_timeout)
            .await
    }

    async fn await_resume_dispatch(
        &self,
        dispatch: impl Future<Output = Result<mpsc::Receiver<LiveEventResult>, CursorError>>,
        dispatch_state: Arc<AtomicU8>,
        dispatch_timeout: Duration,
    ) -> Result<mpsc::Receiver<LiveEventResult>, CursorError> {
        tokio::pin!(dispatch);
        match tokio::time::timeout(dispatch_timeout, &mut dispatch).await {
            Ok(result) => result,
            Err(_) => match dispatch_state.compare_exchange(
                RESUME_DISPATCH_WAITING,
                RESUME_DISPATCH_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => Err(resume_dispatch_retryable_error(
                    "Cursor live resume dispatch timed out before driver acceptance; retry this tool result",
                )),
                Err(RESUME_DISPATCH_STARTED) => dispatch.await,
                Err(_) => Err(resume_dispatch_retryable_error(
                    "Cursor live resume dispatch was cancelled before driver acceptance",
                )),
            },
        }
    }

    pub fn cancel(&self) {
        self.cancel_requested.store(true, Ordering::Release);
        let _ = self.command_tx.try_send(RunCommand::Cancel { ack: None });
    }

    /// Deliver cancellation and wait until the old driver has dropped its
    /// upstream request before a replacement Run may open.
    pub async fn cancel_and_wait(&self) -> Result<(), CursorError> {
        self.cancel_requested.store(true, Ordering::Release);
        if self.is_completed() {
            return self.ensure_replacement_is_safe();
        }
        let (ack, ready) = oneshot::channel();
        match tokio::time::timeout(
            Duration::from_secs(env_u64("CCP_CURSOR_CANCEL_WAIT_SECS", 10)),
            async {
                self.command_tx
                    .send(RunCommand::Cancel { ack: Some(ack) })
                    .await
                    .map_err(|_| "cancellation channel closed")?;
                ready
                    .await
                    .map_err(|_| "cancellation acknowledgement dropped")
            },
        )
        .await
        {
            Ok(Ok(())) => self.ensure_replacement_is_safe(),
            Ok(Err(_)) if self.is_completed() => self.ensure_replacement_is_safe(),
            Ok(Err(reason)) => Err(CursorError::new(
                409,
                format!("Cursor live run {reason}"),
                None,
            )),
            Err(_) => Err(CursorError::new(
                409,
                "Cursor live run cancellation timed out before replacement",
                None,
            )),
        }
    }
}

fn validate_tool_result_batch(
    pending: &[PendingCursorExec],
    tool_results: &[(String, serde_json::Value)],
) -> Result<(), String> {
    if pending.is_empty() {
        return Err("Cursor live run has no pending native tools".into());
    }

    let expected: HashSet<&str> = pending
        .iter()
        .map(|exec| exec.tool_use_id.as_str())
        .collect();
    let mut supplied = HashSet::with_capacity(tool_results.len());
    for (tool_use_id, _) in tool_results {
        if !expected.contains(tool_use_id.as_str()) {
            return Err(format!(
                "Cursor tool result id {tool_use_id} is not pending"
            ));
        }
        if !supplied.insert(tool_use_id.as_str()) {
            return Err(format!(
                "Cursor tool result id {tool_use_id} was supplied more than once"
            ));
        }
    }

    let missing: Vec<&str> = pending
        .iter()
        .map(|exec| exec.tool_use_id.as_str())
        .filter(|tool_use_id| !supplied.contains(tool_use_id))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "Cursor live run is still awaiting tool results for: {}",
            missing.join(", ")
        ));
    }
    Ok(())
}

fn encode_tool_result_batch(
    pending: &[PendingCursorExec],
    tool_results: &[(String, serde_json::Value)],
) -> Result<Vec<Bytes>, String> {
    validate_tool_result_batch(pending, tool_results)?;
    let result_by_id: HashMap<&str, &serde_json::Value> = tool_results
        .iter()
        .map(|(tool_use_id, result)| (tool_use_id.as_str(), result))
        .collect();
    let mut frames = Vec::new();
    for current in pending {
        let result = result_by_id
            .get(current.tool_use_id.as_str())
            .expect("validated result batch contains every pending tool");
        frames.extend(
            encode_tool_result_frames(current, result)
                .map_err(|error| format!("encode Cursor tool result: {error}"))?,
        );
    }
    Ok(frames)
}

/// Pending native execs are split into an exposed batch (Claude Code has seen
/// these tool ids and is executing them) and a collecting batch (new execs that
/// arrived before, or unusually just after, the downstream segment closed).
/// This prevents a late parallel exec from being silently discarded.
#[derive(Debug, Default)]
struct PendingExecState {
    awaiting: Vec<PendingCursorExec>,
    collecting: Vec<PendingCursorExec>,
    seen_execs: HashSet<(u32, String)>,
    emitted_tool_use_ids: HashSet<String>,
    run_generation: Option<String>,
    awaiting_since: Option<Instant>,
    collecting_since: Option<Instant>,
    collect_deadline: Option<tokio::time::Instant>,
}

impl PendingExecState {
    fn for_run(run_id: &str) -> Self {
        Self {
            run_generation: Some(run_id.to_string()),
            ..Self::default()
        }
    }

    fn queue(&mut self, mut exec: PendingCursorExec, quiet: Duration) -> bool {
        let discriminator = exec
            .exec_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(|value| format!("exec:{value}"))
            .unwrap_or_else(|| format!("tool:{}", exec.tool_use_id));
        if !self.seen_execs.insert((exec.id, discriminator)) {
            return false;
        }

        // Bind every downstream id to this live Run. Cursor ids (including
        // fallback `exec_N` ids) may repeat after a replacement Run starts;
        // without this suffix, a delayed result from the old Run could resume
        // a same-named exec in the replacement. Collapse newlines first so
        // grok-build's `[A-Za-z0-9_-]` sanitizer keeps a single matchable token.
        exec.tool_use_id = exec.tool_use_id.replace(['\n', '\r'], "_");
        if let Some(generation) = self.run_generation.as_deref() {
            exec.tool_use_id = format!("{}__cursor_run_{generation}", exec.tool_use_id);
        }

        // Anthropic requires sibling tool_use ids to be unique. Cursor normally
        // supplies unique call ids, but some exec kinds fall back to `exec_id`;
        // if that value is reused, preserve the exec and disambiguate locally
        // rather than silently leaving Cursor waiting forever.
        if self.emitted_tool_use_ids.contains(&exec.tool_use_id) {
            let base = exec.tool_use_id.clone();
            let mut candidate = format!("{base}__cursor_{}", exec.id);
            let mut ordinal = 2_u32;
            while self.emitted_tool_use_ids.contains(&candidate) {
                candidate = format!("{base}__cursor_{}_{}", exec.id, ordinal);
                ordinal += 1;
            }
            exec.tool_use_id = candidate;
        }
        self.emitted_tool_use_ids.insert(exec.tool_use_id.clone());
        if self.collecting.is_empty() {
            self.collecting_since = Some(Instant::now());
        }
        self.collecting.push(exec);
        self.collect_deadline = Some(tokio::time::Instant::now() + quiet);
        true
    }

    fn can_expose(&self) -> bool {
        self.awaiting.is_empty() && !self.collecting.is_empty()
    }

    fn native_collect_deadline(&self) -> Option<tokio::time::Instant> {
        (self.can_expose()
            && self
                .collecting
                .iter()
                .any(|exec| !matches!(exec.kind, CursorExecKind::ClientOnly)))
        .then_some(self.collect_deadline)
        .flatten()
    }

    fn client_only_collect_deadline(&self) -> Option<tokio::time::Instant> {
        (self.can_expose()
            && !self.collecting.is_empty()
            && self
                .collecting
                .iter()
                .all(|exec| matches!(exec.kind, CursorExecKind::ClientOnly)))
        .then_some(self.collect_deadline)
        .flatten()
    }

    fn collecting_has_lifecycle(&self) -> bool {
        // MCP sibling `spawn_subagent` may flush on a quiet window.
        // XML-recovered lifecycle uses `client_only_*` exec ids and must wait
        // for `turn_ended` — tearing after the first streamed chunk dumps the
        // rest as visible `<tool_use>` XML.
        self.collecting.iter().any(|exec| {
            is_grok_build_subagent_lifecycle_tool(&exec.claude_name)
                && exec
                    .exec_id
                    .as_deref()
                    .is_some_and(|id| id.starts_with("mcp_"))
        })
    }

    fn expose(&mut self) -> Vec<PendingCursorExec> {
        if !self.can_expose() {
            return Vec::new();
        }
        let has_client_only = self
            .collecting
            .iter()
            .any(|exec| matches!(exec.kind, CursorExecKind::ClientOnly));
        let has_native = self
            .collecting
            .iter()
            .any(|exec| !matches!(exec.kind, CursorExecKind::ClientOnly));
        if has_client_only && has_native {
            // Split mixed batches: expose Workflow/Skill immediately without
            // flushing still-collecting Cursor Read/Bash into the same Anthropic
            // tool_use pause (those native execs have no Claude-local handler).
            let mut client_only = Vec::new();
            let mut native = Vec::new();
            for exec in self.collecting.drain(..) {
                if matches!(exec.kind, CursorExecKind::ClientOnly) {
                    client_only.push(exec);
                } else {
                    native.push(exec);
                }
            }
            self.collecting = native;
            self.awaiting = client_only;
            self.awaiting_since = Some(Instant::now());
            return self.awaiting.clone();
        }
        self.awaiting = std::mem::take(&mut self.collecting);
        self.awaiting_since = self
            .collecting_since
            .take()
            .or_else(|| Some(Instant::now()));
        self.collect_deadline = None;
        self.awaiting.clone()
    }

    /// Expose only tools that Cursor is blocked waiting for. Claude-local
    /// tools stay queued until an authoritative turn boundary has been
    /// drained, because a later Connect END can invalidate the whole turn.
    fn expose_native(&mut self) -> Vec<PendingCursorExec> {
        if !self.can_expose() {
            return Vec::new();
        }
        let mut native = Vec::new();
        let mut client_only = Vec::new();
        for exec in self.collecting.drain(..) {
            if matches!(exec.kind, CursorExecKind::ClientOnly) {
                client_only.push(exec);
            } else {
                native.push(exec);
            }
        }
        self.collecting = client_only;
        if native.is_empty() {
            self.collect_deadline = None;
            return Vec::new();
        }
        self.awaiting = native;
        self.awaiting_since = self
            .collecting_since
            .take()
            .or_else(|| Some(Instant::now()));
        if !self.collecting.is_empty() {
            self.collecting_since = Some(Instant::now());
        }
        self.collect_deadline = None;
        self.awaiting.clone()
    }

    fn complete_awaiting(&mut self) {
        self.awaiting.clear();
        self.awaiting_since = None;
        if !self.collecting.is_empty() && self.collect_deadline.is_none() {
            self.collect_deadline = Some(tokio::time::Instant::now());
        }
    }

    fn awaiting(&self) -> &[PendingCursorExec] {
        &self.awaiting
    }

    fn all(&self) -> impl Iterator<Item = &PendingCursorExec> {
        self.awaiting.iter().chain(&self.collecting)
    }

    fn is_empty(&self) -> bool {
        self.awaiting.is_empty() && self.collecting.is_empty()
    }

    /// True when every pending exec is Claude-local (Workflow/Skill/…) — Cursor
    /// often emits `turn_ended` in the same chunk as the XML, so we must expose
    /// these before treating pending as a hard failure.
    fn all_client_only(&self) -> bool {
        !self.is_empty()
            && self
                .all()
                .all(|exec| matches!(exec.kind, super::exec_results::CursorExecKind::ClientOnly))
    }

    fn has_outstanding_native(&self) -> bool {
        self.all()
            .any(|exec| !matches!(exec.kind, super::exec_results::CursorExecKind::ClientOnly))
    }

    #[allow(dead_code)]
    fn has_client_only_awaiting(&self) -> bool {
        self.awaiting
            .iter()
            .any(|exec| matches!(exec.kind, super::exec_results::CursorExecKind::ClientOnly))
    }

    /// Remove native execs (Read/Bash/…) so FLAG_END / turn_ended can
    /// `control_close` them instead of dropping the TCP stream. ClientOnly
    /// tools stay queued for Anthropic expose.
    fn drain_natives(&mut self) -> Vec<PendingCursorExec> {
        let mut natives = Vec::new();
        let mut client_collecting = Vec::new();
        for exec in self.collecting.drain(..) {
            if matches!(exec.kind, super::exec_results::CursorExecKind::ClientOnly) {
                client_collecting.push(exec);
            } else {
                natives.push(exec);
            }
        }
        self.collecting = client_collecting;
        if self.collecting.is_empty() {
            self.collecting_since = None;
            self.collect_deadline = None;
        }
        let mut client_awaiting = Vec::new();
        for exec in self.awaiting.drain(..) {
            if matches!(exec.kind, super::exec_results::CursorExecKind::ClientOnly) {
                client_awaiting.push(exec);
            } else {
                natives.push(exec);
            }
        }
        self.awaiting = client_awaiting;
        if self.awaiting.is_empty() {
            self.awaiting_since = None;
        }
        natives
    }

    /// Close unexposed native execs before ResumeAction. Claude-owed `awaiting`
    /// tools stay queued so their `tool_result` can be written on the new stream.
    fn drain_collecting_natives(&mut self) -> Vec<PendingCursorExec> {
        let mut natives = Vec::new();
        let mut keep = Vec::new();
        for exec in self.collecting.drain(..) {
            if matches!(exec.kind, super::exec_results::CursorExecKind::ClientOnly) {
                keep.push(exec);
            } else {
                natives.push(exec);
            }
        }
        self.collecting = keep;
        if self.collecting.is_empty() {
            self.collecting_since = None;
            self.collect_deadline = None;
        }
        natives
    }

    fn restore_collecting_natives(&mut self, natives: Vec<PendingCursorExec>) {
        if natives.is_empty() {
            return;
        }
        if self.collecting.is_empty() {
            self.collecting_since = Some(Instant::now());
        }
        self.collecting.extend(natives);
    }

    fn oldest_since(&self) -> Option<Instant> {
        match (self.awaiting_since, self.collecting_since) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        }
    }
}

#[derive(Debug, Default)]
struct LogicalToolTracker {
    named: HashSet<String>,
    anonymous_by_model: HashMap<String, usize>,
    /// Wall clock of the oldest outstanding UI tool_call_started. Heartbeats
    /// must not refresh this — otherwise we never clear stalled UI-only starts.
    oldest_since: Option<Instant>,
    /// Aggregated `partial_tool_call.args_text_delta` keyed by call_id.
    partial_args: HashMap<String, String>,
}

impl LogicalToolTracker {
    fn started(&mut self, call_id: &str, model_call_id: &str) {
        if self.is_empty() {
            self.oldest_since = Some(Instant::now());
        }
        if !call_id.is_empty() {
            self.named.insert(call_id.to_string());
        } else {
            *self
                .anonymous_by_model
                .entry(model_call_id.to_string())
                .or_default() += 1;
        }
    }

    fn remember_partial_args(&mut self, call_id: &str, model_call_id: &str, delta: &str) {
        if delta.is_empty() {
            return;
        }
        let key = if !call_id.is_empty() {
            call_id
        } else if !model_call_id.is_empty() {
            model_call_id
        } else {
            return;
        };
        let entry = self.partial_args.entry(key.to_string()).or_default();
        accumulate_partial_args_text(entry, delta);
    }

    fn partial_args_for(&self, call_id: &str, model_call_id: &str) -> Option<&str> {
        if !call_id.is_empty()
            && let Some(text) = self.partial_args.get(call_id)
        {
            return Some(text.as_str());
        }
        if !model_call_id.is_empty() {
            return self.partial_args.get(model_call_id).map(String::as_str);
        }
        None
    }

    fn completed(&mut self, call_id: &str, model_call_id: &str) {
        if !call_id.is_empty() {
            self.named.remove(call_id);
            self.partial_args.remove(call_id);
        } else {
            let mut remove_model = false;
            if let Some(count) = self.anonymous_by_model.get_mut(model_call_id) {
                *count = count.saturating_sub(1);
                remove_model = *count == 0;
            }
            if remove_model {
                self.anonymous_by_model.remove(model_call_id);
            }
        }
        if self.is_empty() {
            self.oldest_since = None;
        }
    }

    fn resolve_exec(&mut self, exec: &PendingCursorExec) {
        if self.named.remove(&exec.tool_use_id) {
            if self.is_empty() {
                self.oldest_since = None;
            }
            return;
        }
        if let Some(exec_id) = exec.exec_id.as_deref()
            && self.named.remove(exec_id)
        {
            if self.is_empty() {
                self.oldest_since = None;
            }
            return;
        }
        self.resolve_only_outstanding();
    }

    fn resolve_server_exec_hint(&mut self, exec: &proto::ExecServerMessage) {
        if let Some(tool_call_id) = exec
            .read_args
            .as_ref()
            .map(|args| args.tool_call_id.as_str())
            .filter(|value| !value.is_empty())
            && self.named.remove(tool_call_id)
        {
            if self.is_empty() {
                self.oldest_since = None;
            }
            return;
        }
        if let Some(exec_id) = exec.exec_id.as_deref()
            && self.named.remove(exec_id)
        {
            if self.is_empty() {
                self.oldest_since = None;
            }
            return;
        }
        self.resolve_only_outstanding();
    }

    fn resolve_only_outstanding(&mut self) {
        if self.len() == 1 {
            self.clear();
        }
    }

    fn len(&self) -> usize {
        self.named.len() + self.anonymous_by_model.values().sum::<usize>()
    }

    fn is_empty(&self) -> bool {
        self.named.is_empty() && self.anonymous_by_model.is_empty()
    }

    fn clear(&mut self) {
        self.named.clear();
        self.anonymous_by_model.clear();
        self.partial_args.clear();
        self.oldest_since = None;
    }

    fn oldest_since(&self) -> Option<Instant> {
        self.oldest_since
    }
}

enum LiveRunEntry {
    Starting {
        reservation_id: String,
        cancel: watch::Sender<bool>,
    },
    Running(Arc<CursorLiveRunHandle>),
    /// Open `.send()` timed out: Cursor may already have the Run. Occupy the
    /// slot so a concurrent POST cannot start a duplicate.
    Ambiguous {
        until: Instant,
    },
    /// The previous Run completed successfully. Identical request retries must
    /// not start a second Run before the client could have observed `message_end`.
    Succeeded {
        fingerprint: u64,
        until: Instant,
    },
}

struct LiveRunMap {
    /// Registry key → entry. Key is the Claude session id, or
    /// `{session}::agent::{agent_id}` for a nested Workflow/subagent run.
    runs: HashMap<String, LiveRunEntry>,
    /// Claude `X-Claude-Code-Session-Id` → all registry keys for that session.
    by_session: HashMap<String, Vec<String>>,
}

impl LiveRunMap {
    fn new() -> Self {
        Self {
            runs: HashMap::new(),
            by_session: HashMap::new(),
        }
    }

    fn insert_key(&mut self, key: String, entry: LiveRunEntry) {
        let session = claude_session_of(&key).to_string();
        self.runs.insert(key.clone(), entry);
        let slots = self.by_session.entry(session).or_default();
        if !slots.iter().any(|existing| existing == &key) {
            slots.push(key);
        }
    }

    fn remove_key(&mut self, key: &str) -> Option<LiveRunEntry> {
        let entry = self.runs.remove(key)?;
        let session = claude_session_of(key);
        if let Some(slots) = self.by_session.get_mut(session) {
            slots.retain(|existing| existing != key);
            if slots.is_empty() {
                self.by_session.remove(session);
            }
        }
        Some(entry)
    }

    fn keys_for(&self, session_id: &str) -> Vec<String> {
        self.by_session.get(session_id).cloned().unwrap_or_default()
    }
}

static LIVE_RUNS: LazyLock<Mutex<LiveRunMap>> = LazyLock::new(|| Mutex::new(LiveRunMap::new()));

/// Exclusive claim on a session id while its upstream BiDi request is being
/// established. Dropping an uncommitted reservation makes the session
/// available again after startup failure.
pub struct LiveRunReservation {
    session_id: String,
    reservation_id: String,
    committed: bool,
    seal_on_drop: bool,
    cancel: watch::Sender<bool>,
}

impl LiveRunReservation {
    pub fn cancelled(&self) -> watch::Receiver<bool> {
        self.cancel.subscribe()
    }

    fn abort(&self) {
        let _ = self.cancel.send_replace(true);
    }

    /// Once an upstream open or replacement teardown has started, dropping
    /// the owning request future cannot prove that no Cursor Run survived.
    /// Keep the slot tombstoned instead of making it available to a retry.
    pub fn protect_on_drop(&mut self) {
        self.seal_on_drop = true;
    }

    /// Release a reservation after a definitive pre-acceptance failure.
    pub fn release(mut self) {
        self.abort();
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        let owns_reservation = matches!(
            runs.runs.get(&self.session_id),
            Some(LiveRunEntry::Starting { reservation_id, .. })
                if reservation_id == &self.reservation_id
        );
        if owns_reservation {
            runs.remove_key(&self.session_id);
        }
        self.committed = true;
    }

    /// Keep the slot occupied after an ambiguous open timeout so another POST
    /// cannot start a second Run.
    pub fn seal_ambiguous(mut self, until: Instant) {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        let owns_reservation = matches!(
            runs.runs.get(&self.session_id),
            Some(LiveRunEntry::Starting { reservation_id, .. })
                if reservation_id == &self.reservation_id
        );
        if owns_reservation {
            runs.insert_key(self.session_id.clone(), LiveRunEntry::Ambiguous { until });
        }
        self.committed = true;
    }
}

impl LiveRunReservation {
    /// Atomically replace this reservation with the live handle. If Starting was
    /// removed (cancel) but the slot is vacant, adopt it so the accepted Run is
    /// not dropped and then replaced by a second `start_live`. Occupied slots
    /// return the handle so the caller can cancel without opening another Run.
    pub fn insert(
        mut self,
        handle: Arc<CursorLiveRunHandle>,
    ) -> Result<(), Arc<CursorLiveRunHandle>> {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        LiveRunRegistry::prune_finished(&mut runs);
        let owns_reservation = matches!(
            runs.runs.get(&self.session_id),
            Some(LiveRunEntry::Starting { reservation_id, .. })
                if reservation_id == &self.reservation_id
        );
        if !owns_reservation && LiveRunRegistry::key_occupied(&runs, &self.session_id) {
            return Err(handle);
        }
        // The worker can finish (and set `completed`) before this reservation
        // is published. A completed success must become a fingerprint tombstone
        // here — `seal_success_if` only sees Running entries, and prune would
        // otherwise drop the handle and allow a same-prompt retry to duplicate.
        if handle.is_completed() && !handle.has_terminal_error() {
            runs.insert_key(
                self.session_id.clone(),
                LiveRunEntry::Succeeded {
                    fingerprint: handle.request_fingerprint(),
                    until: Instant::now() + LIVE_AMBIGUOUS_OPEN_TTL,
                },
            );
        } else {
            runs.insert_key(self.session_id.clone(), LiveRunEntry::Running(handle));
        }
        self.committed = true;
        Ok(())
    }
}

impl Drop for LiveRunReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.abort();
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        let owns_reservation = matches!(
            runs.runs.get(&self.session_id),
            Some(LiveRunEntry::Starting { reservation_id, .. })
                if reservation_id == &self.reservation_id
        );
        if owns_reservation {
            if self.seal_on_drop {
                runs.insert_key(
                    self.session_id.clone(),
                    LiveRunEntry::Ambiguous {
                        until: Instant::now() + LIVE_AMBIGUOUS_OPEN_TTL,
                    },
                );
            } else {
                runs.remove_key(&self.session_id);
            }
        }
    }
}

pub enum LiveSlotClaim {
    Reserved(LiveRunReservation),
    Starting,
    Ambiguous,
    Running,
}

pub enum LiveConflictAction {
    Http409,
    SlotFree,
}

pub enum LiveReplacementClaim {
    Reserved {
        reservation: LiveRunReservation,
        superseded: Option<Arc<CursorLiveRunHandle>>,
    },
    Conflict,
}

pub enum LiveRunProbe {
    TerminalError(String),
    Occupied,
    Free,
}

pub struct LiveRunRegistry;

impl LiveRunRegistry {
    /// Claim a session before awaiting upstream startup. This closes the race
    /// where two initial requests both observed an empty registry and started
    /// separate Cursor runs. `agent_id = None` is one run per Claude session.
    pub fn reserve(session_id: &str) -> Option<LiveRunReservation> {
        Self::reserve_run(session_id, None)
    }

    pub fn reserve_run(session_id: &str, agent_id: Option<&str>) -> Option<LiveRunReservation> {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        if Self::key_occupied(&runs, &key) {
            return None;
        }
        Self::reserve_key(&mut runs, key)
    }

    /// Classify and optionally reserve under one lock. An unbound caller never
    /// supersedes an occupied slot; replacement requires an observed run id via
    /// `claim_replacement_for_run`.
    pub fn try_claim_run(session_id: &str, agent_id: Option<&str>) -> LiveSlotClaim {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Starting { .. }) => LiveSlotClaim::Starting,
            Some(LiveRunEntry::Ambiguous { until }) if Instant::now() < *until => {
                LiveSlotClaim::Ambiguous
            }
            Some(LiveRunEntry::Succeeded { until, .. }) if Instant::now() < *until => {
                LiveSlotClaim::Ambiguous
            }
            Some(LiveRunEntry::Running(handle)) if !handle.is_completed() => LiveSlotClaim::Running,
            _ => match Self::reserve_key(&mut runs, key) {
                Some(reservation) => LiveSlotClaim::Reserved(reservation),
                None => LiveSlotClaim::Running,
            },
        }
    }

    pub fn conflict_action(session_id: &str, agent_id: Option<&str>) -> LiveConflictAction {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(
                LiveRunEntry::Starting { .. }
                | LiveRunEntry::Ambiguous { .. }
                | LiveRunEntry::Succeeded { .. }
                | LiveRunEntry::Running(_),
            ) => LiveConflictAction::Http409,
            None => LiveConflictAction::SlotFree,
        }
    }

    /// Replace only the exact Running generation observed by the waiter and
    /// reserve its slot in the same lock acquisition.
    ///
    /// A stale waiter must never cancel a newer replacement, and no third
    /// request may claim the gap between cancellation and replacement.
    pub fn claim_replacement_for_run(
        session_id: &str,
        agent_id: Option<&str>,
        expected_run_id: &str,
    ) -> LiveReplacementClaim {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Running(handle)) if handle.run_id() == expected_run_id => {
                let handle = Arc::clone(handle);
                runs.remove_key(&key);
                match Self::reserve_key(&mut runs, key.clone()) {
                    Some(reservation) => LiveReplacementClaim::Reserved {
                        reservation,
                        superseded: Some(handle),
                    },
                    None => {
                        runs.insert_key(key, LiveRunEntry::Running(handle));
                        LiveReplacementClaim::Conflict
                    }
                }
            }
            Some(LiveRunEntry::Succeeded { .. } | LiveRunEntry::Ambiguous { .. }) => {
                runs.remove_key(&key);
                match Self::reserve_key(&mut runs, key) {
                    Some(reservation) => LiveReplacementClaim::Reserved {
                        reservation,
                        superseded: None,
                    },
                    None => LiveReplacementClaim::Conflict,
                }
            }
            Some(LiveRunEntry::Starting { .. } | LiveRunEntry::Running(_)) => {
                LiveReplacementClaim::Conflict
            }
            None => match Self::reserve_key(&mut runs, key) {
                Some(reservation) => LiveReplacementClaim::Reserved {
                    reservation,
                    superseded: None,
                },
                None => LiveReplacementClaim::Conflict,
            },
        }
    }

    /// After the nested wait, a fresh compact/next-turn may take a slot that
    /// has no runnable handle (Starting / Succeeded / Ambiguous). A still-
    /// Running occupant must use [`Self::claim_replacement_for_run`].
    pub fn claim_replacement_for_occupied_slot(
        session_id: &str,
        agent_id: Option<&str>,
    ) -> LiveReplacementClaim {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Running(_)) => LiveReplacementClaim::Conflict,
            Some(LiveRunEntry::Starting { cancel, .. }) => {
                let _ = cancel.send_replace(true);
                runs.remove_key(&key);
                match Self::reserve_key(&mut runs, key) {
                    Some(reservation) => LiveReplacementClaim::Reserved {
                        reservation,
                        superseded: None,
                    },
                    None => LiveReplacementClaim::Conflict,
                }
            }
            Some(LiveRunEntry::Succeeded { .. } | LiveRunEntry::Ambiguous { .. }) => {
                runs.remove_key(&key);
                match Self::reserve_key(&mut runs, key) {
                    Some(reservation) => LiveReplacementClaim::Reserved {
                        reservation,
                        superseded: None,
                    },
                    None => LiveReplacementClaim::Conflict,
                }
            }
            None => match Self::reserve_key(&mut runs, key) {
                Some(reservation) => LiveReplacementClaim::Reserved {
                    reservation,
                    superseded: None,
                },
                None => LiveReplacementClaim::Conflict,
            },
        }
    }
}

/// After a generation-bound replacement claim, decide whether a failed
/// `cancel_and_wait` may keep the Starting reservation.
///
/// `SupersedeRunning` already decided the old BiDi cannot be resumed. Keeping
/// the reservation lets the next Anthropic/grok turn start a replacement Run
/// (tool results become history). Restoring the dying handle and returning 409
/// strands grok-build, which treats 409 as non-retryable.
pub(crate) fn finish_replacement_after_cancel(
    reservation: LiveRunReservation,
    _handle: Arc<CursorLiveRunHandle>,
    _has_current_tool_results: bool,
    cancel_result: Result<(), CursorError>,
) -> Result<LiveRunReservation, CursorError> {
    match cancel_result {
        Ok(()) => Ok(reservation),
        Err(_error) => Ok(reservation),
    }
}

impl LiveRunRegistry {
    fn reserve_key(runs: &mut LiveRunMap, key: String) -> Option<LiveRunReservation> {
        if runs.runs.contains_key(&key) {
            return None;
        }
        let reservation_id = uuid::Uuid::new_v4().to_string();
        let (cancel, _) = watch::channel(false);
        runs.insert_key(
            key.clone(),
            LiveRunEntry::Starting {
                reservation_id: reservation_id.clone(),
                cancel: cancel.clone(),
            },
        );
        Some(LiveRunReservation {
            session_id: key,
            reservation_id,
            committed: false,
            seal_on_drop: true,
            cancel,
        })
    }

    fn key_occupied(runs: &LiveRunMap, key: &str) -> bool {
        match runs.runs.get(key) {
            Some(LiveRunEntry::Starting { .. }) => true,
            Some(LiveRunEntry::Ambiguous { until }) if Instant::now() < *until => true,
            Some(LiveRunEntry::Succeeded { until, .. }) if Instant::now() < *until => true,
            Some(LiveRunEntry::Running(handle)) if !handle.is_completed() => true,
            _ => false,
        }
    }

    /// Drop the live run for this Claude session (primary / no agent_id).
    pub fn cancel(session_id: &str) -> bool {
        Self::cancel_run(session_id, None)
    }

    /// Cancel only the `(session_id, agent_id)` slot. Nested agents must pass
    /// their agent id so the parent run is left alone.
    pub fn cancel_run(session_id: &str, agent_id: Option<&str>) -> bool {
        let key = live_run_key(session_id, agent_id);
        let entry = {
            let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
            Self::prune_finished(&mut runs);
            if matches!(
                runs.runs.get(&key),
                Some(LiveRunEntry::Ambiguous { .. } | LiveRunEntry::Succeeded { .. })
            ) {
                return true;
            }
            runs.remove_key(&key)
        };
        match entry {
            Some(LiveRunEntry::Running(handle)) => {
                handle.cancel();
                true
            }
            Some(LiveRunEntry::Starting { cancel, .. }) => {
                let _ = cancel.send_replace(true);
                true
            }
            Some(LiveRunEntry::Ambiguous { .. } | LiveRunEntry::Succeeded { .. }) | None => false,
        }
    }

    /// Cancel a Running occupant only. Starting and Ambiguous slots stay put so
    /// a Conflict retry cannot abort an in-flight `.send()` and start another Run.
    pub fn cancel_running_only(session_id: &str, agent_id: Option<&str>) -> bool {
        let key = live_run_key(session_id, agent_id);
        let handle = {
            let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
            Self::prune_finished(&mut runs);
            match runs.runs.get(&key) {
                Some(LiveRunEntry::Running(handle)) if !handle.is_completed() => {
                    let handle = Arc::clone(handle);
                    runs.remove_key(&key);
                    Some(handle)
                }
                _ => None,
            }
        };
        if let Some(handle) = handle {
            handle.cancel();
            true
        } else {
            false
        }
    }

    /// Cancel a **Running** occupant of this slot, then reserve. An in-flight
    /// Starting open is left alone — aborting `.send()` and opening another Run
    /// duplicates a request Cursor may already have accepted.
    pub fn supersede(session_id: &str) -> Option<LiveRunReservation> {
        Self::supersede_run(session_id, None)
    }

    pub fn supersede_run(session_id: &str, agent_id: Option<&str>) -> Option<LiveRunReservation> {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Starting { .. })
            | Some(LiveRunEntry::Ambiguous { .. })
            | Some(LiveRunEntry::Succeeded { .. }) => {
                return None;
            }
            Some(LiveRunEntry::Running(handle)) if !handle.is_completed() => {}
            Some(LiveRunEntry::Running(_)) | None => {
                return Self::reserve_key(&mut runs, key);
            }
        }
        let Some(LiveRunEntry::Running(handle)) = runs.remove_key(&key) else {
            return Self::reserve_key(&mut runs, key);
        };
        let reservation = Self::reserve_key(&mut runs, key);
        drop(runs);
        handle.cancel();
        reservation
    }

    pub fn get(session_id: &str) -> Option<Arc<CursorLiveRunHandle>> {
        Self::get_run(session_id, None)
    }

    pub fn get_run(session_id: &str, agent_id: Option<&str>) -> Option<Arc<CursorLiveRunHandle>> {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Running(handle))
                if !handle.is_completed()
                    && !handle.is_cancel_requested()
                    && !handle.has_terminal_error() =>
            {
                Some(Arc::clone(handle))
            }
            _ => None,
        }
    }

    /// Run id of a `Running` occupant, including handles `get_run` hides
    /// (cancel already requested). Starting / Ambiguous / Succeeded have no id.
    pub fn running_generation(session_id: &str, agent_id: Option<&str>) -> Option<String> {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Running(handle)) => Some(handle.run_id().to_string()),
            _ => None,
        }
    }

    /// True while a reservation or live handle owns this Claude session slot
    /// (no agent id). Nested occupancy is [`Self::is_occupied_run`].
    pub fn is_occupied(session_id: &str) -> bool {
        Self::is_occupied_run(session_id, None)
    }

    pub fn is_occupied_run(session_id: &str, agent_id: Option<&str>) -> bool {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        Self::key_occupied(&runs, &key)
    }

    /// Atomically consume a terminal outcome or classify the current slot.
    pub fn probe_run(session_id: &str, agent_id: Option<&str>) -> LiveRunProbe {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        let error = match runs.runs.get(&key) {
            Some(LiveRunEntry::Running(handle)) if handle.is_completed() => {
                handle.take_terminal_error()
            }
            _ => None,
        };
        if let Some(error) = error {
            if terminal_error_clears_live_slot(&error) {
                runs.remove_key(&key);
                return LiveRunProbe::Free;
            }
            if terminal_error_is_ambiguous_accept(&error) {
                runs.insert_key(
                    key,
                    LiveRunEntry::Ambiguous {
                        until: Instant::now() + LIVE_AMBIGUOUS_OPEN_TTL,
                    },
                );
            } else {
                runs.remove_key(&key);
            }
            return LiveRunProbe::TerminalError(error);
        }
        match runs.runs.get(&key) {
            Some(
                LiveRunEntry::Starting { .. }
                | LiveRunEntry::Ambiguous { .. }
                | LiveRunEntry::Succeeded { .. }
                | LiveRunEntry::Running(_),
            ) => LiveRunProbe::Occupied,
            None => LiveRunProbe::Free,
        }
    }

    /// True while this slot is reserved but the first `Run` has not been
    /// inserted yet. Concurrent POSTs must wait or 409 — not start a second Run.
    pub fn is_starting_run(session_id: &str, agent_id: Option<&str>) -> bool {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        matches!(runs.runs.get(&key), Some(LiveRunEntry::Starting { .. }))
    }

    pub fn is_ambiguous_run(session_id: &str, agent_id: Option<&str>) -> bool {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        matches!(
            runs.runs.get(&key),
            Some(LiveRunEntry::Ambiguous { until }) if Instant::now() < *until
        )
    }

    /// Clear an Ambiguous tombstone for a next-turn POST (compact / new
    /// inference). Starting reservations and Running handles are left alone so
    /// an in-flight open cannot be aborted by a concurrent waiter.
    pub fn take_ambiguous_tombstone(session_id: &str, agent_id: Option<&str>) -> bool {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Ambiguous { .. }) => {
                runs.remove_key(&key);
                true
            }
            _ => false,
        }
    }

    pub fn take_terminal_error(session_id: &str) -> Option<String> {
        Self::take_terminal_error_run(session_id, None)
    }

    pub fn take_terminal_error_run(session_id: &str, agent_id: Option<&str>) -> Option<String> {
        Self::take_terminal_error_matching_run(session_id, agent_id, None)
    }

    pub fn take_terminal_error_for_run(
        session_id: &str,
        agent_id: Option<&str>,
        expected_run_id: &str,
    ) -> Option<String> {
        Self::take_terminal_error_matching_run(session_id, agent_id, Some(expected_run_id))
    }

    fn take_terminal_error_matching_run(
        session_id: &str,
        agent_id: Option<&str>,
        expected_run_id: Option<&str>,
    ) -> Option<String> {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        let error = match runs.runs.get(&key) {
            Some(LiveRunEntry::Running(handle))
                if handle.is_completed()
                    && expected_run_id.is_none_or(|expected| handle.run_id() == expected) =>
            {
                handle.take_terminal_error()
            }
            Some(LiveRunEntry::Starting { .. })
            | Some(LiveRunEntry::Ambiguous { .. })
            | Some(LiveRunEntry::Succeeded { .. })
            | None => None,
            Some(LiveRunEntry::Running(_)) => None,
        };
        let allows_fresh_retry = error
            .as_deref()
            .is_some_and(terminal_error_allows_fresh_retry);
        if error.is_some() {
            if error
                .as_deref()
                .is_some_and(terminal_error_is_ambiguous_accept)
            {
                runs.insert_key(
                    key,
                    LiveRunEntry::Ambiguous {
                        until: Instant::now() + LIVE_AMBIGUOUS_OPEN_TTL,
                    },
                );
            } else {
                runs.remove_key(&key);
            }
        }
        if allows_fresh_retry { None } else { error }
    }

    fn prune_finished(runs: &mut LiveRunMap) {
        let stale: Vec<String> = runs
            .runs
            .iter()
            .filter_map(|(key, entry)| match entry {
                LiveRunEntry::Starting { .. } => None,
                LiveRunEntry::Ambiguous { until } if Instant::now() < *until => None,
                LiveRunEntry::Ambiguous { .. } => Some(key.clone()),
                LiveRunEntry::Succeeded { until, .. } if Instant::now() < *until => None,
                LiveRunEntry::Succeeded { .. } => Some(key.clone()),
                LiveRunEntry::Running(handle)
                    if !handle.is_completed() || handle.has_terminal_error() =>
                {
                    None
                }
                LiveRunEntry::Running(_) => Some(key.clone()),
            })
            .collect();
        for key in stale {
            runs.remove_key(&key);
        }
    }

    fn seal_success_if(session_id: &str, run_id: &str) {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        let keys = runs.keys_for(claude_session_of(session_id));
        let mut extra = Vec::new();
        if !keys.iter().any(|k| k == session_id) {
            extra.push(session_id.to_string());
        }
        for key in keys.into_iter().chain(extra) {
            let fingerprint = match runs.runs.get(&key) {
                Some(LiveRunEntry::Running(handle)) if handle.run_id == run_id => {
                    Some(handle.request_fingerprint())
                }
                _ => None,
            };
            if let Some(fingerprint) = fingerprint {
                runs.insert_key(
                    key,
                    LiveRunEntry::Succeeded {
                        fingerprint,
                        until: Instant::now() + LIVE_AMBIGUOUS_OPEN_TTL,
                    },
                );
                return;
            }
        }
    }

    pub fn release_success_if_new_request(
        session_id: &str,
        agent_id: Option<&str>,
        fingerprint: u64,
    ) {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Succeeded {
                fingerprint: stored,
                until,
            }) if Instant::now() < *until && *stored == fingerprint => {}
            Some(LiveRunEntry::Succeeded { .. }) => {
                runs.remove_key(&key);
            }
            _ => {}
        }
    }

    #[cfg(test)]
    pub fn clear() {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        for entry in runs.runs.values() {
            if let LiveRunEntry::Running(handle) = entry {
                handle.cancel();
            }
        }
        runs.runs.clear();
        runs.by_session.clear();
    }
}

impl CursorHttpClient {
    pub fn live_bidi_enabled(&self) -> bool {
        match std::env::var("CCP_CURSOR_BIDI")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => !self.base_url.starts_with("http://"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_live_agent(
        &self,
        token: &str,
        prompt: &str,
        model: &str,
        images: &[CursorSelectedImage],
        custom_system_prompt: Option<&str>,
        session_id: &str,
        allowed_tool_names: Option<BTreeSet<String>>,
        mcp_tools: Option<super::proto::McpTools>,
    ) -> Result<LiveRunStart, CursorError> {
        self.start_live_agent_with_identity(
            token,
            prompt,
            model,
            images,
            custom_system_prompt,
            LiveRunIdentity::parent(session_id),
            allowed_tool_names,
            mcp_tools,
            super::proto::RequestContext::default(),
            None,
        )
        .await
    }

    /// Start a BiDi run keyed by `(session_id, agent_id)`. Nested Workflow
    /// agents must pass `x-claude-code-agent-id` so they do not steal the
    /// parent's Cursor conversation / live slot.
    #[allow(clippy::too_many_arguments)]
    pub async fn start_live_agent_with_identity(
        &self,
        token: &str,
        prompt: &str,
        model: &str,
        images: &[CursorSelectedImage],
        custom_system_prompt: Option<&str>,
        identity: LiveRunIdentity<'_>,
        allowed_tool_names: Option<BTreeSet<String>>,
        mcp_tools: Option<super::proto::McpTools>,
        request_context: super::proto::RequestContext,
        mut cancel: Option<watch::Receiver<bool>>,
    ) -> Result<LiveRunStart, CursorError> {
        if !self.live_bidi_enabled() {
            return Err(CursorError::internal(
                "Cursor live agent is disabled for this transport",
            ));
        }
        let generation_permit = acquire_live_generation_permit(cancel.as_mut()).await?;

        let force_http1 = live_open_prefers_http1();
        let http = if force_http1 && !self.prefers_http1() {
            CursorHttpClient::with_prefer_http1(true)
        } else {
            self.clone()
        };
        let resolved = super::model::resolve_cursor_model(model)
            .map_err(|e| CursorError::internal(format!("model resolution: {e}")))?;
        let request_id = uuid::Uuid::new_v4().to_string();
        let worker_session = live_run_key_for(identity);
        let continuation = super::conversation::continuation_for(Some(&worker_session));
        if std::env::var_os("CCP_CURSOR_DEBUG").is_some() {
            let names: Vec<&str> = mcp_tools
                .as_ref()
                .map(|m| m.tools.iter().map(|t| t.name.as_str()).collect())
                .unwrap_or_default();
            eprintln!(
                "[ccp-cursor] start_live_agent session={} agent={:?} parent_agent={:?} nested={} mcp_tools={:?} count={}",
                identity.session_id,
                identity.agent_id,
                identity.parent_agent_id,
                identity.is_nested(),
                names,
                names.len()
            );
        }
        let run_request = build_run_request_with_continuation(
            prompt,
            &resolved,
            images,
            &request_id,
            custom_system_prompt,
            &continuation,
            mcp_tools.clone(),
        );
        let first_message = AgentClientMessage {
            run_request: Some(run_request),
            exec_client_message: None,
            kv_client_message: None,
            exec_client_control_message: None,
            interaction_response: None,
            client_heartbeat: None,
        };

        let cursor_identity = LiveIdentityHeaders::build(token);
        let open = http.open_live_transport(
            token,
            &request_id,
            &first_message,
            &cursor_identity,
            force_http1,
            live_reconnect_allow_h1_fallback(force_http1, false),
            live_h2_open_attempt_timeout(),
            live_h1_open_attempt_timeout(),
        );
        let opened = if let Some(rx) = cancel.as_mut() {
            tokio::select! {
                _ = rx.wait_for(|aborted| *aborted) => {
                    Err(CursorError::new(
                        409,
                        "Cursor live open superseded; acceptance is ambiguous",
                        None,
                    ))
                }
                result = open => result,
            }
        } else {
            open.await
        };
        let (outbound, response) = match opened {
            Ok(pair) => pair,
            Err(err) => return Err(annotate_live_cursor_error(&worker_session, err)),
        };
        let force_http1 = matches!(outbound, ClientOutbound::Http1(_));

        // Larger fan-out so token deltas don't block the BiDi read loop under
        // Claude Code backpressure (coalescing in live_sse_response).
        let (event_tx, events) = mpsc::channel(LIVE_EVENT_CHANNEL_CAP);
        let (command_tx, command_rx) = mpsc::channel(8);
        let pending = Arc::new(Mutex::new(Vec::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let run_id = uuid::Uuid::new_v4().to_string();
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: run_id.clone(),
            command_tx,
            pending: Arc::clone(&pending),
            terminal_error: Arc::clone(&terminal_error),
            completed: Arc::clone(&completed),
            cancel_requested: Arc::clone(&cancel_requested),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });

        let seeded_blobs: HashMap<Vec<u8>, Vec<u8>> =
            continuation.pre_fetched_blobs.into_iter().collect();
        let reconnect = LiveReconnectContext {
            http,
            token: token.to_string(),
            identity: cursor_identity,
            session_id: worker_session.clone(),
            model_id: resolved.model_id.clone(),
            conversation_id: continuation.conversation_id.clone(),
            force_http1,
            http1_rejected: false,
            mcp_tools: mcp_tools.clone(),
            opening_checkpoint: opening_live_checkpoint(&continuation.conversation_state),
            recovery: LiveRecoveryEpisode::default(),
            breakers: TransportBreakers::default(),
            last_trigger: String::new(),
        };
        // Match event fan-out: a tiny upstream queue stalls the reqwest body
        // pump (and Cursor's TCP window) during thinking bursts.
        let (upstream_tx, upstream_rx) =
            mpsc::channel::<Result<Option<Bytes>, String>>(LIVE_EVENT_CHANNEL_CAP);
        let upstream_pump = spawn_upstream_pump(response.bytes_stream(), upstream_tx.clone());
        tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx,
            upstream_pump,
            outbound,
            command_rx,
            event_tx,
            pending,
            terminal_error,
            completed,
            cancel_requested,
            allowed_tool_names,
            worker_session,
            run_id,
            seeded_blobs,
            prompt.to_string(),
            request_context,
            reconnect,
            generation_permit,
        ));

        Ok(LiveRunStart { handle, events })
    }

    /// Open BiDi `Run` or HTTP/1 `RunSSE`+`BidiAppend`. When BiDi fails with a
    /// transport-ish status (CLI: FORCE_BIDI_DISABLED / proxy 464), retry once via H1.
    /// Each transport attempt has its own timeout so an H2 hang still leaves
    /// budget for HTTP/1.
    #[allow(clippy::too_many_arguments)]
    async fn open_live_transport(
        &self,
        token: &str,
        request_id: &str,
        first_message: &AgentClientMessage,
        identity: &LiveIdentityHeaders,
        force_http1: bool,
        allow_h1_fallback: bool,
        h2_timeout: Duration,
        h1_timeout: Duration,
    ) -> Result<(ClientOutbound, reqwest::Response), CursorError> {
        if force_http1 {
            return with_live_open_timeout(
                h1_timeout,
                self.open_http1_run_sse(token, request_id, first_message, identity),
            )
            .await;
        }

        let started = Instant::now();
        match with_live_open_timeout(
            h2_timeout,
            self.open_h2_bidi_run(token, request_id, first_message, identity),
        )
        .await
        {
            Ok(pair) => {
                note_process_h2_open_success();
                Ok(pair)
            }
            Err(err) if allow_h1_fallback && live_open_should_retry_http1(&err) => {
                if is_ambiguous_live_open_timeout(&err) {
                    note_process_h2_open_timeout();
                }
                let wait = if is_ambiguous_live_open_timeout(&err) {
                    h1_timeout
                } else {
                    match live_h1_fallback_budget(h1_timeout, started.elapsed()) {
                        Some(wait) => wait,
                        None => return Err(err),
                    }
                };
                if std::env::var("CCP_CURSOR_DEBUG").is_ok() {
                    eprintln!(
                        "[ccp-cursor] BiDi Run failed ({}); falling back to RunSSE+BidiAppend",
                        err.status
                    );
                }
                let h1 = CursorHttpClient::with_prefer_http1(true);
                match with_live_open_timeout(
                    wait,
                    h1.open_http1_run_sse(token, request_id, first_message, identity),
                )
                .await
                {
                    Ok(pair) => Ok(pair),
                    Err(_) if is_ambiguous_live_open_timeout(&err) => Err(err),
                    Err(h1_err) => Err(h1_err),
                }
            }
            Err(err) => {
                if is_ambiguous_live_open_timeout(&err) {
                    note_process_h2_open_timeout();
                }
                Err(err)
            }
        }
    }

    async fn open_http1_run_sse(
        &self,
        token: &str,
        request_id: &str,
        first_message: &AgentClientMessage,
        identity: &LiveIdentityHeaders,
    ) -> Result<(ClientOutbound, reqwest::Response), CursorError> {
        let run_url = format!(
            "{}/agent.v1.AgentService/RunSSE",
            self.base_url.trim_end_matches('/')
        );
        let sse_body = http1::encode_run_sse_request(request_id)?;
        let mut request = self
            .client
            .post(&run_url)
            .bearer_auth(token)
            .header("content-type", "application/connect+proto")
            .header("connect-protocol-version", "1")
            .header("connect-accept-encoding", "gzip")
            .header("user-agent", "connect-es/1.6.1")
            .header("x-cursor-client-type", &identity.client_type)
            .header("x-cursor-client-version", &identity.client_version)
            .header("x-ghost-mode", &identity.ghost_mode)
            .header("x-request-id", request_id)
            .header("x-cursor-streaming", "true")
            .header("x-original-request-id", request_id);
        for (name, value) in &identity.headers {
            if name.starts_with("x-cursor-client-device")
                || name.starts_with("x-cursor-client-os")
                || name.starts_with("x-cursor-client-arch")
                || name == "x-cursor-checksum"
            {
                request = request.header(name.as_str(), value.as_str());
            }
        }
        if identity.ide_profile {
            request = request
                .header("x-new-onboarding-completed", "true")
                .header("x-amzn-trace-id", format!("Root={request_id}"));
        }
        let response = request
            .body(sse_body)
            .send()
            .await
            .map_err(|e| CursorError::from_reqwest(e, self.timeout_secs))?;
        let status = response.status().as_u16();
        if status >= 400 {
            let detail = response.text().await.ok();
            return Err(CursorError::new(
                status,
                format!("Cursor RunSSE HTTP {status}"),
                detail,
            ));
        }

        let append = BidiAppendSession::new(
            self.client.clone(),
            self.base_url.clone(),
            token.to_string(),
            request_id.to_string(),
            identity.headers.clone(),
        );
        append
            .append_message(first_message)
            .await
            .map_err(|error| ambiguous_http1_append_error(error, "initial Run"))?;
        Ok((ClientOutbound::Http1(append), response))
    }

    async fn open_h2_bidi_run(
        &self,
        token: &str,
        request_id: &str,
        first_message: &AgentClientMessage,
        identity: &LiveIdentityHeaders,
    ) -> Result<(ClientOutbound, reqwest::Response), CursorError> {
        let first_frame = encode_agent_message(first_message)?;
        let (request_tx, request_rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(32);
        request_tx
            .send(Ok(first_frame))
            .await
            .map_err(|_| CursorError::internal("Cursor request channel closed at startup"))?;
        let heartbeat_tx = request_tx.clone();
        let heartbeat_secs = env_u64("CCP_CURSOR_HEARTBEAT_SECS", 5);
        let heartbeat_task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(heartbeat_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Ok(frame) = encode_client_heartbeat_frame() else {
                    break;
                };
                if heartbeat_tx.send(Ok(frame)).await.is_err() {
                    break;
                }
            }
        });
        let request_body = futures_util::stream::unfold(request_rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });

        let url = format!(
            "{}/agent.v1.AgentService/Run",
            self.base_url.trim_end_matches('/')
        );
        let mut request = self
            .client
            .post(&url)
            .bearer_auth(token)
            .header("content-type", "application/connect+proto")
            .header("connect-protocol-version", "1")
            .header("connect-accept-encoding", "gzip")
            .header("user-agent", "connect-es/1.6.1")
            .header("x-cursor-client-type", &identity.client_type)
            .header("x-cursor-client-version", &identity.client_version)
            .header("x-ghost-mode", &identity.ghost_mode)
            .header("x-request-id", request_id)
            .header("x-original-request-id", request_id);

        if identity.ide_profile {
            request = request
                .header("x-cursor-client-device-type", "desktop")
                .header("x-cursor-client-os", crate::config::cursor_client_os())
                .header("x-cursor-client-arch", crate::config::cursor_client_arch())
                .header("x-new-onboarding-completed", "true")
                .header("x-amzn-trace-id", format!("Root={request_id}"));
            if let Some(commit) = crate::config::cursor_client_commit() {
                request = request.header("x-cursor-client-commit", commit);
            }
            if let Some(tz) = crate::config::cursor_timezone() {
                request = request.header("x-cursor-timezone", tz);
            }
            if let Some(key) = crate::config::cursor_client_key() {
                request = request.header("x-client-key", key);
            }
            if let Some(sid) = crate::config::cursor_session_id() {
                request = request.header("x-session-id", sid);
            }
        }
        if let Some(cs) = identity
            .headers
            .iter()
            .find(|(n, _)| n == "x-cursor-checksum")
        {
            request = request.header("x-cursor-checksum", &cs.1);
        }

        let send_result = request
            .body(reqwest::Body::wrap_stream(request_body))
            .send()
            .await;
        heartbeat_task.abort();
        let response = send_result.map_err(|e| CursorError::from_reqwest(e, self.timeout_secs))?;
        let status = response.status().as_u16();
        if status >= 400 {
            let detail = response.text().await.ok();
            return Err(CursorError::new(
                status,
                format!("Cursor upstream HTTP {status}"),
                detail,
            ));
        }
        Ok((ClientOutbound::Bidi(request_tx), response))
    }
}

struct LiveIdentityHeaders {
    client_type: String,
    client_version: String,
    ghost_mode: String,
    ide_profile: bool,
    headers: Vec<(String, String)>,
}

impl LiveIdentityHeaders {
    fn build(token: &str) -> Self {
        let client_version = crate::config::cursor_client_version();
        let client_type = crate::config::cursor_client_type();
        let ghost_mode = crate::config::cursor_ghost_mode().to_string();
        let profile = crate::config::cursor_client_profile();
        let ide_profile = profile.eq_ignore_ascii_case("ide");

        let mut headers: Vec<(String, String)> = vec![
            ("x-cursor-client-type".into(), client_type.clone()),
            ("x-cursor-client-version".into(), client_version.clone()),
            ("x-ghost-mode".into(), ghost_mode.clone()),
        ];
        if ide_profile {
            headers.push(("x-cursor-client-device-type".into(), "desktop".into()));
            headers.push((
                "x-cursor-client-os".into(),
                crate::config::cursor_client_os(),
            ));
            headers.push((
                "x-cursor-client-arch".into(),
                crate::config::cursor_client_arch(),
            ));
        }

        let checksum_mode = std::env::var("CCP_CURSOR_CHECKSUM_MODE").unwrap_or_else(|_| {
            if ide_profile {
                "token".into()
            } else {
                "none".into()
            }
        });
        let checksum = if !matches!(
            checksum_mode.to_ascii_lowercase().as_str(),
            "none" | "off" | "0"
        ) {
            if checksum_mode.eq_ignore_ascii_case("storage") {
                let ids = super::identity::load_cursor_machine_ids();
                ids.machine_id.as_ref().map(|machine_id| {
                    super::identity::build_cursor_checksum(
                        machine_id,
                        ids.mac_machine_id.as_deref(),
                    )
                })
            } else {
                Some(super::identity::build_cursor_checksum_for_token(token))
            }
        } else {
            None
        };
        if let Some(cs) = checksum {
            headers.push(("x-cursor-checksum".into(), cs));
        }

        Self {
            client_type,
            client_version,
            ghost_mode,
            ide_profile,
            headers,
        }
    }
}

/// Context needed to reopen AgentService/Run with `ResumeAction` after a stall.
#[derive(Debug, Default)]
struct LiveRecoveryEpisode {
    started: Option<Instant>,
    opens: u32,
    last_was_hollow: bool,
    on_probation: bool,
}

impl LiveRecoveryEpisode {
    fn reset(&mut self) {
        *self = Self::default();
    }

    fn begin(&mut self, now: Instant) {
        if self.started.is_none() {
            self.started = Some(now);
        }
    }

    fn note_delayed_hollow_if_probation(&mut self) {
        if self.on_probation {
            self.last_was_hollow = true;
            self.on_probation = false;
        }
    }

    fn remaining(&self, now: Instant, force_http1: bool) -> Duration {
        let budget = live_recovery_budget(force_http1);
        let Some(started) = self.started else {
            return budget;
        };
        budget.saturating_sub(now.saturating_duration_since(started))
    }

    fn skip_reason(&self, now: Instant, force_http1: bool) -> Option<&'static str> {
        if self.opens >= LIVE_RECOVERY_MAX_OPENS {
            return Some("recovery open budget exhausted");
        }
        if self.started.is_some() && self.remaining(now, force_http1).is_zero() {
            return Some("recovery deadline exhausted");
        }
        None
    }
}

/// Context needed to reopen AgentService/Run with `ResumeAction` after a stall.
struct LiveReconnectContext {
    http: CursorHttpClient,
    token: String,
    identity: LiveIdentityHeaders,
    session_id: String,
    model_id: String,
    conversation_id: Option<String>,
    force_http1: bool,
    /// Clash/Surge 464/421 while on HTTP/1. Do not oscillate back to H1.
    http1_rejected: bool,
    mcp_tools: Option<super::proto::McpTools>,
    opening_checkpoint: Option<Vec<u8>>,
    recovery: LiveRecoveryEpisode,
    breakers: TransportBreakers,
    last_trigger: String,
}

type LiveUpstream = mpsc::Receiver<Result<Option<Bytes>, String>>;

/// Pump a reqwest body stream into an mpsc so the driver can `select!` and
/// swap transports on ResumeAction reconnect without Pin gymnastics.
///
/// Sends `Ok(None)` exactly once when the HTTP body ends so the driver sees EOF
/// even while it still holds a clone of the sender for reconnect pumps.
fn spawn_upstream_pump<S>(
    stream: S,
    tx: mpsc::Sender<Result<Option<Bytes>, String>>,
) -> tokio::task::JoinHandle<()>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Send + Unpin + 'static,
{
    spawn_upstream_pump_prefixed(None, stream, tx)
}

fn spawn_upstream_pump_prefixed<S>(
    prefix: Option<Bytes>,
    stream: S,
    tx: mpsc::Sender<Result<Option<Bytes>, String>>,
) -> tokio::task::JoinHandle<()>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        if let Some(chunk) = prefix
            && tx.send(Ok(Some(chunk))).await.is_err()
        {
            return;
        }
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            let mapped = match item {
                Ok(chunk) => Ok(Some(chunk)),
                Err(e) => Err(format_error_chain(&e)),
            };
            if tx.send(mapped).await.is_err() {
                return;
            }
        }
        let _ = tx.send(Ok(None)).await;
    })
}

/// Drop the previous generation's receiver so a retired pump's `Ok(None)` cannot
/// be consumed as EOF on a healthy replacement stream.
fn fence_live_upstream(
    upstream: &mut LiveUpstream,
    upstream_tx: &mut mpsc::Sender<Result<Option<Bytes>, String>>,
) -> mpsc::Sender<Result<Option<Bytes>, String>> {
    let (new_tx, new_rx) = mpsc::channel(LIVE_EVENT_CHANNEL_CAP);
    *upstream_tx = new_tx.clone();
    *upstream = new_rx;
    new_tx
}

fn live_reconnect_should_reset_budget(frames: &[ConnectFrame]) -> bool {
    frames.iter().any(connect_frame_resets_reconnect_budget)
}

fn connect_frame_resets_reconnect_budget(frame: &ConnectFrame) -> bool {
    if frame.payload.is_empty() {
        return false;
    }
    let decoded = if frame.flags & FLAG_GZIP != 0 {
        match decode_gzip_frame(&frame.payload) {
            Ok(bytes) => proto::AgentServerMessage::decode(bytes.as_slice()),
            Err(_) => return false,
        }
    } else {
        proto::AgentServerMessage::decode(frame.payload.as_ref())
    };
    match decoded {
        Ok(message) => server_message_resets_reconnect_budget(&message),
        Err(_) => false,
    }
}

fn connect_frame_has_top_level_turn_ended(frame: &ConnectFrame) -> bool {
    if frame.flags & FLAG_END != 0 || frame.payload.is_empty() {
        return false;
    }
    let decoded = if frame.flags & FLAG_GZIP != 0 {
        match decode_gzip_frame(&frame.payload) {
            Ok(bytes) => proto::AgentServerMessage::decode(bytes.as_slice()),
            Err(_) => return false,
        }
    } else {
        proto::AgentServerMessage::decode(frame.payload.as_ref())
    };
    decoded
        .ok()
        .and_then(|message| message.interaction_update)
        .and_then(|update| update.turn_ended)
        .is_some()
}

fn server_message_resets_reconnect_budget(message: &proto::AgentServerMessage) -> bool {
    if let Some(exec) = message.exec_server_message.as_ref() {
        return exec.request_context_args.is_none();
    }
    let Some(update) = message.interaction_update.as_ref() else {
        return false;
    };
    !heartbeat_only_interaction(update) && interaction_has_model_progress(update)
}

fn heartbeat_only_interaction(update: &InteractionUpdate) -> bool {
    update.heartbeat.is_some()
        && update.text_delta.is_none()
        && update.thinking_delta.is_none()
        && update.tool_call_started.is_none()
        && update.tool_call_completed.is_none()
        && update.thinking_completed.is_none()
        && update.partial_tool_call.is_none()
        && update.token_delta.is_none()
        && update.turn_ended.is_none()
        && update.tool_call_delta.is_none()
}

fn interaction_has_model_progress(update: &InteractionUpdate) -> bool {
    update.text_delta.is_some()
        || update.thinking_delta.is_some()
        || update.tool_call_started.is_some()
        || update.tool_call_completed.is_some()
        || update.thinking_completed.is_some()
        || update.partial_tool_call.is_some()
        || update.token_delta.is_some()
        || update.turn_ended.is_some()
        || update.tool_call_delta.is_some()
}

/// Reconnect transport is owned by `force_http1`, not `CCP_CURSOR_HTTP1`.
/// Otherwise 464 flip-back still builds an `http1_only` client.
fn reconnect_prefers_http1(force_http1: bool) -> bool {
    force_http1
}

pub(crate) const LIVE_H2_OPEN_ATTEMPT: Duration = Duration::from_secs(20);
const LIVE_H1_OPEN_ATTEMPT: Duration = Duration::from_secs(90);
pub(crate) const LIVE_AMBIGUOUS_OPEN_TTL: Duration = Duration::from_secs(90);
const LIVE_RECOVERY_DEADLINE: Duration = Duration::from_secs(45);
const LIVE_RECOVERY_MAX_OPENS: u32 = 4;
const LIVE_RECONNECT_BACKOFF_CAP_MS: u64 = 8_000;
const LIVE_RECONNECT_BACKOFF_BASE_MS: u64 = 1_000;
const LIVE_OPEN_SOFT_START: usize = 4;
const LIVE_OPEN_MAX: usize = 128;
const LIVE_GENERATION_DEFAULT_MAX: usize = 16;
const LIVE_GENERATION_MAX: usize = 128;
const LIVE_GENERATION_DEFAULT_QUEUE_SECS: u64 = 60;
const TRANSPORT_BREAKER_THRESHOLD: u32 = 3;
const TRANSPORT_BREAKER_COOLDOWN: Duration = Duration::from_secs(30);

/// H2 ResumeAction stays on the 45s episode. After a mid-stream
/// `INTERNAL_ERROR` the next open is HTTP/1 and must use the same budget as
/// a first HTTP/1 open — a 45s cap is how gemini-3.6-flash still died after
/// the flat 10s ResumeAction cap was removed.
fn live_recovery_budget(force_http1: bool) -> Duration {
    if force_http1 {
        live_h1_open_attempt_timeout()
    } else {
        LIVE_RECOVERY_DEADLINE
    }
}

/// ResumeAction open is fatal on timeout (acceptance is ambiguous), so the
/// first attempt must use the same budget as a first open. A flat 10s cap
/// killed HTTP/1 resumes after H2 INTERNAL_ERROR before Cursor answered.
fn live_reconnect_open_timeout(remaining: Duration, force_http1: bool) -> Duration {
    let cap = if force_http1 {
        live_h1_open_attempt_timeout()
    } else {
        live_h2_open_attempt_timeout()
    };
    remaining.min(cap)
}

fn live_h2_open_attempt_timeout() -> Duration {
    Duration::from_secs(
        env_u64(
            "CCP_CURSOR_LIVE_H2_OPEN_SECS",
            LIVE_H2_OPEN_ATTEMPT.as_secs(),
        )
        .clamp(10, 60),
    )
}

fn live_h1_open_attempt_timeout() -> Duration {
    Duration::from_secs(
        env_u64("CCP_CURSOR_LIVE_OPEN_SECS", LIVE_H1_OPEN_ATTEMPT.as_secs()).clamp(30, 180),
    )
}

fn live_hard_timeout_secs() -> u64 {
    env_u64("CCP_CURSOR_LIVE_TIMEOUT_SECS", 1800).min(3600)
}

fn live_h1_fallback_budget(h1_timeout: Duration, elapsed: Duration) -> Option<Duration> {
    let leftover = h1_timeout.saturating_sub(elapsed);
    (!leftover.is_zero()).then_some(leftover)
}

async fn with_live_open_timeout<T>(
    per_attempt: Duration,
    fut: impl std::future::Future<Output = Result<T, CursorError>>,
) -> Result<T, CursorError> {
    let _permit = LIVE_OPEN_GATE.acquire(per_attempt).await?;
    match tokio::time::timeout(per_attempt, fut).await {
        Ok(Ok(value)) => {
            LIVE_OPEN_GATE.on_success();
            Ok(value)
        }
        Ok(Err(err)) => {
            if live_open_should_shrink(&err) {
                LIVE_OPEN_GATE.on_failure();
            }
            Err(err)
        }
        Err(_) => {
            LIVE_OPEN_GATE.on_failure();
            Err(CursorError::new(
                504,
                format!(
                    "Cursor live open timed out after {}s",
                    per_attempt.as_secs()
                ),
                None,
            ))
        }
    }
}

fn live_open_concurrency_max(raw: Option<&str>) -> usize {
    if let Some(n) = raw
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
    {
        return n.min(LIVE_OPEN_MAX);
    }
    LIVE_OPEN_MAX
}

fn live_generation_concurrency_max(raw: Option<&str>) -> usize {
    raw.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .map(|n| n.min(LIVE_GENERATION_MAX))
        .unwrap_or(LIVE_GENERATION_DEFAULT_MAX)
}

fn live_generation_queue_secs(raw: Option<&str>) -> u64 {
    raw.and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(LIVE_GENERATION_DEFAULT_QUEUE_SECS)
        .clamp(1, 3600)
}

fn live_generation_saturated_error() -> CursorError {
    CursorError::new(429, "Cursor live generation concurrency saturated", None)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LiveGenerationPriority {
    Start,
    Resume,
}

#[derive(Clone)]
struct LiveGenerationGate {
    inner: Arc<LiveGenerationGateInner>,
}

struct LiveGenerationGateInner {
    limit: usize,
    inflight: AtomicUsize,
    resume_waiters: AtomicUsize,
    notify: Notify,
}

struct LiveGenerationPermit {
    inner: Arc<LiveGenerationGateInner>,
}

impl std::fmt::Debug for LiveGenerationPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveGenerationPermit")
            .finish_non_exhaustive()
    }
}

impl Drop for LiveGenerationPermit {
    fn drop(&mut self) {
        self.inner.inflight.fetch_sub(1, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }
}

struct LiveGenerationResumeWaiter {
    inner: Arc<LiveGenerationGateInner>,
}

impl Drop for LiveGenerationResumeWaiter {
    fn drop(&mut self) {
        self.inner.resume_waiters.fetch_sub(1, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }
}

impl LiveGenerationGate {
    fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(LiveGenerationGateInner {
                limit: limit.clamp(1, LIVE_GENERATION_MAX),
                inflight: AtomicUsize::new(0),
                resume_waiters: AtomicUsize::new(0),
                notify: Notify::new(),
            }),
        }
    }

    fn limit(&self) -> usize {
        self.inner.limit
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.inner
            .limit
            .saturating_sub(self.inner.inflight.load(Ordering::SeqCst))
    }

    #[cfg(test)]
    fn resume_waiters(&self) -> usize {
        self.inner.resume_waiters.load(Ordering::SeqCst)
    }

    fn register_resume_waiter(&self) -> LiveGenerationResumeWaiter {
        self.inner.resume_waiters.fetch_add(1, Ordering::SeqCst);
        LiveGenerationResumeWaiter {
            inner: Arc::clone(&self.inner),
        }
    }

    fn try_acquire(&self, priority: LiveGenerationPriority) -> Option<LiveGenerationPermit> {
        loop {
            if priority == LiveGenerationPriority::Start
                && self.inner.resume_waiters.load(Ordering::SeqCst) > 0
            {
                return None;
            }
            let inflight = self.inner.inflight.load(Ordering::SeqCst);
            if inflight >= self.inner.limit {
                return None;
            }
            if self
                .inner
                .inflight
                .compare_exchange(inflight, inflight + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue;
            }
            if priority == LiveGenerationPriority::Start
                && self.inner.resume_waiters.load(Ordering::SeqCst) > 0
            {
                self.inner.inflight.fetch_sub(1, Ordering::SeqCst);
                self.inner.notify.notify_waiters();
                return None;
            }
            return Some(LiveGenerationPermit {
                inner: Arc::clone(&self.inner),
            });
        }
    }

    async fn acquire(
        &self,
        priority: LiveGenerationPriority,
        mut cancel: Option<&mut watch::Receiver<bool>>,
        wait: Duration,
    ) -> Result<LiveGenerationPermit, CursorError> {
        let _resume_waiter =
            (priority == LiveGenerationPriority::Resume).then(|| self.register_resume_waiter());
        let deadline = Instant::now() + wait;
        loop {
            if let Some(permit) = self.try_acquire(priority) {
                return Ok(permit);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(live_generation_saturated_error());
            }
            let notified = self.inner.notify.notified();
            if let Some(permit) = self.try_acquire(priority) {
                return Ok(permit);
            }
            if let Some(cancel) = cancel.as_deref_mut() {
                tokio::select! {
                    _ = cancel.wait_for(|aborted| *aborted) => {
                        return Err(CursorError::new(
                            409,
                            "Cursor live start superseded while waiting for generation capacity",
                            None,
                        ));
                    }
                    _ = notified => {}
                    _ = tokio::time::sleep(remaining) => {
                        if let Some(permit) = self.try_acquire(priority) {
                            return Ok(permit);
                        }
                        return Err(live_generation_saturated_error());
                    }
                }
            } else {
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep(remaining) => {
                        if let Some(permit) = self.try_acquire(priority) {
                            return Ok(permit);
                        }
                        return Err(live_generation_saturated_error());
                    }
                }
            }
        }
    }
}

static LIVE_GENERATION_GATE: LazyLock<LiveGenerationGate> = LazyLock::new(|| {
    let limit = live_generation_concurrency_max(
        std::env::var("CCP_CURSOR_LIVE_CONCURRENCY").ok().as_deref(),
    );
    LiveGenerationGate::new(limit)
});

async fn acquire_live_generation_permit_with_priority(
    cancel: Option<&mut watch::Receiver<bool>>,
    priority: LiveGenerationPriority,
) -> Result<LiveGenerationPermit, CursorError> {
    let gate = &*LIVE_GENERATION_GATE;
    let wait = Duration::from_secs(live_generation_queue_secs(
        std::env::var("CCP_CURSOR_LIVE_QUEUE_SECS").ok().as_deref(),
    ));
    let queued_at = Instant::now();
    let permit = gate.acquire(priority, cancel, wait).await?;
    if queued_at.elapsed() >= Duration::from_millis(100) {
        crate::logging::create_logger("cursor").info(
            "live_generation_admitted",
            Some(serde_json::Map::from_iter([
                (
                    "queuedMs".into(),
                    serde_json::json!(queued_at.elapsed().as_millis()),
                ),
                ("limit".into(), serde_json::json!(gate.limit())),
                (
                    "priority".into(),
                    serde_json::json!(match priority {
                        LiveGenerationPriority::Start => "start",
                        LiveGenerationPriority::Resume => "resume",
                    }),
                ),
            ])),
        );
    }
    Ok(permit)
}

async fn acquire_live_generation_permit(
    cancel: Option<&mut watch::Receiver<bool>>,
) -> Result<LiveGenerationPermit, CursorError> {
    acquire_live_generation_permit_with_priority(cancel, LiveGenerationPriority::Start).await
}

async fn acquire_live_generation_resume_permit() -> Result<LiveGenerationPermit, CursorError> {
    acquire_live_generation_permit_with_priority(None, LiveGenerationPriority::Resume).await
}

fn live_open_soft_start(max: usize) -> usize {
    LIVE_OPEN_SOFT_START.min(max.max(1))
}

fn live_open_grow(current: usize, max: usize) -> usize {
    if current >= max {
        return max;
    }
    current
        .saturating_mul(2)
        .max(current.saturating_add(1))
        .min(max)
}

fn live_open_shrink(current: usize, min: usize) -> usize {
    let min = min.max(1);
    if current <= min {
        return min;
    }
    (current / 2).max(min)
}

fn live_open_should_shrink(err: &CursorError) -> bool {
    err.status == 504 || err.message.to_ascii_lowercase().contains("timed out")
}

fn live_open_saturated_error() -> CursorError {
    CursorError::new(429, "Cursor live open concurrency saturated", None)
}

struct AdaptiveLiveOpenGate {
    inner: Arc<AdaptiveLiveOpenInner>,
}

struct AdaptiveLiveOpenInner {
    min: usize,
    max: usize,
    limit: AtomicUsize,
    inflight: AtomicUsize,
    notify: Notify,
}

struct LiveOpenPermit {
    inner: Arc<AdaptiveLiveOpenInner>,
}

impl std::fmt::Debug for LiveOpenPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveOpenPermit").finish_non_exhaustive()
    }
}

impl Drop for LiveOpenPermit {
    fn drop(&mut self) {
        self.inner.inflight.fetch_sub(1, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }
}

impl AdaptiveLiveOpenGate {
    fn new(max: usize) -> Self {
        let max = max.clamp(1, LIVE_OPEN_MAX);
        Self::with_bounds(live_open_soft_start(max), max)
    }

    fn with_bounds(min: usize, max: usize) -> Self {
        let max = max.clamp(1, LIVE_OPEN_MAX);
        let min = min.clamp(1, max);
        Self {
            inner: Arc::new(AdaptiveLiveOpenInner {
                min,
                max,
                limit: AtomicUsize::new(min),
                inflight: AtomicUsize::new(0),
                notify: Notify::new(),
            }),
        }
    }

    #[cfg(test)]
    fn limit(&self) -> usize {
        self.inner.limit.load(Ordering::SeqCst)
    }

    fn on_success(&self) {
        loop {
            let cur = self.inner.limit.load(Ordering::SeqCst);
            let next = live_open_grow(cur, self.inner.max);
            if next == cur {
                return;
            }
            if self
                .inner
                .limit
                .compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.inner.notify.notify_waiters();
                return;
            }
        }
    }

    fn on_failure(&self) {
        loop {
            let cur = self.inner.limit.load(Ordering::SeqCst);
            let next = live_open_shrink(cur, self.inner.min);
            if next == cur {
                return;
            }
            if self
                .inner
                .limit
                .compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return;
            }
        }
    }

    fn try_acquire(&self) -> Option<LiveOpenPermit> {
        loop {
            let inflight = self.inner.inflight.load(Ordering::SeqCst);
            let limit = self.inner.limit.load(Ordering::SeqCst);
            if inflight >= limit {
                return None;
            }
            if self
                .inner
                .inflight
                .compare_exchange(inflight, inflight + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Some(LiveOpenPermit {
                    inner: Arc::clone(&self.inner),
                });
            }
        }
    }

    async fn acquire(&self, wait: Duration) -> Result<LiveOpenPermit, CursorError> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(permit) = self.try_acquire() {
                return Ok(permit);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(live_open_saturated_error());
            }
            let notified = self.inner.notify.notified();
            if let Some(permit) = self.try_acquire() {
                return Ok(permit);
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(remaining) => {
                    if let Some(permit) = self.try_acquire() {
                        return Ok(permit);
                    }
                    return Err(live_open_saturated_error());
                }
            }
        }
    }
}

/// Soft-starts at 4 so a cold process does not stampede H2, then doubles on
/// successful opens up to 128 (grok-cli fan-out). The env var is an optional
/// cap, not something operators have to set for parallelism.
static LIVE_OPEN_GATE: LazyLock<AdaptiveLiveOpenGate> = LazyLock::new(|| {
    let max = live_open_concurrency_max(
        std::env::var("CCP_CURSOR_LIVE_OPEN_CONCURRENCY")
            .ok()
            .as_deref(),
    );
    if cfg!(test) {
        AdaptiveLiveOpenGate::with_bounds(max, max)
    } else {
        AdaptiveLiveOpenGate::new(max)
    }
});

#[derive(Debug, Default, Clone)]
struct ProcessH2Circuit {
    consecutive_timeouts: u32,
    open_since: Option<Instant>,
}

impl ProcessH2Circuit {
    fn prefers_http1(&self) -> bool {
        self.prefers_http1_at(Instant::now())
    }

    fn prefers_http1_at(&self, _now: Instant) -> bool {
        self.open_since.is_some()
    }

    fn on_h2_open_timeout(&mut self) -> bool {
        self.on_h2_open_timeout_at(Instant::now())
    }

    fn on_h2_open_timeout_at(&mut self, now: Instant) -> bool {
        self.consecutive_timeouts = self.consecutive_timeouts.saturating_add(1);
        if self.open_since.is_some() {
            return false;
        }
        self.open_since = Some(now);
        true
    }

    fn on_h2_open_success(&mut self) {
        self.consecutive_timeouts = 0;
        self.open_since = None;
    }

    /// Mid-stream H2 `INTERNAL_ERROR` (gemini-3.6-flash) is not an open
    /// timeout, but the next first-open must not go back to H2 or we loop
    /// RST → ResumeAction → ambiguous timeout.
    fn on_h2_stream_reset_at(&mut self, now: Instant) -> bool {
        self.consecutive_timeouts = TRANSPORT_BREAKER_THRESHOLD;
        if self.open_since.is_some() {
            self.open_since = Some(now);
            return false;
        }
        self.open_since = Some(now);
        true
    }
}

static PROCESS_H2_CIRCUIT: Mutex<ProcessH2Circuit> = Mutex::new(ProcessH2Circuit {
    consecutive_timeouts: 0,
    open_since: None,
});

fn live_open_prefers_http1_from(env_http1: bool, circuit_open: bool) -> bool {
    env_http1 || circuit_open
}

fn process_h2_circuit_prefers_http1() -> bool {
    if cfg!(test) {
        return false;
    }
    PROCESS_H2_CIRCUIT
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .prefers_http1()
}

fn live_open_prefers_http1() -> bool {
    live_open_prefers_http1_from(
        http1::prefer_http1_agent(),
        process_h2_circuit_prefers_http1(),
    )
}

fn note_process_h2_open_timeout() {
    if cfg!(test) {
        return;
    }
    let just_opened = PROCESS_H2_CIRCUIT
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .on_h2_open_timeout();
    if just_opened {
        crate::logging::create_logger("cursor").warn(
            "live_h2_circuit_open",
            Some(serde_json::Map::from_iter([
                ("prefer_http1".into(), serde_json::json!(true)),
                ("consecutive_timeouts".into(), serde_json::json!(1)),
            ])),
        );
    }
}

fn note_process_h2_open_success() {
    if cfg!(test) {
        return;
    }
    PROCESS_H2_CIRCUIT
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .on_h2_open_success();
}

fn note_process_h2_stream_reset() {
    if cfg!(test) {
        return;
    }
    let just_opened = PROCESS_H2_CIRCUIT
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .on_h2_stream_reset_at(Instant::now());
    if just_opened {
        crate::logging::create_logger("cursor").warn(
            "live_h2_circuit_open",
            Some(serde_json::Map::from_iter([
                ("prefer_http1".into(), serde_json::json!(true)),
                ("reason".into(), serde_json::json!("h2_stream_reset")),
            ])),
        );
    }
}

fn live_reconnect_allow_h1_fallback(force_http1: bool, http1_rejected: bool) -> bool {
    !force_http1 && !http1_rejected
}

#[derive(Debug, Default, Clone)]
struct TransportBreaker {
    consecutive_fails: u32,
    open_since: Option<Instant>,
}

impl TransportBreaker {
    fn allows(&self, now: Instant) -> bool {
        match self.open_since {
            None => true,
            Some(opened) => now.saturating_duration_since(opened) >= TRANSPORT_BREAKER_COOLDOWN,
        }
    }

    fn on_failure(&mut self, now: Instant) {
        self.consecutive_fails = self.consecutive_fails.saturating_add(1);
        if self.consecutive_fails >= TRANSPORT_BREAKER_THRESHOLD {
            self.open_since = Some(now);
        }
    }

    fn on_success(&mut self) {
        self.consecutive_fails = 0;
        self.open_since = None;
    }
}

#[derive(Debug, Default, Clone)]
struct TransportBreakers {
    h2: TransportBreaker,
    h1: TransportBreaker,
}

fn breaker_for(breakers: &mut TransportBreakers, http1: bool) -> &mut TransportBreaker {
    if http1 {
        &mut breakers.h1
    } else {
        &mut breakers.h2
    }
}

fn record_transport_failure(reconnect: &mut LiveReconnectContext, now: Instant) {
    breaker_for(&mut reconnect.breakers, reconnect.force_http1).on_failure(now);
}

fn record_transport_success(reconnect: &mut LiveReconnectContext) {
    breaker_for(&mut reconnect.breakers, reconnect.force_http1).on_success();
}

fn apply_transport_breakers(reconnect: &mut LiveReconnectContext, now: Instant) {
    if reconnect.http1_rejected {
        reconnect.force_http1 = false;
        return;
    }
    let h2_ok = reconnect.breakers.h2.allows(now);
    let h1_ok = reconnect.breakers.h1.allows(now);
    if reconnect.force_http1 && !h1_ok && h2_ok {
        reconnect.force_http1 = false;
    }
}

fn live_reconnect_open_allow_h1(_reconnect: &LiveReconnectContext, _now: Instant) -> bool {
    // Transport switches belong to the reconnect state machine (`ForceHttp1` /
    // `FlipToH2`). Falling back inside one `open_live_transport` hides which
    // transport produced 464 and duplicates ResumeAction.
    false
}

#[allow(clippy::too_many_arguments)]
fn live_idle_stall_message(
    useful: bool,
    saw_text: bool,
    tools_advertised: bool,
    pending_empty: bool,
    since_progress: Duration,
    since_liveness: Duration,
    setup_idle: Duration,
    stream_idle: Duration,
) -> Option<&'static str> {
    // Dead stream: no frames at all (including server heartbeats).
    if !useful && since_liveness >= setup_idle {
        return Some("Cursor stream produced no useful progress");
    }
    // Alive heartbeat-only thinking (Fable high) — wait 2× stream idle, not 45s.
    if !useful && since_progress >= stream_idle.saturating_mul(2) {
        return Some("Cursor stream produced no useful progress");
    }
    if useful && !saw_text && since_progress >= stream_idle && !tools_advertised {
        return Some("Cursor stream stalled after partial progress");
    }
    if useful
        && !saw_text
        && tools_advertised
        && pending_empty
        && since_progress >= stream_idle.saturating_mul(2)
    {
        return Some("Cursor stream stalled after partial progress");
    }
    if useful && saw_text && pending_empty && since_progress >= stream_idle.saturating_mul(2) {
        return Some("Cursor stream stalled after partial progress");
    }
    None
}

fn live_probation_expired(on_probation: bool, got_progress: bool, remaining: Duration) -> bool {
    on_probation && !got_progress && remaining.is_zero()
}

fn hollow_resume_terminal_message(
    session_id: &str,
    opened_with_checkpoint: bool,
    useful: bool,
    pending_empty: bool,
    latest_checkpoint: &mut Option<Vec<u8>>,
    kv_blobs: &mut HashMap<Vec<u8>, Vec<u8>>,
    fallback: impl Into<String>,
) -> String {
    // A checkpoint-backed turn that emitted nothing before an accepted
    // ResumeAction also went hollow has no useful state to preserve. Rotate
    // that binding once so the client retry replays its full history. Fresh
    // runs and partially emitted turns remain 409/ambiguous to avoid duplicate
    // execution when Cursor may still be working upstream.
    if opened_with_checkpoint && !useful && pending_empty {
        super::conversation::reset(session_id);
        *latest_checkpoint = None;
        kv_blobs.clear();
        crate::logging::create_logger("cursor").warn(
            "live_conversation_reset",
            Some(serde_json::Map::from_iter([(
                "reason".into(),
                serde_json::json!("checkpoint_resume_hollow"),
            )])),
        );
        return format!(
            "Cursor recovery exhausted without producing output \
             ({CONVERSATION_RESET_RETRY_NOTE})"
        );
    }
    fallback.into()
}

/// After a ResumeAction HTTP 200, another ResumeAction is only safe once this
/// stream has produced a body chunk. Delayed hollow EOF must fail closed.
fn live_should_resume_after_drop(on_probation: bool, got_chunk_since_reconnect: bool) -> bool {
    !on_probation || got_chunk_since_reconnect
}

const CONVERSATION_RESET_RETRY_NOTE: &str =
    "stale Cursor conversation reset; retry this message to continue";
const EMPTY_TURN_RETRY_NOTE: &str =
    "Cursor upstream finished this turn without text or tool calls; retry this turn";
const EMPTY_TURN_CHECKPOINT_RETRY_NOTE: &str =
    "completed tool results retained in Cursor checkpoint; continue without replaying tools";
pub(crate) const EMPTY_TURN_CHECKPOINT_CONTINUE_PROMPT: &str = "Continue from the completed tool results in this Cursor conversation. \
     Produce the final answer requested by the user now. \
     Do not repeat completed tool calls.";
type LiveContinuationState<'a> = (&'a mut Option<Vec<u8>>, &'a mut HashMap<Vec<u8>, Vec<u8>>);

fn annotate_connect_end_error(
    session_id: &str,
    error: ConnectEndError,
    continuation_state: Option<LiveContinuationState<'_>>,
) -> String {
    let mut fields = serde_json::Map::new();
    fields.insert("status".into(), serde_json::json!(error.status));
    fields.insert("code".into(), serde_json::json!(error.code));
    fields.insert("message".into(), serde_json::json!(error.message));
    let mut text = error.to_string();
    if cursor_connect_error_is_missing_conversation_data(&error.message)
        || cursor_connect_error_is_missing_conversation_data(&error.detail)
    {
        super::conversation::reset(session_id);
        if let Some((latest_checkpoint, kv_blobs)) = continuation_state {
            *latest_checkpoint = None;
            kv_blobs.clear();
        }
        fields.insert("conversationReset".into(), serde_json::json!(true));
        text = format!("{text} ({CONVERSATION_RESET_RETRY_NOTE})");
    } else if cursor_connect_error_is_missing_image(&error.message)
        || cursor_connect_error_is_missing_image(&text)
    {
        super::conversation::clear_checkpoint(session_id);
        if let Some((latest_checkpoint, _)) = continuation_state {
            *latest_checkpoint = None;
        }
        fields.insert("checkpointCleared".into(), serde_json::json!(true));
        text = format!(
            "{text} (this turn had no new image; a stale Cursor image id was in the conversation checkpoint — checkpoint cleared, retry the message)"
        );
    }
    crate::logging::create_logger("cursor").warn("connect_end_error", Some(fields));
    text
}

fn cursor_error_is_missing_conversation_data(err: &CursorError) -> bool {
    cursor_connect_error_is_missing_conversation_data(&err.message)
        || err
            .detail
            .as_deref()
            .is_some_and(cursor_connect_error_is_missing_conversation_data)
}

fn annotate_live_cursor_error(session_id: &str, err: CursorError) -> CursorError {
    if !cursor_error_is_missing_conversation_data(&err) {
        return err;
    }
    super::conversation::reset(session_id);
    CursorError::new(
        err.status,
        format!("{} ({CONVERSATION_RESET_RETRY_NOTE})", err.message),
        err.detail,
    )
}

pub(crate) fn live_request_fingerprint(payload: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    payload.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LiveReconnectTransportAction {
    KeepTrying,
    ForceHttp1,
    FlipToH2,
    GiveUp(&'static str),
}

fn live_reconnect_on_open_error(
    force_http1: bool,
    http1_rejected: bool,
    err: &CursorError,
) -> LiveReconnectTransportAction {
    if force_http1 && matches!(err.status, 421 | 464) {
        if http1_rejected {
            return LiveReconnectTransportAction::GiveUp(
                "HTTP/1 rejected (464) after HTTP/2 already failed",
            );
        }
        return LiveReconnectTransportAction::FlipToH2;
    }
    if force_http1 && http1_rejected {
        return LiveReconnectTransportAction::GiveUp(
            "HTTP/1 rejected by proxy and HTTP/2 still failing",
        );
    }
    if !force_http1 && is_explicit_http1_required(err) {
        return LiveReconnectTransportAction::ForceHttp1;
    }
    if err.status == 0 || is_response_less_send_error(err) {
        return LiveReconnectTransportAction::GiveUp(
            "response-less ResumeAction send is ambiguous",
        );
    }
    if http1_rejected {
        return LiveReconnectTransportAction::KeepTrying;
    }
    LiveReconnectTransportAction::KeepTrying
}

fn live_reconnect_on_hollow_body(
    _force_http1: bool,
    _http1_rejected: bool,
) -> LiveReconnectTransportAction {
    LiveReconnectTransportAction::GiveUp(
        "HTTP 200 resume had no body; another ResumeAction would duplicate it",
    )
}

fn live_open_should_retry_http1(err: &CursorError) -> bool {
    if matches!(err.status, 400 | 401 | 403 | 404 | 429) {
        return false;
    }
    if crate::retry::is_billing_block(&err.message) || crate::retry::is_capacity_shed(&err.message)
    {
        return false;
    }
    is_explicit_http1_required(err)
        || is_pre_connect_failure(err)
        || is_ambiguous_live_open_timeout(err)
}

/// After this POST already retried a transport miss, fail closed as 409 so
/// grok-build does not 5xx-retry on top of the proxy loop.
pub(crate) fn exhausted_live_start_error(err: CursorError, attempted_retries: u32) -> CursorError {
    if attempted_retries == 0 {
        return err;
    }
    let text = err.client_message();
    if crate::retry::is_billing_block(&text)
        || crate::retry::is_billing_block(&err.message)
        || crate::retry::is_capacity_shed(&text)
        || crate::retry::is_capacity_shed(&err.message)
    {
        return err;
    }
    if matches!(err.status, 400 | 401 | 403 | 404 | 429) {
        return err;
    }
    if is_pre_connect_failure(&err)
        || is_initial_bidiappend_timeout(&err)
        || err.message.contains("error sending request")
        || matches!(err.status, 0 | 502 | 503 | 504)
    {
        return CursorError::new(409, err.message, err.detail);
    }
    err
}

fn is_explicit_http1_required(err: &CursorError) -> bool {
    if matches!(err.status, 400 | 401 | 403 | 404 | 429) {
        return false;
    }
    matches!(err.status, 421 | 464)
        || err.message.contains("HTTP_1_1_REQUIRED")
        || err.message.contains("FORCE_BIDI_DISABLED")
        || err
            .detail
            .as_deref()
            .is_some_and(|d| d.contains("HTTP_1_1_REQUIRED") || d.contains("FORCE_BIDI_DISABLED"))
}

#[cfg(test)]
fn is_http1_fallback_error(err: &CursorError) -> bool {
    if matches!(err.status, 400 | 401 | 403 | 404 | 429) {
        return false;
    }
    is_explicit_http1_required(err)
        || matches!(err.status, 408 | 502 | 503 | 504)
        || err.message.contains("error sending request")
        || err.message.contains("connection")
        || is_h2_stream_reset(&err.message)
        || err.detail.as_deref().is_some_and(|d| {
            d.contains("HTTP_1_1_REQUIRED") || d.contains("bidi") || is_h2_stream_reset(d)
        })
}

fn live_send_failure_is_terminal(err: &CursorError) -> bool {
    cursor_error_is_missing_conversation_data(err)
        || err.message.contains("acceptance is ambiguous")
        || !is_retryable_live_transport_error(err)
}

fn live_reconnect_open_error_is_fatal(err: &CursorError) -> bool {
    cursor_error_is_missing_conversation_data(err)
        || terminal_error_allows_fresh_retry(&err.message)
        || live_send_failure_is_terminal(err)
        || is_response_less_send_error(err)
}

fn live_should_persist_continuation_message(message: Option<&str>) -> bool {
    !message.is_some_and(terminal_error_allows_fresh_retry)
}

fn live_acceptance_unresolved(
    held_turn_end: bool,
    accepted_resume_unconfirmed: bool,
    unconfirmed_probation: bool,
) -> bool {
    held_turn_end || accepted_resume_unconfirmed || unconfirmed_probation
}

fn live_control_close_message(unresolved: bool) -> &'static str {
    if unresolved {
        "Cursor live run control channel closed after an unresolved operation; completion is ambiguous"
    } else {
        "Cursor live run control channel closed"
    }
}

pub(crate) fn is_ambiguous_live_open_timeout(err: &CursorError) -> bool {
    err.status == 504
}

fn is_initial_bidiappend_timeout(err: &CursorError) -> bool {
    let blob = format!("{} {}", err.message, err.client_message());
    let lower = blob.to_ascii_lowercase();
    lower.contains("bidiappend initial run") && lower.contains("timed out")
}

fn is_pre_connect_failure(err: &CursorError) -> bool {
    let blob = format!("{}{}", err.message, err.detail.as_deref().unwrap_or(""));
    let lower = blob.to_ascii_lowercase();
    if lower.contains("connect failed") {
        return true;
    }
    lower.contains("error sending request for url")
        && !lower.contains("connection reset")
        && !lower.contains("connection closed")
        && !is_h2_stream_reset(&blob)
}

fn is_response_less_send_error(err: &CursorError) -> bool {
    if is_explicit_http1_required(err) {
        return false;
    }
    if is_ambiguous_live_open_timeout(err) || err.status == 0 {
        return true;
    }
    if is_pre_connect_failure(err) {
        return false;
    }
    let blob = format!("{}{}", err.message, err.detail.as_deref().unwrap_or(""));
    err.status == 502
        && (blob.contains("error sending request")
            || blob.contains("connection reset")
            || blob.contains("connection closed")
            || is_h2_stream_reset(&blob))
}

pub(crate) fn live_start_error_seals_tombstone(err: &CursorError) -> bool {
    if terminal_error_allows_fresh_retry(&err.message) {
        return false;
    }
    if terminal_error_is_ambiguous_accept(&err.message) {
        return true;
    }
    if matches!(err.status, 400 | 401 | 403 | 404 | 421 | 429 | 464) {
        return false;
    }
    if is_pre_connect_failure(err) {
        return false;
    }
    matches!(err.status, 0 | 502 | 503 | 504)
        || err.message.contains("timed out")
        || err.message.contains("connection")
        || err.message.contains("reset")
}

pub(crate) fn terminal_error_is_ambiguous_accept(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("timed out")
        || lower.contains("no progress")
        || lower.contains("ambiguous")
        || lower.contains("had no body")
        || lower.contains("ended before the first byte")
}

/// Probe `TerminalError` that must 502 the current POST. Ambiguous accept and
/// same-request-retryable errors must not brick grok-build's next turn.
pub(crate) fn live_probe_error_blocks_new_run(error: &str) -> bool {
    !live_error_is_same_request_retryable(error) && !terminal_error_is_ambiguous_accept(error)
}

fn terminal_error_allows_fresh_retry(message: &str) -> bool {
    message.contains(CONVERSATION_RESET_RETRY_NOTE)
}

fn live_error_is_resource_exhausted(message: &str) -> bool {
    message.contains("[resource_exhausted]")
        || message.contains("ERROR_RESOURCE_EXHAUSTED")
        || message.contains("Connect error 429")
        || message.contains("Cursor error 429")
}

fn terminal_error_clears_live_slot(message: &str) -> bool {
    live_error_is_same_request_retryable(message)
}

pub(crate) fn live_error_allows_fresh_conversation(message: &str) -> bool {
    terminal_error_allows_fresh_retry(message)
}

/// ClientOnly tools (Workflow/Skill/spawn_subagent) tear the BiDi down.
/// Resuming them races the dying driver and 502s with
/// "acknowledgement dropped", which grok-build retries as the same turn.
pub(crate) fn live_pending_must_supersede(pending: &[PendingCursorExec]) -> bool {
    !pending.is_empty()
        && pending
            .iter()
            .all(|exec| matches!(exec.kind, CursorExecKind::ClientOnly))
}

/// The live driver already left the select loop (ClientOnly teardown or
/// channel close). A 502 here becomes grok-build's "Retrying (attempt 1)"
/// loop; the next POST must start a fresh run with tool_result history.
pub(crate) fn live_resume_error_is_dead_driver(error: &CursorError) -> bool {
    let message = error.message.as_str();
    message.contains("acknowledgement dropped") || message.contains("already closed")
}

pub(crate) fn live_error_is_empty_turn_retry(message: &str) -> bool {
    message.contains(EMPTY_TURN_RETRY_NOTE)
}

pub(crate) fn live_error_needs_checkpoint_continue(message: &str) -> bool {
    live_error_is_empty_turn_retry(message) && message.contains(EMPTY_TURN_CHECKPOINT_RETRY_NOTE)
}

pub(crate) fn live_error_is_same_request_retryable(message: &str) -> bool {
    if crate::retry::is_billing_block(message) || crate::retry::is_capacity_shed(message) {
        return false;
    }
    if live_error_is_empty_turn_retry(message) {
        return true;
    }
    if cursor_connect_error_is_missing_conversation_data(message)
        || terminal_error_allows_fresh_retry(message)
    {
        return true;
    }
    let classified = crate::retry::classify_proxy_error_status(502, message);
    if (400..500).contains(&classified) && !crate::retry::is_upstream_rate_limit(message) {
        return false;
    }
    if cursor_connect_error_is_missing_image(message) {
        return false;
    }
    let lower = message.to_ascii_lowercase();
    if lower.contains("outdated_client") || lower.contains("outdated client") {
        return false;
    }
    live_error_is_resource_exhausted(message)
        || message.contains("Connect error 502")
        || message.contains("Connect error 503")
        || message.contains("Connect error 504")
        || lower.contains("unable to reach the model provider")
}

pub(crate) fn cursor_start_error_is_same_request_retryable(err: &CursorError) -> bool {
    let text = err.client_message();
    if crate::retry::is_billing_block(&text)
        || crate::retry::is_billing_block(&err.message)
        || crate::retry::is_capacity_shed(&text)
        || crate::retry::is_capacity_shed(&err.message)
    {
        return false;
    }
    if text.contains("concurrency saturated") || err.message.contains("concurrency saturated") {
        return false;
    }
    if is_initial_bidiappend_timeout(err) {
        return true;
    }
    if cursor_connect_error_is_missing_conversation_data(&text)
        || err
            .detail
            .as_deref()
            .is_some_and(cursor_connect_error_is_missing_conversation_data)
        || terminal_error_allows_fresh_retry(&text)
    {
        return true;
    }
    let classified = crate::retry::classify_proxy_error_status(err.status, &text);
    if (400..500).contains(&classified) && !crate::retry::is_upstream_rate_limit(&text) {
        return false;
    }
    if cursor_connect_error_is_missing_image(&text) {
        return false;
    }
    crate::retry::should_retry_upstream(err.status, &text)
        || live_error_is_same_request_retryable(&text)
}

pub(crate) fn same_request_retry_wait_ms(attempt: u32, message: &str) -> u64 {
    if live_error_allows_fresh_conversation(message) {
        return 0;
    }
    crate::retry::compute_backoff_delay(attempt, None).wait_ms
}

fn classify_outbound_send(result: Result<(), CursorError>) -> Result<bool, CursorError> {
    match result {
        Ok(()) => Ok(true),
        Err(err) if live_send_failure_is_terminal(&err) => Err(err),
        Err(_) => Ok(false),
    }
}

fn partial_tool_result_send_error(
    error: CursorError,
    sent_frames: usize,
    total_frames: usize,
) -> CursorError {
    CursorError::new(
        error.status,
        format!(
            "Cursor tool-result batch partially sent ({sent_frames}/{total_frames}); acceptance is ambiguous: {}",
            error.message
        ),
        error.detail,
    )
}

/// Semantic Cursor errors (400/401/403/429) must not be retried on another
/// transport — that burns quota and can duplicate an already-accepted run.
pub(crate) fn is_retryable_live_transport_error(err: &CursorError) -> bool {
    if matches!(err.status, 400 | 401 | 403 | 404 | 429) {
        return false;
    }
    matches!(err.status, 0 | 408 | 421 | 464 | 502 | 503 | 504)
        || err.message.contains("error sending request")
        || err.message.contains("connection")
        || is_h2_stream_reset(&err.message)
        || err.detail.as_deref().is_some_and(|d| {
            d.contains("HTTP_1_1_REQUIRED") || d.contains("bidi") || is_h2_stream_reset(d)
        })
}

/// Server keep-alives must not reset setup/stream idle clocks.
fn record_server_heartbeat(_last_progress: &mut Instant) {}

fn opening_live_checkpoint(state: &[u8]) -> Option<Vec<u8>> {
    if state.is_empty() {
        None
    } else {
        Some(state.to_vec())
    }
}

fn abrupt_eof_should_error(_had_progress: bool) -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LiveReconnectOutcome {
    Reconnected,
    Skipped(&'static str),
    Failed(String),
}

fn format_error_chain(err: &dyn Error) -> String {
    let mut parts = Vec::new();
    let mut current: Option<&dyn Error> = Some(err);
    while let Some(e) = current {
        let text = e.to_string();
        if parts
            .last()
            .is_none_or(|prev: &String| prev != &text && !prev.contains(&text))
        {
            parts.push(text);
        }
        current = e.source();
    }
    parts.join(": ")
}

fn live_reconnect_resume_state(
    latest_checkpoint: &Option<Vec<u8>>,
    opening_checkpoint: &Option<Vec<u8>>,
    conversation_id: Option<&str>,
) -> Option<Vec<u8>> {
    if let Some(checkpoint) = latest_checkpoint.as_ref().filter(|c| !c.is_empty()) {
        return Some(checkpoint.clone());
    }
    if let Some(checkpoint) = opening_checkpoint.as_ref().filter(|c| !c.is_empty()) {
        return Some(checkpoint.clone());
    }
    // First turn: Cursor may RST H2 before conversation_checkpoint_update.
    // ResumeAction with the same conversation_id and empty state reattaches;
    // it is not a second UserMessageAction.
    if conversation_id.is_some_and(|id| !id.is_empty()) {
        return Some(Vec::new());
    }
    None
}

fn live_reconnect_skip_reason(
    latest_checkpoint: &Option<Vec<u8>>,
    opening_checkpoint: &Option<Vec<u8>>,
    conversation_id: Option<&str>,
    reconnect_attempts: u32,
    max_reconnects: u32,
) -> Option<&'static str> {
    if live_reconnect_resume_state(latest_checkpoint, opening_checkpoint, conversation_id).is_none()
    {
        return Some("no checkpoint");
    }
    if reconnect_attempts >= max_reconnects {
        return Some("reconnect budget exhausted");
    }
    None
}

/// Full jitter from attempt 1: sleep ∈ [0, min(8s, 1s×2^(n-1), remaining/2)].
/// Hollow (HTTP 200 then zero-byte RST) retries stay at 0ms.
fn live_reconnect_backoff_ceiling_ms(attempt: u32, remaining_ms: u64) -> u64 {
    if attempt == 0 || remaining_ms == 0 {
        return 0;
    }
    let exp = 2u64.saturating_pow(attempt.saturating_sub(1).min(16));
    let grown = LIVE_RECONNECT_BACKOFF_BASE_MS.saturating_mul(exp);
    grown
        .min(LIVE_RECONNECT_BACKOFF_CAP_MS)
        .min(remaining_ms / 2)
}

fn live_reconnect_backoff_ms_for(attempt: u32, hollow: bool, remaining_ms: u64) -> u64 {
    if hollow {
        return 0;
    }
    let ceiling = live_reconnect_backoff_ceiling_ms(attempt, remaining_ms);
    if ceiling == 0 {
        return 0;
    }
    rand::thread_rng().gen_range(0..=ceiling)
}

fn is_h2_stream_reset(message: &str) -> bool {
    message.contains("unexpected internal error")
        || message.contains("stream error received")
        || message.contains("broken pipe")
        || message.contains("HTTP2")
        || message.contains("http2")
}

/// H2 `INTERNAL_ERROR` mid-stream: ResumeAction on H2 is always hollow in
/// production. Switch that run to a real `http1_only` RunSSE client immediately.
fn live_reconnect_should_force_http1(
    got_chunk_since_reconnect: bool,
    reconnect_attempts: u32,
    already_http1: bool,
    http1_rejected: bool,
    stream_error: Option<&str>,
) -> bool {
    if already_http1 || http1_rejected {
        return false;
    }
    if stream_error.is_some_and(is_h2_stream_reset) {
        return true;
    }
    !got_chunk_since_reconnect && reconnect_attempts > 0
}

fn prepare_live_reconnect(
    reconnect: &mut LiveReconnectContext,
    got_chunk_since_reconnect: bool,
    reconnect_attempts: u32,
    stream_error: Option<&str>,
) {
    reconnect.last_trigger = stream_error.unwrap_or("stream_drop").to_string();
    if live_reconnect_should_force_http1(
        got_chunk_since_reconnect,
        reconnect_attempts,
        reconnect.force_http1,
        reconnect.http1_rejected,
        stream_error,
    ) {
        reconnect.force_http1 = true;
        note_process_h2_stream_reset();
        let mut fields = serde_json::Map::new();
        fields.insert("attempts".into(), serde_json::json!(reconnect_attempts));
        fields.insert("reason".into(), serde_json::json!("h2_stream_reset"));
        crate::logging::create_logger("cursor").warn("live_reconnect_http1", Some(fields));
    }
}

/// Peek ~1ms for an already-buffered RST/EOF. Quiet Fable thinking is healthy:
/// do not wait seconds (that blocked owed `tool_result`s and false-aborted resumes).
const LIVE_RECONNECT_IMMEDIATE: Duration = Duration::from_millis(1);

async fn take_immediate_resume_chunk<S, E>(mut stream: S) -> Result<(Option<Bytes>, S), String>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Error,
{
    match tokio::time::timeout(LIVE_RECONNECT_IMMEDIATE, stream.next()).await {
        Ok(Some(Ok(chunk))) => Ok(((!chunk.is_empty()).then_some(chunk), stream)),
        Ok(Some(Err(error))) => Err(format_error_chain(&error)),
        Ok(None) => Err("Cursor resume stream ended before the first byte".into()),
        Err(_) => Ok((None, stream)),
    }
}

fn reconnect_note(outcome: &LiveReconnectOutcome) -> String {
    match outcome {
        LiveReconnectOutcome::Reconnected => String::new(),
        LiveReconnectOutcome::Skipped(reason) => {
            format!(" (reconnect skipped: {reason})")
        }
        LiveReconnectOutcome::Failed(detail) => {
            format!(" (reconnect failed: {detail})")
        }
    }
}

fn live_reconnect_log_fields(
    outcome: &LiveReconnectOutcome,
    attempts: u32,
    max_reconnects: u32,
    http1: bool,
    trigger: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut fields = serde_json::Map::new();
    fields.insert("attempts".into(), serde_json::json!(attempts));
    fields.insert("max".into(), serde_json::json!(max_reconnects));
    fields.insert("http1".into(), serde_json::json!(http1));
    if !trigger.is_empty() {
        fields.insert("trigger".into(), serde_json::json!(trigger));
    }
    match outcome {
        LiveReconnectOutcome::Reconnected => {
            fields.insert("outcome".into(), serde_json::json!("ok"));
        }
        LiveReconnectOutcome::Skipped(reason) => {
            fields.insert("outcome".into(), serde_json::json!("skipped"));
            fields.insert("reason".into(), serde_json::json!(reason));
        }
        LiveReconnectOutcome::Failed(detail) => {
            fields.insert("outcome".into(), serde_json::json!("failed"));
            fields.insert("detail".into(), serde_json::json!(detail));
        }
    }
    fields
}

fn log_live_reconnect(
    outcome: &LiveReconnectOutcome,
    attempts: u32,
    max_reconnects: u32,
    http: &CursorHttpClient,
    trigger: &str,
) {
    let fields = live_reconnect_log_fields(
        outcome,
        attempts,
        max_reconnects,
        http.prefers_http1(),
        trigger,
    );
    match outcome {
        LiveReconnectOutcome::Reconnected => {
            crate::logging::create_logger("cursor").info("live_reconnect", Some(fields));
        }
        LiveReconnectOutcome::Skipped(_) | LiveReconnectOutcome::Failed(_) => {
            crate::logging::create_logger("cursor").warn("live_reconnect", Some(fields));
        }
    }
}

/// Re-open AgentService/Run with `ResumeAction` after a transport stall.
/// Retries retryable open failures up to `max_reconnects` with full jitter.
async fn wait_for_live_cancel(cancel_requested: &AtomicBool) {
    while !cancel_requested.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn reconnect_cancelled_ambiguous(
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    detail: &str,
) -> LiveReconnectOutcome {
    let message = format!("Cursor live cancellation interrupted {detail}; acceptance is ambiguous");
    store_terminal_error(terminal_error, &message);
    LiveReconnectOutcome::Failed(message)
}

#[allow(clippy::too_many_arguments)]
async fn try_live_reconnect(
    reconnect: &mut LiveReconnectContext,
    outbound: &mut ClientOutbound,
    upstream: &mut LiveUpstream,
    upstream_tx: &mut mpsc::Sender<Result<Option<Bytes>, String>>,
    upstream_pump: &mut tokio::task::JoinHandle<()>,
    cancel_requested: &AtomicBool,
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    decoder: &mut ConnectFrameDecoder,
    latest_checkpoint: &Option<Vec<u8>>,
    kv_blobs: &HashMap<Vec<u8>, Vec<u8>>,
    pending: &mut PendingExecState,
    reconnect_attempts: &mut u32,
    max_reconnects: u32,
    last_progress: &mut Instant,
    resume_grace_until: &mut Option<Instant>,
    _resume_grace: Duration,
    hard_deadline: Instant,
) -> LiveReconnectOutcome {
    if cancel_requested.load(Ordering::Acquire) {
        return reconnect_cancelled_ambiguous(
            terminal_error,
            "recovery before a replacement ResumeAction",
        );
    }
    let Some(checkpoint) = live_reconnect_resume_state(
        latest_checkpoint,
        &reconnect.opening_checkpoint,
        reconnect.conversation_id.as_deref(),
    ) else {
        let outcome = LiveReconnectOutcome::Skipped("no checkpoint");
        log_live_reconnect(
            &outcome,
            *reconnect_attempts,
            max_reconnects,
            &reconnect.http,
            &reconnect.last_trigger,
        );
        return outcome;
    };

    let mut closed_collecting = false;
    let mut last_fail: Option<String> = None;
    reconnect.recovery.note_delayed_hollow_if_probation();
    reconnect.recovery.begin(Instant::now());
    loop {
        if cancel_requested.load(Ordering::Acquire) {
            return reconnect_cancelled_ambiguous(terminal_error, "an unresolved recovery episode");
        }
        if Instant::now() >= hard_deadline {
            let outcome = LiveReconnectOutcome::Failed(
                last_fail.unwrap_or_else(|| "hard timeout during reconnect".into()),
            );
            log_live_reconnect(
                &outcome,
                *reconnect_attempts,
                max_reconnects,
                &reconnect.http,
                &reconnect.last_trigger,
            );
            return outcome;
        }
        if let Some(reason) = reconnect
            .recovery
            .skip_reason(Instant::now(), reconnect.force_http1)
        {
            let outcome =
                LiveReconnectOutcome::Failed(last_fail.unwrap_or_else(|| reason.to_string()));
            log_live_reconnect(
                &outcome,
                *reconnect_attempts,
                max_reconnects,
                &reconnect.http,
                &reconnect.last_trigger,
            );
            return outcome;
        }
        if let Some(reason) = live_reconnect_skip_reason(
            latest_checkpoint,
            &reconnect.opening_checkpoint,
            reconnect.conversation_id.as_deref(),
            *reconnect_attempts,
            max_reconnects,
        ) {
            let outcome = last_fail
                .map(LiveReconnectOutcome::Failed)
                .unwrap_or(LiveReconnectOutcome::Skipped(reason));
            log_live_reconnect(
                &outcome,
                *reconnect_attempts,
                max_reconnects,
                &reconnect.http,
                &reconnect.last_trigger,
            );
            return outcome;
        }
        apply_transport_breakers(reconnect, Instant::now());
        if reconnect.force_http1 && reconnect.http1_rejected {
            let outcome = LiveReconnectOutcome::Failed(
                last_fail.unwrap_or_else(|| "both transports circuit-open".into()),
            );
            log_live_reconnect(
                &outcome,
                *reconnect_attempts,
                max_reconnects,
                &reconnect.http,
                &reconnect.last_trigger,
            );
            return outcome;
        }
        if !closed_collecting {
            let cancelling_http1_append_is_ambiguous = matches!(outbound, ClientOutbound::Http1(_));
            let close_result = tokio::select! {
                _ = wait_for_live_cancel(cancel_requested) => {
                    if cancelling_http1_append_is_ambiguous {
                        let message =
                            "Cursor live cancellation interrupted an in-flight HTTP/1 control append; acceptance is ambiguous";
                        store_terminal_error(terminal_error, message);
                        return LiveReconnectOutcome::Failed(message.into());
                    }
                    return reconnect_cancelled_ambiguous(
                        terminal_error,
                        "an in-flight control close",
                    );
                }
                result = control_close_collecting_natives(pending, outbound) => result,
            };
            match close_result {
                Err(err) => {
                    let outcome = LiveReconnectOutcome::Failed(format!(
                        "Connect error {}: {}",
                        err.status, err.message
                    ));
                    log_live_reconnect(
                        &outcome,
                        *reconnect_attempts,
                        max_reconnects,
                        &reconnect.http,
                        &reconnect.last_trigger,
                    );
                    return outcome;
                }
                Ok(_) => {
                    closed_collecting = true;
                }
            }
        }
        *reconnect_attempts += 1;
        reconnect.recovery.opens = reconnect.recovery.opens.saturating_add(1);
        let remaining_ms = reconnect
            .recovery
            .remaining(Instant::now(), reconnect.force_http1)
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let delay_ms = live_reconnect_backoff_ms_for(
            *reconnect_attempts,
            reconnect.recovery.last_was_hollow,
            remaining_ms,
        );
        if delay_ms > 0 {
            tokio::select! {
                _ = wait_for_live_cancel(cancel_requested) => {
                    return reconnect_cancelled_ambiguous(
                        terminal_error,
                        "recovery backoff",
                    );
                }
                _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {}
            }
        }
        reconnect.recovery.last_was_hollow = false;
        reconnect.http =
            CursorHttpClient::with_prefer_http1(reconnect_prefers_http1(reconnect.force_http1));

        let cont = super::conversation::RunContinuation {
            conversation_id: reconnect.conversation_id.clone(),
            conversation_state: checkpoint.clone(),
            pre_fetched_blobs: kv_blobs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            has_checkpoint: !checkpoint.is_empty(),
        };
        let resolved = match super::model::resolve_cursor_model(&reconnect.model_id) {
            Ok(r) => r,
            Err(_) => super::model::CursorModelResolution {
                model_id: reconnect.model_id.clone(),
                mode: super::model::CursorAgentMode::Agent,
            },
        };
        let request_id = uuid::Uuid::new_v4().to_string();
        let run_request =
            build_resume_run_request(&resolved, &request_id, &cont, reconnect.mcp_tools.clone());
        let first_message = AgentClientMessage {
            run_request: Some(run_request),
            exec_client_message: None,
            kv_client_message: None,
            exec_client_control_message: None,
            interaction_response: None,
            client_heartbeat: None,
        };

        let open_wait = live_reconnect_open_timeout(
            hard_deadline.saturating_duration_since(Instant::now()).min(
                reconnect
                    .recovery
                    .remaining(Instant::now(), reconnect.force_http1),
            ),
            reconnect.force_http1,
        );
        if open_wait.is_zero() {
            let outcome = LiveReconnectOutcome::Failed(
                last_fail.unwrap_or_else(|| "recovery deadline exhausted".into()),
            );
            log_live_reconnect(
                &outcome,
                *reconnect_attempts,
                max_reconnects,
                &reconnect.http,
                &reconnect.last_trigger,
            );
            return outcome;
        }

        let open = reconnect.http.open_live_transport(
            &reconnect.token,
            &request_id,
            &first_message,
            &reconnect.identity,
            reconnect.force_http1,
            live_reconnect_open_allow_h1(reconnect, Instant::now()),
            open_wait,
            open_wait,
        );
        let opened = tokio::select! {
            _ = wait_for_live_cancel(cancel_requested) => {
                let message =
                    "Cursor live cancellation interrupted an in-flight ResumeAction; acceptance is ambiguous";
                store_terminal_error(terminal_error, message);
                return LiveReconnectOutcome::Failed(message.into());
            }
            result = open => result,
        };

        match opened {
            Err(err) => {
                let err = annotate_live_cursor_error(&reconnect.session_id, err);
                if live_reconnect_open_error_is_fatal(&err) {
                    let message = if is_response_less_send_error(&err)
                        && !cursor_error_is_missing_conversation_data(&err)
                        && !terminal_error_allows_fresh_retry(&err.message)
                    {
                        format!(
                            "{} (response-less ResumeAction send is ambiguous)",
                            err.message
                        )
                    } else {
                        err.message.clone()
                    };
                    let outcome = LiveReconnectOutcome::Failed(message);
                    log_live_reconnect(
                        &outcome,
                        *reconnect_attempts,
                        max_reconnects,
                        &reconnect.http,
                        &reconnect.last_trigger,
                    );
                    return outcome;
                }
                last_fail = Some(format!("{} ({})", err.message, err.status));
                record_transport_failure(reconnect, Instant::now());
                match live_reconnect_on_open_error(
                    reconnect.force_http1,
                    reconnect.http1_rejected,
                    &err,
                ) {
                    LiveReconnectTransportAction::GiveUp(reason) => {
                        let outcome = LiveReconnectOutcome::Failed(format!(
                            "{} ({reason})",
                            last_fail.as_deref().unwrap_or(reason)
                        ));
                        log_live_reconnect(
                            &outcome,
                            *reconnect_attempts,
                            max_reconnects,
                            &reconnect.http,
                            &reconnect.last_trigger,
                        );
                        return outcome;
                    }
                    LiveReconnectTransportAction::FlipToH2 => {
                        reconnect.force_http1 = false;
                        reconnect.http1_rejected = true;
                    }
                    LiveReconnectTransportAction::ForceHttp1 => {
                        reconnect.force_http1 = true;
                    }
                    LiveReconnectTransportAction::KeepTrying => {}
                }
            }
            Ok((new_outbound, response)) => {
                let stream = response.bytes_stream();
                let immediate = tokio::select! {
                    _ = wait_for_live_cancel(cancel_requested) => {
                        let message =
                            "Cursor live cancellation interrupted an accepted ResumeAction before its first response chunk; acceptance is ambiguous";
                        store_terminal_error(terminal_error, message);
                        return LiveReconnectOutcome::Failed(message.into());
                    }
                    result = take_immediate_resume_chunk(stream) => result,
                };
                match immediate {
                    Ok((prefix, rest)) => {
                        upstream_pump.abort();
                        let _ = (&mut *upstream_pump).await;
                        *outbound = new_outbound;
                        reconnect.force_http1 = matches!(*outbound, ClientOutbound::Http1(_));
                        let pump_tx = fence_live_upstream(upstream, upstream_tx);
                        *upstream_pump = spawn_upstream_pump_prefixed(prefix, rest, pump_tx);
                        *decoder = ConnectFrameDecoder::new();
                        *last_progress = Instant::now();
                        // Probation is bounded by the 45s episode, not 120s tool-result grace.
                        *resume_grace_until = None;
                        reconnect.recovery.on_probation = true;
                        let outcome = LiveReconnectOutcome::Reconnected;
                        log_live_reconnect(
                            &outcome,
                            *reconnect_attempts,
                            max_reconnects,
                            &reconnect.http,
                            &reconnect.last_trigger,
                        );
                        return outcome;
                    }
                    Err(msg) => {
                        last_fail = Some(msg.clone());
                        reconnect.recovery.last_was_hollow = true;
                        record_transport_failure(reconnect, Instant::now());
                        match live_reconnect_on_hollow_body(
                            reconnect.force_http1,
                            reconnect.http1_rejected,
                        ) {
                            LiveReconnectTransportAction::GiveUp(reason) => {
                                let outcome =
                                    LiveReconnectOutcome::Failed(format!("{msg} ({reason})"));
                                log_live_reconnect(
                                    &outcome,
                                    *reconnect_attempts,
                                    max_reconnects,
                                    &reconnect.http,
                                    &reconnect.last_trigger,
                                );
                                return outcome;
                            }
                            LiveReconnectTransportAction::ForceHttp1 => {
                                reconnect.force_http1 = true;
                                let mut fields = serde_json::Map::new();
                                fields.insert(
                                    "attempts".into(),
                                    serde_json::json!(*reconnect_attempts),
                                );
                                fields.insert("detail".into(), serde_json::json!(msg));
                                crate::logging::create_logger("cursor")
                                    .warn("live_reconnect_http1", Some(fields));
                            }
                            LiveReconnectTransportAction::FlipToH2
                            | LiveReconnectTransportAction::KeepTrying => {}
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct LiveDeltaCoalescer {
    pending: Option<CursorStreamEvent>,
    started: Option<tokio::time::Instant>,
}

impl LiveDeltaCoalescer {
    fn ingest(&mut self, event: LiveEventResult, remaining: usize) -> Vec<LiveEventResult> {
        let Ok(LiveRunEvent::Cursor(delta)) = &event else {
            let mut out = Vec::new();
            if let Some(flushed) = self.flush() {
                out.push(flushed);
            }
            out.push(event);
            return out;
        };
        let same_kind_merge = matches!(
            (&self.pending, delta),
            (
                Some(CursorStreamEvent::ThinkingDelta { .. }),
                CursorStreamEvent::ThinkingDelta { .. },
            ) | (
                Some(CursorStreamEvent::TextDelta { .. }),
                CursorStreamEvent::TextDelta { .. }
            )
        );
        if !channel_backpressured(remaining, LIVE_EVENT_CHANNEL_CAP) || !same_kind_merge {
            let mut out = Vec::new();
            if let Some(flushed) = self.flush() {
                out.push(flushed);
            }
            if channel_backpressured(remaining, LIVE_EVENT_CHANNEL_CAP)
                && matches!(
                    delta,
                    CursorStreamEvent::ThinkingDelta { .. } | CursorStreamEvent::TextDelta { .. }
                )
            {
                self.pending = Some(delta.clone());
                self.started = Some(tokio::time::Instant::now());
                return out;
            }
            out.push(event);
            return out;
        }
        match (&mut self.pending, delta) {
            (
                Some(CursorStreamEvent::ThinkingDelta { text }),
                CursorStreamEvent::ThinkingDelta { text: more },
            )
            | (
                Some(CursorStreamEvent::TextDelta { text }),
                CursorStreamEvent::TextDelta { text: more },
            ) => {
                text.push_str(more);
                if text.len() >= COALESCE_MAX_CHARS
                    || self
                        .started
                        .is_some_and(|started| started.elapsed() >= COALESCE_WINDOW)
                {
                    return self.flush().into_iter().collect();
                }
                Vec::new()
            }
            _ => {
                let mut out = Vec::new();
                if let Some(flushed) = self.flush() {
                    out.push(flushed);
                }
                out.push(event);
                out
            }
        }
    }

    fn flush(&mut self) -> Option<LiveEventResult> {
        self.started = None;
        self.pending
            .take()
            .map(|event| Ok(LiveRunEvent::Cursor(event)))
    }

    fn deadline(&self) -> Option<tokio::time::Instant> {
        let started = self.started?;
        self.pending.as_ref()?;
        Some(started + COALESCE_WINDOW)
    }
}

async fn flush_coalescer(
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    coalescer: &mut LiveDeltaCoalescer,
) -> bool {
    match coalescer.flush() {
        Some(event) => emit_or_defer(sink, deferred, event).await,
        None => true,
    }
}

async fn flush_turn_coalescer(
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    turn_ctx: Option<&mut LiveTurnCtx<'_>>,
) -> bool {
    match turn_ctx {
        Some(ctx) => flush_coalescer(sink, deferred, ctx.coalescer).await,
        None => true,
    }
}

async fn emit_live_delta(
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    event: CursorStreamEvent,
    turn_ctx: Option<&mut LiveTurnCtx<'_>>,
) -> bool {
    let Some(ctx) = turn_ctx else {
        return emit_cursor_or_defer(sink, deferred, event).await;
    };
    if sink.is_none() {
        return emit_cursor_or_defer(sink, deferred, event).await;
    }
    let remaining = sink.as_ref().map(|tx| tx.capacity()).unwrap_or(0);
    for item in ctx
        .coalescer
        .ingest(Ok(LiveRunEvent::Cursor(event)), remaining)
    {
        if !emit_or_defer(sink, deferred, item).await {
            return false;
        }
    }
    true
}

async fn control_close_natives(
    pending: &mut PendingExecState,
    outbound: &ClientOutbound,
) -> Result<bool, CursorError> {
    for exec in pending.drain_natives() {
        if let Ok(frame) = encode_control_close(exec.id) {
            match classify_outbound_send(outbound.send_connect_frame(frame).await) {
                Ok(true) => {}
                Ok(false) => return Ok(false),
                Err(err) => return Err(err),
            }
        }
    }
    Ok(true)
}

async fn control_close_collecting_natives(
    pending: &mut PendingExecState,
    outbound: &ClientOutbound,
) -> Result<bool, CursorError> {
    let natives = pending.drain_collecting_natives();
    let mut unsent = Vec::new();
    let mut iter = natives.into_iter();
    let mut all_ok = true;
    for exec in iter.by_ref() {
        let Ok(frame) = encode_control_close(exec.id) else {
            all_ok = false;
            unsent.push(exec);
            unsent.extend(iter);
            break;
        };
        match classify_outbound_send(outbound.send_connect_frame(frame).await) {
            Ok(true) => {}
            Ok(false) => {
                all_ok = false;
                unsent.push(exec);
                unsent.extend(iter);
                break;
            }
            Err(err) => {
                unsent.push(exec);
                unsent.extend(iter);
                pending.restore_collecting_natives(unsent);
                return Err(err);
            }
        }
    }
    pending.restore_collecting_natives(unsent);
    Ok(all_ok)
}

struct LiveTurnCtx<'a> {
    session_id: &'a str,
    user_prompt: &'a str,
    request_context: &'a RequestContext,
    decode_failures: &'a mut u32,
    coalescer: &'a mut LiveDeltaCoalescer,
}

fn release_generation_permit_between_segments(
    sink: &Option<mpsc::Sender<LiveEventResult>>,
    generation_permit: &mut Option<LiveGenerationPermit>,
) {
    if sink.is_none() {
        drop(generation_permit.take());
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_live_run(
    mut upstream: LiveUpstream,
    mut upstream_tx: mpsc::Sender<Result<Option<Bytes>, String>>,
    mut upstream_pump: tokio::task::JoinHandle<()>,
    mut outbound: ClientOutbound,
    mut command_rx: mpsc::Receiver<RunCommand>,
    initial_sink: mpsc::Sender<LiveEventResult>,
    pending_shared: Arc<Mutex<Vec<PendingCursorExec>>>,
    terminal_error: Arc<Mutex<Option<TerminalOutcome>>>,
    completed: Arc<AtomicBool>,
    cancel_requested: Arc<AtomicBool>,
    allowed_tool_names: Option<BTreeSet<String>>,
    session_id: String,
    run_id: String,
    seeded_blobs: HashMap<Vec<u8>, Vec<u8>>,
    mut user_prompt: String,
    request_context: RequestContext,
    mut reconnect: LiveReconnectContext,
    generation_permit: LiveGenerationPermit,
) {
    let mut generation_permit = Some(generation_permit);
    let mut sink = Some(initial_sink);
    let mut pending = PendingExecState::for_run(&run_id);
    let mut deferred = VecDeque::<LiveEventResult>::new();
    let mut decoder = ConnectFrameDecoder::new();
    let mut kv_blobs = seeded_blobs;
    let mut latest_checkpoint = reconnect.opening_checkpoint.clone();
    let mut cancel_ack = None;
    let mut was_cancelled = false;
    let mut saw_text = false;
    let mut useful = false;
    let mut logical_tools_waiting = LogicalToolTracker::default();
    let mut last_progress = Instant::now();
    let mut last_liveness = last_progress;
    let mut resume_grace_until: Option<Instant> = None;
    let mut xml_parser = CursorToolUseXmlParser::new(allowed_tool_names.clone());
    let mut coalescer = LiveDeltaCoalescer::default();
    let mut decode_failures: u32 = 0;
    let run_started = Instant::now();
    // Keep the quiet window short: Claude Code cannot start tools until we
    // expose the batch. 100ms felt like extra "tool lag" vs native CLI.
    // 0 is allowed (expose on next select tick). This does NOT gate thinking/
    // text deltas — those forward immediately while the SSE sink is live.
    let tool_batch_quiet =
        Duration::from_millis(env_u64_allow_zero("CCP_CURSOR_TOOL_BATCH_MS", 25));
    let resume_grace = Duration::from_secs(env_u64("CCP_CURSOR_RESUME_GRACE_SECS", 120));
    let mut exec_heartbeat = tokio::time::interval(Duration::from_secs(env_u64(
        "CCP_CURSOR_EXEC_HEARTBEAT_SECS",
        3,
    )));
    exec_heartbeat.tick().await;
    let mut client_heartbeat =
        tokio::time::interval(Duration::from_secs(env_u64("CCP_CURSOR_HEARTBEAT_SECS", 5)));
    client_heartbeat.tick().await;
    let client_hb_frame = {
        let message = AgentClientMessage {
            run_request: None,
            exec_client_message: None,
            kv_client_message: None,
            exec_client_control_message: None,
            interaction_response: None,
            client_heartbeat: Some(ClientHeartbeat {}),
        };
        encode_agent_message(&message).ok()
    };
    // Cache idle/timeout knobs once — the 250ms idle arm used to re-parse env
    // on every tick (thousands of times during long thinking).
    let setup_idle = Duration::from_secs(env_u64("CCP_CURSOR_SETUP_IDLE_SECS", 45));
    // CLI stall-detector failTimeoutMs default 30s; heartbeat-only thinking is
    // 2× stream idle (240s). setup_idle is only for a stream with no frames.
    let stream_idle = Duration::from_secs(env_u64("CCP_CURSOR_IDLE_SECS", 120));
    // Live path always waits for Cursor `turn_ended` (or hard timeout). The old
    // 8s complete_idle for tool-less runs truncated Fable quiet thinking.
    let wait_for_turn_ended = true;
    let complete_idle = Duration::from_millis(env_u64(
        "CCP_CURSOR_COMPLETE_IDLE_MS",
        u64::MAX / 4, // disabled unless explicitly overridden
    ));
    let hard = Duration::from_secs(live_hard_timeout_secs());
    let hard_deadline = run_started + hard;
    let tool_ttl = Duration::from_secs(env_u64("CCP_CURSOR_TOOL_TTL_SECS", 600));
    // CLI transport/stall retries: 10 (prod). Keep Anthropic SSE open across
    // brief Cursor disconnects when we have a checkpoint to ResumeAction.
    let max_reconnects = env_u64("CCP_CURSOR_RECONNECT_MAX", 10) as u32;
    let mut reconnect_attempts: u32 = 0;
    let mut got_chunk_since_reconnect = false;
    let mut accepted_resume_unconfirmed = false;
    // A normal turn_ended is not authoritative until Connect END/EOF. Holding
    // it prevents a later, separately chunked END error from being masked by a
    // fabricated successful Anthropic end_turn.
    let mut held_turn_end_frames = Vec::<ConnectFrame>::new();

    macro_rules! process_driver_frames {
        ($frames:expr) => {{
            let mut keep_running = true;
            for frame in $frames {
                let mut turn = LiveTurnCtx {
                    session_id: &session_id,
                    user_prompt: &user_prompt,
                    request_context: &request_context,
                    decode_failures: &mut decode_failures,
                    coalescer: &mut coalescer,
                };
                if !process_live_frame(
                    frame,
                    &outbound,
                    &mut sink,
                    &mut deferred,
                    &mut pending,
                    &pending_shared,
                    &mut kv_blobs,
                    &mut latest_checkpoint,
                    &terminal_error,
                    allowed_tool_names.as_ref(),
                    &mut saw_text,
                    &mut useful,
                    &mut logical_tools_waiting,
                    &mut last_progress,
                    tool_batch_quiet,
                    &mut xml_parser,
                    Some(&mut turn),
                )
                .await
                {
                    keep_running = false;
                    break;
                }
            }
            if keep_running && pending.can_expose() && pending.collecting_has_lifecycle() {
                if !expose_collected_tools(&mut pending, &pending_shared, &mut sink).await {
                    keep_running = false;
                }
            }
            keep_running
        }};
    }

    'driver: loop {
        if cancel_requested.load(Ordering::Acquire) {
            was_cancelled = true;
            mark_live_cancelled(
                &mut sink,
                &terminal_error,
                live_acceptance_unresolved(
                    !held_turn_end_frames.is_empty(),
                    accepted_resume_unconfirmed,
                    reconnect.recovery.on_probation && !got_chunk_since_reconnect,
                ),
            );
            break 'driver;
        }
        // Check before select: Cursor InteractionUpdate.heartbeat / client
        // heartbeats keep the biased upstream/heartbeat arms ready and would
        // otherwise starve the 250ms closed-sink poll for minutes — leaving a
        // zombie "already generating" run after Claude Code disconnects.
        if sink.as_ref().is_some_and(mpsc::Sender::is_closed) {
            // Keep BiDi only when Claude still owes us native tool_results.
            // logical_tools_waiting alone must not pin the session: those are
            // UI hints, not Anthropic-exposed pending tools.
            if pending.is_empty() {
                if live_acceptance_unresolved(
                    !held_turn_end_frames.is_empty(),
                    accepted_resume_unconfirmed,
                    reconnect.recovery.on_probation && !got_chunk_since_reconnect,
                ) {
                    report_terminal_error(
                        &mut sink,
                        &terminal_error,
                        "Cursor downstream disconnected while upstream completion was unresolved; acceptance is ambiguous".into(),
                    )
                    .await;
                }
                break 'driver;
            }
            sink = None;
        }
        // Cursor is between downstream Anthropic segments. Keep the resumable
        // BiDi worker alive, but return scarce generation capacity until the
        // matching tool results are ready to send.
        release_generation_permit_between_segments(&sink, &mut generation_permit);
        if run_started.elapsed() >= hard {
            let message = if !held_turn_end_frames.is_empty() {
                "Cursor live run timed out after an uncommitted turn_ended frame; completion is ambiguous"
                    .into()
            } else if pending.is_empty() {
                "Cursor live run hard timeout".into()
            } else {
                "Cursor live run hard timeout with pending native tools".into()
            };
            report_terminal_error(&mut sink, &terminal_error, message).await;
            break 'driver;
        }
        if live_probation_expired(
            reconnect.recovery.on_probation,
            got_chunk_since_reconnect,
            reconnect
                .recovery
                .remaining(Instant::now(), reconnect.force_http1),
        ) {
            let message = hollow_resume_terminal_message(
                &session_id,
                reconnect.opening_checkpoint.is_some(),
                useful,
                pending.is_empty(),
                &mut latest_checkpoint,
                &mut kv_blobs,
                "Cursor resume produced no progress before the recovery deadline",
            );
            report_terminal_error(&mut sink, &terminal_error, message).await;
            break 'driver;
        }
        if !held_turn_end_frames.is_empty() {
            // Wait for the authoritative Connect END (which may be delivered
            // in a later HTTP body chunk) before exposing success.
        } else if let Some(since) = pending.oldest_since() {
            if since.elapsed() >= tool_ttl {
                report_terminal_error(
                    &mut sink,
                    &terminal_error,
                    "Cursor tool result wait expired".into(),
                )
                .await;
                break 'driver;
            }
        } else if !reconnect.recovery.on_probation
            && resume_grace_until.is_some_and(|until| Instant::now() < until)
        {
            // Post-tool-result grace: keep waiting for the next model delta.
        } else if !logical_tools_waiting.is_empty() {
            if logical_tools_waiting
                .oldest_since()
                .is_some_and(|since| since.elapsed() >= stream_idle)
            {
                logical_tools_waiting.clear();
            }
        } else if !wait_for_turn_ended && saw_text && last_progress.elapsed() >= complete_idle {
            emit_cursor_or_defer(&mut sink, &mut deferred, CursorStreamEvent::End).await;
            break 'driver;
        } else if let Some(message) = live_idle_stall_message(
            useful,
            saw_text,
            allowed_tool_names.is_some(),
            pending.is_empty(),
            last_progress.elapsed(),
            last_liveness.elapsed(),
            setup_idle,
            stream_idle,
        ) {
            let can_resume = live_reconnect_resume_state(
                &latest_checkpoint,
                &reconnect.opening_checkpoint,
                reconnect.conversation_id.as_deref(),
            )
            .is_some();
            if can_resume {
                let reconnect_outcome = if live_should_resume_after_drop(
                    reconnect.recovery.on_probation,
                    got_chunk_since_reconnect,
                ) {
                    prepare_live_reconnect(
                        &mut reconnect,
                        got_chunk_since_reconnect,
                        reconnect_attempts,
                        Some("idle_stall"),
                    );
                    try_live_reconnect(
                        &mut reconnect,
                        &mut outbound,
                        &mut upstream,
                        &mut upstream_tx,
                        &mut upstream_pump,
                        cancel_requested.as_ref(),
                        &terminal_error,
                        &mut decoder,
                        &latest_checkpoint,
                        &kv_blobs,
                        &mut pending,
                        &mut reconnect_attempts,
                        max_reconnects,
                        &mut last_progress,
                        &mut resume_grace_until,
                        resume_grace,
                        hard_deadline,
                    )
                    .await
                } else {
                    LiveReconnectOutcome::Failed(
                        "Cursor resume produced no progress before the stream stalled".into(),
                    )
                };
                if matches!(reconnect_outcome, LiveReconnectOutcome::Reconnected) {
                    got_chunk_since_reconnect = false;
                    last_liveness = Instant::now();
                    continue 'driver;
                }
                let fallback = format!("{message}{}", reconnect_note(&reconnect_outcome));
                let message = hollow_resume_terminal_message(
                    &session_id,
                    reconnect.opening_checkpoint.is_some(),
                    useful,
                    pending.is_empty(),
                    &mut latest_checkpoint,
                    &mut kv_blobs,
                    fallback,
                );
                report_terminal_error(&mut sink, &terminal_error, message).await;
            } else {
                report_terminal_error(&mut sink, &terminal_error, message.into()).await;
            }
            break 'driver;
        }
        let batch_deadline = pending.native_collect_deadline().or_else(|| {
            pending
                .collecting_has_lifecycle()
                .then(|| pending.client_only_collect_deadline())
                .flatten()
        });
        let coalesce_deadline = coalescer.deadline();
        tokio::select! {
            biased;

            command = command_rx.recv() => {
                match command {
                    Some(RunCommand::Cancel { ack }) => {
                        cancel_ack = ack;
                        was_cancelled = true;
                        // Registry may already have removed us via supersede;
                        // still mark completed so prune/get stay consistent.
                        mark_live_cancelled(
                            &mut sink,
                            &terminal_error,
                            live_acceptance_unresolved(
                                !held_turn_end_frames.is_empty(),
                                accepted_resume_unconfirmed,
                                reconnect.recovery.on_probation && !got_chunk_since_reconnect,
                            ),
                        );
                        break 'driver;
                    }
                    None => {
                        let message = live_control_close_message(live_acceptance_unresolved(
                            !held_turn_end_frames.is_empty(),
                            accepted_resume_unconfirmed,
                            reconnect.recovery.on_probation && !got_chunk_since_reconnect,
                        ));
                        report_terminal_error(
                            &mut sink,
                            &terminal_error,
                            message.into(),
                        )
                        .await;
                        break 'driver;
                    }
                    Some(RunCommand::ResumeBatch {
                        tool_results,
                        sink: next_sink,
                        ack,
                        permit: _resume_permit,
                        generation_permit: resume_generation_permit,
                        dispatch_state,
                    }) => {
                        if dispatch_state
                            .compare_exchange(
                                RESUME_DISPATCH_WAITING,
                                RESUME_DISPATCH_STARTED,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_err()
                        {
                            let _ = ack.send(Err(CursorError::new(
                                409,
                                "Cursor live resume dispatch was cancelled before driver acceptance",
                                None,
                            )));
                            continue;
                        }
                        let frames = match encode_tool_result_batch(pending.awaiting(), &tool_results) {
                            Ok(frames) => frames,
                            Err(error) => {
                                let _ = ack.send(Err(CursorError::new(400, error, None)));
                                continue;
                            }
                        };
                        if generation_permit
                            .replace(resume_generation_permit)
                            .is_some()
                        {
                            crate::logging::create_logger("cursor").warn(
                                "live_generation_lease_replaced",
                                Some(serde_json::Map::from_iter([(
                                    "sessionId".into(),
                                    serde_json::json!(session_id.as_str()),
                                )])),
                            );
                        }
                        // Establish the Anthropic response before any bounded
                        // transport/reconnect work. Send failures are delivered
                        // on this event stream instead of leaving the POST
                        // silent while the driver recovers.
                        if ack.send(Ok(())).is_err() {
                            continue;
                        }
                        sink = Some(next_sink);
                        saw_text = false;
                        useful = false;
                        // The accepted ResumeBatch has already delivered these
                        // native tool results to Cursor. If the next segment is
                        // hollow, continue from its checkpoint instead of
                        // replaying Anthropic history and executing the tools
                        // again.
                        user_prompt = EMPTY_TURN_CHECKPOINT_CONTINUE_PROMPT.to_string();
                        logical_tools_waiting.clear();
                        last_progress = Instant::now();
                        last_liveness = last_progress;

                        let mut send_failed = false;
                        let mut terminal_send: Option<CursorError> = None;
                        let mut sent_frames = 0usize;
                        for frame in &frames {
                            match outbound.send_connect_frame(frame.clone()).await {
                                Ok(()) => {
                                    sent_frames += 1;
                                }
                                Err(err) if sent_frames > 0 => {
                                    terminal_send = Some(partial_tool_result_send_error(
                                        err,
                                        sent_frames,
                                        frames.len(),
                                    ));
                                    break;
                                }
                                Err(err) if live_send_failure_is_terminal(&err) => {
                                    terminal_send = Some(err);
                                    break;
                                }
                                Err(_) => {
                                    send_failed = true;
                                    break;
                                }
                            }
                        }
                        if let Some(err) = terminal_send {
                            report_terminal_error(
                                &mut sink,
                                &terminal_error,
                                annotate_live_cursor_error(&session_id, err).to_string(),
                            )
                            .await;
                            break 'driver;
                        }
                        if cancel_requested.load(Ordering::Acquire) {
                            was_cancelled = true;
                            mark_live_cancelled(
                                &mut sink,
                                &terminal_error,
                                sent_frames > 0
                                    || live_acceptance_unresolved(
                                        !held_turn_end_frames.is_empty(),
                                        accepted_resume_unconfirmed,
                                        reconnect.recovery.on_probation
                                            && !got_chunk_since_reconnect,
                                    ),
                            );
                            break 'driver;
                        }
                        if send_failed {
                            let reconnect_outcome = if live_should_resume_after_drop(
                                reconnect.recovery.on_probation,
                                got_chunk_since_reconnect,
                            ) {
                                prepare_live_reconnect(
                                    &mut reconnect,
                                    got_chunk_since_reconnect,
                                    reconnect_attempts,
                                    Some("outbound_send"),
                                );
                                try_live_reconnect(
                                    &mut reconnect,
                                    &mut outbound,
                                    &mut upstream,
                                    &mut upstream_tx,
                                    &mut upstream_pump,
                                    cancel_requested.as_ref(),
                                    &terminal_error,
                                    &mut decoder,
                                    &latest_checkpoint,
                                    &kv_blobs,
                                    &mut pending,
                                    &mut reconnect_attempts,
                                    max_reconnects,
                                    &mut last_progress,
                                    &mut resume_grace_until,
                                    resume_grace,
                                    hard_deadline,
                                )
                                .await
                            } else {
                                LiveReconnectOutcome::Failed(
                                    "Cursor resume produced no progress before tool-result send failed"
                                        .into(),
                                )
                            };
                            send_failed = !matches!(
                                reconnect_outcome,
                                LiveReconnectOutcome::Reconnected
                            );
                            if !send_failed {
                                got_chunk_since_reconnect = false;
                                let mut resent_frames = 0usize;
                                for frame in &frames {
                                    match outbound.send_connect_frame(frame.clone()).await {
                                        Ok(()) => {
                                            resent_frames += 1;
                                        }
                                        Err(err) if resent_frames > 0 => {
                                            let err = annotate_live_cursor_error(
                                                &session_id,
                                                partial_tool_result_send_error(
                                                    err,
                                                    resent_frames,
                                                    frames.len(),
                                                ),
                                            );
                                            report_terminal_error(
                                                &mut sink,
                                                &terminal_error,
                                                err.to_string(),
                                            )
                                            .await;
                                            break 'driver;
                                        }
                                        Err(err) if live_send_failure_is_terminal(&err) => {
                                            report_terminal_error(
                                                &mut sink,
                                                &terminal_error,
                                                annotate_live_cursor_error(&session_id, err)
                                                    .to_string(),
                                            )
                                            .await;
                                            break 'driver;
                                        }
                                        Err(_) => {
                                            send_failed = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            if cancel_requested.load(Ordering::Acquire) {
                                was_cancelled = true;
                                mark_live_cancelled(
                                    &mut sink,
                                    &terminal_error,
                                    live_acceptance_unresolved(
                                        !held_turn_end_frames.is_empty(),
                                        accepted_resume_unconfirmed,
                                        reconnect.recovery.on_probation
                                            && !got_chunk_since_reconnect,
                                    ),
                                );
                                break 'driver;
                            }
                            if send_failed {
                                let message = format!(
                                    "Cursor request stream closed during tool resume{}",
                                    reconnect_note(&reconnect_outcome)
                                );
                                report_terminal_error(
                                    &mut sink,
                                    &terminal_error,
                                    message.clone(),
                                )
                                .await;
                                break 'driver;
                            }
                        }
                        pending.complete_awaiting();
                        accepted_resume_unconfirmed = true;
                        pending_shared
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .clear();
                        // After tool results, Cursor often thinks quietly before the
                        // next text/tool delta. Don't trip setup_idle during that gap
                        // (was the "no useful progress" hang after a healthy tool_use).
                        resume_grace_until = Some(Instant::now() + resume_grace);
                        while let Some(event) = deferred.pop_front() {
                            record_segment_progress(
                                &event,
                                &mut saw_text,
                                &mut useful,
                                &mut last_progress,
                            );
                            if !send_live_event(&mut sink, event).await {
                                report_terminal_error(
                                    &mut sink,
                                    &terminal_error,
                                    "Cursor downstream disconnected after an accepted tool resume; acceptance is ambiguous".into(),
                                )
                                .await;
                                break 'driver;
                            }
                        }
                    }
                }
            }
            // Prefer draining Cursor InteractionUpdates (thinking/text) over
            // keepalive ticks whenever both are ready — max-effort thinking
            // should not wait behind a heartbeat interval edge.
            item = upstream.recv() => {
                match item {
                    Some(Ok(Some(chunk))) => {
                        last_liveness = Instant::now();
                        let checkpoint_before = latest_checkpoint
                            .as_ref()
                            .map(|checkpoint| (checkpoint.as_ptr() as usize, checkpoint.len()));
                        let frames = match decoder.push(&chunk) {
                            Ok(frames) => frames,
                            Err(error) => {
                                let message = format!("Cursor frame decode: {error}");
                                report_terminal_error(&mut sink, &terminal_error, message).await;
                                break 'driver;
                            }
                        };
                        // A decoded batch can contain turn_ended/client-only
                        // events followed by an authoritative Connect END
                        // error. Pre-scan before any earlier frame can emit a
                        // successful End or tear down the driver.
                        if let Some(error) = frames.iter().find_map(|frame| {
                            (frame.flags & FLAG_END != 0)
                                .then(|| parse_connect_error(&frame.payload))
                                .flatten()
                        }) {
                            let message = annotate_connect_end_error(
                                &session_id,
                                error,
                                Some((&mut latest_checkpoint, &mut kv_blobs)),
                            );
                            report_terminal_error(&mut sink, &terminal_error, message).await;
                            break 'driver;
                        }
                        if live_reconnect_should_reset_budget(&frames) {
                            got_chunk_since_reconnect = true;
                            accepted_resume_unconfirmed = false;
                            reconnect_attempts = 0;
                            record_transport_success(&mut reconnect);
                            reconnect.recovery.reset();
                        }
                        let mut frames = frames;
                        let mut holding_turn_end = false;
                        if !held_turn_end_frames.is_empty() {
                            held_turn_end_frames.append(&mut frames);
                            if held_turn_end_frames
                                .iter()
                                .any(|frame| frame.flags & FLAG_END != 0)
                            {
                                frames = std::mem::take(&mut held_turn_end_frames);
                            } else {
                                continue 'driver;
                            }
                        } else if let Some(index) = frames
                            .iter()
                            .position(connect_frame_has_top_level_turn_ended)
                        {
                            held_turn_end_frames = frames.split_off(index);
                            if held_turn_end_frames
                                .iter()
                                .any(|frame| frame.flags & FLAG_END != 0)
                            {
                                frames.extend(std::mem::take(&mut held_turn_end_frames));
                            } else {
                                holding_turn_end = true;
                            }
                        }
                        if !process_driver_frames!(frames) {
                            break 'driver;
                        }
                        if latest_checkpoint.as_ref().map(|checkpoint| {
                            (checkpoint.as_ptr() as usize, checkpoint.len())
                        }) != checkpoint_before
                        {
                            if let Some(checkpoint) = latest_checkpoint.as_ref() {
                                super::conversation::save_checkpoint(
                                    &session_id,
                                    checkpoint.clone(),
                                );
                            }
                        }
                        if holding_turn_end {
                            continue 'driver;
                        }
                        // Quiet window already elapsed (incl. TOOL_BATCH_MS=0):
                        // expose in this iteration so we do not wait for the
                        // next select pass behind heartbeats / idle sleep.
                        if pending
                            .native_collect_deadline()
                            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
                        {
                            if !flush_coalescer(&mut sink, &mut deferred, &mut coalescer).await {
                                break 'driver;
                            }
                            if !expose_collected_native_tools(
                                &mut pending,
                                &pending_shared,
                                &mut sink,
                            )
                            .await
                            {
                                break 'driver;
                            }
                        } else if pending.collecting_has_lifecycle()
                            && pending
                                .client_only_collect_deadline()
                                .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
                        {
                            if !flush_coalescer(&mut sink, &mut deferred, &mut coalescer).await {
                                break 'driver;
                            }
                            if !expose_collected_tools(&mut pending, &pending_shared, &mut sink)
                                .await
                            {
                                break 'driver;
                            }
                        }
                    }
                    Some(Ok(None)) | None => {
                        if decoder.buffered() != 0 {
                            report_terminal_error(
                                &mut sink,
                                &terminal_error,
                                format!(
                                    "Cursor upstream ended with {} bytes of an incomplete Connect frame; completion is ambiguous",
                                    decoder.buffered()
                                ),
                            )
                            .await;
                            break 'driver;
                        }
                        if !held_turn_end_frames.is_empty() {
                            if !process_driver_frames!(std::mem::take(
                                &mut held_turn_end_frames
                            )) {
                                break 'driver;
                            }
                            // Defensive fallback if a malformed candidate did
                            // not actually terminate when decoded.
                            if !process_driver_frames!(std::iter::once(ConnectFrame {
                                flags: FLAG_END,
                                payload: Bytes::new(),
                            })) {
                                break 'driver;
                            }
                            break 'driver;
                        }
                        // Abrupt EOF without Connect END / turn_ended — try
                        // ResumeAction reconnect (CLI stall recovery) unless this
                        // is a delayed hollow after HTTP 200 with no body.
                        let reconnect_outcome = if live_should_resume_after_drop(
                            reconnect.recovery.on_probation,
                            got_chunk_since_reconnect,
                        ) {
                            prepare_live_reconnect(
                                &mut reconnect,
                                got_chunk_since_reconnect,
                                reconnect_attempts,
                                Some("stream_eof"),
                            );
                            try_live_reconnect(
                                &mut reconnect,
                                &mut outbound,
                                &mut upstream,
                                &mut upstream_tx,
                                &mut upstream_pump,
                                cancel_requested.as_ref(),
                                &terminal_error,
                                &mut decoder,
                                &latest_checkpoint,
                                &kv_blobs,
                                &mut pending,
                                &mut reconnect_attempts,
                                max_reconnects,
                                &mut last_progress,
                                &mut resume_grace_until,
                                resume_grace,
                                hard_deadline,
                            )
                            .await
                        } else {
                            LiveReconnectOutcome::Failed(
                                "Cursor resume produced no progress before the stream ended"
                                    .into(),
                            )
                        };
                        if matches!(reconnect_outcome, LiveReconnectOutcome::Reconnected) {
                            got_chunk_since_reconnect = false;
                            last_liveness = Instant::now();
                            continue 'driver;
                        }
                        // Same empty-turn recovery as FLAG_END: flush trailing
                        // Workflow XML, then surface a note instead of Out:0.
                        if !flush_xml_tool_uses(
                            &mut xml_parser,
                            &mut pending,
                            &pending_shared,
                            &mut sink,
                            &mut deferred,
                            allowed_tool_names.as_ref(),
                            &mut saw_text,
                            &mut useful,
                            &mut last_progress,
                        )
                        .await
                        {
                            break 'driver;
                        }
                        if !pending.is_empty() {
                            if pending.has_outstanding_native()
                                && pending.all().any(|exec| {
                                    matches!(
                                        exec.kind,
                                        super::exec_results::CursorExecKind::ClientOnly
                                    )
                                })
                            {
                                match control_close_natives(&mut pending, &outbound).await {
                                    Ok(_) => {}
                                    Err(err) => {
                                        report_terminal_error(
                                            &mut sink,
                                            &terminal_error,
                                            err.to_string(),
                                        )
                                        .await;
                                        break 'driver;
                                    }
                                }
                            }
                            if pending.all_client_only() {
                                let _ = flush_coalescer(&mut sink, &mut deferred, &mut coalescer)
                                    .await;
                                let _ =
                                    expose_collected_tools(&mut pending, &pending_shared, &mut sink)
                                        .await;
                            } else {
                                report_terminal_error(
                                    &mut sink,
                                    &terminal_error,
                                    format!(
                                        "Cursor upstream ended with pending native tools{}",
                                        reconnect_note(&reconnect_outcome)
                                    ),
                                )
                                .await;
                            }
                            break 'driver;
                        }
                        if !flush_coalescer(&mut sink, &mut deferred, &mut coalescer).await {
                            break 'driver;
                        }
                        if abrupt_eof_should_error(useful || saw_text) {
                            let fallback = format!(
                                "Cursor upstream ended without turn_ended{}",
                                reconnect_note(&reconnect_outcome)
                            );
                            let message = hollow_resume_terminal_message(
                                &session_id,
                                reconnect.opening_checkpoint.is_some(),
                                useful,
                                pending.is_empty(),
                                &mut latest_checkpoint,
                                &mut kv_blobs,
                                fallback,
                            );
                            report_terminal_error(&mut sink, &terminal_error, message).await;
                            break 'driver;
                        }
                        break 'driver;
                    }
                    Some(Err(error)) => {
                        if !held_turn_end_frames.is_empty() {
                            report_terminal_error(
                                &mut sink,
                                &terminal_error,
                                format!(
                                    "Cursor transport failed after an uncommitted turn_ended frame ({error}); completion is ambiguous"
                                ),
                            )
                            .await;
                            break 'driver;
                        }
                        let reconnect_outcome = if live_should_resume_after_drop(
                            reconnect.recovery.on_probation,
                            got_chunk_since_reconnect,
                        ) {
                            prepare_live_reconnect(
                                &mut reconnect,
                                got_chunk_since_reconnect,
                                reconnect_attempts,
                                Some(error.as_str()),
                            );
                            try_live_reconnect(
                                &mut reconnect,
                                &mut outbound,
                                &mut upstream,
                                &mut upstream_tx,
                                &mut upstream_pump,
                                cancel_requested.as_ref(),
                                &terminal_error,
                                &mut decoder,
                                &latest_checkpoint,
                                &kv_blobs,
                                &mut pending,
                                &mut reconnect_attempts,
                                max_reconnects,
                                &mut last_progress,
                                &mut resume_grace_until,
                                resume_grace,
                                hard_deadline,
                            )
                            .await
                        } else {
                            LiveReconnectOutcome::Failed(format!(
                                "Cursor resume produced no progress ({error})"
                            ))
                        };
                        if matches!(reconnect_outcome, LiveReconnectOutcome::Reconnected) {
                            got_chunk_since_reconnect = false;
                            last_liveness = Instant::now();
                            continue 'driver;
                        }
                        let fallback = format!(
                            "Cursor response stream: {error}{}",
                            reconnect_note(&reconnect_outcome)
                        );
                        let message = hollow_resume_terminal_message(
                            &session_id,
                            reconnect.opening_checkpoint.is_some(),
                            useful,
                            pending.is_empty(),
                            &mut latest_checkpoint,
                            &mut kv_blobs,
                            fallback,
                        );
                        report_terminal_error(&mut sink, &terminal_error, message).await;
                        break 'driver;
                    }
                }
            }
            _ = exec_heartbeat.tick(), if !pending.is_empty() => {
                // Never await append/send here — even with upstream preferred in
                // biased select, a blocking BidiAppend freezes this task for a
                // full RTT while Cursor keeps sending deltas (CLI does not).
                let ids: Vec<u32> = pending
                    .all()
                    .filter(|current| {
                        !matches!(current.kind, super::exec_results::CursorExecKind::ClientOnly)
                    })
                    .map(|current| current.id)
                    .collect();
                for id in ids {
                    if let Ok(frame) = encode_exec_heartbeat(id) {
                        let _ = outbound.try_send_heartbeat_frame(frame);
                    }
                }
            }
            _ = client_heartbeat.tick() => {
                if let Some(ref frame) = client_hb_frame {
                    let _ = outbound.try_send_heartbeat_frame(frame.clone());
                }
            }
            // With biased selection, drain a response chunk that is already
            // ready at the quiet-window boundary before closing the batch. A
            // sibling Exec in that chunk therefore joins the same Anthropic
            // response instead of being needlessly serialized.
            _ = async {
                if let Some(deadline) = batch_deadline {
                    tokio::time::sleep_until(deadline).await;
                }
            }, if batch_deadline.is_some() => {
                if !flush_coalescer(&mut sink, &mut deferred, &mut coalescer).await {
                    break 'driver;
                }
                let exposed = if pending.collecting_has_lifecycle()
                    && pending.native_collect_deadline().is_none()
                {
                    expose_collected_tools(&mut pending, &pending_shared, &mut sink).await
                } else {
                    expose_collected_native_tools(&mut pending, &pending_shared, &mut sink).await
                };
                if !exposed {
                    break 'driver;
                }
            }
            _ = async {
                if let Some(deadline) = coalesce_deadline {
                    tokio::time::sleep_until(deadline).await;
                }
            }, if coalesce_deadline.is_some() => {
                if !flush_coalescer(&mut sink, &mut deferred, &mut coalescer).await {
                    break 'driver;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(250)) => {
                // Wake so top-of-loop deadline checks cannot be starved by
                // heartbeat/KV frames on the biased upstream arm.
            }
        }
    }

    // Persist checkpoint + KV blobs so the next Claude turn can resume Cursor state.
    // ClientOnly (Workflow/Skill) teardown must not keep an in-flight MCP
    // checkpoint — the next POST is a fresh turn that includes tool_results.
    let client_only_teardown = pending_shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .any(|exec| matches!(exec.kind, CursorExecKind::ClientOnly));
    if !was_cancelled && client_only_teardown && pending.has_outstanding_native() {
        if let Err(err) = control_close_natives(&mut pending, &outbound).await {
            report_terminal_error(&mut sink, &terminal_error, err.to_string()).await;
        }
    }
    upstream_pump.abort();
    let _ = upstream_pump.await;
    drop(sink);
    drop(outbound);
    drop(upstream);
    drop(upstream_tx);
    let persist_continuation = live_should_persist_continuation_message(
        terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|outcome| outcome.message.as_str()),
    );
    if persist_continuation {
        if client_only_teardown {
            super::conversation::clear_checkpoint(&session_id);
        } else if let Some(checkpoint) = latest_checkpoint.take() {
            super::conversation::save_checkpoint(&session_id, checkpoint);
        }
        super::conversation::merge_blobs(&session_id, &kv_blobs);
    }
    pending_shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    if terminal_error
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_none()
    {
        // Seal before `completed` so prune_finished cannot drop a successful
        // Running handle in the gap and lose the retry fingerprint.
        LiveRunRegistry::seal_success_if(&session_id, &run_id);
    }
    completed.store(true, Ordering::Release);
    if let Some(ack) = cancel_ack {
        let _ = ack.send(());
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_live_frame(
    frame: ConnectFrame,
    outbound: &ClientOutbound,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    pending: &mut PendingExecState,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    kv_blobs: &mut HashMap<Vec<u8>, Vec<u8>>,
    latest_checkpoint: &mut Option<Vec<u8>>,
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    allowed_tool_names: Option<&BTreeSet<String>>,
    saw_text: &mut bool,
    useful: &mut bool,
    logical_tools_waiting: &mut LogicalToolTracker,
    last_progress: &mut Instant,
    tool_batch_quiet: Duration,
    xml_parser: &mut CursorToolUseXmlParser,
    mut turn_ctx: Option<&mut LiveTurnCtx<'_>>,
) -> bool {
    let frame_session_id = turn_ctx.as_ref().map(|ctx| ctx.session_id).unwrap_or("");
    if frame.flags & FLAG_END != 0 {
        // END errors are authoritative. Handle them before flushing buffered
        // XML or exposing any client-only tools, or a poisoned conversation
        // can bypass the reset and leave later requests bound to missing blobs.
        if let Some(error) = parse_connect_error(&frame.payload) {
            let message = annotate_connect_end_error(
                turn_ctx.as_ref().map(|ctx| ctx.session_id).unwrap_or(""),
                error,
                Some((latest_checkpoint, kv_blobs)),
            );
            report_terminal_error(sink, terminal_error, message).await;
            return false;
        }
        // Trailing Workflow/Skill XML may still be buffered when Connect END arrives.
        if !flush_xml_tool_uses(
            xml_parser,
            pending,
            pending_shared,
            sink,
            deferred,
            allowed_tool_names,
            saw_text,
            useful,
            last_progress,
        )
        .await
        {
            return false;
        }
        if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
            return false;
        }
        if !pending.is_empty() {
            if pending.has_outstanding_native()
                && pending.all().any(|exec| {
                    matches!(exec.kind, super::exec_results::CursorExecKind::ClientOnly)
                })
            {
                match control_close_natives(pending, outbound).await {
                    Ok(true) => {}
                    Ok(false) => return false,
                    Err(err) => {
                        report_terminal_error(sink, terminal_error, err.to_string()).await;
                        return false;
                    }
                }
            }
            if pending.all_client_only() {
                return expose_collected_tools(pending, pending_shared, sink).await;
            }
            report_terminal_error(
                sink,
                terminal_error,
                "Cursor upstream ended with pending native tools".to_string(),
            )
            .await;
            return false;
        }
        // Connect END without turn_ended used to emit bare End → silent
        // Anthropic Out:0. Mirror the turn_ended empty-turn recovery.
        if !recover_empty_turn_if_needed(
            saw_text,
            useful,
            sink,
            pending,
            pending_shared,
            terminal_error,
            allowed_tool_names,
            turn_ctx.as_ref().map(|ctx| ctx.user_prompt).unwrap_or(""),
            turn_ctx.as_ref().map(|ctx| ctx.session_id),
            latest_checkpoint,
            kv_blobs,
            "flag_end",
        )
        .await
        {
            return false;
        }
        let _ = emit_cursor_or_defer(sink, deferred, CursorStreamEvent::End).await;
        return false;
    }
    let message = match super::client::decode_frame_payload(&frame) {
        Ok(message) => {
            if let Some(ctx) = turn_ctx.as_mut() {
                *ctx.decode_failures = 0;
            }
            message
        }
        Err(error) => {
            let payload_len = frame.payload.len();
            if let Some(ctx) = turn_ctx.as_mut() {
                *ctx.decode_failures = ctx.decode_failures.saturating_add(1);
                let mut fields = serde_json::Map::new();
                fields.insert("payload_len".into(), serde_json::json!(payload_len));
                fields.insert("error".into(), serde_json::json!(error.to_string()));
                fields.insert(
                    "consecutive".into(),
                    serde_json::json!(*ctx.decode_failures),
                );
                crate::logging::create_logger("cursor").warn("live_frame_decode", Some(fields));
                if *ctx.decode_failures >= MAX_CONSECUTIVE_DECODE_FAILURES {
                    report_terminal_error(
                        sink,
                        terminal_error,
                        format!(
                            "Cursor prost decode failed {MAX_CONSECUTIVE_DECODE_FAILURES} consecutive frames (last: {error}, {payload_len} bytes)"
                        ),
                    )
                    .await;
                    return false;
                }
            } else {
                let mut fields = serde_json::Map::new();
                fields.insert("payload_len".into(), serde_json::json!(payload_len));
                fields.insert("error".into(), serde_json::json!(error.to_string()));
                crate::logging::create_logger("cursor").warn("live_frame_decode", Some(fields));
            }
            return true;
        }
    };

    if let Some(checkpoint) = message.conversation_checkpoint_update {
        if !checkpoint.is_empty() {
            *latest_checkpoint = Some(checkpoint);
        }
        return true;
    }

    if let Some(kv) = message.kv_server_message {
        match encode_kv_reply(&kv, kv_blobs) {
            Ok(Some(reply)) => {
                if !send_frame_or_fail(
                    outbound,
                    sink,
                    terminal_error,
                    reply,
                    "KV reply",
                    frame_session_id,
                )
                .await
                {
                    return false;
                }
            }
            Ok(None) => {}
            Err(error) => {
                report_terminal_error(sink, terminal_error, error.to_string()).await;
                return false;
            }
        }
        return true;
    }

    if let Some(query) = message.interaction_query {
        if let Some(ask) = query.ask_question_interaction_query.as_ref()
            && let Some(emit_name) = advertised_ask_user_question(allowed_tool_names)
        {
            if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                return false;
            }
            pending.queue(
                ask_user_question_pending_exec(query.id, ask, emit_name),
                Duration::ZERO,
            );
            *last_progress = Instant::now();
            *useful = true;
            // Do not auto-reject: expose ClientOnly AskUserQuestion and tear
            // down like Workflow. Cursor AskQuestionResult has no answer tags
            // in proto.rs, so the Claude tool_result path is ClientOnly.
            // The live driver must first drain the authoritative END frame:
            // it can carry a trailing error that invalidates this tool call.
            if turn_ctx.is_some() {
                return true;
            }
            return expose_collected_tools(pending, pending_shared, sink).await;
        }
        if let Some(exec) = hosted_query_client_only_exec(&query, allowed_tool_names) {
            if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                return false;
            }
            if !pending_has_client_only(pending, &exec) {
                pending.queue(exec, Duration::ZERO);
            }
            *last_progress = Instant::now();
            *useful = true;
            // Do not auto-approve Cursor hosted search/fetch/plan. Expose
            // immediately: Cursor blocks on InteractionResponse.
            return expose_collected_tools(pending, pending_shared, sink).await;
        }
        match encode_interaction_auto_response(&query) {
            Ok(Some(reply)) => {
                if !send_frame_or_fail(
                    outbound,
                    sink,
                    terminal_error,
                    reply,
                    "interaction reply",
                    frame_session_id,
                )
                .await
                {
                    return false;
                }
            }
            Ok(None) => {}
            Err(error) => {
                report_terminal_error(sink, terminal_error, error.to_string()).await;
                return false;
            }
        }
        return true;
    }

    if let Some(exec) = message.exec_server_message {
        if exec.request_context_args.is_some() {
            let context = turn_ctx
                .as_ref()
                .map(|ctx| ctx.request_context)
                .cloned()
                .unwrap_or_else(RequestContext::default);
            match encode_request_context_reply(&exec, &context) {
                Ok(reply) => {
                    if !send_frame_or_fail(
                        outbound,
                        sink,
                        terminal_error,
                        reply,
                        "request_context reply",
                        frame_session_id,
                    )
                    .await
                    {
                        return false;
                    }
                }
                Err(error) => {
                    report_terminal_error(sink, terminal_error, error.to_string()).await;
                    return false;
                }
            }
            return true;
        }

        let Some(mut native) = PendingCursorExec::from_server(&exec) else {
            logical_tools_waiting.resolve_server_exec_hint(&exec);
            // Soft-fail: PiWriteExec / ApplyAgentDiff / unknown tags are not
            // decoded — throw instead of inventing a Claude Write.
            if let Ok(frames) = encode_control_throw(
                exec.id,
                "Unsupported Cursor exec tool (mapped: shell/write/delete/grep/read/ls; not PiWrite/ApplyAgentDiff)".into(),
            ) {
                for frame in frames {
                    if !send_frame_or_fail(
                        outbound,
                        sink,
                        terminal_error,
                        frame,
                        "exec throw",
                        frame_session_id,
                    )
                    .await
                    {
                        return false;
                    }
                }
                *useful = true;
                *last_progress = Instant::now();
            }
            return true;
        };
        let Some(emit_name) = resolve_advertised_name(&native.claude_name, allowed_tool_names)
        else {
            logical_tools_waiting.resolve_exec(&native);
            if let Ok(frames) = encode_control_throw(
                exec.id,
                format!("Tool {} is not advertised", native.claude_name),
            ) {
                for frame in frames {
                    if !send_frame_or_fail(
                        outbound,
                        sink,
                        terminal_error,
                        frame,
                        "exec throw",
                        frame_session_id,
                    )
                    .await
                    {
                        return false;
                    }
                }
                *useful = true;
                *last_progress = Instant::now();
            }
            return true;
        };
        native.claude_input = adapt_tool_input_for_client(&emit_name, native.claude_input);
        native.claude_name = emit_name;
        logical_tools_waiting.resolve_exec(&native);
        pending.queue(native, tool_batch_quiet);
        *useful = true;
        *last_progress = Instant::now();
        return true;
    }

    if let Some(update) = message.interaction_update {
        return process_interaction_update(
            update,
            outbound,
            sink,
            deferred,
            pending,
            pending_shared,
            kv_blobs,
            latest_checkpoint,
            terminal_error,
            allowed_tool_names,
            saw_text,
            useful,
            logical_tools_waiting,
            last_progress,
            xml_parser,
            &mut turn_ctx,
            0,
        )
        .await;
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn process_interaction_update(
    update: InteractionUpdate,
    outbound: &ClientOutbound,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    pending: &mut PendingExecState,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    kv_blobs: &mut HashMap<Vec<u8>, Vec<u8>>,
    latest_checkpoint: &mut Option<Vec<u8>>,
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    allowed_tool_names: Option<&BTreeSet<String>>,
    saw_text: &mut bool,
    useful: &mut bool,
    logical_tools_waiting: &mut LogicalToolTracker,
    last_progress: &mut Instant,
    xml_parser: &mut CursorToolUseXmlParser,
    turn_ctx: &mut Option<&mut LiveTurnCtx<'_>>,
    task_nest_depth: u8,
) -> bool {
    let defer_client_only_exposure = turn_ctx.is_some();
    if let Some(partial) = update.partial_tool_call {
        logical_tools_waiting.remember_partial_args(
            &partial.call_id,
            &partial.model_call_id,
            &partial.args_text_delta,
        );
        *last_progress = Instant::now();
        *useful = true;
    }
    if let Some(delta) = update.tool_call_delta {
        if std::env::var("CCP_CURSOR_DEBUG").is_ok() {
            eprintln!(
                "[ccp-cursor] tool_call_delta call_id={} nested_task={}",
                delta.call_id,
                delta.nested_task_update().is_some()
            );
        }
        *last_progress = Instant::now();
        if task_nest_depth < MAX_TASK_DELTA_NEST
            && let Some(nested) = delta.into_nested_task_update()
        {
            *useful = true;
            if !Box::pin(process_interaction_update(
                *nested,
                outbound,
                sink,
                deferred,
                pending,
                pending_shared,
                kv_blobs,
                latest_checkpoint,
                terminal_error,
                allowed_tool_names,
                saw_text,
                useful,
                logical_tools_waiting,
                last_progress,
                xml_parser,
                turn_ctx,
                task_nest_depth + 1,
            ))
            .await
            {
                return false;
            }
        }
    }
    if let Some(started) = update.tool_call_started {
        // Claude-local tools advertised via RunRequest.mcp_tools arrive as
        // MCP tool_call_started (not ExecServerMessage). Expose immediately
        // so Claude Code can fulfill Workflow/Skill locally.
        if let Some(mut mapped) = map_tool_call_started(&started) {
            let streamed = logical_tools_waiting
                .partial_args_for(&started.call_id, &started.model_call_id)
                .map(str::to_owned);
            if let Some(args_text) = streamed.as_deref() {
                merge_partial_args_json(&mut mapped, args_text);
            }
            let provider = mcp_provider_identifier(&started);
            if let Some(emit_name) =
                client_only_anthropic_name(&mapped.name, provider, allowed_tool_names)
            {
                if std::env::var("CCP_CURSOR_DEBUG").is_ok() {
                    eprintln!(
                        "[ccp-cursor] mcp tool_call_started → ClientOnly {} (wire={})",
                        emit_name, mapped.name
                    );
                }
                if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                    return false;
                }
                let mut exec = mcp_client_only_pending_exec(&mapped);
                exec.claude_name = emit_name.clone();
                exec.claude_input = adapt_client_tool_input(&emit_name, mapped.input.clone());
                pending.queue(exec, Duration::ZERO);
                *useful = true;
                *last_progress = Instant::now();
                if !defer_client_only_exposure {
                    return expose_collected_tools(pending, pending_shared, sink).await;
                }
                if is_grok_build_subagent_lifecycle_tool(&emit_name) {
                    return true;
                }
            }
            if mapped.name == "Task"
                && task_nest_depth == 0
                && let Some(emit_name) = advertised_client_task_name(allowed_tool_names)
            {
                // Cursor native Task (tag 19) may start a server-side child
                // before we observe this frame. Expose immediately and drop
                // the BiDi segment; do not wait for turn_ended.
                if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                    return false;
                }
                let mut exec = mcp_client_only_pending_exec(&mapped);
                exec.claude_name = emit_name.clone();
                exec.claude_input = if emit_name == "spawn_subagent" {
                    adapt_native_task_to_spawn_subagent(mapped.input.clone())
                } else {
                    adapt_client_tool_input(&emit_name, mapped.input.clone())
                };
                pending.queue(exec, Duration::ZERO);
                *useful = true;
                *last_progress = Instant::now();
                return expose_collected_tools(pending, pending_shared, sink).await;
            }
            if mapped.name == "AskUserQuestion"
                && let Some(emit_name) = advertised_ask_user_question(allowed_tool_names)
            {
                if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                    return false;
                }
                let mut exec = mcp_client_only_pending_exec(&mapped);
                exec.claude_name = emit_name;
                exec.claude_input = ask_user_question_input_from_mapped(&mapped.input);
                pending.queue(exec, Duration::ZERO);
                *useful = true;
                *last_progress = Instant::now();
                if !defer_client_only_exposure {
                    return expose_collected_tools(pending, pending_shared, sink).await;
                }
            }
            if mapped.name == "Glob"
                && let Some(emit_name) = resolve_glob_client_name(
                    &mapped.input,
                    advertised_hosted_client_name("Glob", allowed_tool_names),
                    advertised_hosted_client_name("Bash", allowed_tool_names),
                )
            {
                // Official ExecServerMessage has no glob_args (0xlane agent_v1).
                // tool_call_started is the only signal — expose as ClientOnly.
                if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                    return false;
                }
                let mut exec = mcp_client_only_pending_exec(&mapped);
                exec.claude_name = emit_name.clone();
                exec.claude_input = adapt_client_tool_input(&emit_name, mapped.input.clone());
                pending.queue(exec, Duration::ZERO);
                *useful = true;
                *last_progress = Instant::now();
                if !defer_client_only_exposure {
                    return expose_collected_tools(pending, pending_shared, sink).await;
                }
            }
            if matches!(mapped.name.as_str(), "TodoWrite" | "TodoRead")
                && let Some(emit_name) =
                    advertised_hosted_client_name(&mapped.name, allowed_tool_names)
            {
                if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                    return false;
                }
                let mut exec = mcp_client_only_pending_exec(&mapped);
                exec.claude_name = emit_name.clone();
                exec.claude_input = adapt_client_tool_input(&emit_name, mapped.input.clone());
                pending.queue(exec, Duration::ZERO);
                *useful = true;
                *last_progress = Instant::now();
                if !defer_client_only_exposure {
                    return expose_collected_tools(pending, pending_shared, sink).await;
                }
            }
            if matches!(
                mapped.name.as_str(),
                "WebSearch" | "WebFetch" | "CreatePlan"
            ) && let Some(emit_name) =
                advertised_hosted_client_name(&mapped.name, allowed_tool_names)
            {
                // Cursor hosted search/fetch/plan stay on Cursor unless we
                // steal them here. grok-build has web_search / web_fetch /
                // enter_plan_mode; Claude Code has WebSearch / WebFetch /
                // CreatePlan. Expose immediately like Task: Cursor then
                // sends InteractionQuery and would otherwise block until we
                // auto-approve the hosted path.
                if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                    return false;
                }
                let mut exec = mcp_client_only_pending_exec(&mapped);
                exec.claude_name = emit_name.clone();
                exec.claude_input = adapt_client_tool_input(&emit_name, mapped.input.clone());
                pending.queue(exec, Duration::ZERO);
                *useful = true;
                *last_progress = Instant::now();
                return expose_collected_tools(pending, pending_shared, sink).await;
            }
        }
        // Native UI transcript only. Execution is driven by ExecServerMessage,
        // otherwise tool_call_started + exec duplicates.
        logical_tools_waiting.started(&started.call_id, &started.model_call_id);
        *useful = true;
        *last_progress = Instant::now();
    }
    if let Some(completed) = update.tool_call_completed {
        logical_tools_waiting.completed(&completed.call_id, &completed.model_call_id);
        *last_progress = Instant::now();
    }
    if let Some(thinking) = update.thinking_delta
        && !thinking.text.is_empty()
    {
        *useful = true;
        *last_progress = Instant::now();
        if !emit_live_delta(
            sink,
            deferred,
            CursorStreamEvent::ThinkingDelta {
                text: thinking.text,
            },
            turn_ctx.as_deref_mut(),
        )
        .await
        {
            return false;
        }
    }
    if update.heartbeat.is_some() {
        // Server keep-alive during quiet thinking — do not refresh idle timers.
        record_server_heartbeat(last_progress);
    }
    if let Some(text) = update.text_delta
        && !text.text.is_empty()
    {
        *useful = true;
        *last_progress = Instant::now();
        let recovered = xml_parser.push(&text.text);
        for evt in recovered {
            match evt {
                RecoveredCursorEvent::Text(t) if !t.is_empty() => {
                    *saw_text = true;
                    if !emit_live_delta(
                        sink,
                        deferred,
                        CursorStreamEvent::TextDelta { text: t },
                        turn_ctx.as_deref_mut(),
                    )
                    .await
                    {
                        return false;
                    }
                }
                RecoveredCursorEvent::Text(_) => {}
                RecoveredCursorEvent::ToolUse(tool_use) => {
                    // Claude-local tools (Workflow/Skill/…) appear as XML in
                    // Fable text when advertised via `<tools>`. Native
                    // Read/Bash still come through ExecServerMessage.
                    if let Some(emit_name) =
                        client_only_anthropic_name(&tool_use.name, "", allowed_tool_names)
                    {
                        let mut exec = client_only_pending_exec(&tool_use);
                        exec.claude_name = emit_name.clone();
                        exec.claude_input = adapt_client_tool_input(&emit_name, exec.claude_input);
                        // Direct frame tests expose immediately. The live
                        // driver defers until turn_ended/END/EOF so a trailing
                        // END error cannot be bypassed by closing the sink.
                        pending.queue(exec, Duration::ZERO);
                        if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                            return false;
                        }
                        if !defer_client_only_exposure
                            && !expose_collected_tools(pending, pending_shared, sink).await
                        {
                            return false;
                        }
                    }
                }
            }
        }
    }
    if let Some(tokens) = update.token_delta
        && tokens.tokens > 0
    {
        if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
            return false;
        }
        if !emit_cursor_or_defer(
            sink,
            deferred,
            CursorStreamEvent::OutputTokenDelta {
                tokens: tokens.tokens as u64,
            },
        )
        .await
        {
            return false;
        }
    }
    if let Some(turn) = update.turn_ended {
        if task_nest_depth > 0 {
            // Subagent finished; the parent Task call is still in flight.
            *last_progress = Instant::now();
            return true;
        }
        // Flush trailing `<tool_use>` still in the XML buffer — Fable often
        // closes the turn in the same InteractionUpdate as Workflow XML.
        if !flush_xml_tool_uses(
            xml_parser,
            pending,
            pending_shared,
            sink,
            deferred,
            allowed_tool_names,
            saw_text,
            useful,
            last_progress,
        )
        .await
        {
            return false;
        }
        if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
            return false;
        }
        if !pending.is_empty() {
            if pending.has_outstanding_native()
                && pending.all().any(|exec| {
                    matches!(exec.kind, super::exec_results::CursorExecKind::ClientOnly)
                })
            {
                match control_close_natives(pending, outbound).await {
                    Ok(true) => {}
                    Ok(false) => return false,
                    Err(err) => {
                        report_terminal_error(sink, terminal_error, err.to_string()).await;
                        return false;
                    }
                }
            }
            if pending.all_client_only() {
                // Workflow/Skill: expose Anthropic tool_use and end BiDi
                // unless native execs are still collecting (mixed keep-alive).
                return expose_collected_tools(pending, pending_shared, sink).await;
            }
            report_terminal_error(
                sink,
                terminal_error,
                "Cursor turn ended with pending native tools".into(),
            )
            .await;
            return false;
        }
        // Heartbeat-only "thinking" with no text/tools must not become a
        // successful Anthropic turn. Retry before the client can accept an
        // empty completion as the model's final answer.
        if !recover_empty_turn_if_needed(
            saw_text,
            useful,
            sink,
            pending,
            pending_shared,
            terminal_error,
            allowed_tool_names,
            turn_ctx.as_ref().map(|ctx| ctx.user_prompt).unwrap_or(""),
            turn_ctx.as_ref().map(|ctx| ctx.session_id),
            latest_checkpoint,
            kv_blobs,
            "turn_ended",
        )
        .await
        {
            return false;
        }
        if !emit_cursor_or_defer(
            sink,
            deferred,
            CursorStreamEvent::Usage {
                input_tokens: turn.input_tokens.unwrap_or(0),
                // Fable thinking often lands in reasoning_tokens while
                // output_tokens stays 0 — Claude Code's Out meter needs both.
                output_tokens: turn
                    .output_tokens
                    .unwrap_or(0)
                    .saturating_add(turn.reasoning_tokens.unwrap_or(0))
                    .max(if *saw_text { 1 } else { 0 }),
                cache_read_tokens: turn.cache_read_tokens.unwrap_or(0),
                cache_write_tokens: turn.cache_write_tokens.unwrap_or(0),
            },
        )
        .await
        {
            return false;
        }
        let _ = emit_cursor_or_defer(sink, deferred, CursorStreamEvent::End).await;
        return false;
    }
    true
}

/// Recover an empty Cursor turn: preserve a checkpoint that already consumed
/// native tool results, emit a real Anthropic `Workflow` tool_use when that
/// tool was advertised, otherwise reset and fail with a bounded retry.
///
/// Used from `turn_ended`, clean Connect `FLAG_END`, and exhausted EOF — all
/// three previously could produce silent Anthropic Out:0 completions.
#[allow(clippy::too_many_arguments)]
async fn recover_empty_turn_if_needed(
    saw_text: &mut bool,
    useful: &mut bool,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    pending: &mut PendingExecState,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    allowed_tool_names: Option<&BTreeSet<String>>,
    user_prompt: &str,
    session_id: Option<&str>,
    latest_checkpoint: &mut Option<Vec<u8>>,
    kv_blobs: &mut HashMap<Vec<u8>, Vec<u8>>,
    reason: &str,
) -> bool {
    if *saw_text || sink.is_none() {
        return true;
    }
    if user_prompt == EMPTY_TURN_CHECKPOINT_CONTINUE_PROMPT && latest_checkpoint.is_some() {
        let mut fields = serde_json::Map::new();
        fields.insert("reason".into(), serde_json::json!(reason));
        fields.insert("recovery".into(), serde_json::json!("checkpoint_continue"));
        crate::logging::create_logger("cursor").warn("empty_turn_retry", Some(fields));
        report_terminal_error(
            sink,
            terminal_error,
            format!("{EMPTY_TURN_RETRY_NOTE} ({EMPTY_TURN_CHECKPOINT_RETRY_NOTE})"),
        )
        .await;
        return false;
    }
    // Claude Code `Workflow` only. grok-cli advertises lowercase `workflow`
    // (Rhai launcher, `{name, agent_budget}`). Synthesizing Claude
    // `Workflow`/`deep-research` becomes `Tool not found: Workflow` and the
    // model reports "workflow 被桥接拦了".
    if advertised_claude_code_workflow(allowed_tool_names) {
        let (name, args) = synthetic_workflow_from_prompt(user_prompt);
        pending.queue(
            synthetic_workflow_pending_exec(&name, &args),
            Duration::ZERO,
        );
        *saw_text = true;
        *useful = true;
        let mut fields = serde_json::Map::new();
        fields.insert("reason".into(), serde_json::json!(reason));
        fields.insert("workflow_name".into(), serde_json::json!(name));
        crate::logging::create_logger("cursor").info("empty_turn_workflow", Some(fields));
        return expose_collected_tools(pending, pending_shared, sink).await;
    }
    // Never turn a transport/upstream anomaly into assistant-authored text:
    // clients treat any TextDelta + End as success and will not retry.
    {
        let mut fields = serde_json::Map::new();
        fields.insert("reason".into(), serde_json::json!(reason));
        fields.insert(
            "claude_local_tools".into(),
            serde_json::json!(
                allowed_tool_names
                    .is_some_and(|set| { set.iter().any(|n| is_claude_local_tool_name(n)) })
            ),
        );
        crate::logging::create_logger("cursor").warn("empty_turn_retry", Some(fields));
    }
    if std::env::var_os("CCP_CURSOR_DEBUG").is_some() {
        eprintln!("[ccp-cursor] empty_turn_retry reason={reason}");
    }
    let message = if let Some(session_id) = session_id {
        super::conversation::reset(session_id);
        *latest_checkpoint = None;
        kv_blobs.clear();
        crate::logging::create_logger("cursor").warn(
            "live_conversation_reset",
            Some(serde_json::Map::from_iter([(
                "reason".into(),
                serde_json::json!("empty_turn"),
            )])),
        );
        format!("{EMPTY_TURN_RETRY_NOTE} ({CONVERSATION_RESET_RETRY_NOTE})")
    } else {
        EMPTY_TURN_RETRY_NOTE.to_string()
    };
    report_terminal_error(sink, terminal_error, message).await;
    false
}

async fn send_frame_or_fail(
    outbound: &ClientOutbound,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    frame: Bytes,
    what: &str,
    session_id: &str,
) -> bool {
    match outbound.send_connect_frame(frame).await {
        Ok(()) => true,
        Err(error) => {
            let error = annotate_live_cursor_error(session_id, error);
            report_terminal_error(
                sink,
                terminal_error,
                format!("Cursor {what} send failed: {error}"),
            )
            .await;
            false
        }
    }
}

async fn report_terminal_error(
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    message: String,
) {
    // Always stash the failure. Previously we only stored it when `sink` was
    // None (between Anthropic segments). Idle/timeouts with a live SSE sink
    // therefore left the registry entry looking "still generating" -> cascade
    // of 409s for concurrent same-session POSTs.
    store_terminal_error(terminal_error, &message);
    // Terminal reporting must never block teardown behind a full downstream
    // channel. If the error cannot be queued, dropping the sender makes the SSE
    // layer emit its explicit "ended without turn_ended" error.
    if let Some(tx) = sink.take() {
        let _ = tx.try_send(Err(message));
    }
}

fn store_terminal_error(terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>, message: &str) {
    let mut slot = terminal_error.lock().unwrap_or_else(|e| e.into_inner());
    if slot.is_none() {
        *slot = Some(TerminalOutcome {
            message: message.to_string(),
            created_at: Instant::now(),
        });
    }
}

fn mark_live_cancelled(
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    completion_ambiguous: bool,
) {
    let message = if completion_ambiguous {
        "Cursor live cancellation interrupted an operation whose completion is unresolved; acceptance is ambiguous"
    } else {
        "Cursor live run cancelled"
    }
    .to_string();
    store_terminal_error(terminal_error, &message);
    if let Some(tx) = sink.take() {
        let _ = tx.try_send(Err(message));
    }
}

async fn expose_collected_tools(
    pending: &mut PendingExecState,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
) -> bool {
    let exposed = pending.expose();
    publish_exposed_tools(exposed, pending_shared, sink).await
}

async fn expose_collected_native_tools(
    pending: &mut PendingExecState,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
) -> bool {
    let exposed = pending.expose_native();
    publish_exposed_tools(exposed, pending_shared, sink).await
}

async fn publish_exposed_tools(
    exposed: Vec<PendingCursorExec>,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
) -> bool {
    if exposed.is_empty() {
        return true;
    }
    let client_only = exposed
        .iter()
        .any(|exec| matches!(exec.kind, CursorExecKind::ClientOnly));
    let tools = exposed
        .iter()
        .map(|exec| LiveNativeTool {
            tool_use_id: exec.tool_use_id.clone(),
            name: exec.claude_name.clone(),
            input: exec.claude_input.clone(),
        })
        .collect();

    *pending_shared.lock().unwrap_or_else(|e| e.into_inner()) = exposed;
    if !send_live_event(sink, Ok(LiveRunEvent::NativeToolBatch(tools))).await {
        return false;
    }
    // Closing this sender ends exactly one downstream Anthropic HTTP segment.
    *sink = None;
    // Client-only tools (Workflow/Skill/AskUserQuestion) are fulfilled by Claude
    // Code locally. End this BiDi run so the next Anthropic turn starts fresh
    // with tool_result history — including mixed batches that still have native
    // Read/Bash collecting (those execs are control_closed on driver teardown).
    if client_only {
        return false;
    }
    true
}

/// Drain buffered XML `<tool_use>` on turn/stream end and expose Claude-local
/// tools immediately when recovered.
#[allow(clippy::too_many_arguments)]
async fn flush_xml_tool_uses(
    xml_parser: &mut CursorToolUseXmlParser,
    pending: &mut PendingExecState,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    allowed_tool_names: Option<&BTreeSet<String>>,
    saw_text: &mut bool,
    useful: &mut bool,
    last_progress: &mut Instant,
) -> bool {
    let mut exposed_client_only = false;
    for evt in xml_parser.flush() {
        match evt {
            RecoveredCursorEvent::Text(t) if !t.is_empty() => {
                *saw_text = true;
                *useful = true;
                *last_progress = Instant::now();
                if !emit_cursor_or_defer(sink, deferred, CursorStreamEvent::TextDelta { text: t })
                    .await
                {
                    return false;
                }
            }
            RecoveredCursorEvent::Text(_) => {}
            RecoveredCursorEvent::ToolUse(tool_use) => {
                if let Some(emit_name) =
                    client_only_anthropic_name(&tool_use.name, "", allowed_tool_names)
                {
                    let mut exec = client_only_pending_exec(&tool_use);
                    exec.claude_name = emit_name.clone();
                    exec.claude_input = adapt_client_tool_input(&emit_name, exec.claude_input);
                    pending.queue(exec, Duration::ZERO);
                    exposed_client_only = true;
                }
            }
        }
    }
    if exposed_client_only {
        return expose_collected_tools(pending, pending_shared, sink).await;
    }
    true
}

fn client_only_pending_exec(
    tool_use: &crate::providers::cursor::tool_use_xml::RecoveredCursorToolUse,
) -> PendingCursorExec {
    // Synthetic exec id: must be unique within the pending set; Cursor never
    // assigned a real exec frame for these XML tool_uses.
    let id = {
        let mut hash: u32 = 0;
        for b in tool_use.id.as_bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(u32::from(*b));
        }
        hash.max(1)
    };
    PendingCursorExec {
        id,
        exec_id: Some(format!("client_only_{}", tool_use.id)),
        tool_use_id: tool_use.id.clone(),
        claude_name: tool_use.name.clone(),
        claude_input: serde_json::Value::Object(tool_use.input.clone()),
        kind: CursorExecKind::ClientOnly,
    }
}

fn mcp_provider_identifier(started: &super::proto::ToolCallStarted) -> &str {
    started
        .tool_call
        .as_ref()
        .and_then(|tc| tc.mcp_tool_call.as_ref())
        .and_then(|call| call.args.as_ref())
        .map(|args| args.provider_identifier.as_str())
        .unwrap_or("")
}

/// Decide whether an MCP/XML tool should be ClientOnly, and which Anthropic
/// `tool_use.name` to emit. Cursor may send `claude-local/Workflow` while Claude
/// Code advertised `Workflow`.
/// grok-build: exact `spawn_subagent`. Claude Code: `Agent`, then legacy `Task`.
/// Lowercase `task` is an alias only — never emit it (grok dispatch is exact).
fn advertised_client_task_name(allowed: Option<&BTreeSet<String>>) -> Option<String> {
    let allowed = allowed?;
    if allowed.contains("spawn_subagent") {
        return Some("spawn_subagent".to_string());
    }
    if allowed.contains("Agent") {
        return Some("Agent".to_string());
    }
    if allowed.contains("Task") {
        return Some("Task".to_string());
    }
    None
}

fn lifecycle_client_only_name(
    mapped_name: &str,
    provider_identifier: &str,
    allowed: Option<&BTreeSet<String>>,
) -> Option<String> {
    let allowed = allowed?;
    let exact = normalize_grok_build_lifecycle_name(mapped_name)?;
    if !allowed.contains(exact) {
        return None;
    }
    if !provider_identifier.is_empty() && provider_identifier != CLAUDE_LOCAL_MCP_PROVIDER {
        return None;
    }
    Some(exact.to_string())
}

fn client_only_anthropic_name(
    mapped_name: &str,
    provider_identifier: &str,
    allowed: Option<&BTreeSet<String>>,
) -> Option<String> {
    let stripped = strip_mcp_provider_prefix(mapped_name);
    if stripped.is_empty() {
        return None;
    }
    if normalize_grok_build_lifecycle_name(mapped_name).is_some()
        || normalize_grok_build_lifecycle_name(stripped).is_some()
    {
        return lifecycle_client_only_name(mapped_name, provider_identifier, allowed);
    }
    // Cursor native Task is translated only by advertised_client_task_name.
    // Claude Task / internal aliases must not become ClientOnly via this path.
    if matches!(mapped_name, "Task" | "task" | "Agent")
        || matches!(stripped, "Task" | "task" | "Agent")
    {
        return None;
    }
    let local = is_claude_local_tool_name(stripped)
        || (stripped != mapped_name && is_claude_local_tool_name(mapped_name));
    if !local {
        return None;
    }

    // Missing/empty tool lists must not invent Workflow/web_search/etc.
    // XML recovery and MCP both go through here.
    let set = allowed.filter(|set| !set.is_empty())?;
    // Prefix stripping is enough to match `claude-local/Workflow` to
    // advertised `Workflow`. Case folding is required for grok-cli
    // `workflow` vs Fable `Workflow`. Emit the exact advertised spelling
    // — grok dispatch is exact and rejects `Tool not found: Workflow`.
    set.get(stripped)
        .or_else(|| set.get(mapped_name))
        .cloned()
        .or_else(|| advertised_workflow_or_skill_name(set, stripped, provider_identifier))
}

fn advertised_workflow_or_skill_name(
    set: &BTreeSet<String>,
    name: &str,
    provider_identifier: &str,
) -> Option<String> {
    if !provider_identifier.is_empty() && provider_identifier != CLAUDE_LOCAL_MCP_PROVIDER {
        return None;
    }
    if !matches!(name, "Workflow" | "workflow" | "Skill" | "skill") {
        return None;
    }
    set.iter()
        .find(|advertised| strip_mcp_provider_prefix(advertised).eq_ignore_ascii_case(name))
        .cloned()
}

fn mcp_client_only_pending_exec(
    mapped: &super::native_tools::MappedClaudeTool,
) -> PendingCursorExec {
    let id = {
        let mut hash: u32 = 0;
        for b in mapped.tool_use_id.as_bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(u32::from(*b));
        }
        hash.max(1)
    };
    PendingCursorExec {
        id,
        exec_id: Some(format!("mcp_{}", mapped.tool_use_id)),
        tool_use_id: mapped.tool_use_id.clone(),
        claude_name: mapped.name.clone(),
        claude_input: mapped.input.clone(),
        kind: CursorExecKind::ClientOnly,
    }
}

fn advertised_ask_user_question(allowed: Option<&BTreeSet<String>>) -> Option<String> {
    advertised_hosted_client_name("AskUserQuestion", allowed)
}

/// Steal a Cursor hosted/native tool only when the client actually advertised
/// an equivalent. `allowed=None` means no tool list — do not invent WebSearch
/// / WebFetch / CreatePlan / AskUserQuestion.
fn advertised_hosted_client_name(
    mapped_name: &str,
    allowed: Option<&BTreeSet<String>>,
) -> Option<String> {
    let allowed = allowed.filter(|set| !set.is_empty())?;
    resolve_advertised_name(mapped_name, Some(allowed))
}

fn hosted_query_client_only_exec(
    query: &InteractionQuery,
    allowed: Option<&BTreeSet<String>>,
) -> Option<PendingCursorExec> {
    if let Some(search) = query.web_search_request_query.as_ref()
        && let Some(emit_name) = advertised_hosted_client_name("WebSearch", allowed)
    {
        let args = search.args.as_ref();
        let tool_use_id = args
            .map(|a| a.tool_call_id.as_str())
            .filter(|id| !id.is_empty())
            .unwrap_or("web_search_query")
            .to_string();
        let query_text = args.map(|a| a.search_term.as_str()).unwrap_or("");
        return Some(query_client_only_pending_exec(
            query.id,
            tool_use_id,
            emit_name,
            serde_json::json!({ "query": query_text }),
        ));
    }
    if let Some(fetch) = query.web_fetch_request_query.as_ref()
        && let Some(emit_name) = advertised_hosted_client_name("WebFetch", allowed)
    {
        let args = fetch.args.as_ref();
        let tool_use_id = args
            .map(|a| a.tool_call_id.as_str())
            .filter(|id| !id.is_empty())
            .unwrap_or("web_fetch_query")
            .to_string();
        let url = args.map(|a| a.url.as_str()).unwrap_or("");
        return Some(query_client_only_pending_exec(
            query.id,
            tool_use_id,
            emit_name,
            serde_json::json!({ "url": url }),
        ));
    }
    if let Some(plan) = query.create_plan_request_query.as_ref()
        && let Some(emit_name) = advertised_hosted_client_name("CreatePlan", allowed)
    {
        let tool_use_id = if plan.tool_call_id.is_empty() {
            "create_plan_query".to_string()
        } else {
            plan.tool_call_id.clone()
        };
        let input = plan.args.as_ref().map_or_else(
            || serde_json::json!({}),
            |args| {
                serde_json::json!({
                    "name": args.name,
                    "overview": args.overview,
                    "plan": args.plan,
                    "is_project": args.is_project,
                })
            },
        );
        return Some(query_client_only_pending_exec(
            query.id,
            tool_use_id,
            emit_name,
            input,
        ));
    }
    None
}

fn query_client_only_pending_exec(
    _query_id: u32,
    tool_use_id: String,
    emit_name: String,
    input: serde_json::Value,
) -> PendingCursorExec {
    // Same id / exec_id as tool_call_started so a later InteractionQuery
    // does not emit a second tool_use for the same call.
    let mapped = super::native_tools::MappedClaudeTool {
        tool_use_id,
        name: emit_name.clone(),
        input: input.clone(),
    };
    let mut exec = mcp_client_only_pending_exec(&mapped);
    exec.claude_name = emit_name.clone();
    exec.claude_input = adapt_client_tool_input(&emit_name, input);
    exec
}

fn pending_has_client_only(pending: &PendingExecState, exec: &PendingCursorExec) -> bool {
    pending.all().any(|queued| {
        queued.claude_name == exec.claude_name
            || queued.tool_use_id == exec.tool_use_id
            || queued
                .tool_use_id
                .starts_with(&format!("{}__cursor_run_", exec.tool_use_id))
            || queued.exec_id == exec.exec_id
    })
}

fn ask_user_question_pending_exec(
    query_id: u32,
    ask: &AskQuestionInteractionQuery,
    emit_name: String,
) -> PendingCursorExec {
    let tool_use_id = if ask.tool_call_id.is_empty() {
        format!("ask_question_{query_id}")
    } else {
        ask.tool_call_id.clone()
    };
    PendingCursorExec {
        id: query_id.max(1),
        exec_id: Some(format!("ask_{tool_use_id}")),
        tool_use_id,
        claude_name: emit_name,
        claude_input: ask_user_question_input(ask.args.as_ref()),
        kind: CursorExecKind::ClientOnly,
    }
}

fn ask_user_question_input(args: Option<&AskQuestionArgs>) -> serde_json::Value {
    let title = args.map(|a| a.title.as_str()).unwrap_or("");
    let items: Vec<(String, Option<Vec<serde_json::Value>>)> = args
        .map(|a| {
            a.questions
                .iter()
                .map(|q| (q.prompt.clone(), None))
                .collect()
        })
        .unwrap_or_default();
    ask_user_question_input_from_parts(title, &items)
}

fn ask_user_question_input_from_mapped(input: &serde_json::Value) -> serde_json::Value {
    let title = input.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let items: Vec<(String, Option<Vec<serde_json::Value>>)> = input
        .get("questions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|q| {
                    let prompt = q
                        .get("question")
                        .or_else(|| q.get("prompt"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let options = q.get("options").and_then(|v| v.as_array()).cloned();
                    (prompt, options)
                })
                .collect()
        })
        .unwrap_or_default();
    ask_user_question_input_from_parts(title, &items)
}

/// Map Cursor `AskQuestionArgs` onto Claude Code 2.1.193 `AskUserQuestion`.
///
/// proto.rs `AskQuestionItem` only has `id` + `prompt` — no `options` /
/// `allow_multiple`. Synthesize the required 2–4 options when missing.
fn ask_user_question_input_from_parts(
    title: &str,
    items: &[(String, Option<Vec<serde_json::Value>>)],
) -> serde_json::Value {
    let header = truncate_ask_header(title);
    let mut questions = Vec::new();
    for (prompt, options) in items.iter().take(4) {
        let mut question = prompt.trim().to_string();
        if question.is_empty() {
            question = title.trim().to_string();
        }
        if question.is_empty() {
            question = "Continue?".to_string();
        }
        if !question.ends_with('?') {
            question.push('?');
        }
        let header = if header.is_empty() {
            truncate_ask_header(&question)
        } else {
            header.clone()
        };
        let options = match options {
            Some(opts) if (2..=4).contains(&opts.len()) => opts.clone(),
            _ => default_ask_options(),
        };
        questions.push(serde_json::json!({
            "question": question,
            "header": header,
            "options": options,
        }));
    }
    if questions.is_empty() {
        let mut question = title.trim().to_string();
        if question.is_empty() {
            question = "Continue?".to_string();
        } else if !question.ends_with('?') {
            question.push('?');
        }
        questions.push(serde_json::json!({
            "question": question,
            "header": if header.is_empty() {
                truncate_ask_header(&question)
            } else {
                header
            },
            "options": default_ask_options(),
        }));
    }
    serde_json::json!({ "questions": questions })
}

fn default_ask_options() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "label": "Continue",
            "description": "Accept this option and continue",
        }),
        serde_json::json!({
            "label": "Skip",
            "description": "Skip this question",
        }),
    ]
}

fn truncate_ask_header(text: &str) -> String {
    text.trim()
        .chars()
        .take(ASK_USER_QUESTION_HEADER_MAX)
        .collect()
}

fn advertised_claude_code_workflow(allowed: Option<&BTreeSet<String>>) -> bool {
    allowed.is_some_and(|set| {
        set.iter()
            .any(|name| strip_mcp_provider_prefix(name) == "Workflow")
    })
}

fn synthetic_workflow_from_prompt(prompt: &str) -> (String, String) {
    if let Some(parsed) = parse_injected_workflow(prompt) {
        return parsed;
    }
    ("deep-research".into(), String::new())
}

/// Parse Claude Code injected slash text:
/// `Invoke: Workflow({ name: "deep-research", args: "..." })`
/// or `Run the "deep-research" workflow.`
fn parse_injected_workflow(prompt: &str) -> Option<(String, String)> {
    if let Some(rest) = find_ignore_ascii_case(prompt, "Invoke: Workflow(") {
        let name = jsonish_quoted_field(rest, "name")?;
        let args = jsonish_quoted_field(rest, "args").unwrap_or_default();
        return Some((name, args));
    }
    parse_run_the_workflow(prompt)
}

fn parse_run_the_workflow(prompt: &str) -> Option<(String, String)> {
    let lower = prompt.to_ascii_lowercase();
    let marker = "run the \"";
    let start = lower.find(marker)?;
    let name_start = start + marker.len();
    let name_end = lower[name_start..].find('"')?;
    let name = prompt[name_start..name_start + name_end].trim().to_string();
    if name.is_empty() {
        return None;
    }
    let after = lower[name_start + name_end + 1..].trim_start();
    if !after.starts_with("workflow") {
        return None;
    }
    Some((name, String::new()))
}

fn find_ignore_ascii_case<'a>(haystack: &'a str, needle: &str) -> Option<&'a str> {
    let hay = haystack.as_bytes();
    let needle_bytes = needle.as_bytes();
    if needle_bytes.is_empty() || hay.len() < needle_bytes.len() {
        return None;
    }
    for i in 0..=hay.len() - needle_bytes.len() {
        if hay[i..i + needle_bytes.len()].eq_ignore_ascii_case(needle_bytes) {
            return Some(&haystack[i + needle_bytes.len()..]);
        }
    }
    None
}

fn jsonish_quoted_field(source: &str, key: &str) -> Option<String> {
    let mut search = 0;
    while let Some(rel) = source[search..].find(key) {
        let abs = search + rel;
        let after_key = source[abs + key.len()..].trim_start();
        if let Some(rest) = after_key.strip_prefix(':') {
            let rest = rest.trim_start();
            if let Some(quote) = rest.chars().next().filter(|c| *c == '"' || *c == '\'') {
                let body = &rest[quote.len_utf8()..];
                if let Some(end) = body.find(quote) {
                    return Some(body[..end].to_string());
                }
            }
        }
        search = abs + key.len();
        if search >= source.len() {
            break;
        }
    }
    None
}

fn synthetic_workflow_pending_exec(name: &str, args: &str) -> PendingCursorExec {
    let tool_use_id = format!("empty_turn_workflow_{name}");
    let id = {
        let mut hash: u32 = 0;
        for b in tool_use_id.as_bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(u32::from(*b));
        }
        hash.max(1)
    };
    PendingCursorExec {
        id,
        exec_id: Some(format!("client_only_{tool_use_id}")),
        tool_use_id,
        claude_name: "Workflow".into(),
        claude_input: serde_json::json!({ "name": name, "args": args }),
        kind: CursorExecKind::ClientOnly,
    }
}

fn record_segment_progress(
    event: &LiveEventResult,
    saw_text: &mut bool,
    useful: &mut bool,
    last_progress: &mut Instant,
) {
    match event {
        Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) if !text.is_empty() => {
            *saw_text = true;
            *useful = true;
            *last_progress = Instant::now();
        }
        Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta { text })) if !text.is_empty() => {
            *useful = true;
            *last_progress = Instant::now();
        }
        Ok(LiveRunEvent::NativeToolBatch(tools)) if !tools.is_empty() => {
            *useful = true;
            *last_progress = Instant::now();
        }
        _ => {}
    }
}

async fn emit_cursor_or_defer(
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    event: CursorStreamEvent,
) -> bool {
    emit_or_defer(sink, deferred, Ok(LiveRunEvent::Cursor(event))).await
}

async fn emit_or_defer(
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    event: LiveEventResult,
) -> bool {
    if sink.is_some() {
        send_live_event(sink, event).await
    } else {
        deferred.push_back(event);
        true
    }
}

async fn send_live_event(
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    event: LiveEventResult,
) -> bool {
    let Some(tx) = sink.as_ref() else {
        return false;
    };
    // Text/thinking deltas: prefer try_send so a slow Claude Code consumer does
    // not stall Cursor BiDi heartbeats on the hot path. Never drop tokens —
    // yield once for the SSE unfold to drain, then await send.
    let is_delta = matches!(
        &event,
        Ok(LiveRunEvent::Cursor(
            CursorStreamEvent::TextDelta { .. } | CursorStreamEvent::ThinkingDelta { .. }
        ))
    );
    if is_delta {
        match tx.try_send(event) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(event)) => {
                tokio::task::yield_now().await;
                match tx.try_send(event) {
                    Ok(()) => true,
                    Err(mpsc::error::TrySendError::Full(event)) => {
                        send_live_event_bounded(tx, event).await
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    } else {
        send_live_event_bounded(tx, event).await
    }
}

async fn send_live_event_bounded(
    tx: &mpsc::Sender<LiveEventResult>,
    event: LiveEventResult,
) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_secs(env_u64("CCP_CURSOR_DOWNSTREAM_SEND_TIMEOUT_SECS", 5)),
            tx.send(event),
        )
        .await,
        Ok(Ok(()))
    )
}

fn apply_live_run_event(encoder: &mut CursorSseEncoder, event: LiveRunEvent) {
    match event {
        LiveRunEvent::Cursor(event) => encoder.push_event(&event),
        LiveRunEvent::NativeToolBatch(tools) => {
            let encoded: Vec<(String, String, String)> = tools
                .into_iter()
                .map(|tool| {
                    let input =
                        serde_json::to_string(&tool.input).unwrap_or_else(|_| "{}".to_string());
                    (tool.tool_use_id, tool.name, input)
                })
                .collect();
            encoder.emit_tool_batch(encoded.iter().map(|(tool_use_id, name, input)| {
                (tool_use_id.as_str(), name.as_str(), input.as_str())
            }));
        }
    }
}

fn resolve_advertised_name(
    mapped_name: &str,
    allowed: Option<&BTreeSet<String>>,
) -> Option<String> {
    let allowed = allowed.filter(|set| !set.is_empty())?;
    if allowed.contains(mapped_name) {
        return Some(mapped_name.to_string());
    }
    // Never fall back to Edit: Claude Edit requires old_string/new_string,
    // while Cursor Write/Edit overwrite maps to {file_path, content}.
    let fallbacks = advertised_name_fallbacks(mapped_name);
    if let Some(name) = fallbacks
        .iter()
        .find_map(|candidate| allowed.get(*candidate).cloned())
    {
        return Some(name);
    }
    // MCP tools: match exact name, or any advertised tool ending with __{tool}.
    if mapped_name.starts_with("mcp__") || mapped_name.contains("__") {
        if let Some(hit) = allowed.iter().find(|n| *n == mapped_name) {
            return Some(hit.clone());
        }
        let suffix = mapped_name.rsplit("__").next().unwrap_or(mapped_name);
        if let Some(hit) = allowed
            .iter()
            .find(|n| *n == mapped_name || n.ends_with(&format!("__{suffix}")))
        {
            return Some(hit.clone());
        }
    }
    None
}

fn encode_request_context_reply(
    exec: &proto::ExecServerMessage,
    context: &RequestContext,
) -> Result<Bytes, CursorError> {
    // CLI does not put RequestContext on RunRequest. The server sends
    // ExecServerMessage.request_context_args (tag 10); we reply
    // ExecClientMessage.request_context_result (tag 10).
    //
    // proto.rs already has env.process_working_directory (tag 21), git_repos
    // (tag 11), and agent_skills (tag 29). cwd/git come from
    // `cursor_request_context` on the Anthropic body — do not dump the
    // full Claude system prompt. agent_skills stays empty here (no skill
    // corpus on this path).
    let message = AgentClientMessage {
        run_request: None,
        exec_client_message: Some(ExecClientMessage {
            id: exec.id,
            exec_id: exec.exec_id.clone(),
            local_execution_time_ms: None,
            shell_result: None,
            write_result: None,
            delete_result: None,
            grep_result: None,
            read_result: None,
            ls_result: None,
            request_context_result: Some(RequestContextResult {
                success: Some(RequestContextSuccess {
                    request_context: Some(context.clone()),
                    served_from_disk_cache: Some(false),
                }),
                error: None,
            }),
            shell_stream: None,
        }),
        kv_client_message: None,
        exec_client_control_message: None,
        interaction_response: None,
        client_heartbeat: None,
    };
    let result = encode_agent_message(&message)?;
    let close = encode_control_close(exec.id)
        .map_err(|error| CursorError::internal(format!("Cursor stream close encode: {error}")))?;
    let mut frames = Vec::with_capacity(result.len() + close.len());
    frames.extend_from_slice(&result);
    frames.extend_from_slice(&close);
    Ok(Bytes::from(frames))
}

fn encode_kv_reply(
    message: &KvServerMessage,
    blobs: &mut HashMap<Vec<u8>, Vec<u8>>,
) -> Result<Option<Bytes>, CursorError> {
    let reply = if let Some(args) = message.set_blob_args.as_ref() {
        blobs.insert(args.blob_id.clone(), args.blob_data.clone());
        KvClientMessage {
            id: message.id,
            get_blob_result: None,
            set_blob_result: Some(SetBlobResult { error: None }),
        }
    } else if let Some(args) = message.get_blob_args.as_ref() {
        KvClientMessage {
            id: message.id,
            get_blob_result: Some(GetBlobResult {
                blob_data: blobs.get(&args.blob_id).cloned(),
            }),
            set_blob_result: None,
        }
    } else {
        return Ok(None);
    };

    encode_agent_message(&AgentClientMessage {
        run_request: None,
        exec_client_message: None,
        kv_client_message: Some(reply),
        exec_client_control_message: None,
        interaction_response: None,
        client_heartbeat: None,
    })
    .map(Some)
}

/// Auto-approve / soft-reject InteractionQuery so HTTP/1 and BiDi agent runs
/// do not stall waiting for IDE UI. Advertised AskUserQuestion is handled in
/// `process_live_frame` (ClientOnly expose). Unadvertised AskQuestion is
/// rejected with an explicit reason.
fn encode_interaction_auto_response(
    query: &InteractionQuery,
) -> Result<Option<Bytes>, CursorError> {
    let mut response = InteractionResponse {
        id: query.id,
        ..Default::default()
    };
    let mut matched = false;
    if query.web_search_request_query.is_some() {
        response.web_search_request_response = Some(WebSearchRequestResponse {
            approved: Some(InteractionApproved {}),
            rejected: None,
        });
        matched = true;
    }
    if query.web_fetch_request_query.is_some() {
        response.web_fetch_request_response = Some(WebFetchRequestResponse {
            approved: Some(InteractionApproved {}),
            rejected: None,
        });
        matched = true;
    }
    if query.switch_mode_request_query.is_some() {
        response.switch_mode_request_response = Some(SwitchModeRequestResponse {
            approved: Some(InteractionApproved {}),
            rejected: None,
        });
        matched = true;
    }
    if query.mcp_auth_request_query.is_some() {
        // Cannot complete browser MCP OAuth from the proxy — reject clearly.
        response.mcp_auth_request_response = Some(McpAuthRequestResponse {
            approved: None,
            rejected: Some(InteractionRejected {
                reason: "claude-cursor-proxy cannot complete Cursor MCP auth UI".into(),
            }),
        });
        matched = true;
    }
    if query.create_plan_request_query.is_some() {
        response.create_plan_request_response = Some(CreatePlanRequestResponse {
            result: Some(CreatePlanResult {
                success: Some(CreatePlanSuccess {}),
                plan_uri: String::new(),
            }),
        });
        matched = true;
    }
    if query.ask_question_interaction_query.is_some() {
        response.ask_question_interaction_response = Some(AskQuestionInteractionResponse {
            result: Some(AskQuestionResult {
                rejected: Some(AskQuestionRejected {
                    reason: "claude-cursor-proxy has no interactive AskQuestion UI; answer via Claude tools instead".into(),
                }),
            }),
        });
        matched = true;
    }
    if !matched {
        return Err(CursorError::internal(
            "unsupported InteractionQuery; cannot present Cursor UI",
        ));
    }
    encode_agent_message(&AgentClientMessage {
        run_request: None,
        exec_client_message: None,
        kv_client_message: None,
        exec_client_control_message: None,
        interaction_response: Some(response),
        client_heartbeat: None,
    })
    .map(Some)
}

fn encode_agent_message(message: &AgentClientMessage) -> Result<Bytes, CursorError> {
    let mut payload = Vec::new();
    message
        .encode(&mut payload)
        .map_err(|e| CursorError::internal(format!("Cursor message encode: {e}")))?;
    Ok(encode_connect_frame(payload, 0))
}

/// How long the tool-result POST waits for the live driver to dequeue
/// `ResumeBatch`. The driver may be blocked on a BidiAppend send (up to 30s).
/// 2s was too short and 409 made grok-build treat a retryable wait as
/// `invalid_request`. Still bounded so a dead driver cannot hold HTTP open
/// until stream-idle.
const DEFAULT_RESUME_DISPATCH_MS: u64 = 20_000;

fn resume_dispatch_timeout() -> Duration {
    Duration::from_millis(env_u64(
        "CCP_CURSOR_LIVE_RESUME_DISPATCH_MS",
        DEFAULT_RESUME_DISPATCH_MS,
    ))
}

fn resume_dispatch_retryable_error(message: &str) -> CursorError {
    CursorError::new(429, message, None)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Like [`env_u64`] but allows an explicit `0` (e.g. disable tool-batch quiet).
fn env_u64_allow_zero(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

/// Minimum gap between monitor progress publishes on the SSE hot path.
/// TUI polls ~4Hz; publishing every thinking delta only contends on the lock.
const MONITOR_PROGRESS_MIN_INTERVAL: Duration = Duration::from_millis(100);

#[allow(clippy::too_many_arguments)]
fn publish_live_usage(
    monitor: &Option<(crate::monitor::MonitorHandle, String)>,
    encoder: &CursorSseEncoder,
    bytes: usize,
    chunks: u64,
    pending_bytes: &mut u64,
    pending_chunks: &mut u64,
    last_publish: &mut Instant,
    force: bool,
) {
    *pending_bytes = pending_bytes.saturating_add(bytes as u64);
    *pending_chunks = pending_chunks.saturating_add(chunks);
    let Some((handle, req_id)) = monitor else {
        *pending_bytes = 0;
        *pending_chunks = 0;
        return;
    };
    if !force && last_publish.elapsed() < MONITOR_PROGRESS_MIN_INTERVAL {
        return;
    }
    let (input_tokens, output_tokens) = encoder.current_usage();
    let input = Some(input_tokens).filter(|v| *v > 0);
    let output = Some(output_tokens).filter(|v| *v > 0);
    let published = if force {
        // Begin / finalize must land so TUI In/Out is not stuck on a stale seed.
        handle.stream_progress(req_id, *pending_bytes, *pending_chunks, input, output);
        true
    } else {
        // try_lock: never stall token emission behind TUI snapshot cloning.
        handle.try_stream_progress(req_id, *pending_bytes, *pending_chunks, input, output)
    };
    if published {
        *pending_bytes = 0;
        *pending_chunks = 0;
        *last_publish = Instant::now();
    }
}

fn live_stream_request_failed_log_fields(
    req_id: &str,
    status: u16,
    error: &str,
) -> serde_json::Map<String, serde_json::Value> {
    serde_json::Map::from_iter([
        ("reqId".to_string(), serde_json::json!(req_id)),
        ("status".to_string(), serde_json::json!(status)),
        ("message".to_string(), serde_json::json!(error)),
        ("path".to_string(), serde_json::json!("live_sse")),
    ])
}

fn log_live_stream_request_failed(req_id: &str, status: u16, error: &str) {
    crate::logging::create_logger("server").info(
        "request_failed",
        Some(live_stream_request_failed_log_fields(req_id, status, error)),
    );
}

fn live_sse_on_driver_drop(encoder: &CursorSseEncoder) -> Option<Vec<u8>> {
    if encoder.is_finalized() {
        return None;
    }
    Some(format_sse_event_bytes(
        EVENT_ERROR,
        &serde_json::json!({
            "type": "error",
            "error": {
                "type": "api_error",
                "message": "Cursor stream ended without turn_ended"
            }
        }),
    ))
}

/// No-byte live events keep `recv()` ready. If the Anthropic ping deadline
/// has already passed, emit that ping instead of draining another counter.
fn sse_keepalive_after_empty_event(
    now: tokio::time::Instant,
    ping_deadline: tokio::time::Instant,
) -> bool {
    now >= ping_deadline
}

pub fn live_sse_response(
    events: mpsc::Receiver<LiveEventResult>,
    message_id: String,
    model: String,
    estimated_input_tokens: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
) -> Response {
    struct State {
        events: mpsc::Receiver<LiveEventResult>,
        encoder: CursorSseEncoder,
        began: bool,
        done: bool,
        monitor: Option<(crate::monitor::MonitorHandle, String)>,
        pending_monitor_bytes: u64,
        pending_monitor_chunks: u64,
        last_monitor_publish: Instant,
        /// Periodic Anthropic `ping` so Claude Code's stream idle watchdog
        /// (≥300s by default) does not abort during quiet Cursor thinking.
        ping: tokio::time::Interval,
        ping_period: Duration,
        next_ping_at: tokio::time::Instant,
    }

    let mut encoder = CursorSseEncoder::new(message_id, model);
    encoder.seed_estimated_input_tokens(estimated_input_tokens);
    if let Some((ref handle, ref req_id)) = monitor {
        let (input_tokens, output_tokens) = encoder.current_usage();
        handle.usage_updated(
            req_id,
            Some(input_tokens).filter(|v| *v > 0),
            Some(output_tokens).filter(|v| *v > 0),
        );
    }

    // Claude Code: Math.max(CLAUDE_STREAM_IDLE_TIMEOUT_MS||0, 300000). Keep
    // well under that; Cursor BiDi heartbeats alone do not produce SSE bytes.
    let ping_secs = env_u64("CCP_ANTHROPIC_SSE_PING_SECS", 15).clamp(5, 120);
    let mut ping = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(ping_secs),
        Duration::from_secs(ping_secs),
    );
    // After a burst of thinking deltas, still space pings — Burst would emit a
    // catch-up flood then go quiet again under Claude's idle watchdog.
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let stream = futures_util::stream::unfold(
        State {
            events,
            encoder,
            began: false,
            done: false,
            monitor,
            pending_monitor_bytes: 0,
            pending_monitor_chunks: 0,
            last_monitor_publish: Instant::now()
                .checked_sub(MONITOR_PROGRESS_MIN_INTERVAL)
                .unwrap_or_else(Instant::now),
            ping,
            ping_period: Duration::from_secs(ping_secs),
            next_ping_at: tokio::time::Instant::now() + Duration::from_secs(ping_secs),
        },
        |mut state| async move {
            loop {
                if state.done {
                    return None;
                }
                if !state.began {
                    state.began = true;
                    state.encoder.begin();
                    let bytes = state.encoder.take_bytes();
                    if !bytes.is_empty() {
                        publish_live_usage(
                            &state.monitor,
                            &state.encoder,
                            bytes.len(),
                            1,
                            &mut state.pending_monitor_bytes,
                            &mut state.pending_monitor_chunks,
                            &mut state.last_monitor_publish,
                            true,
                        );
                        return Some((Ok::<Bytes, Infallible>(Bytes::from(bytes)), state));
                    }
                }
                tokio::select! {
                    biased;
                    maybe = state.events.recv() => {
                        match maybe {
                            Some(Ok(event)) => {
                                // One LiveRunEvent → one HTTP chunk. Do not
                                // coalesce text/thinking deltas: Claude Code's
                                // streaming UX expects near-realtime cadence,
                                // and opportunistic try_recv merges made tokens
                                // arrive in bursts after channel backlog.
                                apply_live_run_event(&mut state.encoder, event);
                                let bytes = state.encoder.take_bytes();
                                if bytes.is_empty() {
                                    // OutputTokenDelta / usage-only events keep
                                    // recv() ready and starve the ping arm of
                                    // this biased select. Emit an already-due
                                    // keepalive before draining more no-byte
                                    // events.
                                    if sse_keepalive_after_empty_event(
                                        tokio::time::Instant::now(),
                                        state.next_ping_at,
                                    ) {
                                        let now = tokio::time::Instant::now();
                                        state.ping.reset();
                                        state.next_ping_at = now + state.ping_period;
                                        let ping = format_sse_event_bytes(
                                            EVENT_PING,
                                            &serde_json::json!({ "type": "ping" }),
                                        );
                                        return Some((Ok(Bytes::from(ping)), state));
                                    }
                                } else {
                                    state.next_ping_at =
                                        tokio::time::Instant::now() + state.ping_period;
                                    let force = state.encoder.is_finalized();
                                    publish_live_usage(
                                        &state.monitor,
                                        &state.encoder,
                                        bytes.len(),
                                        1,
                                        &mut state.pending_monitor_bytes,
                                        &mut state.pending_monitor_chunks,
                                        &mut state.last_monitor_publish,
                                        force,
                                    );
                                    if force {
                                        state.done = true;
                                        let (input_tokens, output_tokens) =
                                            state.encoder.current_usage();
                                        if let Some((ref handle, ref req_id)) = state.monitor {
                                            handle.usage_updated(
                                                req_id,
                                                Some(input_tokens).filter(|v| *v > 0),
                                                Some(output_tokens).filter(|v| *v > 0),
                                            );
                                        }
                                    }
                                    return Some((Ok(Bytes::from(bytes)), state));
                                }
                            }
                            Some(Err(error)) => {
                                state.done = true;
                                let error_type = anthropic_error_type_from_live_error(&error);
                                let status =
                                    crate::retry::classify_proxy_error_status(502, &error);
                                let req_id = state
                                    .monitor
                                    .as_ref()
                                    .map(|(_, id)| id.as_str())
                                    .unwrap_or("-");
                                log_live_stream_request_failed(req_id, status, &error);
                                if let Some((ref handle, ref req_id)) = state.monitor {
                                    handle.request_failed(req_id, Some(status), error.clone());
                                }
                                let data = serde_json::json!({
                                    "type": "error",
                                    "error": {"type": error_type, "message": error}
                                });
                                return Some((
                                    Ok(Bytes::from(format_sse_event_bytes(EVENT_ERROR, &data))),
                                    state,
                                ));
                            }
                            None => {
                                state.done = true;
                                if let Some(error_bytes) = live_sse_on_driver_drop(&state.encoder) {
                                    let req_id = state
                                        .monitor
                                        .as_ref()
                                        .map(|(_, id)| id.as_str())
                                        .unwrap_or("-");
                                    log_live_stream_request_failed(
                                        req_id,
                                        502,
                                        "Cursor stream ended without turn_ended",
                                    );
                                    if let Some((ref handle, ref req_id)) = state.monitor {
                                        handle.request_failed(
                                            req_id,
                                            Some(502),
                                            "Cursor stream ended without turn_ended",
                                        );
                                    }
                                    return Some((Ok(Bytes::from(error_bytes)), state));
                                }
                                let bytes = state.encoder.take_bytes();
                                if !bytes.is_empty() || state.pending_monitor_bytes > 0 {
                                    publish_live_usage(
                                        &state.monitor,
                                        &state.encoder,
                                        bytes.len(),
                                        if bytes.is_empty() { 0 } else { 1 },
                                        &mut state.pending_monitor_bytes,
                                        &mut state.pending_monitor_chunks,
                                        &mut state.last_monitor_publish,
                                        true,
                                    );
                                }
                                let (input_tokens, output_tokens) = state.encoder.current_usage();
                                if let Some((ref handle, ref req_id)) = state.monitor {
                                    handle.usage_updated(
                                        req_id,
                                        Some(input_tokens).filter(|v| *v > 0),
                                        Some(output_tokens).filter(|v| *v > 0),
                                    );
                                }
                                if bytes.is_empty() {
                                    return None;
                                }
                                return Some((Ok(Bytes::from(bytes)), state));
                            }
                        }
                    }
                    _ = state.ping.tick(), if !state.encoder.is_finalized() => {
                        // Keep the Anthropic SSE byte stream alive during long
                        // quiet thinking (Cursor may only send BiDi heartbeats).
                        state.next_ping_at =
                            tokio::time::Instant::now() + state.ping_period;
                        let ping = format_sse_event_bytes(
                            EVENT_PING,
                            &serde_json::json!({ "type": "ping" }),
                        );
                        return Some((Ok(Bytes::from(ping)), state));
                    }
                }
            }
        },
    );

    let mut response = Body::from_stream(stream).into_response();
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/event-stream"),
    );
    response.headers_mut().insert(
        http::header::CACHE_CONTROL,
        http::HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("keep-alive"),
    );
    response.headers_mut().insert(
        http::HeaderName::from_static("x-accel-buffering"),
        http::HeaderValue::from_static("no"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::super::proto::{
        ExecReadArgs, ExecServerMessage, GitRepoInfo, InteractionUpdate, RequestContextArgs,
        RequestContextEnv, TextDelta, TurnEnded,
    };
    use super::*;
    use prost::Message;

    fn pending_exec(id: u32, tool_use_id: &str) -> PendingCursorExec {
        PendingCursorExec {
            id,
            exec_id: Some(format!("exec-{id}")),
            tool_use_id: tool_use_id.to_string(),
            claude_name: "Read".into(),
            claude_input: serde_json::json!({"file_path": format!("/{id}.txt")}),
            kind: super::super::exec_results::CursorExecKind::Read {
                path: format!("/{id}.txt"),
                range_applied: false,
            },
        }
    }

    fn test_reconnect_context() -> LiveReconnectContext {
        LiveReconnectContext {
            http: CursorHttpClient::new(),
            token: "test-token".into(),
            identity: LiveIdentityHeaders {
                client_type: "cli".into(),
                client_version: "cli-test".into(),
                ghost_mode: "true".into(),
                ide_profile: false,
                headers: vec![],
            },
            session_id: "sess-test".into(),
            model_id: "composer-2.5".into(),
            conversation_id: Some("conv-test".into()),
            force_http1: false,
            http1_rejected: false,
            mcp_tools: None,
            opening_checkpoint: None,
            recovery: LiveRecoveryEpisode::default(),
            breakers: TransportBreakers::default(),
            last_trigger: String::new(),
        }
    }

    fn test_generation_permit() -> LiveGenerationPermit {
        LiveGenerationGate::new(1)
            .try_acquire(LiveGenerationPriority::Start)
            .expect("test generation permit")
    }

    #[test]
    fn generation_permit_releases_between_segments_before_native_batch_exposure() {
        let generation_gate = LiveGenerationGate::new(1);
        let mut generation_permit = Some(
            generation_gate
                .try_acquire(LiveGenerationPriority::Start)
                .expect("initial generation permit"),
        );
        let mut pending = PendingExecState::for_run("between-segments-run");
        assert!(pending.queue(pending_exec(1, "read-call"), Duration::from_secs(30)));
        assert!(
            pending.awaiting().is_empty() && pending.has_outstanding_native(),
            "the native tool is still collecting, not exposed"
        );
        let sink = None;

        release_generation_permit_between_segments(&sink, &mut generation_permit);

        assert!(
            generation_permit.is_none(),
            "a driver without a downstream segment must not retain generation capacity"
        );
        assert_eq!(generation_gate.available_permits(), 1);
    }

    #[tokio::test]
    async fn generation_permit_releases_after_native_tool_handoff() {
        let generation_gate = LiveGenerationGate::new(1);
        let generation_permit = generation_gate
            .try_acquire(LiveGenerationPriority::Start)
            .expect("initial generation permit");
        let (upstream_tx, upstream_rx) = mpsc::channel(4);
        let (request_tx, _request_rx) = mpsc::channel(8);
        let (command_tx, command_rx) = mpsc::channel(4);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let completed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));

        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            tokio::spawn(std::future::pending::<()>()),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            event_tx,
            Arc::clone(&pending_shared),
            Arc::new(Mutex::new(None)),
            Arc::clone(&completed),
            Arc::clone(&cancel_requested),
            Some(BTreeSet::from(["Read".into()])),
            "generation-lease-handoff".into(),
            "generation-lease-run".into(),
            HashMap::new(),
            "read the file".into(),
            RequestContext::default(),
            test_reconnect_context(),
            generation_permit,
        ));

        let mut payload = Vec::new();
        proto::AgentServerMessage {
            exec_server_message: Some(ExecServerMessage {
                id: 1,
                exec_id: Some("read-exec".into()),
                read_args: Some(ExecReadArgs {
                    path: "/tmp/input.txt".into(),
                    tool_call_id: "read-call".into(),
                    offset: None,
                    limit: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut payload)
        .expect("encode read tool");
        upstream_tx
            .send(Ok(Some(encode_connect_frame(payload, 0))))
            .await
            .expect("send read tool");

        let event = tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
            .await
            .expect("native tool was not exposed")
            .expect("native tool event");
        let tool_use_id = match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => tools[0].tool_use_id.clone(),
            other => panic!("expected native tool batch, got {other:?}"),
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while generation_gate.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("generation permit stayed held while waiting for local tool results");
        assert!(
            !completed.load(Ordering::Acquire),
            "the live driver must remain resumable after releasing generation capacity"
        );
        assert_eq!(
            pending_shared
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .len(),
            1
        );

        let resume_generation_permit = generation_gate
            .try_acquire(LiveGenerationPriority::Resume)
            .expect("resume must reacquire generation capacity");
        let (resume_sink, _resume_events) = mpsc::channel(8);
        let (ack_tx, ack_rx) = oneshot::channel();
        command_tx
            .send(RunCommand::ResumeBatch {
                tool_results: vec![(
                    tool_use_id,
                    serde_json::json!({"type":"tool_result","content":"done"}),
                )],
                sink: resume_sink,
                ack: ack_tx,
                permit: LiveResumePermit {
                    in_flight: Arc::new(AtomicBool::new(true)),
                },
                generation_permit: resume_generation_permit,
                dispatch_state: Arc::new(AtomicU8::new(RESUME_DISPATCH_WAITING)),
            })
            .await
            .expect("dispatch resume");
        ack_rx
            .await
            .expect("resume acknowledgement")
            .expect("resume accepted");
        assert_eq!(
            generation_gate.available_permits(),
            0,
            "active post-tool generation must hold capacity again"
        );

        cancel_requested.store(true, Ordering::Release);
        command_tx
            .send(RunCommand::Cancel { ack: None })
            .await
            .expect("cancel driver");
        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("driver did not stop")
            .expect("driver join");
        assert_eq!(generation_gate.available_permits(), 1);
    }

    #[test]
    fn advertised_name_requires_a_real_downstream_tool() {
        let allowed = BTreeSet::from(["Read".to_string()]);
        assert_eq!(
            resolve_advertised_name("Read", Some(&allowed)).as_deref(),
            Some("Read")
        );
        assert!(resolve_advertised_name("Bash", Some(&allowed)).is_none());
    }

    #[test]
    fn server_heartbeat_does_not_refresh_idle_progress() {
        let mut last_progress = Instant::now() - Duration::from_secs(45);
        let before = last_progress;
        record_server_heartbeat(&mut last_progress);
        assert_eq!(
            last_progress, before,
            "heartbeats keep TCP alive but must not reset setup/stream idle"
        );
    }

    #[test]
    fn rate_limit_is_not_an_http1_fallback() {
        let err = CursorError::new(429, "rate limited", None);
        assert!(
            !is_http1_fallback_error(&err),
            "429 must not be retried over H1 after H2 already consumed the quota"
        );
    }

    #[test]
    fn semantic_client_errors_are_not_retryable_live_transports() {
        for status in [400, 401, 403, 404, 429] {
            let err = CursorError::new(status, "no", None);
            assert!(
                !is_retryable_live_transport_error(&err),
                "HTTP {status} must not fall through to buffered run_agent"
            );
        }
        let transport = CursorError::new(502, "error sending request", None);
        assert!(is_retryable_live_transport_error(&transport));
        let rate_limit_with_connection = CursorError::new(
            429,
            "Connect error 429: connection closed: rate limit exceeded",
            None,
        );
        assert!(
            !is_retryable_live_transport_error(&rate_limit_with_connection),
            "429 mentioning connection must still not retry"
        );
    }

    #[test]
    fn initial_h2_open_fails_fast_http1_keeps_upload_budget() {
        assert_eq!(LIVE_H2_OPEN_ATTEMPT.as_secs(), 20);
        assert_eq!(LIVE_H1_OPEN_ATTEMPT.as_secs(), 90);
    }

    #[test]
    fn ambiguous_open_timeout_does_not_start_http1() {
        let timeout = CursorError::new(504, "Cursor live open timed out after 20s", None);
        assert!(
            !is_explicit_http1_required(&timeout),
            "H2 send() timeout may already have delivered Run; HTTP/1 would duplicate it"
        );
        let rst = CursorError::new(0, "error sending request: connection reset", None);
        assert!(!is_explicit_http1_required(&rst));
        let clash = CursorError::new(464, "incompatible version", None);
        assert!(is_explicit_http1_required(&clash));
        let required = CursorError::new(421, "HTTP_1_1_REQUIRED", Some("HTTP_1_1_REQUIRED".into()));
        assert!(is_explicit_http1_required(&required));
    }

    #[test]
    fn live_open_retries_http1_on_same_request_before_409() {
        let timeout = CursorError::new(504, "Cursor live open timed out after 20s", None);
        assert!(
            live_open_should_retry_http1(&timeout),
            "H2 open timeout must try HTTP/1 on this same request_id before 409"
        );
        let connect = CursorError::new(502, "Cursor upstream connect failed", None);
        assert!(
            live_open_should_retry_http1(&connect),
            "a refused connect never reached Cursor; retry HTTP/1 before 409"
        );
        let url_send = CursorError::new(
            502,
            "error sending request for url (https://api2.cursor.sh/aiserver.v1.BidiService/BidiAppend)",
            None,
        );
        assert!(live_open_should_retry_http1(&url_send));
        let reset = CursorError::new(502, "error sending request: connection reset", None);
        assert!(
            !live_open_should_retry_http1(&reset),
            "a mid-send reset is still an accept risk; do not start HTTP/1"
        );
        let rate = CursorError::new(429, "High Load — switch to another model", None);
        assert!(!live_open_should_retry_http1(&rate));
        let invoice = CursorError::new(429, "You have an unpaid invoice", None);
        assert!(!live_open_should_retry_http1(&invoice));
    }

    #[test]
    fn exhausted_start_retries_surface_original_as_409() {
        let connect = CursorError::new(502, "Cursor upstream connect failed", None);
        let exhausted = exhausted_live_start_error(connect.clone(), 3);
        assert_eq!(
            exhausted.status, 409,
            "after proxy-internal retries grok-build must see 409, not 5xx-retry"
        );
        assert_eq!(exhausted.message, "Cursor upstream connect failed");
        let first = exhausted_live_start_error(connect, 0);
        assert_eq!(
            first.status, 502,
            "the first miss is still retryable inside this POST"
        );
        let timeout = CursorError::new(504, "Cursor live open timed out after 20s", None);
        let still_timeout = exhausted_live_start_error(timeout, 0);
        assert_eq!(
            still_timeout.status, 504,
            "H2 timeout is retried as HTTP/1 on the same request_id, not a new Run"
        );
        let shed = CursorError::new(
            429,
            "High Load — We're experiencing high demand. Please switch to Auto, another model, or try again in a few moments.",
            None,
        );
        assert_eq!(exhausted_live_start_error(shed, 3).status, 429);
    }

    #[test]
    fn semantic_status_is_never_an_http1_fallback() {
        for status in [400, 401, 403, 404, 429] {
            let err = CursorError::new(
                status,
                "connection closed: bidi disabled",
                Some("bidi".into()),
            );
            assert!(
                !is_http1_fallback_error(&err),
                "HTTP {status} must not flip to HTTP/1"
            );
            assert!(!is_explicit_http1_required(&err));
        }
    }

    #[test]
    fn h1_fallback_budget_is_shared_wall_clock() {
        let h1 = Duration::from_secs(90);
        assert_eq!(
            live_h1_fallback_budget(h1, Duration::from_millis(100))
                .unwrap()
                .as_secs(),
            89
        );
        assert!(live_h1_fallback_budget(h1, h1).is_none());
        assert!(live_h1_fallback_budget(h1, h1 + Duration::from_secs(1)).is_none());
    }

    #[test]
    fn live_open_timeout_is_an_http1_fallback() {
        let err = CursorError::new(504, "Cursor live open timed out after 90s", None);
        assert!(
            is_http1_fallback_error(&err),
            "reconnect may retry the same episode after a 504"
        );
        assert!(
            !is_explicit_http1_required(&err),
            "initial open must not H1-fallback a timed-out H2 send"
        );
    }

    #[test]
    fn kv_checkpoint_and_query_do_not_reset_reconnect_budget() {
        let checkpoint = proto::AgentServerMessage {
            conversation_checkpoint_update: Some(vec![1, 2, 3]),
            ..proto::AgentServerMessage::default()
        };
        assert!(
            !server_message_resets_reconnect_budget(&checkpoint),
            "checkpoint loops must not replenish ResumeAction forever"
        );

        let kv = proto::AgentServerMessage {
            kv_server_message: Some(proto::KvServerMessage {
                id: 1,
                get_blob_args: None,
                set_blob_args: None,
                span_context: None,
            }),
            ..proto::AgentServerMessage::default()
        };
        assert!(!server_message_resets_reconnect_budget(&kv));

        let query = proto::AgentServerMessage {
            interaction_query: Some(proto::InteractionQuery {
                id: 1,
                ..proto::InteractionQuery::default()
            }),
            ..proto::AgentServerMessage::default()
        };
        assert!(!server_message_resets_reconnect_budget(&query));

        let request_context = proto::AgentServerMessage {
            exec_server_message: Some(proto::ExecServerMessage {
                request_context_args: Some(proto::RequestContextArgs::default()),
                ..proto::ExecServerMessage::default()
            }),
            ..proto::AgentServerMessage::default()
        };
        assert!(
            !server_message_resets_reconnect_budget(&request_context),
            "handshake request_context must not replenish ResumeAction"
        );
        let native_exec = proto::AgentServerMessage {
            exec_server_message: Some(proto::ExecServerMessage::default()),
            ..proto::AgentServerMessage::default()
        };
        assert!(server_message_resets_reconnect_budget(&native_exec));

        let raw = native_exec.encode_to_vec();
        let mut compressed = Vec::new();
        {
            use std::io::Write;
            let mut encoder =
                flate2::write::GzEncoder::new(&mut compressed, flate2::Compression::fast());
            encoder.write_all(&raw).unwrap();
            encoder.finish().unwrap();
        }
        let gzip_frame = ConnectFrame {
            flags: super::super::connect::FLAG_GZIP,
            payload: Bytes::from(compressed),
        };
        assert!(
            connect_frame_resets_reconnect_budget(&gzip_frame),
            "gzip-wrapped native exec must still reset the reconnect budget"
        );
    }

    #[test]
    fn probation_expires_only_without_progress_at_deadline() {
        assert!(!live_probation_expired(false, false, Duration::ZERO));
        assert!(!live_probation_expired(true, true, Duration::ZERO));
        assert!(!live_probation_expired(true, false, Duration::from_secs(1)));
        assert!(live_probation_expired(true, false, Duration::ZERO));
        assert!(
            !live_should_resume_after_drop(true, false),
            "delayed hollow after HTTP 200 must not ResumeAction again"
        );
        assert!(live_should_resume_after_drop(true, true));
        assert!(
            live_should_resume_after_drop(false, false),
            "first stream drop of an already-accepted Run may ResumeAction"
        );
    }

    #[test]
    fn hollow_resume_of_checkpoint_without_output_rotates_the_binding() {
        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        super::super::conversation::reset_for_test();
        let session_id = "sess-hollow-checkpoint";
        let original =
            super::super::conversation::continuation_for(Some(session_id)).conversation_id;
        super::super::conversation::save_checkpoint(session_id, vec![0x08, 0x01]);
        let stale_blobs = HashMap::from([(vec![0xaa], vec![0xbb])]);
        super::super::conversation::merge_blobs(session_id, &stale_blobs);
        let mut latest_checkpoint = Some(vec![0x08, 0x01]);
        let mut kv_blobs = stale_blobs;

        let message = hollow_resume_terminal_message(
            session_id,
            true,
            false,
            true,
            &mut latest_checkpoint,
            &mut kv_blobs,
            "Cursor resume produced no progress before the recovery deadline",
        );

        assert!(message.contains(CONVERSATION_RESET_RETRY_NOTE), "{message}");
        assert_eq!(
            crate::retry::classify_proxy_error_status(502, &message),
            502,
            "a known-stale checkpoint must be retryable rather than ambiguous"
        );
        assert!(!live_should_persist_continuation_message(Some(&message)));
        assert!(latest_checkpoint.is_none());
        assert!(kv_blobs.is_empty());
        let recovered = super::super::conversation::continuation_for(Some(session_id));
        assert_ne!(recovered.conversation_id, original);
        assert!(!recovered.has_checkpoint);
        assert!(recovered.pre_fetched_blobs.is_empty());
    }

    #[test]
    fn hollow_resume_without_safe_reset_condition_stays_ambiguous() {
        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        super::super::conversation::reset_for_test();
        let session_id = "sess-hollow-ambiguous";
        let original =
            super::super::conversation::continuation_for(Some(session_id)).conversation_id;
        let fallback = "Cursor resume produced no progress before the recovery deadline";

        for (opened_with_checkpoint, useful, pending_empty) in [
            (false, false, true),
            (true, true, true),
            (true, false, false),
        ] {
            let mut latest_checkpoint = Some(vec![0x08, 0x01]);
            let mut kv_blobs = HashMap::from([(vec![0xaa], vec![0xbb])]);
            let message = hollow_resume_terminal_message(
                session_id,
                opened_with_checkpoint,
                useful,
                pending_empty,
                &mut latest_checkpoint,
                &mut kv_blobs,
                fallback,
            );

            assert_eq!(message, fallback);
            assert!(
                terminal_error_is_ambiguous_accept(&message),
                "unsafe reset case must remain fail-closed: {message}"
            );
            assert_eq!(
                crate::retry::classify_proxy_error_status(502, &message),
                409
            );
            assert!(live_should_persist_continuation_message(Some(&message)));
            assert_eq!(latest_checkpoint, Some(vec![0x08, 0x01]));
            assert_eq!(kv_blobs, HashMap::from([(vec![0xaa], vec![0xbb])]));
            assert_eq!(
                super::super::conversation::continuation_for(Some(session_id)).conversation_id,
                original
            );
        }
    }

    #[test]
    fn h2_breaker_does_not_force_http1() {
        let mut ctx = test_reconnect_context();
        let now = Instant::now();
        for _ in 0..TRANSPORT_BREAKER_THRESHOLD {
            record_transport_failure(&mut ctx, now);
        }
        apply_transport_breakers(&mut ctx, now);
        assert!(
            !ctx.force_http1,
            "H2 timeouts must not pin HTTP/1 — that would duplicate an accepted Run"
        );
        assert!(
            !ctx.http1_rejected,
            "circuit-open must not poison HTTP/1 as 464-rejected"
        );
        assert!(!ctx.breakers.h2.allows(now));
    }

    #[test]
    fn open_h1_breaker_returns_to_h2_when_h2_allows() {
        let mut ctx = test_reconnect_context();
        ctx.force_http1 = true;
        let now = Instant::now();
        for _ in 0..TRANSPORT_BREAKER_THRESHOLD {
            record_transport_failure(&mut ctx, now);
        }
        apply_transport_breakers(&mut ctx, now);
        assert!(
            !ctx.force_http1,
            "open HTTP/1 breaker must not pin a rejected-looking H1 forever"
        );
    }

    #[test]
    fn semantic_send_failure_is_terminal_not_reconnect() {
        let rate = CursorError::new(429, "BidiAppend failed with HTTP 429", None);
        assert!(live_send_failure_is_terminal(&rate));
        let reset = CursorError::new(0, "error sending request: connection reset", None);
        assert!(!live_send_failure_is_terminal(&reset));
        let append_timeout = CursorError::new(
            504,
            "Cursor BidiAppend timed out; acceptance is ambiguous",
            None,
        );
        assert!(
            live_send_failure_is_terminal(&append_timeout),
            "a timed-out HTTP/1 append must never be replayed"
        );
        let ambiguous_initial_append = ambiguous_http1_append_error(
            CursorError::new(500, "BidiAppend failed with HTTP 500", None),
            "initial Run",
        );
        assert!(live_send_failure_is_terminal(&ambiguous_initial_append));
        assert!(
            live_start_error_seals_tombstone(&ambiguous_initial_append),
            "an HTTP/1 open whose initial append may have landed must seal Starting"
        );
        let reset_open = CursorError::new(
            502,
            format!("Cursor RunSSE HTTP 502 ({CONVERSATION_RESET_RETRY_NOTE})"),
            Some("Conversation data missing".into()),
        );
        assert!(
            !live_start_error_seals_tombstone(&reset_open),
            "a reset conversation must be retryable immediately"
        );
        let retryable_missing = CursorError::new(
            502,
            "Cursor RunSSE HTTP 502",
            Some("Conversation data missing (1 missing blob: abc)".into()),
        );
        assert!(
            is_retryable_live_transport_error(&retryable_missing),
            "HTTP 502 is otherwise a reconnect candidate"
        );
        assert!(
            live_send_failure_is_terminal(&retryable_missing),
            "missing conversation must not ResumeAction or replay a send"
        );
        assert!(
            live_reconnect_open_error_is_fatal(&retryable_missing),
            "ResumeAction must not retry a missing-conversation 502"
        );
        let annotated_missing = CursorError::new(
            502,
            format!("Cursor RunSSE HTTP 502 ({CONVERSATION_RESET_RETRY_NOTE})"),
            Some("Conversation data missing (1 missing blob: abc)".into()),
        );
        assert!(live_reconnect_open_error_is_fatal(&annotated_missing));
        assert!(
            !live_should_persist_continuation_message(Some(&annotated_missing.message)),
            "driver teardown must not re-bind the poisoned checkpoint after reset"
        );
        assert!(live_should_persist_continuation_message(None));
        assert!(live_should_persist_continuation_message(Some(
            "Cursor live run hard timeout"
        )));
        assert!(live_acceptance_unresolved(false, true, false));
        assert!(
            live_control_close_message(true).contains("ambiguous"),
            "an unconfirmed resume plus a dropped control channel must not free the slot"
        );
        assert!(!live_control_close_message(false).contains("ambiguous"));
        let partial_batch =
            partial_tool_result_send_error(CursorError::new(502, "connection reset", None), 1, 2);
        assert!(
            live_send_failure_is_terminal(&partial_batch),
            "a partially queued result batch must fail closed"
        );
        assert!(
            classify_outbound_send(Err(rate.clone())).is_err(),
            "control_close/BidiAppend 429 must fail the turn, not ResumeAction"
        );
        assert!(!classify_outbound_send(Err(reset)).unwrap());
        assert!(classify_outbound_send(Ok(())).unwrap());
        let timeout = CursorError::new(504, "Cursor live open timed out after 20s", None);
        assert!(is_ambiguous_live_open_timeout(&timeout));
        let gateway = CursorError::new(504, "Gateway Timeout", None);
        assert!(
            is_ambiguous_live_open_timeout(&gateway),
            "any HTTP 504 is an ambiguous accept"
        );
        assert!(!is_ambiguous_live_open_timeout(&rate));
        let mapped_reset = CursorError::new(
            502,
            "error sending request: connection reset",
            Some("error sending request: connection reset".into()),
        );
        assert!(
            is_response_less_send_error(&mapped_reset),
            "reqwest maps no-status reset to 502; that is still an ambiguous send"
        );
        assert!(!is_response_less_send_error(&CursorError::new(
            502,
            "Cursor upstream connect failed",
            None
        )));
        assert!(live_start_error_seals_tombstone(&timeout));
        assert!(live_start_error_seals_tombstone(&mapped_reset));
        assert!(!live_start_error_seals_tombstone(&rate));
        assert!(!live_start_error_seals_tombstone(&CursorError::new(
            502,
            "Cursor upstream connect failed",
            None
        )));
        assert!(!live_start_error_seals_tombstone(&CursorError::new(
            464,
            "incompatible version",
            None
        )));
        let connect_failed = CursorError::new(
            502,
            "Cursor upstream connect failed",
            Some(
                "error sending request for url (https://api2.cursor.sh/aiserver.v1.BidiService/BidiAppend)"
                    .into(),
            ),
        );
        let wrapped_connect = ambiguous_http1_append_error(connect_failed.clone(), "initial Run");
        assert_eq!(
            wrapped_connect.message, "Cursor upstream connect failed",
            "wrapping a refused TCP connect as 'acceptance is ambiguous' 409s grok-build"
        );
        assert!(
            !live_send_failure_is_terminal(&wrapped_connect),
            "never-connected BidiAppend must reconnect, not fail the turn"
        );
        assert!(
            !live_start_error_seals_tombstone(&wrapped_connect),
            "a refused connect must not seal an 'already active' tombstone"
        );
        assert!(!live_reconnect_open_error_is_fatal(&wrapped_connect));
        let url_send = CursorError::new(
            502,
            "error sending request for url (https://api2.cursor.sh/aiserver.v1.BidiService/BidiAppend)",
            Some(
                "error sending request for url (https://api2.cursor.sh/aiserver.v1.BidiService/BidiAppend)"
                    .into(),
            ),
        );
        assert!(
            is_pre_connect_failure(&url_send),
            "reqwest's URL-only Display is a send that never got an HTTP status"
        );
        assert!(
            !is_response_less_send_error(&url_send),
            "URL-only send failure is not an accepted Run"
        );
        assert!(
            !live_start_error_seals_tombstone(&url_send),
            "16:37 BidiAppend URL errors must not brick the session with 409"
        );
    }

    #[tokio::test]
    async fn auxiliary_http1_connect_refused_is_not_ambiguous_accept() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve closed test port");
        let address = listener.local_addr().expect("test port");
        drop(listener);
        let outbound = ClientOutbound::Http1(BidiAppendSession::new(
            reqwest::Client::builder()
                .no_proxy()
                .build()
                .expect("reqwest client"),
            format!("http://{address}"),
            "token".into(),
            "request-id".into(),
            vec![],
        ));
        let (event_tx, _events) = mpsc::channel(1);
        let mut sink = Some(event_tx);
        let terminal_error = Arc::new(Mutex::new(None));

        assert!(
            !send_frame_or_fail(
                &outbound,
                &mut sink,
                &terminal_error,
                Bytes::from_static(b"frame"),
                "KV response",
                "sess-test",
            )
            .await
        );
        let message = terminal_error
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|outcome| outcome.message.clone())
            .expect("terminal error");
        assert!(
            !message.contains("acceptance is ambiguous"),
            "connection refused never reached Cursor: {message}"
        );
        assert!(
            message.contains("connect failed") || message.contains("error sending request"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn cancelling_in_flight_http1_control_close_preserves_ambiguity() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled BidiAppend server");
        let address = listener.local_addr().expect("server address");
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept BidiAppend");
            let mut request = [0u8; 4096];
            let bytes_read = socket.read(&mut request).await.expect("read BidiAppend");
            assert!(bytes_read > 0, "empty BidiAppend request");
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
        });
        let mut outbound = ClientOutbound::Http1(BidiAppendSession::new(
            reqwest::Client::new(),
            format!("http://{address}"),
            "token".into(),
            "request-id".into(),
            vec![],
        ));
        let (upstream_tx, mut upstream) = mpsc::channel(1);
        let mut driver_upstream_tx = upstream_tx;
        let mut upstream_pump = tokio::spawn(std::future::pending::<()>());
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let cancel_signal = Arc::clone(&cancel_requested);
        let terminal_error = Arc::new(Mutex::new(None));
        let mut reconnect = test_reconnect_context();
        let mut decoder = ConnectFrameDecoder::new();
        let latest_checkpoint = Some(vec![0x08, 0x01]);
        let kv_blobs = HashMap::new();
        let mut pending = PendingExecState::default();
        assert!(pending.queue(
            pending_exec(1, "control-close-tool"),
            Duration::from_secs(1)
        ));
        let mut reconnect_attempts = 0;
        let mut last_progress = Instant::now();
        let mut resume_grace_until = None;

        let cancel = async move {
            accepted_rx.await.expect("BidiAppend accepted by mock");
            cancel_signal.store(true, Ordering::Release);
        };
        let reconnecting = try_live_reconnect(
            &mut reconnect,
            &mut outbound,
            &mut upstream,
            &mut driver_upstream_tx,
            &mut upstream_pump,
            cancel_requested.as_ref(),
            &terminal_error,
            &mut decoder,
            &latest_checkpoint,
            &kv_blobs,
            &mut pending,
            &mut reconnect_attempts,
            10,
            &mut last_progress,
            &mut resume_grace_until,
            Duration::from_secs(1),
            Instant::now() + Duration::from_secs(10),
        );
        let (outcome, ()) = tokio::join!(reconnecting, cancel);
        server.abort();
        let _ = server.await;

        let LiveReconnectOutcome::Failed(detail) = outcome else {
            panic!("expected failed reconnect");
        };
        assert!(detail.contains("ambiguous"), "{detail}");
        let stored = terminal_error
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|outcome| outcome.message.clone())
            .expect("ambiguous terminal outcome");
        assert!(stored.contains("ambiguous"), "{stored}");
    }

    #[test]
    fn reconnect_open_does_not_h1_fallback_inside_one_send() {
        let ctx = test_reconnect_context();
        assert!(
            !live_reconnect_open_allow_h1(&ctx, Instant::now()),
            "reconnect must switch transports in the loop, not inside open_live_transport"
        );
    }

    #[test]
    fn dropped_sse_channel_is_an_error_not_end_turn() {
        let mut encoder = CursorSseEncoder::new("msg_drop", "claude-fable-5");
        encoder.begin();
        encoder.push_event(&CursorStreamEvent::TextDelta {
            text: "partial".into(),
        });
        let bytes = live_sse_on_driver_drop(&encoder).expect("incomplete stream must error");
        let text = String::from_utf8(bytes).unwrap();
        assert!(text.contains("event: error"), "{text}");
        assert!(text.contains("without turn_ended"), "{text}");
        assert!(!text.contains("end_turn"), "{text}");

        encoder.finalize();
        assert!(live_sse_on_driver_drop(&encoder).is_none());
    }

    #[test]
    fn opening_checkpoint_seeds_reconnect_state() {
        assert!(opening_live_checkpoint(&[]).is_none());
        assert_eq!(
            opening_live_checkpoint(&[0x0a, 0x02, 0x01, 0x02]),
            Some(vec![0x0a, 0x02, 0x01, 0x02])
        );
    }

    #[test]
    fn reconnect_skip_reason_requires_checkpoint() {
        assert_eq!(
            live_reconnect_skip_reason(&None, &None, None, 0, 10),
            Some("no checkpoint")
        );
        assert!(live_reconnect_skip_reason(&Some(vec![0x0a]), &None, None, 0, 10).is_none());
        assert!(live_reconnect_skip_reason(&None, &Some(vec![0x0a]), None, 0, 10).is_none());
        assert_eq!(
            live_reconnect_skip_reason(&Some(vec![0x0a]), &None, None, 10, 10),
            Some("reconnect budget exhausted")
        );
        assert!(
            live_reconnect_skip_reason(&None, &None, Some("conv-first-turn"), 0, 10).is_none(),
            "first-turn ResumeAction uses conversation_id when Cursor has not sent a checkpoint yet"
        );
        assert_eq!(
            live_reconnect_resume_state(&None, &None, Some("conv-first-turn")),
            Some(Vec::new())
        );
        assert!(live_reconnect_resume_state(&None, &None, None).is_none());
    }

    #[test]
    fn reconnect_note_explains_skip_and_failure() {
        assert_eq!(reconnect_note(&LiveReconnectOutcome::Reconnected), "");
        assert_eq!(
            reconnect_note(&LiveReconnectOutcome::Skipped("no checkpoint")),
            " (reconnect skipped: no checkpoint)"
        );
        assert_eq!(
            reconnect_note(&LiveReconnectOutcome::Failed("timeout (504)".into())),
            " (reconnect failed: timeout (504))"
        );
    }

    #[test]
    fn reconnect_skip_reason_allows_pending_tools() {
        assert!(
            live_reconnect_skip_reason(&Some(vec![0x01]), &None, None, 0, 10).is_none(),
            "Claude-owed tool_results must not block ResumeAction; the BiDi is still needed"
        );
    }

    #[test]
    fn reconnect_backoff_uses_full_jitter_from_attempt_one() {
        assert_eq!(live_reconnect_backoff_ceiling_ms(1, u64::MAX), 1_000);
        assert_eq!(live_reconnect_backoff_ceiling_ms(2, u64::MAX), 2_000);
        assert_eq!(live_reconnect_backoff_ceiling_ms(3, u64::MAX), 4_000);
        assert_eq!(
            live_reconnect_backoff_ceiling_ms(20, u64::MAX),
            LIVE_RECONNECT_BACKOFF_CAP_MS,
            "backoff must not recreate the five-minute 502"
        );
        assert_eq!(
            live_reconnect_backoff_ceiling_ms(3, 800),
            400,
            "sleep at most half the remaining recovery window"
        );
        assert_eq!(live_reconnect_backoff_ms_for(10, true, u64::MAX), 0);

        let samples: Vec<u64> = (0..40)
            .map(|_| live_reconnect_backoff_ms_for(1, false, u64::MAX))
            .collect();
        assert!(
            samples.iter().all(|&ms| ms <= 1_000),
            "attempt 1 full jitter must stay in 0..=1000: {samples:?}"
        );
        assert!(
            samples.iter().any(|&ms| ms > 0),
            "attempt 1 must not stay at 0ms or reconnect storms collide: {samples:?}"
        );
        for _ in 0..20 {
            let ms = live_reconnect_backoff_ms_for(2, false, u64::MAX);
            assert!(ms <= 2_000, "{ms}");
        }
    }

    #[test]
    fn process_h2_circuit_trips_on_first_open_timeout() {
        let mut circuit = ProcessH2Circuit::default();
        let t0 = Instant::now();
        assert!(!circuit.prefers_http1_at(t0));
        assert!(
            circuit.on_h2_open_timeout_at(t0),
            "the first H2 open timeout must pin HTTP/1; waiting for 3 consecutive 20s 409s is the grok-build loop"
        );
        assert!(circuit.prefers_http1_at(t0));
        assert!(
            !circuit.on_h2_open_timeout_at(t0),
            "already-open circuit must not log a second trip"
        );
        circuit.on_h2_open_success();
        assert!(!circuit.prefers_http1_at(t0));
        assert_eq!(circuit.consecutive_timeouts, 0);
        assert!(circuit.open_since.is_none());
    }

    #[test]
    fn process_h2_circuit_stays_on_http1_until_h2_success() {
        let mut circuit = ProcessH2Circuit::default();
        let t0 = Instant::now();
        assert!(circuit.on_h2_open_timeout_at(t0));
        assert!(
            circuit.prefers_http1_at(t0 + TRANSPORT_BREAKER_COOLDOWN),
            "time-based H2 probes after 30s put a user Run on H2 and 409 after 20s"
        );
        assert!(
            circuit.prefers_http1_at(t0 + Duration::from_secs(15 * 60)),
            "HTTP/1 pin must last until an H2 open actually succeeds"
        );
        assert!(
            !circuit.on_h2_open_timeout_at(t0 + TRANSPORT_BREAKER_COOLDOWN),
            "already-open circuit must not log another trip"
        );
        circuit.on_h2_open_success();
        assert!(!circuit.prefers_http1_at(t0 + TRANSPORT_BREAKER_COOLDOWN));
    }

    #[test]
    fn process_h2_circuit_trips_on_first_midstream_reset() {
        let mut circuit = ProcessH2Circuit::default();
        let t0 = Instant::now();
        assert!(
            circuit.on_h2_stream_reset_at(t0),
            "gemini-style H2 INTERNAL_ERROR must pin HTTP/1 on the next first open"
        );
        assert!(circuit.prefers_http1_at(t0));
        assert!(
            !circuit.on_h2_stream_reset_at(t0),
            "already-open circuit must not log a second trip"
        );
        assert!(
            circuit.prefers_http1_at(t0 + TRANSPORT_BREAKER_COOLDOWN),
            "mid-stream RST must not probe H2 on the next user open after 30s"
        );
        circuit.on_h2_open_success();
        assert!(!circuit.prefers_http1_at(t0 + TRANSPORT_BREAKER_COOLDOWN));
    }

    #[test]
    fn live_open_prefers_http1_from_env_or_circuit() {
        assert!(!live_open_prefers_http1_from(false, false));
        assert!(live_open_prefers_http1_from(true, false));
        assert!(live_open_prefers_http1_from(false, true));
    }

    #[test]
    fn live_open_max_defaults_to_grok_cli_parallelism() {
        assert_eq!(
            live_open_concurrency_max(None),
            128,
            "grok-cli can fan out to 128; do not require CCP_CURSOR_LIVE_OPEN_CONCURRENCY"
        );
        assert_eq!(live_open_concurrency_max(Some("16")), 16);
        assert_eq!(live_open_concurrency_max(Some("1")), 1);
        assert_eq!(live_open_concurrency_max(Some("0")), 128);
        assert_eq!(live_open_concurrency_max(Some("999")), 128);
        assert_eq!(live_open_concurrency_max(Some("nope")), 128);
    }

    #[test]
    fn live_generation_max_defaults_to_safe_parallelism() {
        assert_eq!(live_generation_concurrency_max(None), 16);
        assert_eq!(live_generation_concurrency_max(Some("8")), 8);
        assert_eq!(live_generation_concurrency_max(Some("1")), 1);
        assert_eq!(live_generation_concurrency_max(Some("0")), 16);
        assert_eq!(live_generation_concurrency_max(Some("999")), 128);
        assert_eq!(live_generation_concurrency_max(Some("nope")), 16);
    }

    #[test]
    fn live_generation_queue_timeout_is_decoupled_from_run_lifetime() {
        assert_eq!(live_generation_queue_secs(None), 60);
        assert_eq!(live_generation_queue_secs(Some("15")), 15);
        assert_eq!(live_generation_queue_secs(Some("0")), 60);
        assert_eq!(live_generation_queue_secs(Some("9999")), 3600);
        assert_eq!(live_generation_queue_secs(Some("nope")), 60);
    }

    #[test]
    fn live_generation_saturation_is_retryable_without_ambiguous_acceptance() {
        let err = live_generation_saturated_error();
        assert_eq!(err.status, 429);
        assert!(!is_ambiguous_live_open_timeout(&err));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn queued_resume_overtakes_queued_new_generation() {
        let gate = LiveGenerationGate::new(1);
        let held = gate
            .try_acquire(LiveGenerationPriority::Start)
            .expect("hold the generation slot");

        let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
        let (admitted_tx, mut admitted_rx) = mpsc::unbounded_channel();
        let start_ready = ready_tx.clone();
        let start_admitted = admitted_tx.clone();
        let start_gate = gate.clone();
        let queued_start = tokio::spawn(async move {
            start_ready.send("start").expect("announce queued start");
            let _permit = start_gate
                .acquire(LiveGenerationPriority::Start, None, Duration::from_secs(1))
                .await
                .expect("queued start permit");
            start_admitted
                .send("start")
                .expect("announce admitted start");
        });
        assert_eq!(ready_rx.recv().await, Some("start"));
        tokio::task::yield_now().await;

        let resume_ready = ready_tx.clone();
        let resume_admitted = admitted_tx.clone();
        let resume_gate = gate.clone();
        let queued_resume = tokio::spawn(async move {
            resume_ready.send("resume").expect("announce queued resume");
            let _permit = resume_gate
                .acquire(LiveGenerationPriority::Resume, None, Duration::from_secs(1))
                .await
                .expect("queued resume permit");
            resume_admitted
                .send("resume")
                .expect("announce admitted resume");
        });
        assert_eq!(ready_rx.recv().await, Some("resume"));
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.resume_waiters() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resume waiter should register before capacity is released");

        drop(held);
        let first = tokio::time::timeout(Duration::from_secs(1), admitted_rx.recv())
            .await
            .expect("one queued generation should be admitted");
        assert_eq!(
            first,
            Some("resume"),
            "tool-result continuation must not sit behind unrelated new starts"
        );

        queued_start.await.expect("queued start task");
        queued_resume.await.expect("queued resume task");
    }

    #[test]
    fn live_open_limit_grows_to_128_and_shrinks_to_soft_start() {
        let mut n = live_open_soft_start(128);
        assert_eq!(
            n, 4,
            "first wave stays small so H2 handshakes do not stampede"
        );
        while n < 128 {
            let next = live_open_grow(n, 128);
            assert!(next > n, "must grow from {n}");
            n = next;
        }
        assert_eq!(n, 128);
        assert_eq!(live_open_grow(128, 128), 128);
        assert_eq!(
            live_open_grow(100, 16),
            16,
            "optional env still caps the max"
        );

        n = live_open_shrink(128, 4);
        assert_eq!(n, 64);
        while n > 4 {
            n = live_open_shrink(n, 4);
        }
        assert_eq!(n, 4);
        assert_eq!(live_open_shrink(4, 4), 4);
    }

    #[tokio::test]
    async fn adaptive_live_open_waits_instead_of_instant_429() {
        let gate = AdaptiveLiveOpenGate::new(4);
        let mut held = Vec::new();
        for i in 0..4 {
            held.push(
                gate.try_acquire()
                    .unwrap_or_else(|| panic!("soft-start slot {i}")),
            );
        }
        let started = Instant::now();
        let err = gate
            .acquire(Duration::from_millis(80))
            .await
            .expect_err("full gate must 429 after waiting");
        assert_eq!(err.status, 429);
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "must queue behind in-flight opens, not fail immediately: {:?}",
            started.elapsed()
        );
        drop(held);
    }

    #[tokio::test]
    async fn adaptive_live_open_grows_so_a_burst_does_not_need_an_env_var() {
        let gate = AdaptiveLiveOpenGate::new(128);
        assert_eq!(gate.limit(), 4);
        gate.on_success();
        assert_eq!(gate.limit(), 8);
        gate.on_success();
        assert_eq!(gate.limit(), 16);
        let mut held = Vec::new();
        for i in 0..16 {
            held.push(
                gate.try_acquire()
                    .unwrap_or_else(|| panic!("admit {i} after growth")),
            );
        }
        assert!(
            gate.try_acquire().is_none(),
            "limit 16 must still bound a stampede"
        );
        drop(held);
        gate.on_failure();
        assert_eq!(gate.limit(), 8, "open timeouts shrink the window");
    }

    #[test]
    fn live_open_saturation_is_429_so_grok_retries() {
        let err = live_open_saturated_error();
        assert_eq!(
            err.status, 429,
            "saturation never sent a Run; grok-build must retry 429, not treat 409 as invalid_request"
        );
        assert!(!is_ambiguous_live_open_timeout(&err));
        assert!(!crate::retry::is_ambiguous_live_accept(&err.message));
        assert_eq!(
            crate::retry::classify_proxy_error_status(err.status, &err.message),
            429
        );
        assert!(
            crate::retry::should_retry_status(err.status),
            "grok-build retries 429; a fifth concurrent open must not fail closed"
        );
        assert!(
            !cursor_start_error_is_same_request_retryable(&err),
            "saturation never sent a Run; do not replay it inside the same POST"
        );
        assert!(
            !live_start_error_seals_tombstone(&err),
            "saturation must not tombstone the live slot"
        );
    }

    #[test]
    fn live_reconnect_log_includes_trigger() {
        let fields = live_reconnect_log_fields(
            &LiveReconnectOutcome::Reconnected,
            2,
            10,
            false,
            "stream error received",
        );
        assert_eq!(
            fields.get("trigger").and_then(|v| v.as_str()),
            Some("stream error received")
        );
        assert_eq!(fields.get("outcome").and_then(|v| v.as_str()), Some("ok"));
    }

    #[test]
    fn live_stream_request_failed_log_is_proxy_log_shaped() {
        let fields = live_stream_request_failed_log_fields(
            "req-1",
            409,
            "Cursor live open timed out after 20s",
        );
        assert_eq!(fields.get("reqId").and_then(|v| v.as_str()), Some("req-1"));
        assert_eq!(fields.get("status").and_then(|v| v.as_u64()), Some(409));
        assert_eq!(
            fields.get("path").and_then(|v| v.as_str()),
            Some("live_sse")
        );
    }

    #[test]
    fn recovery_episode_caps_opens_and_wall_clock() {
        let now = Instant::now();
        let mut episode = LiveRecoveryEpisode::default();
        episode.begin(now);
        episode.opens = LIVE_RECOVERY_MAX_OPENS;
        assert_eq!(
            episode.skip_reason(now, false),
            Some("recovery open budget exhausted")
        );
        episode.opens = 0;
        episode.started = Some(now - LIVE_RECOVERY_DEADLINE);
        assert_eq!(
            episode.skip_reason(now, false),
            Some("recovery deadline exhausted")
        );
        assert!(
            episode.skip_reason(now, true).is_none(),
            "HTTP/1 ResumeAction after H2 RST must still have budget at the 45s H2 cap"
        );
        episode.started = Some(now);
        assert!(episode.skip_reason(now, false).is_none());
        episode.started = Some(now - live_h1_open_attempt_timeout());
        assert_eq!(
            episode.skip_reason(now, true),
            Some("recovery deadline exhausted")
        );
    }

    #[test]
    fn delayed_hollow_eof_keeps_hollow_flag_across_resume_attempts() {
        let mut episode = LiveRecoveryEpisode {
            on_probation: true,
            ..LiveRecoveryEpisode::default()
        };
        episode.note_delayed_hollow_if_probation();
        assert!(episode.last_was_hollow);
        assert!(!episode.on_probation);
        assert_eq!(
            live_reconnect_backoff_ms_for(8, episode.last_was_hollow, u64::MAX),
            0
        );
    }

    #[test]
    fn h1_fallback_is_skipped_when_http1_was_rejected() {
        assert!(live_reconnect_allow_h1_fallback(false, false));
        assert!(
            !live_reconnect_allow_h1_fallback(false, true),
            "464/421 must not retry HTTP/1 on the same incident"
        );
        assert!(!live_reconnect_allow_h1_fallback(true, false));
    }

    #[test]
    fn transport_breaker_opens_after_repeated_failures_then_half_opens() {
        let now = Instant::now();
        let mut breaker = TransportBreaker::default();
        breaker.on_failure(now);
        breaker.on_failure(now);
        assert!(breaker.allows(now), "two failures stay closed");
        breaker.on_failure(now);
        assert!(!breaker.allows(now), "third failure opens the breaker");
        assert!(breaker.allows(now + TRANSPORT_BREAKER_COOLDOWN));
        breaker.on_success();
        assert!(breaker.allows(now));
        assert_eq!(breaker.consecutive_fails, 0);
    }

    #[test]
    fn tool_turn_stalls_after_double_stream_idle_without_text() {
        let setup = Duration::from_secs(45);
        let idle = Duration::from_secs(120);
        let fresh = Duration::from_millis(200);
        assert_eq!(
            live_idle_stall_message(false, false, true, true, setup, setup, setup, idle),
            Some("Cursor stream produced no useful progress"),
            "a stream with no frames at all still dies at setup_idle"
        );
        assert!(
            live_idle_stall_message(false, false, true, true, setup, fresh, setup, idle).is_none(),
            "heartbeat-only Fable thinking must not die at 45s setup_idle"
        );
        assert_eq!(
            live_idle_stall_message(false, false, true, true, idle * 2, fresh, setup, idle),
            Some("Cursor stream produced no useful progress"),
            "heartbeat-only thinking still stalls at 2× stream idle"
        );
        assert!(
            live_idle_stall_message(true, false, true, true, idle, fresh, setup, idle).is_none(),
            "tools advertised: 120s of thinking-only is still allowed"
        );
        assert_eq!(
            live_idle_stall_message(true, false, true, true, idle * 2, fresh, setup, idle),
            Some("Cursor stream stalled after partial progress")
        );
        assert_eq!(
            live_idle_stall_message(true, true, true, true, idle * 2, fresh, setup, idle),
            Some("Cursor stream stalled after partial progress"),
            "heartbeat-only silence after text must ResumeAction or error, not wait 1800s"
        );
        assert!(
            live_idle_stall_message(true, true, true, true, idle, fresh, setup, idle).is_none(),
            "one stream-idle window of quiet thinking after text is still allowed"
        );
        assert!(
            live_idle_stall_message(true, false, true, false, idle * 2, fresh, setup, idle)
                .is_none(),
            "do not stall while Claude still owes native tool_results"
        );
    }

    #[test]
    fn hollow_h2_reconnect_forces_http1() {
        let rst = "error decoding response body: error reading a body from connection: stream error received: unexpected internal error encountered";
        assert!(
            live_reconnect_should_force_http1(true, 0, false, false, Some(rst)),
            "H2 INTERNAL_ERROR must switch to real HTTP/1.1 immediately — ResumeAction on H2 is always hollow"
        );
        assert!(
            !live_reconnect_should_force_http1(true, 0, false, false, Some("upstream ended")),
            "clean EOF after progress may retry H2 on a fresh client"
        );
        assert!(live_reconnect_should_force_http1(
            false, 1, false, false, None
        ));
        assert!(!live_reconnect_should_force_http1(
            false,
            1,
            true,
            false,
            Some(rst)
        ));
        assert!(!live_reconnect_should_force_http1(
            true, 3, false, false, None
        ));
        assert!(
            !live_reconnect_should_force_http1(true, 0, false, true, Some(rst)),
            "464-rejected HTTP/1 must not be forced back, or the next loop GiveUps remaining H2"
        );
    }

    #[test]
    fn h2_internal_error_is_a_stream_reset() {
        let msg = "error decoding response body: error reading a body from connection: stream error received: unexpected internal error encountered";
        assert!(is_h2_stream_reset(msg));
        assert!(
            is_h2_stream_reset(
                "error decoding response body: error reading a body from connection: stream closed because of a broken pipe"
            ),
            "Clash/H2 broken pipe must force HTTP/1 ResumeAction, same as INTERNAL_ERROR"
        );
        assert!(!is_h2_stream_reset("Connect error 429: quota"));
    }

    fn decoded_frames(bytes: &[u8]) -> Vec<ConnectFrame> {
        ConnectFrameDecoder::new().push(bytes).unwrap()
    }

    #[test]
    fn heartbeat_frames_do_not_reset_reconnect_budget() {
        let frames = decoded_frames(&super::super::test_frames::heartbeat_frame());
        assert!(
            !live_reconnect_should_reset_budget(&frames),
            "server heartbeats must not replenish ResumeAction attempts"
        );
        assert!(!live_reconnect_should_reset_budget(&[]));
        assert!(!live_reconnect_should_reset_budget(&[ConnectFrame {
            flags: 0,
            payload: Bytes::new(),
        }]));
        let partial = vec![0x00, 0x00, 0x00];
        assert!(
            !live_reconnect_should_reset_budget(&decoded_frames(&partial)),
            "incomplete Connect bytes must not reset the budget"
        );
    }

    #[test]
    fn text_frames_reset_reconnect_budget() {
        let frames = decoded_frames(&super::super::test_frames::text_frame("hello"));
        assert!(live_reconnect_should_reset_budget(&frames));
    }

    #[test]
    fn reconnect_client_follows_force_flag_not_env() {
        assert!(
            reconnect_prefers_http1(true),
            "H2 INTERNAL_ERROR must pin http1_only()"
        );
        assert!(
            !reconnect_prefers_http1(false),
            "464 flip-back must rebuild H2 even when CCP_CURSOR_HTTP1=1"
        );
    }

    #[test]
    fn h1_464_after_h2_hollow_gives_up_instead_of_oscillating() {
        let clash = CursorError::new(464, "incompatible version", None);
        assert_eq!(
            live_reconnect_on_open_error(true, false, &clash),
            LiveReconnectTransportAction::FlipToH2
        );
        assert_eq!(
            live_reconnect_on_open_error(true, true, &clash),
            LiveReconnectTransportAction::GiveUp(
                "HTTP/1 rejected (464) after HTTP/2 already failed"
            )
        );
        assert_eq!(
            live_reconnect_on_open_error(false, false, &clash),
            LiveReconnectTransportAction::ForceHttp1
        );
        let timeout = CursorError::new(504, "Cursor live open timed out after 10s", None);
        assert_eq!(
            live_reconnect_on_open_error(false, false, &timeout),
            LiveReconnectTransportAction::GiveUp("response-less ResumeAction send is ambiguous"),
            "timed-out ResumeAction send must not open another transport"
        );
        assert_eq!(
            live_reconnect_on_hollow_body(true, true),
            LiveReconnectTransportAction::GiveUp(
                "HTTP 200 resume had no body; another ResumeAction would duplicate it"
            )
        );
        assert_eq!(
            live_reconnect_on_hollow_body(false, false),
            LiveReconnectTransportAction::GiveUp(
                "HTTP 200 resume had no body; another ResumeAction would duplicate it"
            )
        );
        assert_eq!(
            live_reconnect_on_hollow_body(false, true),
            LiveReconnectTransportAction::GiveUp(
                "HTTP 200 resume had no body; another ResumeAction would duplicate it"
            )
        );
        let h2_reset = CursorError::new(
            0,
            "stream error received: unexpected internal error encountered",
            None,
        );
        assert_eq!(
            live_reconnect_on_open_error(false, true, &h2_reset),
            LiveReconnectTransportAction::GiveUp("response-less ResumeAction send is ambiguous"),
            "response-less H2 reset after send is an ambiguous accept"
        );
    }

    #[test]
    fn missing_image_connect_end_clears_poisoned_checkpoint() {
        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        super::super::conversation::reset_for_test();
        super::super::conversation::save_checkpoint("sess-img-missing", vec![0x08, 0x01]);
        assert!(
            super::super::conversation::continuation_for(Some("sess-img-missing")).has_checkpoint
        );
        let text = annotate_connect_end_error(
            "sess-img-missing",
            ConnectEndError {
                status: 502,
                code: "internal".into(),
                message: "Image not found".into(),
                detail: String::new(),
            },
            None,
        );
        assert!(
            text.contains("checkpoint cleared"),
            "user-facing error must say the poisoned checkpoint was dropped: {text}"
        );
        assert!(
            !super::super::conversation::continuation_for(Some("sess-img-missing")).has_checkpoint
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn missing_conversation_connect_end_rotates_binding_without_teardown_repoisoning() {
        let _registry = lock_live_registry_for_test();
        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        LiveRunRegistry::clear();
        super::super::conversation::reset_for_test();
        let session_id = "sess-conversation-missing";
        let original =
            super::super::conversation::continuation_for(Some(session_id)).conversation_id;
        super::super::conversation::save_checkpoint(session_id, vec![0x08, 0x01]);
        let stale_blobs = HashMap::from([(vec![0xaa], vec![0xbb])]);
        super::super::conversation::merge_blobs(session_id, &stale_blobs);

        let (request_tx, _request_rx) = mpsc::channel(1);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let client_only = PendingCursorExec {
            id: 7,
            exec_id: Some("workflow-7".into()),
            tool_use_id: "workflow-tool-7".into(),
            claude_name: "Workflow".into(),
            claude_input: serde_json::json!({"name":"deep-research"}),
            kind: CursorExecKind::ClientOnly,
        };
        assert!(pending.queue(client_only, Duration::ZERO));
        let exposed = pending.expose();
        let pending_shared = Arc::new(Mutex::new(exposed));
        let mut kv_blobs = stale_blobs;
        let mut latest_checkpoint = Some(vec![0x08, 0x01]);
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let mut xml_parser = CursorToolUseXmlParser::new(None);
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id,
            user_prompt: "continue",
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };
        let frame = ConnectFrame {
            flags: FLAG_END,
            payload: Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "error": {
                        "code": "internal",
                        "message": "ERROR_CUSTOM_MESSAGE: Conversation data missing – This conversation’s data is missing and can’t be restored. Start a new chat to continue. (14 missing blobs: abc)",
                        "details": [{
                            "debug": {
                                "error": "ERROR_CUSTOM_MESSAGE",
                                "details": {
                                    "title": "Internal error",
                                    "detail": "Unable to resume"
                                }
                            }
                        }]
                    }
                }))
                .unwrap(),
            ),
        };

        assert!(
            !process_live_frame(
                frame,
                &outbound,
                &mut sink,
                &mut deferred,
                &mut pending,
                &pending_shared,
                &mut kv_blobs,
                &mut latest_checkpoint,
                &terminal_error,
                None,
                &mut saw_text,
                &mut useful,
                &mut logical,
                &mut last_progress,
                Duration::from_millis(50),
                &mut xml_parser,
                Some(&mut turn),
            )
            .await
        );

        // Mirror driver teardown: stale local state must not recreate the
        // invalid binding after the Connect error reset it.
        if let Some(checkpoint) = latest_checkpoint.take() {
            super::super::conversation::save_checkpoint(session_id, checkpoint);
        }
        super::super::conversation::merge_blobs(session_id, &kv_blobs);

        let error = event_rx.recv().await.unwrap().unwrap_err();
        assert!(error.contains("conversation reset"), "{error}");
        let recovered = super::super::conversation::continuation_for(Some(session_id));
        assert_ne!(recovered.conversation_id, original);
        assert!(!recovered.has_checkpoint);
        assert!(recovered.pre_fetched_blobs.is_empty());

        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "missing-conversation-run".into(),
            command_tx,
            pending: pending_shared,
            terminal_error,
            completed: Arc::new(AtomicBool::new(true)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        LiveRunRegistry::reserve(session_id)
            .expect("fresh registry slot")
            .insert(handle)
            .expect("insert failed run");
        assert!(
            matches!(
                LiveRunRegistry::probe_run(session_id, None),
                LiveRunProbe::Free
            ),
            "the first retry must start fresh instead of replaying the terminal error"
        );
    }

    #[test]
    fn resource_exhausted_terminal_error_frees_the_live_slot() {
        let _registry = lock_live_registry_for_test();
        let session = format!("quota-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "quota-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(Some(TerminalOutcome {
                message: "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]".into(),
                created_at: Instant::now(),
            }))),
            completed: Arc::new(AtomicBool::new(true)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert quota failure");
        assert!(
            matches!(
                LiveRunRegistry::probe_run(&session, None),
                LiveRunProbe::Free
            ),
            "the next grok turn must not replay a consumed 429 as 502"
        );
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn trailing_missing_conversation_end_overrides_prior_chunk_turn_ended() {
        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        super::super::conversation::reset_for_test();
        let session_id = "sess-trailing-conversation-missing";
        let original =
            super::super::conversation::continuation_for(Some(session_id)).conversation_id;
        let checkpoint = vec![0x08, 0x01];
        let stale_blobs = HashMap::from([(vec![0xaa], vec![0xbb])]);
        super::super::conversation::save_checkpoint(session_id, checkpoint.clone());
        super::super::conversation::merge_blobs(session_id, &stale_blobs);

        let (upstream_tx, upstream_rx) = mpsc::channel(2);
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _events) = mpsc::channel(8);
        let terminal_error = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicBool::new(false));
        let mut reconnect = test_reconnect_context();
        reconnect.conversation_id = original.clone();
        reconnect.opening_checkpoint = Some(checkpoint);
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            tokio::spawn(std::future::pending::<()>()),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            event_tx,
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&terminal_error),
            Arc::clone(&completed),
            Arc::new(AtomicBool::new(false)),
            None,
            session_id.into(),
            "trailing-end-run".into(),
            stale_blobs,
            "continue".into(),
            RequestContext::default(),
            reconnect,
            test_generation_permit(),
        ));

        let mut turn_payload = Vec::new();
        proto::AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                turn_ended: Some(TurnEnded {
                    output_tokens: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut turn_payload)
        .expect("encode turn_ended");
        let turn_ended = encode_connect_frame(turn_payload, 0);
        let missing_end = encode_connect_frame(
            serde_json::to_vec(&serde_json::json!({
                "error": {
                    "code": "internal",
                    "message": "ERROR_CUSTOM_MESSAGE: Internal error",
                    "details": [{
                        "debug": {
                            "error": "ERROR_CUSTOM_MESSAGE",
                            "details": {
                                "title": "Internal error",
                                "detail": "Conversation data missing (14 missing blobs: abc)"
                            }
                        }
                    }]
                }
            }))
            .unwrap(),
            FLAG_END,
        );
        upstream_tx
            .send(Ok(Some(turn_ended)))
            .await
            .expect("send turn_ended chunk");
        tokio::time::sleep(Duration::from_millis(10)).await;
        upstream_tx
            .send(Ok(Some(missing_end)))
            .await
            .expect("send trailing END chunk");
        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("driver timeout")
            .expect("driver");

        let error = terminal_error
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|outcome| outcome.message.clone())
            .expect("trailing END must be terminal");
        assert!(error.contains("conversation reset"), "{error}");
        let recovered = super::super::conversation::continuation_for(Some(session_id));
        assert_ne!(recovered.conversation_id, original);
        assert!(!recovered.has_checkpoint);
        assert!(recovered.pre_fetched_blobs.is_empty());
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn trailing_missing_conversation_end_overrides_prior_client_only_chunk() {
        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        super::super::conversation::reset_for_test();
        let session_id = "sess-client-only-conversation-missing";
        let original =
            super::super::conversation::continuation_for(Some(session_id)).conversation_id;
        let checkpoint = vec![0x08, 0x01];
        let stale_blobs = HashMap::from([(vec![0xaa], vec![0xbb])]);
        super::super::conversation::save_checkpoint(session_id, checkpoint.clone());
        super::super::conversation::merge_blobs(session_id, &stale_blobs);

        let (upstream_tx, upstream_rx) = mpsc::channel(2);
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _events) = mpsc::channel(8);
        let terminal_error = Arc::new(Mutex::new(None));
        let mut reconnect = test_reconnect_context();
        reconnect.conversation_id = original.clone();
        reconnect.opening_checkpoint = Some(checkpoint);
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            tokio::spawn(std::future::pending::<()>()),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            event_tx,
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&terminal_error),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            Some(BTreeSet::from(["Workflow".into()])),
            session_id.into(),
            "client-only-end-run".into(),
            stale_blobs,
            "continue".into(),
            RequestContext::default(),
            reconnect,
            test_generation_permit(),
        ));

        let mut workflow_payload = Vec::new();
        proto::AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(proto::ToolCallStarted {
                    call_id: "workflow-call".into(),
                    model_call_id: "model-call".into(),
                    tool_call: Some(proto::ToolCall {
                        mcp_tool_call: Some(proto::McpToolCall {
                            args: Some(proto::McpArgs {
                                name: "Workflow".into(),
                                tool_name: "Workflow".into(),
                                tool_call_id: "workflow-call".into(),
                                provider_identifier: "claude-local".into(),
                                args: HashMap::from([(
                                    "name".into(),
                                    br#""deep-research""#.to_vec(),
                                )]),
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut workflow_payload)
        .expect("encode Workflow");
        upstream_tx
            .send(Ok(Some(encode_connect_frame(workflow_payload, 0))))
            .await
            .expect("send Workflow chunk");
        tokio::time::sleep(Duration::from_millis(10)).await;
        let missing_end = encode_connect_frame(
            serde_json::to_vec(&serde_json::json!({
                "error": {
                    "code": "internal",
                    "message": "ERROR_CUSTOM_MESSAGE: Internal error",
                    "details": [{
                        "debug": {
                            "error": "ERROR_CUSTOM_MESSAGE",
                            "details": {
                                "title": "Internal error",
                                "detail": "Conversation data missing (14 missing blobs: abc)"
                            }
                        }
                    }]
                }
            }))
            .unwrap(),
            FLAG_END,
        );
        upstream_tx
            .send(Ok(Some(missing_end)))
            .await
            .expect("send trailing END chunk");
        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("driver timeout")
            .expect("driver");

        let error = terminal_error
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|outcome| outcome.message.clone())
            .expect("trailing END must be terminal");
        assert!(error.contains("conversation reset"), "{error}");
        let recovered = super::super::conversation::continuation_for(Some(session_id));
        assert_ne!(recovered.conversation_id, original);
        assert!(!recovered.has_checkpoint);
        assert!(recovered.pre_fetched_blobs.is_empty());
    }

    #[tokio::test]
    async fn transport_error_after_held_turn_end_fails_ambiguous_without_reconnect() {
        let (upstream_tx, upstream_rx) = mpsc::channel(2);
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _events) = mpsc::channel(8);
        let terminal_error = Arc::new(Mutex::new(None));
        let mut reconnect = test_reconnect_context();
        reconnect.conversation_id = None;
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            tokio::spawn(std::future::pending::<()>()),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            event_tx,
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&terminal_error),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            None,
            "held-error-session".into(),
            "held-error-run".into(),
            HashMap::new(),
            "continue".into(),
            RequestContext::default(),
            reconnect,
            test_generation_permit(),
        ));
        let mut turn_payload = Vec::new();
        proto::AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                turn_ended: Some(TurnEnded {
                    output_tokens: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut turn_payload)
        .expect("encode turn_ended");
        upstream_tx
            .send(Ok(Some(encode_connect_frame(turn_payload, 0))))
            .await
            .expect("send turn_ended");
        tokio::time::sleep(Duration::from_millis(10)).await;
        upstream_tx
            .send(Err("broken pipe".into()))
            .await
            .expect("send transport error");
        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("driver timeout")
            .expect("driver");

        let error = terminal_error
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|outcome| outcome.message.clone())
            .expect("held terminal transport error");
        assert!(error.contains("ambiguous"), "{error}");
    }

    #[tokio::test]
    async fn partial_end_frame_at_eof_cannot_commit_held_turn_end() {
        let (upstream_tx, upstream_rx) = mpsc::channel(2);
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, _events) = mpsc::channel(8);
        let terminal_error = Arc::new(Mutex::new(None));
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            tokio::spawn(std::future::pending::<()>()),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            event_tx,
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&terminal_error),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
            None,
            "partial-end-session".into(),
            "partial-end-run".into(),
            HashMap::new(),
            "continue".into(),
            RequestContext::default(),
            test_reconnect_context(),
            test_generation_permit(),
        ));
        let mut turn_payload = Vec::new();
        proto::AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                turn_ended: Some(TurnEnded {
                    output_tokens: Some(1),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut turn_payload)
        .expect("encode turn_ended");
        let turn_frame = encode_connect_frame(turn_payload, 0);
        let missing_end = encode_connect_frame(
            serde_json::to_vec(&serde_json::json!({
                "error": {
                    "code": "internal",
                    "message": "Conversation data missing (1 missing blob: abc)"
                }
            }))
            .unwrap(),
            FLAG_END,
        );
        let mut chunk = Vec::from(turn_frame.as_ref());
        chunk.extend_from_slice(&missing_end[..8]);
        upstream_tx
            .send(Ok(Some(Bytes::from(chunk))))
            .await
            .expect("send turn plus partial END");
        upstream_tx
            .send(Ok(None))
            .await
            .expect("send EOF after partial END");
        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("driver timeout")
            .expect("driver");

        let error = terminal_error
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|outcome| outcome.message.clone())
            .expect("partial terminal frame must fail closed");
        assert!(error.contains("ambiguous"), "{error}");
    }

    #[test]
    fn reconnect_open_timeout_matches_transport_not_a_flat_10s() {
        assert_eq!(
            live_reconnect_open_timeout(Duration::from_secs(1800), false),
            LIVE_H2_OPEN_ATTEMPT,
            "H2 ResumeAction must get the same budget as first H2 open"
        );
        assert_eq!(
            live_reconnect_open_timeout(Duration::from_secs(1800), true),
            LIVE_H1_OPEN_ATTEMPT,
            "HTTP/1 ResumeAction after H2 INTERNAL_ERROR must not die at 10s"
        );
        assert_eq!(
            live_recovery_budget(true),
            live_h1_open_attempt_timeout(),
            "H2 INTERNAL_ERROR recovery must keep the full HTTP/1 open budget"
        );
        assert_eq!(
            live_reconnect_open_timeout(live_recovery_budget(true), true),
            live_h1_open_attempt_timeout(),
            "HTTP/1 ResumeAction after H2 RST must not die at the 45s H2 episode cap"
        );
        assert_eq!(
            live_reconnect_open_timeout(LIVE_RECOVERY_DEADLINE, true),
            LIVE_RECOVERY_DEADLINE,
            "an already-short remaining window still bounds a fresh HTTP/1 resume"
        );
        assert_eq!(
            live_reconnect_open_timeout(Duration::from_secs(5), false),
            Duration::from_secs(5)
        );
        assert_eq!(
            live_reconnect_open_timeout(Duration::from_secs(5), true),
            Duration::from_secs(5)
        );
        assert_eq!(
            live_reconnect_open_timeout(Duration::ZERO, true),
            Duration::ZERO
        );
    }

    #[tokio::test]
    async fn silent_resume_open_is_accepted_without_waiting_seconds() {
        let stream = futures_util::stream::pending::<Result<Bytes, reqwest::Error>>();
        let started = Instant::now();
        let result = take_immediate_resume_chunk(stream).await;
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "quiet Fable thinking must not sit behind a 3s first-byte gate"
        );
        let (prefix, _) = result.expect("silent open is healthy");
        assert!(prefix.is_none());
    }

    #[tokio::test]
    async fn immediate_resume_eof_is_hollow() {
        let stream = futures_util::stream::empty::<Result<Bytes, reqwest::Error>>();
        let err = take_immediate_resume_chunk(stream)
            .await
            .expect_err("HTTP 200 then immediate EOF is hollow");
        assert!(err.contains("ended before the first byte"), "{err}");
    }

    #[tokio::test]
    async fn fence_live_upstream_drops_stale_pump_eof() {
        let (old_tx, old_rx) = mpsc::channel::<Result<Option<Bytes>, String>>(8);
        let mut upstream = old_rx;
        let mut upstream_tx = old_tx.clone();
        let new_tx = fence_live_upstream(&mut upstream, &mut upstream_tx);
        let _ = old_tx.send(Ok(None)).await;
        new_tx
            .send(Ok(Some(Bytes::from_static(b"fresh"))))
            .await
            .unwrap();
        let got = upstream.recv().await;
        assert_eq!(got, Some(Ok(Some(Bytes::from_static(b"fresh")))));
    }

    #[tokio::test]
    async fn control_close_keeps_natives_when_send_fails() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let outbound = ClientOutbound::Bidi(tx);
        let mut pending = PendingExecState::default();
        pending.queue(pending_exec(2, "read-2"), Duration::from_millis(50));
        assert_eq!(pending.collecting.len(), 1);
        let ok = control_close_collecting_natives(&mut pending, &outbound).await;
        assert!(
            matches!(ok, Ok(false)),
            "transport send fail is Ok(false), not terminal: {ok:?}"
        );
        assert_eq!(
            pending.collecting.len(),
            1,
            "failed close must not drain collecting natives"
        );
        assert_eq!(pending.collecting[0].tool_use_id, "read-2");
    }

    #[test]
    fn drain_collecting_natives_leaves_awaiting_intact() {
        let mut pending = PendingExecState::default();
        pending.queue(pending_exec(1, "read-1"), Duration::ZERO);
        let _ = pending.expose();
        pending.queue(pending_exec(2, "read-2"), Duration::from_millis(50));
        let closed = pending.drain_collecting_natives();
        assert_eq!(closed.len(), 1);
        assert_eq!(closed[0].tool_use_id, "read-2");
        assert_eq!(pending.awaiting().len(), 1);
        assert_eq!(pending.awaiting()[0].tool_use_id, "read-1");
        assert!(pending.collecting.is_empty());
    }

    #[test]
    fn format_error_chain_appends_io_kind() {
        let err = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "unexpected EOF");
        let text = format_error_chain(&err);
        assert!(text.contains("unexpected EOF"), "{text}");
    }

    #[test]
    fn format_error_chain_joins_unique_sources() {
        #[derive(Debug)]
        struct Src(&'static str);
        impl std::fmt::Display for Src {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::error::Error for Src {}

        #[derive(Debug)]
        struct Wrap {
            msg: &'static str,
            src: Src,
        }
        impl std::fmt::Display for Wrap {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.msg)
            }
        }
        impl std::error::Error for Wrap {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.src)
            }
        }

        let err = Wrap {
            msg: "error decoding response body",
            src: Src("connection reset"),
        };
        assert_eq!(
            format_error_chain(&err),
            "error decoding response body: connection reset"
        );
    }

    #[test]
    fn abrupt_eof_without_turn_ended_is_an_error() {
        assert!(abrupt_eof_should_error(true));
        assert!(abrupt_eof_should_error(false));
    }

    #[test]
    fn connect_errors_map_to_anthropic_error_types() {
        assert_eq!(
            anthropic_error_type_from_live_error("Connect error 429: quota [resource_exhausted]"),
            "rate_limit_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error("Connect error 401: no [unauthenticated]"),
            "authentication_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error("Connect error 403: no [permission_denied]"),
            "permission_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error("Cursor error 429: BidiAppend failed"),
            "rate_limit_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error("Cursor stream stalled"),
            "api_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error(
                "Connect error 502: This model is not available in your country or region [internal]"
            ),
            "permission_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error("Connect error 502: 不支持的国家/区域 [internal]"),
            "permission_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error(
                "Connect error 502: model slug is not supported [invalid_argument]"
            ),
            "invalid_request_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error("Cursor upstream HTTP 403"),
            "permission_error"
        );
        assert_eq!(
            anthropic_error_type_from_live_error("Cursor RunSSE HTTP 429"),
            "rate_limit_error"
        );
    }

    #[test]
    fn start_error_classifies_http_body_detail_not_just_message() {
        let invoice = CursorError::new(
            502,
            "Cursor upstream HTTP 502",
            Some(
                "You have an unpaid invoice — Visit cursor.com/dashboard and pay your invoice"
                    .into(),
            ),
        );
        assert!(
            !cursor_start_error_is_same_request_retryable(&invoice),
            "billing text in the HTTP body must fail closed"
        );

        let geo = CursorError::new(
            403,
            "Cursor upstream HTTP 403",
            Some("This model is not available in your country or region".into()),
        );
        assert!(!cursor_start_error_is_same_request_retryable(&geo));

        let missing = CursorError::new(
            400,
            "Cursor upstream HTTP 400",
            Some("Conversation data missing [failed_precondition]".into()),
        );
        assert!(
            cursor_start_error_is_same_request_retryable(&missing),
            "missing conversation in a 4xx body must still same-request retry"
        );

        let open_timeout = CursorError::new(504, "Cursor live open timed out after 20s", None);
        assert!(
            !cursor_start_error_is_same_request_retryable(&open_timeout),
            "response-less live open must seal, not replay Run with a new request id"
        );
        assert!(live_start_error_seals_tombstone(&open_timeout));

        let connect = CursorError::new(502, "Cursor upstream connect failed", None);
        assert!(
            cursor_start_error_is_same_request_retryable(&connect),
            "a refused connect must retry inside this POST before returning 409"
        );
    }

    #[test]
    fn initial_bidiappend_timeout_retries_inside_post_then_409() {
        let initial = ambiguous_http1_append_error(
            CursorError::new(408, "BidiAppend timed out", None),
            "initial Run",
        );
        assert_eq!(
            initial.message,
            "Cursor BidiAppend initial Run failed; acceptance is ambiguous: BidiAppend timed out"
        );
        assert!(
            cursor_start_error_is_same_request_retryable(&initial),
            "first-open BidiAppend timeout must retry this POST before 409"
        );
        assert!(
            live_start_error_seals_tombstone(&initial),
            "exhausted retries still seal Starting so the next POST is not a duplicate"
        );
        let exhausted = exhausted_live_start_error(initial.clone(), 3);
        assert_eq!(
            exhausted.status, 409,
            "after proxy-internal retries grok-build must see 409, not 408"
        );
        assert_eq!(exhausted.message, initial.message);

        let mid_stream = ambiguous_http1_append_error(
            CursorError::new(408, "BidiAppend timed out", None),
            "send",
        );
        assert!(
            !cursor_start_error_is_same_request_retryable(&mid_stream),
            "mid-stream BidiAppend timeout must not start a second Run"
        );
        assert!(live_send_failure_is_terminal(&mid_stream));

        let send_wrapper = CursorError::new(
            504,
            "Cursor BidiAppend timed out; acceptance is ambiguous",
            None,
        );
        assert!(
            !cursor_start_error_is_same_request_retryable(&send_wrapper),
            "a timed-out in-flight send must not be replayed as a new Run"
        );
    }

    #[test]
    fn kv_set_then_get_round_trips_the_latest_blob() {
        let key = b"conversation-state".to_vec();
        let mut blobs = HashMap::new();
        let set = KvServerMessage {
            id: 70,
            get_blob_args: None,
            set_blob_args: Some(proto::SetBlobArgs {
                blob_id: key.clone(),
                blob_data: b"state-before".to_vec(),
            }),
            span_context: None,
        };
        let set_frame = encode_kv_reply(&set, &mut blobs).unwrap().unwrap();
        let decoded = super::super::client::decode_upstream_frames(&set_frame).unwrap();
        let set_reply = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        assert_eq!(set_reply.kv_client_message.as_ref().unwrap().id, 70);
        assert!(
            set_reply
                .kv_client_message
                .unwrap()
                .set_blob_result
                .unwrap()
                .error
                .is_none()
        );

        let overwrite = KvServerMessage {
            id: 71,
            get_blob_args: None,
            set_blob_args: Some(proto::SetBlobArgs {
                blob_id: key.clone(),
                blob_data: b"state-after".to_vec(),
            }),
            span_context: None,
        };
        encode_kv_reply(&overwrite, &mut blobs).unwrap();

        let get = KvServerMessage {
            id: 72,
            get_blob_args: Some(proto::GetBlobArgs { blob_id: key }),
            set_blob_args: None,
            span_context: None,
        };
        let get_frame = encode_kv_reply(&get, &mut blobs).unwrap().unwrap();
        let decoded = super::super::client::decode_upstream_frames(&get_frame).unwrap();
        let get_reply = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        assert_eq!(
            get_reply
                .kv_client_message
                .unwrap()
                .get_blob_result
                .unwrap()
                .blob_data
                .as_deref(),
            Some(b"state-after".as_slice())
        );
    }

    #[test]
    fn request_context_reply_closes_the_exec_stream() {
        let exec = ExecServerMessage {
            id: 99,
            exec_id: Some("context-99".into()),
            request_context_args: Some(RequestContextArgs::default()),
            ..Default::default()
        };
        let context = RequestContext {
            env: Some(RequestContextEnv {
                process_working_directory: "/tmp/work".into(),
                project_folder: "/tmp/work".into(),
                workspace_paths: vec!["/tmp/work".into()],
                ..Default::default()
            }),
            git_repos: vec![GitRepoInfo {
                path: "/tmp/work".into(),
                branch_name: "main".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let frames = encode_request_context_reply(&exec, &context).unwrap();
        let decoded = super::super::client::decode_upstream_frames(&frames).unwrap();
        assert_eq!(decoded.len(), 2);

        let result = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        let filled = result
            .exec_client_message
            .unwrap()
            .request_context_result
            .unwrap()
            .success
            .unwrap()
            .request_context
            .unwrap();
        let env = filled.env.expect("env");
        assert_eq!(env.process_working_directory, "/tmp/work");
        assert_eq!(env.project_folder, "/tmp/work");
        assert_eq!(env.workspace_paths, vec!["/tmp/work".to_string()]);
        assert_eq!(filled.git_repos.len(), 1);
        assert_eq!(filled.git_repos[0].branch_name, "main");
        assert!(
            filled.agent_skills.is_empty(),
            "do not invent agent_skills from the Claude system prompt"
        );
        let close = AgentClientMessage::decode(decoded[1].payload.as_ref()).unwrap();
        assert_eq!(
            close
                .exec_client_control_message
                .unwrap()
                .stream_close
                .unwrap()
                .id,
            99
        );
    }

    #[test]
    fn pending_exec_state_batches_parallel_tools_in_arrival_order() {
        let mut state = PendingExecState::default();
        assert!(state.queue(pending_exec(1, "tool-1"), Duration::from_millis(10)));
        assert!(state.queue(pending_exec(2, "tool-2"), Duration::from_millis(10)));
        assert!(!state.queue(pending_exec(2, "tool-2"), Duration::from_millis(10)));
        assert!(state.can_expose());

        let exposed = state.expose();
        assert_eq!(
            exposed
                .iter()
                .map(|exec| exec.tool_use_id.as_str())
                .collect::<Vec<_>>(),
            ["tool-1", "tool-2"]
        );
        assert_eq!(state.awaiting().len(), 2);
        assert!(!state.can_expose());
    }

    #[test]
    fn xml_lifecycle_does_not_trip_mcp_sibling_flush() {
        let mut xml_state = PendingExecState::default();
        xml_state.queue(
            PendingCursorExec {
                id: 1,
                exec_id: Some("client_only_spawn-40".into()),
                tool_use_id: "spawn-40".into(),
                claude_name: "spawn_subagent".into(),
                claude_input: serde_json::json!({"prompt": "a"}),
                kind: CursorExecKind::ClientOnly,
            },
            Duration::ZERO,
        );
        assert!(
            !xml_state.collecting_has_lifecycle(),
            "XML spawn_subagent must wait for turn_ended, not the MCP sibling quiet window"
        );

        let mut mcp_state = PendingExecState::default();
        mcp_state.queue(
            PendingCursorExec {
                id: 2,
                exec_id: Some("mcp_spawn-a".into()),
                tool_use_id: "spawn-a".into(),
                claude_name: "spawn_subagent".into(),
                claude_input: serde_json::json!({"prompt": "b"}),
                kind: CursorExecKind::ClientOnly,
            },
            Duration::ZERO,
        );
        assert!(
            mcp_state.collecting_has_lifecycle(),
            "MCP sibling spawn must still flush on the quiet window"
        );
    }

    #[test]
    fn pending_exec_state_preserves_late_exec_for_the_next_segment() {
        let mut state = PendingExecState::default();
        state.queue(pending_exec(1, "tool-1"), Duration::from_millis(10));
        state.expose();
        state.queue(pending_exec(2, "tool-2"), Duration::from_millis(10));

        assert!(!state.can_expose());
        assert_eq!(state.awaiting()[0].tool_use_id, "tool-1");
        state.complete_awaiting();
        assert!(state.can_expose());
        assert_eq!(state.expose()[0].tool_use_id, "tool-2");
    }

    #[test]
    fn pending_exec_state_keeps_completed_exec_tombstones() {
        let mut state = PendingExecState::default();
        let exec = pending_exec(1, "tool-1");
        assert!(state.queue(exec.clone(), Duration::from_millis(10)));
        state.expose();
        state.complete_awaiting();
        assert!(state.is_empty());
        assert!(!state.queue(exec, Duration::from_millis(10)));
        assert!(state.is_empty());
    }

    #[test]
    fn pending_exec_state_disambiguates_colliding_downstream_tool_ids() {
        let mut state = PendingExecState::default();
        assert!(state.queue(pending_exec(1, "shared-id"), Duration::from_millis(10)));
        assert!(state.queue(pending_exec(2, "shared-id"), Duration::from_millis(10)));

        let exposed = state.expose();
        assert_eq!(exposed[0].tool_use_id, "shared-id");
        assert_eq!(exposed[1].tool_use_id, "shared-id__cursor_2");
    }

    #[test]
    fn pending_tool_ids_are_bound_to_the_live_run_generation() {
        let mut old = PendingExecState::for_run("old-generation");
        let mut replacement = PendingExecState::for_run("new-generation");
        assert!(old.queue(pending_exec(1, "recycled-id"), Duration::ZERO));
        assert!(replacement.queue(pending_exec(1, "recycled-id"), Duration::ZERO));

        let old_exposed = old.expose();
        let replacement_exposed = replacement.expose();
        let old_id = &old_exposed[0].tool_use_id;
        let replacement_id = &replacement_exposed[0].tool_use_id;
        assert_ne!(
            old_id, replacement_id,
            "a stale result id must not match a replacement Run's pending exec"
        );
    }

    #[test]
    fn pending_tool_ids_collapse_newlines_before_generation_suffix() {
        let mut state = PendingExecState::for_run("gen-1");
        assert!(state.queue(
            pending_exec(
                1,
                "call-72ee1731-4917-4d55-96f6-89841af2f48f-3\nfc_owTHooM-2dTqGa-65a125c0",
            ),
            Duration::ZERO
        ));
        assert_eq!(
            state.expose()[0].tool_use_id,
            "call-72ee1731-4917-4d55-96f6-89841af2f48f-3_fc_owTHooM-2dTqGa-65a125c0__cursor_run_gen-1"
        );
    }

    fn pending_client_only(id: u32, tool_use_id: &str) -> PendingCursorExec {
        PendingCursorExec {
            id,
            exec_id: Some(format!("client_only_{tool_use_id}")),
            tool_use_id: tool_use_id.to_string(),
            claude_name: "Workflow".into(),
            claude_input: serde_json::json!({"name": "deep-research"}),
            kind: CursorExecKind::ClientOnly,
        }
    }

    #[test]
    fn client_only_pending_must_supersede_instead_of_resume() {
        let client = pending_client_only(1, "spawn-1");
        let native = pending_exec(2, "read-1");
        assert!(
            live_pending_must_supersede(std::slice::from_ref(&client)),
            "ClientOnly spawn_subagent/Workflow must start a fresh run"
        );
        assert!(
            !live_pending_must_supersede(std::slice::from_ref(&native)),
            "native Read/Bash still resume the same BiDi"
        );
        assert!(
            !live_pending_must_supersede(&[client, native]),
            "a mixed batch is not a ClientOnly teardown"
        );
        assert!(!live_pending_must_supersede(&[]));
    }

    #[test]
    fn dead_driver_resume_is_supersede_not_502() {
        assert!(live_resume_error_is_dead_driver(&CursorError::internal(
            "Cursor live resume acknowledgement dropped"
        )));
        assert!(live_resume_error_is_dead_driver(&CursorError::internal(
            "Cursor live run already closed"
        )));
        assert!(!live_resume_error_is_dead_driver(&CursorError::new(
            400,
            "Cursor tool result id x is not pending",
            None
        )));
        assert!(
            !live_resume_error_is_dead_driver(&CursorError::new(
                429,
                "Cursor live resume dispatch timed out before driver acceptance; retry this tool result",
                None
            )),
            "a late driver is still alive; 429 must retry the same tool result, not supersede"
        );
    }

    #[test]
    fn resume_dispatch_timeout_is_retryable_rate_limit() {
        if std::env::var("CCP_CURSOR_LIVE_RESUME_DISPATCH_MS").is_err() {
            assert_eq!(
                resume_dispatch_timeout(),
                Duration::from_millis(DEFAULT_RESUME_DISPATCH_MS)
            );
        }
        let err = resume_dispatch_retryable_error(
            "Cursor live resume dispatch timed out before driver acceptance; retry this tool result",
        );
        assert_eq!(err.status, 429);
        assert_eq!(
            crate::retry::classify_proxy_error_status(err.status, &err.message),
            429
        );
        assert_eq!(
            crate::retry::anthropic_error_kind_for_status(err.status, &err.message),
            "rate_limit_error"
        );
        assert!(!crate::retry::is_ambiguous_live_accept(&err.message));
    }

    fn dummy_handle(run_id: &str) -> Arc<CursorLiveRunHandle> {
        let (command_tx, _command_rx) = mpsc::channel(1);
        Arc::new(CursorLiveRunHandle {
            run_id: run_id.into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        })
    }

    fn lock_live_registry_for_test() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: Mutex<()> = Mutex::new(());
        LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn pending_exec_state_exposes_client_only_without_flushing_native() {
        let mut state = PendingExecState::default();
        assert!(state.queue(pending_exec(1, "read-1"), Duration::from_millis(50)));
        assert!(state.queue(pending_client_only(2, "wf-1"), Duration::ZERO));
        let exposed = state.expose();
        assert_eq!(exposed.len(), 1);
        assert_eq!(exposed[0].tool_use_id, "wf-1");
        assert!(matches!(exposed[0].kind, CursorExecKind::ClientOnly));
        assert_eq!(state.awaiting().len(), 1);
        assert_eq!(state.awaiting()[0].tool_use_id, "wf-1");
        assert!(
            state.all().any(|exec| exec.tool_use_id == "read-1"),
            "native Read must remain collecting"
        );
        assert!(
            !state
                .awaiting()
                .iter()
                .any(|exec| exec.tool_use_id == "read-1")
        );
    }

    #[tokio::test]
    async fn expose_mixed_batch_emits_only_client_only_and_ends_bidi() {
        let mut pending = PendingExecState::default();
        pending.queue(pending_exec(1, "read-1"), Duration::from_millis(50));
        pending.queue(pending_client_only(2, "wf-1"), Duration::ZERO);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let keep_bidi = expose_collected_tools(&mut pending, &pending_shared, &mut sink).await;
        assert!(
            !keep_bidi,
            "mixed ClientOnly+native must end BiDi so the next POST includes Workflow/Skill results"
        );
        let event = event_rx.recv().await.expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "Workflow");
                assert_eq!(tools[0].tool_use_id, "wf-1");
            }
            other => panic!("expected NativeToolBatch, got {other:?}"),
        }
        let shared = pending_shared.lock().unwrap();
        assert!(
            shared
                .iter()
                .all(|exec| matches!(exec.kind, CursorExecKind::ClientOnly))
        );
        assert!(pending.all().any(|exec| exec.tool_use_id == "read-1"));
    }

    #[test]
    fn starting_reservation_is_occupied_without_a_handle() {
        let _registry = lock_live_registry_for_test();
        let session = format!("starting-session-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        assert!(
            LiveRunRegistry::get(&session).is_none(),
            "Starting has no runnable handle"
        );
        assert!(
            LiveRunRegistry::is_occupied(&session),
            "Starting must look occupied to concurrent POSTs"
        );
        reservation.release();
        assert!(!LiveRunRegistry::is_occupied(&session));
        LiveRunRegistry::clear();
    }

    #[test]
    fn dropping_an_uncommitted_reservation_seals_ambiguous() {
        let _registry = lock_live_registry_for_test();
        let session = format!("drop-seal-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        drop(reservation);
        assert!(
            LiveRunRegistry::is_ambiguous_run(&session, None),
            "dropping a Starting reservation must not free the slot"
        );
        assert!(matches!(
            LiveRunRegistry::try_claim_run(&session, None),
            LiveSlotClaim::Ambiguous
        ));
        LiveRunRegistry::clear();
    }

    #[test]
    fn success_tombstone_blocks_identical_retry_but_allows_a_new_prompt() {
        let _registry = lock_live_registry_for_test();
        let session = format!("success-tombstone-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("ok-run");
        handle.set_request_fingerprint(42);
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert");
        LiveRunRegistry::seal_success_if(&session, "ok-run");
        assert!(LiveRunRegistry::is_occupied(&session));
        LiveRunRegistry::release_success_if_new_request(&session, None, 42);
        assert!(
            LiveRunRegistry::is_occupied(&session),
            "retrying the same request must not start another Run"
        );
        LiveRunRegistry::release_success_if_new_request(&session, None, 99);
        assert!(
            !LiveRunRegistry::is_occupied(&session),
            "a new prompt may start a fresh Run after success"
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn insert_of_already_completed_success_seals_fingerprint_tombstone() {
        let _registry = lock_live_registry_for_test();
        let session = format!("fast-success-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("fast-run");
        handle.set_request_fingerprint(7);
        handle.completed.store(true, Ordering::Release);
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert");
        assert!(
            LiveRunRegistry::is_occupied(&session),
            "a Run that finished before insert must not be pruned as a free slot"
        );
        LiveRunRegistry::release_success_if_new_request(&session, None, 7);
        assert!(
            LiveRunRegistry::is_occupied(&session),
            "retrying the same request after a fast success must not start another Run"
        );
        LiveRunRegistry::release_success_if_new_request(&session, None, 8);
        assert!(
            !LiveRunRegistry::is_occupied(&session),
            "a new prompt may start a fresh Run after fast success"
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn supersede_does_not_replace_a_starting_open() {
        let _registry = lock_live_registry_for_test();
        let session = format!("starting-supersede-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        assert!(LiveRunRegistry::is_starting_run(&session, None));
        assert!(
            LiveRunRegistry::supersede(&session).is_none(),
            "aborting Starting and opening another Run duplicates a maybe-accepted request"
        );
        assert!(LiveRunRegistry::is_occupied(&session));
        drop(reservation);
        LiveRunRegistry::clear();
    }

    #[test]
    fn http_open_missing_conversation_resets_binding() {
        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        super::super::conversation::reset_for_test();
        let session_id = "sess-open-conversation-missing";
        let original =
            super::super::conversation::continuation_for(Some(session_id)).conversation_id;
        super::super::conversation::save_checkpoint(session_id, vec![0x08, 0x01]);
        let annotated = annotate_live_cursor_error(
            session_id,
            CursorError::new(
                400,
                "Cursor RunSSE HTTP 400",
                Some("Conversation data missing (1 missing blob: abc)".into()),
            ),
        );
        assert!(
            annotated.message.contains("conversation reset"),
            "{}",
            annotated.message
        );
        let recovered = super::super::conversation::continuation_for(Some(session_id));
        assert_ne!(recovered.conversation_id, original);
        assert!(!recovered.has_checkpoint);
    }

    #[test]
    fn try_claim_classifies_starting_without_a_second_lock() {
        let _registry = lock_live_registry_for_test();
        let session = format!("try-claim-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        assert!(matches!(
            LiveRunRegistry::try_claim_run(&session, None),
            LiveSlotClaim::Starting
        ));
        assert!(matches!(
            LiveRunRegistry::conflict_action(&session, None),
            LiveConflictAction::Http409
        ));
        drop(reservation);
        LiveRunRegistry::clear();
    }

    #[test]
    fn try_claim_classifies_ambiguous_without_reserving() {
        let _registry = lock_live_registry_for_test();
        let session = format!("try-claim-amb-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        reservation.seal_ambiguous(Instant::now() + Duration::from_secs(60));
        assert!(matches!(
            LiveRunRegistry::try_claim_run(&session, None),
            LiveSlotClaim::Ambiguous
        ));
        assert!(matches!(
            LiveRunRegistry::conflict_action(&session, None),
            LiveConflictAction::Http409
        ));
        LiveRunRegistry::clear();
    }

    #[test]
    fn insert_adopts_a_vacant_slot_after_starting_was_removed() {
        let _registry = lock_live_registry_for_test();
        let session = format!("insert-adopt-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        let handle = dummy_handle("adopted-run");
        assert!(LiveRunRegistry::cancel(&session));
        reservation
            .insert(Arc::clone(&handle))
            .expect("accepted Run must occupy a vacant slot instead of starting another");
        assert!(LiveRunRegistry::get(&session).is_some());
        LiveRunRegistry::clear();
    }

    #[test]
    fn insert_does_not_overwrite_an_ambiguous_tombstone() {
        let _registry = lock_live_registry_for_test();
        let session = format!("insert-tombstone-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        reservation.seal_ambiguous(Instant::now() + Duration::from_secs(60));
        let (cancel, _) = watch::channel(false);
        let stale = LiveRunReservation {
            session_id: session.clone(),
            reservation_id: "stale".into(),
            committed: false,
            seal_on_drop: false,
            cancel,
        };
        assert!(
            stale.insert(dummy_handle("must-not-overwrite")).is_err(),
            "adopt-if-vacant must not clobber an Ambiguous tombstone"
        );
        assert!(LiveRunRegistry::is_ambiguous_run(&session, None));
        LiveRunRegistry::clear();
    }

    #[test]
    fn try_claim_classifies_running_without_reserving() {
        let _registry = lock_live_registry_for_test();
        let session = format!("try-claim-run-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        reservation
            .insert(dummy_handle("already-running"))
            .expect("insert running");
        assert!(matches!(
            LiveRunRegistry::try_claim_run(&session, None),
            LiveSlotClaim::Running
        ));
        assert!(LiveRunRegistry::get(&session).is_some());
        LiveRunRegistry::clear();
    }

    #[test]
    fn unbound_try_claim_never_supersedes_a_running_generation() {
        let _registry = lock_live_registry_for_test();
        let session = format!("try-claim-supersede-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        reservation
            .insert(dummy_handle("to-supersede"))
            .expect("insert running");
        assert!(
            matches!(
                LiveRunRegistry::try_claim_run(&session, None),
                LiveSlotClaim::Running
            ),
            "an unbound caller must 409 rather than cancel whichever Run is current"
        );
        assert_eq!(
            LiveRunRegistry::get(&session)
                .expect("running generation remains")
                .run_id(),
            "to-supersede"
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn generation_bound_replacement_reserves_the_matching_running_slot_atomically() {
        let _registry = lock_live_registry_for_test();
        let session = format!("conflict-running-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        reservation
            .insert(dummy_handle("conflict-target"))
            .expect("insert running");
        let LiveReplacementClaim::Reserved {
            reservation,
            superseded: Some(superseded),
        } = LiveRunRegistry::claim_replacement_for_run(&session, None, "conflict-target")
        else {
            panic!("the matching generation should be claimed");
        };
        assert_eq!(superseded.run_id(), "conflict-target");
        assert!(
            LiveRunRegistry::is_starting_run(&session, None),
            "the replacement must reserve atomically without exposing a free gap"
        );
        superseded.cancel();
        reservation.release();
        assert!(!LiveRunRegistry::is_occupied(&session));
        LiveRunRegistry::clear();
    }

    #[test]
    fn generic_conflict_does_not_cancel_an_unbound_running_generation() {
        let _registry = lock_live_registry_for_test();
        let session = format!("stale-conflict-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(dummy_handle("newer-generation"))
            .expect("insert newer run");

        assert!(
            matches!(
                LiveRunRegistry::conflict_action(&session, None),
                LiveConflictAction::Http409
            ),
            "a generic/stale conflict must not cancel whichever Run is current"
        );
        assert!(matches!(
            LiveRunRegistry::claim_replacement_for_run(&session, None, "older-generation"),
            LiveReplacementClaim::Conflict
        ));
        assert!(LiveRunRegistry::get(&session).is_some());
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn tool_result_waiter_does_not_attach_to_a_replacement_run() {
        let _registry = lock_live_registry_for_test();
        let session = format!("result-generation-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        LiveRunRegistry::reserve(&session)
            .expect("reserve old")
            .insert(dummy_handle("old-generation"))
            .expect("insert old");
        let body: crate::anthropic::schema::MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model": "claude-fable-5",
                "stream": true,
                "messages": [{"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "recycled-id",
                    "content": "belongs to old generation"
                }]}]
            }))
            .unwrap();

        let replacement = async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            let LiveReplacementClaim::Reserved {
                reservation,
                superseded: Some(superseded),
            } = LiveRunRegistry::claim_replacement_for_run(&session, None, "old-generation")
            else {
                panic!("replace old running slot");
            };
            superseded.cancel();
            let replacement = dummy_handle("new-generation");
            replacement
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(pending_exec(2, "recycled-id"));
            reservation.insert(replacement).expect("insert replacement");
        };
        let wait = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_old_result".into(),
            "claude-fable-5".into(),
            1,
            None,
            true,
        );
        let (outcome, ()) = tokio::join!(wait, replacement);

        assert!(
            matches!(outcome, super::super::LiveResumeOutcome::Conflict),
            "an old result waiter must 409 when the registry generation changes"
        );
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn tool_result_waiter_does_not_leave_http_silent_for_thirty_seconds() {
        let _registry = lock_live_registry_for_test();
        let session = format!("bounded-result-wait-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(dummy_handle("bounded-wait-generation"))
            .expect("insert running handle");
        let body: crate::anthropic::schema::MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model": "claude-fable-5",
                "stream": true,
                "messages": [{"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "stale-tool-result",
                    "content": "done"
                }]}]
            }))
            .unwrap();

        let outcome = tokio::time::timeout(
            Duration::from_secs(6),
            super::super::await_live_run_resume(
                &session,
                None,
                &body,
                "msg_bounded_wait".into(),
                "claude-fable-5".into(),
                1,
                None,
                true,
            ),
        )
        .await
        .expect("tool-result classification must finish before downstream stream-idle");

        assert!(
            matches!(
                outcome,
                super::super::LiveResumeOutcome::SupersedeRunning(ref run_id)
                    if run_id == "bounded-wait-generation"
            ),
            "stale tool_result against an empty pending batch must take over, not 409"
        );
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn partial_tool_result_stays_invalid_if_the_running_slot_disappears() {
        let _registry = lock_live_registry_for_test();
        let session = format!("partial-disappears-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("partial-generation");
        {
            let mut pending = handle
                .pending
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            pending.push(pending_exec(1, "tool-a"));
            pending.push(pending_exec(2, "tool-b"));
        }
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert partial run");
        let body: crate::anthropic::schema::MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model": "claude-fable-5",
                "stream": true,
                "messages": [{"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "tool-a",
                    "content": "only one result"
                }]}]
            }))
            .unwrap();

        let remove = async {
            tokio::time::sleep(Duration::from_millis(40)).await;
            assert!(LiveRunRegistry::cancel(&session));
        };
        let wait = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_partial".into(),
            "claude-fable-5".into(),
            1,
            None,
            true,
        );
        let (outcome, ()) = tokio::join!(wait, remove);

        match outcome {
            super::super::LiveResumeOutcome::SupersedeRunning(run_id) => {
                assert_eq!(run_id, "partial-generation");
            }
            _ => panic!("a partial current batch must supersede the observed generation"),
        }
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn fresh_request_supersedes_abandoned_pending_generation_without_a_400() {
        let _registry = lock_live_registry_for_test();
        let session = format!("abandoned-pending-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let completed = Arc::new(AtomicBool::new(false));
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "abandoned-generation".into(),
            command_tx,
            pending: Arc::new(Mutex::new(vec![pending_exec(1, "abandoned-tool")])),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::clone(&completed),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert abandoned run");
        let body: crate::anthropic::schema::MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model": "claude-fable-5",
                "stream": true,
                "messages": [{"role": "user", "content": "continue with new work"}]
            }))
            .unwrap();

        let outcome = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_fresh".into(),
            "claude-fable-5".into(),
            1,
            None,
            true,
        )
        .await;
        let super::super::LiveResumeOutcome::SupersedeRunning(run_id) = outcome else {
            panic!("fresh request must supersede, not report missing tool results");
        };
        assert_eq!(run_id, "abandoned-generation");

        let LiveReplacementClaim::Reserved {
            reservation,
            superseded: Some(superseded),
        } = LiveRunRegistry::claim_replacement_for_run(&session, None, &run_id)
        else {
            panic!("observed generation must be atomically claimed");
        };
        let driver = tokio::spawn(async move {
            let Some(RunCommand::Cancel { ack: Some(ack) }) = command_rx.recv().await else {
                panic!("replacement must request acknowledged cancellation");
            };
            completed.store(true, Ordering::Release);
            let _ = ack.send(());
        });
        superseded
            .cancel_and_wait()
            .await
            .expect("old generation cancellation");
        driver.await.expect("mock old driver");
        assert!(LiveRunRegistry::is_starting_run(&session, None));
        drop(reservation);
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn fresh_request_supersedes_cancel_requested_generation_instead_of_409() {
        let _registry = lock_live_registry_for_test();
        let session = format!("dying-compact-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("dying-compact-generation");
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(Arc::clone(&handle))
            .expect("insert dying run");
        handle.cancel();
        assert!(
            LiveRunRegistry::get_run(&session, None).is_none(),
            "get_run hides a cancel-requested handle so compact's next POST must not depend on it"
        );
        assert!(
            matches!(
                LiveRunRegistry::probe_run(&session, None),
                LiveRunProbe::Occupied
            ),
            "the dying handle still occupies the slot until teardown finishes"
        );
        let body: crate::anthropic::schema::MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model": "claude-fable-5",
                "stream": true,
                "messages": [{"role": "user", "content": "post-compact turn"}]
            }))
            .unwrap();

        let outcome = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_after_compact".into(),
            "claude-fable-5".into(),
            1,
            None,
            true,
        )
        .await;
        let super::super::LiveResumeOutcome::SupersedeRunning(run_id) = outcome else {
            panic!("a cancel-requested occupant must be superseded, not 409");
        };
        assert_eq!(run_id, "dying-compact-generation");

        let LiveReplacementClaim::Reserved {
            reservation,
            superseded: Some(superseded),
        } = LiveRunRegistry::claim_replacement_for_run(&session, None, &run_id)
        else {
            panic!("the observed dying generation must be claimable");
        };
        assert_eq!(superseded.run_id(), "dying-compact-generation");
        assert!(LiveRunRegistry::is_starting_run(&session, None));
        reservation.release();
        LiveRunRegistry::clear();
    }

    #[test]
    fn claim_replacement_cancel_requested_is_generation_bound() {
        let _registry = lock_live_registry_for_test();
        let session = format!("dying-bound-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("dying-generation");
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(Arc::clone(&handle))
            .expect("insert dying run");
        handle.cancel();

        assert!(
            matches!(
                LiveRunRegistry::claim_replacement_for_run(&session, None, "other-generation"),
                LiveReplacementClaim::Conflict
            ),
            "a stale waiter must not take over a cancel-requested generation it did not observe"
        );
        assert!(
            LiveRunRegistry::is_occupied(&session),
            "a mismatched claim must leave the dying generation in place"
        );

        let LiveReplacementClaim::Reserved {
            reservation,
            superseded: Some(superseded),
        } = LiveRunRegistry::claim_replacement_for_run(&session, None, "dying-generation")
        else {
            panic!("the matching cancel-requested generation must be replaceable");
        };
        assert_eq!(superseded.run_id(), "dying-generation");
        assert!(LiveRunRegistry::is_starting_run(&session, None));
        reservation.release();
        LiveRunRegistry::clear();
    }

    #[test]
    fn fresh_request_keeps_reservation_when_cancel_is_ambiguous() {
        let _registry = lock_live_registry_for_test();
        let session = format!("fresh-amb-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("amb-generation");
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(Arc::clone(&handle))
            .expect("insert");
        let LiveReplacementClaim::Reserved {
            mut reservation,
            superseded: Some(superseded),
        } = LiveRunRegistry::claim_replacement_for_run(&session, None, "amb-generation")
        else {
            panic!("matching generation must be claimable");
        };
        reservation.protect_on_drop();
        let error = CursorError::new(
            409,
            "Cursor live run ended in an ambiguous upstream state; replacement blocked: acceptance is ambiguous",
            None,
        );
        let kept = finish_replacement_after_cancel(reservation, superseded, false, Err(error))
            .expect("a fresh turn must keep the Starting reservation");
        assert!(
            LiveRunRegistry::is_starting_run(&session, None),
            "re-inserting the dying handle would 409 grok-build's next turn"
        );
        assert!(LiveRunRegistry::running_generation(&session, None).is_none());
        kept.release();
        LiveRunRegistry::clear();
    }

    #[test]
    fn tool_result_request_keeps_reservation_when_cancel_is_ambiguous() {
        let _registry = lock_live_registry_for_test();
        let session = format!("tool-amb-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("amb-tool-generation");
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(Arc::clone(&handle))
            .expect("insert");
        let LiveReplacementClaim::Reserved {
            mut reservation,
            superseded: Some(superseded),
        } = LiveRunRegistry::claim_replacement_for_run(&session, None, "amb-tool-generation")
        else {
            panic!("matching generation must be claimable");
        };
        reservation.protect_on_drop();
        let error = CursorError::new(
            409,
            "Cursor live run ended in an ambiguous upstream state; replacement blocked: acceptance is ambiguous",
            None,
        );
        let kept = finish_replacement_after_cancel(reservation, superseded, true, Err(error))
            .expect("a superseded tool-result turn must keep the Starting reservation");
        assert!(
            LiveRunRegistry::is_starting_run(&session, None),
            "re-inserting the dying handle 409s grok-build after compact/tool batches"
        );
        assert!(LiveRunRegistry::running_generation(&session, None).is_none());
        kept.release();
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn fresh_request_clears_ambiguous_tombstone_instead_of_409() {
        let _registry = lock_live_registry_for_test();
        let session = format!("amb-tombstone-compact-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        reservation.seal_ambiguous(Instant::now() + Duration::from_secs(60));
        assert!(LiveRunRegistry::is_ambiguous_run(&session, None));
        assert!(
            LiveRunRegistry::reserve(&session).is_none(),
            "untouched Ambiguous must still block reserve / same-request open"
        );

        let body: crate::anthropic::schema::MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model": "claude-fable-5",
                "stream": true,
                "messages": [{"role": "user", "content": "post-compact turn"}]
            }))
            .unwrap();
        let outcome = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_tombstone".into(),
            "claude-fable-5".into(),
            1,
            None,
            true,
        )
        .await;
        assert!(
            matches!(outcome, super::super::LiveResumeOutcome::Free),
            "compact's next POST must clear the Ambiguous tombstone, not 409 after 1.5s"
        );
        assert!(
            !LiveRunRegistry::is_ambiguous_run(&session, None),
            "the next turn must remove the tombstone so start_live can claim"
        );
        assert!(
            !LiveRunRegistry::is_occupied(&session),
            "cleared tombstone must look Free to try_claim_run"
        );
        LiveRunRegistry::clear();
    }

    fn compact_turn_body() -> crate::anthropic::schema::MessagesRequest {
        serde_json::from_value(serde_json::json!({
            "model": "cursor-grok-4.6-xhigh-fast",
            "stream": true,
            "messages": [{"role": "user", "content": "Context 100% full. Compact the conversation."}]
        }))
        .unwrap()
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn compact_wait_does_not_409_when_running_seals_success_mid_wait() {
        let _registry = lock_live_registry_for_test();
        let session = format!("compact-seal-mid-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("compact-seal-generation");
        handle.set_request_fingerprint(1);
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(Arc::clone(&handle))
            .expect("insert running");
        let body = compact_turn_body();
        let sealer = async {
            tokio::time::sleep(Duration::from_millis(80)).await;
            handle.completed.store(true, Ordering::Release);
            LiveRunRegistry::seal_success_if(&session, "compact-seal-generation");
        };
        let wait = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_compact_seal".into(),
            "cursor-grok-4.6-xhigh-fast".into(),
            1,
            None,
            true,
        );
        let (outcome, ()) = tokio::join!(wait, sealer);
        assert!(
            !matches!(outcome, super::super::LiveResumeOutcome::Conflict),
            "compact must not 409 after the prior generation seals Succeeded mid-wait"
        );
        if let super::super::LiveResumeOutcome::SupersedeRunning(run_id) = &outcome {
            assert_eq!(run_id, "compact-seal-generation");
            assert!(
                !matches!(
                    LiveRunRegistry::claim_replacement_for_run(
                        &session,
                        None,
                        "compact-seal-generation"
                    ),
                    LiveReplacementClaim::Conflict
                ),
                "the sealed generation must still be claimable"
            );
        }
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn compact_takes_same_fingerprint_succeeded_after_nested_wait() {
        let _registry = lock_live_registry_for_test();
        let session = format!("compact-same-fp-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let body = compact_turn_body();
        let fingerprint =
            live_request_fingerprint(&serde_json::to_vec(&body.messages).unwrap_or_default());
        let handle = dummy_handle("same-fp-generation");
        handle.set_request_fingerprint(fingerprint);
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert");
        LiveRunRegistry::seal_success_if(&session, "same-fp-generation");
        assert!(
            LiveRunRegistry::is_occupied(&session),
            "same-fingerprint Succeeded must still block reserve at entry"
        );
        LiveRunRegistry::release_success_if_new_request(&session, None, fingerprint);
        assert!(
            LiveRunRegistry::is_occupied(&session),
            "entry-time identical retry must keep the Succeeded tombstone"
        );

        let outcome = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_same_fp".into(),
            "cursor-grok-4.6-xhigh-fast".into(),
            1,
            None,
            true,
        )
        .await;
        assert!(
            !matches!(outcome, super::super::LiveResumeOutcome::Conflict),
            "after the nested wait, compact must take the same-fingerprint Succeeded tombstone"
        );
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn compact_claims_starting_slot_after_nested_wait() {
        let _registry = lock_live_registry_for_test();
        let session = format!("compact-starting-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        let body = compact_turn_body();
        let outcome = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_starting".into(),
            "cursor-grok-4.6-xhigh-fast".into(),
            1,
            None,
            true,
        )
        .await;
        assert!(
            !matches!(outcome, super::super::LiveResumeOutcome::Conflict),
            "compact must not 409 a Starting occupant after the nested wait"
        );
        assert!(
            !matches!(
                LiveRunRegistry::claim_replacement_for_occupied_slot(&session, None),
                LiveReplacementClaim::Conflict
            ),
            "Starting after nested wait must be claimable for a fresh compact"
        );
        drop(reservation);
        LiveRunRegistry::clear();
    }

    #[test]
    fn claim_replacement_reserves_when_observed_generation_already_succeeded() {
        let _registry = lock_live_registry_for_test();
        let session = format!("claim-succeeded-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let handle = dummy_handle("finished-generation");
        handle.set_request_fingerprint(9);
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert");
        LiveRunRegistry::seal_success_if(&session, "finished-generation");
        assert!(LiveRunRegistry::is_occupied(&session));

        let LiveReplacementClaim::Reserved {
            reservation,
            superseded,
        } = LiveRunRegistry::claim_replacement_for_run(&session, None, "finished-generation")
        else {
            panic!("an observed generation that sealed Succeeded must still be replaceable");
        };
        assert!(
            superseded.is_none(),
            "Succeeded has no live handle to cancel"
        );
        assert!(LiveRunRegistry::is_starting_run(&session, None));
        reservation.release();
        LiveRunRegistry::clear();
    }

    #[test]
    fn claim_occupied_slot_refuses_a_running_handle() {
        let _registry = lock_live_registry_for_test();
        let session = format!("claim-occupied-running-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(dummy_handle("still-running"))
            .expect("insert");
        assert!(
            matches!(
                LiveRunRegistry::claim_replacement_for_occupied_slot(&session, None),
                LiveReplacementClaim::Conflict
            ),
            "a Running occupant must be claimed by observed run id, not the Starting/Succeeded path"
        );
        assert_eq!(
            LiveRunRegistry::running_generation(&session, None).as_deref(),
            Some("still-running")
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn take_ambiguous_tombstone_does_not_abort_starting() {
        let _registry = lock_live_registry_for_test();
        let session = format!("take-starting-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        assert!(
            !LiveRunRegistry::take_ambiguous_tombstone(&session, None),
            "Starting is not a tombstone"
        );
        assert!(LiveRunRegistry::is_starting_run(&session, None));
        reservation.seal_ambiguous(Instant::now() + Duration::from_secs(60));
        assert!(LiveRunRegistry::take_ambiguous_tombstone(&session, None));
        assert!(!LiveRunRegistry::is_occupied(&session));
        assert!(!LiveRunRegistry::take_ambiguous_tombstone(&session, None));
        LiveRunRegistry::clear();
    }

    #[test]
    fn ambiguous_cancel_probe_error_does_not_block_new_run() {
        assert!(
            !live_probe_error_blocks_new_run(
                "Cursor live cancellation interrupted an operation whose completion is unresolved; acceptance is ambiguous"
            ),
            "compact's next POST must not 502 an ambiguous cancel"
        );
        assert!(!live_probe_error_blocks_new_run(
            "Cursor live run ended in an ambiguous upstream state; replacement blocked: acceptance is ambiguous"
        ));
        assert!(
            live_probe_error_blocks_new_run("Cursor live run hard timeout"),
            "a definitive non-retryable failure must still 502"
        );
        assert!(!live_probe_error_blocks_new_run(
            "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]"
        ));
    }

    #[test]
    fn advertised_name_maps_bash_to_run_terminal_command() {
        let allowed = BTreeSet::from(["run_terminal_command".to_string(), "read_file".to_string()]);
        assert_eq!(
            resolve_advertised_name("Bash", Some(&allowed)).as_deref(),
            Some("run_terminal_command")
        );
        assert_eq!(
            resolve_advertised_name("Read", Some(&allowed)).as_deref(),
            Some("read_file")
        );
        assert!(
            resolve_advertised_name("Write", Some(&allowed)).is_none(),
            "unadvertised Cursor tools must still throw, not invent a name"
        );
        assert!(
            resolve_advertised_name("Read", None).is_none(),
            "allowed=None must not invent Read/Bash/Write for grok or Claude Code"
        );
        assert!(resolve_advertised_name("Bash", None).is_none());
        assert!(resolve_advertised_name("Write", None).is_none());
        assert!(resolve_advertised_name("Grep", None).is_none());
        assert!(resolve_advertised_name("LS", None).is_none());
        let empty = BTreeSet::new();
        assert!(
            resolve_advertised_name("Read", Some(&empty)).is_none(),
            "empty allowlist is the same as missing tools"
        );
        let task_allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        assert_eq!(
            resolve_advertised_name("Task", Some(&task_allowed)).as_deref(),
            Some("spawn_subagent")
        );
        let grok = BTreeSet::from([
            "run_terminal_command".to_string(),
            "read_file".to_string(),
            "write".to_string(),
            "list_dir".to_string(),
            "todo_write".to_string(),
            "get_command_or_subagent_output".to_string(),
        ]);
        assert_eq!(
            resolve_advertised_name("Write", Some(&grok)).as_deref(),
            Some("write")
        );
        assert_eq!(
            resolve_advertised_name("LS", Some(&grok)).as_deref(),
            Some("list_dir")
        );
        assert_eq!(
            resolve_advertised_name("TodoWrite", Some(&grok)).as_deref(),
            Some("todo_write")
        );
        assert_eq!(
            resolve_advertised_name("TaskOutput", Some(&grok)).as_deref(),
            Some("get_command_or_subagent_output")
        );
        let canonical_task = BTreeSet::from(["task".to_string()]);
        assert_eq!(
            resolve_advertised_name("Task", Some(&canonical_task)).as_deref(),
            Some("task"),
            "grok-build canonical `task` must match Cursor native Task"
        );
    }

    #[test]
    fn advertised_name_prefers_claude_bash_when_both_clients_listed() {
        let both = BTreeSet::from([
            "Bash".to_string(),
            "run_terminal_command".to_string(),
            "Read".to_string(),
            "read_file".to_string(),
        ]);
        assert_eq!(
            resolve_advertised_name("Bash", Some(&both)).as_deref(),
            Some("Bash"),
            "Claude Code sessions must not emit grok run_terminal_command just because the alias exists"
        );
        assert_eq!(
            resolve_advertised_name("Read", Some(&both)).as_deref(),
            Some("Read")
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn unrelated_tool_results_supersede_abandoned_pending_generation() {
        let _registry = lock_live_registry_for_test();
        let session = format!("unrelated-pending-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "stale-generation".into(),
            command_tx,
            pending: Arc::new(Mutex::new(vec![
                pending_exec(1, "call-f38a5db0-c948-4429-890d-d1113d2c7a36-0"),
                pending_exec(2, "fc_owSziHw-6jKPYy-a2c1c5de7ba52d13_0"),
            ])),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert stale run");
        let body: crate::anthropic::schema::MessagesRequest =
            serde_json::from_value(serde_json::json!({
                "model": "cursor-grok-4.6-xhigh-fast",
                "stream": true,
                "messages": [{"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "fc_other_tool",
                    "content": "from a different turn"
                }]}]
            }))
            .unwrap();

        let outcome = super::super::await_live_run_resume(
            &session,
            None,
            &body,
            "msg_unrelated".into(),
            "cursor-grok-4.6-xhigh-fast".into(),
            1,
            None,
            true,
        )
        .await;
        assert!(
            matches!(
                outcome,
                super::super::LiveResumeOutcome::SupersedeRunning(ref run_id) if run_id == "stale-generation"
            ),
            "unrelated tool_result ids must supersede, not 400 missing tools"
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn cancel_is_visible_to_a_late_watch_subscriber() {
        let _registry = lock_live_registry_for_test();
        let session = format!("cancel-watch-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        assert!(LiveRunRegistry::cancel(&session));
        let rx = reservation.cancelled();
        assert!(
            *rx.borrow(),
            "send_replace must persist cancel even if no receiver existed yet"
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn cancel_running_only_leaves_starting_alone() {
        let _registry = lock_live_registry_for_test();
        let session = format!("cancel-running-only-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        assert!(!LiveRunRegistry::cancel_running_only(&session, None));
        assert!(LiveRunRegistry::is_starting_run(&session, None));
        drop(reservation);
        LiveRunRegistry::clear();
    }

    #[test]
    fn ambiguous_open_tombstone_blocks_a_second_run() {
        let _registry = lock_live_registry_for_test();
        let session = format!("ambiguous-tombstone-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        reservation.seal_ambiguous(Instant::now() + Duration::from_secs(60));
        assert!(LiveRunRegistry::is_ambiguous_run(&session, None));
        assert!(LiveRunRegistry::is_occupied(&session));
        assert!(
            LiveRunRegistry::reserve(&session).is_none(),
            "tombstone must not allow a duplicate Run"
        );
        assert!(LiveRunRegistry::supersede(&session).is_none());
        LiveRunRegistry::clear();
    }

    #[test]
    fn expired_ambiguous_tombstone_is_pruned() {
        let _registry = lock_live_registry_for_test();
        let session = format!("ambiguous-expired-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        reservation.seal_ambiguous(Instant::now() - Duration::from_secs(1));
        assert!(!LiveRunRegistry::is_occupied(&session));
        assert!(LiveRunRegistry::reserve(&session).is_some());
        LiveRunRegistry::clear();
    }

    #[test]
    fn ambiguous_terminal_error_leaves_a_tombstone() {
        assert!(terminal_error_is_ambiguous_accept(
            "Cursor resume produced no progress before the stream ended"
        ));
        assert!(terminal_error_is_ambiguous_accept(
            "Cursor live open timed out after 20s"
        ));
        assert!(!terminal_error_is_ambiguous_accept(
            "Cursor live run hard timeout"
        ));
    }

    #[test]
    fn nested_agent_run_does_not_supersede_parent() {
        let _registry = lock_live_registry_for_test();
        LiveRunRegistry::clear();
        let session = format!("nested-session-{}", uuid::Uuid::new_v4());
        let parent_res = LiveRunRegistry::reserve(&session).expect("parent reserve");
        let parent = dummy_handle("parent-run");
        parent_res
            .insert(Arc::clone(&parent))
            .expect("insert parent");

        let nested_res = LiveRunRegistry::reserve_run(&session, Some("agent-nested"))
            .expect("nested slot must be free while parent is running");
        assert!(
            LiveRunRegistry::get(&session).is_some(),
            "parent slot must stay occupied"
        );
        assert!(LiveRunRegistry::is_occupied_run(
            &session,
            Some("agent-nested")
        ));
        let nested = dummy_handle("nested-run");
        nested_res
            .insert(Arc::clone(&nested))
            .expect("insert nested");

        assert_eq!(LiveRunRegistry::get(&session).unwrap().run_id, "parent-run");
        assert_eq!(
            LiveRunRegistry::get_run(&session, Some("agent-nested"))
                .unwrap()
                .run_id,
            "nested-run"
        );
        assert_eq!(
            live_run_key(&session, Some("agent-nested")),
            format!("{session}::agent::agent-nested")
        );
        assert!(
            !live_run_key(&session, Some("agent-nested")).contains("::nested::"),
            "must not invent a nested session UUID"
        );

        let _ = LiveRunRegistry::supersede(&session);
        assert!(
            LiveRunRegistry::get_run(&session, Some("agent-nested")).is_some(),
            "supersede(session) must not cancel the nested agent slot"
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn live_run_identity_from_mod_headers_keys_separate_slot() {
        // Mirrors `mod.rs` `live_run_identity`: nested POSTs keep the parent
        // `X-Claude-Code-Session-Id` and add URL-encoded agent headers.
        let _registry = lock_live_registry_for_test();
        LiveRunRegistry::clear();
        let session = "parent-session";
        let parent = LiveRunIdentity::parent(session);
        let nested = LiveRunIdentity {
            session_id: session,
            agent_id: Some("agent%2Fchild"),
            parent_agent_id: Some("agent%2Fparent"),
        };
        assert_eq!(live_run_key_for(parent), session);
        assert_eq!(
            live_run_key_for(nested),
            format!("{session}::agent::agent%2Fchild")
        );
        assert!(nested.is_nested());
        assert!(!parent.is_nested());

        let parent_res =
            LiveRunRegistry::reserve_run(session, parent.agent_id).expect("parent reserve");
        parent_res
            .insert(dummy_handle("parent-run"))
            .expect("insert parent");
        let nested_res = LiveRunRegistry::reserve_run(session, nested.agent_id)
            .expect("nested agent_id must not collide with the parent slot");
        nested_res
            .insert(dummy_handle("nested-run"))
            .expect("insert nested");

        assert_eq!(
            LiveRunRegistry::get_run(session, parent.agent_id)
                .unwrap()
                .run_id,
            "parent-run"
        );
        assert_eq!(
            LiveRunRegistry::get_run(session, nested.agent_id)
                .unwrap()
                .run_id,
            "nested-run"
        );
        let _ = LiveRunRegistry::supersede_run(session, parent.agent_id);
        assert!(
            LiveRunRegistry::get_run(session, nested.agent_id).is_some(),
            "supersede_run on the parent slot must leave the nested agent"
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn tool_result_batch_requires_each_pending_id_exactly_once() {
        let pending = vec![pending_exec(1, "tool-1"), pending_exec(2, "tool-2")];
        let result = |id: &str| {
            (
                id.to_string(),
                serde_json::json!({"type":"tool_result","tool_use_id":id,"content":"ok"}),
            )
        };

        assert!(
            validate_tool_result_batch(&pending, &[result("tool-2"), result("tool-1")]).is_ok()
        );
        assert!(validate_tool_result_batch(&pending, &[result("tool-1")]).is_err());
        assert!(
            validate_tool_result_batch(&pending, &[result("tool-1"), result("tool-1")]).is_err()
        );
        assert!(
            validate_tool_result_batch(&pending, &[result("tool-1"), result("other")]).is_err()
        );
    }

    #[test]
    fn replayed_deferred_text_restores_segment_progress_flags() {
        let event = Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: "already buffered".into(),
        }));
        let mut saw_text = false;
        let mut useful = false;
        let mut last_progress = Instant::now() - Duration::from_secs(60);
        record_segment_progress(&event, &mut saw_text, &mut useful, &mut last_progress);
        assert!(saw_text);
        assert!(useful);
        assert!(last_progress.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn interaction_heartbeat_does_not_refresh_idle_progress() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{AgentServerMessage, InteractionHeartbeat, InteractionUpdate};
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: Some(InteractionHeartbeat {}),
                text_delta: None,
                tool_call_started: None,
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                partial_tool_call: None,
                tool_call_delta: None,
                turn_ended: None,
            }),
            exec_server_message: None,
            kv_server_message: None,
            interaction_query: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();
        assert_eq!(frames.len(), 1);

        let (request_tx, _request_rx) = mpsc::channel(1);
        let outbound = ClientOutbound::Bidi(request_tx);
        let mut sink = None;
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now() - Duration::from_secs(600);
        let mut xml_parser = CursorToolUseXmlParser::new(None);
        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            None,
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(cont);
        assert!(
            last_progress.elapsed() >= Duration::from_secs(599),
            "server InteractionUpdate.heartbeat must not reset setup/stream idle"
        );
    }

    #[test]
    fn client_only_anthropic_name_strips_qualified_mcp_names() {
        let allowed = BTreeSet::from(["Workflow".to_string(), "Skill".to_string()]);
        assert_eq!(
            client_only_anthropic_name("claude-local/Workflow", "claude-local", Some(&allowed))
                .as_deref(),
            Some("Workflow")
        );
        assert_eq!(
            client_only_anthropic_name("claude-local:Workflow", "", Some(&allowed)).as_deref(),
            Some("Workflow")
        );
        assert_eq!(
            client_only_anthropic_name("Workflow", "claude-local", Some(&allowed)).as_deref(),
            Some("Workflow")
        );
        assert_eq!(
            client_only_anthropic_name("Read", "", Some(&allowed)),
            None,
            "native tools must not become ClientOnly"
        );
        let read_only = BTreeSet::from(["Read".to_string()]);
        assert_eq!(
            client_only_anthropic_name("claude-local/Workflow", "claude-local", Some(&read_only)),
            None,
            "claude-local must not invent Workflow when the client only advertised Read"
        );
        assert_eq!(
            client_only_anthropic_name("web_search", "claude-local", Some(&read_only)),
            None,
            "claude-local must not invent web_search against an unrelated allowlist"
        );
        assert_eq!(
            client_only_anthropic_name("plugin/search", "plugin", Some(&allowed)),
            None,
            "non-claude-local qualified names stay UI transcript unless advertised"
        );
    }

    #[test]
    fn client_only_anthropic_name_rejects_lifecycle_spoof() {
        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        assert_eq!(
            client_only_anthropic_name("spawn_subagent", "claude-local", Some(&allowed)).as_deref(),
            Some("spawn_subagent")
        );
        assert_eq!(
            client_only_anthropic_name(
                "mcp_claude-local_spawn_subagent",
                "claude-local",
                Some(&allowed)
            )
            .as_deref(),
            Some("spawn_subagent"),
            "Cursor MCP catalog name must become grok-build spawn_subagent"
        );
        assert_eq!(
            client_only_anthropic_name(
                "mcp__claude-local__spawn_subagent",
                "claude-local",
                Some(&allowed)
            )
            .as_deref(),
            Some("spawn_subagent")
        );
        assert_eq!(
            client_only_anthropic_name(
                "claude-local/spawn_subagent",
                "claude-local",
                Some(&allowed)
            )
            .as_deref(),
            Some("spawn_subagent")
        );
        assert_eq!(
            client_only_anthropic_name("evil/spawn_subagent", "evil", Some(&allowed)),
            None,
            "prefix stripping must not promote a foreign MCP spawn"
        );
        assert_eq!(
            client_only_anthropic_name("spawn_subagent", "evil", Some(&allowed)),
            None,
            "external provider cannot impersonate claude-local spawn_subagent"
        );
        assert_eq!(
            client_only_anthropic_name("spawn_subagent", "claude-local", None),
            None,
            "allowed=None must not translate lifecycle tools"
        );
        assert_eq!(
            client_only_anthropic_name("web_search", "", None),
            None,
            "allowed=None must not invent XML/hosted web_search"
        );
        assert_eq!(
            client_only_anthropic_name("web_fetch", "claude-local", None),
            None,
            "allowed=None must not invent web_fetch even with claude-local provider"
        );
        assert_eq!(
            client_only_anthropic_name("enter_plan_mode", "", None),
            None
        );
        assert_eq!(
            client_only_anthropic_name("Workflow", "claude-local", None),
            None,
            "allowed=None must not invent Workflow via MCP provider bypass"
        );
        let aliases = BTreeSet::from(["task".to_string(), "Agent".to_string(), "Task".to_string()]);
        assert_eq!(
            client_only_anthropic_name("spawn_subagent", "claude-local", Some(&aliases)),
            None
        );
        assert_eq!(
            client_only_anthropic_name("task", "claude-local", Some(&aliases)),
            None,
            "internal task alias is not a reserved lifecycle MCP name"
        );
    }

    #[test]
    fn client_only_anthropic_name_maps_fable_workflow_to_grok_workflow() {
        let allowed = BTreeSet::from(["workflow".to_string(), "skill".to_string()]);
        for mapped in [
            "Workflow",
            "workflow",
            "claude-local/Workflow",
            "mcp_claude-local_Workflow",
            "mcp_claude-local_workflow",
            "mcp__claude-local__Workflow",
        ] {
            assert_eq!(
                client_only_anthropic_name(mapped, "claude-local", Some(&allowed)).as_deref(),
                Some("workflow"),
                "{mapped} must steal as the exact grok-cli name"
            );
        }
        assert_eq!(
            client_only_anthropic_name("Skill", "claude-local", Some(&allowed)).as_deref(),
            Some("skill")
        );
        let grep_allowed = BTreeSet::from(["grep".to_string()]);
        assert_eq!(
            client_only_anthropic_name(
                "mcp_claude-local_Grep",
                "claude-local",
                Some(&grep_allowed)
            ),
            None,
            "case folding must not turn Cursor Grep into grok grep"
        );
        assert!(
            client_only_anthropic_name("Workflow", "claude-local", Some(&allowed)).as_deref()
                != Some("Workflow"),
            "must not emit Claude Code Workflow when grok advertised workflow"
        );
    }

    #[tokio::test]
    async fn mcp_workflow_tool_call_started_exposes_client_only() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, McpArgs, McpToolCall, ToolCall, ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: None,
                tool_call_started: Some(ToolCallStarted {
                    call_id: "mcp-wf-1".into(),
                    model_call_id: "model-1".into(),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(McpToolCall {
                            args: Some(McpArgs {
                                name: "Workflow".into(),
                                tool_name: "Workflow".into(),
                                tool_call_id: "mcp-wf-1".into(),
                                provider_identifier: "claude-local".into(),
                                args: {
                                    let mut m = std::collections::HashMap::new();
                                    m.insert("name".into(), br#""deep-research""#.to_vec());
                                    m
                                },
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                partial_tool_call: None,
                tool_call_delta: None,
                turn_ended: None,
            }),
            exec_server_message: None,
            kv_server_message: None,
            interaction_query: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["Workflow".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(!cont, "MCP Workflow must end BiDi segment");
        let event = event_rx.recv().await.expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "Workflow");
                assert_eq!(
                    tools[0].input.get("name").and_then(|v| v.as_str()),
                    Some("deep-research")
                );
                assert!(
                    tools[0].input.get("provider_identifier").is_none(),
                    "Workflow Anthropic input must not include provider_identifier"
                );
            }
            other => panic!("expected NativeToolBatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mcp_fable_workflow_exposes_exact_grok_workflow() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, McpArgs, McpToolCall, ToolCall, ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "mcp-wf-grok".into(),
                    model_call_id: "model-1".into(),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(McpToolCall {
                            args: Some(McpArgs {
                                name: "Workflow".into(),
                                tool_name: "Workflow".into(),
                                tool_call_id: "mcp-wf-grok".into(),
                                provider_identifier: "claude-local".into(),
                                args: {
                                    let mut m = std::collections::HashMap::new();
                                    m.insert("name".into(), br#""carve-bind-wave64""#.to_vec());
                                    m.insert("agent_budget".into(), b"80".to_vec());
                                    m
                                },
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();
        let (cont, event, _) = drive_native_task_frame(
            frames.into_iter().next().unwrap(),
            Some(&BTreeSet::from(["workflow".to_string()])),
            None,
        )
        .await;
        assert!(!cont, "stolen grok workflow must end the BiDi segment");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "workflow");
                assert_eq!(
                    tools[0].input.get("name").and_then(|v| v.as_str()),
                    Some("carve-bind-wave64")
                );
                assert_eq!(
                    tools[0].input.get("agent_budget"),
                    Some(&serde_json::json!(80))
                );
            }
            other => panic!("expected grok workflow batch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn glob_tool_call_started_exposes_client_only() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, GlobToolArgs, GlobToolCall, InteractionUpdate, ToolCall,
            ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: None,
                tool_call_started: Some(ToolCallStarted {
                    call_id: "glob-1".into(),
                    model_call_id: "model-1".into(),
                    tool_call: Some(ToolCall {
                        glob_tool_call: Some(GlobToolCall {
                            args: Some(GlobToolArgs {
                                glob_pattern: "**/*.rs".into(),
                                target_directory: Some("/tmp/proj".into()),
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                partial_tool_call: None,
                tool_call_delta: None,
                turn_ended: None,
            }),
            exec_server_message: None,
            kv_server_message: None,
            interaction_query: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["Glob".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(
            !cont,
            "Glob has no ExecServerMessage arm; expose ClientOnly and end the segment"
        );
        let event = event_rx.recv().await.expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "Glob");
                assert_eq!(
                    tools[0].input.get("pattern").and_then(|v| v.as_str()),
                    Some("**/*.rs")
                );
            }
            other => panic!("expected Glob NativeToolBatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn task_tool_call_started_exposes_spawn_subagent_client_only() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, TaskToolCall, TaskToolCallArgsProto, ToolCall,
            ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: None,
                tool_call_started: Some(ToolCallStarted {
                    call_id: "task-1".into(),
                    model_call_id: "model-1".into(),
                    tool_call: Some(ToolCall {
                        task_tool_call: Some(TaskToolCall {
                            args: Some(TaskToolCallArgsProto {
                                description: "explore live".into(),
                                prompt: "find TaskToolCall".into(),
                                model: None,
                                subagent_type: "explore".into(),
                                resume: None,
                                run_in_background: Some(true),
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                partial_tool_call: None,
                tool_call_delta: None,
                turn_ended: None,
            }),
            exec_server_message: None,
            kv_server_message: None,
            interaction_query: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(!cont, "native Task must become ClientOnly spawn_subagent");
        let event = event_rx.recv().await.expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "spawn_subagent");
                assert_eq!(
                    tools[0].input.get("prompt").and_then(|v| v.as_str()),
                    Some("find TaskToolCall")
                );
                assert_eq!(
                    tools[0].input.get("subagent_type").and_then(|v| v.as_str()),
                    Some("explore")
                );
                assert_eq!(
                    tools[0].input.get("background"),
                    Some(&serde_json::json!(true))
                );
            }
            other => panic!("expected spawn_subagent NativeToolBatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn task_tool_call_started_stays_transcript_when_unadvertised() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, TaskToolCall, TaskToolCallArgsProto, ToolCall,
            ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: None,
                tool_call_started: Some(ToolCallStarted {
                    call_id: "task-hidden".into(),
                    model_call_id: "model-1".into(),
                    tool_call: Some(ToolCall {
                        task_tool_call: Some(TaskToolCall {
                            args: Some(TaskToolCallArgsProto {
                                description: "hidden".into(),
                                prompt: "must not invent".into(),
                                model: None,
                                subagent_type: "explore".into(),
                                resume: None,
                                run_in_background: None,
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                partial_tool_call: None,
                tool_call_delta: None,
                turn_ended: None,
            }),
            exec_server_message: None,
            kv_server_message: None,
            interaction_query: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["read_file".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(cont, "unadvertised Task must not invent spawn_subagent");
        assert!(event_rx.try_recv().is_err(), "no ClientOnly batch");
    }

    fn native_task_started_frame(
        call_id: &str,
        prompt: &str,
        background: Option<bool>,
    ) -> super::super::connect::ConnectFrame {
        native_task_started_frame_typed(call_id, prompt, background, "explore")
    }

    fn native_task_started_frame_typed(
        call_id: &str,
        prompt: &str,
        background: Option<bool>,
        subagent_type: &str,
    ) -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, TaskToolCall, TaskToolCallArgsProto, ToolCall,
            ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: call_id.into(),
                    model_call_id: "model-1".into(),
                    tool_call: Some(ToolCall {
                        task_tool_call: Some(TaskToolCall {
                            args: Some(TaskToolCallArgsProto {
                                description: "explore live".into(),
                                prompt: prompt.into(),
                                model: Some("cursor-grok4.6".into()),
                                subagent_type: subagent_type.into(),
                                resume: None,
                                run_in_background: background,
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    fn glob_started_frame(pattern: &str, dir: &str) -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, GlobToolArgs, GlobToolCall, InteractionUpdate, ToolCall,
            ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "glob-map".into(),
                    model_call_id: "model-glob".into(),
                    tool_call: Some(ToolCall {
                        glob_tool_call: Some(GlobToolCall {
                            args: Some(GlobToolArgs {
                                glob_pattern: pattern.into(),
                                target_directory: Some(dir.into()),
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    fn todo_write_started_frame() -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, TodoItem, ToolCall, ToolCallStarted,
            UpdateTodosArgs, UpdateTodosToolCall,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "todo-1".into(),
                    model_call_id: "model-todo".into(),
                    tool_call: Some(ToolCall {
                        update_todos_tool_call: Some(UpdateTodosToolCall {
                            args: Some(UpdateTodosArgs {
                                todos: vec![TodoItem {
                                    id: "1".into(),
                                    content: "collect".into(),
                                    status: 1,
                                }],
                                merge: true,
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    async fn drive_native_task_frame(
        frame: super::super::connect::ConnectFrame,
        allowed: Option<&BTreeSet<String>>,
        turn_ctx: Option<&mut LiveTurnCtx<'_>>,
    ) -> (bool, Option<Result<LiveRunEvent, String>>, PendingExecState) {
        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let mut xml_parser = CursorToolUseXmlParser::new(allowed.cloned());
        let cont = process_live_frame(
            frame,
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            allowed,
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            turn_ctx,
        )
        .await;
        let event = event_rx.try_recv().ok();
        (cont, event, pending)
    }

    #[tokio::test]
    async fn glob_listing_maps_to_list_dir() {
        let allowed = BTreeSet::from(["list_dir".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(glob_started_frame("*", "/tmp/carve"), Some(&allowed), None)
                .await;
        assert!(
            !cont,
            "listing glob must expose list_dir and end the segment"
        );
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "list_dir");
                assert_eq!(tools[0].input["target_directory"], "/tmp/carve");
                assert!(tools[0].input.get("pattern").is_none());
            }
            other => panic!("expected list_dir, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn glob_pattern_maps_to_shell_not_list_dir() {
        let allowed = BTreeSet::from(["list_dir".to_string(), "run_terminal_command".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(glob_started_frame("**/*.rs", "src"), Some(&allowed), None)
                .await;
        assert!(!cont);
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "run_terminal_command");
                assert_eq!(
                    tools[0].input["command"].as_str(),
                    Some("rg --files -g '**/*.rs' -- 'src'")
                );
                assert!(
                    tools[0].input["description"]
                        .as_str()
                        .is_some_and(|s| !s.is_empty())
                );
            }
            other => panic!("expected run_terminal_command, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn todo_write_started_exposes_claude_todowrite_schema() {
        let allowed = BTreeSet::from(["TodoWrite".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(todo_write_started_frame(), Some(&allowed), None).await;
        assert!(!cont, "TodoWrite must expose as ClientOnly");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "TodoWrite");
                assert_eq!(tools[0].input["todos"][0]["content"], "collect");
                assert_eq!(
                    tools[0].input["todos"][0]["activeForm"],
                    "Working on collect"
                );
                assert!(tools[0].input.get("merge").is_none());
            }
            other => panic!("expected TodoWrite, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn todo_write_started_exposes_todo_write() {
        let allowed = BTreeSet::from(["todo_write".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(todo_write_started_frame(), Some(&allowed), None).await;
        assert!(!cont, "TodoWrite must expose as ClientOnly");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "todo_write");
                assert_eq!(tools[0].input["todos"][0]["content"], "collect");
                assert_eq!(tools[0].input["merge"], true);
            }
            other => panic!("expected todo_write, got {other:?}"),
        }
    }

    fn web_search_started_frame(term: &str) -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, ToolCall, ToolCallStarted, WebSearchArgs,
            WebSearchToolCall,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "ws-1".into(),
                    model_call_id: "model-ws".into(),
                    tool_call: Some(ToolCall {
                        web_search_tool_call: Some(WebSearchToolCall {
                            args: Some(WebSearchArgs {
                                search_term: term.into(),
                                tool_call_id: "ws-1".into(),
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    fn web_fetch_started_frame(url: &str) -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, FetchArgs, InteractionUpdate, ToolCall, ToolCallStarted,
            WebFetchToolCall,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "wf-37".into(),
                    model_call_id: "model-wf".into(),
                    tool_call: Some(ToolCall {
                        web_fetch_tool_call: Some(WebFetchToolCall {
                            args: Some(FetchArgs {
                                url: url.into(),
                                tool_call_id: "wf-37".into(),
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    fn create_plan_started_frame() -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, CreatePlanArgs, CreatePlanToolCall, InteractionUpdate, ToolCall,
            ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "plan-1".into(),
                    model_call_id: "model-plan".into(),
                    tool_call: Some(ToolCall {
                        create_plan_tool_call: Some(CreatePlanToolCall {
                            args: Some(CreatePlanArgs {
                                name: "carve".into(),
                                overview: "prep".into(),
                                plan: "step 1".into(),
                                is_project: false,
                                todos: vec![],
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    fn read_exec_frame(path: &str) -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use prost::Message;

        let mut full = Vec::new();
        proto::AgentServerMessage {
            exec_server_message: Some(ExecServerMessage {
                id: 21,
                exec_id: Some("exec-read".into()),
                read_args: Some(ExecReadArgs {
                    path: path.into(),
                    tool_call_id: "read-1".into(),
                    offset: None,
                    limit: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn native_read_none_allowed_throws_instead_of_inventing() {
        let (cont, event, pending) =
            drive_native_task_frame(read_exec_frame("/tmp/x"), None, None).await;
        assert!(
            cont,
            "unadvertised native Read must throw and keep the stream"
        );
        assert!(event.is_none(), "must not invent a client Read: {event:?}");
        assert!(
            pending.is_empty(),
            "must not queue invented Read: {pending:?}"
        );
    }

    #[tokio::test]
    async fn web_search_started_none_allowed_stays_transcript() {
        let (cont, event, pending) =
            drive_native_task_frame(web_search_started_frame("rust async"), None, None).await;
        assert!(cont, "allowed=None must not invent web_search");
        assert!(event.is_none(), "no ClientOnly batch: {event:?}");
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn web_search_started_exposes_web_search() {
        let allowed = BTreeSet::from(["web_search".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(web_search_started_frame("rust async"), Some(&allowed), None)
                .await;
        assert!(!cont, "WebSearch must not stay on Cursor hosted search");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "web_search");
                assert_eq!(tools[0].input["query"], "rust async");
                assert!(tools[0].input.get("search_term").is_none());
            }
            other => panic!("expected web_search, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn web_fetch_tag37_exposes_claude_webfetch_with_prompt() {
        let allowed = BTreeSet::from(["WebFetch".to_string()]);
        let (cont, event, _) = drive_native_task_frame(
            web_fetch_started_frame("https://example.com/doc"),
            Some(&allowed),
            None,
        )
        .await;
        assert!(!cont, "WebFetch tag 37 must not be ignored");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "WebFetch");
                assert_eq!(tools[0].input["url"], "https://example.com/doc");
                assert!(
                    tools[0].input["prompt"]
                        .as_str()
                        .is_some_and(|p| !p.is_empty())
                );
            }
            other => panic!("expected WebFetch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_plan_started_exposes_enter_plan_mode_for_claude_code() {
        let allowed = BTreeSet::from(["EnterPlanMode".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(create_plan_started_frame(), Some(&allowed), None).await;
        assert!(!cont, "CreatePlan must map to Claude Code EnterPlanMode");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "EnterPlanMode");
                assert_eq!(tools[0].input, serde_json::json!({}));
            }
            other => panic!("expected EnterPlanMode, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn web_fetch_tag37_exposes_web_fetch() {
        let allowed = BTreeSet::from(["web_fetch".to_string()]);
        let (cont, event, _) = drive_native_task_frame(
            web_fetch_started_frame("https://example.com/doc"),
            Some(&allowed),
            None,
        )
        .await;
        assert!(!cont, "WebFetch tag 37 must not be ignored");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "web_fetch");
                assert_eq!(tools[0].input["url"], "https://example.com/doc");
            }
            other => panic!("expected web_fetch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_plan_started_exposes_enter_plan_mode() {
        let allowed = BTreeSet::from(["enter_plan_mode".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(create_plan_started_frame(), Some(&allowed), None).await;
        assert!(!cont, "CreatePlan must not stay on Cursor plan UI");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "enter_plan_mode");
                assert_eq!(tools[0].input, serde_json::json!({}));
            }
            other => panic!("expected enter_plan_mode, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn web_search_started_exposes_immediately_with_turn_ctx() {
        let allowed = BTreeSet::from(["web_search".to_string()]);
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id: "sess-ws",
            user_prompt: "search",
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };
        let (cont, event, _) = drive_native_task_frame(
            web_search_started_frame("rust async"),
            Some(&allowed),
            Some(&mut turn),
        )
        .await;
        assert!(!cont, "hosted WebSearch must not wait for turn_ended");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "web_search");
            }
            other => panic!("expected web_search, got {other:?}"),
        }
    }

    fn xml_tool_use_frame(xml: &str) -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{AgentServerMessage, InteractionUpdate, TextDelta};
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                text_delta: Some(TextDelta {
                    text: xml.to_string(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn xml_parameter_spawn_subagent_exposes_spawn_not_transcript() {
        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        let xml = concat!(
            r#"<tool_use id="spawn-40" name="spawn_subagent">"#,
            r#"<parameter name="background">true</parameter>"#,
            r#"<parameter name="description">CARVE A1560 0013 REREAD_1</parameter>"#,
            r#"<parameter name="prompt">Read 40.txt completely.</parameter>"#,
            r#"<parameter name="subagent_type">general-purpose</parameter>"#,
            "</tool_use>",
            " </tool_call>",
        );
        let (cont, event, _) =
            drive_native_task_frame(xml_tool_use_frame(xml), Some(&allowed), None).await;
        assert!(
            !cont,
            "parameter-style XML spawn_subagent must be ClientOnly, not transcript XML"
        );
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "spawn_subagent");
                assert_eq!(tools[0].input["prompt"], "Read 40.txt completely.");
                assert_eq!(tools[0].input["subagent_type"], "general-purpose");
            }
            other => panic!("expected spawn_subagent from <parameter> XML, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn xml_wrapped_tool_call_exposes_sibling_client_tools() {
        use super::super::proto::RequestContext;

        let allowed = BTreeSet::from([
            "read_file".to_string(),
            "get_command_or_subagent_output".to_string(),
            "run_terminal_command".to_string(),
        ]);
        let xml = concat!(
            "<tool_call>",
            r#"<tool_use name="read_file"><parameter name="path">SKILL.md</parameter></tool_use>"#,
            r#"<tool_use name="get_command_or_subagent_output"><parameter name="task_id">t-1</parameter></tool_use>"#,
            r#"<tool_use name="run_terminal_command"><parameter name="command">echo hi</parameter></tool_use>"#,
            r#"<tool_use name="Glob"><parameter name="glob_pattern">**/*.rhai</parameter></tool_use>"#,
            "</tool_call>",
        );
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id: "sess-wrap",
            user_prompt: "fan-out",
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };
        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));
        let cont = process_live_frame(
            xml_tool_use_frame(xml),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(25),
            &mut xml_parser,
            Some(&mut turn),
        )
        .await;
        assert!(
            cont,
            "wrapped sibling XML must stay on the BiDi until expose"
        );
        while let Ok(event) = event_rx.try_recv() {
            match event {
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) => {
                    assert!(
                        !text.contains("<tool_call")
                            && !text.contains("<tool_use")
                            && !text.contains("<parameter")
                            && !text.contains("read_file"),
                        "wrapper XML must not leak as transcript: {text}"
                    );
                }
                other => panic!("unexpected event before expose: {other:?}"),
            }
        }
        assert!(
            !expose_collected_tools(&mut pending, &pending_shared, &mut sink).await,
            "wrapped siblings must tear down as one ClientOnly batch"
        );
        match event_rx.recv().await.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                let names: Vec<_> = tools.iter().map(|tool| tool.name.as_str()).collect();
                assert_eq!(
                    names,
                    [
                        "read_file",
                        "get_command_or_subagent_output",
                        "run_terminal_command"
                    ],
                    "{tools:?}"
                );
                assert!(
                    !names.contains(&"Glob"),
                    "unadvertised Glob must not be invented: {tools:?}"
                );
            }
            other => panic!("expected wrapped sibling batch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn xml_malformed_command_and_unadvertised_glob_do_not_leak() {
        let allowed = BTreeSet::from(["run_terminal_command".to_string()]);
        let xml = concat!(
            r#"<tool_use id="dump-and-collect" name="run_terminal_command">"#,
            "<parameter name=\"command\">python3 -c <<'PY'\nprint(1)\nPY\n",
            r#"<parameter name="description">Dump progress, in-flight, launch pack</parameter>"#,
            "</tool_use>",
            r#"<tool_use id="glob-rhai" name="Glob">"#,
            r#"<parameter name="glob_pattern">**/*.rhai</parameter>"#,
            r#"<parameter name="target_directory">/Users/yeauty/.grok/bundled/skills/create-workflow</parameter>"#,
            "</tool_use>",
        );
        let (cont, event, pending) =
            drive_native_task_frame(xml_tool_use_frame(xml), Some(&allowed), None).await;
        assert!(
            pending.is_empty(),
            "malformed shell XML and unadvertised Glob must not become tools: {pending:?}"
        );
        match event {
            None => {}
            Some(Ok(LiveRunEvent::NativeToolBatch(tools))) => {
                panic!("must not invent tools from rejected XML: {tools:?}");
            }
            Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }))) => {
                assert!(
                    !text.contains("<tool_use")
                        && !text.contains("<tool_call")
                        && !text.contains("<parameter"),
                    "rejected control XML must not become transcript: {text}"
                );
            }
            other => panic!("unexpected event for rejected XML: {other:?}"),
        }
        assert!(
            cont,
            "quarantined XML must not tear the BiDi down as ClientOnly: {cont}"
        );
    }

    #[tokio::test]
    async fn xml_recovered_unbridgeable_glob_does_not_reconstruct() {
        let allowed = BTreeSet::from(["Glob".to_string(), "run_terminal_command".to_string()]);
        let xml = concat!(
            r#"<tool_use id="glob-rhai" name="Glob">"#,
            r#"<parameter name="glob_pattern">**/*.rhai</parameter>"#,
            "</tool_use>",
        );
        let (cont, event, pending) =
            drive_native_task_frame(xml_tool_use_frame(xml), Some(&allowed), None).await;
        assert!(
            pending.is_empty(),
            "XML Glob must not become a client tool just because the name is listed: {pending:?}"
        );
        match event {
            None => {}
            Some(Ok(LiveRunEvent::NativeToolBatch(tools))) => {
                panic!("must not invent XML Glob as NativeToolBatch: {tools:?}");
            }
            Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }))) => {
                assert!(
                    !text.contains("<tool_use") && !text.contains("<parameter"),
                    "unbridgeable recovered Glob must not be reconstructed as XML: {text}"
                );
            }
            other => panic!("unexpected event for unbridgeable Glob XML: {other:?}"),
        }
        assert!(cont, "dropped Glob XML must keep the BiDi open");
    }

    #[tokio::test]
    async fn xml_grok_tool_call_todo_write_exposes_todo_write() {
        let allowed = BTreeSet::from(["todo_write".to_string()]);
        let xml = concat!(
            r#"<tool_call> todo_write <parameter> {"todos":[{"id":"1","content":"x","status":"completed"}]} </parameter> </tool_call>"#,
            " </assistant>"
        );
        let (cont, event, _) =
            drive_native_task_frame(xml_tool_use_frame(xml), Some(&allowed), None).await;
        assert!(
            !cont,
            "grok <tool_call> todo_write must be ClientOnly, not transcript text"
        );
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "todo_write");
                assert_eq!(tools[0].input["todos"][0]["content"], "x");
            }
            other => panic!("expected todo_write from grok <tool_call> XML, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn xml_web_search_exposes_web_search() {
        let allowed = BTreeSet::from(["web_search".to_string()]);
        let xml = r#"<tool_use id="ws-xml" name="web_search">{"query":"rust async"}</tool_use>"#;
        let (cont, event, _) =
            drive_native_task_frame(xml_tool_use_frame(xml), Some(&allowed), None).await;
        assert!(
            !cont,
            "XML web_search must be ClientOnly, not transcript text"
        );
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "web_search");
                assert_eq!(tools[0].input["query"], "rust async");
            }
            other => panic!("expected web_search from XML, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn xml_web_search_none_allowed_stays_transcript() {
        let xml = r#"<tool_use id="ws-xml" name="web_search">{"query":"rust async"}</tool_use>"#;
        let (cont, event, pending) =
            drive_native_task_frame(xml_tool_use_frame(xml), None, None).await;
        assert!(
            cont,
            "allowed=None XML web_search must not tear down as ClientOnly"
        );
        assert!(
            pending.is_empty(),
            "must not queue invented web_search: {pending:?}"
        );
        match event {
            None => {}
            Some(Ok(LiveRunEvent::NativeToolBatch(tools))) => {
                panic!("allowed=None must not invent a client tool: {tools:?}");
            }
            Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }))) => {
                assert!(
                    !text.contains("<tool_use") && !text.contains("<parameter"),
                    "allowed=None XML must not leak as transcript: {text}"
                );
            }
            other => panic!("unexpected event for unadvertised XML web_search: {other:?}"),
        }
    }

    fn web_search_query_frame(term: &str) -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionQuery, WebSearchArgs, WebSearchRequestQuery,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_query: Some(InteractionQuery {
                id: 11,
                web_search_request_query: Some(WebSearchRequestQuery {
                    args: Some(WebSearchArgs {
                        search_term: term.into(),
                        tool_call_id: "ws-q".into(),
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    async fn drive_live_frame_io(
        frame: super::super::connect::ConnectFrame,
        allowed: Option<&BTreeSet<String>>,
    ) -> (
        bool,
        Option<Result<LiveRunEvent, String>>,
        Option<bytes::Bytes>,
    ) {
        let (request_tx, mut request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let mut xml_parser = CursorToolUseXmlParser::new(allowed.cloned());
        let cont = process_live_frame(
            frame,
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            allowed,
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        let event = event_rx.try_recv().ok();
        let reply = request_rx.try_recv().ok().and_then(|r| r.ok());
        (cont, event, reply)
    }

    #[tokio::test]
    async fn web_search_query_exposes_web_search_without_auto_approve() {
        let allowed = BTreeSet::from(["web_search".to_string()]);
        let (cont, event, reply) =
            drive_live_frame_io(web_search_query_frame("rust async"), Some(&allowed)).await;
        assert!(!cont, "advertised web_search query must tear down");
        assert!(
            reply.is_none(),
            "must not auto-approve Cursor hosted search"
        );
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "web_search");
                assert_eq!(tools[0].input["query"], "rust async");
                assert!(tools[0].input.get("search_term").is_none());
            }
            other => panic!("expected web_search, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn web_search_query_unadvertised_still_auto_approves() {
        use prost::Message;

        let allowed = BTreeSet::from(["Read".to_string()]);
        let (cont, event, reply) =
            drive_live_frame_io(web_search_query_frame("rust async"), Some(&allowed)).await;
        assert!(cont, "unadvertised search stays on Cursor hosted path");
        assert!(event.is_none(), "must not invent web_search: {event:?}");
        let reply = reply.expect("auto-approve frame");
        let decoded = super::super::client::decode_upstream_frames(&reply).unwrap();
        let message = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        assert!(
            message
                .interaction_response
                .as_ref()
                .and_then(|r| r.web_search_request_response.as_ref())
                .and_then(|r| r.approved.as_ref())
                .is_some(),
            "unadvertised WebSearch must still auto-approve Cursor hosted search"
        );
    }

    #[tokio::test]
    async fn web_search_query_none_allowed_still_auto_approves() {
        use prost::Message;

        let (cont, event, reply) =
            drive_live_frame_io(web_search_query_frame("rust async"), None).await;
        assert!(cont, "missing tool list must not invent web_search");
        assert!(event.is_none(), "must not invent web_search: {event:?}");
        let reply = reply.expect("auto-approve frame");
        let decoded = super::super::client::decode_upstream_frames(&reply).unwrap();
        let message = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        assert!(
            message
                .interaction_response
                .as_ref()
                .and_then(|r| r.web_search_request_response.as_ref())
                .and_then(|r| r.approved.as_ref())
                .is_some(),
            "allowed=None must auto-approve Cursor hosted search, not emit WebSearch"
        );
    }

    #[tokio::test]
    async fn create_plan_query_exposes_enter_plan_mode() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, CreatePlanArgs, CreatePlanRequestQuery, InteractionQuery,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_query: Some(InteractionQuery {
                id: 12,
                create_plan_request_query: Some(CreatePlanRequestQuery {
                    args: Some(CreatePlanArgs {
                        name: "carve".into(),
                        overview: "prep".into(),
                        plan: "step 1".into(),
                        is_project: false,
                        todos: vec![],
                    }),
                    tool_call_id: "plan-q".into(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frame = decoder.push(&framed).unwrap().into_iter().next().unwrap();
        let allowed = BTreeSet::from(["enter_plan_mode".to_string()]);
        let (cont, event, reply) = drive_live_frame_io(frame, Some(&allowed)).await;
        assert!(!cont, "CreatePlan query must not stay on Cursor plan UI");
        assert!(reply.is_none(), "must not auto-succeed Cursor CreatePlan");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "enter_plan_mode");
                assert_eq!(tools[0].input, serde_json::json!({}));
            }
            other => panic!("expected enter_plan_mode, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn task_tool_call_started_exposes_agent_for_claude_code() {
        let frame = native_task_started_frame("task-agent", "must become Agent", Some(true));
        let allowed = BTreeSet::from(["task".to_string(), "Agent".to_string(), "Task".to_string()]);
        let (cont, event, _) = drive_native_task_frame(frame, Some(&allowed), None).await;
        assert!(!cont, "Claude Code Agent must steal native Task");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "Agent");
                assert_eq!(tools[0].input["prompt"], "must become Agent");
                assert_eq!(tools[0].input["run_in_background"], true);
                assert!(tools[0].input.get("resume_from").is_none());
                assert!(tools[0].input.get("background").is_none());
            }
            other => panic!("expected Agent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn task_tool_call_started_remaps_gemini_slug_for_claude_code_agent() {
        let frame = native_task_started_frame_typed(
            "task-gemini",
            "CARVE INITIAL A1585-0026",
            Some(true),
            "gemini-3.6-flash-high",
        );
        let allowed = BTreeSet::from(["task".to_string(), "Agent".to_string(), "Task".to_string()]);
        let (cont, event, _) = drive_native_task_frame(frame, Some(&allowed), None).await;
        assert!(!cont, "Claude Code Agent must steal native Task");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools[0].name, "Agent");
                assert_eq!(
                    tools[0].input.get("subagent_type").and_then(|v| v.as_str()),
                    Some("general-purpose"),
                    "Cursor puts the live model slug in Task.subagent_type; Claude Code Agent catalog rejects it"
                );
                assert_eq!(tools[0].input["prompt"], "CARVE INITIAL A1585-0026");
                assert_eq!(tools[0].input["run_in_background"], true);
                assert!(tools[0].input.get("background").is_none());
            }
            other => panic!("expected Agent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn task_tool_call_started_does_not_invent_spawn_from_task_alias() {
        let frame = native_task_started_frame("task-alias", "must not invent", Some(true));
        let allowed = BTreeSet::from(["task".to_string()]);
        let (cont, event, pending) = drive_native_task_frame(frame, Some(&allowed), None).await;
        assert!(cont, "lowercase task must not authorize spawn_subagent");
        assert!(event.is_none(), "no ClientOnly batch: {event:?}");
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn task_tool_call_started_none_allowed_stays_transcript() {
        let frame = native_task_started_frame("task-open", "must not invent", Some(true));
        let (cont, event, pending) = drive_native_task_frame(frame, None, None).await;
        assert!(cont, "allowed=None must not translate native Task");
        assert!(event.is_none(), "no ClientOnly batch: {event:?}");
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn task_tool_call_started_exposes_spawn_immediately_with_turn_ctx() {
        let frame = native_task_started_frame("task-prod", "find TaskToolCall", Some(false));
        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id: "sess-task",
            user_prompt: "spawn a child",
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };
        let (cont, event, _) =
            drive_native_task_frame(frame, Some(&allowed), Some(&mut turn)).await;
        assert!(!cont, "production turn_ctx must still expose and tear down");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "spawn_subagent");
                assert_eq!(
                    tools[0].input.get("prompt").and_then(|v| v.as_str()),
                    Some("find TaskToolCall")
                );
                assert_eq!(
                    tools[0].input.get("background"),
                    Some(&serde_json::json!(false))
                );
                assert!(tools[0].input.get("model").is_none());
                assert!(tools[0].input.get("readonly").is_none());
            }
            other => panic!("expected spawn_subagent NativeToolBatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn nested_native_task_is_not_promoted_to_spawn() {
        use super::super::proto::{
            InteractionUpdate, TaskToolCall, TaskToolCallArgsProto, ToolCall, ToolCallStarted,
        };

        let nested = InteractionUpdate {
            tool_call_started: Some(ToolCallStarted {
                call_id: "nested-task".into(),
                model_call_id: "model-2".into(),
                tool_call: Some(ToolCall {
                    task_tool_call: Some(TaskToolCall {
                        args: Some(TaskToolCallArgsProto {
                            description: "nested".into(),
                            prompt: "child context only".into(),
                            model: None,
                            subagent_type: "explore".into(),
                            resume: None,
                            run_in_background: Some(true),
                        }),
                    }),
                    ..Default::default()
                }),
            }),
            ..Default::default()
        };
        let frame = task_nested_frame(nested);
        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut logical = LogicalToolTracker::default();
        let cont = drive_task_frame(
            frame,
            &outbound,
            &mut sink,
            &mut pending,
            &pending_shared,
            &mut logical,
            &allowed,
        )
        .await;
        assert!(cont, "nested Task must stay in the Cursor child transcript");
        assert!(
            event_rx.try_recv().is_err(),
            "nested Task must not emit spawn_subagent"
        );
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn partial_tool_call_args_fill_empty_mcp_started_input() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, McpArgs, McpToolCall, PartialToolCall, ToolCall,
            ToolCallStarted,
        };
        use prost::Message;

        fn frame_for(msg: AgentServerMessage) -> super::super::connect::ConnectFrame {
            let mut full = Vec::new();
            msg.encode(&mut full).unwrap();
            let framed = encode_connect_frame(full, 0);
            let mut decoder = super::super::connect::ConnectFrameDecoder::new();
            decoder.push(&framed).unwrap().into_iter().next().unwrap()
        }

        async fn drive(
            frame: super::super::connect::ConnectFrame,
            outbound: &ClientOutbound,
            sink: &mut Option<mpsc::Sender<LiveEventResult>>,
            pending: &mut PendingExecState,
            pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
            logical: &mut LogicalToolTracker,
            allowed: &BTreeSet<String>,
        ) -> bool {
            let mut deferred = VecDeque::new();
            let mut kv_blobs = HashMap::new();
            let mut latest_checkpoint = None;
            let terminal_error = Arc::new(Mutex::new(None));
            let mut saw_text = false;
            let mut useful = false;
            let mut last_progress = Instant::now();
            let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));
            process_live_frame(
                frame,
                outbound,
                sink,
                &mut deferred,
                pending,
                pending_shared,
                &mut kv_blobs,
                &mut latest_checkpoint,
                &terminal_error,
                Some(allowed),
                &mut saw_text,
                &mut useful,
                logical,
                &mut last_progress,
                Duration::from_millis(50),
                &mut xml_parser,
                None,
            )
            .await
        }

        let mcp = |args: std::collections::HashMap<String, Vec<u8>>| McpToolCall {
            args: Some(McpArgs {
                name: "Workflow".into(),
                tool_name: "Workflow".into(),
                tool_call_id: "mcp-partial-1".into(),
                provider_identifier: "claude-local".into(),
                args,
            }),
        };

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut logical = LogicalToolTracker::default();
        let allowed = BTreeSet::from(["Workflow".to_string()]);

        let cont = drive(
            frame_for(AgentServerMessage {
                interaction_update: Some(InteractionUpdate {
                    partial_tool_call: Some(PartialToolCall {
                        call_id: "mcp-partial-1".into(),
                        model_call_id: "model-1".into(),
                        args_text_delta: r#"{"name":"deep-research"}"#.into(),
                        tool_call: Some(ToolCall {
                            mcp_tool_call: Some(mcp(Default::default())),
                            ..Default::default()
                        }),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            &outbound,
            &mut sink,
            &mut pending,
            &pending_shared,
            &mut logical,
            &allowed,
        )
        .await;
        assert!(cont, "partial_tool_call must not tear down BiDi");
        assert!(
            event_rx.try_recv().is_err(),
            "must wait for tool_call_started"
        );

        let cont = drive(
            frame_for(AgentServerMessage {
                interaction_update: Some(InteractionUpdate {
                    tool_call_started: Some(ToolCallStarted {
                        call_id: "mcp-partial-1".into(),
                        model_call_id: "model-1".into(),
                        tool_call: Some(ToolCall {
                            mcp_tool_call: Some(mcp(Default::default())),
                            ..Default::default()
                        }),
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            &outbound,
            &mut sink,
            &mut pending,
            &pending_shared,
            &mut logical,
            &allowed,
        )
        .await;
        assert!(!cont, "MCP Workflow must end BiDi segment");
        let event = event_rx.recv().await.expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "Workflow");
                assert_eq!(
                    tools[0].input.get("name").and_then(|v| v.as_str()),
                    Some("deep-research"),
                    "ClientOnly input must come from partial_tool_call args_text_delta"
                );
            }
            other => panic!("expected NativeToolBatch, got {other:?}"),
        }
    }

    fn task_nested_frame(nested: InteractionUpdate) -> super::super::connect::ConnectFrame {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, TaskToolCallDelta, ToolCallDelta, ToolCallDeltaUpdate,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_delta: Some(ToolCallDeltaUpdate {
                    call_id: "task-1".into(),
                    model_call_id: "model-1".into(),
                    tool_call_delta: Some(ToolCallDelta {
                        task_tool_call_delta: Some(TaskToolCallDelta {
                            interaction_update: Some(Box::new(nested)),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    async fn drive_task_frame(
        frame: super::super::connect::ConnectFrame,
        outbound: &ClientOutbound,
        sink: &mut Option<mpsc::Sender<LiveEventResult>>,
        pending: &mut PendingExecState,
        pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
        logical: &mut LogicalToolTracker,
        allowed: &BTreeSet<String>,
    ) -> bool {
        let mut deferred = VecDeque::new();
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut last_progress = Instant::now();
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));
        process_live_frame(
            frame,
            outbound,
            sink,
            &mut deferred,
            pending,
            pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(allowed),
            &mut saw_text,
            &mut useful,
            logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn task_tool_call_delta_nested_partial_args_fill_mcp_started() {
        use super::super::proto::{
            McpArgs, McpToolCall, PartialToolCall, ToolCall, ToolCallStarted,
        };

        let mcp = |args: std::collections::HashMap<String, Vec<u8>>| McpToolCall {
            args: Some(McpArgs {
                name: "Workflow".into(),
                tool_name: "Workflow".into(),
                tool_call_id: "mcp-nested-1".into(),
                provider_identifier: "claude-local".into(),
                args,
            }),
        };

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut logical = LogicalToolTracker::default();
        let allowed = BTreeSet::from(["Workflow".to_string()]);

        let cont = drive_task_frame(
            task_nested_frame(InteractionUpdate {
                partial_tool_call: Some(PartialToolCall {
                    call_id: "mcp-nested-1".into(),
                    model_call_id: "model-1".into(),
                    args_text_delta: r#"{"name":"deep-research"}"#.into(),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(mcp(Default::default())),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            &outbound,
            &mut sink,
            &mut pending,
            &pending_shared,
            &mut logical,
            &allowed,
        )
        .await;
        assert!(cont, "nested partial_tool_call must not tear down BiDi");
        assert!(
            event_rx.try_recv().is_err(),
            "must wait for nested tool_call_started"
        );

        let cont = drive_task_frame(
            task_nested_frame(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "mcp-nested-1".into(),
                    model_call_id: "model-1".into(),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(mcp(Default::default())),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            &outbound,
            &mut sink,
            &mut pending,
            &pending_shared,
            &mut logical,
            &allowed,
        )
        .await;
        assert!(!cont, "nested MCP Workflow must end BiDi segment");
        let event = event_rx.recv().await.expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "Workflow");
                assert_eq!(
                    tools[0].input.get("name").and_then(|v| v.as_str()),
                    Some("deep-research"),
                    "ClientOnly input must come from nested task partial_tool_call"
                );
            }
            other => panic!("expected NativeToolBatch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn task_tool_call_delta_nested_text_surfaces() {
        use super::super::proto::TextDelta;

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut logical = LogicalToolTracker::default();
        let allowed = BTreeSet::from(["Workflow".to_string()]);

        let cont = drive_task_frame(
            task_nested_frame(InteractionUpdate {
                text_delta: Some(TextDelta {
                    text: "from subagent".into(),
                }),
                ..Default::default()
            }),
            &outbound,
            &mut sink,
            &mut pending,
            &pending_shared,
            &mut logical,
            &allowed,
        )
        .await;
        assert!(cont, "nested text must not tear down BiDi");
        let event = event_rx.try_recv().expect("text delta");
        match event {
            Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) => {
                assert_eq!(text, "from subagent");
            }
            other => panic!("expected TextDelta, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn task_tool_call_delta_nested_turn_ended_does_not_end_parent() {
        use super::super::proto::TurnEnded;

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut logical = LogicalToolTracker::default();
        let allowed = BTreeSet::from(["Workflow".to_string()]);

        let cont = drive_task_frame(
            task_nested_frame(InteractionUpdate {
                turn_ended: Some(TurnEnded {
                    output_tokens: Some(4),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            &outbound,
            &mut sink,
            &mut pending,
            &pending_shared,
            &mut logical,
            &allowed,
        )
        .await;
        assert!(cont, "nested turn_ended must not end the parent Task");
        assert!(
            event_rx.try_recv().is_err(),
            "nested turn_ended must not emit parent End/Usage"
        );
    }

    #[tokio::test]
    async fn task_tool_call_delta_caps_second_nested_level() {
        use super::super::proto::{
            TaskToolCallDelta, TextDelta, ToolCallDelta, ToolCallDeltaUpdate,
        };

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut logical = LogicalToolTracker::default();
        let allowed = BTreeSet::from(["Workflow".to_string()]);

        let inner = InteractionUpdate {
            text_delta: Some(TextDelta {
                text: "secret".into(),
            }),
            ..Default::default()
        };
        let cont = drive_task_frame(
            task_nested_frame(InteractionUpdate {
                text_delta: Some(TextDelta {
                    text: "visible".into(),
                }),
                tool_call_delta: Some(ToolCallDeltaUpdate {
                    call_id: "task-inner".into(),
                    model_call_id: "model-1".into(),
                    tool_call_delta: Some(ToolCallDelta {
                        task_tool_call_delta: Some(TaskToolCallDelta {
                            interaction_update: Some(Box::new(inner)),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            &outbound,
            &mut sink,
            &mut pending,
            &pending_shared,
            &mut logical,
            &allowed,
        )
        .await;
        assert!(cont, "capped nest must keep parent BiDi");
        let event = event_rx.try_recv().expect("visible text");
        match event {
            Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) => {
                assert_eq!(text, "visible");
            }
            other => panic!("expected visible TextDelta, got {other:?}"),
        }
        assert!(
            event_rx.try_recv().is_err(),
            "second nested Task level must not surface"
        );
    }

    async fn client_only_tools_from_mcp_started(
        tool_name: &str,
        name: &str,
        provider: &str,
    ) -> Vec<LiveNativeTool> {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, McpArgs, McpToolCall, ToolCall, ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: None,
                tool_call_started: Some(ToolCallStarted {
                    call_id: "mcp-wf-q".into(),
                    model_call_id: "model-q".into(),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(McpToolCall {
                            args: Some(McpArgs {
                                name: name.into(),
                                tool_name: tool_name.into(),
                                tool_call_id: "mcp-wf-q".into(),
                                provider_identifier: provider.into(),
                                args: {
                                    let mut m = std::collections::HashMap::new();
                                    m.insert("name".into(), br#""deep-research""#.to_vec());
                                    m
                                },
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                partial_tool_call: None,
                tool_call_delta: None,
                turn_ended: None,
            }),
            exec_server_message: None,
            kv_server_message: None,
            interaction_query: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["Workflow".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(
            !cont,
            "qualified MCP Workflow ({tool_name}) must end BiDi segment"
        );
        let event = event_rx.recv().await.expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => tools,
            other => panic!("expected NativeToolBatch for {tool_name}, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mcp_cursor_catalog_spawn_name_exposes_spawn_subagent() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, McpArgs, McpToolCall, ToolCall, ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "mcp-spawn-1".into(),
                    model_call_id: "model-1".into(),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(McpToolCall {
                            args: Some(McpArgs {
                                name: "mcp_claude-local_spawn_subagent".into(),
                                tool_name: "mcp_claude-local_spawn_subagent".into(),
                                tool_call_id: "mcp-spawn-1".into(),
                                provider_identifier: "claude-local".into(),
                                args: {
                                    let mut m = std::collections::HashMap::new();
                                    m.insert("prompt".into(), br#""find TaskToolCall""#.to_vec());
                                    m.insert("description".into(), br#""explore""#.to_vec());
                                    m
                                },
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();
        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(frames.into_iter().next().unwrap(), Some(&allowed), None).await;
        assert!(!cont, "catalog MCP spawn must expose and tear down");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "spawn_subagent");
                assert_eq!(
                    tools[0].input.get("prompt").and_then(|v| v.as_str()),
                    Some("find TaskToolCall")
                );
            }
            other => panic!("expected spawn_subagent, got {other:?}"),
        }
    }

    fn mcp_spawn_started_frame(call_id: &str, prompt: &str) -> super::super::connect::ConnectFrame {
        use super::super::connect::{ConnectFrameDecoder, encode_connect_frame};
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, McpArgs, McpToolCall, ToolCall, ToolCallStarted,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: call_id.into(),
                    model_call_id: format!("model-{call_id}"),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(McpToolCall {
                            args: Some(McpArgs {
                                name: "mcp_claude-local_spawn_subagent".into(),
                                tool_name: "mcp_claude-local_spawn_subagent".into(),
                                tool_call_id: call_id.into(),
                                provider_identifier: "claude-local".into(),
                                args: {
                                    let mut m = std::collections::HashMap::new();
                                    m.insert("prompt".into(), format!("\"{prompt}\"").into_bytes());
                                    m.insert("description".into(), br#""explore""#.to_vec());
                                    m
                                },
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = ConnectFrameDecoder::new();
        decoder.push(&framed).unwrap().into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn sibling_lifecycle_spawns_share_one_batch() {
        use super::super::proto::RequestContext;

        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id: "sess-sib",
            user_prompt: "spawn two",
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        for (call_id, prompt) in [("spawn-a", "first"), ("spawn-b", "second")] {
            let cont = process_live_frame(
                mcp_spawn_started_frame(call_id, prompt),
                &outbound,
                &mut sink,
                &mut deferred,
                &mut pending,
                &pending_shared,
                &mut kv_blobs,
                &mut latest_checkpoint,
                &terminal_error,
                Some(&allowed),
                &mut saw_text,
                &mut useful,
                &mut logical,
                &mut last_progress,
                Duration::from_millis(25),
                &mut xml_parser,
                Some(&mut turn),
            )
            .await;
            assert!(
                cont,
                "sibling {call_id} must stay on the BiDi long enough to collect the pair"
            );
        }
        assert!(
            !expose_collected_tools(&mut pending, &pending_shared, &mut sink).await,
            "lifecycle batch must tear down BiDi after both siblings"
        );
        match event_rx.recv().await.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(
                    tools.len(),
                    2,
                    "both sibling spawns must survive: {tools:?}"
                );
                assert!(tools.iter().all(|tool| tool.name == "spawn_subagent"));
                let prompts: Vec<_> = tools
                    .iter()
                    .filter_map(|tool| tool.input.get("prompt").and_then(|v| v.as_str()))
                    .collect();
                assert!(prompts.contains(&"first"), "{prompts:?}");
                assert!(prompts.contains(&"second"), "{prompts:?}");
            }
            other => panic!("expected sibling spawn batch, got {other:?}"),
        }
    }

    fn xml_parameter_spawn(id: &str, prompt: &str) -> String {
        format!(
            concat!(
                r#"<tool_use id="{id}" name="spawn_subagent">"#,
                r#"<parameter name="background">true</parameter>"#,
                r#"<parameter name="description">{id}</parameter>"#,
                r#"<parameter name="prompt">{prompt}</parameter>"#,
                r#"<parameter name="subagent_type">general-purpose</parameter>"#,
                "</tool_use>",
            ),
            id = id,
            prompt = prompt
        )
    }

    #[tokio::test]
    async fn xml_lifecycle_spawns_wait_for_turn_end_across_chunks() {
        use super::super::proto::RequestContext;

        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id: "sess-xml-spawn",
            user_prompt: "spawn 64",
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        for (call_id, prompt) in [("spawn-40", "first"), ("spawn-41", "second")] {
            let cont = process_live_frame(
                xml_tool_use_frame(&xml_parameter_spawn(call_id, prompt)),
                &outbound,
                &mut sink,
                &mut deferred,
                &mut pending,
                &pending_shared,
                &mut kv_blobs,
                &mut latest_checkpoint,
                &terminal_error,
                Some(&allowed),
                &mut saw_text,
                &mut useful,
                &mut logical,
                &mut last_progress,
                Duration::from_millis(25),
                &mut xml_parser,
                Some(&mut turn),
            )
            .await;
            assert!(
                cont,
                "XML {call_id} must stay on the BiDi until turn_ended so later chunks can join"
            );
            assert!(
                !pending.collecting_has_lifecycle(),
                "XML lifecycle must not arm the MCP sibling flush after {call_id}"
            );
        }
        assert!(
            event_rx.try_recv().is_err(),
            "XML spawns must not emit a batch until turn_ended"
        );
        assert!(
            !expose_collected_tools(&mut pending, &pending_shared, &mut sink).await,
            "turn_ended flush must tear down BiDi after both XML spawns"
        );
        match event_rx.recv().await.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 2, "both XML spawns must survive: {tools:?}");
                assert!(tools.iter().all(|tool| tool.name == "spawn_subagent"));
                let prompts: Vec<_> = tools
                    .iter()
                    .filter_map(|tool| tool.input.get("prompt").and_then(|v| v.as_str()))
                    .collect();
                assert!(prompts.contains(&"first"), "{prompts:?}");
                assert!(prompts.contains(&"second"), "{prompts:?}");
            }
            other => panic!("expected XML spawn batch, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mcp_spawn_protobuf_value_args_are_clean_json() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, InteractionUpdate, McpArgs, McpToolCall, ToolCall, ToolCallStarted,
        };
        use prost::Message;
        use prost_types::value::Kind;

        fn proto_value_bytes(kind: Kind) -> Vec<u8> {
            prost_types::Value { kind: Some(kind) }.encode_to_vec()
        }

        let mut full = Vec::new();
        AgentServerMessage {
            interaction_update: Some(InteractionUpdate {
                tool_call_started: Some(ToolCallStarted {
                    call_id: "mcp-spawn-proto".into(),
                    model_call_id: "model-proto".into(),
                    tool_call: Some(ToolCall {
                        mcp_tool_call: Some(McpToolCall {
                            args: Some(McpArgs {
                                name: "mcp_claude-local_spawn_subagent".into(),
                                tool_name: "mcp_claude-local_spawn_subagent".into(),
                                tool_call_id: "mcp-spawn-proto".into(),
                                provider_identifier: "claude-local".into(),
                                args: {
                                    let mut m = std::collections::HashMap::new();
                                    m.insert(
                                        "description".into(),
                                        proto_value_bytes(Kind::StringValue(
                                            "SPAWN smoke test".into(),
                                        )),
                                    );
                                    m.insert(
                                        "prompt".into(),
                                        proto_value_bytes(Kind::StringValue(
                                            "Reply with exactly one line: SPAWN_OK".into(),
                                        )),
                                    );
                                    m.insert(
                                        "subagent_type".into(),
                                        proto_value_bytes(Kind::StringValue(
                                            "general-purpose".into(),
                                        )),
                                    );
                                    m.insert(
                                        "background".into(),
                                        proto_value_bytes(Kind::BoolValue(true)),
                                    );
                                    m
                                },
                            }),
                        }),
                        ..Default::default()
                    }),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();
        let allowed = BTreeSet::from(["spawn_subagent".to_string()]);
        let (cont, event, _) =
            drive_native_task_frame(frames.into_iter().next().unwrap(), Some(&allowed), None).await;
        assert!(!cont, "protobuf MCP spawn must expose and tear down");
        match event.expect("NativeToolBatch") {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "spawn_subagent");
                assert_eq!(
                    tools[0].input.get("description").and_then(|v| v.as_str()),
                    Some("SPAWN smoke test")
                );
                assert_eq!(
                    tools[0].input.get("prompt").and_then(|v| v.as_str()),
                    Some("Reply with exactly one line: SPAWN_OK")
                );
                assert_eq!(
                    tools[0].input.get("subagent_type").and_then(|v| v.as_str()),
                    Some("general-purpose")
                );
                assert_eq!(
                    tools[0].input.get("background"),
                    Some(&serde_json::json!(true))
                );
            }
            other => panic!("expected clean spawn_subagent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mcp_qualified_workflow_name_exposes_client_only_as_workflow() {
        for (tool_name, name, provider) in [
            (
                "claude-local/Workflow",
                "claude-local/Workflow",
                "claude-local",
            ),
            ("claude-local:Workflow", "Workflow", "claude-local"),
            ("Workflow", "claude-local/Workflow", "claude-local"),
            (
                "mcp_claude-local_Workflow",
                "mcp_claude-local_Workflow",
                "claude-local",
            ),
            (
                "mcp__claude-local__Workflow",
                "mcp__claude-local__Workflow",
                "claude-local",
            ),
        ] {
            let tools = client_only_tools_from_mcp_started(tool_name, name, provider).await;
            assert_eq!(tools.len(), 1, "{tool_name} / {name}");
            assert_eq!(
                tools[0].name, "Workflow",
                "Anthropic tool_use.name must be Workflow, not the qualified MCP name {tool_name}"
            );
            assert_eq!(
                tools[0].input.get("name").and_then(|v| v.as_str()),
                Some("deep-research")
            );
            assert!(
                tools[0].input.get("provider_identifier").is_none(),
                "Workflow Anthropic input must not include provider_identifier"
            );
        }
    }

    #[tokio::test]
    async fn workflow_xml_with_same_chunk_turn_ended_exposes_tool_use() {
        // Regression: Fable often emits Workflow `<tool_use>` XML and turn_ended
        // in one InteractionUpdate. Queuing without exposing raced into
        // "pending native tools" and dropped the Anthropic tool_use (Out=0 hang).
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{AgentServerMessage, InteractionUpdate, TextDelta, TurnEnded};
        use prost::Message;

        let xml = r#"<tool_use id="wf-1" name="Workflow">{"name":"deep-research"}</tool_use>"#;
        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: Some(TextDelta {
                    text: xml.to_string(),
                }),
                tool_call_started: None,
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                partial_tool_call: None,
                tool_call_delta: None,
                turn_ended: Some(TurnEnded {
                    input_tokens: Some(10),
                    output_tokens: Some(2),
                    cache_read_tokens: None,
                    cache_write_tokens: None,
                    reasoning_tokens: None,
                }),
            }),
            exec_server_message: None,
            kv_server_message: None,
            interaction_query: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();
        assert_eq!(frames.len(), 1);

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["Workflow".to_string(), "Read".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        // Client-only expose ends the BiDi segment (return false).
        assert!(!cont, "Workflow expose must end the live segment");
        assert!(
            terminal_error.lock().unwrap().is_none(),
            "must not treat Workflow+turn_ended as pending-native failure"
        );
        assert!(sink.is_none(), "Anthropic segment closes after tool_use");

        let event = event_rx.recv().await.expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "Workflow");
                assert_eq!(
                    tools[0].input.get("name").and_then(|v| v.as_str()),
                    Some("deep-research")
                );
            }
            other => panic!("expected NativeToolBatch, got {other:?}"),
        }
        let shared = pending_shared.lock().unwrap();
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].claude_name, "Workflow");
    }

    #[tokio::test]
    async fn flag_end_with_workflow_emits_tool_use() {
        use super::super::connect::{FLAG_END, encode_connect_frame};

        let framed = encode_connect_frame(b"", FLAG_END);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();
        assert_eq!(frames.len(), 1);

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["Workflow".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));
        let prompt = r#"Invoke: Workflow({ name: "deep-research", args: "why rust?" })"#;
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id: "sess-test",
            user_prompt: prompt,
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            Some(&mut turn),
        )
        .await;
        assert!(!cont, "Workflow tool_use ends the live segment");
        let event = event_rx.try_recv().expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "Workflow");
                assert_eq!(
                    tools[0].input.get("name").and_then(|v| v.as_str()),
                    Some("deep-research")
                );
                assert_eq!(
                    tools[0].input.get("args").and_then(|v| v.as_str()),
                    Some("why rust?")
                );
            }
            other => panic!("expected Workflow tool_use, got {other:?}"),
        }
        assert!(event_rx.try_recv().is_err(), "must not also emit End/note");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn empty_turn_does_not_invent_claude_workflow_for_grok() {
        use super::super::connect::{FLAG_END, encode_connect_frame};

        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        super::super::conversation::reset_for_test();
        let session_id = "sess-grok-empty-turn";
        let original =
            super::super::conversation::continuation_for(Some(session_id)).conversation_id;
        let stale_blobs = HashMap::from([(vec![0xaa], vec![0xbb])]);
        super::super::conversation::save_checkpoint(session_id, vec![0x08, 0x01]);
        super::super::conversation::merge_blobs(session_id, &stale_blobs);

        let framed = encode_connect_frame(b"", FLAG_END);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();
        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = stale_blobs;
        let mut latest_checkpoint = Some(vec![0x08, 0x01]);
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["workflow".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id,
            user_prompt: "用 workflow 按原 nonce 一次扇出 64 人",
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            Some(&mut turn),
        )
        .await;
        assert!(!cont, "FLAG_END must still end the live segment");
        assert!(!saw_text, "a proxy diagnostic must not become model text");
        let error = event_rx
            .try_recv()
            .expect("retryable empty-turn error")
            .expect_err("empty grok turn must fail instead of succeeding");
        assert!(error.contains("without text or tool calls"), "{error}");
        assert!(
            live_error_is_same_request_retryable(&error),
            "empty turn must retry within the same client request"
        );
        assert!(
            error.contains(CONVERSATION_RESET_RETRY_NOTE),
            "the retry must replay from a fresh Cursor conversation: {error}"
        );
        assert!(
            event_rx.try_recv().is_err(),
            "empty turn must not also emit End"
        );
        assert!(pending.is_empty(), "must not queue a synthetic workflow");
        assert!(latest_checkpoint.is_none());
        assert!(kv_blobs.is_empty());
        let recovered = super::super::conversation::continuation_for(Some(session_id));
        assert_ne!(recovered.conversation_id, original);
        assert!(!recovered.has_checkpoint);
        assert!(recovered.pre_fetched_blobs.is_empty());
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn checkpoint_continue_empty_turn_keeps_completed_tool_state() {
        let _guard = super::super::conversation::STORE_TEST_LOCK.lock().unwrap();
        super::super::conversation::reset_for_test();
        let session_id = "sess-grok-checkpoint-continue-empty-turn";
        let original =
            super::super::conversation::continuation_for(Some(session_id)).conversation_id;
        let checkpoint = vec![0x08, 0x02];
        let blobs = HashMap::from([(vec![0xcc], vec![0xdd])]);
        super::super::conversation::save_checkpoint(session_id, checkpoint.clone());
        super::super::conversation::merge_blobs(session_id, &blobs);

        let (event_tx, mut event_rx) = mpsc::channel(2);
        let mut sink = Some(event_tx);
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut latest_checkpoint = Some(checkpoint.clone());
        let mut kv_blobs = blobs.clone();

        assert!(
            !recover_empty_turn_if_needed(
                &mut saw_text,
                &mut useful,
                &mut sink,
                &mut pending,
                &pending_shared,
                &terminal_error,
                None,
                "Continue from the completed tool results in this Cursor conversation. \
                 Produce the final answer requested by the user now. \
                 Do not repeat completed tool calls.",
                Some(session_id),
                &mut latest_checkpoint,
                &mut kv_blobs,
                "turn_ended",
            )
            .await
        );

        let error = event_rx
            .recv()
            .await
            .expect("retryable checkpoint continuation error")
            .expect_err("an empty continuation must retry");
        assert!(
            !error.contains(CONVERSATION_RESET_RETRY_NOTE),
            "completed tool state must not be discarded: {error}"
        );
        assert_eq!(latest_checkpoint.as_deref(), Some(checkpoint.as_slice()));
        assert_eq!(kv_blobs, blobs);
        let recovered = super::super::conversation::continuation_for(Some(session_id));
        assert_eq!(recovered.conversation_id, original);
        assert!(recovered.has_checkpoint);
        assert_eq!(recovered.pre_fetched_blobs.len(), 1);
        assert!(
            recovered
                .pre_fetched_blobs
                .contains(&(vec![0xcc], vec![0xdd]))
        );
    }

    #[tokio::test]
    async fn flag_end_without_workflow_emits_retryable_error() {
        use super::super::connect::{FLAG_END, encode_connect_frame};

        let framed = encode_connect_frame(b"", FLAG_END);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();

        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["Read".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(!cont, "FLAG_END must end the live segment");
        assert!(!saw_text, "a proxy diagnostic must not become model text");

        let error = event_rx
            .try_recv()
            .expect("retryable empty-turn error")
            .expect_err("empty turn must not be reported as a success");
        assert!(error.contains("without text or tool calls"), "{error}");
        assert!(
            live_error_is_same_request_retryable(&error),
            "empty turn must trigger the bounded same-request retry path"
        );
        assert!(
            event_rx.try_recv().is_err(),
            "empty turn must not also emit End"
        );
    }

    #[test]
    fn parse_injected_workflow_invoke_and_run_the() {
        let (name, args) = parse_injected_workflow(
            r#"Invoke: Workflow({ name: "deep-research", args: "what is rust?" })"#,
        )
        .unwrap();
        assert_eq!(name, "deep-research");
        assert_eq!(args, "what is rust?");

        let (name, args) = parse_injected_workflow(r#"Run the "deep-research" workflow."#).unwrap();
        assert_eq!(name, "deep-research");
        assert_eq!(args, "");
    }

    fn text_delta_event(text: &str) -> LiveEventResult {
        Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
            text: text.into(),
        }))
    }

    #[test]
    fn coalescer_passes_through_when_queue_healthy() {
        let mut coalescer = LiveDeltaCoalescer::default();
        let out = coalescer.ingest(text_delta_event("a"), LIVE_EVENT_CHANNEL_CAP);
        assert_eq!(out.len(), 1);
        assert!(coalescer.flush().is_none());
    }

    #[test]
    fn coalescer_merges_consecutive_text_under_backpressure() {
        let mut coalescer = LiveDeltaCoalescer::default();
        let remaining = LIVE_EVENT_CHANNEL_CAP / 4;
        assert!(
            coalescer
                .ingest(text_delta_event("hello"), remaining)
                .is_empty()
        );
        assert!(
            coalescer
                .ingest(text_delta_event(" world"), remaining)
                .is_empty()
        );
        match coalescer.flush() {
            Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }))) => {
                assert_eq!(text, "hello world");
            }
            other => panic!("expected merged text, got {other:?}"),
        }
    }

    #[test]
    fn coalescer_does_not_merge_across_tool_batch() {
        let mut coalescer = LiveDeltaCoalescer::default();
        let remaining = LIVE_EVENT_CHANNEL_CAP / 4;
        assert!(
            coalescer
                .ingest(text_delta_event("hello"), remaining)
                .is_empty()
        );
        let out = coalescer.ingest(Ok(LiveRunEvent::NativeToolBatch(Vec::new())), remaining);
        assert_eq!(out.len(), 2, "flush text then pass the tool batch");
        assert!(
            coalescer
                .ingest(text_delta_event("after"), remaining)
                .is_empty()
        );
        match coalescer.flush() {
            Some(Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text }))) => {
                assert_eq!(text, "after");
            }
            other => panic!("expected post-tool text, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ask_user_question_advertised_exposes_client_only() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, AskQuestionArgs, AskQuestionInteractionQuery, AskQuestionItem,
            InteractionQuery,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: Some(InteractionQuery {
                id: 7,
                ask_question_interaction_query: Some(AskQuestionInteractionQuery {
                    args: Some(AskQuestionArgs {
                        title: "Choose a path forward now".into(),
                        questions: vec![AskQuestionItem {
                            id: "q1".into(),
                            prompt: "Which approach".into(),
                        }],
                    }),
                    tool_call_id: "ask-1".into(),
                }),
                ..Default::default()
            }),
            exec_server_message: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();

        let (request_tx, mut request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["AskUserQuestion".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(!cont, "AskUserQuestion expose tears down like Workflow");
        assert!(
            request_rx.try_recv().is_err(),
            "must not auto-reject advertised AskUserQuestion"
        );
        let event = event_rx.try_recv().expect("NativeToolBatch");
        match event {
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                assert_eq!(tools.len(), 1);
                assert_eq!(tools[0].name, "AskUserQuestion");
                let questions = tools[0]
                    .input
                    .get("questions")
                    .and_then(|v| v.as_array())
                    .expect("questions");
                assert_eq!(questions.len(), 1);
                assert_eq!(
                    questions[0].get("question").and_then(|v| v.as_str()),
                    Some("Which approach?")
                );
                let header = questions[0]
                    .get("header")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                assert!(header.chars().count() <= ASK_USER_QUESTION_HEADER_MAX);
                let options = questions[0]
                    .get("options")
                    .and_then(|v| v.as_array())
                    .expect("options");
                assert!(
                    (2..=4).contains(&options.len()),
                    "AskUserQuestion options must be 2-4, got {}",
                    options.len()
                );
            }
            other => panic!("expected AskUserQuestion tool_use, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn ask_question_unadvertised_is_rejected() {
        use super::super::connect::encode_connect_frame;
        use super::super::proto::{
            AgentServerMessage, AskQuestionArgs, AskQuestionInteractionQuery, AskQuestionItem,
            InteractionQuery,
        };
        use prost::Message;

        let mut full = Vec::new();
        AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: Some(InteractionQuery {
                id: 7,
                ask_question_interaction_query: Some(AskQuestionInteractionQuery {
                    args: Some(AskQuestionArgs {
                        title: "Choose".into(),
                        questions: vec![AskQuestionItem {
                            id: "q1".into(),
                            prompt: "Go?".into(),
                        }],
                    }),
                    tool_call_id: "ask-2".into(),
                }),
                ..Default::default()
            }),
            exec_server_message: None,
        }
        .encode(&mut full)
        .unwrap();
        let framed = encode_connect_frame(full, 0);
        let mut decoder = super::super::connect::ConnectFrameDecoder::new();
        let frames = decoder.push(&framed).unwrap();

        let (request_tx, mut request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let allowed = BTreeSet::from(["Read".to_string()]);
        let mut xml_parser = CursorToolUseXmlParser::new(Some(allowed.clone()));

        let cont = process_live_frame(
            frames.into_iter().next().unwrap(),
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            Some(&allowed),
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            None,
        )
        .await;
        assert!(cont, "rejecting AskQuestion must keep BiDi");
        assert!(event_rx.try_recv().is_err(), "must not expose tool_use");
        let reply = request_rx.try_recv().expect("reject frame");
        let decoded = super::super::client::decode_upstream_frames(&reply.unwrap()).unwrap();
        let message = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        assert!(
            message
                .interaction_response
                .as_ref()
                .and_then(|r| r.ask_question_interaction_response.as_ref())
                .and_then(|r| r.result.as_ref())
                .and_then(|r| r.rejected.as_ref())
                .is_some(),
            "unadvertised AskQuestion must still be rejected"
        );
    }

    #[tokio::test]
    async fn prost_decode_failure_skips_frame_without_502() {
        let (request_tx, _request_rx) = mpsc::channel(4);
        let outbound = ClientOutbound::Bidi(request_tx);
        let (event_tx, _event_rx) = mpsc::channel(8);
        let mut sink = Some(event_tx);
        let mut deferred = VecDeque::new();
        let mut pending = PendingExecState::default();
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let mut kv_blobs = HashMap::new();
        let mut latest_checkpoint = None;
        let terminal_error = Arc::new(Mutex::new(None));
        let mut saw_text = false;
        let mut useful = false;
        let mut logical = LogicalToolTracker::default();
        let mut last_progress = Instant::now();
        let mut xml_parser = CursorToolUseXmlParser::new(None);
        let prompt = "";
        let request_context = RequestContext::default();
        let mut decode_failures = 0;
        let mut coalescer = LiveDeltaCoalescer::default();
        let mut turn = LiveTurnCtx {
            session_id: "sess-test",
            user_prompt: prompt,
            request_context: &request_context,
            decode_failures: &mut decode_failures,
            coalescer: &mut coalescer,
        };
        let frame = ConnectFrame {
            flags: super::super::connect::FLAG_GZIP,
            payload: Bytes::from_static(&[0xff, 0x00, 0x00]),
        };
        let cont = process_live_frame(
            frame,
            &outbound,
            &mut sink,
            &mut deferred,
            &mut pending,
            &pending_shared,
            &mut kv_blobs,
            &mut latest_checkpoint,
            &terminal_error,
            None,
            &mut saw_text,
            &mut useful,
            &mut logical,
            &mut last_progress,
            Duration::from_millis(50),
            &mut xml_parser,
            Some(&mut turn),
        )
        .await;
        assert!(cont, "a single prost decode error must skip, not 502");
        assert_eq!(decode_failures, 1);
        assert!(terminal_error.lock().unwrap().is_none());
    }

    #[test]
    fn anthropic_ping_sse_bytes_are_nonempty() {
        let bytes = format_sse_event_bytes(EVENT_PING, &serde_json::json!({ "type": "ping" }));
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("event: ping"));
        assert!(text.contains(r#""type":"ping"#) || text.contains(r#""type": "ping"#));
    }

    #[test]
    fn empty_live_event_emits_keepalive_when_due() {
        let now = tokio::time::Instant::now();
        assert!(
            !sse_keepalive_after_empty_event(now, now + Duration::from_secs(15)),
            "a future ping deadline must keep draining"
        );
        assert!(
            sse_keepalive_after_empty_event(now, now),
            "an already-due ping must win over another no-byte event"
        );
        let mut encoder = CursorSseEncoder::new("msg_ping", "claude-fable-5");
        encoder.begin();
        let _ = encoder.take_bytes();
        apply_live_run_event(
            &mut encoder,
            LiveRunEvent::Cursor(CursorStreamEvent::OutputTokenDelta { tokens: 4 }),
        );
        assert!(
            encoder.take_bytes().is_empty(),
            "OutputTokenDelta must stay a no-byte event so the keepalive path is the one that fires"
        );
    }

    #[test]
    fn unknown_interaction_query_does_not_use_ask_question_oneof() {
        use super::super::proto::InteractionQuery;

        let query = InteractionQuery {
            id: 99,
            ..Default::default()
        };
        let result = encode_interaction_auto_response(&query);
        assert!(
            result.is_err(),
            "unmatched InteractionQuery must error instead of AskQuestion oneof"
        );
    }

    #[test]
    fn completed_terminal_failure_does_not_look_generating() {
        let _registry = lock_live_registry_for_test();
        let session = format!("fail-session-{}", uuid::Uuid::new_v4());
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "failed-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(vec![pending_exec(1, "tool-a")])),
            terminal_error: Arc::new(Mutex::new(Some(TerminalOutcome {
                message: "idle timeout".into(),
                created_at: Instant::now(),
            }))),
            completed: Arc::new(AtomicBool::new(true)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        if reservation.insert(handle).is_err() {
            panic!("insert running handle");
        }

        // get() must not surface completed failures as "still generating"
        assert!(LiveRunRegistry::get(&session).is_none());
        assert_eq!(
            LiveRunRegistry::take_terminal_error(&session).as_deref(),
            Some("idle timeout")
        );
        assert!(LiveRunRegistry::get(&session).is_none());
    }

    #[tokio::test]
    async fn report_terminal_error_always_stashes_even_with_live_sink() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut sink = Some(tx);
        let terminal_error = Arc::new(Mutex::new(None));
        report_terminal_error(&mut sink, &terminal_error, "boom".into()).await;
        assert!(
            terminal_error
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|o| o.message == "boom")
        );
        let event = rx.recv().await.expect("error event");
        assert!(event.is_err());
    }

    #[tokio::test]
    async fn thinking_deltas_are_not_dropped_when_sse_channel_is_full() {
        // Capacity 1: first fill, second must await rather than the old 5ms-drop path.
        let (tx, mut rx) = mpsc::channel(1);
        let mut sink = Some(tx);
        assert!(
            send_live_event(
                &mut sink,
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta {
                    text: "first".into(),
                })),
            )
            .await
        );

        let send = tokio::spawn(async move {
            send_live_event(
                &mut sink,
                Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta {
                    text: "second-must-not-drop".into(),
                })),
            )
            .await
        });

        // Give the spawned send a chance to block on the full channel.
        tokio::task::yield_now().await;
        let first = rx.recv().await.expect("first delta");
        match first {
            Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta { text })) => {
                assert_eq!(text, "first");
            }
            other => panic!("unexpected first event: {other:?}"),
        }
        assert!(send.await.expect("join"), "second send must succeed");
        let second = rx.recv().await.expect("second delta");
        match second {
            Ok(LiveRunEvent::Cursor(CursorStreamEvent::ThinkingDelta { text })) => {
                assert_eq!(text, "second-must-not-drop");
            }
            other => panic!("unexpected second event: {other:?}"),
        }
    }

    #[test]
    fn live_encoder_seed_and_token_delta_reach_monitor() {
        use crate::monitor::{EndpointKind, MonitorHandle};

        let monitor = MonitorHandle::new(16);
        monitor.request_started(
            "req-live",
            Some("sess".into()),
            None,
            EndpointKind::Messages,
        );
        monitor.upstream_started("req-live");

        let mut encoder = CursorSseEncoder::new("msg_test", "claude-fable-5");
        encoder.seed_estimated_input_tokens(1_200);
        encoder.push_event(&CursorStreamEvent::OutputTokenDelta { tokens: 7 });
        let (input, output) = encoder.current_usage();
        assert_eq!(input, 1_200);
        assert_eq!(output, 7);

        monitor.stream_progress("req-live", 64, 1, Some(input), Some(output));
        let active = &monitor.snapshot().active[0];
        assert_eq!(active.input_tokens, Some(1_200));
        assert_eq!(active.output_tokens, Some(7));
    }

    #[test]
    fn consecutive_text_deltas_flush_as_separate_sse_chunks() {
        // Mirrors live_sse_response: one LiveRunEvent → take_bytes() → one HTTP
        // chunk. Coalescing consecutive deltas would merge "A"+"B" into a single
        // content_block_delta and make Claude Code paint in bursts.
        let mut encoder = CursorSseEncoder::new("msg_rt", "claude-fable-5");
        encoder.begin();
        let _ = encoder.take_bytes();

        apply_live_run_event(
            &mut encoder,
            LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text: "A".into() }),
        );
        let first = String::from_utf8(encoder.take_bytes()).unwrap();
        assert!(
            first.contains("content_block_delta") && first.contains("\"text\":\"A\""),
            "first chunk missing A: {first}"
        );

        apply_live_run_event(
            &mut encoder,
            LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text: "B".into() }),
        );
        let second = String::from_utf8(encoder.take_bytes()).unwrap();
        assert!(
            second.contains("content_block_delta") && second.contains("\"text\":\"B\""),
            "second chunk missing B: {second}"
        );
        assert!(
            !second.contains("\"text\":\"AB\"") && !second.contains("\"text\":\"A\""),
            "deltas must not be coalesced across flushes: {second}"
        );
    }

    #[test]
    fn terminal_error_is_atomically_consumed_with_registry_entry() {
        let _registry = lock_live_registry_for_test();
        LiveRunRegistry::clear();
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "terminal-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(Some(TerminalOutcome {
                message: "upstream ended with pending tools".into(),
                created_at: Instant::now(),
            }))),
            completed: Arc::new(AtomicBool::new(true)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        let reservation = LiveRunRegistry::reserve("terminal-session").expect("reserve");
        if reservation.insert(handle).is_err() {
            panic!("insert running handle");
        }

        assert_eq!(
            LiveRunRegistry::take_terminal_error("terminal-session").as_deref(),
            Some("upstream ended with pending tools")
        );
        assert!(LiveRunRegistry::get("terminal-session").is_none());
    }

    #[test]
    fn stale_generation_cannot_consume_a_replacement_terminal_error() {
        let _registry = lock_live_registry_for_test();
        LiveRunRegistry::clear();
        let session = format!("terminal-generation-{}", uuid::Uuid::new_v4());
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "replacement-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(Some(TerminalOutcome {
                message: "replacement failed".into(),
                created_at: Instant::now(),
            }))),
            completed: Arc::new(AtomicBool::new(true)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        LiveRunRegistry::reserve(&session)
            .expect("reserve")
            .insert(handle)
            .expect("insert replacement");

        assert!(LiveRunRegistry::take_terminal_error_for_run(&session, None, "old-run").is_none());
        assert_eq!(
            LiveRunRegistry::take_terminal_error_for_run(&session, None, "replacement-run")
                .as_deref(),
            Some("replacement failed")
        );
        LiveRunRegistry::clear();
    }

    #[test]
    fn logical_tool_tracking_keeps_other_parallel_start_pending() {
        let mut waiting = LogicalToolTracker::default();
        waiting.started("call-a", "model-a");
        waiting.started("call-b", "model-a");
        waiting.completed("call-a", "model-a");
        assert_eq!(waiting.len(), 1);

        let exec = pending_exec(2, "call-b");
        waiting.resolve_exec(&exec);
        assert!(waiting.is_empty());
    }

    #[test]
    fn logical_tool_tracking_counts_anonymous_siblings_per_model_call() {
        let mut waiting = LogicalToolTracker::default();
        waiting.started("", "shared-model-call");
        waiting.started("", "shared-model-call");
        waiting.completed("", "shared-model-call");
        assert_eq!(waiting.len(), 1);
        waiting.completed("", "shared-model-call");
        assert!(waiting.is_empty());
    }

    #[test]
    fn tool_result_batch_encodes_each_result_and_close_in_pending_order() {
        let pending = vec![pending_exec(1, "tool-1"), pending_exec(2, "tool-2")];
        let frames = encode_tool_result_batch(
            &pending,
            &[
                (
                    "tool-2".into(),
                    serde_json::json!({"type":"tool_result","content":"two"}),
                ),
                (
                    "tool-1".into(),
                    serde_json::json!({"type":"tool_result","content":"one"}),
                ),
            ],
        )
        .unwrap();
        let body: Vec<u8> = frames.into_iter().flatten().collect();
        let decoded = super::super::client::decode_upstream_frames(&body).unwrap();
        let messages: Vec<AgentClientMessage> = decoded
            .iter()
            .map(|frame| AgentClientMessage::decode(frame.payload.as_ref()).unwrap())
            .collect();

        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0].exec_client_message.as_ref().unwrap().id, 1);
        assert_eq!(
            messages[0]
                .exec_client_message
                .as_ref()
                .unwrap()
                .read_result
                .as_ref()
                .unwrap()
                .success
                .as_ref()
                .unwrap()
                .content
                .as_deref(),
            Some("one")
        );
        assert_eq!(
            messages[1]
                .exec_client_control_message
                .as_ref()
                .unwrap()
                .stream_close
                .as_ref()
                .unwrap()
                .id,
            1
        );
        assert_eq!(messages[2].exec_client_message.as_ref().unwrap().id, 2);
        assert_eq!(
            messages[3]
                .exec_client_control_message
                .as_ref()
                .unwrap()
                .stream_close
                .as_ref()
                .unwrap()
                .id,
            2
        );
    }

    #[tokio::test]
    async fn live_driver_exposes_and_resumes_two_execs_as_one_batch() {
        fn server_frame(message: proto::AgentServerMessage) -> Bytes {
            let mut payload = Vec::new();
            message.encode(&mut payload).unwrap();
            encode_connect_frame(payload, 0)
        }

        fn read_exec(id: u32, tool_use_id: &str, path: &str) -> Bytes {
            server_frame(proto::AgentServerMessage {
                conversation_checkpoint_update: None,
                interaction_update: None,
                kv_server_message: None,
                interaction_query: None,
                exec_server_message: Some(ExecServerMessage {
                    id,
                    exec_id: Some(format!("exec-{id}")),
                    read_args: Some(ExecReadArgs {
                        path: path.into(),
                        tool_call_id: tool_use_id.into(),
                        offset: None,
                        limit: None,
                    }),
                    ..Default::default()
                }),
            })
        }

        let (upstream_tx, upstream_rx) = mpsc::channel::<Result<Option<Bytes>, String>>(8);
        let (request_tx, mut request_rx) = mpsc::channel(32);
        let (command_tx, command_rx) = mpsc::channel(8);
        let (initial_sink, mut first_events) = mpsc::channel(16);
        let pending_shared = Arc::new(Mutex::new(Vec::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let upstream_pump = tokio::spawn(std::future::pending::<()>());
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            upstream_pump,
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::clone(&pending_shared),
            Arc::clone(&terminal_error),
            Arc::clone(&completed),
            Arc::clone(&cancel_requested),
            Some(BTreeSet::from(["Read".into()])),
            "multi-test-session".into(),
            "multi-test-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            test_reconnect_context(),
            test_generation_permit(),
        ));
        let handle = CursorLiveRunHandle {
            run_id: "multi-test-run".into(),
            command_tx,
            pending: Arc::clone(&pending_shared),
            terminal_error,
            completed,
            cancel_requested,
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        };

        upstream_tx
            .send(Ok(Some(read_exec(11, "tool-11", "/one.txt"))))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        upstream_tx
            .send(Ok(Some(read_exec(12, "tool-12", "/two.txt"))))
            .await
            .unwrap();

        let batch = tokio::time::timeout(Duration::from_secs(2), first_events.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let LiveRunEvent::NativeToolBatch(tools) = batch else {
            panic!("expected native tool batch");
        };
        let tool_ids: Vec<String> = tools.iter().map(|tool| tool.tool_use_id.clone()).collect();
        assert_eq!(
            tool_ids,
            [
                "tool-11__cursor_run_multi-test-run",
                "tool-12__cursor_run_multi-test-run"
            ]
        );
        assert_eq!(handle.pending_tools().len(), 2);

        // Buffer more than the continuation sink capacity while Claude Code is
        // executing tools. A request-context round trip is an ordering barrier
        // proving the driver processed all 70 events before resume.
        let mut buffered = Vec::new();
        for index in 0..70 {
            buffered.extend_from_slice(&server_frame(proto::AgentServerMessage {
                conversation_checkpoint_update: None,
                interaction_update: Some(InteractionUpdate {
                    heartbeat: None,
                    text_delta: Some(TextDelta {
                        text: format!("buffered-{index}"),
                    }),
                    ..Default::default()
                }),
                kv_server_message: None,
                interaction_query: None,
                exec_server_message: None,
            }));
        }
        buffered.extend_from_slice(&server_frame(proto::AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: Some(ExecServerMessage {
                id: 99,
                exec_id: Some("context-barrier".into()),
                request_context_args: Some(RequestContextArgs::default()),
                ..Default::default()
            }),
        }));
        upstream_tx
            .send(Ok(Some(Bytes::from(buffered))))
            .await
            .unwrap();
        let barrier_frame = tokio::time::timeout(Duration::from_secs(1), request_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut barrier_decoder = ConnectFrameDecoder::new();
        let barrier = barrier_decoder.push(&barrier_frame).unwrap();
        assert!(
            AgentClientMessage::decode(barrier[0].payload.as_ref())
                .unwrap()
                .exec_client_message
                .unwrap()
                .request_context_result
                .is_some()
        );

        let mut second_events = tokio::time::timeout(
            Duration::from_secs(1),
            handle.resume_batch(vec![
                (
                    tool_ids[1].clone(),
                    serde_json::json!({"type":"tool_result","content":"two"}),
                ),
                (
                    tool_ids[0].clone(),
                    serde_json::json!({"type":"tool_result","content":"one"}),
                ),
            ]),
        )
        .await
        .expect("resume ack deadlocked behind a full continuation sink")
        .unwrap();

        let mut client_messages = Vec::new();
        for _ in 0..4 {
            let frame = tokio::time::timeout(Duration::from_secs(1), request_rx.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let mut decoder = ConnectFrameDecoder::new();
            let decoded = decoder.push(&frame).unwrap();
            client_messages.push(AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap());
        }
        assert_eq!(
            client_messages[0].exec_client_message.as_ref().unwrap().id,
            11
        );
        assert_eq!(
            client_messages[1]
                .exec_client_control_message
                .as_ref()
                .unwrap()
                .stream_close
                .as_ref()
                .unwrap()
                .id,
            11
        );
        assert_eq!(
            client_messages[2].exec_client_message.as_ref().unwrap().id,
            12
        );
        assert_eq!(
            client_messages[3]
                .exec_client_control_message
                .as_ref()
                .unwrap()
                .stream_close
                .as_ref()
                .unwrap()
                .id,
            12
        );

        for index in 0..70 {
            assert!(matches!(
                second_events.recv().await.unwrap().unwrap(),
                LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })
                    if text == format!("buffered-{index}")
            ));
        }

        upstream_tx
            .send(Ok(Some(server_frame(proto::AgentServerMessage {
                conversation_checkpoint_update: None,
                interaction_update: Some(InteractionUpdate {
                    heartbeat: None,
                    text_delta: Some(TextDelta {
                        text: "both results received".into(),
                    }),
                    ..Default::default()
                }),
                kv_server_message: None,
                interaction_query: None,
                exec_server_message: None,
            }))))
            .await
            .unwrap();
        upstream_tx
            .send(Ok(Some(server_frame(proto::AgentServerMessage {
                conversation_checkpoint_update: None,
                interaction_update: Some(InteractionUpdate {
                    heartbeat: None,
                    turn_ended: Some(TurnEnded {
                        input_tokens: Some(10),
                        output_tokens: Some(4),
                        cache_read_tokens: None,
                        cache_write_tokens: None,
                        reasoning_tokens: None,
                    }),
                    ..Default::default()
                }),
                kv_server_message: None,
                interaction_query: None,
                exec_server_message: None,
            }))))
            .await
            .unwrap();
        upstream_tx.send(Ok(None)).await.unwrap();

        assert!(matches!(
            second_events.recv().await.unwrap().unwrap(),
            LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })
                if text == "both results received"
        ));
        assert!(matches!(
            second_events.recv().await.unwrap().unwrap(),
            LiveRunEvent::Cursor(CursorStreamEvent::Usage { .. })
        ));
        assert!(matches!(
            second_events.recv().await.unwrap().unwrap(),
            LiveRunEvent::Cursor(CursorStreamEvent::End)
        ));
        tokio::time::timeout(Duration::from_secs(1), driver)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn acknowledged_cancel_waits_for_a_full_driver_channel() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        command_tx
            .try_send(RunCommand::Cancel { ack: None })
            .expect("prefill command channel");
        let completed = Arc::new(AtomicBool::new(false));
        let handle = CursorLiveRunHandle {
            run_id: "cancel-ack-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::clone(&completed),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        };
        let driver = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(matches!(
                command_rx.recv().await,
                Some(RunCommand::Cancel { ack: None })
            ));
            let Some(RunCommand::Cancel { ack: Some(ack) }) = command_rx.recv().await else {
                panic!("expected acknowledged cancellation");
            };
            completed.store(true, Ordering::Release);
            let _ = ack.send(());
        });

        handle
            .cancel_and_wait()
            .await
            .expect("cancellation should wait for delivery and acknowledgement");
        driver.await.expect("mock driver");
    }

    #[tokio::test]
    async fn cancellation_does_not_authorize_replacement_after_ambiguous_resume_send() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = CursorLiveRunHandle {
            run_id: "ambiguous-cancel-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(Some(TerminalOutcome {
                message:
                    "Cursor live open timed out (response-less ResumeAction send is ambiguous)"
                        .into(),
                created_at: Instant::now(),
            }))),
            completed: Arc::new(AtomicBool::new(true)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        };

        let error = handle
            .cancel_and_wait()
            .await
            .expect_err("ambiguous ResumeAction acceptance must block replacement");
        assert_eq!(error.status, 409);
        assert!(error.message.contains("ambiguous"), "{}", error.message);
    }

    #[test]
    fn ambiguous_terminal_outcome_survives_the_short_terminal_ttl() {
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = CursorLiveRunHandle {
            run_id: "aged-ambiguous-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(Some(TerminalOutcome {
                message: "response-less ResumeAction acceptance is ambiguous".into(),
                created_at: Instant::now() - Duration::from_secs(70),
            }))),
            completed: Arc::new(AtomicBool::new(true)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        };

        assert!(
            handle.has_terminal_error(),
            "ambiguity must remain registered for the full ambiguity window"
        );
        assert!(
            handle.ensure_replacement_is_safe().is_err(),
            "an aged ambiguous result must still block a replacement"
        );
    }

    #[tokio::test]
    async fn downstream_disconnect_during_resume_probation_is_ambiguous() {
        let (upstream_tx, upstream_rx) = mpsc::channel(1);
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (initial_sink, initial_events) = mpsc::channel(1);
        drop(initial_events);
        let terminal_error = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicBool::new(false));
        let mut reconnect = test_reconnect_context();
        reconnect.recovery.on_probation = true;
        reconnect.recovery.started = Some(Instant::now());
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx,
            tokio::spawn(std::future::pending::<()>()),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::new(Mutex::new(Vec::new())),
            Arc::clone(&terminal_error),
            Arc::clone(&completed),
            Arc::new(AtomicBool::new(false)),
            None,
            "probation-disconnect-session".into(),
            "probation-disconnect-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            reconnect,
            test_generation_permit(),
        ));

        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("driver timeout")
            .expect("driver");
        let message = terminal_error
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|outcome| outcome.message.clone())
            .expect("probation disconnect must be terminal");
        assert!(message.contains("ambiguous"), "{message}");
    }

    #[tokio::test]
    async fn cancellation_during_resume_probation_blocks_replacement() {
        let (upstream_tx, upstream_rx) = mpsc::channel(1);
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (command_tx, command_rx) = mpsc::channel(1);
        let (initial_sink, _initial_events) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(Vec::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let upstream_pump = tokio::spawn(std::future::pending::<()>());
        let mut reconnect = test_reconnect_context();
        reconnect.recovery.on_probation = true;
        reconnect.recovery.started = Some(Instant::now());
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx,
            upstream_pump,
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::clone(&pending),
            Arc::clone(&terminal_error),
            Arc::clone(&completed),
            Arc::clone(&cancel_requested),
            None,
            "probation-cancel-session".into(),
            "probation-cancel-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            reconnect,
            test_generation_permit(),
        ));
        let handle = CursorLiveRunHandle {
            run_id: "probation-cancel-run".into(),
            command_tx,
            pending,
            terminal_error,
            completed,
            cancel_requested,
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        };

        let error = handle
            .cancel_and_wait()
            .await
            .expect_err("accepted ResumeAction probation must block replacement");
        assert_eq!(error.status, 409);
        assert!(error.message.contains("ambiguous"), "{}", error.message);
        driver.await.expect("probation driver");
    }

    #[tokio::test]
    async fn cancelled_resume_guard_lives_until_the_queued_command_is_dropped() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "cancelled-resume-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(vec![pending_exec(1, "tool-1")])),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        let resume_handle = Arc::clone(&handle);
        let resume = tokio::spawn(async move {
            resume_handle
                .resume_batch(vec![(
                    "tool-1".into(),
                    serde_json::json!({"type":"tool_result","content":"done"}),
                )])
                .await
        });
        let queued = tokio::time::timeout(Duration::from_secs(1), command_rx.recv())
            .await
            .expect("resume command timeout")
            .expect("resume command");

        resume.abort();
        let _ = resume.await;
        assert!(
            handle.resume_in_flight.load(Ordering::Acquire),
            "a delivered command still owns the exact-once resume guard"
        );
        drop(queued);
        assert!(
            !handle.resume_in_flight.load(Ordering::Acquire),
            "dropping the unprocessed command must release the guard"
        );
    }

    #[tokio::test]
    async fn queued_resume_dispatch_is_bounded_before_http_response() {
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let handle = CursorLiveRunHandle {
            run_id: "bounded-resume-dispatch".into(),
            command_tx,
            pending: Arc::new(Mutex::new(vec![pending_exec(1, "tool-1")])),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        };

        let error = tokio::time::timeout(
            Duration::from_secs(3),
            handle.resume_batch_within(
                vec![(
                    "tool-1".into(),
                    serde_json::json!({"type":"tool_result","content":"done"}),
                )],
                Duration::from_millis(200),
            ),
        )
        .await
        .expect("resume dispatch must finish before downstream stream-idle")
        .expect_err("an unprocessed command must return a bounded retryable error");
        assert_eq!(
            error.status, 429,
            "a busy driver is transient; grok-build retries 429 and treats 409 as invalid_request"
        );
        assert_eq!(
            crate::retry::anthropic_error_kind_for_status(error.status, &error.message),
            "rate_limit_error"
        );
        let queued = command_rx.recv().await.expect("cancelled queued command");
        let RunCommand::ResumeBatch { dispatch_state, .. } = &queued else {
            panic!("expected queued resume");
        };
        assert_eq!(
            dispatch_state.load(Ordering::Acquire),
            RESUME_DISPATCH_CANCELLED,
            "the driver must not execute a command after its HTTP waiter timed out"
        );
        drop(queued);
        assert!(
            !handle.resume_in_flight.load(Ordering::Acquire),
            "discarding the cancelled command releases the resume generation"
        );
    }

    #[tokio::test]
    async fn cancellation_ack_fences_the_upstream_pump_and_driver_teardown() {
        struct DropSignal(Option<oneshot::Sender<()>>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(tx) = self.0.take() {
                    let _ = tx.send(());
                }
            }
        }

        let (upstream_tx, upstream_rx) = mpsc::channel(1);
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (command_tx, command_rx) = mpsc::channel(1);
        let (initial_sink, _initial_events) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(Vec::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let (pump_dropped_tx, mut pump_dropped_rx) = oneshot::channel();
        let upstream_pump = tokio::spawn(async move {
            let _signal = DropSignal(Some(pump_dropped_tx));
            std::future::pending::<()>().await;
        });
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx,
            upstream_pump,
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::clone(&pending),
            Arc::clone(&terminal_error),
            Arc::clone(&completed),
            Arc::clone(&cancel_requested),
            None,
            "cancel-fence-session".into(),
            "cancel-fence-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            test_reconnect_context(),
            test_generation_permit(),
        ));
        let handle = CursorLiveRunHandle {
            run_id: "cancel-fence-run".into(),
            command_tx,
            pending,
            terminal_error,
            completed: Arc::clone(&completed),
            cancel_requested,
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        };

        handle
            .cancel_and_wait()
            .await
            .expect("driver cancellation should be acknowledged");
        assert!(
            pump_dropped_rx.try_recv().is_ok(),
            "ack must follow upstream pump destruction"
        );
        assert!(
            completed.load(Ordering::Acquire),
            "completed must mean teardown and persistence have finished"
        );
        driver.await.expect("live driver");
    }

    #[tokio::test]
    async fn cancellation_preempts_a_backpressured_tool_result_send() {
        let (upstream_tx, upstream_rx) = mpsc::channel(2);
        let (request_tx, _request_rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
        request_tx
            .try_send(Ok(Bytes::from_static(b"fill-request-channel")))
            .expect("prefill outbound request channel");
        let (command_tx, command_rx) = mpsc::channel(8);
        let (initial_sink, mut events) = mpsc::channel(4);
        let pending = Arc::new(Mutex::new(Vec::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicBool::new(false));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let upstream_pump = tokio::spawn(std::future::pending::<()>());
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            upstream_pump,
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::clone(&pending),
            Arc::clone(&terminal_error),
            Arc::clone(&completed),
            Arc::clone(&cancel_requested),
            Some(BTreeSet::from(["Read".into()])),
            "blocked-send-session".into(),
            "blocked-send-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            test_reconnect_context(),
            test_generation_permit(),
        ));
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "blocked-send-run".into(),
            command_tx,
            pending,
            terminal_error,
            completed: Arc::clone(&completed),
            cancel_requested,
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });

        let mut payload = Vec::new();
        proto::AgentServerMessage {
            exec_server_message: Some(ExecServerMessage {
                id: 7,
                exec_id: Some("blocked-read".into()),
                read_args: Some(ExecReadArgs {
                    path: "/tmp/blocked".into(),
                    tool_call_id: "blocked-tool".into(),
                    offset: None,
                    limit: None,
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode(&mut payload)
        .expect("encode blocked exec");
        upstream_tx
            .send(Ok(Some(encode_connect_frame(payload, 0))))
            .await
            .expect("send blocked exec");
        let batch = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("tool exposure timeout")
            .expect("tool exposure channel")
            .expect("tool exposure event");
        let LiveRunEvent::NativeToolBatch(tools) = batch else {
            panic!("expected native tool batch");
        };
        let tool_use_id = tools[0].tool_use_id.clone();
        let resume_handle = Arc::clone(&handle);
        let resume = tokio::spawn(async move {
            resume_handle
                .resume_batch(vec![(
                    tool_use_id,
                    serde_json::json!({"type":"tool_result","content":"done"}),
                )])
                .await
        });
        let mut resumed_events = tokio::time::timeout(Duration::from_secs(1), resume)
            .await
            .expect("resume dispatch must establish the response before the blocked send")
            .expect("resume task")
            .expect("resume dispatch");

        tokio::time::timeout(Duration::from_secs(8), handle.cancel_and_wait())
            .await
            .expect("cancellation must not inherit an unbounded H2 send")
            .expect("cancellation should complete after bounded send teardown");
        assert!(completed.load(Ordering::Acquire));
        let error = tokio::time::timeout(Duration::from_secs(1), resumed_events.recv())
            .await
            .expect("cancel error event timeout")
            .expect("cancel error event")
            .expect_err("the preempted resume stream must report cancellation");
        assert!(error.contains("cancelled"), "{error}");
        driver.await.expect("blocked-send driver");
    }

    #[test]
    fn cancel_removes_running_entry_so_reserve_succeeds() {
        let _registry = lock_live_registry_for_test();
        let session = format!("cancel-session-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "run-cancel".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        if reservation.insert(Arc::clone(&handle)).is_err() {
            panic!("insert running handle");
        }
        assert!(LiveRunRegistry::get(&session).is_some());

        assert!(LiveRunRegistry::cancel(&session));
        assert!(LiveRunRegistry::get(&session).is_none());
        // Cancel command must be delivered so the driver can exit.
        assert!(matches!(
            command_rx.try_recv(),
            Ok(RunCommand::Cancel { ack: None })
        ));
        // Slot is free for a new turn (Claude Code retry after idle timeout).
        assert!(LiveRunRegistry::reserve(&session).is_some());
        LiveRunRegistry::clear();
    }

    #[test]
    fn supersede_replaces_occupant_with_fresh_reservation() {
        let _registry = lock_live_registry_for_test();
        let session = format!("supersede-session-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "old-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
            cancel_requested: Arc::new(AtomicBool::new(false)),
            resume_in_flight: Arc::new(AtomicBool::new(false)),
            request_fingerprint: AtomicU64::new(0),
        });
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        if reservation.insert(handle).is_err() {
            panic!("insert running handle");
        }

        let next = LiveRunRegistry::supersede(&session).expect("supersede");
        // Committed insert of a new handle would succeed; dropping frees Starting.
        drop(next);
        assert!(LiveRunRegistry::get(&session).is_none());
        LiveRunRegistry::clear();
    }

    #[tokio::test]
    async fn live_driver_terminates_on_disconnect_even_with_heartbeat_flood() {
        // Regression: Cursor InteractionUpdate.heartbeat used to keep the biased
        // upstream arm ready forever, starving the closed-sink poll so retries
        // hit 409 "already generating".
        let (upstream_tx, upstream_rx) = mpsc::channel::<Result<Option<Bytes>, String>>(8);
        let (request_tx, _request_rx) = mpsc::channel(4);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (initial_sink, initial_events) = mpsc::channel(1);
        drop(initial_events);
        let completed = Arc::new(AtomicBool::new(false));
        let terminal_error = Arc::new(Mutex::new(None));
        let upstream_pump = tokio::spawn(std::future::pending::<()>());

        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            upstream_pump,
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::new(Mutex::new(Vec::new())),
            terminal_error,
            Arc::clone(&completed),
            Arc::new(AtomicBool::new(false)),
            Some(BTreeSet::from(["Read".to_string()])),
            "heartbeat-drop-session".into(),
            "heartbeat-drop-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            test_reconnect_context(),
            test_generation_permit(),
        ));

        use super::super::proto::{AgentServerMessage, InteractionHeartbeat};
        for _ in 0..8 {
            let mut payload = Vec::new();
            AgentServerMessage {
                conversation_checkpoint_update: None,
                interaction_update: Some(InteractionUpdate {
                    heartbeat: Some(InteractionHeartbeat {}),
                    ..Default::default()
                }),
                kv_server_message: None,
                interaction_query: None,
                exec_server_message: None,
            }
            .encode(&mut payload)
            .unwrap();
            upstream_tx
                .send(Ok(Some(encode_connect_frame(payload, 0))))
                .await
                .unwrap();
        }

        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("driver must exit despite heartbeat flood after SSE drop")
            .unwrap();
        assert!(completed.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn live_driver_terminates_when_downstream_segment_is_dropped() {
        let (upstream_tx, upstream_rx) = mpsc::channel::<Result<Option<Bytes>, String>>(2);
        let (request_tx, _request_rx) = mpsc::channel(4);
        let (_command_tx, command_rx) = mpsc::channel(1);
        let (initial_sink, initial_events) = mpsc::channel(1);
        drop(initial_events);
        let completed = Arc::new(AtomicBool::new(false));
        let terminal_error = Arc::new(Mutex::new(None));
        let upstream_pump = tokio::spawn(std::future::pending::<()>());
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            upstream_pump,
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::new(Mutex::new(Vec::new())),
            terminal_error,
            Arc::clone(&completed),
            Arc::new(AtomicBool::new(false)),
            None,
            "drop-test-session".into(),
            "drop-test-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            test_reconnect_context(),
            test_generation_permit(),
        ));

        let mut payload = Vec::new();
        proto::AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: Some(TextDelta {
                    text: "this send observes the dropped receiver".into(),
                }),
                ..Default::default()
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        }
        .encode(&mut payload)
        .unwrap();
        upstream_tx
            .send(Ok(Some(encode_connect_frame(payload, 0))))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), driver)
            .await
            .expect("driver remained registered after downstream disconnect")
            .unwrap();
        assert!(completed.load(Ordering::Acquire));
    }
}
