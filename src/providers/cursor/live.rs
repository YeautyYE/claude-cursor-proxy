//! Long-lived Cursor Agent BiDi runs.
//!
//! A Claude Code tool turn spans two Anthropic HTTP requests, while Cursor keeps
//! the model + exec loop on one `AgentService/Run` stream. This module owns that
//! upstream stream between requests and sends native exec results back through
//! the original request-body channel.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::StatusCode;
use prost::Message;
use tokio::sync::{mpsc, oneshot};

use super::client::{
    CursorError, CursorHttpClient, build_resume_run_request, build_run_request_with_continuation,
};
use super::connect::{
    ConnectFrame, ConnectFrameDecoder, FLAG_END, anthropic_error_type_from_live_error,
    encode_connect_frame, parse_connect_error,
};
use super::exec_results::{
    CursorExecKind, PendingCursorExec, encode_control_close, encode_control_throw,
    encode_exec_heartbeat, encode_tool_result_frames,
};
use super::http1::{self, BidiAppendSession};
use super::native_tools::{
    accumulate_partial_args_text, map_tool_call_started, merge_partial_args_json,
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

impl ClientOutbound {
    async fn send_connect_frame(&self, frame: Bytes) -> bool {
        match self {
            Self::Bidi(tx) => tx.send(Ok(frame)).await.is_ok(),
            Self::Http1(session) => session.append_connect_or_raw(&frame).await.is_ok(),
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
    tool_use_id: String,
    name: String,
    input: serde_json::Value,
}

pub type LiveEventResult = Result<LiveRunEvent, String>;

#[derive(Debug, Clone)]
struct TerminalOutcome {
    message: String,
    created_at: Instant,
}

pub struct LiveRunStart {
    pub handle: Arc<CursorLiveRunHandle>,
    pub events: mpsc::Receiver<LiveEventResult>,
}

enum RunCommand {
    ResumeBatch {
        tool_results: Vec<(String, serde_json::Value)>,
        sink: mpsc::Sender<LiveEventResult>,
        ack: oneshot::Sender<Result<(), String>>,
    },
    Cancel,
}

#[derive(Debug)]
pub struct CursorLiveRunHandle {
    run_id: String,
    command_tx: mpsc::Sender<RunCommand>,
    pending: Arc<Mutex<Vec<PendingCursorExec>>>,
    terminal_error: Arc<Mutex<Option<TerminalOutcome>>>,
    completed: Arc<AtomicBool>,
}

impl CursorLiveRunHandle {
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

    fn take_terminal_error(&self) -> Option<String> {
        self.terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .filter(|outcome| {
                outcome.created_at.elapsed()
                    < Duration::from_secs(env_u64("CCP_CURSOR_TERMINAL_TTL_SECS", 60))
            })
            .map(|outcome| outcome.message)
    }

    fn has_terminal_error(&self) -> bool {
        self.terminal_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|outcome| {
                outcome.created_at.elapsed()
                    < Duration::from_secs(env_u64("CCP_CURSOR_TERMINAL_TTL_SECS", 60))
            })
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
        let pending = self.pending_tools();
        validate_tool_result_batch(&pending, &tool_results)
            .map_err(|message| CursorError::new(400, message, None))?;
        // Match start_live_agent capacity — post-tool thinking bursts must not
        // trip the old 64-slot ceiling (silent drop under try_send timeout).
        let (sink, events) = mpsc::channel(LIVE_EVENT_CHANNEL_CAP);
        let (ack, ready) = oneshot::channel();
        self.command_tx
            .send(RunCommand::ResumeBatch {
                tool_results,
                sink,
                ack,
            })
            .await
            .map_err(|_| CursorError::internal("Cursor live run already closed"))?;
        ready
            .await
            .map_err(|_| CursorError::internal("Cursor live resume acknowledgement dropped"))?
            .map_err(CursorError::internal)?;
        Ok(events)
    }

    pub fn cancel(&self) {
        let _ = self.command_tx.try_send(RunCommand::Cancel);
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
    awaiting_since: Option<Instant>,
    collecting_since: Option<Instant>,
    collect_deadline: Option<tokio::time::Instant>,
}

impl PendingExecState {
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

    fn collect_deadline(&self) -> Option<tokio::time::Instant> {
        self.can_expose().then_some(self.collect_deadline).flatten()
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
    Starting { reservation_id: String },
    Running(Arc<CursorLiveRunHandle>),
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
}

impl LiveRunReservation {
    /// Atomically replace this reservation with the live handle. The returned
    /// handle on failure lets the caller explicitly cancel the orphaned run.
    pub fn insert(
        mut self,
        handle: Arc<CursorLiveRunHandle>,
    ) -> Result<(), Arc<CursorLiveRunHandle>> {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        let owns_reservation = matches!(
            runs.runs.get(&self.session_id),
            Some(LiveRunEntry::Starting { reservation_id })
                if reservation_id == &self.reservation_id
        );
        if !owns_reservation {
            return Err(handle);
        }
        runs.insert_key(self.session_id.clone(), LiveRunEntry::Running(handle));
        self.committed = true;
        Ok(())
    }
}

impl Drop for LiveRunReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        let owns_reservation = matches!(
            runs.runs.get(&self.session_id),
            Some(LiveRunEntry::Starting { reservation_id })
                if reservation_id == &self.reservation_id
        );
        if owns_reservation {
            runs.remove_key(&self.session_id);
        }
    }
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

    fn reserve_key(runs: &mut LiveRunMap, key: String) -> Option<LiveRunReservation> {
        if runs.runs.contains_key(&key) {
            return None;
        }
        let reservation_id = uuid::Uuid::new_v4().to_string();
        runs.insert_key(
            key.clone(),
            LiveRunEntry::Starting {
                reservation_id: reservation_id.clone(),
            },
        );
        Some(LiveRunReservation {
            session_id: key,
            reservation_id,
            committed: false,
        })
    }

    fn key_occupied(runs: &LiveRunMap, key: &str) -> bool {
        match runs.runs.get(key) {
            Some(LiveRunEntry::Starting { .. }) => true,
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
            runs.remove_key(&key)
        };
        match entry {
            Some(LiveRunEntry::Running(handle)) => {
                handle.cancel();
                true
            }
            Some(LiveRunEntry::Starting { .. }) => true,
            None => false,
        }
    }

    /// Cancel any occupant of this slot, then reserve. Nested callers must use
    /// [`Self::supersede_run`] with `agent_id` so the parent is not stolen.
    pub fn supersede(session_id: &str) -> Option<LiveRunReservation> {
        Self::supersede_run(session_id, None)
    }

    pub fn supersede_run(session_id: &str, agent_id: Option<&str>) -> Option<LiveRunReservation> {
        Self::cancel_run(session_id, agent_id);
        Self::reserve_run(session_id, agent_id)
    }

    pub fn get(session_id: &str) -> Option<Arc<CursorLiveRunHandle>> {
        Self::get_run(session_id, None)
    }

    pub fn get_run(session_id: &str, agent_id: Option<&str>) -> Option<Arc<CursorLiveRunHandle>> {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.runs.get(&key) {
            Some(LiveRunEntry::Running(handle)) if !handle.is_completed() => {
                Some(Arc::clone(handle))
            }
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

    pub fn take_terminal_error(session_id: &str) -> Option<String> {
        Self::take_terminal_error_run(session_id, None)
    }

    pub fn take_terminal_error_run(session_id: &str, agent_id: Option<&str>) -> Option<String> {
        let key = live_run_key(session_id, agent_id);
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        let error = match runs.runs.get(&key) {
            Some(LiveRunEntry::Running(handle)) => handle.take_terminal_error(),
            Some(LiveRunEntry::Starting { .. }) | None => None,
        };
        if error.is_some() {
            runs.remove_key(&key);
        }
        error
    }

    fn prune_finished(runs: &mut LiveRunMap) {
        let stale: Vec<String> = runs
            .runs
            .iter()
            .filter_map(|(key, entry)| match entry {
                LiveRunEntry::Starting { .. } => None,
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

    fn remove_if(session_id: &str, run_id: &str) {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        let keys = runs.keys_for(claude_session_of(session_id));
        let mut extra = Vec::new();
        if !keys.iter().any(|k| k == session_id) {
            extra.push(session_id.to_string());
        }
        for key in keys.into_iter().chain(extra) {
            if matches!(
                runs.runs.get(&key),
                Some(LiveRunEntry::Running(handle)) if handle.run_id == run_id
            ) {
                runs.remove_key(&key);
                return;
            }
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
    ) -> Result<LiveRunStart, CursorError> {
        if !self.live_bidi_enabled() {
            return Err(CursorError::internal(
                "Cursor live agent is disabled for this transport",
            ));
        }

        let force_http1 = http1::prefer_http1_agent();
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
        let (outbound, response) = self
            .open_live_transport(
                token,
                &request_id,
                &first_message,
                &cursor_identity,
                force_http1,
                /*allow_h1_fallback=*/ !force_http1,
            )
            .await?;

        // Larger fan-out so token deltas don't block the BiDi read loop under
        // Claude Code backpressure (coalescing in live_sse_response).
        let (event_tx, events) = mpsc::channel(LIVE_EVENT_CHANNEL_CAP);
        let (command_tx, command_rx) = mpsc::channel(8);
        let pending = Arc::new(Mutex::new(Vec::new()));
        let terminal_error = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicBool::new(false));
        let run_id = uuid::Uuid::new_v4().to_string();
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: run_id.clone(),
            command_tx,
            pending: Arc::clone(&pending),
            terminal_error: Arc::clone(&terminal_error),
            completed: Arc::clone(&completed),
        });

        let seeded_blobs: HashMap<Vec<u8>, Vec<u8>> =
            continuation.pre_fetched_blobs.into_iter().collect();
        let reconnect = LiveReconnectContext {
            http: self.clone(),
            token: token.to_string(),
            identity: cursor_identity,
            model_id: resolved.model_id.clone(),
            conversation_id: continuation.conversation_id.clone(),
            force_http1,
            mcp_tools: mcp_tools.clone(),
            opening_checkpoint: opening_live_checkpoint(&continuation.conversation_state),
        };
        // Match event fan-out: a tiny upstream queue stalls the reqwest body
        // pump (and Cursor's TCP window) during thinking bursts.
        let (upstream_tx, upstream_rx) =
            mpsc::channel::<Result<Option<Bytes>, String>>(LIVE_EVENT_CHANNEL_CAP);
        spawn_upstream_pump(response.bytes_stream(), upstream_tx.clone());
        tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx,
            outbound,
            command_rx,
            event_tx,
            pending,
            terminal_error,
            completed,
            allowed_tool_names,
            worker_session,
            run_id,
            seeded_blobs,
            prompt.to_string(),
            request_context,
            reconnect,
        ));

        Ok(LiveRunStart { handle, events })
    }

    /// Open BiDi `Run` or HTTP/1 `RunSSE`+`BidiAppend`. When BiDi fails with a
    /// transport-ish status (CLI: FORCE_BIDI_DISABLED / proxy 464), retry once via H1.
    async fn open_live_transport(
        &self,
        token: &str,
        request_id: &str,
        first_message: &AgentClientMessage,
        identity: &LiveIdentityHeaders,
        force_http1: bool,
        allow_h1_fallback: bool,
    ) -> Result<(ClientOutbound, reqwest::Response), CursorError> {
        if force_http1 {
            return self
                .open_http1_run_sse(token, request_id, first_message, identity)
                .await;
        }

        match self
            .open_h2_bidi_run(token, request_id, first_message, identity)
            .await
        {
            Ok(pair) => Ok(pair),
            Err(err) if allow_h1_fallback && is_http1_fallback_error(&err) => {
                if std::env::var("CCP_CURSOR_DEBUG").is_ok() {
                    eprintln!(
                        "[ccp-cursor] BiDi Run failed ({}); falling back to RunSSE+BidiAppend",
                        err.status
                    );
                }
                self.open_http1_run_sse(token, request_id, first_message, identity)
                    .await
            }
            Err(err) => Err(err),
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
        append.append_message(first_message).await?;
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

        let response = request
            .body(reqwest::Body::wrap_stream(request_body))
            .send()
            .await
            .map_err(|e| CursorError::from_reqwest(e, self.timeout_secs))?;
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
struct LiveReconnectContext {
    http: CursorHttpClient,
    token: String,
    identity: LiveIdentityHeaders,
    model_id: String,
    conversation_id: Option<String>,
    force_http1: bool,
    mcp_tools: Option<super::proto::McpTools>,
    opening_checkpoint: Option<Vec<u8>>,
}

type LiveUpstream = mpsc::Receiver<Result<Option<Bytes>, String>>;

/// Pump a reqwest body stream into an mpsc so the driver can `select!` and
/// swap transports on ResumeAction reconnect without Pin gymnastics.
///
/// Sends `Ok(None)` exactly once when the HTTP body ends so the driver sees EOF
/// even while it still holds a clone of the sender for reconnect pumps.
fn spawn_upstream_pump<S>(stream: S, tx: mpsc::Sender<Result<Option<Bytes>, String>>)
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Send + Unpin + 'static,
{
    spawn_upstream_pump_prefixed(None, stream, tx);
}

fn spawn_upstream_pump_prefixed<S>(
    prefix: Option<Bytes>,
    stream: S,
    tx: mpsc::Sender<Result<Option<Bytes>, String>>,
) where
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
    });
}

fn is_http1_fallback_error(err: &CursorError) -> bool {
    matches!(
        err.status,
        // Proxy/CDN HTTP version rejects (Surge/Clash 464), gateway blips, or
        // Connect "unimplemented"/BiDi-disabled style failures.
        408 | 421 | 464 | 502 | 503 | 504
    ) || err.message.contains("error sending request")
        || err.message.contains("connection")
        || is_h2_stream_reset(&err.message)
        || err.detail.as_deref().is_some_and(|d| {
            d.contains("HTTP_1_1_REQUIRED") || d.contains("bidi") || is_h2_stream_reset(d)
        })
}

/// Semantic Cursor errors (400/401/403/429) must not be retried on another
/// transport — that burns quota and can duplicate an already-accepted run.
pub(crate) fn is_retryable_live_transport_error(err: &CursorError) -> bool {
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

fn live_reconnect_skip_reason(
    latest_checkpoint: &Option<Vec<u8>>,
    opening_checkpoint: &Option<Vec<u8>>,
    reconnect_attempts: u32,
    max_reconnects: u32,
) -> Option<&'static str> {
    let has_checkpoint = latest_checkpoint
        .as_ref()
        .is_some_and(|checkpoint| !checkpoint.is_empty())
        || opening_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| !checkpoint.is_empty());
    if !has_checkpoint {
        return Some("no checkpoint");
    }
    if reconnect_attempts >= max_reconnects {
        return Some("reconnect budget exhausted");
    }
    None
}

/// First ResumeAction is immediate; later attempts match CLI 1s×2^n + 20% jitter, cap 60s.
/// Hollow (HTTP 200 then zero-byte RST) retries stay at 0ms — backoff was turning
/// ten instant INTERNAL_ERRORs into a 5-minute 502.
fn live_reconnect_backoff_ms_for(attempt: u32, hollow: bool) -> u64 {
    if hollow || attempt <= 1 {
        return 0;
    }
    let shift = (attempt - 2).min(6);
    let base_ms = 1_000u64 << shift;
    let jitter = ((base_ms as f64) * 0.2 * ((attempt as f64 * 0.37) % 1.0)) as u64;
    (base_ms + jitter).min(60_000)
}

fn is_h2_stream_reset(message: &str) -> bool {
    message.contains("unexpected internal error")
        || message.contains("stream error received")
        || message.contains("HTTP2")
        || message.contains("http2")
}

/// H2 `INTERNAL_ERROR` mid-stream: ResumeAction on H2 is always hollow in
/// production. Switch that run to a real `http1_only` RunSSE client immediately.
fn live_reconnect_should_force_http1(
    got_chunk_since_reconnect: bool,
    reconnect_attempts: u32,
    already_http1: bool,
    stream_error: Option<&str>,
) -> bool {
    if already_http1 {
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
    if live_reconnect_should_force_http1(
        got_chunk_since_reconnect,
        reconnect_attempts,
        reconnect.force_http1,
        stream_error,
    ) {
        reconnect.force_http1 = true;
        let mut fields = serde_json::Map::new();
        fields.insert("attempts".into(), serde_json::json!(reconnect_attempts));
        fields.insert("reason".into(), serde_json::json!("h2_stream_reset"));
        crate::logging::create_logger("cursor").warn("live_reconnect_http1", Some(fields));
    }
}

const LIVE_RECONNECT_FIRST_BYTE: Duration = Duration::from_secs(3);

async fn take_first_live_chunk<S>(mut stream: S) -> Result<(Bytes, S), String>
where
    S: Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    match tokio::time::timeout(LIVE_RECONNECT_FIRST_BYTE, stream.next()).await {
        Ok(Some(Ok(chunk))) => Ok((chunk, stream)),
        Ok(Some(Err(error))) => Err(format_error_chain(&error)),
        Ok(None) => Err("Cursor resume stream ended before the first byte".into()),
        Err(_) => Err("Cursor resume stream produced no bytes within 3s".into()),
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

fn log_live_reconnect(
    outcome: &LiveReconnectOutcome,
    attempts: u32,
    max_reconnects: u32,
    http: &CursorHttpClient,
) {
    let mut fields = serde_json::Map::new();
    fields.insert("attempts".into(), serde_json::json!(attempts));
    fields.insert("max".into(), serde_json::json!(max_reconnects));
    fields.insert("http1".into(), serde_json::json!(http.prefers_http1()));
    match outcome {
        LiveReconnectOutcome::Reconnected => {
            fields.insert("outcome".into(), serde_json::json!("ok"));
            crate::logging::create_logger("cursor").info("live_reconnect", Some(fields));
        }
        LiveReconnectOutcome::Skipped(reason) => {
            fields.insert("outcome".into(), serde_json::json!("skipped"));
            fields.insert("reason".into(), serde_json::json!(reason));
            crate::logging::create_logger("cursor").warn("live_reconnect", Some(fields));
        }
        LiveReconnectOutcome::Failed(detail) => {
            fields.insert("outcome".into(), serde_json::json!("failed"));
            fields.insert("detail".into(), serde_json::json!(detail));
            crate::logging::create_logger("cursor").warn("live_reconnect", Some(fields));
        }
    }
}

/// Re-open AgentService/Run with `ResumeAction` after a transport stall.
/// Retries retryable open failures up to `max_reconnects` (first attempt has no delay).
#[allow(clippy::too_many_arguments)]
async fn try_live_reconnect(
    reconnect: &mut LiveReconnectContext,
    outbound: &mut ClientOutbound,
    upstream_tx: &mpsc::Sender<Result<Option<Bytes>, String>>,
    decoder: &mut ConnectFrameDecoder,
    latest_checkpoint: &Option<Vec<u8>>,
    kv_blobs: &HashMap<Vec<u8>, Vec<u8>>,
    pending: &mut PendingExecState,
    reconnect_attempts: &mut u32,
    max_reconnects: u32,
    last_progress: &mut Instant,
    resume_grace_until: &mut Option<Instant>,
    resume_grace: Duration,
) -> LiveReconnectOutcome {
    let checkpoint = latest_checkpoint
        .as_ref()
        .filter(|c| !c.is_empty())
        .or(reconnect
            .opening_checkpoint
            .as_ref()
            .filter(|c| !c.is_empty()))
        .cloned();
    let Some(checkpoint) = checkpoint else {
        let outcome = LiveReconnectOutcome::Skipped("no checkpoint");
        log_live_reconnect(&outcome, *reconnect_attempts, max_reconnects, &reconnect.http);
        return outcome;
    };

    let mut closed_collecting = false;
    let mut last_fail: Option<String> = None;
    let mut last_was_hollow = false;
    loop {
        if let Some(reason) = live_reconnect_skip_reason(
            latest_checkpoint,
            &reconnect.opening_checkpoint,
            *reconnect_attempts,
            max_reconnects,
        ) {
            let outcome = last_fail
                .map(LiveReconnectOutcome::Failed)
                .unwrap_or(LiveReconnectOutcome::Skipped(reason));
            log_live_reconnect(&outcome, *reconnect_attempts, max_reconnects, &reconnect.http);
            return outcome;
        }
        if !closed_collecting {
            let _ = control_close_collecting_natives(pending, outbound).await;
            closed_collecting = true;
        }
        *reconnect_attempts += 1;
        let delay_ms = live_reconnect_backoff_ms_for(*reconnect_attempts, last_was_hollow);
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        last_was_hollow = false;
        // Pin HTTP/1.1 on the reqwest client itself. Posting RunSSE with an H2
        // client still negotiates HTTP/2 and dies with INTERNAL_ERROR.
        reconnect.http = CursorHttpClient::with_prefer_http1(
            reconnect.force_http1 || super::http1::prefer_http1_agent(),
        );

        let cont = super::conversation::RunContinuation {
            conversation_id: reconnect.conversation_id.clone(),
            conversation_state: checkpoint.clone(),
            pre_fetched_blobs: kv_blobs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            has_checkpoint: true,
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

        match reconnect
            .http
            .open_live_transport(
                &reconnect.token,
                &request_id,
                &first_message,
                &reconnect.identity,
                reconnect.force_http1,
                /*allow_h1_fallback=*/ !reconnect.force_http1,
            )
            .await
        {
            Ok((new_outbound, response)) => {
                let stream = response.bytes_stream();
                match take_first_live_chunk(stream).await {
                    Ok((chunk, rest)) => {
                        *outbound = new_outbound;
                        spawn_upstream_pump_prefixed(Some(chunk), rest, upstream_tx.clone());
                        *decoder = ConnectFrameDecoder::new();
                        *last_progress = Instant::now();
                        *resume_grace_until = Some(Instant::now() + resume_grace);
                        let outcome = LiveReconnectOutcome::Reconnected;
                        log_live_reconnect(&outcome, *reconnect_attempts, max_reconnects, &reconnect.http);
                        return outcome;
                    }
                    Err(msg) => {
                        last_fail = Some(msg.clone());
                        last_was_hollow = true;
                        if !reconnect.force_http1 {
                            reconnect.force_http1 = true;
                            let mut fields = serde_json::Map::new();
                            fields
                                .insert("attempts".into(), serde_json::json!(*reconnect_attempts));
                            fields.insert("detail".into(), serde_json::json!(msg));
                            crate::logging::create_logger("cursor")
                                .warn("live_reconnect_http1", Some(fields));
                        }
                    }
                }
            }
            Err(err) => {
                last_fail = Some(format!("{} ({})", err.message, err.status));
                if !is_retryable_live_transport_error(&err) {
                    let outcome = LiveReconnectOutcome::Failed(
                        last_fail.clone().unwrap_or_else(|| err.to_string()),
                    );
                    log_live_reconnect(&outcome, *reconnect_attempts, max_reconnects, &reconnect.http);
                    return outcome;
                }
                if reconnect.force_http1 && matches!(err.status, 421 | 464) {
                    reconnect.force_http1 = false;
                } else if !reconnect.force_http1 && is_http1_fallback_error(&err) {
                    reconnect.force_http1 = true;
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

async fn control_close_natives(pending: &mut PendingExecState, outbound: &ClientOutbound) -> bool {
    for exec in pending.drain_natives() {
        if let Ok(frame) = encode_control_close(exec.id) {
            if !outbound.send_connect_frame(frame).await {
                return false;
            }
        }
    }
    true
}

async fn control_close_collecting_natives(
    pending: &mut PendingExecState,
    outbound: &ClientOutbound,
) -> bool {
    for exec in pending.drain_collecting_natives() {
        if let Ok(frame) = encode_control_close(exec.id) {
            if !outbound.send_connect_frame(frame).await {
                return false;
            }
        }
    }
    true
}

struct LiveTurnCtx<'a> {
    user_prompt: &'a str,
    request_context: &'a RequestContext,
    decode_failures: &'a mut u32,
    coalescer: &'a mut LiveDeltaCoalescer,
}

#[allow(clippy::too_many_arguments)]
async fn drive_live_run(
    mut upstream: LiveUpstream,
    upstream_tx: mpsc::Sender<Result<Option<Bytes>, String>>,
    mut outbound: ClientOutbound,
    mut command_rx: mpsc::Receiver<RunCommand>,
    initial_sink: mpsc::Sender<LiveEventResult>,
    pending_shared: Arc<Mutex<Vec<PendingCursorExec>>>,
    terminal_error: Arc<Mutex<Option<TerminalOutcome>>>,
    completed: Arc<AtomicBool>,
    allowed_tool_names: Option<BTreeSet<String>>,
    session_id: String,
    run_id: String,
    seeded_blobs: HashMap<Vec<u8>, Vec<u8>>,
    user_prompt: String,
    request_context: RequestContext,
    mut reconnect: LiveReconnectContext,
) {
    let mut sink = Some(initial_sink);
    let mut pending = PendingExecState::default();
    let mut deferred = VecDeque::<LiveEventResult>::new();
    let mut decoder = ConnectFrameDecoder::new();
    let mut kv_blobs = seeded_blobs;
    let mut latest_checkpoint = reconnect.opening_checkpoint.clone();
    let mut saw_text = false;
    let mut useful = false;
    let mut logical_tools_waiting = LogicalToolTracker::default();
    let mut last_progress = Instant::now();
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
    // CLI stall-detector failTimeoutMs default 30s; we stay looser because
    // server InteractionUpdate.heartbeat refreshes last_progress (CLI treats
    // heartbeat-only as 3× fail = 90s).
    let stream_idle = Duration::from_secs(env_u64("CCP_CURSOR_IDLE_SECS", 120));
    // Live path always waits for Cursor `turn_ended` (or hard timeout). The old
    // 8s complete_idle for tool-less runs truncated Fable quiet thinking.
    let wait_for_turn_ended = true;
    let complete_idle = Duration::from_millis(env_u64(
        "CCP_CURSOR_COMPLETE_IDLE_MS",
        u64::MAX / 4, // disabled unless explicitly overridden
    ));
    let hard = Duration::from_secs(env_u64("CCP_CURSOR_TIMEOUT_SECS", 1800));
    let tool_ttl = Duration::from_secs(env_u64("CCP_CURSOR_TOOL_TTL_SECS", 600));
    // CLI transport/stall retries: 10 (prod). Keep Anthropic SSE open across
    // brief Cursor disconnects when we have a checkpoint to ResumeAction.
    let max_reconnects = env_u64("CCP_CURSOR_RECONNECT_MAX", 10) as u32;
    let mut reconnect_attempts: u32 = 0;
    let mut got_chunk_since_reconnect = false;

    'driver: loop {
        // Check before select: Cursor InteractionUpdate.heartbeat / client
        // heartbeats keep the biased upstream/heartbeat arms ready and would
        // otherwise starve the 250ms closed-sink poll for minutes — leaving a
        // zombie "already generating" run after Claude Code disconnects.
        if sink.as_ref().is_some_and(mpsc::Sender::is_closed) {
            // Keep BiDi only when Claude still owes us native tool_results.
            // logical_tools_waiting alone must not pin the session: those are
            // UI hints, not Anthropic-exposed pending tools.
            if pending.is_empty() {
                break 'driver;
            }
            sink = None;
        }
        let batch_deadline = pending.collect_deadline();
        let coalesce_deadline = coalescer.deadline();
        tokio::select! {
            biased;

            command = command_rx.recv() => {
                match command {
                    Some(RunCommand::Cancel) => {
                        // Registry may already have removed us via supersede;
                        // still mark completed so prune/get stay consistent.
                        report_terminal_error(
                            &mut sink,
                            &terminal_error,
                            "Cursor live run cancelled".into(),
                        )
                        .await;
                        break 'driver;
                    }
                    None => {
                        report_terminal_error(
                            &mut sink,
                            &terminal_error,
                            "Cursor live run control channel closed".into(),
                        )
                        .await;
                        break 'driver;
                    }
                    Some(RunCommand::ResumeBatch { tool_results, sink: next_sink, ack }) => {
                        let frames = match encode_tool_result_batch(pending.awaiting(), &tool_results) {
                            Ok(frames) => frames,
                            Err(error) => {
                                let _ = ack.send(Err(error));
                                continue;
                            }
                        };

                        let mut send_failed = false;
                        for frame in &frames {
                            if !outbound.send_connect_frame(frame.clone()).await {
                                send_failed = true;
                                break;
                            }
                        }
                        if send_failed {
                            prepare_live_reconnect(
                                &mut reconnect,
                                got_chunk_since_reconnect,
                                reconnect_attempts,
                                None,
                            );
                            let reconnect_outcome = try_live_reconnect(
                                &mut reconnect,
                                &mut outbound,
                                &upstream_tx,
                                &mut decoder,
                                &latest_checkpoint,
                                &kv_blobs,
                                &mut pending,
                                &mut reconnect_attempts,
                                max_reconnects,
                                &mut last_progress,
                                &mut resume_grace_until,
                                resume_grace,
                            )
                            .await;
                            send_failed = !matches!(
                                reconnect_outcome,
                                LiveReconnectOutcome::Reconnected
                            );
                            if !send_failed {
                                got_chunk_since_reconnect = false;
                                for frame in &frames {
                                    if !outbound.send_connect_frame(frame.clone()).await {
                                        send_failed = true;
                                        break;
                                    }
                                }
                            }
                            if send_failed {
                                let _ = ack.send(Err(format!(
                                    "Cursor request stream closed during tool resume{}",
                                    reconnect_note(&reconnect_outcome)
                                )));
                                break 'driver;
                            }
                        }
                        pending.complete_awaiting();
                        pending_shared
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .clear();
                        sink = Some(next_sink);
                        saw_text = false;
                        useful = false;
                        logical_tools_waiting.clear();
                        last_progress = Instant::now();
                        // After tool results, Cursor often thinks quietly before the
                        // next text/tool delta. Don't trip setup_idle during that gap
                        // (was the "no useful progress" hang after a healthy tool_use).
                        resume_grace_until = Some(Instant::now() + resume_grace);
                        // Wake the HTTP handler before replaying buffered events.
                        // The caller only starts polling `next_sink` after this ack;
                        // filling its bounded channel first would deadlock at 65+
                        // deferred events.
                        if ack.send(Ok(())).is_err() {
                            break 'driver;
                        }
                        while let Some(event) = deferred.pop_front() {
                            record_segment_progress(
                                &event,
                                &mut saw_text,
                                &mut useful,
                                &mut last_progress,
                            );
                            if !send_live_event(&mut sink, event).await {
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
                        if !chunk.is_empty() {
                            got_chunk_since_reconnect = true;
                            reconnect_attempts = 0;
                        }
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
                        for frame in frames {
                            let mut turn = LiveTurnCtx {
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
                                break 'driver;
                            }
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
                        // Quiet window already elapsed (incl. TOOL_BATCH_MS=0):
                        // expose in this iteration so we do not wait for the
                        // next select pass behind heartbeats / idle sleep.
                        if pending
                            .collect_deadline()
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
                        // Abrupt EOF without Connect END / turn_ended — try
                        // ResumeAction reconnect (CLI stall recovery).
                        prepare_live_reconnect(
                            &mut reconnect,
                            got_chunk_since_reconnect,
                            reconnect_attempts,
                            None,
                        );
                        let reconnect_outcome = try_live_reconnect(
                            &mut reconnect,
                            &mut outbound,
                            &upstream_tx,
                            &mut decoder,
                            &latest_checkpoint,
                            &kv_blobs,
                            &mut pending,
                            &mut reconnect_attempts,
                            max_reconnects,
                            &mut last_progress,
                            &mut resume_grace_until,
                            resume_grace,
                        )
                        .await;
                        if matches!(reconnect_outcome, LiveReconnectOutcome::Reconnected) {
                            got_chunk_since_reconnect = false;
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
                                let _ = control_close_natives(&mut pending, &outbound).await;
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
                            report_terminal_error(
                                &mut sink,
                                &terminal_error,
                                format!(
                                    "Cursor upstream ended without turn_ended{}",
                                    reconnect_note(&reconnect_outcome)
                                ),
                            )
                            .await;
                            break 'driver;
                        }
                        break 'driver;
                    }
                    Some(Err(error)) => {
                        prepare_live_reconnect(
                            &mut reconnect,
                            got_chunk_since_reconnect,
                            reconnect_attempts,
                            Some(error.as_str()),
                        );
                        let reconnect_outcome = try_live_reconnect(
                            &mut reconnect,
                            &mut outbound,
                            &upstream_tx,
                            &mut decoder,
                            &latest_checkpoint,
                            &kv_blobs,
                            &mut pending,
                            &mut reconnect_attempts,
                            max_reconnects,
                            &mut last_progress,
                            &mut resume_grace_until,
                            resume_grace,
                        )
                        .await;
                        if matches!(reconnect_outcome, LiveReconnectOutcome::Reconnected) {
                            got_chunk_since_reconnect = false;
                            continue 'driver;
                        }
                        let message = format!(
                            "Cursor response stream: {error}{}",
                            reconnect_note(&reconnect_outcome)
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
                if !expose_collected_tools(&mut pending, &pending_shared, &mut sink).await {
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
                // Agent/tool runs must wait for Cursor `turn_ended` (or a native
                // exec). Fable often emits a short plan text then thinks quietly
                // for many minutes — inventing End after a few minutes truncates
                // real work and also races Claude Code's ≥5m stream idle watchdog.
                if run_started.elapsed() >= hard {
                    let message = if pending.is_empty() {
                        "Cursor live run hard timeout".into()
                    } else {
                        "Cursor live run hard timeout with pending native tools".into()
                    };
                    report_terminal_error(&mut sink, &terminal_error, message).await;
                    break 'driver;
                }
                if let Some(since) = pending.oldest_since() {
                    if since.elapsed() >= tool_ttl {
                        report_terminal_error(
                            &mut sink,
                            &terminal_error,
                            "Cursor tool result wait expired".into(),
                        )
                        .await;
                        break 'driver;
                    }
                } else if resume_grace_until.is_some_and(|until| Instant::now() < until) {
                    // Post-tool-result grace: keep waiting for the next model delta.
                } else if !logical_tools_waiting.is_empty() {
                    // A UI tool_call_started is not executable by itself. Wait for the
                    // authoritative ExecServerMessage instead of falsely ending the turn.
                    // Use oldest_since (ignores heartbeats) — last_progress used to stall
                    // forever under InteractionUpdate.heartbeat floods (~1–2m empty turns).
                    if logical_tools_waiting
                        .oldest_since()
                        .is_some_and(|since| since.elapsed() >= stream_idle)
                    {
                        logical_tools_waiting.clear();
                    }
                } else if !wait_for_turn_ended
                    && saw_text
                    && last_progress.elapsed() >= complete_idle
                {
                    emit_cursor_or_defer(&mut sink, &mut deferred, CursorStreamEvent::End).await;
                    break 'driver;
                } else if useful && !saw_text && last_progress.elapsed() >= stream_idle {
                    // Thinking-only agent turns can stay quiet for a long time; only
                    // treat as stalled when no tools were advertised for this run.
                    if allowed_tool_names.is_none() {
                        report_terminal_error(
                            &mut sink,
                            &terminal_error,
                            "Cursor stream stalled after partial progress".into(),
                        )
                        .await;
                        break 'driver;
                    }
                } else if !useful && last_progress.elapsed() >= setup_idle {
                    report_terminal_error(
                        &mut sink,
                        &terminal_error,
                        "Cursor stream produced no useful progress".into(),
                    )
                    .await;
                    break 'driver;
                }
            }
        }
    }

    completed.store(true, Ordering::Release);
    // Persist checkpoint + KV blobs so the next Claude turn can resume Cursor state.
    // ClientOnly (Workflow/Skill) teardown must not keep an in-flight MCP
    // checkpoint — the next POST is a fresh turn that includes tool_results.
    let client_only_teardown = pending_shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .any(|exec| matches!(exec.kind, CursorExecKind::ClientOnly));
    if client_only_teardown && pending.has_outstanding_native() {
        let _ = control_close_natives(&mut pending, &outbound).await;
    }
    if client_only_teardown {
        super::conversation::clear_checkpoint(&session_id);
    } else if let Some(checkpoint) = latest_checkpoint.take() {
        super::conversation::save_checkpoint(&session_id, checkpoint);
    }
    super::conversation::merge_blobs(&session_id, &kv_blobs);
    pending_shared
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clear();
    drop(sink);
    drop(outbound);
    if terminal_error
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_none()
    {
        LiveRunRegistry::remove_if(&session_id, &run_id);
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
    if frame.flags & FLAG_END != 0 {
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
                && !control_close_natives(pending, outbound).await
            {
                return false;
            }
            if pending.all_client_only() {
                return expose_collected_tools(pending, pending_shared, sink).await;
            }
            let message = parse_connect_error(&frame.payload)
                .map(|error| error.to_string())
                .unwrap_or_else(|| "Cursor upstream ended with pending native tools".to_string());
            report_terminal_error(sink, terminal_error, message).await;
            return false;
        }
        if let Some(error) = parse_connect_error(&frame.payload) {
            let mut fields = serde_json::Map::new();
            fields.insert("status".into(), serde_json::json!(error.status));
            fields.insert("code".into(), serde_json::json!(error.code));
            fields.insert("message".into(), serde_json::json!(error.message));
            crate::logging::create_logger("cursor").warn("connect_end_error", Some(fields));
            report_terminal_error(sink, terminal_error, error.to_string()).await;
            return false;
        } else {
            // Connect END without turn_ended used to emit bare End → silent
            // Anthropic Out:0. Mirror the turn_ended empty-note recovery.
            if !emit_empty_turn_note_if_needed(
                saw_text,
                useful,
                sink,
                deferred,
                pending,
                pending_shared,
                allowed_tool_names,
                turn_ctx.as_ref().map(|ctx| ctx.user_prompt).unwrap_or(""),
                "flag_end",
            )
            .await
            {
                return false;
            }
            let _ = emit_cursor_or_defer(sink, deferred, CursorStreamEvent::End).await;
        }
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
            *useful = true;
            *last_progress = Instant::now();
        }
        return true;
    }

    if let Some(kv) = message.kv_server_message {
        match encode_kv_reply(&kv, kv_blobs) {
            Ok(Some(reply)) => {
                if !send_frame_or_fail(outbound, sink, terminal_error, reply, "KV reply").await {
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
            return expose_collected_tools(pending, pending_shared, sink).await;
        }
        match encode_interaction_auto_response(&query) {
            Ok(Some(reply)) => {
                if !send_frame_or_fail(outbound, sink, terminal_error, reply, "interaction reply")
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
        *last_progress = Instant::now();
        *useful = true;
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
            *last_progress = Instant::now();
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
                    if !send_frame_or_fail(outbound, sink, terminal_error, frame, "exec throw")
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
            if started
                .tool_call
                .as_ref()
                .and_then(|tc| tc.web_fetch_tool_call.as_ref())
                .is_some()
            {
                // Cursor-native WebFetch (ToolCall tag 37). Nested Anthropic
                // hosted web_fetch is emulated on the Messages path; this
                // frame is UI/exec transcript, not ClientOnly.
                if std::env::var("CCP_CURSOR_DEBUG").is_ok() {
                    eprintln!(
                        "[ccp-cursor] web_fetch_tool_call ignored (hosted web_fetch emulator covers nested Anthropic)"
                    );
                }
            }
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
                exec.claude_name = emit_name;
                pending.queue(exec, Duration::ZERO);
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
                return expose_collected_tools(pending, pending_shared, sink).await;
            }
            if mapped.name == "Glob"
                && let Some(emit_name) = resolve_advertised_name("Glob", allowed_tool_names)
            {
                // Official ExecServerMessage has no glob_args (0xlane agent_v1).
                // tool_call_started is the only signal — expose as ClientOnly.
                if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                    return false;
                }
                let mut exec = mcp_client_only_pending_exec(&mapped);
                exec.claude_name = emit_name;
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
                        exec.claude_name = emit_name;
                        // Expose immediately — Cursor may turn_ended right
                        // after the XML in the same chunk; waiting for the
                        // outer select would race into "pending native tools".
                        pending.queue(exec, Duration::ZERO);
                        if !flush_turn_coalescer(sink, deferred, turn_ctx.as_deref_mut()).await {
                            return false;
                        }
                        if !expose_collected_tools(pending, pending_shared, sink).await {
                            return false;
                        }
                    } else if !tool_use.name.is_empty() {
                        // Unknown / native-shaped XML: keep visible as text
                        // so we do not invent a fake Claude tool_use.
                        let input_json = serde_json::to_string(&tool_use.input)
                            .unwrap_or_else(|_| "{}".to_string());
                        let visible = format!(
                            "<tool_use id=\"{}\" name=\"{}\">\n{input_json}\n</tool_use>",
                            tool_use.id, tool_use.name
                        );
                        *saw_text = true;
                        if !emit_cursor_or_defer(
                            sink,
                            deferred,
                            CursorStreamEvent::TextDelta { text: visible },
                        )
                        .await
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
                && !control_close_natives(pending, outbound).await
            {
                return false;
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
        // Heartbeat-only "thinking" with no text/tools yields a contentless
        // Anthropic 200 (Out:0) — Claude Code looks hung then idle. Surface a
        // short visible note so the agent can recover / call Workflow.
        if !emit_empty_turn_note_if_needed(
            saw_text,
            useful,
            sink,
            deferred,
            pending,
            pending_shared,
            allowed_tool_names,
            turn_ctx.as_ref().map(|ctx| ctx.user_prompt).unwrap_or(""),
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

/// Recover an empty Cursor turn: emit a real Anthropic `Workflow` tool_use when
/// that tool was advertised, otherwise a short visible note.
///
/// Used from `turn_ended`, clean Connect `FLAG_END`, and exhausted EOF — all
/// three previously could produce silent Anthropic Out:0 completions.
#[allow(clippy::too_many_arguments)]
async fn emit_empty_turn_note_if_needed(
    saw_text: &mut bool,
    useful: &mut bool,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    deferred: &mut VecDeque<LiveEventResult>,
    pending: &mut PendingExecState,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    allowed_tool_names: Option<&BTreeSet<String>>,
    user_prompt: &str,
    reason: &str,
) -> bool {
    if *saw_text || sink.is_none() {
        return true;
    }
    if workflow_tool_advertised(allowed_tool_names) {
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
    let note = if allowed_tool_names
        .is_some_and(|set| set.iter().any(|n| is_claude_local_tool_name(n)))
    {
        "Cursor finished this turn without text or tool calls. If the user asked for /deep-research or /workflows, call the Workflow tool (for example Workflow with name \"deep-research\") instead of ending silently."
    } else {
        "Cursor finished this turn without text or tool calls."
    };
    *saw_text = true;
    *useful = true;
    // Always leave a structured breadcrumb — empty turns are otherwise invisible
    // in proxy.log (no InteractionUpdate dump unless CCP_CURSOR_DEBUG=1).
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
        crate::logging::create_logger("cursor").info("empty_turn_note", Some(fields));
    }
    if std::env::var_os("CCP_CURSOR_DEBUG").is_some() {
        eprintln!("[ccp-cursor] empty_turn_note reason={reason}");
    }
    emit_cursor_or_defer(
        sink,
        deferred,
        CursorStreamEvent::TextDelta {
            text: note.to_string(),
        },
    )
    .await
}

async fn send_frame_or_fail(
    outbound: &ClientOutbound,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
    terminal_error: &Arc<Mutex<Option<TerminalOutcome>>>,
    frame: Bytes,
    what: &str,
) -> bool {
    if outbound.send_connect_frame(frame).await {
        return true;
    }
    report_terminal_error(sink, terminal_error, format!("Cursor {what} send failed")).await;
    false
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
    {
        let mut slot = terminal_error.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(TerminalOutcome {
                message: message.clone(),
                created_at: Instant::now(),
            });
        }
    }
    if sink.is_some() {
        let _ = send_live_event(sink, Err(message)).await;
    }
}

async fn expose_collected_tools(
    pending: &mut PendingExecState,
    pending_shared: &Arc<Mutex<Vec<PendingCursorExec>>>,
    sink: &mut Option<mpsc::Sender<LiveEventResult>>,
) -> bool {
    let exposed = pending.expose();
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
                    exec.claude_name = emit_name;
                    pending.queue(exec, Duration::ZERO);
                    exposed_client_only = true;
                } else if !tool_use.name.is_empty() {
                    let input_json =
                        serde_json::to_string(&tool_use.input).unwrap_or_else(|_| "{}".to_string());
                    let visible = format!(
                        "<tool_use id=\"{}\" name=\"{}\">\n{input_json}\n</tool_use>",
                        tool_use.id, tool_use.name
                    );
                    *saw_text = true;
                    *useful = true;
                    *last_progress = Instant::now();
                    if !emit_cursor_or_defer(
                        sink,
                        deferred,
                        CursorStreamEvent::TextDelta { text: visible },
                    )
                    .await
                    {
                        return false;
                    }
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

fn qualified_mcp_provider(name: &str) -> Option<&str> {
    name.split_once('/')
        .or_else(|| name.split_once(':'))
        .and_then(|(provider, tool)| {
            (!provider.is_empty() && !tool.is_empty() && !tool.contains('/') && !tool.contains(':'))
                .then_some(provider)
        })
}

/// Decide whether an MCP/XML tool should be ClientOnly, and which Anthropic
/// `tool_use.name` to emit. Cursor may send `claude-local/Workflow` while Claude
/// Code advertised `Workflow`.
fn client_only_anthropic_name(
    mapped_name: &str,
    provider_identifier: &str,
    allowed: Option<&BTreeSet<String>>,
) -> Option<String> {
    let stripped = strip_mcp_provider_prefix(mapped_name);
    if stripped.is_empty() {
        return None;
    }
    let local = is_claude_local_tool_name(stripped)
        || (stripped != mapped_name && is_claude_local_tool_name(mapped_name));
    if !local {
        return None;
    }

    let in_advertised = match allowed {
        None => true,
        Some(set) => set.contains(mapped_name) || set.contains(stripped),
    };
    let claude_local_provider = provider_identifier == CLAUDE_LOCAL_MCP_PROVIDER
        || qualified_mcp_provider(mapped_name) == Some(CLAUDE_LOCAL_MCP_PROVIDER);
    if !claude_local_provider && !in_advertised {
        return None;
    }

    if let Some(set) = allowed {
        if let Some(hit) = set.get(stripped) {
            return Some(hit.clone());
        }
    }
    Some(stripped.to_string())
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
    resolve_advertised_name("AskUserQuestion", allowed)
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

fn workflow_tool_advertised(allowed: Option<&BTreeSet<String>>) -> bool {
    allowed.is_some_and(|set| {
        set.iter()
            .any(|name| strip_mcp_provider_prefix(name).eq_ignore_ascii_case("Workflow"))
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
                    Err(mpsc::error::TrySendError::Full(event)) => tx.send(event).await.is_ok(),
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    } else {
        tx.send(event).await.is_ok()
    }
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
    let Some(allowed) = allowed else {
        return Some(mapped_name.to_string());
    };
    if allowed.contains(mapped_name) {
        return Some(mapped_name.to_string());
    }
    let fallbacks: &[&str] = match mapped_name {
        "Bash" => &["Bash", "Shell", "bash"],
        "Read" => &["Read", "read_file", "ReadFile"],
        // Never fall back to Edit: Claude Edit requires old_string/new_string,
        // while Cursor Write/Edit overwrite maps to {file_path, content}.
        "Write" => &["Write", "write_file", "WriteFile"],
        "Grep" => &["Grep", "grep", "Search"],
        "Glob" => &["Glob", "glob", "Find"],
        "WebSearch" => &["WebSearch", "web_search"],
        "WebFetch" => &["WebFetch", "web_fetch", "Fetch"],
        "TodoWrite" => &["TodoWrite", "TodoWrite"],
        "TodoRead" => &["TodoRead"],
        "AskUserQuestion" => &["AskUserQuestion", "AskQuestion"],
        "CreatePlan" => &["CreatePlan", "Plan"],
        _ => &[],
    };
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
                                if !bytes.is_empty() {
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
                                let status = match error_type {
                                    "rate_limit_error" => 429,
                                    "authentication_error" => 401,
                                    "permission_error" => 403,
                                    _ => 502,
                                };
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
                                state.encoder.finalize();
                                state.done = true;
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
        http::HeaderValue::from_static("no-cache"),
    );
    response.headers_mut().insert(
        http::header::CONNECTION,
        http::HeaderValue::from_static("keep-alive"),
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
            model_id: "composer-2.5".into(),
            conversation_id: Some("conv-test".into()),
            force_http1: false,
            mcp_tools: None,
            opening_checkpoint: None,
        }
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
            live_reconnect_skip_reason(&None, &None, 0, 10),
            Some("no checkpoint")
        );
        assert!(live_reconnect_skip_reason(&Some(vec![0x0a]), &None, 0, 10).is_none());
        assert!(live_reconnect_skip_reason(&None, &Some(vec![0x0a]), 0, 10).is_none());
        assert_eq!(
            live_reconnect_skip_reason(&Some(vec![0x0a]), &None, 10, 10),
            Some("reconnect budget exhausted")
        );
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
            live_reconnect_skip_reason(&Some(vec![0x01]), &None, 0, 10).is_none(),
            "Claude-owed tool_results must not block ResumeAction; the BiDi is still needed"
        );
    }

    #[test]
    fn first_reconnect_has_no_backoff() {
        assert_eq!(live_reconnect_backoff_ms_for(1, false), 0);
        assert!(live_reconnect_backoff_ms_for(2, false) >= 1_000);
        assert!(live_reconnect_backoff_ms_for(2, false) <= 1_200);
        assert!(live_reconnect_backoff_ms_for(3, false) >= 2_000);
        assert_eq!(live_reconnect_backoff_ms_for(20, false), 60_000);
        assert_eq!(
            live_reconnect_backoff_ms_for(10, true),
            0,
            "zero-byte INTERNAL_ERROR must not wait 60s between attempts"
        );
    }

    #[test]
    fn hollow_h2_reconnect_forces_http1() {
        let rst = "error decoding response body: error reading a body from connection: stream error received: unexpected internal error encountered";
        assert!(
            live_reconnect_should_force_http1(true, 0, false, Some(rst)),
            "H2 INTERNAL_ERROR must switch to real HTTP/1.1 immediately — ResumeAction on H2 is always hollow"
        );
        assert!(
            !live_reconnect_should_force_http1(true, 0, false, Some("upstream ended")),
            "clean EOF after progress may retry H2 on a fresh client"
        );
        assert!(live_reconnect_should_force_http1(false, 1, false, None));
        assert!(!live_reconnect_should_force_http1(
            false,
            1,
            true,
            Some(rst)
        ));
        assert!(!live_reconnect_should_force_http1(true, 3, false, None));
    }

    #[test]
    fn h2_internal_error_is_a_stream_reset() {
        let msg = "error decoding response body: error reading a body from connection: stream error received: unexpected internal error encountered";
        assert!(is_h2_stream_reset(msg));
        assert!(!is_h2_stream_reset("Connect error 429: quota"));
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
            anthropic_error_type_from_live_error("Cursor stream stalled"),
            "api_error"
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

    fn dummy_handle(run_id: &str) -> Arc<CursorLiveRunHandle> {
        let (command_tx, _command_rx) = mpsc::channel(1);
        Arc::new(CursorLiveRunHandle {
            run_id: run_id.into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
        })
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
        drop(reservation);
        assert!(!LiveRunRegistry::is_occupied(&session));
        LiveRunRegistry::clear();
    }

    #[test]
    fn nested_agent_run_does_not_supersede_parent() {
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
            client_only_anthropic_name("claude-local/Workflow", "claude-local", Some(&read_only))
                .as_deref(),
            Some("Workflow"),
            "claude-local provider still exposes Workflow when tools[].name did not match"
        );
        assert_eq!(
            client_only_anthropic_name("plugin/search", "plugin", Some(&allowed)),
            None,
            "non-claude-local qualified names stay UI transcript unless advertised"
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
    async fn mcp_qualified_workflow_name_exposes_client_only_as_workflow() {
        for (tool_name, name, provider) in [
            (
                "claude-local/Workflow",
                "claude-local/Workflow",
                "claude-local",
            ),
            ("claude-local:Workflow", "Workflow", "claude-local"),
            ("Workflow", "claude-local/Workflow", "claude-local"),
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
    async fn flag_end_without_workflow_emits_empty_turn_note() {
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
        assert!(saw_text, "empty-note must mark saw_text");

        let note = event_rx.try_recv().expect("empty-note TextDelta");
        match note {
            Ok(LiveRunEvent::Cursor(CursorStreamEvent::TextDelta { text })) => {
                assert!(
                    text.contains("without text or tool calls"),
                    "unexpected note: {text}"
                );
                assert!(
                    !text.contains("Workflow"),
                    "note-only when Workflow was not advertised"
                );
            }
            other => panic!("expected TextDelta note, got {other:?}"),
        }
        let end = event_rx.try_recv().expect("End after note");
        assert!(matches!(
            end,
            Ok(LiveRunEvent::Cursor(CursorStreamEvent::End))
        ));
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
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::clone(&pending_shared),
            Arc::clone(&terminal_error),
            Arc::clone(&completed),
            Some(BTreeSet::from(["Read".into()])),
            "multi-test-session".into(),
            "multi-test-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            test_reconnect_context(),
        ));
        let handle = CursorLiveRunHandle {
            run_id: "multi-test-run".into(),
            command_tx,
            pending: Arc::clone(&pending_shared),
            terminal_error,
            completed,
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
        assert_eq!(
            tools
                .iter()
                .map(|tool| tool.tool_use_id.as_str())
                .collect::<Vec<_>>(),
            ["tool-11", "tool-12"]
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
                    "tool-12".into(),
                    serde_json::json!({"type":"tool_result","content":"two"}),
                ),
                (
                    "tool-11".into(),
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

    #[test]
    fn cancel_removes_running_entry_so_reserve_succeeds() {
        let session = format!("cancel-session-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "run-cancel".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
        });
        let reservation = LiveRunRegistry::reserve(&session).expect("reserve");
        if reservation.insert(Arc::clone(&handle)).is_err() {
            panic!("insert running handle");
        }
        assert!(LiveRunRegistry::get(&session).is_some());

        assert!(LiveRunRegistry::cancel(&session));
        assert!(LiveRunRegistry::get(&session).is_none());
        // Cancel command must be delivered so the driver can exit.
        assert!(matches!(command_rx.try_recv(), Ok(RunCommand::Cancel)));
        // Slot is free for a new turn (Claude Code retry after idle timeout).
        assert!(LiveRunRegistry::reserve(&session).is_some());
        LiveRunRegistry::clear();
    }

    #[test]
    fn supersede_replaces_occupant_with_fresh_reservation() {
        let session = format!("supersede-session-{}", uuid::Uuid::new_v4());
        LiveRunRegistry::clear();
        let (command_tx, _command_rx) = mpsc::channel(1);
        let handle = Arc::new(CursorLiveRunHandle {
            run_id: "old-run".into(),
            command_tx,
            pending: Arc::new(Mutex::new(Vec::new())),
            terminal_error: Arc::new(Mutex::new(None)),
            completed: Arc::new(AtomicBool::new(false)),
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

        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::new(Mutex::new(Vec::new())),
            terminal_error,
            Arc::clone(&completed),
            Some(BTreeSet::from(["Read".to_string()])),
            "heartbeat-drop-session".into(),
            "heartbeat-drop-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            test_reconnect_context(),
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
        let driver = tokio::spawn(drive_live_run(
            upstream_rx,
            upstream_tx.clone(),
            ClientOutbound::Bidi(request_tx),
            command_rx,
            initial_sink,
            Arc::new(Mutex::new(Vec::new())),
            terminal_error,
            Arc::clone(&completed),
            None,
            "drop-test-session".into(),
            "drop-test-run".into(),
            HashMap::new(),
            String::new(),
            RequestContext::default(),
            test_reconnect_context(),
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
