//! Long-lived Cursor Agent BiDi runs.
//!
//! A Claude Code tool turn spans two Anthropic HTTP requests, while Cursor keeps
//! the model + exec loop on one `AgentService/Run` stream. This module owns that
//! upstream stream between requests and sends native exec results back through
//! the original request-body channel.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::convert::Infallible;
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
    ConnectFrame, ConnectFrameDecoder, FLAG_END, encode_connect_frame, parse_connect_error,
};
use super::exec_results::{
    CursorExecKind, PendingCursorExec, encode_control_close, encode_control_throw,
    encode_exec_heartbeat, encode_tool_result_frames,
};
use super::http1::{self, BidiAppendSession};
use super::proto::{
    self, AgentClientMessage, AskQuestionInteractionResponse, AskQuestionRejected,
    AskQuestionResult, ClientHeartbeat, CreatePlanRequestResponse, CreatePlanResult,
    CreatePlanSuccess, ExecClientMessage, GetBlobResult, InteractionApproved, InteractionQuery,
    InteractionRejected, InteractionResponse, KvClientMessage, KvServerMessage,
    McpAuthRequestResponse, RequestContext, RequestContextResult, RequestContextSuccess,
    SetBlobResult, SwitchModeRequestResponse, WebFetchRequestResponse, WebSearchRequestResponse,
};
use super::request::{CursorSelectedImage, is_claude_local_tool_name};
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
}

impl LogicalToolTracker {
    fn started(&mut self, call_id: &str, model_call_id: &str) {
        if !call_id.is_empty() {
            self.named.insert(call_id.to_string());
        } else {
            *self
                .anonymous_by_model
                .entry(model_call_id.to_string())
                .or_default() += 1;
        }
    }

    fn completed(&mut self, call_id: &str, model_call_id: &str) {
        if !call_id.is_empty() {
            self.named.remove(call_id);
            return;
        }
        let mut remove_model = false;
        if let Some(count) = self.anonymous_by_model.get_mut(model_call_id) {
            *count = count.saturating_sub(1);
            remove_model = *count == 0;
        }
        if remove_model {
            self.anonymous_by_model.remove(model_call_id);
        }
    }

    fn resolve_exec(&mut self, exec: &PendingCursorExec) {
        if self.named.remove(&exec.tool_use_id) {
            return;
        }
        if let Some(exec_id) = exec.exec_id.as_deref()
            && self.named.remove(exec_id)
        {
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
            return;
        }
        if let Some(exec_id) = exec.exec_id.as_deref()
            && self.named.remove(exec_id)
        {
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
    }
}

enum LiveRunEntry {
    Starting { reservation_id: String },
    Running(Arc<CursorLiveRunHandle>),
}

static LIVE_RUNS: LazyLock<Mutex<HashMap<String, LiveRunEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
            runs.get(&self.session_id),
            Some(LiveRunEntry::Starting { reservation_id })
                if reservation_id == &self.reservation_id
        );
        if !owns_reservation {
            return Err(handle);
        }
        runs.insert(self.session_id.clone(), LiveRunEntry::Running(handle));
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
            runs.get(&self.session_id),
            Some(LiveRunEntry::Starting { reservation_id })
                if reservation_id == &self.reservation_id
        );
        if owns_reservation {
            runs.remove(&self.session_id);
        }
    }
}

pub struct LiveRunRegistry;

impl LiveRunRegistry {
    /// Claim a session before awaiting upstream startup. This closes the race
    /// where two initial requests both observed an empty registry and started
    /// separate Cursor runs.
    pub fn reserve(session_id: &str) -> Option<LiveRunReservation> {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        if runs.contains_key(session_id) {
            return None;
        }
        let reservation_id = uuid::Uuid::new_v4().to_string();
        runs.insert(
            session_id.to_string(),
            LiveRunEntry::Starting {
                reservation_id: reservation_id.clone(),
            },
        );
        Some(LiveRunReservation {
            session_id: session_id.to_string(),
            reservation_id,
            committed: false,
        })
    }

    /// Drop any in-flight live run for `session_id` and signal Cancel so the
    /// BiDi driver exits. Used when Claude Code disconnects/retries and the
    /// old run would otherwise 409 as "already generating".
    pub fn cancel(session_id: &str) -> bool {
        let entry = {
            let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
            runs.remove(session_id)
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

    /// Cancel any occupant, then reserve. Returns None only if another starter
    /// wins a race after our cancel (caller should 409 or retry once).
    pub fn supersede(session_id: &str) -> Option<LiveRunReservation> {
        Self::cancel(session_id);
        Self::reserve(session_id)
    }

    pub fn get(session_id: &str) -> Option<Arc<CursorLiveRunHandle>> {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        match runs.get(session_id) {
            // Completed runs (incl. terminal failures awaiting take_terminal_error)
            // must not look "still generating" to concurrent POSTs.
            Some(LiveRunEntry::Running(handle)) if !handle.is_completed() => {
                Some(Arc::clone(handle))
            }
            Some(LiveRunEntry::Running(_)) | Some(LiveRunEntry::Starting { .. }) | None => None,
        }
    }

    pub fn take_terminal_error(session_id: &str) -> Option<String> {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        Self::prune_finished(&mut runs);
        let error = match runs.get(session_id) {
            Some(LiveRunEntry::Running(handle)) => handle.take_terminal_error(),
            Some(LiveRunEntry::Starting { .. }) | None => None,
        };
        if error.is_some() {
            runs.remove(session_id);
        }
        error
    }

    fn prune_finished(runs: &mut HashMap<String, LiveRunEntry>) {
        runs.retain(|_, entry| match entry {
            LiveRunEntry::Starting { .. } => true,
            LiveRunEntry::Running(handle) => !handle.is_completed() || handle.has_terminal_error(),
        });
    }

    fn remove_if(session_id: &str, run_id: &str) {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        if matches!(
            runs.get(session_id),
            Some(LiveRunEntry::Running(handle)) if handle.run_id == run_id
        ) {
            runs.remove(session_id);
        }
    }

    #[cfg(test)]
    pub fn clear() {
        let mut runs = LIVE_RUNS.lock().unwrap_or_else(|e| e.into_inner());
        for entry in runs.values() {
            if let LiveRunEntry::Running(handle) = entry {
                handle.cancel();
            }
        }
        runs.clear();
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
        let continuation = super::conversation::continuation_for(Some(session_id));
        let run_request = build_run_request_with_continuation(
            prompt,
            &resolved,
            images,
            &request_id,
            custom_system_prompt,
            &continuation,
            None,
        );
        let first_message = AgentClientMessage {
            run_request: Some(run_request),
            exec_client_message: None,
            kv_client_message: None,
            exec_client_control_message: None,
            interaction_response: None,
            client_heartbeat: None,
        };

        let identity = LiveIdentityHeaders::build(token);
        let (outbound, response) = self
            .open_live_transport(
                token,
                &request_id,
                &first_message,
                &identity,
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

        let worker_session = session_id.to_string();
        let seeded_blobs: HashMap<Vec<u8>, Vec<u8>> =
            continuation.pre_fetched_blobs.into_iter().collect();
        let reconnect = LiveReconnectContext {
            http: self.clone(),
            token: token.to_string(),
            identity,
            model_id: resolved.model_id.clone(),
            conversation_id: continuation.conversation_id.clone(),
            force_http1,
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
            .header("connect-accept-encoding", "gzip,br")
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
            .header("connect-accept-encoding", "gzip,br")
            .header("user-agent", "connect-es/1.6.1")
            .header("x-cursor-client-type", &identity.client_type)
            .header("x-cursor-client-version", &identity.client_version)
            .header("x-ghost-mode", &identity.ghost_mode)
            .header("x-request-id", request_id)
            .header("x-cursor-streaming", "true")
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
    tokio::spawn(async move {
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            let mapped = match item {
                Ok(chunk) => Ok(Some(chunk)),
                Err(e) => Err(e.to_string()),
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
        408 | 421 | 429 | 464 | 502 | 503 | 504
    ) || err.message.contains("error sending request")
        || err.message.contains("connection")
        || err
            .detail
            .as_deref()
            .is_some_and(|d| d.contains("HTTP_1_1_REQUIRED") || d.contains("bidi"))
}

/// Re-open AgentService/Run with `ResumeAction` after a transport stall.
/// Returns true when a new upstream was installed and the driver should continue.
#[allow(clippy::too_many_arguments)]
async fn try_live_reconnect(
    reconnect: &LiveReconnectContext,
    outbound: &mut ClientOutbound,
    upstream_tx: &mpsc::Sender<Result<Option<Bytes>, String>>,
    decoder: &mut ConnectFrameDecoder,
    latest_checkpoint: &Option<Vec<u8>>,
    kv_blobs: &HashMap<Vec<u8>, Vec<u8>>,
    pending: &PendingExecState,
    reconnect_attempts: &mut u32,
    max_reconnects: u32,
    last_progress: &mut Instant,
    resume_grace_until: &mut Option<Instant>,
    resume_grace: Duration,
) -> bool {
    if !pending.is_empty() {
        return false;
    }
    let Some(checkpoint) = latest_checkpoint.as_ref().filter(|c| !c.is_empty()) else {
        return false;
    };
    if *reconnect_attempts >= max_reconnects {
        return false;
    }
    *reconnect_attempts += 1;

    // CLI turn-runner: base 1s * 2^attempt, cap 60s, +20% jitter.
    let base_ms = 1_000u64 << (*reconnect_attempts - 1).min(6);
    let jitter = ((base_ms as f64) * 0.2 * ((*reconnect_attempts as f64 * 0.37) % 1.0)) as u64;
    tokio::time::sleep(Duration::from_millis((base_ms + jitter).min(60_000))).await;

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
    let run_request = build_resume_run_request(&resolved, &request_id, &cont, None);
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
            if std::env::var("CCP_CURSOR_DEBUG").is_ok() {
                eprintln!(
                    "[ccp-cursor] ResumeAction reconnect ok (attempt {reconnect_attempts}/{max_reconnects})"
                );
            }
            *outbound = new_outbound;
            spawn_upstream_pump(response.bytes_stream(), upstream_tx.clone());
            *decoder = ConnectFrameDecoder::new();
            *last_progress = Instant::now();
            *resume_grace_until = Some(Instant::now() + resume_grace);
            true
        }
        Err(err) => {
            if std::env::var("CCP_CURSOR_DEBUG").is_ok() {
                eprintln!(
                    "[ccp-cursor] ResumeAction reconnect failed: {} ({})",
                    err.message, err.status
                );
            }
            false
        }
    }
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
    reconnect: LiveReconnectContext,
) {
    let mut sink = Some(initial_sink);
    let mut pending = PendingExecState::default();
    let mut deferred = VecDeque::<LiveEventResult>::new();
    let mut decoder = ConnectFrameDecoder::new();
    let mut kv_blobs = seeded_blobs;
    let mut latest_checkpoint: Option<Vec<u8>> = None;
    let mut saw_text = false;
    let mut useful = false;
    let mut logical_tools_waiting = LogicalToolTracker::default();
    let mut last_progress = Instant::now();
    let mut resume_grace_until: Option<Instant> = None;
    let mut xml_parser = CursorToolUseXmlParser::new(allowed_tool_names.clone());
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
                        for frame in frames {
                            if !outbound.send_connect_frame(frame).await {
                                send_failed = true;
                                break;
                            }
                        }
                        if send_failed {
                            let _ = ack.send(Err("Cursor request stream closed during tool resume".into()));
                            break 'driver;
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
                        reconnect_attempts = 0;
                        let frames = match decoder.push(&chunk) {
                            Ok(frames) => frames,
                            Err(error) => {
                                let message = format!("Cursor frame decode: {error}");
                                report_terminal_error(&mut sink, &terminal_error, message).await;
                                break 'driver;
                            }
                        };
                        for frame in frames {
                            if !process_live_frame(
                                frame,
                                &outbound,
                                &mut sink,
                                &mut deferred,
                                &mut pending,
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
                            ).await {
                                break 'driver;
                            }
                        }
                        // Quiet window already elapsed (incl. TOOL_BATCH_MS=0):
                        // expose in this iteration so we do not wait for the
                        // next select pass behind heartbeats / idle sleep.
                        if pending
                            .collect_deadline()
                            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
                            && !expose_collected_tools(&mut pending, &pending_shared, &mut sink)
                                .await
                        {
                            break 'driver;
                        }
                    }
                    Some(Ok(None)) | None => {
                        // Abrupt EOF without Connect END / turn_ended — try
                        // ResumeAction reconnect (CLI stall recovery).
                        if try_live_reconnect(
                            &reconnect,
                            &mut outbound,
                            &upstream_tx,
                            &mut decoder,
                            &latest_checkpoint,
                            &kv_blobs,
                            &pending,
                            &mut reconnect_attempts,
                            max_reconnects,
                            &mut last_progress,
                            &mut resume_grace_until,
                            resume_grace,
                        )
                        .await
                        {
                            continue 'driver;
                        }
                        if pending.is_empty() {
                            let _ = emit_cursor_or_defer(&mut sink, &mut deferred, CursorStreamEvent::End).await;
                        } else {
                            report_terminal_error(
                                &mut sink,
                                &terminal_error,
                                "Cursor upstream ended with pending native tools".into(),
                            )
                            .await;
                        }
                        break 'driver;
                    }
                    Some(Err(error)) => {
                        let message = format!("Cursor response stream: {error}");
                        if try_live_reconnect(
                            &reconnect,
                            &mut outbound,
                            &upstream_tx,
                            &mut decoder,
                            &latest_checkpoint,
                            &kv_blobs,
                            &pending,
                            &mut reconnect_attempts,
                            max_reconnects,
                            &mut last_progress,
                            &mut resume_grace_until,
                            resume_grace,
                        )
                        .await
                        {
                            continue 'driver;
                        }
                        report_terminal_error(&mut sink, &terminal_error, message).await;
                        break 'driver;
                    }
                }
            }
            _ = exec_heartbeat.tick(), if !pending.is_empty() => {
                // Never await append/send here — even with upstream preferred in
                // biased select, a blocking BidiAppend freezes this task for a
                // full RTT while Cursor keeps sending deltas (CLI does not).
                let ids: Vec<u32> = pending.all().map(|current| current.id).collect();
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
                if !expose_collected_tools(&mut pending, &pending_shared, &mut sink).await {
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
                    if last_progress.elapsed() >= stream_idle {
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
    if let Some(checkpoint) = latest_checkpoint.take() {
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
) -> bool {
    if frame.flags & FLAG_END != 0 {
        if !pending.is_empty() {
            let message = parse_connect_error(&frame.payload)
                .map(|error| error.to_string())
                .unwrap_or_else(|| "Cursor upstream ended with pending native tools".to_string());
            report_terminal_error(sink, terminal_error, message).await;
            return false;
        }
        if let Some(error) = parse_connect_error(&frame.payload) {
            let _ = emit_or_defer(sink, deferred, Err(error.to_string())).await;
        } else {
            let _ = emit_cursor_or_defer(sink, deferred, CursorStreamEvent::End).await;
        }
        return false;
    }
    let message = match super::client::decode_frame_payload(&frame) {
        Ok(message) => message,
        Err(_) => return true,
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
                if !outbound.send_connect_frame(reply).await {
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
        match encode_interaction_auto_response(&query) {
            Ok(Some(reply)) => {
                if !outbound.send_connect_frame(reply).await {
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
            match encode_request_context_reply(&exec) {
                Ok(reply) => {
                    if !outbound.send_connect_frame(reply).await {
                        return false;
                    }
                }
                Err(error) => {
                    let _ = emit_or_defer(sink, deferred, Err(error.to_string())).await;
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
                    if !outbound.send_connect_frame(frame).await {
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
                    if !outbound.send_connect_frame(frame).await {
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
        if let Some(started) = update.tool_call_started {
            // UI transcript only. Execution is driven exclusively by
            // ExecServerMessage, otherwise tool_call_started + exec duplicates.
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
            if !emit_cursor_or_defer(
                sink,
                deferred,
                CursorStreamEvent::ThinkingDelta {
                    text: thinking.text,
                },
            )
            .await
            {
                return false;
            }
        }
        if update.heartbeat.is_some() {
            // Server keep-alive during quiet thinking — refresh idle timers so
            // we do not invent End / stall while BiDi is still healthy.
            *last_progress = Instant::now();
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
                        if !emit_cursor_or_defer(
                            sink,
                            deferred,
                            CursorStreamEvent::TextDelta { text: t },
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
                        let allowed = allowed_tool_names
                            .map(|set| set.contains(&tool_use.name))
                            .unwrap_or(true);
                        if allowed && is_claude_local_tool_name(&tool_use.name) {
                            let exec = client_only_pending_exec(&tool_use);
                            // Expose immediately — Cursor may turn_ended right
                            // after the XML; a quiet window would race into
                            // "pending native tools" errors.
                            pending.queue(exec, Duration::ZERO);
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
            && !emit_cursor_or_defer(
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
        if let Some(turn) = update.turn_ended {
            if !pending.is_empty() {
                report_terminal_error(
                    sink,
                    terminal_error,
                    "Cursor turn ended with pending native tools".into(),
                )
                .await;
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
                        .saturating_add(turn.reasoning_tokens.unwrap_or(0)),
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
    }
    true
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
    // Client-only tools (Workflow/Skill/…) are fulfilled by Claude Code locally.
    // End this BiDi run so the next Anthropic turn starts fresh with tool_result
    // history — Cursor has no exec protocol for these tools.
    !client_only
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

fn encode_request_context_reply(exec: &proto::ExecServerMessage) -> Result<Bytes, CursorError> {
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
                    request_context: Some(RequestContext {}),
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
/// do not stall waiting for IDE UI. AskQuestion is rejected with an explicit
/// reason (Claude Code has no Cursor approval modal).
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
        // Unknown / future InteractionQuery — reject rather than hang the BiDi
        // stream waiting for an IDE approval UI that will never appear.
        response.ask_question_interaction_response = Some(AskQuestionInteractionResponse {
            result: Some(AskQuestionResult {
                rejected: Some(AskQuestionRejected {
                    reason:
                        "claude-code-proxy: unsupported InteractionQuery; cannot present Cursor UI"
                            .into(),
                }),
            }),
        });
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
                                let data = serde_json::json!({
                                    "type": "error",
                                    "error": {"type": "api_error", "message": error}
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
        ExecReadArgs, ExecServerMessage, InteractionUpdate, RequestContextArgs, TextDelta,
        TurnEnded,
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
        let frames = encode_request_context_reply(&exec).unwrap();
        let decoded = super::super::client::decode_upstream_frames(&frames).unwrap();
        assert_eq!(decoded.len(), 2);

        let result = AgentClientMessage::decode(decoded[0].payload.as_ref()).unwrap();
        assert!(
            result
                .exec_client_message
                .unwrap()
                .request_context_result
                .is_some()
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
    async fn interaction_heartbeat_refreshes_idle_progress() {
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
        )
        .await;
        assert!(cont);
        assert!(
            last_progress.elapsed() < Duration::from_secs(1),
            "server InteractionUpdate.heartbeat must refresh idle timer"
        );
    }

    #[test]
    fn anthropic_ping_sse_bytes_are_nonempty() {
        let bytes = format_sse_event_bytes(EVENT_PING, &serde_json::json!({ "type": "ping" }));
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("event: ping"));
        assert!(text.contains(r#""type":"ping"#) || text.contains(r#""type": "ping"#));
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
            LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
                text: "A".into(),
            }),
        );
        let first = String::from_utf8(encoder.take_bytes()).unwrap();
        assert!(
            first.contains("content_block_delta") && first.contains("\"text\":\"A\""),
            "first chunk missing A: {first}"
        );

        apply_live_run_event(
            &mut encoder,
            LiveRunEvent::Cursor(CursorStreamEvent::TextDelta {
                text: "B".into(),
            }),
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
