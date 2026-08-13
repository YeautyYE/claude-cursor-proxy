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
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
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
    LiveEventResult, LiveRunEvent, LiveRunIdentity, LiveRunRegistry, live_run_key_for,
    live_sse_response,
};
use crate::providers::cursor::model::{anthropic_wire_model, resolve_cursor_model};
use crate::providers::cursor::request::{
    CursorPromptOptions, claude_local_mcp_tools, cursor_request_context,
    latest_user_is_only_tool_results, render_cursor_prompt, render_cursor_prompt_parts_with,
    request_has_client_only_tool_results,
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

/// Process-wide HTTP client so TLS/H2 connections to api2.cursor.sh are reused
/// across Claude Code turns. Rebuilds when `CCP_CURSOR_BASE_URL` changes (tests
/// and mock upstreams flip this between runs).
fn shared_cursor_http_client() -> CursorHttpClient {
    use std::sync::{Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<Option<CursorHttpClient>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(None));
    let base = crate::config::cursor_base_url();
    let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(existing) = guard.as_ref()
        && existing.base_url() == base.as_str()
    {
        return existing.clone();
    }
    let fresh = CursorHttpClient::new();
    *guard = Some(fresh.clone());
    fresh
}

const MAX_SESSION_USAGE: usize = 10_000;
const LIVE_USAGE_TAP_CAP: usize = 512;

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
    while let Some(item) = events.recv().await {
        match item {
            Ok(LiveRunEvent::Cursor(event)) => {
                let ended = matches!(event, CursorStreamEvent::End);
                acc.push(&event);
                if ended {
                    break;
                }
            }
            Ok(LiveRunEvent::NativeToolBatch(tools)) => {
                for tool in tools {
                    acc.push_native_tool(tool.tool_use_id, tool.name, tool.input);
                }
                break;
            }
            Err(error) => return Err(error),
        }
    }
    if !acc.has_useful() {
        return Err("Cursor stream produced no useful progress".into());
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
        Err(error) => json_error(StatusCode::BAD_GATEWAY, "api_error", error),
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
    if ctx.claude_code.agent_id.is_none()
        && ctx.claude_code.parent_agent_id.is_none()
        && ctx.claude_code.app.is_none()
    {
        return;
    }
    create_logger("cursor").info(
        "live_start_nested_headers",
        Some(serde_json::Map::from_iter([
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
    Resumed(Response),
    TerminalError(String),
    MissingTools(Vec<String>),
    ResumeError(CursorError),
    Conflict,
    Free,
}

/// Wait for an in-flight BiDi run to expose pending tools (and resume), finish,
/// or fail — instead of immediately 409'ing concurrent same-session POSTs.
#[allow(clippy::too_many_arguments)]
async fn await_live_run_resume(
    session_id: &str,
    agent_id: Option<&str>,
    body: &MessagesRequest,
    message_id: String,
    wire_model: String,
    estimated_input: u64,
    monitor: Option<(crate::monitor::MonitorHandle, String)>,
    want_stream: bool,
) -> LiveResumeOutcome {
    let has_tool_results = request_has_any_tool_result(body);
    // Tool-result resumes: wait for pending tools to appear (race with expose).
    // Nested turns without tool_results: brief queue, then supersede — a long
    // wait (was 15s) just delayed Claude Code retries after idle disconnect.
    let wait_ms = if has_tool_results {
        env_u64_millis("CCP_CURSOR_LIVE_RESUME_WAIT_MS", 30_000)
    } else {
        env_u64_millis("CCP_CURSOR_LIVE_NESTED_WAIT_MS", 1_500)
    };
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
    let mut last_missing: Option<Vec<String>> = None;

    while tokio::time::Instant::now() < deadline {
        if let Some(error) = LiveRunRegistry::take_terminal_error_run(session_id, agent_id) {
            return LiveResumeOutcome::TerminalError(error);
        }
        let Some(run) = LiveRunRegistry::get_run(session_id, agent_id) else {
            if LiveRunRegistry::is_occupied_run(session_id, agent_id) {
                // Starting reservation: wait for Running instead of treating
                // the session as free (which opened a second Cursor run).
                tokio::time::sleep(Duration::from_millis(25)).await;
                continue;
            }
            return LiveResumeOutcome::Free;
        };
        let pending = run.pending_tools();
        if !pending.is_empty() {
            match collect_live_tool_results(body, &pending) {
                Ok(tool_results) => match run.resume_batch(tool_results).await {
                    Ok(events) => {
                        return LiveResumeOutcome::Resumed(
                            live_downstream_response(
                                want_stream,
                                session_id,
                                events,
                                message_id,
                                wire_model,
                                estimated_input,
                                monitor,
                            )
                            .await,
                        );
                    }
                    Err(error) => return LiveResumeOutcome::ResumeError(error),
                },
                Err(missing) => {
                    last_missing = Some(missing);
                    if !has_tool_results {
                        // Another agent's tool turn owns the pending set — keep
                        // queuing until those results land and the run frees.
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        continue;
                    }
                    // Partial/mismatched tool_results: brief grace then 400.
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    continue;
                }
            }
        }
        // Still generating with empty pending — wait for tools or completion.
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    if let Some(error) = LiveRunRegistry::take_terminal_error_run(session_id, agent_id) {
        return LiveResumeOutcome::TerminalError(error);
    }
    let Some(run) = LiveRunRegistry::get_run(session_id, agent_id) else {
        if LiveRunRegistry::is_occupied_run(session_id, agent_id) {
            return LiveResumeOutcome::Conflict;
        }
        return LiveResumeOutcome::Free;
    };
    let pending = run.pending_tools();
    if !pending.is_empty() {
        match collect_live_tool_results(body, &pending) {
            Ok(tool_results) => match run.resume_batch(tool_results).await {
                Ok(events) => {
                    return LiveResumeOutcome::Resumed(
                        live_downstream_response(
                            want_stream,
                            session_id,
                            events,
                            message_id,
                            wire_model,
                            estimated_input,
                            monitor,
                        )
                        .await,
                    );
                }
                Err(error) => return LiveResumeOutcome::ResumeError(error),
            },
            Err(missing) => return LiveResumeOutcome::MissingTools(missing),
        }
    }
    if let Some(missing) = last_missing {
        return LiveResumeOutcome::MissingTools(missing);
    }
    LiveResumeOutcome::Conflict
}

fn env_u64_millis(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn collect_live_tool_results(
    body: &MessagesRequest,
    pending: &[PendingCursorExec],
) -> Result<Vec<(String, serde_json::Value)>, Vec<String>> {
    let tool_results: Vec<(String, serde_json::Value)> = pending
        .iter()
        .filter_map(|exec| {
            find_tool_result(body, &exec.tool_use_id)
                .cloned()
                .map(|result| (exec.tool_use_id.clone(), result))
        })
        .collect();
    if tool_results.len() == pending.len() {
        return Ok(tool_results);
    }

    let returned: std::collections::HashSet<&str> = tool_results
        .iter()
        .map(|(tool_use_id, _)| tool_use_id.as_str())
        .collect();
    Err(pending
        .iter()
        .map(|exec| exec.tool_use_id.clone())
        .filter(|tool_use_id| !returned.contains(tool_use_id.as_str()))
        .collect())
}

fn request_has_any_tool_result(body: &MessagesRequest) -> bool {
    body.messages
        .iter()
        .rev()
        .any(|message| match &message.content {
            serde_json::Value::Array(blocks) => blocks.iter().any(|block| {
                block.get("type").and_then(|value| value.as_str()) == Some("tool_result")
            }),
            _ => false,
        })
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
        let model = body.model.as_deref().unwrap_or("cursor");
        let wire_model = anthropic_wire_model(model);

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
        if let Some(session_id) = ctx.session_id.as_deref() {
            let agent_id = claude_agent_id(&ctx);
            if let Some(error) = LiveRunRegistry::take_terminal_error_run(session_id, agent_id) {
                return json_error(StatusCode::BAD_GATEWAY, "api_error", error);
            }
            if LiveRunRegistry::is_occupied_run(session_id, agent_id) {
                let estimated_input = estimate_request_input_tokens(&body);
                let monitor = ctx
                    .monitor
                    .clone()
                    .map(|handle| (handle, ctx.req_id.clone()));
                match await_live_run_resume(
                    session_id,
                    agent_id,
                    &body,
                    message_id.clone(),
                    wire_model.clone(),
                    estimated_input,
                    monitor.clone(),
                    want_stream,
                )
                .await
                {
                    LiveResumeOutcome::Resumed(response) => return response,
                    LiveResumeOutcome::TerminalError(error) => {
                        return json_error(StatusCode::BAD_GATEWAY, "api_error", error);
                    }
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
                    LiveResumeOutcome::Conflict => {
                        // Zombie / still-thinking BiDi after Claude Code idle
                        // disconnect or a nested retry without matching tools.
                        // Waiting then 409 cascaded retries; supersede instead.
                        LiveRunRegistry::cancel_run(session_id, agent_id);
                    }
                    LiveResumeOutcome::ResumeError(error) => {
                        return map_cursor_error_to_response(&error);
                    }
                    LiveResumeOutcome::Free => {}
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

        let client = shared_cursor_http_client();
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.upstream_started(&ctx.req_id);
        }
        let mut token = auth.access_token.clone();

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

            // Concurrent same-session POSTs (Claude Code retry after idle / 409)
            // race on Starting→Running. Retry supersede+start instead of 409.
            // Nested agents reserve `(session, agent_id)` so they do not steal
            // the parent that shares X-Claude-Code-Session-Id.
            let mut start_error: Option<CursorError> = None;
            for attempt in 0..3_u8 {
                let Some(reservation) = (if attempt == 0 {
                    LiveRunRegistry::reserve_run(sid, identity.agent_id)
                        .or_else(|| LiveRunRegistry::supersede_run(sid, identity.agent_id))
                } else {
                    LiveRunRegistry::supersede_run(sid, identity.agent_id)
                }) else {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    continue;
                };

                let start = match client
                    .start_live_agent_with_identity(
                        &token,
                        user_text,
                        model,
                        &images,
                        custom_system,
                        identity,
                        allowed.clone(),
                        mcp_tools.clone(),
                        cursor_request_context(&body),
                    )
                    .await
                {
                    Ok(start) => Ok(start),
                    Err(error) if error.status == 401 && auth.refresh_token.is_some() => {
                        match force_refresh_cursor_auth() {
                            Ok(Some(refreshed)) => {
                                token = refreshed.access_token;
                                client
                                    .start_live_agent_with_identity(
                                        &token,
                                        user_text,
                                        model,
                                        &images,
                                        custom_system,
                                        identity,
                                        allowed.clone(),
                                        mcp_tools.clone(),
                                        cursor_request_context(&body),
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
                        if let Err(orphaned) = reservation.insert(Arc::clone(&start.handle)) {
                            // Another request stole the slot during upstream open.
                            orphaned.cancel();
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            continue;
                        }
                        return live_downstream_response(
                            want_stream,
                            sid,
                            start.events,
                            message_id,
                            wire_model,
                            estimated_input,
                            monitor,
                        )
                        .await;
                    }
                    Err(error) => {
                        drop(reservation);
                        start_error = Some(error);
                        break;
                    }
                }
            }

            if let Some(error) = start_error {
                // Only fall through to buffered run_agent on transport failures.
                // 400/401/403/429 are semantic — retrying wastes quota and can
                // duplicate a request Cursor already accepted.
                if bridge_eligible
                    || !crate::providers::cursor::live::is_retryable_live_transport_error(&error)
                {
                    return map_cursor_error_to_response(&error);
                }
            } else if bridge_eligible {
                // Exhausted takeover retries while tools require the live path.
                return json_error(
                    StatusCode::CONFLICT,
                    "invalid_request_error",
                    "A Cursor live run is already active for this session",
                );
            }
        }

        let upstream = match client
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
            Ok(r) => r,
            Err(e) if e.status == 401 && auth.refresh_token.is_some() => {
                // One force-refresh + retry on unauthenticated (Codex-style).
                match force_refresh_cursor_auth() {
                    Ok(Some(refreshed)) => {
                        token = refreshed.access_token;
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
                            Ok(r) => r,
                            Err(e2) => return map_cursor_error_to_response(&e2),
                        }
                    }
                    _ => return map_cursor_error_to_response(&e),
                }
            }
            Err(e) => {
                return map_cursor_error_to_response(&e);
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

fn map_cursor_error_to_response(err: &client::CursorError) -> Response {
    let detail = err
        .detail
        .as_deref()
        .filter(|s| !s.is_empty())
        .unwrap_or(err.message.as_str());
    match err.status {
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
        429 => {
            let retry_after = err.retry_after.as_deref().unwrap_or("5");
            let resp = json_error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                &err.message,
            );
            let headers = [(http::header::RETRY_AFTER, retry_after)];
            (headers, resp).into_response()
        }
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
        crate::providers::cursor::conversation::save_checkpoint(
            &session,
            vec![0x0a, 0x02, 0x01, 0x02],
        );
        let nested = RequestContext {
            req_id: "req".into(),
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
        };
        let nested_cont = continuation_for_request(Some(&session), &nested);
        assert!(
            !nested_cont.has_checkpoint,
            "nested agent must not compact against the parent Cursor checkpoint"
        );

        let parent = RequestContext {
            req_id: "req".into(),
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
