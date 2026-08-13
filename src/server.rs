use crate::{
    anthropic::json_error,
    anthropic::schema::MessagesRequest,
    logging::{Logger, REDACT_KEYS, create_logger},
    monitor::{EndpointKind, MonitorHandle},
    project,
    provider::{ClaudeCodeAgentHeaders, RequestContext},
    registry::{Registry, normalize_incoming_model},
    session::{self},
    traffic::{TrafficCaptureOptions, create_traffic_capture},
};
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    response::Response,
    routing::{get, post},
    serve::ListenerExt,
};
use http_body_util::{BodyExt, StreamBody};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use uuid::Uuid;

pub struct ServerConfig {
    pub bind_address: String,
    pub port: u16,
    pub monitor: Option<MonitorHandle>,
}

pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    serve_inner(config, std::future::pending::<()>()).await
}

pub async fn serve_with_shutdown(
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    serve_inner(config, shutdown).await
}

async fn serve_inner(
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = bind_proxy_listener(&config.bind_address, config.port).await?;
    serve_listener(listener, config.monitor, shutdown).await
}

pub async fn bind_proxy_listener(bind_address: &str, port: u16) -> anyhow::Result<TcpListener> {
    let ip = bind_address
        .parse::<std::net::IpAddr>()
        .map_err(|err| anyhow::anyhow!("invalid proxy bind address {bind_address:?}: {err}"))?;
    let addr = std::net::SocketAddr::new(ip, port);
    TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow::anyhow!("failed to bind proxy listener on {addr}: {err}"))
}

pub async fn serve_listener(
    listener: TcpListener,
    monitor: Option<MonitorHandle>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let local_addr = listener.local_addr()?;
    let port = local_addr.port();
    create_logger("server").info(
        "server listening",
        Some(serde_json::Map::from_iter([
            ("port".to_string(), json!(port)),
            (
                "bindAddress".to_string(),
                json!(local_addr.ip().to_string()),
            ),
            (
                "logDir".to_string(),
                json!(
                    crate::paths::log_file()
                        .parent()
                        .map(|path| path.display().to_string())
                ),
            ),
        ])),
    );
    let app = app_with_monitor(Arc::new(Registry::with_default_alias()), monitor);
    axum::serve(listener.tap_io(enable_accepted_tcp_nodelay), app)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}

/// Disable Nagle on the Anthropic listen hop so tiny SSE frames flush immediately.
fn enable_accepted_tcp_nodelay(stream: &mut tokio::net::TcpStream) {
    if let Err(err) = stream.set_nodelay(true) {
        tracing::debug!("failed to set TCP_NODELAY on accepted connection: {err}");
    }
}

pub fn app(registry: Arc<Registry>) -> Router {
    app_with_monitor(registry, None)
}

pub fn app_with_monitor(registry: Arc<Registry>, monitor: Option<MonitorHandle>) -> Router {
    let state = Arc::new(AppState { registry, monitor });
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/models", get(handler_models))
        .route("/v1/messages", post(handler_messages))
        .route("/v1/messages/count_tokens", post(handler_count_tokens))
        .fallback(fallback_handler)
        .with_state(state)
}

#[derive(Clone)]
struct AppState {
    registry: Arc<Registry>,
    monitor: Option<MonitorHandle>,
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({ "ok": true }))
}

/// Anthropic/OpenAI-compatible model list.
///
/// Prefer a union of Cursor's live `GetUsableModels` catalog (when auth is
/// available) and the registry so Codex/Kimi/Grok ids stay discoverable.
///
/// Cursor fable-family ids are rewritten through [`anthropic_list_model_id`] so
/// Claude Code sees a `[1m]` marker (1M context) on this surface.
async fn handler_models(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    use crate::providers::cursor::model::{
        CURSOR_GROK_46_ALIAS, anthropic_list_model_id, cursor_anthropic_surface_models,
    };

    let mut seen = std::collections::BTreeSet::new();
    let mut data: Vec<Value> = Vec::new();
    let discovery_allowlist = std::env::var("CCP_CLAUDE_MODEL_DISCOVERY_ALLOWLIST").ok();

    let push = |data: &mut Vec<Value>,
                seen: &mut std::collections::BTreeSet<String>,
                id: String,
                owned_by: &str| {
        if seen.insert(id.clone()) {
            let mut item = json!({
                "id": id,
                "object": "model",
                "created": 0,
                "owned_by": owned_by,
            });
            if item["id"] == CURSOR_GROK_46_ALIAS {
                item["display_name"] = json!("Cursor Grok 4.6");
            }
            data.push(item);
        }
    };

    if let Ok(Some(auth)) = crate::providers::cursor::auth::load_cursor_auth() {
        let client = crate::providers::cursor::client::CursorHttpClient::new();
        if let Ok(ids) = client.fetch_usable_models(&auth.access_token).await {
            for id in ids {
                // Fable catalog → …[1m]; other catalog ids unchanged.
                let surface = anthropic_list_model_id(&id);
                if model_allowed_by_discovery_filter(&surface, discovery_allowlist.as_deref()) {
                    push(&mut data, &mut seen, surface, "cursor");
                }
            }
        }
    }

    for (id, provider) in state.registry.all_supported_models() {
        let surface = if provider == "cursor" {
            anthropic_list_model_id(&id)
        } else {
            id
        };
        if model_allowed_by_discovery_filter(&surface, discovery_allowlist.as_deref()) {
            push(&mut data, &mut seen, surface, &provider);
        }
    }

    if data.is_empty() {
        for id in cursor_anthropic_surface_models() {
            if model_allowed_by_discovery_filter(&id, discovery_allowlist.as_deref()) {
                push(&mut data, &mut seen, id, "cursor");
            }
        }
    } else {
        // Ensure the Fable 1M wire id is always present for gateway discovery.
        let fable = anthropic_list_model_id("claude-fable-5");
        if model_allowed_by_discovery_filter(&fable, discovery_allowlist.as_deref()) {
            push(&mut data, &mut seen, fable, "cursor");
        }
    }

    Json(json!({
        "object": "list",
        "data": data,
    }))
}

fn model_allowed_by_discovery_filter(id: &str, allowlist: Option<&str>) -> bool {
    let lower = id.to_ascii_lowercase();
    if !lower.starts_with("claude") && !lower.starts_with("anthropic") {
        return true;
    }
    let Some(allowlist) = allowlist else {
        return true;
    };
    allowlist
        .split(',')
        .map(str::trim)
        .any(|allowed| !allowed.is_empty() && allowed.eq_ignore_ascii_case(id))
}

async fn handler_messages(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_request(state, req, false).await
}

async fn handler_count_tokens(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_request(state, req, true).await
}

async fn dispatch_request(
    state: Arc<AppState>,
    req: Request<Body>,
    count_tokens: bool,
) -> Response {
    let started_at = Instant::now();
    let log = create_logger("server");
    let req_id = Uuid::new_v4().to_string();
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let path = uri.path().to_string();
    let query = redacted_query(&uri);
    let endpoint = if count_tokens {
        EndpointKind::CountTokens
    } else {
        EndpointKind::Messages
    };
    log.info(
        "request",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), json!(&req_id)),
            ("method".to_string(), json!(method.as_str())),
            ("path".to_string(), json!(&path)),
            ("query".to_string(), json!(&query)),
        ])),
    );
    let header_session_id = session_id_from_headers(&headers);
    let claude_code = claude_code_headers_from(&headers);
    if claude_code.agent_id.is_some()
        || claude_code.parent_agent_id.is_some()
        || claude_code.app.is_some()
    {
        log.info(
            "claude_code_headers",
            Some(serde_json::Map::from_iter([
                ("reqId".to_string(), json!(&req_id)),
                ("app".to_string(), json!(&claude_code.app)),
                ("agentId".to_string(), json!(&claude_code.agent_id)),
                (
                    "parentAgentId".to_string(),
                    json!(&claude_code.parent_agent_id),
                ),
            ])),
        );
    }
    let request_guard = RequestMonitorGuard::new(state.monitor.clone(), req_id.clone());
    let now = current_millis();
    let body_bytes = match axum::body::to_bytes(req.into_body(), 64 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(err) => {
            start_request_monitor(
                state.monitor.as_ref(),
                &req_id,
                header_session_id.clone(),
                endpoint,
            );
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("Invalid JSON: {err}"),
            );
            log_request_completed(
                &log,
                RequestLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    status: response.status(),
                    started_at,
                },
            );
            let (response, details) = record_failed_response(
                &log,
                FailedResponseLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    started_at,
                },
                response,
            )
            .await;
            monitor_failed(
                state.monitor.as_ref(),
                &req_id,
                Some(response.status()),
                details
                    .as_ref()
                    .map(|details| details.message.as_str())
                    .unwrap_or("Invalid JSON"),
            );
            return response;
        }
    };

    let mut body: MessagesRequest = match parse_json_body(&body_bytes) {
        Ok(body) => body,
        Err(response) => {
            start_request_monitor(
                state.monitor.as_ref(),
                &req_id,
                header_session_id.clone(),
                endpoint,
            );
            let status = response.status();
            log_request_completed(
                &log,
                RequestLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    status: response.status(),
                    started_at,
                },
            );
            let (response, details) = record_failed_response(
                &log,
                FailedResponseLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    started_at,
                },
                *response,
            )
            .await;
            monitor_failed(
                state.monitor.as_ref(),
                &req_id,
                Some(status),
                details
                    .as_ref()
                    .map(|details| details.message.as_str())
                    .unwrap_or("Invalid JSON"),
            );
            return response;
        }
    };

    let resolved_session = resolve_session_id(header_session_id, &body);
    if resolved_session.fallback {
        log.info(
            "session_id_fallback",
            Some(serde_json::Map::from_iter([
                ("reqId".to_string(), json!(&req_id)),
                ("sessionId".to_string(), json!(&resolved_session.session_id)),
                (
                    "reason".to_string(),
                    json!("missing X-Claude-Code-Session-Id"),
                ),
                (
                    "hasUserId".to_string(),
                    json!(metadata_user_id(&body).is_some()),
                ),
                (
                    "hasCwd".to_string(),
                    json!(working_directory_from_body(&body).is_some()),
                ),
            ])),
        );
    }
    let session_id = Some(resolved_session.session_id);
    start_request_monitor(
        state.monitor.as_ref(),
        &req_id,
        session_id.clone(),
        endpoint,
    );

    if let Some(project) = project::name_from_request(
        body.extra.get("system"),
        body.messages.iter().rev().map(|message| &message.content),
    ) && let Some(monitor) = state.monitor.as_ref()
    {
        monitor.project_resolved(&req_id, project);
    }

    let model = match body.model.as_deref() {
        Some(model) => model,
        None => {
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Missing \"model\" in request body. {}",
                    state.registry.unknown_model_message()
                ),
            );
            log_request_completed(
                &log,
                RequestLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    status: response.status(),
                    started_at,
                },
            );
            let (response, details) = record_failed_response(
                &log,
                FailedResponseLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: None,
                    count_tokens,
                    started_at,
                },
                response,
            )
            .await;
            monitor_failed(
                state.monitor.as_ref(),
                &req_id,
                Some(response.status()),
                details
                    .as_ref()
                    .map(|details| details.message.as_str())
                    .unwrap_or("Missing model"),
            );
            return response;
        }
    };

    let normalized_model = normalize_incoming_model(model);
    body.model = Some(normalized_model.clone());
    let session_state = if let Some(session_id) = session_id.as_deref() {
        session::existing_session(Some(session_id), now)
    } else {
        None
    };

    let provider = state.registry.provider_for_model(
        &normalized_model,
        session_state
            .as_ref()
            .and_then(|state| state.affinity_provider.as_ref()),
    );

    let provider = match provider {
        Some(provider) => provider,
        None => {
            log.warn(
                "unknown model",
                Some(serde_json::Map::from_iter([
                    ("reqId".to_string(), json!(&req_id)),
                    ("model".to_string(), json!(&normalized_model)),
                ])),
            );
            let response = json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Unknown model \"{normalized_model}\". {}",
                    state.registry.unknown_model_message()
                ),
            );
            log_request_completed(
                &log,
                RequestLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: Some(&normalized_model),
                    count_tokens,
                    status: response.status(),
                    started_at,
                },
            );
            let (response, details) = record_failed_response(
                &log,
                FailedResponseLogContext {
                    req_id: &req_id,
                    provider: None,
                    model: Some(&normalized_model),
                    count_tokens,
                    started_at,
                },
                response,
            )
            .await;
            monitor_failed(
                state.monitor.as_ref(),
                &req_id,
                Some(response.status()),
                details
                    .as_ref()
                    .map(|details| details.message.as_str())
                    .unwrap_or("Unknown model"),
            );
            return response;
        }
    };

    let effort = crate::providers::translate_shared::read_effort(&body)
        .ok()
        .flatten()
        .map(str::to_string);
    let current = session::record_session_request(
        session_id.as_deref(),
        session_state.as_ref(),
        provider.name(),
        &normalized_model,
        now,
    );
    if let Some(monitor) = state.monitor.as_ref() {
        if let Some(current) = current.as_ref() {
            monitor.session_sequence_resolved(&req_id, current.seq);
        }
        monitor.provider_selected(&req_id, provider.name(), &normalized_model, effort);
    }

    let traffic = create_traffic_capture(TrafficCaptureOptions {
        req_id: req_id.clone(),
        session_id: session_id.clone(),
        session_seq: current.as_ref().map(|s| s.seq),
        provider: Some(provider.name().to_string()),
        state_dir_override: None,
    })
    .map(Arc::new);

    if let Some(capture) = traffic.as_ref() {
        if let Some(monitor) = state.monitor.as_ref() {
            monitor.traffic_capture_path(&req_id, capture.root().to_path_buf());
        }
        capture.write_json(
            "000-metadata",
            &json!({
                "reqId": &req_id,
                "sessionId": &session_id,
                "sessionSeq": current.as_ref().map(|s| s.seq),
                "kind": if count_tokens { "count_tokens" } else { "messages" },
                "provider": provider.name(),
                "model": &normalized_model,
                "method": method.as_str(),
                "path": &path,
                "query": &query,
                "headers": headers_to_record(&headers),
            }),
        );
        capture.write_json(
            "010-anthropic-request",
            &serde_json::to_value(&body).unwrap_or_else(|_| json!({})),
        );
    }

    let context = RequestContext {
        req_id: req_id.clone(),
        session_id,
        session_seq: current.map(|s| s.seq),
        provider: provider.name().to_string(),
        traffic,
        monitor: state.monitor.clone(),
        claude_code,
    };

    let response = if count_tokens {
        provider.handle_count_tokens(body, context).await
    } else {
        provider.handle_messages(body, context).await
    };
    log_request_completed(
        &log,
        RequestLogContext {
            req_id: &req_id,
            provider: Some(provider.name()),
            model: Some(&normalized_model),
            count_tokens,
            status: response.status(),
            started_at,
        },
    );
    let status = response.status();
    if status.is_success() {
        return monitor_response_body(response, request_guard);
    }

    let (response, details) = record_failed_response(
        &log,
        FailedResponseLogContext {
            req_id: &req_id,
            provider: Some(provider.name()),
            model: Some(&normalized_model),
            count_tokens,
            started_at,
        },
        response,
    )
    .await;
    if let Some(details) = details.as_ref() {
        monitor_failed(
            state.monitor.as_ref(),
            &req_id,
            Some(status),
            details.message.as_str(),
        );
    } else {
        monitor_failed(
            state.monitor.as_ref(),
            &req_id,
            Some(status),
            format!("HTTP {}", status.as_u16()),
        );
    }
    response
}

fn monitor_response_body(response: Response, guard: RequestMonitorGuard) -> Response {
    let status = response.status();
    let (parts, body) = response.into_parts();
    let stream =
        futures_util::stream::unfold((body, guard), move |(mut body, mut guard)| async move {
            match body.frame().await {
                Some(Ok(frame)) => Some((Ok(frame), (body, guard))),
                Some(Err(err)) => {
                    guard.failed(status, err.to_string());
                    Some((Err(err), (body, guard)))
                }
                None => {
                    guard.completed(status);
                    None
                }
            }
        });
    Response::from_parts(parts, Body::new(StreamBody::new(stream)))
}

struct RequestLogContext<'a> {
    req_id: &'a str,
    provider: Option<&'a str>,
    model: Option<&'a str>,
    count_tokens: bool,
    status: StatusCode,
    started_at: Instant,
}

fn log_request_completed(log: &Logger, ctx: RequestLogContext<'_>) {
    log.info(
        "request_completed",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), json!(ctx.req_id)),
            ("provider".to_string(), json!(ctx.provider)),
            ("model".to_string(), json!(ctx.model)),
            ("countTokens".to_string(), json!(ctx.count_tokens)),
            ("status".to_string(), json!(ctx.status.as_u16())),
            (
                "ms".to_string(),
                json!(ctx.started_at.elapsed().as_millis()),
            ),
        ])),
    );
}

struct FailedResponseLogContext<'a> {
    req_id: &'a str,
    provider: Option<&'a str>,
    model: Option<&'a str>,
    count_tokens: bool,
    started_at: Instant,
}

struct FailedResponseDetails {
    message: String,
}

async fn record_failed_response(
    log: &Logger,
    ctx: FailedResponseLogContext<'_>,
    response: Response,
) -> (Response, Option<FailedResponseDetails>) {
    if response.status().is_success() {
        return (response, None);
    }

    let status = response.status();
    let (parts, body) = response.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            log.info(
                "request_failed",
                Some(serde_json::Map::from_iter([
                    ("reqId".to_string(), json!(ctx.req_id)),
                    ("provider".to_string(), json!(ctx.provider)),
                    ("model".to_string(), json!(ctx.model)),
                    ("countTokens".to_string(), json!(ctx.count_tokens)),
                    ("status".to_string(), json!(status.as_u16())),
                    (
                        "ms".to_string(),
                        json!(ctx.started_at.elapsed().as_millis()),
                    ),
                    ("bodyReadError".to_string(), json!(err.to_string())),
                ])),
            );
            return (Response::from_parts(parts, Body::empty()), None);
        }
    };

    let response_body = response_body_value(&bytes);
    let message = error_message_from_response(&response_body)
        .unwrap_or_else(|| format!("HTTP {}", status.as_u16()));
    let document = json!({
        "reqId": ctx.req_id,
        "provider": ctx.provider,
        "model": ctx.model,
        "countTokens": ctx.count_tokens,
        "status": status.as_u16(),
        "elapsedMs": ctx.started_at.elapsed().as_millis(),
        "message": message,
        "response": response_body,
    });
    let error_file = write_error_capture(ctx.req_id, &redact_error_value(document));

    let mut fields = serde_json::Map::from_iter([
        ("reqId".to_string(), json!(ctx.req_id)),
        ("provider".to_string(), json!(ctx.provider)),
        ("model".to_string(), json!(ctx.model)),
        ("countTokens".to_string(), json!(ctx.count_tokens)),
        ("status".to_string(), json!(status.as_u16())),
        (
            "ms".to_string(),
            json!(ctx.started_at.elapsed().as_millis()),
        ),
        ("message".to_string(), json!(message)),
    ]);
    if let Some(path) = error_file.as_ref() {
        fields.insert("errorFile".to_string(), json!(path.display().to_string()));
    }
    log.info("request_failed", Some(fields));

    (
        Response::from_parts(parts, Body::from(bytes)),
        Some(FailedResponseDetails { message }),
    )
}

fn response_body_value(bytes: &[u8]) -> Value {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => json!({ "json": value }),
        Err(_) => json!({ "text": String::from_utf8_lossy(bytes) }),
    }
}

fn error_message_from_response(response_body: &Value) -> Option<String> {
    response_body
        .get("json")
        .and_then(|body| body.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .or_else(|| {
            response_body
                .get("text")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
        })
        .map(std::string::ToString::to_string)
}

fn write_error_capture(req_id: &str, document: &Value) -> Option<PathBuf> {
    let dir = crate::paths::state_dir().join("errors");
    fs::create_dir_all(&dir).ok()?;
    set_mode(&dir, 0o700);
    let path = dir.join(format!(
        "{}-{}.json",
        current_millis(),
        sanitize_path_part(req_id)
    ));
    let mut file = File::create(&path).ok()?;
    set_mode(&path, 0o600);
    let payload = serde_json::to_vec_pretty(document).ok()?;
    file.write_all(&payload).ok()?;
    file.write_all(b"\n").ok()?;
    Some(path)
}

fn sanitize_path_part(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn redact_error_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(redact_error_value).collect()),
        Value::Object(fields) => {
            let mut out = Map::new();
            for (key, value) in fields {
                if REDACT_KEYS.contains(&key.to_lowercase().as_str()) {
                    out.insert(key, redact_error_key(value));
                } else {
                    out.insert(key, redact_error_value(value));
                }
            }
            Value::Object(out)
        }
        value => value,
    }
}

fn redact_error_key(value: Value) -> Value {
    match value {
        Value::String(value) => Value::String(format!("[redacted len={}]", value.len())),
        _ => Value::String("[redacted]".to_string()),
    }
}

struct RequestMonitorGuard {
    monitor: Option<MonitorHandle>,
    req_id: String,
}

impl RequestMonitorGuard {
    fn new(monitor: Option<MonitorHandle>, req_id: String) -> Self {
        Self { monitor, req_id }
    }

    fn completed(&mut self, status: StatusCode) {
        if let Some(monitor) = self.monitor.take() {
            monitor.request_completed(&self.req_id, status.as_u16(), None, None);
        }
    }

    fn failed(&mut self, status: StatusCode, error: String) {
        if let Some(monitor) = self.monitor.take() {
            monitor.request_failed(&self.req_id, Some(status.as_u16()), error);
        }
    }
}

impl Drop for RequestMonitorGuard {
    fn drop(&mut self) {
        if let Some(monitor) = self.monitor.as_ref() {
            monitor.request_abandoned(&self.req_id, "Request future ended before completion");
        }
    }
}

fn monitor_failed(
    monitor: Option<&MonitorHandle>,
    req_id: &str,
    status: Option<StatusCode>,
    error: impl Into<String>,
) {
    if let Some(monitor) = monitor {
        monitor.request_failed(req_id, status.map(|status| status.as_u16()), error);
    }
}

fn start_request_monitor(
    monitor: Option<&MonitorHandle>,
    req_id: &str,
    session_id: Option<String>,
    endpoint: EndpointKind,
) {
    if let Some(monitor) = monitor {
        monitor.request_started(req_id, session_id, None, endpoint);
    }
}

/// Claude Code 2.1.193 (`cli.js`) and installed 2.1.211 (`claude.exe`) both send
/// `X-Claude-Code-Session-Id` on Anthropic requests. HTTP header names are
/// case-insensitive (`x-claude-code-session-id` matches). Keep this list so a
/// future rename can be accepted alongside the current name.
const CLAUDE_SESSION_ID_HEADERS: &[&str] = &["x-claude-code-session-id"];

struct ResolvedSessionId {
    session_id: String,
    fallback: bool,
}

fn session_id_from_headers(headers: &http::HeaderMap) -> Option<String> {
    for name in CLAUDE_SESSION_ID_HEADERS {
        if let Some(value) = headers.get(*name).and_then(|v| v.to_str().ok()) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn resolve_session_id(
    header_session_id: Option<String>,
    body: &MessagesRequest,
) -> ResolvedSessionId {
    if let Some(session_id) = header_session_id.filter(|id| !id.is_empty()) {
        return ResolvedSessionId {
            session_id,
            fallback: false,
        };
    }
    ResolvedSessionId {
        session_id: derive_fallback_session_id(body),
        fallback: true,
    }
}

fn derive_fallback_session_id(body: &MessagesRequest) -> String {
    let user_id = metadata_user_id(body).unwrap_or("");
    let cwd = working_directory_from_body(body).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(user_id.as_bytes());
    hasher.update([0]);
    hasher.update(cwd.as_bytes());
    format!("ccp-fb-{:x}", hasher.finalize())
}

fn metadata_user_id(body: &MessagesRequest) -> Option<&str> {
    let metadata = body.extra.get("metadata")?;
    metadata
        .get("user_id")
        .or_else(|| metadata.get("userId"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn working_directory_from_body(body: &MessagesRequest) -> Option<String> {
    project::cwd_from_request(
        body.extra.get("system"),
        body.messages.iter().rev().map(|message| &message.content),
    )
}

fn header_text(headers: &http::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn claude_code_headers_from(headers: &http::HeaderMap) -> ClaudeCodeAgentHeaders {
    ClaudeCodeAgentHeaders {
        agent_id: header_text(headers, "x-claude-code-agent-id"),
        parent_agent_id: header_text(headers, "x-claude-code-parent-agent-id"),
        app: header_text(headers, "x-app"),
    }
}

fn headers_to_record(headers: &http::HeaderMap) -> Value {
    let mut out = Map::new();
    for (key, value) in headers {
        if let Ok(raw) = value.to_str() {
            out.insert(key.as_str().to_string(), Value::String(raw.to_string()));
        }
    }
    Value::Object(out)
}

fn redacted_query(uri: &http::Uri) -> Value {
    let mut out = Map::new();
    let Some(query) = uri.query() else {
        return Value::Object(out);
    };
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let key = key.into_owned();
        let lower = key.to_lowercase();
        let value = if REDACT_KEYS.contains(&lower.as_str()) {
            Value::String(format!("[redacted len={}]", value.len()))
        } else {
            Value::String(value.into_owned())
        };
        out.insert(key, value);
    }
    Value::Object(out)
}

fn parse_json_body<T>(body: &[u8]) -> Result<T, Box<Response>>
where
    T: DeserializeOwned,
{
    if body.is_empty() {
        return Err(Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "Invalid JSON: empty body",
        )));
    }

    serde_json::from_slice::<T>(body).map_err(|err| {
        Box::new(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!("Invalid JSON: {err}"),
        ))
    })
}

async fn fallback_handler(method: axum::http::Method, uri: axum::http::Uri) -> Response {
    json_error(
        StatusCode::NOT_FOUND,
        "not_found",
        format!("No route for {method} {}", uri.path()),
    )
}

fn current_millis() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn set_mode(path: &Path, mode: u32) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = fs::metadata(path) {
            let mut perm = meta.permissions();
            perm.set_mode(mode);
            let _ = fs::set_permissions(path, perm);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        claude_code_headers_from, derive_fallback_session_id, enable_accepted_tcp_nodelay,
        model_allowed_by_discovery_filter, resolve_session_id, session_id_from_headers,
    };
    use crate::anthropic::schema::MessagesRequest;
    use serde_json::json;
    use tokio::net::TcpListener;

    fn body_from(value: serde_json::Value) -> MessagesRequest {
        serde_json::from_value(value).unwrap()
    }

    fn fallback_body(user_id: &str, cwd: &str, first_user: &str) -> MessagesRequest {
        body_from(json!({
            "model": "cursor",
            "messages": [{"role": "user", "content": first_user}],
            "metadata": {"user_id": user_id},
            "system": format!("# Environment\n - Primary working directory: {cwd}\n - Is a git repository: true")
        }))
    }

    #[test]
    fn discovery_allowlist_only_filters_anthropic_shaped_ids() {
        let allowlist = Some("cursor-grok-4.6");
        assert!(model_allowed_by_discovery_filter(
            "cursor-grok-4.6",
            allowlist
        ));
        assert!(!model_allowed_by_discovery_filter(
            "claude-opus-5-thinking-high",
            allowlist
        ));
        assert!(model_allowed_by_discovery_filter("gpt-5.6-sol", allowlist));
    }

    #[tokio::test]
    async fn accepted_connections_enable_tcp_nodelay() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await });
        let (mut stream, _) = listener.accept().await.unwrap();
        enable_accepted_tcp_nodelay(&mut stream);
        assert!(
            stream.nodelay().unwrap(),
            "Anthropic listen sockets must disable Nagle (TCP_NODELAY)"
        );
        client.await.unwrap().unwrap();
    }

    #[test]
    fn session_id_reads_claude_code_header_canonical_and_http2_case() {
        let mut headers = http::HeaderMap::new();
        headers.insert("X-Claude-Code-Session-Id", "sess-from-cli".parse().unwrap());
        assert_eq!(
            session_id_from_headers(&headers).as_deref(),
            Some("sess-from-cli")
        );

        let mut lower = http::HeaderMap::new();
        lower.insert("x-claude-code-session-id", "sess-lower".parse().unwrap());
        assert_eq!(
            session_id_from_headers(&lower).as_deref(),
            Some("sess-lower")
        );
    }

    #[test]
    fn session_id_header_wins_over_derived_fallback() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-claude-code-session-id",
            "header-session".parse().unwrap(),
        );
        let body = fallback_body("user-1", "/tmp/proj", "hello");
        let resolved = resolve_session_id(session_id_from_headers(&headers), &body);
        assert_eq!(resolved.session_id, "header-session");
        assert!(!resolved.fallback);
    }

    #[test]
    fn session_id_falls_back_when_header_absent() {
        let body = fallback_body("user-1", "/tmp/proj", "hello");
        let resolved = resolve_session_id(None, &body);
        assert!(
            resolved.fallback,
            "missing header must log a derived fallback"
        );
        assert!(
            resolved.session_id.starts_with("ccp-fb-"),
            "derived id should be distinguishable from Claude UUIDs: {}",
            resolved.session_id
        );
        assert_eq!(resolved.session_id.len(), "ccp-fb-".len() + 64);
    }

    #[test]
    fn session_id_fallback_is_stable_across_later_turns() {
        let first = fallback_body("user-1", "/tmp/proj", "hello");
        let continued = body_from(json!({
            "model": "cursor",
            "messages": [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "hi"},
                {"role": "user", "content": "continue"}
            ],
            "metadata": {"user_id": "user-1"},
            "system": "# Environment\n - Primary working directory: /tmp/proj\n"
        }));
        let a = derive_fallback_session_id(&first);
        let b = derive_fallback_session_id(&continued);
        assert_eq!(a, b, "later turns must keep the same live BiDi session");
    }

    #[test]
    fn session_id_fallback_changes_with_user_or_cwd_not_later_messages() {
        let base = derive_fallback_session_id(&fallback_body("user-1", "/tmp/proj", "hello"));
        let other_user = derive_fallback_session_id(&fallback_body("user-2", "/tmp/proj", "hello"));
        let other_cwd = derive_fallback_session_id(&fallback_body("user-1", "/tmp/other", "hello"));
        let other_msg = derive_fallback_session_id(&fallback_body("user-1", "/tmp/proj", "hola"));
        assert_ne!(base, other_user);
        assert_ne!(base, other_cwd);
        assert_eq!(
            base, other_msg,
            "fallback is user_id + cwd; first-message text must not mint a new session"
        );
    }

    #[test]
    fn nested_agent_headers_do_not_replace_session_id() {
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "X-Claude-Code-Session-Id",
            "parent-session".parse().unwrap(),
        );
        headers.insert("x-claude-code-agent-id", "agent%2Fchild".parse().unwrap());
        headers.insert(
            "x-claude-code-parent-agent-id",
            "agent%2Fparent".parse().unwrap(),
        );
        headers.insert("x-app", "cli-bg".parse().unwrap());
        let nested = claude_code_headers_from(&headers);
        assert_eq!(nested.agent_id.as_deref(), Some("agent%2Fchild"));
        assert_eq!(nested.parent_agent_id.as_deref(), Some("agent%2Fparent"));
        assert_eq!(nested.app.as_deref(), Some("cli-bg"));
        let resolved = resolve_session_id(
            session_id_from_headers(&headers),
            &fallback_body("u", "/tmp/p", "x"),
        );
        assert_eq!(resolved.session_id, "parent-session");
        assert!(!resolved.fallback);
    }

    #[test]
    fn session_id_fallback_never_returns_empty() {
        let empty = body_from(json!({
            "model": "cursor",
            "messages": []
        }));
        let resolved = resolve_session_id(None, &empty);
        assert!(resolved.fallback);
        assert!(!resolved.session_id.is_empty());
    }
}
