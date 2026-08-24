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
    extract::Path as PathParam,
    extract::State,
    http::{Method, Request, StatusCode},
    response::{IntoResponse, Response},
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
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

static PROCESS_STARTED_AT: LazyLock<String> = LazyLock::new(|| jiff::Timestamp::now().to_string());
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

const PORT_CONFLICT_PROBE_TIMEOUT: Duration = Duration::from_millis(200);

pub async fn bind_proxy_listener(bind_address: &str, port: u16) -> anyhow::Result<TcpListener> {
    let ip = bind_address
        .parse::<IpAddr>()
        .map_err(|err| anyhow::anyhow!("invalid proxy bind address {bind_address:?}: {err}"))?;
    let addr = SocketAddr::new(ip, port);
    if port != 0 {
        if let Some(conflict) = probe_port_already_accepts(addr) {
            anyhow::bail!(
                "failed to bind proxy listener on {addr}: port {port} is already in use on {conflict}; \
                 another process is listening. macOS can bind 127.0.0.1:{port} and 0.0.0.0:{port} at the same time, \
                 which splits client traffic — refusing to start a second serve"
            );
        }
    }
    TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow::anyhow!("failed to bind proxy listener on {addr}: {err}"))
}

fn probe_port_already_accepts(addr: SocketAddr) -> Option<SocketAddr> {
    port_conflict_probe_addrs(addr)
        .into_iter()
        .find(|probe| TcpStream::connect_timeout(probe, PORT_CONFLICT_PROBE_TIMEOUT).is_ok())
}

fn port_conflict_probe_addrs(addr: SocketAddr) -> Vec<SocketAddr> {
    let port = addr.port();
    let mut addrs = Vec::new();
    match addr.ip() {
        IpAddr::V4(ip) => {
            if !ip.is_unspecified() {
                addrs.push(addr);
            }
            addrs.push(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
        }
        IpAddr::V6(ip) => {
            if !ip.is_unspecified() {
                addrs.push(addr);
            }
            addrs.push(SocketAddr::from((Ipv6Addr::LOCALHOST, port)));
        }
    }
    addrs.sort();
    addrs.dedup();
    addrs
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
            ("version".to_string(), json!(env!("CARGO_PKG_VERSION"))),
            ("startedAt".to_string(), json!(PROCESS_STARTED_AT.as_str())),
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
        .route("/v1/responses", post(handler_responses))
        .route("/v1/images/generations", post(handler_image_generations))
        .route("/v1/images/edits", post(handler_image_edits))
        .route("/v1/videos/generations", post(handler_video_generations))
        .route("/v1/videos/{id}", get(handler_video_status))
        .fallback(fallback_handler)
        .with_state(state)
}

#[derive(Clone)]
struct AppState {
    registry: Arc<Registry>,
    monitor: Option<MonitorHandle>,
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "started_at": PROCESS_STARTED_AT.as_str(),
    }))
}

/// Anthropic/OpenAI-compatible model list.
///
/// Prefer a union of Cursor's live `GetUsableModels` catalog (when auth is
/// available) and the registry so Codex/Kimi/Grok ids stay discoverable.
///
/// Cursor fable-family ids are rewritten through [`anthropic_list_model_id`] so
/// Claude Code sees a `[1m]` marker (1M context) on this surface.
fn parse_advertised_models(raw: &str) -> Option<Vec<String>> {
    let mut seen = std::collections::BTreeSet::new();
    let models: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .filter_map(|model| {
            let model = model.to_string();
            seen.insert(model.clone()).then_some(model)
        })
        .collect();
    (!models.is_empty()).then_some(models)
}

fn configured_advertised_models() -> Option<Vec<String>> {
    std::env::var("CCP_ADVERTISED_MODELS")
        .ok()
        .and_then(|raw| parse_advertised_models(&raw))
}

fn advertised_surface_model(id: &str, provider: &str) -> String {
    if provider == "cursor" {
        crate::providers::cursor::model::anthropic_list_model_id(id)
    } else {
        id.to_string()
    }
}

async fn handler_models(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    use crate::providers::cursor::model::{
        anthropic_list_model_id, cursor_anthropic_surface_models,
    };

    // A Claude-only deployment can expose a small, verified picker surface
    // while retaining the proxy's full internal routing catalog.
    if let Some(models) = configured_advertised_models() {
        let data: Vec<Value> = models
            .into_iter()
            .map(|id| {
                let provider = state
                    .registry
                    .provider_for_model(&id, None)
                    .map(|provider| provider.name().to_string())
                    .unwrap_or_else(|| "configured".to_string());
                let surface = advertised_surface_model(&id, &provider);
                crate::openai::catalog_model(&surface, &provider)
            })
            .collect();
        return Json(json!({
            "object": "list",
            "data": data,
        }));
    }

    let mut seen = std::collections::BTreeSet::new();
    let mut data: Vec<Value> = Vec::new();

    let push = |data: &mut Vec<Value>,
                seen: &mut std::collections::BTreeSet<String>,
                id: String,
                owned_by: &str| {
        if seen.insert(id.clone()) {
            data.push(crate::openai::catalog_model(&id, owned_by));
        }
    };

    if let Ok(Some(auth)) = crate::providers::cursor::auth::load_cursor_auth() {
        let client = crate::providers::cursor::client::CursorHttpClient::new();
        if let Ok(ids) = client.fetch_usable_models(&auth.access_token).await {
            for id in ids {
                // Fable catalog → …[1m]; other catalog ids unchanged.
                push(&mut data, &mut seen, anthropic_list_model_id(&id), "cursor");
            }
        }
    }

    for (id, provider) in state.registry.all_supported_models() {
        let surface = if provider == "cursor" {
            anthropic_list_model_id(&id)
        } else {
            id
        };
        push(&mut data, &mut seen, surface, &provider);
    }

    if data.is_empty() {
        for id in cursor_anthropic_surface_models() {
            push(&mut data, &mut seen, id, "cursor");
        }
    } else {
        // Ensure the Fable 1M wire id is always present for gateway discovery.
        push(
            &mut data,
            &mut seen,
            anthropic_list_model_id("claude-fable-5"),
            "cursor",
        );
    }

    Json(json!({
        "object": "list",
        "data": data,
    }))
}

async fn handler_messages(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_request(state, req, false).await
}

async fn handler_count_tokens(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_request(state, req, true).await
}

async fn handler_responses(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    dispatch_responses(state, req).await
}

async fn handler_image_generations(req: Request<Body>) -> Response {
    dispatch_media(Method::POST, "/images/generations", req).await
}

async fn handler_image_edits(req: Request<Body>) -> Response {
    dispatch_media(Method::POST, "/images/edits", req).await
}

async fn handler_video_generations(req: Request<Body>) -> Response {
    dispatch_media(Method::POST, "/videos/generations", req).await
}

async fn handler_video_status(PathParam(id): PathParam<String>, req: Request<Body>) -> Response {
    if !crate::media::valid_video_id(&id) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "invalid video id",
        );
    }
    dispatch_media(Method::GET, &format!("/videos/{id}"), req).await
}

async fn dispatch_media(method: Method, upstream_path: &str, req: Request<Body>) -> Response {
    let headers = req.headers().clone();
    let body = match axum::body::to_bytes(req.into_body(), crate::media::MEDIA_MAX_BODY_BYTES).await
    {
        Ok(bytes) => bytes,
        Err(_) => {
            return json_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid_request_error",
                "media request exceeds the 20MiB limit",
            );
        }
    };
    crate::media::proxy_media(method, upstream_path, &headers, body).await
}

async fn dispatch_responses(state: Arc<AppState>, req: Request<Body>) -> Response {
    let started_at = Instant::now();
    let log = create_logger("server");
    let req_id = Uuid::new_v4().to_string();
    let headers = req.headers().clone();
    let body_bytes = match axum::body::to_bytes(req.into_body(), 64 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!("Invalid JSON: {err}"),
            );
        }
    };
    let value: Value = match serde_json::from_slice(&body_bytes) {
        Ok(value) => value,
        Err(_) => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "Invalid JSON",
            );
        }
    };
    let Some(model) = crate::openai::responses_model(&value) else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            format!(
                "Missing \"model\" in request body. {}",
                state.registry.unknown_model_message()
            ),
        );
    };
    let normalized_model = normalize_incoming_model(&model);
    let provider = match state.registry.provider_for_model(&normalized_model, None) {
        Some(provider) => provider,
        None => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Unknown model \"{normalized_model}\". {}",
                    state.registry.unknown_model_message()
                ),
            );
        }
    };
    log.info(
        "request",
        Some(serde_json::Map::from_iter([
            ("reqId".to_string(), json!(&req_id)),
            (
                "clientRequestId".to_string(),
                json!(header_text(&headers, "x-grok-req-id")),
            ),
            (
                "grokConversationId".to_string(),
                json!(header_text(&headers, "x-grok-conv-id")),
            ),
            ("method".to_string(), json!("POST")),
            ("path".to_string(), json!("/v1/responses")),
            ("query".to_string(), json!({})),
        ])),
    );
    let converted = crate::openai::responses_to_messages(&value);
    let resolved_session = resolve_responses_session_id(&headers, &value, converted.as_ref().ok());
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
                    "hasPromptCacheKey".to_string(),
                    json!(responses_session_id_from_body(&value).is_some()),
                ),
            ])),
        );
    }
    let session_id = Some(resolved_session.session_id);
    start_request_monitor(
        state.monitor.as_ref(),
        &req_id,
        session_id.clone(),
        EndpointKind::Messages,
    );
    if let Some(monitor) = state.monitor.as_ref() {
        monitor.provider_selected(&req_id, provider.name(), &normalized_model, None);
    }
    let traffic = create_traffic_capture(TrafficCaptureOptions {
        req_id: req_id.clone(),
        session_id: session_id.clone(),
        session_seq: None,
        provider: Some(provider.name().to_string()),
        state_dir_override: None,
    })
    .map(Arc::new);
    let client_request_id = header_text(&headers, "x-grok-req-id");
    let context = RequestContext {
        req_id: req_id.clone(),
        client_request_id: client_request_id.clone(),
        session_id,
        session_seq: None,
        provider: provider.name().to_string(),
        traffic,
        monitor: state.monitor.clone(),
        claude_code: claude_code_headers_from(&headers),
        hold_http_until_live_open: true,
    };
    let mut request_guard = RequestMonitorGuard::new(state.monitor.clone(), req_id.clone());
    let response = if provider.name() == "grok" {
        shared_grok_provider()
            .handle_responses_raw(&body_bytes, normalized_model.clone(), context, &headers)
            .await
    } else {
        let mut messages = match converted {
            Ok(messages) => messages,
            Err(error) => {
                let response = json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    error.to_string(),
                );
                request_guard.failed(response.status(), error.to_string());
                return response;
            }
        };
        messages.model = Some(normalized_model.clone());
        wrap_anthropic_as_responses(
            provider.handle_messages(messages, context).await,
            &normalized_model,
        )
        .await
    };
    log_request_completed(
        &log,
        RequestLogContext {
            req_id: &req_id,
            provider: Some(provider.name()),
            model: Some(&normalized_model),
            count_tokens: false,
            status: response.status(),
            started_at,
        },
    );
    if response.status().is_success() {
        return monitor_response_body(response, request_guard);
    }
    let status = response.status();
    let (response, details) = record_failed_response(
        &log,
        FailedResponseLogContext {
            req_id: &req_id,
            provider: Some(provider.name()),
            model: Some(&normalized_model),
            count_tokens: false,
            started_at,
        },
        response,
    )
    .await;
    request_guard.failed(
        status,
        details
            .map(|details| details.message)
            .unwrap_or_else(|| format!("HTTP {}", status.as_u16())),
    );
    response
}

fn shared_grok_provider() -> &'static crate::providers::grok::GrokProvider {
    static PROVIDER: LazyLock<crate::providers::grok::GrokProvider> =
        LazyLock::new(crate::providers::grok::GrokProvider::new);
    &PROVIDER
}

async fn wrap_anthropic_as_responses(response: Response, model: &str) -> Response {
    if !response.status().is_success() {
        return rewrite_classified_error_response(response).await;
    }
    let content_type = response
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    if !content_type.contains("text/event-stream") {
        let bytes = match axum::body::to_bytes(response.into_body(), 8 * 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(_) => {
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "upstream response exceeds the size limit",
                );
            }
        };
        let value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(_) => {
                return json_error(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "upstream response is not JSON",
                );
            }
        };
        let id = format!("resp_{}", Uuid::new_v4().simple());
        return (
            StatusCode::OK,
            Json(crate::openai::messages_json_to_responses(
                &value, &id, model,
            )),
        )
            .into_response();
    }
    let id = format!("resp_{}", Uuid::new_v4().simple());
    let model = model.to_string();
    let (parts, mut body) = response.into_parts();
    let mut translator = crate::openai::AnthropicToResponses::new(id, model);
    let mut prelude = Vec::new();
    // Hold HTTP headers so pre-output Cursor policy errors can become JSON 4xx.
    // grok-build treats streamed response.failed as HTTP 500 "our side".
    let deadline = tokio::time::Instant::now() + responses_error_peek_timeout();
    let mut body_done = false;
    loop {
        if let Some(response) = responses_json_error_from_translator(&translator) {
            return response;
        }
        if translator.should_stop_error_peek() {
            break;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, body.frame()).await {
            Ok(Some(Ok(frame))) => {
                let data = frame.into_data().ok().unwrap_or_else(bytes::Bytes::new);
                prelude.extend(translator.push(&data));
            }
            Ok(Some(Err(_))) => {
                prelude.extend(translator.fail("upstream stream failed"));
                body_done = true;
                break;
            }
            Ok(None) => {
                prelude.extend(translator.finish());
                body_done = true;
                break;
            }
            Err(_) => break,
        }
    }
    if let Some(response) = responses_json_error_from_translator(&translator) {
        return response;
    }
    let stream = futures_util::stream::unfold(
        (Some(prelude), body, translator, body_done),
        |(prelude, mut body, mut translator, done)| async move {
            if let Some(prelude) = prelude {
                return Some((
                    Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(prelude)),
                    (None, body, translator, done),
                ));
            }
            if done {
                return None;
            }
            match body.frame().await {
                Some(Ok(frame)) => {
                    let data = frame.into_data().ok().unwrap_or_else(bytes::Bytes::new);
                    let out = translator.push(&data);
                    Some((
                        Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(out)),
                        (None, body, translator, false),
                    ))
                }
                Some(Err(_)) => {
                    let out = translator.fail("upstream stream failed");
                    Some((
                        Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(out)),
                        (None, body, translator, true),
                    ))
                }
                None => {
                    let out = translator.finish();
                    Some((
                        Ok::<bytes::Bytes, std::convert::Infallible>(bytes::Bytes::from(out)),
                        (None, body, translator, true),
                    ))
                }
            }
        },
    );
    let mut response = Response::from_parts(parts, Body::from_stream(stream));
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/event-stream"),
    );
    response
}

fn responses_json_error_from_translator(
    translator: &crate::openai::AnthropicToResponses,
) -> Option<Response> {
    let status = translator.http_error_status()?;
    let message = translator
        .failure_message()
        .unwrap_or("upstream rate limited")
        .to_string();
    let kind = crate::retry::anthropic_error_kind_for_status(status, &message);
    Some(json_error(
        StatusCode::from_u16(status).unwrap_or(StatusCode::TOO_MANY_REQUESTS),
        kind,
        message,
    ))
}

fn responses_error_peek_timeout() -> Duration {
    // grok-build maps every streamed `response.failed` to HTTP 500 and hides
    // the body. Hold headers until a pre-output 4xx is classifiable, first
    // model output arrives, or this cap. Cursor live often starts after 250ms.
    Duration::from_millis(
        std::env::var("CCP_RESPONSES_ERROR_PEEK_MS")
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
            .filter(|ms| *ms > 0)
            .unwrap_or(8_000),
    )
}

async fn rewrite_classified_error_response(response: Response) -> Response {
    let status = response.status();
    let (parts, body) = response.into_parts();
    let retry_after = parts.headers.get(http::header::RETRY_AFTER).cloned();
    let bytes = match axum::body::to_bytes(body, 8 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return json_error(
                StatusCode::BAD_GATEWAY,
                "api_error",
                "upstream error body exceeds the size limit",
            );
        }
    };
    let message = error_message_from_json_bytes(&bytes).unwrap_or_default();
    let classified = crate::retry::classify_proxy_error_status(status.as_u16(), &message);
    if classified == status.as_u16() {
        let mut response = Response::from_parts(parts, Body::from(bytes));
        response.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/json"),
        );
        return response;
    }
    let kind = crate::retry::anthropic_error_kind_for_status(classified, &message);
    let shown = if message.is_empty() {
        String::from_utf8_lossy(&bytes).into_owned()
    } else {
        message
    };
    let mut response = json_error(
        StatusCode::from_u16(classified).unwrap_or(status),
        kind,
        shown,
    );
    if let Some(value) = retry_after {
        response
            .headers_mut()
            .insert(http::header::RETRY_AFTER, value);
    }
    response
}

fn error_message_from_json_bytes(bytes: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    value
        .pointer("/error/message")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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
            (
                "clientRequestId".to_string(),
                json!(header_text(&headers, "x-grok-req-id")),
            ),
            (
                "grokConversationId".to_string(),
                json!(header_text(&headers, "x-grok-conv-id")),
            ),
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

    let client_request_id = header_text(&headers, "x-grok-req-id");
    let context = RequestContext {
        req_id: req_id.clone(),
        client_request_id: client_request_id.clone(),
        session_id,
        session_seq: current.map(|s| s.seq),
        provider: provider.name().to_string(),
        traffic,
        monitor: state.monitor.clone(),
        claude_code,
        hold_http_until_live_open: client_request_id.is_some(),
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
    let error_file = if should_capture_failed_response(status, &message) {
        let req_id = ctx.req_id.to_string();
        let document = redact_error_value(document);
        tokio::task::spawn_blocking(move || write_error_capture(&req_id, &document))
            .await
            .ok()
            .flatten()
    } else {
        None
    };

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

fn should_capture_failed_response(status: StatusCode, message: &str) -> bool {
    if status != StatusCode::SERVICE_UNAVAILABLE {
        return true;
    }
    let lower = message.to_ascii_lowercase();
    !lower.contains("cursor live generation concurrency saturated")
        && !lower.contains("cursor live open concurrency saturated")
        && !lower.contains("cursor live generation admission queue timed out")
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
    let max_files = std::env::var("CCP_ERROR_CAPTURE_MAX_FILES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(1_000);
    if max_files == 0 {
        return None;
    }
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
    prune_error_captures(&dir, max_files);
    Some(path)
}

fn prune_error_captures(dir: &Path, max_files: usize) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    if paths.len() <= max_files {
        return;
    }
    paths.sort_unstable();
    let remove_count = paths.len().saturating_sub(max_files);
    for path in paths.into_iter().take(remove_count) {
        let _ = fs::remove_file(path);
    }
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
            monitor.request_abandoned(
                &self.req_id,
                "Client response stream disconnected before completion",
            );
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

/// Claude Code sends `X-Claude-Code-Session-Id`. grok-build's Messages client
/// sends `x-grok-session-id` / `x-grok-conv-id` instead. Without those, every
/// grok chat hashed to the same fallback live slot and 409'd each other.
const CLAUDE_SESSION_ID_HEADERS: &[&str] = &[
    "x-claude-code-session-id",
    "x-grok-conv-id",
    "x-grok-session-id",
];

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

/// grok-build's Responses client puts `x_grok_conv_id` on `prompt_cache_key`
/// even when the `x-grok-*` headers are empty. Without a session, Cursor
/// skips live BiDi (`no_session`), XML/tool steal never runs, and the model
/// narrates the same "restore native tools / fan-out" turn until grok retries.
fn responses_session_id_from_body(body: &Value) -> Option<String> {
    body.get("prompt_cache_key")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn resolve_responses_session_id(
    headers: &http::HeaderMap,
    body: &Value,
    messages: Option<&MessagesRequest>,
) -> ResolvedSessionId {
    if let Some(session_id) =
        session_id_from_headers(headers).or_else(|| responses_session_id_from_body(body))
    {
        return ResolvedSessionId {
            session_id,
            fallback: false,
        };
    }
    ResolvedSessionId {
        session_id: derive_fallback_session_id_from_responses(body, messages),
        fallback: true,
    }
}

fn derive_fallback_session_id(body: &MessagesRequest) -> String {
    hash_fallback_session(
        metadata_user_id(body).unwrap_or(""),
        &working_directory_from_body(body).unwrap_or_default(),
        &first_user_message_fingerprint(body),
    )
}

fn derive_fallback_session_id_from_responses(
    body: &Value,
    messages: Option<&MessagesRequest>,
) -> String {
    let user_id = responses_metadata_user_id(body)
        .or_else(|| messages.and_then(metadata_user_id))
        .unwrap_or("");
    let cwd = messages
        .and_then(working_directory_from_body)
        .or_else(|| crate::project::cwd_from_system(body.get("instructions")))
        .unwrap_or_default();
    let first_user = messages
        .map(first_user_message_fingerprint)
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| first_user_text_from_responses_input(body));
    hash_fallback_session(user_id, &cwd, &first_user)
}

fn hash_fallback_session(user_id: &str, cwd: &str, first_user: &str) -> String {
    // Claude Desktop gateway health probes omit the session header, metadata,
    // working directory, and user content. A deterministic hash for that fully
    // empty identity makes every probe share one Cursor live slot; a cancelled
    // probe can then poison later checks with "live run is already active".
    // Preserve stable hashing for real headerless conversations, but isolate
    // context-free probes per request.
    let is_desktop_probe = first_user.is_empty() || first_user == ".";
    if user_id.is_empty() && cwd.is_empty() && is_desktop_probe {
        return format!("ccp-probe-{}", Uuid::new_v4().simple());
    }
    let mut hasher = Sha256::new();
    hasher.update(user_id.as_bytes());
    hasher.update([0]);
    hasher.update(cwd.as_bytes());
    hasher.update([0]);
    hasher.update(first_user.as_bytes());
    format!("ccp-fb-{:x}", hasher.finalize())
}

fn first_user_message_fingerprint(body: &MessagesRequest) -> String {
    body.messages
        .iter()
        .filter(|message| message.role.eq_ignore_ascii_case("user"))
        .map(|message| fingerprint_message_content(&message.content))
        .find(|text| !text.is_empty())
        .unwrap_or_default()
}

fn fingerprint_message_content(content: &Value) -> String {
    match content {
        serde_json::Value::String(text) => text.trim().to_string(),
        serde_json::Value::Array(parts) => {
            if parts.iter().all(part_is_tool_result) {
                return String::new();
            }
            parts
                .iter()
                .filter_map(|part| {
                    part.get("text")
                        .and_then(serde_json::Value::as_str)
                        .or_else(|| part.as_str())
                })
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string()
        }
        other => other.to_string(),
    }
}

fn part_is_tool_result(part: &Value) -> bool {
    part.get("type").and_then(Value::as_str) == Some("tool_result")
}

fn first_user_text_from_responses_input(body: &Value) -> String {
    let Some(input) = body.get("input") else {
        return String::new();
    };
    if let Some(text) = input.as_str() {
        return text.trim().to_string();
    }
    let Some(items) = input.as_array() else {
        return String::new();
    };
    for item in items {
        let kind = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message");
        if kind != "message" {
            continue;
        }
        let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
        if !role.eq_ignore_ascii_case("user") {
            continue;
        }
        let text = match item.get("content") {
            Some(Value::String(text)) => text.trim().to_string(),
            Some(Value::Array(parts)) => parts
                .iter()
                .filter_map(|part| {
                    part.get("text")
                        .and_then(Value::as_str)
                        .or_else(|| part.as_str())
                })
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string(),
            _ => String::new(),
        };
        if !text.is_empty() {
            return text;
        }
    }
    String::new()
}

fn metadata_user_id(body: &MessagesRequest) -> Option<&str> {
    json_metadata_user_id(body.extra.get("metadata")?)
}

fn responses_metadata_user_id(body: &Value) -> Option<&str> {
    json_metadata_user_id(body.get("metadata")?)
}

fn json_metadata_user_id(metadata: &Value) -> Option<&str> {
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
        advertised_surface_model, claude_code_headers_from, derive_fallback_session_id,
        enable_accepted_tcp_nodelay, parse_advertised_models, resolve_responses_session_id,
        resolve_session_id, session_id_from_headers, wrap_anthropic_as_responses,
    };
    use crate::anthropic::error::json_error;
    use crate::anthropic::schema::MessagesRequest;
    use axum::body::Body;
    use axum::http::{Response, StatusCode};
    use serde_json::json;
    use std::sync::Mutex;
    use std::time::{Duration, Instant};
    use tokio::net::TcpListener;

    static ERROR_CAPTURE_TEST_LOCK: Mutex<()> = Mutex::new(());

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
    fn local_admission_overload_does_not_create_error_capture_files() {
        let _guard = ERROR_CAPTURE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let previous_state = std::env::var_os("XDG_STATE_HOME");
        unsafe {
            std::env::set_var("XDG_STATE_HOME", dir.path());
        }

        let response = Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from(
                r#"{"type":"error","error":{"type":"api_error","message":"Cursor live generation admission queue timed out"}}"#,
            ))
            .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let _ = runtime.block_on(super::record_failed_response(
            &crate::logging::create_logger("test"),
            super::FailedResponseLogContext {
                req_id: "local-overload",
                provider: Some("cursor"),
                model: Some("fable"),
                count_tokens: false,
                started_at: Instant::now(),
            },
            response,
        ));

        let error_dir = dir.path().join(crate::paths::APP_DIR).join("errors");
        assert!(
            !error_dir.exists() || std::fs::read_dir(&error_dir).unwrap().next().is_none(),
            "expected local overload to be logged without permanent per-request files"
        );
        unsafe {
            match previous_state {
                Some(value) => std::env::set_var("XDG_STATE_HOME", value),
                None => std::env::remove_var("XDG_STATE_HOME"),
            }
        }
    }

    #[test]
    fn error_capture_retention_is_bounded() {
        let _guard = ERROR_CAPTURE_TEST_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let previous_state = std::env::var_os("XDG_STATE_HOME");
        let previous_max = std::env::var_os("CCP_ERROR_CAPTURE_MAX_FILES");
        unsafe {
            std::env::set_var("XDG_STATE_HOME", dir.path());
            std::env::set_var("CCP_ERROR_CAPTURE_MAX_FILES", "2");
        }

        for req_id in ["capture-a", "capture-b", "capture-c"] {
            super::write_error_capture(req_id, &json!({"error": req_id}))
                .expect("write error capture");
        }
        let error_dir = dir.path().join(crate::paths::APP_DIR).join("errors");
        let retained = std::fs::read_dir(error_dir).unwrap().count();
        assert!(retained <= 2, "retained {retained} capture files");

        unsafe {
            match previous_state {
                Some(value) => std::env::set_var("XDG_STATE_HOME", value),
                None => std::env::remove_var("XDG_STATE_HOME"),
            }
            match previous_max {
                Some(value) => std::env::set_var("CCP_ERROR_CAPTURE_MAX_FILES", value),
                None => std::env::remove_var("CCP_ERROR_CAPTURE_MAX_FILES"),
            }
        }
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
    fn session_id_reads_grok_build_headers_before_fallback() {
        let mut session = http::HeaderMap::new();
        session.insert("x-grok-session-id", "grok-sess-1".parse().unwrap());
        assert_eq!(
            session_id_from_headers(&session).as_deref(),
            Some("grok-sess-1")
        );

        let mut conv = http::HeaderMap::new();
        conv.insert("x-grok-conv-id", "grok-conv-9".parse().unwrap());
        assert_eq!(
            session_id_from_headers(&conv).as_deref(),
            Some("grok-conv-9")
        );

        let mut both = http::HeaderMap::new();
        both.insert("x-grok-session-id", "grok-sess-1".parse().unwrap());
        both.insert("x-grok-conv-id", "grok-conv-9".parse().unwrap());
        assert_eq!(
            session_id_from_headers(&both).as_deref(),
            Some("grok-conv-9"),
            "the sampling conversation, not its owning session, is the live-run boundary"
        );

        let mut claude_wins = http::HeaderMap::new();
        claude_wins.insert("x-claude-code-session-id", "claude-sess".parse().unwrap());
        claude_wins.insert("x-grok-session-id", "grok-sess-1".parse().unwrap());
        claude_wins.insert("x-grok-conv-id", "grok-conv-9".parse().unwrap());
        assert_eq!(
            session_id_from_headers(&claude_wins).as_deref(),
            Some("claude-sess")
        );

        let body = fallback_body("user-1", "/tmp/proj", "hello");
        let resolved = resolve_session_id(session_id_from_headers(&session), &body);
        assert_eq!(resolved.session_id, "grok-sess-1");
        assert!(!resolved.fallback);
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
    fn session_id_fallback_changes_with_user_or_cwd_or_first_message() {
        let base = derive_fallback_session_id(&fallback_body("user-1", "/tmp/proj", "hello"));
        let other_user = derive_fallback_session_id(&fallback_body("user-2", "/tmp/proj", "hello"));
        let other_cwd = derive_fallback_session_id(&fallback_body("user-1", "/tmp/other", "hello"));
        let other_msg = derive_fallback_session_id(&fallback_body("user-1", "/tmp/proj", "hola"));
        assert_ne!(base, other_user);
        assert_ne!(base, other_cwd);
        assert_ne!(
            base, other_msg,
            "headerless concurrent grok agents in one cwd must not share a live slot"
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
        assert!(resolved.session_id.starts_with("ccp-probe-"));

        let next = resolve_session_id(None, &empty);
        assert_ne!(
            resolved.session_id, next.session_id,
            "context-free desktop probes must not share a Cursor live slot"
        );

        let desktop_probe = body_from(json!({
            "model": "haiku",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "."}]
        }));
        let first_probe = resolve_session_id(None, &desktop_probe);
        let second_probe = resolve_session_id(None, &desktop_probe);
        assert!(first_probe.session_id.starts_with("ccp-probe-"));
        assert_ne!(
            first_probe.session_id, second_probe.session_id,
            "Claude Desktop's literal dot health probes must be isolated"
        );
    }

    #[test]
    fn configured_advertised_models_are_trimmed_and_deduplicated() {
        assert_eq!(
            parse_advertised_models(
                " claude-opus-4-7,claude-opus-4-8,claude-opus-4-7, claude-opus-5 ",
            ),
            Some(vec![
                "claude-opus-4-7".to_string(),
                "claude-opus-4-8".to_string(),
                "claude-opus-5".to_string(),
            ])
        );
        assert_eq!(parse_advertised_models(" , , "), None);
        assert_eq!(
            advertised_surface_model("fable", "cursor"),
            "claude-fable-5[1m]"
        );
        assert_eq!(
            advertised_surface_model("gpt-5.6-sol", "codex"),
            "gpt-5.6-sol"
        );
    }

    #[test]
    fn responses_session_uses_grok_header_when_present() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-grok-session-id", "grok-sess-live".parse().unwrap());
        let body = json!({
            "model": "cursor-grok-4.5-high-fast",
            "input": "hello",
            "prompt_cache_key": "conv-should-lose"
        });
        let messages = crate::openai::responses_to_messages(&body).unwrap();
        let resolved = resolve_responses_session_id(&headers, &body, Some(&messages));
        assert_eq!(resolved.session_id, "grok-sess-live");
        assert!(!resolved.fallback);
    }

    #[test]
    fn responses_session_uses_prompt_cache_key_when_headers_missing() {
        let headers = http::HeaderMap::new();
        let body = json!({
            "model": "cursor-grok-4.5-high-fast",
            "input": "hello",
            "prompt_cache_key": "conv-from-body"
        });
        let messages = crate::openai::responses_to_messages(&body).unwrap();
        let resolved = resolve_responses_session_id(&headers, &body, Some(&messages));
        assert_eq!(
            resolved.session_id, "conv-from-body",
            "/v1/responses must keep live BiDi when grok-build only puts conv id in prompt_cache_key"
        );
        assert!(!resolved.fallback);
    }

    #[test]
    fn responses_fallback_session_splits_on_raw_input_when_conversion_missing() {
        let headers = http::HeaderMap::new();
        let hello = json!({
            "model": "cursor-grok-4.5-high-fast",
            "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}]
        });
        let hola = json!({
            "model": "cursor-grok-4.5-high-fast",
            "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hola"}]}]
        });
        let a = resolve_responses_session_id(&headers, &hello, None);
        let b = resolve_responses_session_id(&headers, &hola, None);
        assert!(a.fallback && b.fallback);
        assert_ne!(
            a.session_id, b.session_id,
            "headerless grok /v1/responses must not share ccp-fb-6e34 when conversion is None"
        );
        let hello_again = resolve_responses_session_id(&headers, &hello, None);
        assert_eq!(
            a.session_id, hello_again.session_id,
            "later turns with the same first user text must keep the live slot"
        );
    }

    #[test]
    fn responses_fallback_session_stable_for_full_history_tool_follow_up() {
        let headers = http::HeaderMap::new();
        let first = json!({
            "model": "cursor-grok-4.5-high-fast",
            "instructions": "# Environment\n - Primary working directory: /tmp/carve\n",
            "input": [
                {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}
            ]
        });
        let follow = json!({
            "model": "cursor-grok-4.5-high-fast",
            "instructions": "# Environment\n - Primary working directory: /tmp/carve\n",
            "input": [
                {"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]},
                {"type":"function_call","call_id":"c1","name":"todo_write","arguments":"{}"},
                {"type":"function_call_output","call_id":"c1","output":"ok"}
            ]
        });
        let first_messages = crate::openai::responses_to_messages(&first).unwrap();
        let follow_messages = crate::openai::responses_to_messages(&follow).unwrap();
        let a = resolve_responses_session_id(&headers, &first, Some(&first_messages));
        let b = resolve_responses_session_id(&headers, &follow, Some(&follow_messages));
        assert_eq!(
            a.session_id, b.session_id,
            "grok-build replays full history; a tool follow-up must keep the live slot"
        );
    }

    #[test]
    fn responses_fallback_reads_metadata_user_id_from_raw_body() {
        let headers = http::HeaderMap::new();
        let user_a = json!({
            "model": "cursor-grok-4.5-high-fast",
            "metadata": {"user_id": "user-a"},
            "instructions": "# Environment\n - Primary working directory: /tmp/carve\n",
            "input": "hello"
        });
        let user_b = json!({
            "model": "cursor-grok-4.5-high-fast",
            "metadata": {"user_id": "user-b"},
            "instructions": "# Environment\n - Primary working directory: /tmp/carve\n",
            "input": "hello"
        });
        let messages_a = crate::openai::responses_to_messages(&user_a).unwrap();
        let messages_b = crate::openai::responses_to_messages(&user_b).unwrap();
        let a = resolve_responses_session_id(&headers, &user_a, Some(&messages_a));
        let b = resolve_responses_session_id(&headers, &user_b, Some(&messages_b));
        assert!(a.fallback && b.fallback);
        assert_ne!(
            a.session_id, b.session_id,
            "headerless users with the same cwd and prompt must not share a live slot"
        );
    }

    #[test]
    fn grok_machine_agent_id_is_not_a_nested_agent_identity() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-grok-agent-id", "agent/child-9".parse().unwrap());
        let mapped = claude_code_headers_from(&headers);
        assert!(
            mapped.agent_id.is_none(),
            "x-grok-agent-id is install-wide telemetry metadata; x-grok-conv-id isolates runs"
        );
    }

    #[test]
    fn responses_session_always_assigns_id_when_headers_and_cache_key_missing() {
        let headers = http::HeaderMap::new();
        let body = json!({
            "model": "cursor-grok-4.5-high-fast",
            "instructions": "# Environment\n - Primary working directory: /tmp/carve\n",
            "input": "hello"
        });
        let messages = crate::openai::responses_to_messages(&body).unwrap();
        let resolved = resolve_responses_session_id(&headers, &body, Some(&messages));
        assert!(
            resolved.fallback,
            "missing grok session headers must log a derived fallback, not skip live"
        );
        assert!(resolved.session_id.starts_with("ccp-fb-"));
        assert!(!resolved.session_id.is_empty());
        let again = resolve_responses_session_id(&headers, &body, Some(&messages));
        assert_eq!(
            resolved.session_id, again.session_id,
            "later /v1/responses turns must reuse the same live slot"
        );
    }

    #[tokio::test]
    async fn wrap_anthropic_json_502_unpaid_invoice_becomes_429() {
        let response = json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — pay your invoice. [resource_exhausted]",
        );
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(wrapped.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = axum::body::to_bytes(wrapped.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "rate_limit_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("unpaid invoice"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn wrap_anthropic_rate_limit_sse_becomes_http_429() {
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"cursor-grok-4.5-high-fast\"}}\n\n",
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — pay your invoice. [resource_exhausted]\"}}\n\n",
        );
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(sse))
            .unwrap();
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(
            wrapped.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "grok-build treats streamed server_error as HTTP 500 'our side'"
        );
        let bytes = axum::body::to_bytes(wrapped.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "rate_limit_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("unpaid invoice"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn wrap_anthropic_json_502_geo_block_becomes_403() {
        let response = json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            "Connect error 502: ERROR_OPENAI: This model is not available in your country or region [internal]",
        );
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(wrapped.status(), StatusCode::FORBIDDEN);
        let bytes = axum::body::to_bytes(wrapped.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "permission_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("country or region"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn wrap_anthropic_json_502_unsupported_region_zh_becomes_403() {
        let response = json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            "Connect error 502: 不支持的国家/区域 [internal]",
        );
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(wrapped.status(), StatusCode::FORBIDDEN);
        let bytes = axum::body::to_bytes(wrapped.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "permission_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("不支持的国家/区域"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn wrap_anthropic_geo_sse_becomes_http_403() {
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"cursor-grok-4.5-high-fast\"}}\n\n",
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Connect error 502: This model is not available in your country or region [internal]\"}}\n\n",
        );
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(sse))
            .unwrap();
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(wrapped.status(), StatusCode::FORBIDDEN);
        let bytes = axum::body::to_bytes(wrapped.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "permission_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("country or region"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn wrap_anthropic_delayed_geo_sse_becomes_http_403() {
        let first = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"cursor-grok-4.5-high-fast\"}}\n\n";
        let second = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Connect error 502: This model is not available in your country or region [internal]\"}}\n\n";
        let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(2);
        tokio::spawn(async move {
            let _ = tx.send(bytes::Bytes::from(first)).await;
            tokio::time::sleep(Duration::from_millis(400)).await;
            let _ = tx.send(bytes::Bytes::from(second)).await;
        });
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv()
                .await
                .map(|chunk| (Ok::<_, std::convert::Infallible>(chunk), rx))
        });
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream))
            .unwrap();
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(
            wrapped.status(),
            StatusCode::FORBIDDEN,
            "pre-output geo errors after message_start must still become HTTP 403, not streamed 200"
        );
        let bytes = axum::body::to_bytes(wrapped.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "permission_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("country or region"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn rewrite_preserves_retry_after_when_status_stays_429() {
        let response = Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::RETRY_AFTER, "7")
            .body(Body::from(
                r#"{"type":"error","error":{"type":"rate_limit_error","message":"You have an unpaid invoice"}}"#,
            ))
            .unwrap();
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(wrapped.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            wrapped
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("7"),
            "grok-build honors Retry-After on 429"
        );
    }

    #[tokio::test]
    async fn rewrite_preserves_retry_after_when_502_invoice_becomes_429() {
        let response = Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .header(http::header::CONTENT_TYPE, "application/json")
            .header(http::header::RETRY_AFTER, "5")
            .body(Body::from(
                r#"{"type":"error","error":{"type":"api_error","message":"Connect error 429: You have an unpaid invoice"}}"#,
            ))
            .unwrap();
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(wrapped.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            wrapped
                .headers()
                .get(http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("5")
        );
    }

    #[tokio::test]
    async fn wrap_anthropic_live_open_timeout_sse_becomes_http_409() {
        let sse = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"cursor-grok-4.5-high-fast\"}}\n\n",
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Cursor error 504: Cursor live open timed out after 20s\"}}\n\n",
        );
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from(sse))
            .unwrap();
        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.5-high-fast").await;
        assert_eq!(
            wrapped.status(),
            StatusCode::CONFLICT,
            "ambiguous live open must be JSON 409 before SSE 200, not grok 500"
        );
        let bytes = axum::body::to_bytes(wrapped.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "invalid_request_error");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains("timed out after 20s"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn wrap_anthropic_late_ambiguous_completion_is_non_retryable_after_http_200() {
        let first = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"model\":\"cursor-grok-4.6-xhigh-fast\"}}\n\n",
            "event: ping\n",
            "data: {\"type\":\"ping\"}\n\n",
        );
        let second = concat!(
            "event: error\n",
            "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"Cursor stream produced no useful progress; upstream transport remained live, so completion is ambiguous\"}}\n\n",
        );
        let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(2);
        tokio::spawn(async move {
            let _ = tx.send(bytes::Bytes::from(first)).await;
            tokio::time::sleep(Duration::from_millis(25)).await;
            let _ = tx.send(bytes::Bytes::from(second)).await;
        });
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv()
                .await
                .map(|chunk| (Ok::<_, std::convert::Infallible>(chunk), rx))
        });
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/event-stream")
            .body(Body::from_stream(stream))
            .unwrap();

        let wrapped = wrap_anthropic_as_responses(response, "cursor-grok-4.6-xhigh-fast").await;
        assert_eq!(
            wrapped.status(),
            StatusCode::OK,
            "HTTP status is immutable once keepalive streaming has begun"
        );
        let bytes = axum::body::to_bytes(wrapped.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("\"type\":\"response.failed\""), "{text}");
        assert!(text.contains("\"code\":\"invalid_request\""), "{text}");
        assert!(
            !text.contains("\"code\":\"server_error\""),
            "late ambiguity must not enter grok-build's retryable HTTP-500 path: {text}"
        );
    }
}
