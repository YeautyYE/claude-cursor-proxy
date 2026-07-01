pub mod auth;
pub mod client;
pub mod continuation;
pub mod count_tokens;
pub mod request_summary;
pub mod translate;
pub mod websocket;

use async_trait::async_trait;
use axum::Json;
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http::StatusCode;
use std::sync::Arc;

use crate::anthropic::error::json_error;
use crate::anthropic::schema::{CountTokensResponse, MessagesRequest};
use crate::config;
use crate::monitor::usage_from_anthropic_sse;
use crate::provider::{CliHandlers, Provider, RequestContext};
use crate::registry;

use self::auth::browser_login::run_browser_login;
use self::auth::device::DeviceAuthClient;
use self::auth::manager::CodexAuthManager;
use self::auth::token_store::file_store;
use self::client::CodexHttpClient;
use self::continuation::{clear_continuation, continuation_candidate, record_continuation};
use self::count_tokens::count_translated_tokens;
use self::translate::accumulate::accumulate_response_with_traffic;
use self::translate::live_stream::LiveStreamTranslator;
use self::translate::model_allowlist::{assert_allowed_model, resolve_model_request};
use self::translate::reducer::finish_metadata_from_upstream;
use self::translate::request::{TranslateOptions, translate_request};
use self::translate::stream::translate_stream_bytes_with_traffic;

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

pub struct CodexProvider;

impl Default for CodexProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexProvider {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Provider for CodexProvider {
    fn name(&self) -> &'static str {
        "codex"
    }

    fn supported_models(&self) -> Vec<String> {
        let mut models: Vec<String> = registry::CODEX_MODELS
            .iter()
            .map(|m| m.to_string())
            .collect();
        for m in registry::CODEX_MODELS {
            models.push(format!("{m}-fast"));
        }
        models.sort_unstable();
        models.dedup();
        models
    }

    fn cli(&self) -> &'static dyn CliHandlers {
        &CODEX_CLI
    }

    async fn handle_messages(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let message_id = format!("msg_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
        let want_stream = body.stream;
        let model = body.model.as_deref().unwrap_or("gpt-5.5");

        let resolved = resolve_model_request(model);
        if let Err(e) = assert_allowed_model(&resolved.model) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Model \"{model}\" resolves to unsupported model \"{}\"",
                    e.model
                ),
            );
        }

        let translated = match translate_request(
            &body,
            TranslateOptions {
                session_id: ctx.session_id.clone(),
                service_tier: resolved.service_tier.clone(),
                model: resolved.model.clone(),
            },
        ) {
            Ok(t) => t,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    e.to_string(),
                );
            }
        };

        // Check continuation
        let previous_response_id_enabled = config::codex_previous_response_id();
        let continuation = continuation_candidate(
            ctx.session_id.as_deref(),
            &translated,
            previous_response_id_enabled,
        );

        // Post to upstream with continuation
        let client = Arc::new(CodexHttpClient::new());
        if let Some(monitor) = ctx.monitor.as_ref() {
            monitor.upstream_started(&ctx.req_id);
        }
        if want_stream && matches!(config::codex_transport(), config::CodexTransport::WebSocket) {
            let upstream_events = match client
                .stream_codex_websocket_events(&translated, &ctx, None)
                .await
            {
                Ok(events) => events,
                Err(e) => {
                    clear_continuation(ctx.session_id.as_deref());
                    return map_codex_error_to_response(&e);
                }
            };
            return live_stream_response(upstream_events, message_id, model, ctx).await;
        }

        let upstream = match client
            .post_codex(&translated, &ctx, Some(&continuation))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                clear_continuation(ctx.session_id.as_deref());
                return map_codex_error_to_response(&e);
            }
        };

        if want_stream {
            let sse_bytes = match translate_stream_bytes_with_traffic(
                &upstream.body,
                &message_id,
                model,
                ctx.traffic.as_deref(),
            ) {
                Ok(b) => b,
                Err(e) => {
                    clear_continuation(ctx.session_id.as_deref());
                    return json_error(
                        StatusCode::BAD_GATEWAY,
                        "api_error",
                        format!("Stream translation error: {e}"),
                    );
                }
            };
            if let Some(monitor) = ctx.monitor.as_ref() {
                let (input_tokens, output_tokens) = usage_from_anthropic_sse(&sse_bytes);
                monitor.stream_progress(
                    &ctx.req_id,
                    sse_bytes.len() as u64,
                    count_sse_events(&sse_bytes),
                    input_tokens,
                    output_tokens,
                );
            }
            update_continuation_from_upstream(
                ctx.session_id.as_deref(),
                &translated,
                &upstream.body,
            );

            let headers = [
                (http::header::CONTENT_TYPE, "text/event-stream"),
                (http::header::CACHE_CONTROL, "no-cache"),
                (http::header::CONNECTION, "keep-alive"),
            ];
            (headers, sse_bytes).into_response()
        } else {
            match accumulate_response_with_traffic(
                &upstream.body,
                &message_id,
                model,
                ctx.traffic.as_deref(),
            ) {
                Ok(json) => {
                    if let Some(monitor) = ctx.monitor.as_ref() {
                        monitor.usage_updated(
                            &ctx.req_id,
                            json.pointer("/usage/input_tokens").and_then(|v| v.as_u64()),
                            json.pointer("/usage/output_tokens")
                                .and_then(|v| v.as_u64()),
                        );
                    }
                    update_continuation_from_upstream(
                        ctx.session_id.as_deref(),
                        &translated,
                        &upstream.body,
                    );
                    (StatusCode::OK, Json(json)).into_response()
                }
                Err(e) => {
                    clear_continuation(ctx.session_id.as_deref());
                    json_error(
                        StatusCode::BAD_GATEWAY,
                        "api_error",
                        format!("Accumulation error: {e}"),
                    )
                }
            }
        }
    }

    async fn handle_count_tokens(&self, body: MessagesRequest, ctx: RequestContext) -> Response {
        let model = body.model.as_deref().unwrap_or("gpt-5.5");
        let resolved = resolve_model_request(model);
        if let Err(e) = assert_allowed_model(&resolved.model) {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                format!(
                    "Model \"{model}\" resolves to unsupported model \"{}\"",
                    e.model
                ),
            );
        }

        let translated = match translate_request(
            &body,
            TranslateOptions {
                session_id: None,
                service_tier: resolved.service_tier.clone(),
                model: resolved.model.clone(),
            },
        ) {
            Ok(t) => t,
            Err(e) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_request_error",
                    e.to_string(),
                );
            }
        };

        let tokens = count_translated_tokens(&translated);
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

async fn live_stream_response(
    mut upstream_events: websocket::CodexWebSocketEventReceiver,
    message_id: String,
    model: &str,
    ctx: RequestContext,
) -> Response {
    let mut translator = LiveStreamTranslator::new(message_id, model.to_string());

    while let Some(item) = upstream_events.recv().await {
        let payload = match item {
            Ok(payload) => payload,
            Err(err) => {
                clear_continuation(ctx.session_id.as_deref());
                return map_codex_error_to_response(&err);
            }
        };
        let (chunk, terminal) = match translate_live_stream_payload(&mut translator, &payload, &ctx)
        {
            Ok(result) => result,
            Err(message) => {
                clear_continuation(ctx.session_id.as_deref());
                return json_error(StatusCode::BAD_GATEWAY, "api_error", message);
            }
        };
        if !chunk.is_empty() {
            record_live_stream_progress(&ctx, &chunk);
            if terminal {
                clear_continuation(ctx.session_id.as_deref());
                return single_live_stream_response(chunk);
            }
            return remaining_live_stream_response(upstream_events, translator, chunk, ctx);
        }
        if terminal {
            clear_continuation(ctx.session_id.as_deref());
            return empty_live_stream_response();
        }
    }

    clear_continuation(ctx.session_id.as_deref());
    json_error(
        StatusCode::BAD_GATEWAY,
        "api_error",
        "WebSocket connection closed before terminal Codex response event",
    )
}

fn translate_live_stream_payload(
    translator: &mut LiveStreamTranslator,
    payload: &serde_json::Value,
    ctx: &RequestContext,
) -> Result<(Vec<u8>, bool), String> {
    let terminal = is_codex_terminal_event(payload);
    let chunk = translator.accept(payload, ctx.traffic.as_deref())?;
    Ok((chunk, terminal))
}

fn record_live_stream_progress(ctx: &RequestContext, chunk: &[u8]) {
    if let Some(monitor) = ctx.monitor.as_ref() {
        monitor.stream_progress(
            &ctx.req_id,
            chunk.len() as u64,
            count_sse_events(chunk),
            None,
            None,
        );
    }
}

fn single_live_stream_response(chunk: Vec<u8>) -> Response {
    event_stream_response(futures_util::stream::once(async move {
        Ok::<Bytes, std::io::Error>(Bytes::from(chunk))
    }))
}

fn empty_live_stream_response() -> Response {
    event_stream_response(futures_util::stream::empty::<Result<Bytes, std::io::Error>>())
}

fn remaining_live_stream_response(
    mut upstream_events: websocket::CodexWebSocketEventReceiver,
    mut translator: LiveStreamTranslator,
    first_chunk: Vec<u8>,
    ctx: RequestContext,
) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(64);
    tokio::spawn(async move {
        if tx.send(Ok(Bytes::from(first_chunk))).await.is_err() {
            return;
        }
        while let Some(item) = upstream_events.recv().await {
            match item {
                Ok(payload) => {
                    let (chunk, terminal) =
                        match translate_live_stream_payload(&mut translator, &payload, &ctx) {
                            Ok(result) => result,
                            Err(message) => {
                                clear_continuation(ctx.session_id.as_deref());
                                let _ = tx.send(Err(stream_error(message))).await;
                                return;
                            }
                        };
                    if !chunk.is_empty() {
                        record_live_stream_progress(&ctx, &chunk);
                        if tx.send(Ok(Bytes::from(chunk))).await.is_err() {
                            return;
                        }
                    }
                    if terminal {
                        clear_continuation(ctx.session_id.as_deref());
                        return;
                    }
                }
                Err(err) => {
                    clear_continuation(ctx.session_id.as_deref());
                    let _ = tx.send(Err(stream_error(codex_error_message(&err)))).await;
                    return;
                }
            }
        }

        clear_continuation(ctx.session_id.as_deref());
        let _ = tx
            .send(Err(stream_error(
                "WebSocket connection closed before terminal Codex response event",
            )))
            .await;
    });

    let stream = futures_util::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|item| (item, rx))
    });
    event_stream_response(stream)
}

fn event_stream_response<S>(stream: S) -> Response
where
    S: futures_util::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    let headers = [
        (http::header::CONTENT_TYPE, "text/event-stream"),
        (http::header::CACHE_CONTROL, "no-cache"),
        (http::header::CONNECTION, "keep-alive"),
    ];
    (headers, Body::from_stream(stream)).into_response()
}

fn stream_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::other(message.into())
}

fn is_codex_terminal_event(payload: &serde_json::Value) -> bool {
    matches!(
        payload.get("type").and_then(|v| v.as_str()),
        Some("response.completed")
            | Some("response.incomplete")
            | Some("response.done")
            | Some("response.failed")
            | Some("response.error")
            | Some("error")
    )
}

fn update_continuation_from_upstream(
    session_id: Option<&str>,
    request_body: &translate::request::ResponsesRequest,
    upstream_body: &[u8],
) {
    match finish_metadata_from_upstream(upstream_body) {
        Ok(Some(finish)) if finish.continuation_eligible => {
            record_continuation(
                session_id,
                request_body,
                finish.response_id.as_deref(),
                &finish.output_items,
            );
        }
        _ => clear_continuation(session_id),
    }
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn map_codex_error_to_response(err: &client::CodexError) -> Response {
    match err.status {
        401 | 403 => json_error(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            err.detail.as_deref().unwrap_or("Authentication failed"),
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
        _ => json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            codex_error_message(err),
        ),
    }
}

fn codex_error_message(err: &client::CodexError) -> &str {
    err.detail.as_deref().unwrap_or({
        if err.status == 0 {
            err.message.as_str()
        } else {
            "Upstream error"
        }
    })
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

pub(crate) struct CodexCli;

impl CliHandlers for CodexCli {
    fn login(&self) -> Result<(), anyhow::Error> {
        let tokens = run_browser_login()?;
        let store = file_store();
        let manager = CodexAuthManager::new(store);
        let saved = manager.persist_initial_tokens(&tokens)?;
        print!(
            "{}",
            format_auth_saved_output(&manager.store.auth_path(), saved.account_id.as_deref())
        );
        Ok(())
    }

    fn device(&self) -> Result<(), anyhow::Error> {
        let tokens = DeviceAuthClient::new().run()?;
        let store = file_store();
        let manager = CodexAuthManager::new(store);
        let saved = manager.persist_initial_tokens(&tokens)?;
        print!(
            "{}",
            format_auth_saved_output(&manager.store.auth_path(), saved.account_id.as_deref())
        );
        Ok(())
    }

    fn status(&self) -> Result<(), anyhow::Error> {
        let store = file_store();
        let stored = store.load_auth()?;
        match stored {
            Some(auth) => {
                println!(
                    "Account: {}",
                    auth.account_id.as_deref().unwrap_or("(none)")
                );
                println!("{}", format_expiry(auth.expires, now_ms()));
                println!("Storage: {}", store.auth_path());
                Ok(())
            }
            None => {
                anyhow::bail!("Not authenticated");
            }
        }
    }

    fn logout(&self) -> Result<(), anyhow::Error> {
        let store = file_store();
        store.clear_auth()?;
        println!("Logged out");
        Ok(())
    }
}

pub(crate) static CODEX_CLI: CodexCli = CodexCli;

// ---------------------------------------------------------------------------
// CLI helpers
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn format_expiry(expires: u64, now: u64) -> String {
    let remaining = (i128::from(expires) - i128::from(now)).div_euclid(1000);
    let iso = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(expires) * 1_000_000)
        .ok()
        .and_then(|dt| {
            let fmt = time::format_description::parse_borrowed::<2>(
                "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
            )
            .ok()?;
            dt.format(&fmt).ok()
        })
        .unwrap_or_else(|| "invalid".to_string());
    format!("Expires: {iso} (in {remaining}s)")
}

fn format_auth_saved_output(auth_path: &str, account_id: Option<&str>) -> String {
    let mut out = format!("Auth saved in {auth_path}\n");
    if let Some(account_id) = account_id {
        out.push_str(&format!("Account: {account_id}\n"));
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous.take() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    fn supported_models_includes_fast_variants() {
        let provider = CodexProvider::new();
        let models = provider.supported_models();
        assert!(models.contains(&"gpt-5.5".to_string()));
        assert!(models.contains(&"gpt-5.5-fast".to_string()));
        assert!(models.contains(&"gpt-5.4".to_string()));
        assert!(models.contains(&"gpt-5.4-mini".to_string()));
    }

    #[test]
    fn codex_cli_status_unauthenticated() {
        let _lock = env_lock();
        let config = tempfile::TempDir::new().unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        // Without stored auth, status should fail
        let result = CODEX_CLI.status();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Not authenticated")
        );
    }

    #[test]
    fn codex_cli_logout_does_not_error() {
        let _lock = env_lock();
        let config = tempfile::TempDir::new().unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        // Logout without auth should succeed
        let result = CODEX_CLI.logout();
        assert!(result.is_ok());
    }

    #[test]
    fn format_auth_saved_output_with_account() {
        assert_eq!(
            format_auth_saved_output("/tmp/auth.json", Some("acct_1")),
            "Auth saved in /tmp/auth.json\nAccount: acct_1\n"
        );
    }

    #[test]
    fn format_auth_saved_output_without_account() {
        assert_eq!(
            format_auth_saved_output("/tmp/auth.json", None),
            "Auth saved in /tmp/auth.json\n"
        );
    }

    #[test]
    fn format_expiry_with_future_expiry() {
        // 2100-01-01T00:00:00Z in ms
        let expires = 4102444800000;
        let now = 4102444790000; // 10s before
        let output = format_expiry(expires, now);
        assert!(output.starts_with("Expires: 2100-01-01T00:00:00.000Z (in "));
        assert!(output.ends_with("s)"));
    }

    #[test]
    fn format_expiry_with_past_expiry() {
        // 2000-01-01T00:00:00Z in ms
        let expires = 946684800000;
        let now = 946684810000; // 10s after
        let output = format_expiry(expires, now);
        assert!(output.starts_with("Expires: 2000-01-01T00:00:00.000Z (in -"));
    }

    #[tokio::test]
    async fn statusless_codex_error_returns_source_message() {
        let err = client::CodexError {
            status: 0,
            message: "WebSocket connect error: HTTP error: 502 Bad Gateway".to_string(),
            detail: None,
            retry_after: None,
            origin: client::CodexErrorOrigin::WebSocket,
        };

        let response = map_codex_error_to_response(&err);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body.pointer("/error/message").and_then(|v| v.as_str()),
            Some("WebSocket connect error: HTTP error: 502 Bad Gateway")
        );
    }
}
