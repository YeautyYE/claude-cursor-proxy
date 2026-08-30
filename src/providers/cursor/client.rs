use base64::Engine as _;
use bytes::Bytes;
use futures_util::StreamExt;
use prost::Message;
use serde_json::json;
use std::io::Write;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::config;
use crate::logging::create_logger;
use crate::paths;
use crate::providers::cursor::connect::{
    ConnectFrame, ConnectFrameDecoder, FLAG_END, FLAG_GZIP,
    cursor_connect_error_is_missing_conversation_data, cursor_connect_error_is_missing_image,
    encode_connect_frame, parse_connect_error,
};
use crate::providers::cursor::live::{
    ambiguous_http1_append_error, cursor_error_is_kv_blob_overflow,
};
use crate::providers::cursor::model::CursorModelResolution;
use crate::providers::cursor::proto::{
    self, AgentClientMessage, ClientHeartbeat, ExecClientMessage, RequestContext,
    RequestContextResult, RequestContextSuccess, RunRequest,
};
use crate::providers::cursor::request::CursorSelectedImage;
use crate::providers::cursor::response::{CursorStreamEvent, decode_upstream_response};

/// Upstream response from the Cursor API.
///
/// Contains the raw response bytes (or body bytes for streaming) and the
/// HTTP status.
pub struct CursorUpstreamResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub error_detail: Option<String>,
}

/// Request-scoped values that must remain stable across a Cursor run and any
/// transport retry. Keeping these together also makes it harder for a new
/// call site to accidentally omit the identity override.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CursorRunOptions<'a> {
    pub session_id: Option<&'a str>,
    pub client_type: Option<&'a str>,
    /// Conversation binding observed while rendering the caller's prompt.
    /// The request handler may render a checkpoint delta and then yield before
    /// this client opens the upstream stream.  If KV normalization rotates the
    /// binding in that window, the delta belongs to the old conversation and
    /// must be replaced with the caller's full-history replay text.
    pub expected_conversation_id: Option<&'a str>,
    pub reset_user_text: Option<&'a str>,
    pub reset_images: Option<&'a [CursorSelectedImage]>,
}

/// Compare the continuation binding captured while rendering a request with
/// the just-in-time binding used for upstream dispatch.  A changed UUID means
/// the old checkpoint delta cannot be applied to the new Cursor conversation.
pub(crate) fn continuation_binding_changed(
    expected_conversation_id: Option<&str>,
    actual_conversation_id: Option<&str>,
) -> bool {
    expected_conversation_id.is_some_and(|expected| actual_conversation_id != Some(expected))
}

impl CursorUpstreamResponse {
    pub fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }
}

/// HTTP client for the Cursor AgentService/Run endpoint.
///
/// Fingerprint defaults match official Cursor Agent CLI
/// (`~/.local/share/cursor-agent`, version e.g. 2026.07.16-899851b):
/// - `x-cursor-client-type: cli`
/// - `x-cursor-client-version: cli-<install-version>`
/// - `x-ghost-mode: true` when privacy unset
/// - `User-Agent: connect-es/1.6.1`
/// - HTTP/1.1 preferred (CLI uses H1 when server forces BiDi disabled)
/// - No `x-cursor-checksum` on the main Agent path (IDE-only)
#[derive(Clone)]
pub struct CursorHttpClient {
    pub(crate) client: reqwest::Client,
    pub(crate) base_url: String,
    pub(crate) timeout_secs: u64,
    http1_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorReqwestMode {
    Http1Only,
    Http2Alpn,
    CleartextH2PriorKnowledge,
}

pub(crate) fn cursor_reqwest_mode(prefer_http1: bool, is_cleartext: bool) -> CursorReqwestMode {
    if is_cleartext {
        CursorReqwestMode::CleartextH2PriorKnowledge
    } else if prefer_http1 {
        CursorReqwestMode::Http1Only
    } else {
        CursorReqwestMode::Http2Alpn
    }
}

fn apply_cursor_reqwest_mode(
    builder: reqwest::ClientBuilder,
    mode: CursorReqwestMode,
) -> reqwest::ClientBuilder {
    match mode {
        CursorReqwestMode::CleartextH2PriorKnowledge => {
            // Mock/tests use http://127.0.0.1 — never send loopback through Clash.
            builder.no_proxy().http2_prior_knowledge()
        }
        CursorReqwestMode::Http1Only => builder.http1_only(),
        // gRPC keepalive: do not PING more often than ~60s. 10s/15s closed the
        // shared H2 connection (every live stream) when Clash delayed ACK.
        CursorReqwestMode::Http2Alpn => builder
            .http2_keep_alive_timeout(std::time::Duration::from_secs(20))
            .http2_keep_alive_while_idle(false)
            .http2_keep_alive_interval(std::time::Duration::from_secs(60)),
    }
}

impl Default for CursorHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CursorHttpClient {
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn new() -> Self {
        Self::with_prefer_http1(super::http1::prefer_http1_agent())
    }

    /// Build a client pinned to HTTP/1.1 (`true`) or HTTP/2 (`false`).
    ///
    /// RunSSE fallback must use this with `true`. A default H2 client posting to
    /// `/RunSSE` still negotiates HTTP/2, which is what made 0.1.36's "HTTP/1"
    /// reconnect die with `stream error received: unexpected internal error`.
    pub fn with_prefer_http1(prefer_http1: bool) -> Self {
        let base_url = config::cursor_base_url();
        let is_cleartext = base_url.starts_with("http://");
        let timeout_secs = config::cursor_request_timeout_secs();

        // No whole-request timeout on the HTTP client: BiDi agent turns can exceed
        // several minutes while still streaming. Completion / stall is enforced in
        // the frame read loop (setup idle / complete idle / hard timeout).
        //
        // Reuse TLS+H2 connections across turns — `pool_max_idle_per_host(0)` forced
        // a full TCP+TLS handshake on every claude -p / messages call (~100–400ms).
        let mut builder = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(15))
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .pool_max_idle_per_host(8)
            .tcp_nodelay(true)
            .tcp_keepalive(std::time::Duration::from_secs(30));
        let _ = timeout_secs; // retained for error messages / hard-timeout default

        let mode = cursor_reqwest_mode(prefer_http1, is_cleartext);
        if !is_cleartext
            && cursor_http_bypass_proxy(std::env::var("CCP_CURSOR_NO_PROXY").ok().as_deref())
        {
            builder = builder.no_proxy();
        }
        builder = apply_cursor_reqwest_mode(builder, mode);
        let http1_only = matches!(mode, CursorReqwestMode::Http1Only);

        let client = builder.build().expect("CursorHttpClient: reqwest client");

        Self {
            client,
            base_url,
            timeout_secs,
            http1_only,
        }
    }

    pub(crate) fn prefers_http1(&self) -> bool {
        self.http1_only
    }

    /// Fetch the live Cursor model catalog via `AgentService/GetUsableModels`.
    ///
    /// Prefers Connect JSON (same as official CLI `agent models`); falls back to
    /// Connect protobuf unary when JSON fails. Results are cached in-process for
    /// ~5 minutes and partitioned by the active Cursor account.
    pub async fn fetch_usable_models(&self, token: &str) -> Result<Vec<String>, CursorError> {
        // Capture the account generation before the network request. Auth can
        // be hot-swapped by another process while this request is in flight;
        // the generation-aware store below then drops a stale completion.
        let account_generation = super::model::observe_live_usable_models_account(token);
        if let Some(cached) = super::model::cached_live_usable_models_for_account(token) {
            return Ok(cached);
        }

        match self.fetch_usable_models_json(token).await {
            Ok(models) if !models.is_empty() => {
                super::model::store_live_usable_models_for_account_at_generation(
                    token,
                    account_generation,
                    models.clone(),
                );
                return Ok(models);
            }
            Ok(_) => { /* empty — try proto */ }
            Err(_) => { /* fall through to proto */ }
        }

        let models = self.fetch_usable_models_proto(token).await?;
        if !models.is_empty() {
            super::model::store_live_usable_models_for_account_at_generation(
                token,
                account_generation,
                models.clone(),
            );
        }
        Ok(models)
    }

    async fn fetch_usable_models_json(&self, token: &str) -> Result<Vec<String>, CursorError> {
        let url = format!(
            "{}/agent.v1.AgentService/GetUsableModels",
            self.base_url.trim_end_matches('/')
        );
        let req = self
            .client
            .post(&url)
            .timeout(Duration::from_secs(30))
            .bearer_auth(token)
            .header("content-type", "application/json")
            .header("connect-protocol-version", "1")
            .header("user-agent", "connect-es/1.6.1")
            .body("{}");
        let req = apply_cursor_identity_headers(req, token);

        let resp = req
            .send()
            .await
            .map_err(|e| CursorError::from_reqwest(e, 30))?;
        let status = resp.status().as_u16();
        let retry_after = retry_after_header(resp.headers());
        let body = resp
            .text()
            .await
            .map_err(|e| CursorError::from_reqwest(e, 30))?;

        if !(200..300).contains(&status) {
            return Err(CursorError::new(
                status,
                format!("GetUsableModels JSON failed with HTTP {status}"),
                Some(body.chars().take(500).collect()),
            )
            .with_retry_after(retry_after));
        }

        parse_usable_models_json(&body)
    }

    async fn fetch_usable_models_proto(&self, token: &str) -> Result<Vec<String>, CursorError> {
        let url = format!(
            "{}/agent.v1.AgentService/GetUsableModels",
            self.base_url.trim_end_matches('/')
        );
        let request = proto::GetUsableModelsRequest {
            custom_model_ids: Vec::new(),
        };
        let mut payload = Vec::new();
        request
            .encode(&mut payload)
            .map_err(|e| CursorError::internal(format!("GetUsableModels encode: {e}")))?;

        let req = self
            .client
            .post(&url)
            .timeout(Duration::from_secs(30))
            .bearer_auth(token)
            .header("content-type", "application/proto")
            .header("connect-protocol-version", "1")
            .header("user-agent", "connect-es/1.6.1")
            .body(payload);
        let req = apply_cursor_identity_headers(req, token);

        let resp = req
            .send()
            .await
            .map_err(|e| CursorError::from_reqwest(e, 30))?;
        let status = resp.status().as_u16();
        let retry_after = retry_after_header(resp.headers());
        let body = resp
            .bytes()
            .await
            .map_err(|e| CursorError::from_reqwest(e, 30))?;

        if !(200..300).contains(&status) {
            return Err(CursorError::new(
                status,
                format!("GetUsableModels proto failed with HTTP {status}"),
                Some(String::from_utf8_lossy(&body).chars().take(500).collect()),
            )
            .with_retry_after(retry_after));
        }

        decode_usable_models_proto(&body)
    }

    /// Run the Cursor agent with the given prompt and token.
    ///
    /// `AgentService/Run` is **BiDiStreaming**. Official CLI keeps the client
    /// stream open and sends `client_heartbeat` (~5s). A unary POST that
    /// half-closes immediately leaves the server sending only heartbeats until
    /// timeout. We therefore:
    /// 1. open a duplex request body channel
    /// 2. send the initial `run_request` frame
    /// 3. periodically send empty `client_heartbeat` frames
    /// 4. stream-read the response until a Connect END frame (or timeout)
    pub async fn run_agent(
        &self,
        token: &str,
        prompt: &str,
        model: &str,
        images: &[CursorSelectedImage],
        custom_system_prompt: Option<&str>,
    ) -> Result<CursorUpstreamResponse, CursorError> {
        self.run_agent_with_session(token, prompt, model, images, custom_system_prompt, None)
            .await
    }

    pub async fn run_agent_with_session(
        &self,
        token: &str,
        prompt: &str,
        model: &str,
        images: &[CursorSelectedImage],
        custom_system_prompt: Option<&str>,
        session_id: Option<&str>,
    ) -> Result<CursorUpstreamResponse, CursorError> {
        let client_type = config::cursor_client_type_for_model(model);
        self.run_agent_with_session_profile(
            token,
            prompt,
            model,
            images,
            custom_system_prompt,
            CursorRunOptions {
                session_id,
                client_type: Some(&client_type),
                expected_conversation_id: None,
                reset_user_text: None,
                reset_images: None,
            },
        )
        .await
    }

    /// Run an agent with an optional request-scoped client type override. The
    /// override is captured before opening the stream and is reused by HTTP/1
    /// retries, keeping a Sand run on one identity for its full lifetime.
    pub(crate) async fn run_agent_with_session_profile(
        &self,
        token: &str,
        prompt: &str,
        model: &str,
        images: &[CursorSelectedImage],
        custom_system_prompt: Option<&str>,
        options: CursorRunOptions<'_>,
    ) -> Result<CursorUpstreamResponse, CursorError> {
        let resolved = super::model::resolve_cursor_model(model)
            .map_err(|e| CursorError::internal(format!("model resolution: {e}")))?;

        let request_id = uuid::Uuid::new_v4().to_string();
        let continuation = super::conversation::continuation_for(options.session_id);
        // `continuation_for` is intentionally called as close to dispatch as
        // possible, but the handler may have rendered a checkpoint delta from
        // an earlier snapshot.  A KV-overflow normalization in that gap binds
        // this request to a fresh conversation; replay the complete history
        // rather than sending the stale delta against an empty checkpoint.
        let binding_rotated = continuation_binding_changed(
            options.expected_conversation_id,
            continuation.conversation_id.as_deref(),
        );
        let request_prompt = if binding_rotated {
            options.reset_user_text.unwrap_or(prompt)
        } else {
            prompt
        };
        let request_images = if binding_rotated {
            options.reset_images.unwrap_or(images)
        } else {
            images
        };
        if binding_rotated {
            create_logger("cursor").warn(
                "continuation_binding_changed_before_buffered_open",
                Some(serde_json::Map::from_iter([
                    (
                        "expectedConversationId".into(),
                        serde_json::json!(options.expected_conversation_id),
                    ),
                    (
                        "actualConversationId".into(),
                        serde_json::json!(continuation.conversation_id),
                    ),
                    ("replay".into(), serde_json::json!("full_history")),
                ])),
            );
        }
        let run_request = build_run_request_with_continuation(
            request_prompt,
            &resolved,
            request_images,
            &request_id,
            custom_system_prompt,
            &continuation,
            None,
        );

        let msg = AgentClientMessage {
            run_request: Some(run_request),
            exec_client_message: None,
            kv_client_message: None,
            exec_client_control_message: None,
            interaction_response: None,
            client_heartbeat: None,
        };

        let mut payload = Vec::new();
        msg.encode(&mut payload)
            .map_err(|e| CursorError::internal(format!("prost encode: {e}")))?;
        let first_frame = encode_connect_frame(&payload, 0);

        let url = format!(
            "{}/agent.v1.AgentService/Run",
            self.base_url.trim_end_matches('/')
        );

        let client_version = config::cursor_client_version();
        let client_type = options
            .client_type
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(config::cursor_client_type);
        let ghost_mode = if config::cursor_ghost_mode() {
            "true"
        } else {
            "false"
        };
        let profile = config::cursor_client_profile();
        let ide_profile = profile.eq_ignore_ascii_case("ide");

        // HTTPS → BiDi duplex (heartbeats). Cleartext mock servers are unary and
        // deadlock if the request stream never ends — send a finite body there.
        // HTTP/1 pinning follows this client (`with_prefer_http1`), not a second
        // read of `CCP_CURSOR_HTTP1` (that made 0.1.36 retries stay on H2).
        let use_bidi = !self.base_url.starts_with("http://")
            && !matches!(
                std::env::var("CCP_CURSOR_BIDI")
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase()
                    .as_str(),
                "0" | "false" | "no" | "off"
            );
        let use_http1_sse = buffered_run_use_http1_sse(use_bidi, self.prefers_http1());

        let (tx, h1_append, body, url, heartbeat_task) = if use_http1_sse {
            let run_url = format!(
                "{}/agent.v1.AgentService/RunSSE",
                self.base_url.trim_end_matches('/')
            );
            let sse_body = super::http1::encode_run_sse_request(&request_id)?;
            let append = super::http1::BidiAppendSession::new(
                self.client.clone(),
                self.base_url.clone(),
                token.to_string(),
                request_id.clone(),
                vec![
                    ("x-cursor-client-type".into(), client_type.clone()),
                    ("x-cursor-client-version".into(), client_version.clone()),
                    ("x-ghost-mode".into(), ghost_mode.to_string()),
                ],
            );
            // The append can be accepted by Cursor even when its HTTP response
            // is lost.  Preserve the same ambiguity marker used by the live
            // transport; otherwise the buffered caller treats a 502 as a safe
            // same-request retry and may create a duplicate Run / 503 storm.
            append
                .append_message(&msg)
                .await
                .map_err(|error| ambiguous_http1_append_error(error, "initial Run"))?;
            let hb_append = append.clone();
            let heartbeat_secs = env_u64("CCP_CURSOR_HEARTBEAT_SECS", 5);
            let heartbeat_task = tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let frame = match encode_client_heartbeat_frame() {
                        Ok(f) => f,
                        Err(_) => break,
                    };
                    if hb_append.append_connect_or_raw(&frame).await.is_err() {
                        break;
                    }
                }
            });
            (
                None,
                Some(append),
                reqwest::Body::from(sse_body.to_vec()),
                run_url,
                Some(heartbeat_task),
            )
        } else if use_bidi {
            // Keep a little headroom for request_context/KV replies while the
            // network body is briefly backpressured. Heartbeats are best
            // effort and must never occupy all slots in this queue.
            let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(32);
            tx.send(Ok(first_frame.clone()))
                .await
                .map_err(|_| CursorError::internal("cursor request channel closed"))?;

            let hb_tx = tx.clone();
            let heartbeat_secs = env_u64("CCP_CURSOR_HEARTBEAT_SECS", 5);
            let heartbeat_task = tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    let frame = match encode_client_heartbeat_frame() {
                        Ok(f) => f,
                        Err(_) => break,
                    };
                    match hb_tx.try_send(Ok(frame)) {
                        Ok(()) => {}
                        // A full queue means the body pump is already
                        // carrying useful frames. Drop this tick and let the
                        // next interval try again; awaiting here can block
                        // the producer forever and starve a KV reply.
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
            });

            let body_stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            });
            (
                Some(tx),
                None,
                reqwest::Body::wrap_stream(body_stream),
                url,
                Some(heartbeat_task),
            )
        } else {
            (
                None,
                None,
                reqwest::Body::from(first_frame.to_vec()),
                url,
                None,
            )
        };
        let mut heartbeat_task = AbortOnDrop::new(heartbeat_task);

        // Official CLI Agent interceptor (index.js):
        //   authorization, x-ghost-mode, x-cursor-client-version, x-cursor-client-type,
        //   x-request-id, x-cursor-streaming, User-Agent connect-es/1.6.1
        let mut req = self
            .client
            .post(&url)
            .bearer_auth(token)
            .header("content-type", "application/connect+proto")
            .header("connect-protocol-version", "1")
            .header("connect-accept-encoding", "gzip")
            .header("user-agent", "connect-es/1.6.1")
            .header("x-cursor-client-type", &client_type)
            .header("x-cursor-client-version", &client_version)
            .header("x-ghost-mode", ghost_mode)
            .header("x-request-id", &request_id)
            .header("x-cursor-streaming", "true")
            .header("x-original-request-id", &request_id);

        if ide_profile {
            req = req
                .header("x-cursor-client-device-type", "desktop")
                .header("x-cursor-client-os", config::cursor_client_os())
                .header("x-cursor-client-arch", config::cursor_client_arch())
                .header("x-new-onboarding-completed", "true")
                .header("x-amzn-trace-id", format!("Root={request_id}"));

            if let Some(commit) = config::cursor_client_commit() {
                req = req.header("x-cursor-client-commit", commit);
            }
            if let Some(tz) = config::cursor_timezone() {
                req = req.header("x-cursor-timezone", tz);
            }
            if let Some(key) = config::cursor_client_key() {
                req = req.header("x-client-key", key);
            }
            if let Some(sid) = config::cursor_session_id() {
                req = req.header("x-session-id", sid);
            }
        }

        let checksum_mode = std::env::var("CCP_CURSOR_CHECKSUM_MODE").unwrap_or_else(|_| {
            if ide_profile {
                "token".into()
            } else {
                "none".into()
            }
        });
        if !checksum_mode.eq_ignore_ascii_case("none")
            && !checksum_mode.eq_ignore_ascii_case("off")
            && !checksum_mode.eq_ignore_ascii_case("0")
        {
            let checksum = if checksum_mode.eq_ignore_ascii_case("storage") {
                let machine_ids = super::identity::load_cursor_machine_ids();
                machine_ids.machine_id.as_ref().map(|mid| {
                    super::identity::build_cursor_checksum(
                        mid,
                        machine_ids.mac_machine_id.as_deref(),
                    )
                })
            } else {
                Some(super::identity::build_cursor_checksum_for_token(token))
            };
            if let Some(cs) = checksum {
                req = req.header("x-cursor-checksum", cs);
            }
        }

        let resp = match tokio::time::timeout(
            Duration::from_secs(self.timeout_secs),
            req.body(body).send(),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                heartbeat_task.abort();
                drop(tx);
                return Err(buffered_open_reqwest_error(e, self.timeout_secs));
            }
            Err(_) => {
                heartbeat_task.abort();
                drop(tx);
                return Err(CursorError::new(
                    504,
                    format!(
                        "Cursor Agent open timed out after {}s; acceptance is ambiguous",
                        self.timeout_secs
                    ),
                    None,
                ));
            }
        };

        let status = resp.status().as_u16();
        // Error responses cannot make progress on the accepted Run. Stop the
        // heartbeat producer before draining the diagnostic body so it cannot
        // keep issuing BidiAppend calls after the RunSSE/H2 open failed.
        if status >= 400 {
            heartbeat_task.abort();
        }
        let headers = resp.headers().clone();
        let retry_after = retry_after_header(&headers);
        let error_detail = resp
            .headers()
            .get("grpc-message")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        // ── Completion policy (measured on live Fable BiDi captures) ──────────
        // After the model emits text, Cursor often never sends InteractionUpdate
        // turn_ended (tag 14). It keeps the BiDi stream open with server heartbeats
        // + kv_server_message blobs. Waiting on heartbeats until hard timeout was
        // ~90s; waiting a fixed post-useful idle of 8s still left ~30s wall time.
        //
        // Server heartbeats / KV frames must NOT reset the progress clock.
        // Progress = text / thinking / turn_ended / answered exec / Connect END.
        //
        // Stages:
        //   setup_idle   — no useful frames yet (request_context + first token)
        //   stream_idle  — thinking/tokens but no assistant text yet
        //   complete_ms  — saw non-empty text; finish after brief silence
        //   turn_ended   — exit immediately (tiny grace for trailing bytes)
        let setup_idle_secs = env_u64("CCP_CURSOR_SETUP_IDLE_SECS", 45);
        let stream_idle_secs = env_u64("CCP_CURSOR_IDLE_SECS", 12);
        // After first text, Fable often pauses between paragraphs (0.5–2s) while
        // still streaming. 350ms cut replies mid-sentence (user saw "CLAU…").
        // Heartbeats still do not reset progress; only text/thinking/exec do.
        let complete_idle_ms = env_u64("CCP_CURSOR_COMPLETE_IDLE_MS", 2500);
        let hard_secs = env_u64("CCP_CURSOR_TIMEOUT_SECS", self.timeout_secs.max(180));
        let started = Instant::now();
        let mut last_progress = Instant::now();

        let mut body_bytes: Vec<u8> = Vec::with_capacity(64 * 1024);
        // Keep a bounded copy of error responses.  The normal success path
        // intentionally retains only useful Connect frames, so a plain JSON
        // 4xx body can otherwise disappear when the decoder reports an error
        // before producing a frame.  In particular, Cursor's KV-overflow 413
        // diagnostic is often delivered as an unframed body (or alongside a
        // stream read error) and must remain visible to the recovery classifier.
        let mut raw_error_body: Vec<u8> = Vec::new();
        let mut decoder = ConnectFrameDecoder::new();
        let mut saw_end = false;
        let mut saw_turn_ended = false;
        let mut saw_text = false;
        let mut saw_thinking_completed = false;
        let mut saw_tool_call = false;
        let mut frame_count: u32 = 0;
        let mut useful = false;
        #[allow(unused_assignments)]
        let mut finish_reason = "unknown";
        let mut byte_stream = resp.bytes_stream();
        let mut request_context_replies: u32 = 0;
        let read_err = loop {
            if started.elapsed() > Duration::from_secs(hard_secs) {
                finish_reason = "hard_timeout";
                break Some(format!(
                    "hard timeout after {hard_secs}s (CCP_CURSOR_TIMEOUT_SECS)"
                ));
            }

            // Adaptive idle: once we have assistant text (and no open tool wait),
            // only wait complete_idle_ms of silence (heartbeats ignored).
            // If we already saw a tool call, finish immediately (Claude must run it).
            let idle_limit = if saw_turn_ended || saw_end || saw_tool_call {
                Duration::from_millis(50)
            } else if saw_text {
                Duration::from_millis(complete_idle_ms)
            } else if useful {
                Duration::from_secs(stream_idle_secs)
            } else {
                Duration::from_secs(setup_idle_secs)
            };
            let wait = idle_limit.saturating_sub(last_progress.elapsed());
            if wait.is_zero() {
                if saw_tool_call {
                    finish_reason = "tool_call_ready";
                    break None;
                }
                // Silence after progress → treat as successful completion when we
                // already have text (or any useful content for partial path).
                if saw_text || saw_turn_ended {
                    finish_reason = if saw_text {
                        "complete_idle_after_text"
                    } else {
                        "complete_idle_after_turn_ended"
                    };
                    break None;
                }
                if useful {
                    finish_reason = "stream_idle_partial";
                    break Some(format!(
                        "idle timeout after {}s with thinking but no text yet",
                        stream_idle_secs
                    ));
                }
                finish_reason = "setup_idle";
                break Some(format!(
                    "idle timeout after {setup_idle_secs}s with no useful progress"
                ));
            }
            match tokio::time::timeout(wait, byte_stream.next()).await {
                Err(_) => {
                    if saw_tool_call {
                        finish_reason = "tool_call_ready";
                        break None;
                    }
                    if saw_text || saw_turn_ended {
                        finish_reason = if saw_text {
                            "complete_idle_after_text"
                        } else {
                            "complete_idle_after_turn_ended"
                        };
                        break None;
                    }
                    if useful {
                        finish_reason = "stream_idle_partial";
                        break Some(format!(
                            "idle timeout after {}s with thinking but no text yet",
                            stream_idle_secs
                        ));
                    }
                    finish_reason = "setup_idle";
                    break Some(format!(
                        "idle timeout after {setup_idle_secs}s with no useful progress"
                    ));
                }
                Ok(Some(Ok(chunk))) => {
                    if status >= 400 && raw_error_body.len() < MAX_BUFFERED_ERROR_BODY_BYTES {
                        let remaining = MAX_BUFFERED_ERROR_BODY_BYTES - raw_error_body.len();
                        raw_error_body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                    }
                    // Decode frames first; only retain interaction/exec/end frames in
                    // body_bytes. Live Fable runs stream ~200KB of kv_server_message
                    // blobs we never decode for Anthropic output — buffering them
                    // only inflates latency on the post-stream decode path.
                    match decoder.push(&chunk) {
                        Ok(frames) => {
                            frame_count += frames.len() as u32;
                            for frame in frames {
                                let class = classify_frame(&frame);
                                // A single InteractionUpdate can carry several
                                // signals at once (for example reasoning text
                                // plus turn_ended). Retain the wire frame once;
                                // appending it from each classification branch
                                // duplicates deltas when the buffered decoder
                                // later folds the retained body.
                                let retain_frame = class.is_end
                                    || class.has_text
                                    || class.has_thinking
                                    || class.thinking_completed
                                    || class.turn_ended
                                    || class.has_tool_call
                                    || class.wants_request_context;

                                if class.is_end {
                                    saw_end = true;
                                    last_progress = Instant::now();
                                }
                                if class.has_text {
                                    saw_text = true;
                                    useful = true;
                                    last_progress = Instant::now();
                                }
                                if class.has_thinking {
                                    useful = true;
                                    last_progress = Instant::now();
                                }
                                if class.thinking_completed {
                                    saw_thinking_completed = true;
                                    // Completion marker for reasoning phase — progress,
                                    // but alone not enough to finish (wait for text).
                                    last_progress = Instant::now();
                                }
                                if class.turn_ended {
                                    saw_turn_ended = true;
                                    useful = true;
                                    last_progress = Instant::now();
                                }
                                if class.has_tool_call {
                                    saw_tool_call = true;
                                    useful = true;
                                    last_progress = Instant::now();
                                }
                                // Keep token_delta / other interaction updates that
                                // classify as neither text nor thinking when they
                                // carry usage — already covered by turn_ended.
                                // Exec frames needed for session id + request_context.
                                if retain_frame {
                                    append_connect_frame(&mut body_bytes, &frame);
                                }

                                // Auto-answer every request_context exec (may repeat).
                                if class.wants_request_context {
                                    let reply = match build_request_context_reply(&frame) {
                                        Ok(Some(reply)) => reply,
                                        Ok(None) => continue,
                                        Err(error) => {
                                            heartbeat_task.abort();
                                            drop(tx);
                                            return Err(error);
                                        }
                                    };
                                    let send_result = if let Some(tx_ref) = tx.as_ref() {
                                        tx_ref.send(Ok(reply)).await.map_err(|_| {
                                            CursorError::internal(
                                                "Cursor BiDi request stream closed while replying to request_context",
                                            )
                                        })
                                    } else if let Some(append) = h1_append.as_ref() {
                                        append.append_connect_or_raw(&reply).await.map_err(
                                            |error| {
                                                ambiguous_http1_append_error(
                                                    error,
                                                    "request_context reply",
                                                )
                                            },
                                        )
                                    } else {
                                        Err(CursorError::internal(
                                            "Cursor request_context requires a live client channel",
                                        ))
                                    };
                                    if let Err(error) = send_result {
                                        heartbeat_task.abort();
                                        drop(tx);
                                        return Err(error);
                                    }
                                    request_context_replies += 1;
                                    last_progress = Instant::now();
                                    cursor_debug_log(
                                        &format!(
                                            "auto-replied request_context_result #{request_context_replies}"
                                        ),
                                        &[],
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            finish_reason = "frame_decode";
                            break Some(format!("frame decode: {e}"));
                        }
                    }
                    // Tool call ready → hand off to Claude Code immediately.
                    if saw_tool_call {
                        finish_reason = "tool_call_ready";
                        break None;
                    }
                    // Immediate finish: Connect END or turn_ended (with any useful).
                    if saw_end {
                        finish_reason = "connect_end";
                        break None;
                    }
                    if saw_turn_ended {
                        // Tiny grace for trailing usage / end frame only.
                        if let Ok(Some(Ok(extra))) =
                            tokio::time::timeout(Duration::from_millis(80), byte_stream.next())
                                .await
                            && let Ok(more) = decoder.push(&extra)
                        {
                            frame_count += more.len() as u32;
                            for frame in more {
                                let class = classify_frame(&frame);
                                if class.is_end
                                    || class.has_text
                                    || class.has_thinking
                                    || class.turn_ended
                                    || class.thinking_completed
                                {
                                    append_connect_frame(&mut body_bytes, &frame);
                                }
                            }
                        }
                        finish_reason = "turn_ended";
                        break None;
                    }
                    // Fast path: text already arrived and silence already exceeded
                    // complete idle — but never if a tool call is pending handoff.
                    if saw_text
                        && !saw_tool_call
                        && last_progress.elapsed() >= Duration::from_millis(complete_idle_ms)
                    {
                        finish_reason = "complete_idle_after_text";
                        break None;
                    }
                }
                Ok(Some(Err(e))) => {
                    finish_reason = "read_error";
                    break Some(format!("read body: {e}"));
                }
                Ok(None) => {
                    finish_reason = "stream_closed";
                    break None;
                }
            }
        };

        // Close client BiDi stream / stop heartbeats ASAP so the server can free
        // the run and we don't keep sending client_heartbeat into a closed call.
        heartbeat_task.abort();
        drop(tx);

        // Always emit debug when requested — including error paths. TUI mode
        // suppresses stderr, so we also write proxy.log + cursor-debug.log.
        let model_id = resolved.model_id.as_str();
        let body_len = body_bytes.len();
        let elapsed_ms = started.elapsed().as_millis();
        cursor_debug_log(
            &format!(
                "profile={profile} type={client_type} ver={client_version} model={model_id} bidi={use_bidi} status={status} body_len={body_len} frames={frame_count} saw_end={saw_end} saw_text={saw_text} saw_tool={saw_tool_call} saw_turn_ended={saw_turn_ended} think_done={saw_thinking_completed} useful={useful} finish={finish_reason} elapsed_ms={elapsed_ms} rc_replies={request_context_replies} complete_idle_ms={complete_idle_ms} read_err={read_err:?} grpc_message={error_detail:?}"
            ),
            &body_bytes,
        );
        if std::env::var_os("CCP_CURSOR_DEBUG").is_some() && !body_bytes.is_empty() {
            let dump = paths::resolve_state_dir(&crate::paths::DirResolverEnv::default())
                .join("cursor-last-body.bin");
            let _ = std::fs::write(&dump, &body_bytes);
        }

        // Only accept a dropped stream when Cursor actually finished the turn.
        // Useful tokens without turn_ended / Connect END are an error — otherwise
        // Claude Code sees a successful end_turn and stops retrying.
        if let Some(ref msg) = read_err {
            if status < 400
                && buffered_stream_complete_enough_to_accept(saw_end, saw_turn_ended)
                && (useful || body_has_useful_content(&body_bytes))
            {
                cursor_debug_log(
                    &format!("accepting body after stream close: {msg}"),
                    &body_bytes,
                );
                // fall through to Ok
            } else {
                // Preserve an upstream HTTP status even when the response
                // body stream itself failed.  Rewriting a 413 to 502 loses
                // Cursor's KV-overflow signal and sends the generic transport
                // retry path into a loop.  Include all available diagnostics:
                // raw body, Connect-selected frames, grpc-message, and the
                // local stream error.
                if status >= 400 {
                    let detail = buffered_http_error_detail(
                        status,
                        &raw_error_body,
                        &body_bytes,
                        error_detail.as_deref(),
                        Some(msg),
                        frame_count,
                    );
                    let error =
                        CursorError::new(status, format!("Cursor upstream HTTP {status}"), detail)
                            .with_retry_after(retry_after);
                    return Err(buffered_status_error(error, use_http1_sse));
                }
                let detail = if body_bytes.is_empty() {
                    run_agent_empty_body_detail(msg, frame_count)
                } else {
                    format!(
                        "{msg} (got {frame_count} Connect frames / {} bytes; no decodable text/thinking yet. May still be waiting for more exec tools.)",
                        body_bytes.len(),
                    )
                };
                if run_agent_should_retry_http1(
                    self.prefers_http1(),
                    finish_reason,
                    useful,
                    body_bytes.is_empty(),
                ) {
                    heartbeat_task.abort();
                    let mut fields = serde_json::Map::new();
                    fields.insert("finish".into(), json!(finish_reason));
                    fields.insert("frames".into(), json!(frame_count));
                    fields.insert("elapsedMs".into(), json!(elapsed_ms as u64));
                    create_logger("cursor").warn("run_agent_retry_http1", Some(fields));
                    return Box::pin(
                        CursorHttpClient::with_prefer_http1(true).run_agent_with_session_profile(
                            token,
                            prompt,
                            model,
                            images,
                            custom_system_prompt,
                            options,
                        ),
                    )
                    .await;
                }
                return Err(CursorError::new(502, msg.clone(), Some(detail)));
            }
        }

        if status >= 400 {
            let detail = buffered_http_error_detail(
                status,
                &raw_error_body,
                &body_bytes,
                error_detail.as_deref(),
                None,
                frame_count,
            );
            let error = CursorError::new(status, format!("Cursor upstream HTTP {status}"), detail)
                .with_retry_after(retry_after);
            return Err(buffered_status_error(error, use_http1_sse));
        }

        if !buffered_finish_accepts_incomplete(finish_reason, saw_end, saw_turn_ended, saw_text) {
            return Err(CursorError::new(
                502,
                "Cursor stream ended without turn_ended",
                Some(format!(
                    "finish={finish_reason} frames={frame_count} bytes={}",
                    body_bytes.len()
                )),
            ));
        }

        if body_bytes.is_empty() {
            return Err(CursorError::new(
                502,
                "Cursor upstream returned empty body",
                error_detail,
            ));
        }

        Ok(CursorUpstreamResponse {
            status,
            body: body_bytes,
            error_detail,
        })
    }
}

/// Debug helper: TUI `serve` suppresses stderr, so eprintln alone is invisible.
/// When `CCP_CURSOR_DEBUG` is set, write to proxy.log + cursor-debug.log and try stderr.
fn cursor_debug_log(summary: &str, body: &[u8]) {
    if std::env::var_os("CCP_CURSOR_DEBUG").is_none() {
        return;
    }
    let preview: String = body
        .iter()
        .take(80)
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    let line = format!("[cursor-debug] {summary} hex80={preview}");

    // 1) Structured proxy.log (always visible under ~/.local/state/...)
    let mut fields = serde_json::Map::new();
    fields.insert("summary".into(), json!(summary));
    fields.insert("body_len".into(), json!(body.len()));
    fields.insert("hex80".into(), json!(preview));
    create_logger("cursor").info("cursor_debug", Some(fields));

    // 2) Dedicated plain-text log next to proxy.log
    let path =
        paths::resolve_state_dir(&crate::paths::DirResolverEnv::default()).join("cursor-debug.log");
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = writeln!(f, "{line}");
    }

    // 3) stderr only when not suppressed by TUI
    let _ = writeln!(std::io::stderr(), "{line}");
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// Tokio detaches a task when its `JoinHandle` is dropped.  Buffered runs can
/// be cancelled by a disconnected downstream before reaching one of the
/// explicit return branches below, so heartbeat producers need cancellation
/// tied to the request future itself rather than a bare handle.
struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);

impl AbortOnDrop {
    fn new(handle: Option<tokio::task::JoinHandle<()>>) -> Self {
        Self(handle)
    }

    fn abort(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.abort();
    }
}

/// A request-body error after the initial Run frame was handed to reqwest has
/// an unknown acceptance boundary.  Only a definite connection-establishment
/// failure is safe to replay; timeout/reset errors must fail closed so the
/// caller does not create a second Cursor Run while the first is still alive.
fn buffered_open_reqwest_error(error: reqwest::Error, timeout_secs: u64) -> CursorError {
    let pre_connect = error.is_connect();
    let mut converted = CursorError::from_reqwest(error, timeout_secs);
    if !pre_connect
        && converted.status >= 500
        && !crate::retry::is_policy_rate_limit(&converted.client_message())
    {
        converted.message = format!(
            "Cursor Agent open failed; acceptance is ambiguous: {}",
            converted.message
        );
    }
    converted
}

fn buffered_stream_complete_enough_to_accept(saw_end: bool, saw_turn_ended: bool) -> bool {
    saw_end || saw_turn_ended
}

fn buffered_finish_accepts_incomplete(
    finish_reason: &str,
    saw_end: bool,
    saw_turn_ended: bool,
    saw_text: bool,
) -> bool {
    if buffered_stream_complete_enough_to_accept(saw_end, saw_turn_ended) {
        return true;
    }
    if finish_reason == "tool_call_ready" {
        return true;
    }
    // The idle handler already treats silence after text as success
    // (`complete_idle_after_text`). 502 here makes grok-build retry the
    // same buffered monologue.
    finish_reason == "complete_idle_after_text" && saw_text
}

/// `CCP_CURSOR_NO_PROXY=1` skips HTTP(S)_PROXY for Cursor API calls.
/// Clash/Surge TUN mode ignores this — those users need a DIRECT rule for `*.cursor.sh`.
pub(crate) fn cursor_http_bypass_proxy(raw: Option<&str>) -> bool {
    matches!(
        raw.unwrap_or("").trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Re-encode a decoded Connect frame into the retained response body.
fn append_connect_frame(body: &mut Vec<u8>, frame: &ConnectFrame) {
    body.extend_from_slice(&encode_connect_frame(&frame.payload, frame.flags));
}

fn frame_payload_bytes(frame: &ConnectFrame) -> Option<bytes::Bytes> {
    if frame.flags & FLAG_GZIP != 0 {
        super::connect::decode_gzip_frame(&frame.payload)
            .ok()
            .map(bytes::Bytes::from)
    } else {
        // Cheap refcount bump — classify_frame is on the buffered path too.
        Some(frame.payload.clone())
    }
}

/// Single-pass classification of a Connect frame (avoids double prost decode).
#[derive(Default)]
struct FrameClass {
    is_end: bool,
    has_text: bool,
    has_thinking: bool,
    thinking_completed: bool,
    turn_ended: bool,
    has_tool_call: bool,
    wants_request_context: bool,
}

fn classify_frame(frame: &ConnectFrame) -> FrameClass {
    let mut class = FrameClass {
        is_end: frame.flags & FLAG_END != 0,
        ..FrameClass::default()
    };
    if class.is_end {
        return class;
    }
    let Some(payload) = frame_payload_bytes(frame) else {
        return class;
    };
    let Ok(msg) = proto::AgentServerMessage::decode(payload.as_ref()) else {
        return class;
    };
    if let Some(update) = msg.interaction_update {
        class.has_text = update
            .text_delta
            .as_ref()
            .is_some_and(|t| !t.text.is_empty());
        class.has_thinking = update
            .thinking_delta
            .as_ref()
            .is_some_and(|t| !t.text.is_empty());
        class.thinking_completed = update.thinking_completed.is_some();
        class.turn_ended = update.turn_ended.is_some();
        // tool_call_started is a UI/transcript notification. The executable
        // boundary is ExecServerMessage; treating both as tools duplicates a
        // call and used to close the BiDi stream before its exec id arrived.
    }
    if let Some(exec) = msg.exec_server_message {
        // Empty request_context_args still means the server is waiting for a reply.
        class.wants_request_context = exec.request_context_args.is_some();
        if !class.wants_request_context {
            class.has_tool_call = class.has_tool_call
                || super::native_tools::map_exec_server_message(&exec).is_some();
        }
    }
    class
}

fn body_has_useful_content(body: &[u8]) -> bool {
    match decode_upstream_response(body) {
        Ok(events) => events.iter().any(|e| {
            matches!(
                e,
                CursorStreamEvent::TextDelta { text } if !text.is_empty()
            ) || matches!(
                e,
                CursorStreamEvent::ThinkingDelta { text } if !text.is_empty()
            ) || matches!(e, CursorStreamEvent::NativeTool { .. })
                || matches!(e, CursorStreamEvent::Usage { .. } | CursorStreamEvent::End)
        }),
        Err(_) => false,
    }
}

/// Shared identity headers for AgentService unary + BiDi calls (CLI/IDE profile).
fn apply_cursor_identity_headers(
    mut req: reqwest::RequestBuilder,
    token: &str,
) -> reqwest::RequestBuilder {
    let client_version = config::cursor_client_version();
    let client_type = config::cursor_client_type();
    let ghost_mode = if config::cursor_ghost_mode() {
        "true"
    } else {
        "false"
    };
    let profile = config::cursor_client_profile();
    let ide_profile = profile.eq_ignore_ascii_case("ide");

    req = req
        .header("x-cursor-client-type", &client_type)
        .header("x-cursor-client-version", &client_version)
        .header("x-ghost-mode", ghost_mode);

    if ide_profile {
        req = req
            .header("x-cursor-client-device-type", "desktop")
            .header("x-cursor-client-os", config::cursor_client_os())
            .header("x-cursor-client-arch", config::cursor_client_arch())
            .header("x-new-onboarding-completed", "true");

        if let Some(commit) = config::cursor_client_commit() {
            req = req.header("x-cursor-client-commit", commit);
        }
        if let Some(tz) = config::cursor_timezone() {
            req = req.header("x-cursor-timezone", tz);
        }
        if let Some(key) = config::cursor_client_key() {
            req = req.header("x-client-key", key);
        }
        if let Some(sid) = config::cursor_session_id() {
            req = req.header("x-session-id", sid);
        }
    }

    let checksum_mode = std::env::var("CCP_CURSOR_CHECKSUM_MODE").unwrap_or_else(|_| {
        if ide_profile {
            "token".into()
        } else {
            "none".into()
        }
    });
    if !checksum_mode.eq_ignore_ascii_case("none")
        && !checksum_mode.eq_ignore_ascii_case("off")
        && !checksum_mode.eq_ignore_ascii_case("0")
    {
        let checksum = if checksum_mode.eq_ignore_ascii_case("storage") {
            let machine_ids = super::identity::load_cursor_machine_ids();
            machine_ids.machine_id.as_ref().map(|mid| {
                super::identity::build_cursor_checksum(mid, machine_ids.mac_machine_id.as_deref())
            })
        } else {
            Some(super::identity::build_cursor_checksum_for_token(token))
        };
        if let Some(cs) = checksum {
            req = req.header("x-cursor-checksum", cs);
        }
    }

    req
}

/// Parse a Connect-JSON `GetUsableModelsResponse` body into model ids.
///
/// Accepts camelCase (`modelId` / `displayModelId`) and snake_case
/// (`model_id` / `display_model_id`) field names.
pub fn parse_usable_models_json(body: &str) -> Result<Vec<String>, CursorError> {
    let value: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| CursorError::internal(format!("GetUsableModels JSON parse: {e}")))?;

    // Connect error envelope: {"code":"...","message":"..."}
    if value.get("models").is_none()
        && (value.get("code").is_some() || value.get("error").is_some())
    {
        let msg = value
            .get("message")
            .or_else(|| value.pointer("/error/message"))
            .and_then(|v| v.as_str())
            .unwrap_or("GetUsableModels error");
        return Err(CursorError::new(
            502,
            msg.to_string(),
            Some(body.chars().take(500).collect()),
        ));
    }

    let models = value
        .get("models")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CursorError::internal("GetUsableModels JSON missing models[]"))?;

    let mut out = Vec::with_capacity(models.len());
    let mut seen = std::collections::HashSet::new();
    for model in models {
        let id = model
            .get("modelId")
            .or_else(|| model.get("model_id"))
            .or_else(|| model.get("displayModelId"))
            .or_else(|| model.get("display_model_id"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(id) = id
            && seen.insert(id.to_string())
        {
            out.push(id.to_string());
        }
    }
    Ok(out)
}

fn decode_usable_models_proto(body: &[u8]) -> Result<Vec<String>, CursorError> {
    // Unary Connect proto: raw message body.
    if let Ok(resp) = proto::GetUsableModelsResponse::decode(body) {
        let ids = model_details_to_ids(&resp.models);
        if !ids.is_empty() || body.is_empty() {
            return Ok(ids);
        }
    }

    // Some gateways wrap unary in a Connect envelope (flags + length + payload).
    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder
        .push(body)
        .map_err(|e| CursorError::internal(format!("GetUsableModels frame: {e}")))?;
    for frame in frames {
        if frame.flags & FLAG_END != 0 && frame.payload.is_empty() {
            continue;
        }
        let payload = if frame.flags & FLAG_GZIP != 0 {
            super::connect::decode_gzip_frame(&frame.payload)
                .map_err(|e| CursorError::internal(format!("gzip: {e}")))?
        } else {
            frame.payload.to_vec()
        };
        if let Ok(resp) = proto::GetUsableModelsResponse::decode(&payload[..]) {
            return Ok(model_details_to_ids(&resp.models));
        }
    }

    Err(CursorError::internal(
        "GetUsableModels proto: could not decode response",
    ))
}

fn model_details_to_ids(models: &[proto::ModelDetails]) -> Vec<String> {
    let mut out = Vec::with_capacity(models.len());
    let mut seen = std::collections::HashSet::new();
    for m in models {
        let id = m
            .model_id
            .as_deref()
            .or(m.display_model_id.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(id) = id
            && seen.insert(id.to_string())
        {
            out.push(id.to_string());
        }
    }
    out
}

pub(crate) fn encode_client_heartbeat_frame() -> Result<Bytes, CursorError> {
    // Empty ClientHeartbeat is identical every tick — cache the Connect frame.
    static CACHED: std::sync::OnceLock<Bytes> = std::sync::OnceLock::new();
    if let Some(frame) = CACHED.get() {
        return Ok(frame.clone());
    }
    let msg = AgentClientMessage {
        run_request: None,
        exec_client_message: None,
        kv_client_message: None,
        exec_client_control_message: None,
        interaction_response: None,
        client_heartbeat: Some(ClientHeartbeat {}),
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload)
        .map_err(|e| CursorError::internal(format!("heartbeat encode: {e}")))?;
    let frame = encode_connect_frame(payload, 0);
    Ok(CACHED.get_or_init(|| frame.clone()).clone())
}

/// Build empty-success `request_context_result` for an exec_server_message frame.
fn build_request_context_reply(frame: &ConnectFrame) -> Result<Option<Bytes>, CursorError> {
    if frame.flags & FLAG_END != 0 {
        return Ok(None);
    }
    let payload = if frame.flags & FLAG_GZIP != 0 {
        super::connect::decode_gzip_frame(&frame.payload)
            .map_err(|e| CursorError::internal(format!("gzip: {e}")))?
    } else {
        frame.payload.to_vec()
    };
    let msg = match proto::AgentServerMessage::decode(&payload[..]) {
        Ok(m) => m,
        Err(_) => return Ok(None),
    };
    let Some(exec) = msg.exec_server_message else {
        return Ok(None);
    };
    if exec.request_context_args.is_none() {
        return Ok(None);
    }
    let reply = AgentClientMessage {
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
                    request_context: Some(RequestContext::default()),
                    served_from_disk_cache: Some(false),
                }),
                error: None,
            }),
            shell_stream: None,
            pi_write_result: None,
            pi_edit_result: None,
        }),
        kv_client_message: None,
        exec_client_control_message: None,
        interaction_response: None,
        client_heartbeat: None,
    };
    let mut payload = Vec::new();
    reply
        .encode(&mut payload)
        .map_err(|e| CursorError::internal(format!("request_context encode: {e}")))?;
    Ok(Some(encode_connect_frame(payload, 0)))
}

#[allow(dead_code)] // Convenience wrapper for callers that need a fresh turn.
pub(crate) fn build_run_request(
    prompt: &str,
    resolved: &CursorModelResolution,
    images: &[CursorSelectedImage],
    request_id: &str,
    custom_system_prompt: Option<&str>,
) -> RunRequest {
    build_run_request_with_continuation(
        prompt,
        resolved,
        images,
        request_id,
        custom_system_prompt,
        &super::conversation::RunContinuation::default(),
        None,
    )
}

pub(crate) fn build_run_request_with_continuation(
    prompt: &str,
    resolved: &CursorModelResolution,
    images: &[CursorSelectedImage],
    request_id: &str,
    custom_system_prompt: Option<&str>,
    continuation: &super::conversation::RunContinuation,
    mcp_tools: Option<proto::McpTools>,
) -> RunRequest {
    let selected_images: Vec<proto::SelectedImage> = images
        .iter()
        .filter_map(|img| {
            let data = base64::engine::general_purpose::STANDARD
                .decode(img.data.trim())
                .ok()?;
            Some(proto::SelectedImage {
                // Inline bytes belong to field 8. Keep blob_id and the
                // blob_id_with_data oneof arm empty: Anthropic supplies the
                // bytes directly and does not give us a Cursor blob id.
                blob_id: Vec::new(),
                data,
                blob_id_with_data: None,
                uuid: img.uuid.clone(),
                path: img.path.clone(),
                dimension: None,
                mime_type: img.mime_type.clone(),
            })
        })
        .collect();

    let pre_fetched_blobs: Vec<proto::PreFetchedBlob> = continuation
        .pre_fetched_blobs
        .iter()
        .map(|(id, value)| proto::PreFetchedBlob {
            id: id.clone(),
            value: value.clone(),
        })
        .collect();

    RunRequest {
        // Empty bytes = fresh ConversationState {}; otherwise opaque Structure.
        conversation_state: Some(continuation.conversation_state.clone()),
        action: Some(proto::Action {
            user_message_action: Some(proto::UserMessageAction {
                user_message: Some(proto::UserMessage {
                    text: prompt.to_string(),
                    message_id: request_id.to_string(),
                    selected_context: if selected_images.is_empty() {
                        None
                    } else {
                        Some(proto::SelectedContext { selected_images })
                    },
                    mode: resolved.mode.as_proto_enum(),
                }),
            }),
            resume_action: None,
        }),
        model_details: Some(proto::ModelDetails {
            model_id: Some(resolved.model_id.clone()),
            display_model_id: Some(resolved.model_id.clone()),
            display_name: Some(resolved.model_id.clone()),
        }),
        mcp_tools,
        conversation_id: continuation.conversation_id.clone(),
        // Official CLI: file contents from --system-prompt → customSystemPrompt (field 8).
        // Claude Code Anthropic `system` maps here — not into UserMessage.text.
        custom_system_prompt: custom_system_prompt
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        requested_model: Some(proto::CursorModel {
            model_id: resolved.model_id.clone(),
            max_mode: None,
            parameters: super::model::requested_model_parameters(&resolved.model_id),
        }),
        // Server rejects exclude_workspace_context=true for many accounts/models:
        // "Workspace context exclusion is not allowed for this user, team, or selected model".
        // Only set when explicitly requested via CCP_CURSOR_EXCLUDE_WORKSPACE=1.
        exclude_workspace_context: match std::env::var("CCP_CURSOR_EXCLUDE_WORKSPACE") {
            Ok(raw)
                if matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                ) =>
            {
                Some(true)
            }
            Ok(raw)
                if matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "no" | "off"
                ) =>
            {
                Some(false)
            }
            _ => None,
        },
        harness: std::env::var("CCP_CURSOR_HARNESS")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        selected_subagent_models: vec![],
        conversation_group_id: None,
        pre_fetched_blobs,
        client_supports_inline_images: Some(true),
    }
}

/// Mid-turn reconnect: Cursor CLI sends `ResumeAction` with the latest
/// conversation checkpoint after a transport stall/disconnect (no new user text).
pub(crate) fn build_resume_run_request(
    resolved: &CursorModelResolution,
    _request_id: &str,
    continuation: &super::conversation::RunContinuation,
    mcp_tools: Option<proto::McpTools>,
) -> RunRequest {
    let pre_fetched_blobs: Vec<proto::PreFetchedBlob> = continuation
        .pre_fetched_blobs
        .iter()
        .map(|(id, value)| proto::PreFetchedBlob {
            id: id.clone(),
            value: value.clone(),
        })
        .collect();

    RunRequest {
        conversation_state: Some(continuation.conversation_state.clone()),
        action: Some(proto::Action {
            user_message_action: None,
            resume_action: Some(proto::ResumeAction {
                request_context: Some(proto::RequestContext::default()),
            }),
        }),
        model_details: Some(proto::ModelDetails {
            model_id: Some(resolved.model_id.clone()),
            display_model_id: Some(resolved.model_id.clone()),
            display_name: Some(resolved.model_id.clone()),
        }),
        mcp_tools,
        conversation_id: continuation.conversation_id.clone(),
        custom_system_prompt: None,
        requested_model: Some(proto::CursorModel {
            model_id: resolved.model_id.clone(),
            max_mode: None,
            parameters: super::model::requested_model_parameters(&resolved.model_id),
        }),
        exclude_workspace_context: match std::env::var("CCP_CURSOR_EXCLUDE_WORKSPACE") {
            Ok(raw)
                if matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                ) =>
            {
                Some(true)
            }
            Ok(raw)
                if matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "0" | "false" | "no" | "off"
                ) =>
            {
                Some(false)
            }
            _ => None,
        },
        harness: std::env::var("CCP_CURSOR_HARNESS")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        selected_subagent_models: vec![],
        conversation_group_id: None,
        pre_fetched_blobs,
        client_supports_inline_images: Some(true),
    }
}

// Error responses are expected to be small JSON/Connect diagnostics. Keep a
// hard cap so a misbehaving upstream cannot turn an HTTP failure into an
// unbounded allocation while we preserve its status/details.
const MAX_BUFFERED_ERROR_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Assemble a useful detail for a non-success buffered Run response.
///
/// Success responses retain only selected Connect frames in `body_bytes`; the
/// bounded `raw_body` copy is therefore preferred for HTTP errors, where the
/// server may return plain JSON (or a body that the Connect decoder cannot
/// frame).  Header and stream diagnostics are appended without duplicating
/// text already present.  Keeping this logic pure makes status-preservation
/// behavior easy to regression-test without a network server.
fn buffered_http_error_detail(
    status: u16,
    raw_body: &[u8],
    selected_body: &[u8],
    grpc_detail: Option<&str>,
    stream_error: Option<&str>,
    frame_count: u32,
) -> Option<String> {
    let body = if raw_body.is_empty() {
        selected_body
    } else {
        raw_body
    };
    let mut parts = Vec::<String>::new();

    if !body.is_empty() {
        if let Some(detail) = parse_error_body(body, &reqwest::header::HeaderMap::new()) {
            if !detail.trim().is_empty() {
                parts.push(detail);
            }
        } else if let Ok(text) = std::str::from_utf8(body) {
            let text = text.trim();
            if !text.is_empty() {
                parts.push(text.to_string());
            }
        }
    }
    if let Some(detail) = grpc_detail
        .map(str::trim)
        .filter(|detail| !detail.is_empty())
    {
        if !parts.iter().any(|part| part.contains(detail)) {
            parts.push(detail.to_string());
        }
    }
    if let Some(error) = stream_error
        .map(str::trim)
        .filter(|error| !error.is_empty())
    {
        if !parts.iter().any(|part| part.contains(error)) {
            parts.push(error.to_string());
        }
    }
    if parts.is_empty() {
        Some(format!(
            "HTTP {status} empty body ({} Connect frames; often a local proxy/VPN reject — e.g. Surge HTTP/1.1 464 — not a Cursor model error)",
            frame_count
        ))
    } else {
        Some(parts.join("; "))
    }
}

/// In the buffered HTTP/1 transport the initial Run is submitted through
/// `BidiAppend` before `RunSSE` is opened.  A 5xx response from that follow-up
/// subscription therefore does not prove that the Run was rejected: replaying
/// it can execute the same turn twice and provoke an active-session 503 wave.
/// Preserve deterministic recovery diagnostics (KV/conversation/image/policy)
/// and only annotate otherwise ambiguous server failures.
fn buffered_status_error(error: CursorError, http1_sse: bool) -> CursorError {
    if !http1_sse || error.status < 500 {
        return error;
    }
    let message = error.client_message();
    if cursor_error_is_kv_blob_overflow(&error)
        || cursor_connect_error_is_missing_conversation_data(&message)
        || cursor_connect_error_is_missing_image(&message)
        || crate::retry::is_policy_rate_limit(&message)
    {
        return error;
    }
    ambiguous_http1_append_error(error, "RunSSE open")
}

fn parse_error_body(body_bytes: &[u8], _headers: &reqwest::header::HeaderMap) -> Option<String> {
    if body_bytes.len() < 5 {
        return None;
    }
    if body_bytes.len() >= 5 {
        let flags = body_bytes[0];
        let len = u32::from_be_bytes([body_bytes[1], body_bytes[2], body_bytes[3], body_bytes[4]])
            as usize;
        if flags & FLAG_END != 0 && body_bytes.len() >= 5 + len {
            let payload = &body_bytes[5..5 + len];
            let err = parse_connect_error(payload);
            if err.is_some() {
                return err.map(|e| e.detail);
            }
        }
    }

    if let Ok(text) = String::from_utf8(body_bytes.to_vec())
        && !text.is_empty()
    {
        return Some(text);
    }
    None
}

/// Decode upstream response bytes into Connect frames containing
/// AgentServerMessage values.
pub fn decode_upstream_frames(body: &[u8]) -> Result<Vec<ConnectFrame>, CursorError> {
    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder
        .push(body)
        .map_err(|e| CursorError::internal(format!("frame decode: {e}")))?;
    Ok(frames)
}

/// Decode a single Connect frame payload into an AgentServerMessage.
/// Handles gzip decompression if the FLAG_GZIP bit is set.
pub fn decode_frame_payload(
    frame: &ConnectFrame,
) -> Result<proto::AgentServerMessage, CursorError> {
    // Hot path: every token delta hits this. Uncompressed frames are already
    // `Bytes` — decode in place instead of copying into a fresh `Vec` each time.
    if frame.flags & FLAG_GZIP != 0 {
        let payload = super::connect::decode_gzip_frame(&frame.payload)
            .map_err(|e| CursorError::internal(format!("gzip decompress: {e}")))?;
        return proto::AgentServerMessage::decode(payload.as_slice())
            .map_err(|e| CursorError::internal(format!("prost decode: {e}")));
    }
    proto::AgentServerMessage::decode(frame.payload.as_ref())
        .map_err(|e| CursorError::internal(format!("prost decode: {e}")))
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Buffered `/Run` uses HTTP/1 RunSSE when *this client* is pinned, not when
/// the process env happens to say so. `CursorHttpClient::new()` already applied
/// `CCP_CURSOR_HTTP1`; retry clients pass `with_prefer_http1(true)`.
pub(crate) fn buffered_run_use_http1_sse(use_bidi: bool, client_prefers_http1: bool) -> bool {
    use_bidi && client_prefers_http1
}

pub(crate) fn run_agent_should_retry_http1(
    _already_http1: bool,
    _finish_reason: &str,
    _useful: bool,
    _body_empty: bool,
) -> bool {
    false
}

pub(crate) fn run_agent_empty_body_detail(msg: &str, frame_count: u32) -> String {
    format!(
        "{msg} (0 response bytes, {frame_count} Connect frames — check Surge node / auth / CCP_CURSOR_HTTP1)"
    )
}

#[derive(Debug, Clone)]
pub struct CursorError {
    pub status: u16,
    pub message: String,
    pub detail: Option<String>,
    pub retry_after: Option<String>,
}

impl CursorError {
    pub fn new(status: u16, message: impl Into<String>, detail: Option<String>) -> Self {
        Self {
            status,
            message: message.into(),
            detail,
            retry_after: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: 502,
            message: message.into(),
            detail: None,
            retry_after: None,
        }
    }

    /// Preserve an upstream HTTP `Retry-After` value all the way to the
    /// downstream response. The raw header may be either delta-seconds or an
    /// HTTP date, so do not normalize it at the transport boundary.
    pub(crate) fn with_retry_after(mut self, retry_after: Option<String>) -> Self {
        self.retry_after = retry_after;
        self
    }

    /// Text live SSE and retry gates can classify. Direct HTTP errors keep
    /// the status in `message` ("Cursor upstream HTTP 403") and the policy
    /// body in `detail`; both must reach grok-build.
    pub fn client_message(&self) -> String {
        let mut text = format!("Cursor error {}: {}", self.status, self.message);
        if let Some(detail) = self.detail.as_deref().filter(|s| !s.is_empty())
            && !self.message.contains(detail)
        {
            text.push(' ');
            text.push_str(detail);
        }
        text
    }

    pub fn from_reqwest(e: reqwest::Error, timeout_secs: u64) -> Self {
        if e.is_timeout() {
            return Self {
                status: 504,
                message: format!("Cursor upstream timed out after {timeout_secs}s"),
                detail: Some(format!(
                    "Cursor Agent API did not finish within {timeout_secs}s. Official CLI can still work on the same node. Try: same HTTP(S)_PROXY as `~/.local/bin/agent`; CCP_CURSOR_HTTP1=0 for HTTP/2; CCP_CURSOR_TIMEOUT_SECS=600; or a different node."
                )),
                retry_after: None,
            };
        }
        if e.is_connect() {
            return Self {
                status: 502,
                message: "Cursor upstream connect failed".into(),
                detail: Some(e.to_string()),
                retry_after: None,
            };
        }
        let status = e.status().map(|s| s.as_u16()).unwrap_or(502);
        Self {
            status,
            message: e.to_string(),
            detail: Some(e.to_string()),
            retry_after: None,
        }
    }
}

pub(crate) fn retry_after_header(headers: &reqwest::header::HeaderMap) -> Option<String> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

impl std::fmt::Display for CursorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cursor error {}: {}", self.status, self.message)
    }
}

impl std::error::Error for CursorError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_header_preserves_the_upstream_value() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("120"),
        );
        assert_eq!(retry_after_header(&headers).as_deref(), Some("120"));

        headers.remove(reqwest::header::RETRY_AFTER);
        assert_eq!(retry_after_header(&headers), None);
    }

    #[test]
    fn client_message_includes_http_status_and_body_detail() {
        let err = CursorError::new(
            403,
            "Cursor upstream HTTP 403",
            Some("This model is not available in your country or region".into()),
        );
        let text = err.client_message();
        assert!(text.contains("Cursor error 403"), "{text}");
        assert!(text.contains("Cursor upstream HTTP 403"), "{text}");
        assert!(text.contains("country or region"), "{text}");
    }

    #[test]
    fn selected_image_uses_official_inline_data_field() {
        let resolved = crate::providers::cursor::model::resolve_cursor_model("claude-opus-5")
            .expect("resolve model");
        let image = CursorSelectedImage {
            data: "AAAA".into(),
            uuid: "image-uuid".into(),
            path: String::new(),
            mime_type: "image/png".into(),
        };
        let req = build_run_request("describe", &resolved, &[image], "req-image", None);
        let selected = &req
            .action
            .as_ref()
            .and_then(|action| action.user_message_action.as_ref())
            .and_then(|action| action.user_message.as_ref())
            .and_then(|message| message.selected_context.as_ref())
            .expect("selected context")
            .selected_images[0];
        assert_eq!(selected.data, vec![0, 0, 0]);
        assert!(
            selected.path.is_empty(),
            "inline images must not carry a path"
        );

        let mut encoded = Vec::new();
        selected
            .encode(&mut encoded)
            .expect("encode selected image");
        assert!(
            encoded
                .windows(5)
                .any(|window| window == [0x42, 0x03, 0, 0, 0]),
            "inline image bytes must be encoded as length-delimited protobuf field 8: {encoded:?}"
        );
    }

    #[test]
    fn refreshed_image_retry_reencodes_same_bytes_with_new_asset_identity() {
        use crate::providers::cursor::request::refresh_image_uuids;

        let resolved = crate::providers::cursor::model::resolve_cursor_model("claude-fable-5")
            .expect("resolve model");
        let original = CursorSelectedImage {
            // A tiny valid PNG prefix is enough to exercise the exact inline
            // data path; the builder decodes the canonical base64 verbatim.
            data: "iVBORw0KGgo=".into(),
            uuid: "stale-image-id".into(),
            path: String::new(),
            mime_type: "image/png".into(),
        };
        let retry = refresh_image_uuids(std::slice::from_ref(&original));
        let first = build_run_request("inspect", &resolved, &[original], "req-first", None);
        let second = build_run_request("inspect", &resolved, &retry, "req-retry", None);
        let first_image = &first
            .action
            .as_ref()
            .and_then(|action| action.user_message_action.as_ref())
            .and_then(|action| action.user_message.as_ref())
            .and_then(|message| message.selected_context.as_ref())
            .expect("first selected context")
            .selected_images[0];
        let second_image = &second
            .action
            .as_ref()
            .and_then(|action| action.user_message_action.as_ref())
            .and_then(|action| action.user_message.as_ref())
            .and_then(|message| message.selected_context.as_ref())
            .expect("retry selected context")
            .selected_images[0];
        assert_eq!(first_image.data, second_image.data);
        assert_eq!(first_image.mime_type, second_image.mime_type);
        assert!(first_image.path.is_empty() && second_image.path.is_empty());
        assert_ne!(first_image.uuid, second_image.uuid);
        assert_eq!(
            second_image.data,
            vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]
        );
    }

    #[test]
    fn buffered_run_rejects_useful_stream_without_turn_ended() {
        assert!(!buffered_stream_complete_enough_to_accept(false, false));
        assert!(buffered_stream_complete_enough_to_accept(true, false));
        assert!(buffered_stream_complete_enough_to_accept(false, true));
        assert!(
            !buffered_finish_accepts_incomplete("stream_closed", false, false, false),
            "clean EOF without turn_ended must not become end_turn"
        );
        assert!(buffered_finish_accepts_incomplete(
            "stream_closed",
            true,
            false,
            false
        ));
        assert!(buffered_finish_accepts_incomplete(
            "turn_ended",
            false,
            true,
            false
        ));
        assert!(
            !buffered_finish_accepts_incomplete("complete_idle_after_text", false, false, false),
            "idle without text is not a complete turn"
        );
        assert!(
            buffered_finish_accepts_incomplete("complete_idle_after_text", false, false, true),
            "idle after text must return the turn; 502 makes grok-build retry the same monologue"
        );
        assert!(buffered_finish_accepts_incomplete(
            "tool_call_ready",
            false,
            false,
            false
        ));
    }

    #[test]
    fn setup_idle_does_not_start_a_second_http1_run() {
        assert!(!run_agent_should_retry_http1(
            false,
            "setup_idle",
            false,
            true
        ));
        assert!(!run_agent_should_retry_http1(
            false,
            "setup_idle",
            false,
            false
        ));
        assert!(!run_agent_should_retry_http1(
            true,
            "setup_idle",
            false,
            true
        ));
        assert!(!run_agent_should_retry_http1(
            false,
            "hard_timeout",
            false,
            true
        ));
    }

    #[test]
    fn buffered_open_timeout_is_fail_closed_as_ambiguous() {
        let error = CursorError::new(
            504,
            "Cursor Agent open timed out after 30s; acceptance is ambiguous",
            None,
        );
        assert_eq!(
            crate::retry::classify_proxy_error_status(error.status, &error.client_message()),
            409,
            "a response-less open must not be replayed as a fresh Run"
        );
        assert!(!crate::retry::should_retry_upstream(
            error.status,
            &error.client_message()
        ));
    }

    #[test]
    fn buffered_http1_initial_append_uses_ambiguity_marker() {
        let error = ambiguous_http1_append_error(
            CursorError::new(502, "BidiAppend failed with HTTP 502", None),
            "initial Run",
        );
        assert!(error.message.contains("acceptance is ambiguous"));
        assert_eq!(
            crate::retry::classify_proxy_error_status(error.status, &error.client_message()),
            409
        );
    }

    #[test]
    fn buffered_http1_runsse_5xx_is_ambiguous_after_initial_append() {
        let error = buffered_status_error(
            CursorError::new(502, "Cursor upstream HTTP 502", None),
            true,
        );
        assert!(error.message.contains("acceptance is ambiguous"));
        assert_eq!(
            crate::retry::classify_proxy_error_status(error.status, &error.client_message()),
            409
        );
    }

    #[test]
    fn buffered_http1_runsse_recovery_errors_keep_their_status() {
        let kv = buffered_status_error(
            CursorError::new(
                413,
                "Cursor upstream HTTP 413",
                Some("Cursor KV blob store limit exceeded".into()),
            ),
            true,
        );
        assert!(!kv.message.contains("acceptance is ambiguous"));
        assert_eq!(kv.status, 413);

        let missing = buffered_status_error(
            CursorError::new(
                502,
                "Cursor upstream HTTP 502",
                Some("Conversation data missing".into()),
            ),
            true,
        );
        assert!(!missing.message.contains("acceptance is ambiguous"));
    }

    #[test]
    fn buffered_run_http1_follows_client_pin_not_process_env() {
        assert!(buffered_run_use_http1_sse(true, true));
        assert!(
            !buffered_run_use_http1_sse(true, false),
            "an H2 client must not take RunSSE just because CCP_CURSOR_HTTP1 might be set elsewhere"
        );
        assert!(!buffered_run_use_http1_sse(false, true));
    }

    #[test]
    fn continuation_binding_change_requires_an_expected_snapshot() {
        assert!(!continuation_binding_changed(None, Some("fresh")));
        assert!(!continuation_binding_changed(Some("same"), Some("same")));
        assert!(continuation_binding_changed(Some("old"), Some("fresh")));
        assert!(continuation_binding_changed(Some("old"), None));
    }

    #[test]
    fn buffered_http_error_detail_preserves_kv_diagnostic_after_stream_failure() {
        let raw = br#"{"error":{"message":"Request too large (413): Cursor KV blob store limit exceeded (blobs=4097)"}}"#;
        let detail =
            buffered_http_error_detail(413, raw, &[], None, Some("connection reset by peer"), 0)
                .expect("error detail");
        assert!(detail.contains("KV blob store limit exceeded"), "{detail}");
        assert!(detail.contains("connection reset by peer"), "{detail}");
    }

    #[test]
    fn buffered_http_error_detail_uses_original_status_for_classifier_input() {
        // This mirrors the status/read_err branch in `run_agent_with_session_profile`:
        // the caller constructs CursorError with the returned status rather than
        // collapsing an upstream 413 into a generic 502.
        let detail = buffered_http_error_detail(
            413,
            &[],
            &[],
            Some("Cursor KV blob store limit exceeded"),
            Some("read body: connection reset"),
            0,
        )
        .expect("error detail");
        let error = CursorError::new(413, "Cursor upstream HTTP 413", Some(detail));
        assert_eq!(error.status, 413);
        assert!(
            error
                .client_message()
                .contains("KV blob store limit exceeded")
        );
    }

    #[test]
    fn empty_setup_idle_detail_includes_frame_count() {
        let zero = run_agent_empty_body_detail("idle timeout after 45s with no useful progress", 0);
        assert!(zero.contains("0 Connect frames"), "{zero}");
        assert!(zero.contains("CCP_CURSOR_HTTP1"), "{zero}");
        let heartbeats =
            run_agent_empty_body_detail("idle timeout after 45s with no useful progress", 4);
        assert!(heartbeats.contains("4 Connect frames"), "{heartbeats}");
    }

    #[test]
    fn parse_usable_models_json_camel_case() {
        let body = r#"{
            "models": [
                {"modelId": "composer-2.5", "displayName": "Composer 2.5"},
                {"modelId": "claude-fable-5-thinking-max", "displayModelId": "fable"},
                {"displayModelId": "gpt-5.5"}
            ]
        }"#;
        let ids = parse_usable_models_json(body).unwrap();
        assert_eq!(
            ids,
            vec!["composer-2.5", "claude-fable-5-thinking-max", "gpt-5.5"]
        );
    }

    #[test]
    fn parse_usable_models_json_snake_case_and_dedupe() {
        let body = r#"{
            "models": [
                {"model_id": "composer-2.5"},
                {"model_id": "composer-2.5", "display_model_id": "Composer"},
                {"display_model_id": "  gemini-3-flash  "},
                {"model_id": ""}
            ]
        }"#;
        let ids = parse_usable_models_json(body).unwrap();
        assert_eq!(ids, vec!["composer-2.5", "gemini-3-flash"]);
    }

    #[test]
    fn parse_usable_models_json_error_envelope() {
        let body = r#"{"code":"unauthenticated","message":"not logged in"}"#;
        let err = parse_usable_models_json(body).unwrap_err();
        assert_eq!(err.message, "not logged in");
    }

    #[test]
    fn decode_usable_models_proto_raw() {
        let resp = proto::GetUsableModelsResponse {
            models: vec![
                proto::ModelDetails {
                    model_id: Some("composer-2.5".into()),
                    display_model_id: None,
                    display_name: Some("Composer".into()),
                },
                proto::ModelDetails {
                    model_id: None,
                    display_model_id: Some("gpt-5.5".into()),
                    display_name: None,
                },
            ],
        };
        let mut buf = Vec::new();
        resp.encode(&mut buf).unwrap();
        let ids = decode_usable_models_proto(&buf).unwrap();
        assert_eq!(ids, vec!["composer-2.5", "gpt-5.5"]);
    }

    #[test]
    fn resume_run_request_keeps_mcp_tools() {
        let resolved = crate::providers::cursor::model::resolve_cursor_model("fable").unwrap();
        let cont = super::super::conversation::RunContinuation {
            conversation_id: Some("conv-1".into()),
            conversation_state: vec![0x08, 0x01],
            pre_fetched_blobs: vec![],
            has_checkpoint: true,
        };
        let mcp_tools = Some(proto::McpTools {
            tools: vec![proto::McpTool {
                tool_name: "Workflow".into(),
                provider_identifier: "claude-local".into(),
                name: "Workflow".into(),
                description: "wf".into(),
                input_schema: None,
            }],
        });
        let req = build_resume_run_request(&resolved, "req-1", &cont, mcp_tools.clone());
        assert!(
            req.action
                .as_ref()
                .and_then(|action| action.resume_action.as_ref())
                .is_some()
        );
        let tools = req.mcp_tools.expect("resume must keep mcp_tools");
        assert_eq!(tools.tools.len(), 1);
        assert_eq!(tools.tools[0].name, "Workflow");
        assert_eq!(tools.tools[0].provider_identifier, "claude-local");
    }

    #[test]
    fn cursor_http_bypass_proxy_parses_truthy_flags() {
        assert!(!cursor_http_bypass_proxy(None));
        assert!(!cursor_http_bypass_proxy(Some("")));
        assert!(!cursor_http_bypass_proxy(Some("0")));
        assert!(cursor_http_bypass_proxy(Some("1")));
        assert!(cursor_http_bypass_proxy(Some("true")));
        assert!(cursor_http_bypass_proxy(Some(" YES ")));
    }

    #[test]
    fn with_prefer_http1_builds_an_http1_only_client() {
        let http1 = CursorHttpClient::with_prefer_http1(true);
        assert!(
            http1.prefers_http1(),
            "RunSSE fallback must use http1_only(), not an H2 client posting to RunSSE"
        );
        assert_eq!(
            cursor_reqwest_mode(true, false),
            CursorReqwestMode::Http1Only
        );
        let http2 = CursorHttpClient::with_prefer_http1(false);
        assert!(!http2.prefers_http1());
        assert_eq!(
            cursor_reqwest_mode(false, false),
            CursorReqwestMode::Http2Alpn
        );
        assert_eq!(
            cursor_reqwest_mode(true, true),
            CursorReqwestMode::CleartextH2PriorKnowledge
        );
    }

    async fn collect_preface(mode: CursorReqwestMode) -> Vec<u8> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 32];
            let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf)
                .await
                .unwrap_or(0);
            buf.truncate(n);
            buf
        });
        let client = apply_cursor_reqwest_mode(
            reqwest::Client::builder()
                .no_proxy()
                .connect_timeout(Duration::from_secs(2)),
            mode,
        )
        .build()
        .unwrap();
        let url = format!("http://{addr}/");
        let _ = tokio::time::timeout(Duration::from_secs(2), client.get(url).send()).await;
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server join")
            .expect("server task")
    }

    #[tokio::test]
    async fn http1_only_mode_sends_http11_not_h2_preface() {
        let preface = collect_preface(CursorReqwestMode::Http1Only).await;
        assert!(
            !preface.starts_with(b"PRI * HTTP/2.0"),
            "Http1Only must not send the HTTP/2 preface: {:?}",
            String::from_utf8_lossy(&preface)
        );
        assert!(
            preface.starts_with(b"GET "),
            "Http1Only must speak HTTP/1.1, got {:?}",
            String::from_utf8_lossy(&preface)
        );
    }

    #[tokio::test]
    async fn cleartext_h2_mode_sends_pri_preface() {
        let preface = collect_preface(CursorReqwestMode::CleartextH2PriorKnowledge).await;
        assert!(
            preface.starts_with(b"PRI * HTTP/2.0"),
            "cleartext mock path must keep h2 prior knowledge, got {:?}",
            String::from_utf8_lossy(&preface)
        );
    }
}
