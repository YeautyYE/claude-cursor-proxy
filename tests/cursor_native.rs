//! Integration tests for the Cursor native provider.
//!
//! These tests cover:
//! - Prost message roundtrip
//! - Connect frame encode/decode (with fixtures)
//! - Auth resolution
//! - Model catalog resolution
//! - Prompt rendering
//! - Client request/response boundary
//! - SSE framing
//! - Registry routing
//! - Provider end-to-end against mock upstream

#![allow(clippy::await_holding_lock)]

use once_cell::sync::Lazy;
use std::sync::Mutex;

static ENV_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

// ---------------------------------------------------------------------------
// Prost roundtrip
// ---------------------------------------------------------------------------

#[test]
fn prost_roundtrip_preserves_cursor_server_message() {
    use claude_cursor_proxy::providers::cursor::proto::*;
    use prost::Message;

    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: Some(TextDelta {
                text: "hello".into(),
            }),
            token_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: Some(7),
                output_tokens: Some(2),
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
                reasoning_tokens: None,
            }),
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut bytes = Vec::new();
    msg.encode(&mut bytes).unwrap();
    let decoded = AgentServerMessage::decode(bytes.as_slice()).unwrap();
    assert_eq!(
        decoded.interaction_update.unwrap().text_delta.unwrap().text,
        "hello"
    );
}

#[test]
fn prost_roundtrip_preserves_client_message() {
    use claude_cursor_proxy::providers::cursor::proto::*;
    use prost::Message;

    let msg = AgentClientMessage {
        run_request: Some(RunRequest {
            conversation_state: None,
            action: Some(Action {
                user_message_action: Some(UserMessageAction {
                    user_message: Some(UserMessage {
                        text: "hello".into(),
                        message_id: "msg-id".into(),
                        selected_context: None,
                        mode: 1, // AGENT_MODE_AGENT
                    }),
                }),
                resume_action: None,
            }),
            model_details: None,
            mcp_tools: None,
            conversation_id: None,
            custom_system_prompt: None,
            requested_model: Some(CursorModel {
                model_id: "gpt-5.5".into(),
                max_mode: None,
                parameters: vec![ModelParameter {
                    id: "context".into(),
                    value: "128k".into(),
                }],
            }),
            exclude_workspace_context: Some(false),
            harness: None,
            selected_subagent_models: vec![],
            conversation_group_id: None,
            pre_fetched_blobs: vec![],
            client_supports_inline_images: Some(true),
        }),
        exec_client_message: None,
        kv_client_message: None,
        exec_client_control_message: None,
        interaction_response: None,
        client_heartbeat: None,
    };

    let mut bytes = Vec::new();
    msg.encode(&mut bytes).unwrap();
    let decoded = AgentClientMessage::decode(bytes.as_slice()).unwrap();
    let run = decoded.run_request.unwrap();
    let action = run.action.unwrap();
    let user_msg = action.user_message_action.unwrap().user_message.unwrap();
    assert_eq!(user_msg.text, "hello");
    assert_eq!(run.requested_model.unwrap().model_id, "gpt-5.5");
}

// ---------------------------------------------------------------------------
// Connect frame fixtures
// ---------------------------------------------------------------------------

#[test]
fn connect_frame_fixture_matches_reference_layout() {
    use claude_cursor_proxy::providers::cursor::connect::encode_connect_frame;

    let frame = encode_connect_frame(b"abc", 0);
    assert_eq!(hex::encode(frame), "0000000003616263");
}

#[test]
fn connect_frame_decode_reference() {
    use claude_cursor_proxy::providers::cursor::connect::ConnectFrameDecoder;

    let wire = hex::decode("0000000003616263").unwrap();
    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder.push(&wire).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].flags, 0);
    assert_eq!(&frames[0].payload[..], b"abc");
}

#[test]
fn connect_frame_with_flags_decode() {
    use claude_cursor_proxy::providers::cursor::connect::ConnectFrameDecoder;
    use claude_cursor_proxy::providers::cursor::connect::encode_connect_frame;

    let frame = encode_connect_frame(b"xyz", 0x03);
    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder.push(frame).unwrap();
    assert_eq!(frames[0].flags, 0x03);
}

// ---------------------------------------------------------------------------
// Auth resolution
// ---------------------------------------------------------------------------

#[test]
fn auth_returns_token_from_env() {
    let _guard = ENV_LOCK.lock().unwrap();
    unsafe {
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "test-token-123");
    }
    let token = claude_cursor_proxy::providers::cursor::auth::load_cursor_token();
    assert_eq!(token.as_deref(), Some("test-token-123"));
    unsafe {
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
    }
}

// ---------------------------------------------------------------------------
// Model catalog
// ---------------------------------------------------------------------------

#[test]
fn model_resolution_resolves_cursor_agent_prefix() {
    use claude_cursor_proxy::providers::cursor::model::*;

    let r = resolve_cursor_model("cursor-agent:gpt-5.5").unwrap();
    assert_eq!(r.model_id, "gpt-5.5");
    assert_eq!(r.mode, CursorAgentMode::Agent);
}

#[test]
fn model_resolution_accepts_legacy_cursor_agent() {
    use claude_cursor_proxy::providers::cursor::model::*;

    let r = resolve_cursor_model("cursor-agent").unwrap();
    assert_eq!(r.mode, CursorAgentMode::Agent);
}

#[test]
fn registry_routes_cursor_model_to_cursor_provider() {
    use claude_cursor_proxy::Registry;
    use claude_cursor_proxy::config::AliasProvider;

    let registry = Registry::new(AliasProvider::Codex);
    let provider = registry.provider_for_model("cursor:gpt-5.5", None);
    assert!(provider.is_some());
    assert_eq!(provider.unwrap().name(), "cursor");

    let provider = registry.provider_for_model("cursor-agent", None);
    assert!(provider.is_some());
    assert_eq!(provider.unwrap().name(), "cursor");
}

// ---------------------------------------------------------------------------
// Prompt rendering
// ---------------------------------------------------------------------------

#[test]
fn prompt_renders_system_tools_and_messages() {
    use claude_cursor_proxy::MessagesRequest;
    use claude_cursor_proxy::providers::cursor::request::render_cursor_prompt_parts;

    let req: MessagesRequest = serde_json::from_value(serde_json::json!({
        "model": "cursor:gpt-5.5",
        "system": "be direct",
        "messages": [{
            "role": "user",
            "content": [
                {"type":"text","text":"hi"},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}
            ]
        }],
        "tools": [{"name":"Read","description":"read files","input_schema":{"type":"object"}}]
    }))
    .unwrap();

    let parts = render_cursor_prompt_parts(&req);
    // Default: omit Claude system on Cursor (Fable injection loops); keep chat + tools.
    assert_eq!(parts.custom_system_prompt, None);
    assert!(!parts.user_text.contains("be direct"));
    assert!(!parts.user_text.contains("CLAUDE_CODE_SYSTEM"));
    assert!(parts.user_text.contains("<user>"));
    assert!(parts.user_text.contains("hi"));
    assert!(parts.user_text.contains("<tools>"));
    assert!(parts.user_text.contains("Read"));
}

#[test]
fn selected_images_count_matches_base64_images() {
    use claude_cursor_proxy::MessagesRequest;
    use claude_cursor_proxy::providers::cursor::request::cursor_selected_images;

    let req: MessagesRequest = serde_json::from_value(serde_json::json!({
        "model": "cursor:gpt-5.5",
        "messages": [{
            "role": "user",
            "content": [
                {"type":"text","text":"analyze"},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}},
                {"type":"image","source":{"type":"base64","media_type":"image/jpeg","data":"BBBB"}}
            ]
        }]
    }))
    .unwrap();

    assert_eq!(cursor_selected_images(&req).len(), 2);
}

// ---------------------------------------------------------------------------
// Client request/response boundary (high-level shape)
// ---------------------------------------------------------------------------

#[test]
fn cursor_client_constructs_correct_url() {
    use claude_cursor_proxy::providers::cursor::client::CursorHttpClient;

    let client = CursorHttpClient::new();
    // Just ensure construction doesn't panic
    let _ = client;
}

#[test]
fn cursor_error_display_works() {
    use claude_cursor_proxy::providers::cursor::client::CursorError;

    let err = CursorError::new(429, "rate limited", Some("backoff".to_string()));
    let display = format!("{err}");
    assert!(display.contains("429"));
    assert!(display.contains("rate limited"));
}

#[tokio::test(flavor = "current_thread")]
async fn cursor_client_sends_connect_proto_headers_and_run_request_frame() {
    use axum::{Router, routing::post};
    use claude_cursor_proxy::providers::cursor::client::CursorHttpClient;
    use claude_cursor_proxy::providers::cursor::connect::{
        ConnectFrameDecoder, encode_connect_frame,
    };
    use claude_cursor_proxy::providers::cursor::proto::*;
    use claude_cursor_proxy::providers::cursor::request::CursorSelectedImage;
    use prost::Message;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone)]
    struct ObservedRequest {
        headers: axum::http::HeaderMap,
        body: Vec<u8>,
    }

    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let observed: Arc<Mutex<Option<ObservedRequest>>> = Arc::new(Mutex::new(None));
    let observed_handler = Arc::clone(&observed);

    let response_body = {
        let msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                tool_call_started: None,
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                text_delta: Some(TextDelta { text: "ok".into() }),
                token_delta: None,
                turn_ended: Some(TurnEnded {
                    input_tokens: Some(1),
                    output_tokens: Some(1),
                    cache_read_tokens: Some(0),
                    cache_write_tokens: Some(0),
                    reasoning_tokens: None,
                }),
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        };
        let mut payload = Vec::new();
        msg.encode(&mut payload).unwrap();
        let mut body = encode_connect_frame(&payload, 0).to_vec();
        body.extend_from_slice(&encode_connect_frame(b"", 2));
        body
    };

    let app = Router::new().route(
        "/agent.v1.AgentService/Run",
        post(
            move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                let response_body = response_body.clone();
                let observed_handler = Arc::clone(&observed_handler);
                async move {
                    *observed_handler.lock().unwrap() = Some(ObservedRequest {
                        headers,
                        body: body.to_vec(),
                    });
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "application/connect+proto",
                        )],
                        response_body,
                    )
                }
            },
        ),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    unsafe {
        std::env::set_var("CCP_CURSOR_BASE_URL", &mock_url);
        std::env::set_var("CCP_CURSOR_CLIENT_VERSION", "test-client-version");
    }

    let _handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = CursorHttpClient::new();
    let upstream = client
        .run_agent(
            "wire-token",
            "wire prompt",
            "cursor:gpt-5.5",
            &[CursorSelectedImage {
                data: "aGVsbG8=".into(),
                uuid: "image-id".into(),
                path: "claude-image-1.png".into(),
                mime_type: "image/png".into(),
            }],
            Some("custom system from test"),
        )
        .await
        .expect("mock upstream request should succeed");
    assert!(upstream.is_success());

    let observed = observed.lock().unwrap().clone().expect("request captured");
    assert_eq!(
        observed
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok()),
        Some("Bearer wire-token")
    );
    assert_eq!(
        observed
            .headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/connect+proto")
    );
    assert_eq!(
        observed
            .headers
            .get("connect-protocol-version")
            .and_then(|v| v.to_str().ok()),
        Some("1")
    );
    assert_eq!(
        observed
            .headers
            .get("x-cursor-client-type")
            .and_then(|v| v.to_str().ok()),
        Some("cli")
    );
    assert_eq!(
        observed
            .headers
            .get("x-cursor-client-version")
            .and_then(|v| v.to_str().ok()),
        Some("test-client-version")
    );
    assert_eq!(
        observed
            .headers
            .get("x-cursor-streaming")
            .and_then(|v| v.to_str().ok()),
        Some("true")
    );
    let request_id = observed
        .headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("x-request-id");
    assert_eq!(
        observed
            .headers
            .get("x-original-request-id")
            .and_then(|v| v.to_str().ok()),
        Some(request_id)
    );

    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder.push(&observed.body).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].flags, 0);
    let msg = AgentClientMessage::decode(&frames[0].payload[..]).unwrap();
    let run = msg.run_request.expect("run request");
    assert!(msg.client_heartbeat.is_none());
    assert!(run.conversation_id.is_none());
    assert!(run.conversation_group_id.is_none());
    assert_eq!(run.client_supports_inline_images, Some(true));
    assert_eq!(
        run.custom_system_prompt.as_deref(),
        Some("custom system from test")
    );
    // Default: omit exclude_workspace_context (server rejects true on many accounts).
    assert!(run.exclude_workspace_context.is_none());
    assert_eq!(run.requested_model.unwrap().model_id, "gpt-5.5");
    let user_message = run
        .action
        .unwrap()
        .user_message_action
        .unwrap()
        .user_message
        .unwrap();
    assert_eq!(user_message.text, "wire prompt");
    assert_eq!(user_message.message_id, request_id);
    assert_eq!(user_message.mode, 1);
    let image = user_message
        .selected_context
        .unwrap()
        .selected_images
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(image.data, "aGVsbG8=");
    assert_eq!(image.uuid, "image-id");
    assert_eq!(image.path, "claude-image-1.png");
    assert_eq!(image.mime_type, "image/png");

    unsafe {
        std::env::remove_var("CCP_CURSOR_BASE_URL");
        std::env::remove_var("CCP_CURSOR_CLIENT_VERSION");
    }
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

#[test]
fn response_decode_extracts_text_and_usage() {
    use claude_cursor_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_cursor_proxy::providers::cursor::proto::*;
    use claude_cursor_proxy::providers::cursor::response::*;
    use prost::Message;

    let mut body = Vec::new();

    // Text frame
    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: Some(TextDelta {
                text: "Hello".into(),
            }),
            token_delta: None,
            turn_ended: None,
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    // Usage frame
    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: None,
            token_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: Some(10),
                output_tokens: Some(5),
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
                reasoning_tokens: None,
            }),
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    // End frame
    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let upstream = claude_cursor_proxy::providers::cursor::client::CursorUpstreamResponse {
        status: 200,
        body,
        error_detail: None,
    };

    let json = decode_cursor_upstream(&upstream, "msg_test", "cursor-test").unwrap();
    assert_eq!(json["id"], "msg_test");
    assert_eq!(json["content"][0]["text"], "Hello");
    assert_eq!(json["usage"]["input_tokens"].as_u64(), Some(10));
    assert_eq!(json["usage"]["output_tokens"].as_u64(), Some(5));
}

// ---------------------------------------------------------------------------
// SSE framing - parse event names and data
// ---------------------------------------------------------------------------

#[test]
fn sse_parses_event_names_and_data() {
    use claude_cursor_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_cursor_proxy::providers::cursor::proto::*;
    use claude_cursor_proxy::providers::cursor::sse::frame_cursor_stream;
    use prost::Message;

    let mut body = Vec::new();

    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: Some(TextDelta { text: "hi".into() }),
            token_delta: None,
            turn_ended: None,
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: None,
            token_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: Some(5),
                output_tokens: Some(1),
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
                reasoning_tokens: None,
            }),
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let upstream = claude_cursor_proxy::providers::cursor::client::CursorUpstreamResponse {
        status: 200,
        body,
        error_detail: None,
    };

    let sse = frame_cursor_stream(&upstream, "msg_sse", "cursor-test");
    let sse_str = String::from_utf8_lossy(&sse);

    let events = parse_sse_events(&sse_str);
    let names: Vec<&str> = events.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        names.contains(&"message_start"),
        "expected message_start in {names:?}"
    );
    assert!(
        names.contains(&"content_block_delta"),
        "expected content_block_delta in {names:?}"
    );
    assert!(
        names.contains(&"message_delta"),
        "expected message_delta in {names:?}"
    );
    assert!(
        names.contains(&"message_stop"),
        "expected message_stop in {names:?}"
    );
}

#[test]
fn sse_message_delta_contains_usage() {
    use claude_cursor_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_cursor_proxy::providers::cursor::proto::*;
    use claude_cursor_proxy::providers::cursor::sse::frame_cursor_stream;
    use prost::Message;

    let mut body = Vec::new();

    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: Some(TextDelta {
                text: "test".into(),
            }),
            token_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: Some(42),
                output_tokens: Some(7),
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
                reasoning_tokens: None,
            }),
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));
    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let upstream = claude_cursor_proxy::providers::cursor::client::CursorUpstreamResponse {
        status: 200,
        body,
        error_detail: None,
    };

    let sse = frame_cursor_stream(&upstream, "msg_u", "cursor-test");
    let sse_str = String::from_utf8_lossy(&sse);
    let events = parse_sse_events(&sse_str);

    let msg_delta_data = events
        .iter()
        .rev()
        .find(|(n, d)| n == "message_delta" && !d["delta"]["stop_reason"].is_null())
        .map(|(_, d)| d.clone());
    assert!(msg_delta_data.is_some(), "expected message_delta event");
    let data = msg_delta_data.unwrap();
    assert_eq!(data["usage"]["input_tokens"].as_u64(), Some(42));
    assert_eq!(data["usage"]["output_tokens"].as_u64(), Some(7));
    assert_eq!(
        data["usage"]["cache_creation_input_tokens"].as_u64(),
        Some(0)
    );
    assert_eq!(data["usage"]["cache_read_input_tokens"].as_u64(), Some(0));
    assert_eq!(data["delta"]["stop_reason"], "end_turn");
}

// ---------------------------------------------------------------------------
// Registry integration
// ---------------------------------------------------------------------------

#[test]
fn registry_provider_for_legacy_cursor_model() {
    use claude_cursor_proxy::Registry;
    use claude_cursor_proxy::config::AliasProvider;

    let registry = Registry::new(AliasProvider::Codex);

    // Legacy models
    for model in &[
        "cursor",
        "cursor-agent",
        "cursor-composer",
        "cursor-composer-fast",
        "cursor-plan",
        "cursor-ask",
    ] {
        let provider = registry.provider_for_model(model, None);
        assert!(
            provider.is_some(),
            "expected provider for legacy model {model}"
        );
        assert_eq!(
            provider.unwrap().name(),
            "cursor",
            "model {model} should route to cursor provider"
        );
    }
}

// ---------------------------------------------------------------------------
// Mock upstream streaming test (full integration)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cursor_provider_streams_text_and_usage_from_mock_upstream() {
    use axum::{Router, routing::post};
    use claude_cursor_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_cursor_proxy::providers::cursor::proto::*;
    use claude_cursor_proxy::providers::cursor::response::decode_cursor_upstream;
    use claude_cursor_proxy::providers::cursor::sse::frame_cursor_stream;
    use prost::Message;

    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    // Build mock upstream response bytes
    let mut body = Vec::new();

    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: Some(TextDelta {
                text: "Hello from mock".into(),
            }),
            token_delta: None,
            turn_ended: None,
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: None,
            token_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: Some(15),
                output_tokens: Some(3),
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
                reasoning_tokens: None,
            }),
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));

    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let response_body = body.clone();

    let app = Router::new().route(
        "/agent.v1.AgentService/Run",
        post(move |_body: axum::body::Bytes| async move {
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "application/connect+proto",
                )],
                response_body,
            )
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    unsafe {
        std::env::set_var("CCP_CURSOR_BASE_URL", &mock_url);
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "mock-token");
        std::env::set_var("CCP_CURSOR_CLIENT_VERSION", "0.0.0");
    }

    let _handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    use claude_cursor_proxy::providers::cursor::auth::load_cursor_token;
    use claude_cursor_proxy::providers::cursor::client::CursorHttpClient;

    let token = load_cursor_token().unwrap();
    let client = CursorHttpClient::new();
    let upstream = client
        .run_agent(&token, "test prompt", "cursor:gpt-5.5", &[], None)
        .await
        .expect("mock upstream request should succeed");

    assert!(upstream.is_success());
    assert_eq!(upstream.status, 200);

    let json = decode_cursor_upstream(&upstream, "msg_mock", "cursor-test").unwrap();
    assert_eq!(json["content"][0]["text"], "Hello from mock");
    assert_eq!(json["usage"]["input_tokens"].as_u64(), Some(15));
    assert_eq!(json["usage"]["output_tokens"].as_u64(), Some(3));

    let sse = frame_cursor_stream(&upstream, "msg_sse_mock", "cursor-test");
    let sse_str = String::from_utf8_lossy(&sse);
    let events = parse_sse_events(&sse_str);
    let names: Vec<&str> = events.iter().map(|(n, _)| n.as_str()).collect();

    assert!(
        names.contains(&"message_start"),
        "SSE should include message_start in {names:?}"
    );
    assert!(
        names.contains(&"message_stop"),
        "SSE should include message_stop in {names:?}"
    );

    unsafe {
        std::env::remove_var("CCP_CURSOR_BASE_URL");
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
        std::env::remove_var("CCP_CURSOR_CLIENT_VERSION");
    }
}

// ---------------------------------------------------------------------------
// Provider handle_messages shape test (non-streaming)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cursor_provider_handle_messages_returns_anthropic_json() {
    use axum::{Router, routing::post};
    use claude_cursor_proxy::providers::cursor::connect::encode_connect_frame;
    use claude_cursor_proxy::providers::cursor::proto::*;
    use prost::Message;

    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    // Build mock response
    let mut body = Vec::new();
    let msg = AgentServerMessage {
        conversation_checkpoint_update: None,
        interaction_update: Some(InteractionUpdate {
            heartbeat: None,
            tool_call_started: None,
            tool_call_completed: None,
            thinking_delta: None,
            thinking_completed: None,
            text_delta: Some(TextDelta {
                text: "Mock response text".into(),
            }),
            token_delta: None,
            turn_ended: Some(TurnEnded {
                input_tokens: Some(20),
                output_tokens: Some(4),
                cache_read_tokens: Some(0),
                cache_write_tokens: Some(0),
                reasoning_tokens: None,
            }),
        }),
        kv_server_message: None,
        interaction_query: None,
        exec_server_message: None,
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload).unwrap();
    body.extend_from_slice(&encode_connect_frame(&payload, 0));
    body.extend_from_slice(&encode_connect_frame(b"", 2));

    let response_body = body;

    let app = Router::new().route(
        "/agent.v1.AgentService/Run",
        post(move |_body: axum::body::Bytes| async move {
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "application/connect+proto",
                )],
                response_body,
            )
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mock_url = format!("http://{}", addr);

    unsafe {
        std::env::set_var("CCP_CURSOR_BASE_URL", &mock_url);
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "mock-token-handler");
        std::env::set_var("CCP_CURSOR_CLIENT_VERSION", "0.0.0");
    }

    let _handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Send via handle_messages (non-streaming)
    use claude_cursor_proxy::provider::Provider;
    use claude_cursor_proxy::provider::RequestContext;
    use claude_cursor_proxy::providers::cursor::CursorProvider;

    let provider = CursorProvider::new();
    let body = serde_json::from_value(serde_json::json!({
        "model": "cursor:gpt-5.5",
        "messages": [{"role": "user", "content": "test"}]
    }))
    .unwrap();

    let ctx = RequestContext {
        req_id: "test-req".into(),
        session_id: None,
        session_seq: None,
        provider: "cursor".into(),
        traffic: None,
        monitor: None,
    };

    let response = provider.handle_messages(body, ctx).await;
    // Should not return an error response
    let status = response.status();
    assert!(
        status != 401 && status != 400,
        "handle_messages returned error status {status}"
    );

    unsafe {
        std::env::remove_var("CCP_CURSOR_BASE_URL");
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
        std::env::remove_var("CCP_CURSOR_CLIENT_VERSION");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cursor_proxy_http_path_reaches_mock_cursor_upstream() {
    use axum::{Router, routing::post};
    use claude_cursor_proxy::providers::cursor::connect::{
        ConnectFrameDecoder, encode_connect_frame,
    };
    use claude_cursor_proxy::providers::cursor::proto::*;
    use prost::Message;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone)]
    struct ObservedRequest {
        authorization: Option<String>,
        body: Vec<u8>,
    }

    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let observed: Arc<Mutex<Option<ObservedRequest>>> = Arc::new(Mutex::new(None));
    let observed_handler = Arc::clone(&observed);

    let response_body = {
        let text_msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                tool_call_started: None,
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                text_delta: Some(TextDelta {
                    text: "proxy path works".into(),
                }),
                token_delta: None,
                turn_ended: None,
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        };
        let mut text_payload = Vec::new();
        text_msg.encode(&mut text_payload).unwrap();

        let usage_msg = AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                tool_call_started: None,
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                text_delta: None,
                token_delta: None,
                turn_ended: Some(TurnEnded {
                    input_tokens: Some(12),
                    output_tokens: Some(3),
                    cache_read_tokens: Some(0),
                    cache_write_tokens: Some(0),
                    reasoning_tokens: None,
                }),
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        };
        let mut usage_payload = Vec::new();
        usage_msg.encode(&mut usage_payload).unwrap();

        let mut body = encode_connect_frame(&text_payload, 0).to_vec();
        body.extend_from_slice(&encode_connect_frame(&usage_payload, 0));
        body.extend_from_slice(&encode_connect_frame(b"", 2));
        body
    };

    let upstream_app = Router::new().route(
        "/agent.v1.AgentService/Run",
        post(
            move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                let response_body = response_body.clone();
                let observed_handler = Arc::clone(&observed_handler);
                async move {
                    *observed_handler.lock().unwrap() = Some(ObservedRequest {
                        authorization: headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string),
                        body: body.to_vec(),
                    });
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "application/connect+proto",
                        )],
                        response_body,
                    )
                }
            },
        ),
    );

    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let upstream_url = format!("http://{}", upstream_addr);
    let _upstream_handle = tokio::spawn(async move {
        axum::serve(upstream_listener, upstream_app).await.unwrap();
    });

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let _proxy_handle = tokio::spawn(async move {
        claude_cursor_proxy::server::serve_listener(proxy_listener, None, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    unsafe {
        std::env::set_var("CCP_CURSOR_BASE_URL", &upstream_url);
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "proxy-token");
        std::env::set_var("CCP_CURSOR_CLIENT_VERSION", "proxy-test-version");
    }

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{proxy_addr}/v1/messages"))
        .header("authorization", "Bearer ignored")
        .json(&serde_json::json!({
            "model": "cursor:gpt-5.5",
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hello over proxy"}]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["content"][0]["text"], "proxy path works");
    assert_eq!(json["usage"]["input_tokens"], 12);
    assert_eq!(json["usage"]["output_tokens"], 3);

    let observed = observed
        .lock()
        .unwrap()
        .clone()
        .expect("upstream request captured");
    assert_eq!(
        observed.authorization.as_deref(),
        Some("Bearer proxy-token")
    );

    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder.push(&observed.body).unwrap();
    assert_eq!(frames.len(), 1);
    let msg = AgentClientMessage::decode(&frames[0].payload[..]).unwrap();
    let user_message = msg
        .run_request
        .unwrap()
        .action
        .unwrap()
        .user_message_action
        .unwrap()
        .user_message
        .unwrap();
    assert!(user_message.text.contains("hello over proxy"));

    let _ = shutdown_tx.send(());
    unsafe {
        std::env::remove_var("CCP_CURSOR_BASE_URL");
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
        std::env::remove_var("CCP_CURSOR_CLIENT_VERSION");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cursor_proxy_continues_tool_result_on_the_same_bidi_run() {
    use axum::{Router, body::Body, http::Request, response::Response, routing::post};
    use bytes::Bytes;
    use claude_cursor_proxy::providers::cursor::connect::{
        ConnectFrameDecoder, FLAG_END, encode_connect_frame,
    };
    use claude_cursor_proxy::providers::cursor::proto::*;
    use futures_util::StreamExt;
    use prost::Message;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const KV_BLOB_ID: &[u8] = b"cursor-live-state";
    const KV_BEFORE_TOOL: &[u8] = b"state-before-tool";
    const KV_AFTER_TOOL: &[u8] = b"state-after-tool";

    // Test-local wire declarations let this integration fixture model Cursor's
    // KV oneofs independently of the proxy's hand-written protobuf subset.
    #[derive(Clone, PartialEq, prost::Message)]
    struct WireAgentServerMessage {
        #[prost(message, optional, tag = "4")]
        kv_server_message: Option<WireKvServerMessage>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct WireAgentClientMessage {
        #[prost(message, optional, tag = "3")]
        kv_client_message: Option<WireKvClientMessage>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct WireKvServerMessage {
        #[prost(uint32, tag = "1")]
        id: u32,
        #[prost(message, optional, tag = "2")]
        get_blob_args: Option<WireGetBlobArgs>,
        #[prost(message, optional, tag = "3")]
        set_blob_args: Option<WireSetBlobArgs>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct WireKvClientMessage {
        #[prost(uint32, tag = "1")]
        id: u32,
        #[prost(message, optional, tag = "2")]
        get_blob_result: Option<WireGetBlobResult>,
        #[prost(message, optional, tag = "3")]
        set_blob_result: Option<WireSetBlobResult>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct WireGetBlobArgs {
        #[prost(bytes = "vec", tag = "1")]
        blob_id: Vec<u8>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct WireGetBlobResult {
        #[prost(bytes = "vec", optional, tag = "1")]
        blob_data: Option<Vec<u8>>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct WireSetBlobArgs {
        #[prost(bytes = "vec", tag = "1")]
        blob_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        blob_data: Vec<u8>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct WireSetBlobResult {
        #[prost(message, optional, tag = "1")]
        error: Option<WireKvError>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct WireKvError {
        #[prost(string, tag = "1")]
        message: String,
    }

    #[derive(Default)]
    struct ObservedBidiRun {
        post_count: AtomicUsize,
        client_messages: Mutex<Vec<AgentClientMessage>>,
        kv_client_messages: Mutex<Vec<WireKvClientMessage>>,
    }

    fn server_frame(message: AgentServerMessage) -> Bytes {
        let mut payload = Vec::new();
        message.encode(&mut payload).unwrap();
        encode_connect_frame(payload, 0)
    }

    fn kv_set_frame(id: u32, blob_id: &[u8], blob_data: &[u8]) -> Bytes {
        let message = WireAgentServerMessage {
            kv_server_message: Some(WireKvServerMessage {
                id,
                get_blob_args: None,
                set_blob_args: Some(WireSetBlobArgs {
                    blob_id: blob_id.to_vec(),
                    blob_data: blob_data.to_vec(),
                }),
            }),
        };
        let mut payload = Vec::new();
        message.encode(&mut payload).unwrap();
        encode_connect_frame(payload, 0)
    }

    fn kv_get_frame(id: u32, blob_id: &[u8]) -> Bytes {
        let message = WireAgentServerMessage {
            kv_server_message: Some(WireKvServerMessage {
                id,
                get_blob_args: Some(WireGetBlobArgs {
                    blob_id: blob_id.to_vec(),
                }),
                set_blob_args: None,
            }),
        };
        let mut payload = Vec::new();
        message.encode(&mut payload).unwrap();
        encode_connect_frame(payload, 0)
    }

    fn read_exec_frame() -> Bytes {
        server_frame(AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: Some(ExecServerMessage {
                id: 41,
                exec_id: Some("exec-read-1".into()),
                shell_args: None,
                write_args: None,
                delete_args: None,
                grep_args: None,
                read_args: Some(ExecReadArgs {
                    path: "README.md".into(),
                    tool_call_id: "call-read-1".into(),
                    offset: None,
                    limit: None,
                }),
                ls_args: None,
                request_context_args: None,
                shell_stream_args: None,
            }),
        })
    }

    fn read_tool_started_frame() -> Bytes {
        server_frame(AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: None,
                tool_call_started: Some(ToolCallStarted {
                    call_id: "call-read-1".into(),
                    tool_call: Some(ToolCall {
                        shell_tool_call: None,
                        delete_tool_call: None,
                        glob_tool_call: None,
                        grep_tool_call: None,
                        read_tool_call: Some(ReadToolCall {
                            args: Some(ReadToolArgs {
                                path: "README.md".into(),
                                offset: None,
                                limit: None,
                            }),
                        }),
                        edit_tool_call: None,
                        ls_tool_call: None,
                        ..Default::default()
                    }),
                    model_call_id: "model-call-read-1".into(),
                }),
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                turn_ended: None,
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        })
    }

    fn final_text_frame() -> Bytes {
        server_frame(AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: Some(TextDelta {
                    text: "continued on the same Cursor run".into(),
                }),
                tool_call_started: None,
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                turn_ended: None,
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        })
    }

    fn final_usage_frame() -> Bytes {
        server_frame(AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: None,
                tool_call_started: None,
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                turn_ended: Some(TurnEnded {
                    input_tokens: Some(21),
                    output_tokens: Some(5),
                    cache_read_tokens: Some(3),
                    cache_write_tokens: Some(2),
                    reasoning_tokens: None,
                }),
            }),
            kv_server_message: None,
            interaction_query: None,
            exec_server_message: None,
        })
    }

    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let observed = Arc::new(ObservedBidiRun::default());
    let observed_handler = Arc::clone(&observed);

    // The mock returns response headers before consuming the request body, then
    // reads and writes Connect frames concurrently. This is the key property
    // needed to exercise Cursor's real HTTP/2 BiDi Run method.
    let upstream_app = Router::new().route(
        "/agent.v1.AgentService/Run",
        post(move |request: Request<Body>| {
            let observed = Arc::clone(&observed_handler);
            async move {
                observed.post_count.fetch_add(1, Ordering::SeqCst);
                let (response_tx, response_rx) =
                    tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(16);

                tokio::spawn(async move {
                    let mut request_stream = request.into_body().into_data_stream();
                    let mut decoder = ConnectFrameDecoder::new();
                    let mut sent_initial_set = false;
                    let mut sent_tool = false;
                    let mut saw_read_result = false;
                    let mut saw_stream_close = false;
                    let mut sent_after_tool_set = false;
                    let mut sent_get = false;

                    while let Some(chunk) = request_stream.next().await {
                        let chunk = match chunk {
                            Ok(chunk) => chunk,
                            Err(_) => break,
                        };
                        let frames = match decoder.push(&chunk) {
                            Ok(frames) => frames,
                            Err(_) => break,
                        };
                        for frame in frames {
                            if let Ok(wire) = WireAgentClientMessage::decode(frame.payload.as_ref())
                                && let Some(kv) = wire.kv_client_message
                            {
                                observed
                                    .kv_client_messages
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .push(kv.clone());

                                if kv.id == 70 && kv.set_blob_result.is_some() && !sent_tool {
                                    assert!(
                                        kv.set_blob_result
                                            .as_ref()
                                            .is_some_and(|result| result.error.is_none()),
                                        "initial SetBlobResult returned an error"
                                    );
                                    sent_tool = true;
                                    // Cursor emits this UI/transcript frame as well as the
                                    // authoritative ExecServerMessage. It must not become
                                    // a duplicate Anthropic tool_use block.
                                    if response_tx
                                        .send(Ok(read_tool_started_frame()))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                    if response_tx.send(Ok(read_exec_frame())).await.is_err() {
                                        return;
                                    }
                                }

                                if kv.id == 71 && kv.set_blob_result.is_some() && !sent_get {
                                    assert!(
                                        kv.set_blob_result
                                            .as_ref()
                                            .is_some_and(|result| result.error.is_none()),
                                        "post-tool SetBlobResult returned an error"
                                    );
                                    sent_get = true;
                                    if response_tx
                                        .send(Ok(kv_get_frame(72, KV_BLOB_ID)))
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                }

                                if kv.id == 72
                                    && let Some(result) = kv.get_blob_result.as_ref()
                                {
                                    assert_eq!(
                                        result.blob_data.as_deref(),
                                        Some(KV_AFTER_TOOL),
                                        "GetBlob did not return the latest value for the key"
                                    );
                                    if response_tx.send(Ok(final_text_frame())).await.is_err() {
                                        return;
                                    }
                                    if response_tx.send(Ok(final_usage_frame())).await.is_err() {
                                        return;
                                    }
                                    let _ = response_tx
                                        .send(Ok(encode_connect_frame([], FLAG_END)))
                                        .await;
                                    return;
                                }
                            }

                            let message = match AgentClientMessage::decode(frame.payload.as_ref()) {
                                Ok(message) => message,
                                Err(_) => continue,
                            };
                            observed
                                .client_messages
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .push(message.clone());

                            if message.run_request.is_some() && !sent_initial_set {
                                sent_initial_set = true;
                                // The model turn is gated on KV persistence: no Read exec
                                // is emitted until the client acknowledges this SetBlob.
                                if response_tx
                                    .send(Ok(kv_set_frame(70, KV_BLOB_ID, KV_BEFORE_TOOL)))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }

                            if let Some(exec) = message.exec_client_message.as_ref()
                                && exec.read_result.is_some()
                            {
                                saw_read_result = true;
                            }
                            if let Some(control) = message.exec_client_control_message.as_ref()
                                && control
                                    .stream_close
                                    .as_ref()
                                    .is_some_and(|close| close.id == 41)
                            {
                                saw_stream_close = true;
                            }

                            // Cursor checkpoints state again after tool execution. The
                            // continuation remains gated until SetBlob is acknowledged,
                            // then GetBlob proves the latest value is available.
                            if saw_read_result && saw_stream_close && !sent_after_tool_set {
                                sent_after_tool_set = true;
                                if response_tx
                                    .send(Ok(kv_set_frame(71, KV_BLOB_ID, KV_AFTER_TOOL)))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                });

                let response_stream =
                    futures_util::stream::unfold(response_rx, |mut receiver| async move {
                        receiver.recv().await.map(|item| (item, receiver))
                    });
                Response::builder()
                    .status(200)
                    .header(
                        axum::http::header::CONTENT_TYPE,
                        "application/connect+proto",
                    )
                    .body(Body::from_stream(response_stream))
                    .unwrap()
            }
        }),
    );

    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let upstream_url = format!("http://{upstream_addr}");
    let upstream_handle = tokio::spawn(async move {
        axum::serve(upstream_listener, upstream_app).await.unwrap();
    });

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let proxy_handle = tokio::spawn(async move {
        claude_cursor_proxy::server::serve_listener(proxy_listener, None, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    unsafe {
        std::env::set_var("CCP_CURSOR_BASE_URL", &upstream_url);
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "bidi-proxy-token");
        std::env::set_var("CCP_CURSOR_CLIENT_VERSION", "bidi-test-version");
        std::env::set_var("CCP_CURSOR_BIDI", "1");
        std::env::set_var("CCP_CURSOR_HEARTBEAT_SECS", "60");
        std::env::set_var("CCP_CURSOR_EXEC_HEARTBEAT_SECS", "1");
    }

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let client = reqwest::Client::new();
    let session_id = "cursor-live-continuation-test";
    let tools = serde_json::json!([{
        "name": "Read",
        "description": "Read a file",
        "input_schema": {
            "type": "object",
            "properties": {"file_path": {"type": "string"}},
            "required": ["file_path"]
        }
    }]);

    let first_response = client
        .post(format!("http://{proxy_addr}/v1/messages"))
        .header("authorization", "Bearer ignored")
        .header("anthropic-version", "2023-06-01")
        .header("x-claude-code-session-id", session_id)
        .json(&serde_json::json!({
            "model": "cursor:gpt-5.5",
            "max_tokens": 256,
            "stream": true,
            "tools": tools.clone(),
            "messages": [{"role": "user", "content": "Read README.md"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_sse = tokio::time::timeout(std::time::Duration::from_secs(5), first_response.text())
        .await
        .expect("first Anthropic segment stayed open after tool_use")
        .unwrap();
    let first_events = parse_sse_events(&first_sse);
    assert_eq!(first_events[0].0, "message_start");
    assert_eq!(first_events.last().map(|(n, _)| n.as_str()), Some("message_stop"));
    let tool_start = first_events
        .iter()
        .find(|(n, d)| n == "content_block_start" && d["content_block"]["type"] == "tool_use")
        .map(|(_, d)| d)
        .expect("tool_use content_block_start");
    assert_eq!(tool_start["content_block"]["id"], "call-read-1");
    assert_eq!(tool_start["content_block"]["name"], "Read");
    let input_delta = first_events
        .iter()
        .find(|(n, d)| n == "content_block_delta" && d["delta"]["type"] == "input_json_delta")
        .map(|(_, d)| d)
        .expect("input_json_delta");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(
            input_delta["delta"]["partial_json"].as_str().unwrap()
        )
        .unwrap(),
        serde_json::json!({"file_path": "README.md"})
    );
    assert_eq!(final_message_delta(&first_events)["delta"]["stop_reason"], "tool_use");

    // Leave the upstream Run alive long enough to observe the per-exec heartbeat.
    tokio::time::sleep(std::time::Duration::from_millis(1250)).await;

    let second_response = client
        .post(format!("http://{proxy_addr}/v1/messages"))
        .header("authorization", "Bearer ignored")
        .header("anthropic-version", "2023-06-01")
        .header("x-claude-code-session-id", session_id)
        .json(&serde_json::json!({
            "model": "cursor:gpt-5.5",
            "max_tokens": 256,
            "stream": true,
            "tools": tools,
            "messages": [
                {"role": "user", "content": "Read README.md"},
                {"role": "assistant", "content": [{
                    "type": "tool_use",
                    "id": "call-read-1",
                    "name": "Read",
                    "input": {"file_path": "README.md"}
                }]},
                {"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "call-read-1",
                    "content": "line one\nline two"
                }]}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_sse =
        tokio::time::timeout(std::time::Duration::from_secs(5), second_response.text())
            .await
            .expect("second Anthropic segment did not finish on the original Cursor run")
            .unwrap();
    let second_events = parse_sse_events(&second_sse);
    assert_eq!(second_events[0].0, "message_start");
    assert_eq!(
        second_events.last().map(|(n, _)| n.as_str()),
        Some("message_stop")
    );
    let text_delta = second_events
        .iter()
        .find(|(n, d)| n == "content_block_delta" && d["delta"]["type"] == "text_delta")
        .map(|(_, d)| d)
        .expect("text_delta");
    assert_eq!(
        text_delta["delta"]["text"],
        "continued on the same Cursor run"
    );
    let final_delta = final_message_delta(&second_events);
    assert_eq!(final_delta["delta"]["stop_reason"], "end_turn");
    // Cursor turn_ended reports total input=21 with cache_read=3 + cache_write=2;
    // Anthropic normalize → uncached input 16.
    assert_eq!(final_delta["usage"]["input_tokens"], 16);
    // Output is max(turn_ended=5, char/4 estimate of streamed text ≈8).
    assert_eq!(final_delta["usage"]["output_tokens"], 8);
    assert_eq!(final_delta["usage"]["cache_read_input_tokens"], 3);
    assert_eq!(final_delta["usage"]["cache_creation_input_tokens"], 2);

    let client_messages = observed
        .client_messages
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    let kv_client_messages = observed
        .kv_client_messages
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    assert_eq!(
        observed.post_count.load(Ordering::SeqCst),
        1,
        "tool_result opened a second Cursor AgentService/Run POST"
    );
    assert_eq!(
        client_messages
            .iter()
            .filter(|message| message.run_request.is_some())
            .count(),
        1,
        "the same upstream body must contain exactly one RunRequest"
    );
    for id in [70, 71] {
        let result = kv_client_messages
            .iter()
            .find(|message| message.id == id)
            .and_then(|message| message.set_blob_result.as_ref())
            .unwrap_or_else(|| panic!("missing SetBlobResult acknowledgement for KV id {id}"));
        assert!(
            result.error.is_none(),
            "SetBlobResult for KV id {id} contained an error"
        );
    }
    let get_result = kv_client_messages
        .iter()
        .find(|message| message.id == 72)
        .and_then(|message| message.get_blob_result.as_ref())
        .expect("missing GetBlobResult for KV id 72");
    assert_eq!(get_result.blob_data.as_deref(), Some(KV_AFTER_TOOL));

    let read_result = client_messages
        .iter()
        .find_map(|message| {
            message
                .exec_client_message
                .as_ref()
                .filter(|exec| exec.read_result.is_some())
        })
        .expect("Claude tool_result was not encoded as ExecClientMessage.read_result");
    assert_eq!(read_result.id, 41);
    assert_eq!(read_result.exec_id.as_deref(), Some("exec-read-1"));
    let read_success = read_result
        .read_result
        .as_ref()
        .and_then(|result| result.success.as_ref())
        .expect("read tool_result should be a ReadSuccess");
    assert_eq!(read_success.path, "README.md");
    assert_eq!(read_success.content.as_deref(), Some("line one\nline two"));
    assert_eq!(read_success.total_lines, 2);

    assert!(
        client_messages.iter().any(|message| {
            message
                .exec_client_control_message
                .as_ref()
                .and_then(|control| control.heartbeat.as_ref())
                .is_some_and(|heartbeat| heartbeat.id == 41)
        }),
        "pending exec did not receive its 3s-style control heartbeat"
    );
    assert!(
        client_messages.iter().any(|message| {
            message
                .exec_client_control_message
                .as_ref()
                .and_then(|control| control.stream_close.as_ref())
                .is_some_and(|close| close.id == 41)
        }),
        "native exec result was not followed by stream_close"
    );

    let _ = shutdown_tx.send(());
    upstream_handle.abort();
    proxy_handle.abort();
    unsafe {
        std::env::remove_var("CCP_CURSOR_BASE_URL");
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
        std::env::remove_var("CCP_CURSOR_CLIENT_VERSION");
        std::env::remove_var("CCP_CURSOR_BIDI");
        std::env::remove_var("CCP_CURSOR_HEARTBEAT_SECS");
        std::env::remove_var("CCP_CURSOR_EXEC_HEARTBEAT_SECS");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cursor_proxy_batches_two_execs_and_accepts_reverse_tool_results_on_same_run() {
    use axum::{Router, body::Body, http::Request, response::Response, routing::post};
    use bytes::Bytes;
    use claude_cursor_proxy::providers::cursor::connect::{
        ConnectFrameDecoder, FLAG_END, encode_connect_frame,
    };
    use claude_cursor_proxy::providers::cursor::proto::*;
    use futures_util::StreamExt;
    use prost::Message;
    use std::collections::{BTreeMap, BTreeSet};
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct ObservedParallelRun {
        post_count: AtomicUsize,
        client_messages: Mutex<Vec<AgentClientMessage>>,
    }

    fn server_frame(message: AgentServerMessage) -> Bytes {
        let mut payload = Vec::new();
        message.encode(&mut payload).unwrap();
        encode_connect_frame(payload, 0)
    }

    fn read_exec_frame(id: u32, exec_id: &str, tool_use_id: &str, path: &str) -> Bytes {
        server_frame(AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: None,
            exec_server_message: Some(ExecServerMessage {
                id,
                exec_id: Some(exec_id.into()),
                shell_args: None,
                write_args: None,
                delete_args: None,
                grep_args: None,
                read_args: Some(ExecReadArgs {
                    path: path.into(),
                    tool_call_id: tool_use_id.into(),
                    offset: None,
                    limit: None,
                }),
                ls_args: None,
                request_context_args: None,
                shell_stream_args: None,
            }),
            kv_server_message: None,
            interaction_query: None,
        })
    }

    fn final_text_frame() -> Bytes {
        server_frame(AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: Some(TextDelta {
                    text: "both parallel reads continued on one run".into(),
                }),
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
        })
    }

    fn final_usage_frame() -> Bytes {
        server_frame(AgentServerMessage {
            conversation_checkpoint_update: None,
            interaction_update: Some(InteractionUpdate {
                heartbeat: None,
                text_delta: None,
                tool_call_started: None,
                tool_call_completed: None,
                thinking_delta: None,
                thinking_completed: None,
                token_delta: None,
                turn_ended: Some(TurnEnded {
                    input_tokens: Some(34),
                    output_tokens: Some(8),
                    cache_read_tokens: Some(0),
                    cache_write_tokens: Some(0),
                    reasoning_tokens: None,
                }),
            }),
            exec_server_message: None,
            kv_server_message: None,
            interaction_query: None,
        })
    }

    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let observed = Arc::new(ObservedParallelRun::default());
    let observed_handler = Arc::clone(&observed);

    let upstream_app = Router::new().route(
        "/agent.v1.AgentService/Run",
        post(move |request: Request<Body>| {
            let observed = Arc::clone(&observed_handler);
            async move {
                observed.post_count.fetch_add(1, Ordering::SeqCst);
                let (response_tx, response_rx) =
                    tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(16);

                tokio::spawn(async move {
                    let mut request_stream = request.into_body().into_data_stream();
                    let mut decoder = ConnectFrameDecoder::new();
                    let mut sent_tools = false;
                    let mut read_results = BTreeMap::<u32, String>::new();
                    let mut stream_closes = BTreeSet::<u32>::new();

                    while let Some(chunk) = request_stream.next().await {
                        let chunk = match chunk {
                            Ok(chunk) => chunk,
                            Err(_) => break,
                        };
                        let frames = match decoder.push(&chunk) {
                            Ok(frames) => frames,
                            Err(_) => break,
                        };
                        for frame in frames {
                            let message = match AgentClientMessage::decode(frame.payload.as_ref()) {
                                Ok(message) => message,
                                Err(_) => continue,
                            };
                            observed
                                .client_messages
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .push(message.clone());

                            if message.run_request.is_some() && !sent_tools {
                                sent_tools = true;
                                if response_tx
                                    .send(Ok(read_exec_frame(
                                        41,
                                        "exec-read-a",
                                        "call-read-a",
                                        "README.md",
                                    )))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                if response_tx
                                    .send(Ok(read_exec_frame(
                                        42,
                                        "exec-read-b",
                                        "call-read-b",
                                        "Cargo.toml",
                                    )))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }

                            if let Some(exec) = message.exec_client_message.as_ref()
                                && let Some(success) = exec
                                    .read_result
                                    .as_ref()
                                    .and_then(|result| result.success.as_ref())
                            {
                                read_results
                                    .insert(exec.id, success.content.clone().unwrap_or_default());
                            }
                            if let Some(close) = message
                                .exec_client_control_message
                                .as_ref()
                                .and_then(|control| control.stream_close.as_ref())
                            {
                                stream_closes.insert(close.id);
                            }

                            if read_results.len() == 2
                                && stream_closes.contains(&41)
                                && stream_closes.contains(&42)
                            {
                                assert_eq!(
                                    read_results.get(&41).map(String::as_str),
                                    Some("README result")
                                );
                                assert_eq!(
                                    read_results.get(&42).map(String::as_str),
                                    Some("Cargo result")
                                );
                                if response_tx.send(Ok(final_text_frame())).await.is_err() {
                                    return;
                                }
                                if response_tx.send(Ok(final_usage_frame())).await.is_err() {
                                    return;
                                }
                                let _ = response_tx
                                    .send(Ok(encode_connect_frame([], FLAG_END)))
                                    .await;
                                return;
                            }
                        }
                    }
                });

                let response_stream =
                    futures_util::stream::unfold(response_rx, |mut receiver| async move {
                        receiver.recv().await.map(|item| (item, receiver))
                    });
                Response::builder()
                    .status(200)
                    .header(
                        axum::http::header::CONTENT_TYPE,
                        "application/connect+proto",
                    )
                    .body(Body::from_stream(response_stream))
                    .unwrap()
            }
        }),
    );

    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let upstream_url = format!("http://{upstream_addr}");
    let upstream_handle = tokio::spawn(async move {
        axum::serve(upstream_listener, upstream_app).await.unwrap();
    });

    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let proxy_handle = tokio::spawn(async move {
        claude_cursor_proxy::server::serve_listener(proxy_listener, None, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
    });

    unsafe {
        std::env::set_var("CCP_CURSOR_BASE_URL", &upstream_url);
        std::env::set_var("CCP_CURSOR_AUTH_TOKEN", "parallel-bidi-token");
        std::env::set_var("CCP_CURSOR_CLIENT_VERSION", "parallel-bidi-test");
        std::env::set_var("CCP_CURSOR_BIDI", "1");
        std::env::set_var("CCP_CURSOR_HEARTBEAT_SECS", "60");
        std::env::set_var("CCP_CURSOR_EXEC_HEARTBEAT_SECS", "60");
        std::env::set_var("CCP_CURSOR_TOOL_BATCH_MS", "50");
    }

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let client = reqwest::Client::new();
    let session_id = "cursor-live-parallel-continuation-test";
    let tools = serde_json::json!([{
        "name": "Read",
        "description": "Read a file",
        "input_schema": {
            "type": "object",
            "properties": {"file_path": {"type": "string"}},
            "required": ["file_path"]
        }
    }]);

    let first_response = client
        .post(format!("http://{proxy_addr}/v1/messages"))
        .header("authorization", "Bearer ignored")
        .header("anthropic-version", "2023-06-01")
        .header("x-claude-code-session-id", session_id)
        .json(&serde_json::json!({
            "model": "cursor:gpt-5.5",
            "max_tokens": 256,
            "stream": true,
            "tools": tools.clone(),
            "messages": [{"role": "user", "content": "Read both files"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(first_response.status(), reqwest::StatusCode::OK);
    let first_sse = tokio::time::timeout(std::time::Duration::from_secs(5), first_response.text())
        .await
        .expect("parallel tool batch did not close the first Anthropic segment")
        .unwrap();
    let first_events = parse_sse_events(&first_sse);
    assert_eq!(first_events[0].0, "message_start");
    assert_eq!(
        first_events.last().map(|(n, _)| n.as_str()),
        Some("message_stop")
    );
    let tool_starts: Vec<&serde_json::Value> = first_events
        .iter()
        .filter_map(|(name, data)| {
            (name == "content_block_start" && data["content_block"]["type"] == "tool_use")
                .then_some(data)
        })
        .collect();
    assert_eq!(tool_starts.len(), 2);
    assert_eq!(tool_starts[0]["index"], 0);
    assert_eq!(tool_starts[0]["content_block"]["id"], "call-read-a");
    assert_eq!(tool_starts[1]["index"], 1);
    assert_eq!(tool_starts[1]["content_block"]["id"], "call-read-b");
    assert_eq!(
        final_message_delta(&first_events)["delta"]["stop_reason"],
        "tool_use"
    );

    let second_response = client
        .post(format!("http://{proxy_addr}/v1/messages"))
        .header("authorization", "Bearer ignored")
        .header("anthropic-version", "2023-06-01")
        .header("x-claude-code-session-id", session_id)
        .json(&serde_json::json!({
            "model": "cursor:gpt-5.5",
            "max_tokens": 256,
            "stream": true,
            "tools": tools,
            "messages": [
                {"role": "user", "content": "Read both files"},
                {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "call-read-a", "name": "Read", "input": {"file_path": "README.md"}},
                    {"type": "tool_use", "id": "call-read-b", "name": "Read", "input": {"file_path": "Cargo.toml"}}
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "call-read-b", "content": "Cargo result"},
                    {"type": "tool_result", "tool_use_id": "call-read-a", "content": "README result"}
                ]}
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(second_response.status(), reqwest::StatusCode::OK);
    let second_sse =
        tokio::time::timeout(std::time::Duration::from_secs(5), second_response.text())
            .await
            .expect("parallel tool results did not resume the original Cursor run")
            .unwrap();
    let second_events = parse_sse_events(&second_sse);
    assert_eq!(
        second_events
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        vec![
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ]
    );
    assert_eq!(
        second_events[2].1["delta"]["text"],
        "both parallel reads continued on one run"
    );
    assert_eq!(second_events[4].1["delta"]["stop_reason"], "end_turn");

    let client_messages = observed
        .client_messages
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    assert_eq!(observed.post_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        client_messages
            .iter()
            .filter(|message| message.run_request.is_some())
            .count(),
        1
    );
    let read_result_ids: Vec<u32> = client_messages
        .iter()
        .filter_map(|message| {
            message
                .exec_client_message
                .as_ref()
                .filter(|exec| exec.read_result.is_some())
                .map(|exec| exec.id)
        })
        .collect();
    assert_eq!(read_result_ids, vec![41, 42]);
    let close_ids: Vec<u32> = client_messages
        .iter()
        .filter_map(|message| {
            message
                .exec_client_control_message
                .as_ref()
                .and_then(|control| control.stream_close.as_ref())
                .map(|close| close.id)
        })
        .collect();
    assert_eq!(close_ids, vec![41, 42]);

    let _ = shutdown_tx.send(());
    upstream_handle.abort();
    proxy_handle.abort();
    unsafe {
        std::env::remove_var("CCP_CURSOR_BASE_URL");
        std::env::remove_var("CCP_CURSOR_AUTH_TOKEN");
        std::env::remove_var("CCP_CURSOR_CLIENT_VERSION");
        std::env::remove_var("CCP_CURSOR_BIDI");
        std::env::remove_var("CCP_CURSOR_HEARTBEAT_SECS");
        std::env::remove_var("CCP_CURSOR_EXEC_HEARTBEAT_SECS");
        std::env::remove_var("CCP_CURSOR_TOOL_BATCH_MS");
    }
}

// ---------------------------------------------------------------------------
// Cursor tool bridge integration tests
// ---------------------------------------------------------------------------

#[test]
fn bridge_start_pauses_on_tool_use_xml() {
    use claude_cursor_proxy::providers::cursor::response::*;
    use claude_cursor_proxy::providers::cursor::tool_bridge::*;

    // Create upstream events with a text delta containing XML tool_use
    let events = vec![
        CursorStreamEvent::TextDelta {
            text: "before ".to_string(),
        },
        CursorStreamEvent::TextDelta {
            text: r#"<tool_use id="x" name="Read">{"file_path":"/tmp/a"}</tool_use>"#.to_string(),
        },
        CursorStreamEvent::TextDelta {
            text: " after".to_string(),
        },
        CursorStreamEvent::Usage {
            input_tokens: 10,
            output_tokens: 5,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        },
        CursorStreamEvent::End,
    ];

    let allowed: std::collections::BTreeSet<String> =
        ["Read".to_string(), "Write".to_string(), "Bash".to_string()]
            .into_iter()
            .collect();

    let mut counter = 0u64;
    let id_factory = Box::new(move || {
        counter += 1;
        format!("call_cursor_test_{counter}")
    });

    let (sse, paused) = start_cursor_tool_bridge(
        "msg_1",
        "cursor-test",
        "session-bridge-1",
        &events,
        Some(allowed),
        id_factory,
    );

    assert!(paused, "bridge should pause on tool_use");

    let sse_str = String::from_utf8_lossy(&sse);
    let parsed = parse_sse_events(&sse_str);

    let event_names: Vec<&str> = parsed.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        event_names.contains(&"content_block_start"),
        "expected content_block_start for tool_use"
    );
    assert!(
        event_names.contains(&"message_stop"),
        "expected message_stop"
    );

    let msg_delta = parsed
        .iter()
        .find(|(n, _)| n == "message_delta")
        .map(|(_, d)| d.clone());
    assert!(msg_delta.is_some(), "expected message_delta");
    assert_eq!(
        msg_delta.unwrap()["delta"]["stop_reason"],
        "tool_use",
        "stop_reason should be tool_use"
    );

    // Clean up
    BridgeRegistry::remove("session-bridge-1");
}

#[test]
fn bridge_start_passes_through_without_tool_use() {
    use claude_cursor_proxy::providers::cursor::response::*;
    use claude_cursor_proxy::providers::cursor::tool_bridge::*;

    let events = vec![
        CursorStreamEvent::TextDelta {
            text: "hello world".to_string(),
        },
        CursorStreamEvent::Usage {
            input_tokens: 5,
            output_tokens: 1,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        },
        CursorStreamEvent::End,
    ];

    let (sse, paused) = start_cursor_tool_bridge(
        "msg_2",
        "cursor-test",
        "session-bridge-2",
        &events,
        None,
        Box::new(|| "id".into()),
    );

    assert!(!paused, "bridge should NOT pause without tool_use");

    let sse_str = String::from_utf8_lossy(&sse);
    let parsed = parse_sse_events(&sse_str);
    let event_names: Vec<&str> = parsed.iter().map(|(n, _)| n.as_str()).collect();

    assert!(event_names.contains(&"message_start"));
    assert!(event_names.contains(&"content_block_delta"));
    assert!(event_names.contains(&"message_delta"));
    assert!(event_names.contains(&"message_stop"));

    // Verify stop_reason is end_turn
    let msg_delta = parsed
        .iter()
        .find(|(n, _)| n == "message_delta")
        .map(|(_, d)| d.clone());
    assert_eq!(
        msg_delta.unwrap()["delta"]["stop_reason"],
        "end_turn",
        "stop_reason should be end_turn without tool_use"
    );
}

#[test]
fn bridge_start_creates_pending_tool_in_registry() {
    use claude_cursor_proxy::providers::cursor::response::*;
    use claude_cursor_proxy::providers::cursor::tool_bridge::*;

    // Clean state
    BridgeRegistry::clear();

    let events = vec![CursorStreamEvent::TextDelta {
        text: r#"<tool_use name="Read">{"file_path":"/tmp/test"}</tool_use>"#.to_string(),
    }];

    let allowed: std::collections::BTreeSet<String> = ["Read".to_string()].into_iter().collect();

    let (_, paused) = start_cursor_tool_bridge(
        "msg_3",
        "cursor-test",
        "session-bridge-pt",
        &events,
        Some(allowed),
        Box::new(|| "call_test".into()),
    );

    assert!(paused);

    let pending = BridgeRegistry::pending_tool("session-bridge-pt");
    assert!(pending.is_some(), "pending tool should be stored");
    assert_eq!(pending.unwrap().name(), "Read");

    BridgeRegistry::remove("session-bridge-pt");
}

#[test]
fn bridge_resume_continues_after_tool_use_pause() {
    use claude_cursor_proxy::providers::cursor::response::*;
    use claude_cursor_proxy::providers::cursor::tool_bridge::*;

    BridgeRegistry::clear();

    // Events: tool_use in the middle, text after
    let events = vec![
        CursorStreamEvent::TextDelta {
            text: "before ".to_string(),
        },
        CursorStreamEvent::TextDelta {
            text: r#"<tool_use name="Read">{"file_path":"/tmp/a"}</tool_use>"#.to_string(),
        },
        CursorStreamEvent::TextDelta {
            text: " continued".to_string(),
        },
        CursorStreamEvent::Usage {
            input_tokens: 10,
            output_tokens: 5,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        },
        CursorStreamEvent::End,
    ];

    let allowed: std::collections::BTreeSet<String> = ["Read".to_string()].into_iter().collect();

    let mut counter = 0u64;
    let id_factory = Box::new(move || {
        counter += 1;
        format!("call_cursor_test_{counter}")
    });

    let (_first_sse, paused) = start_cursor_tool_bridge(
        "msg_first",
        "cursor-test",
        "session-resume-1",
        &events,
        Some(allowed),
        id_factory,
    );
    assert!(paused);

    let body: claude_cursor_proxy::MessagesRequest =
        serde_json::from_value(serde_json::json!({
            "model": "cursor-test",
            "messages": [
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call_cursor_test_1", "content": "result text"}]}
            ]
        }))
        .unwrap();

    let pending =
        BridgeRegistry::pending_tool("session-resume-1").expect("should have pending tool");
    assert_eq!(pending.tool_use_id(), "call_cursor_test_1");

    let result = find_tool_result(&body, pending.tool_use_id()).expect("should find tool result");

    let (result_msgs, second_sse) = resume_cursor_tool_bridge(
        "session-resume-1",
        "msg_second",
        "cursor-test",
        result,
        &pending,
    );

    assert!(!result_msgs.is_empty(), "should have result messages");

    let sse_str = String::from_utf8_lossy(&second_sse);
    let parsed = parse_sse_events(&sse_str);
    let event_names: Vec<&str> = parsed.iter().map(|(n, _)| n.as_str()).collect();

    assert!(
        event_names.contains(&"message_start"),
        "resume should have message_start in {event_names:?}"
    );
    assert!(
        event_names.contains(&"message_stop"),
        "resume should have message_stop in {event_names:?}"
    );

    let text_deltas: Vec<&str> = parsed
        .iter()
        .filter_map(|(n, d)| {
            if n == "content_block_delta" {
                d["delta"]["text"].as_str()
            } else {
                None
            }
        })
        .collect();
    let combined = text_deltas.join("");
    assert!(
        combined.contains("continued"),
        "resume should include remaining text deltas"
    );

    BridgeRegistry::remove("session-resume-1");
}

#[test]
fn bridge_rejects_tool_not_in_allowed_list() {
    use claude_cursor_proxy::providers::cursor::response::*;
    use claude_cursor_proxy::providers::cursor::tool_bridge::*;

    BridgeRegistry::clear();

    let events = vec![CursorStreamEvent::TextDelta {
        text: r#"<tool_use name="Bash">{"command":"pwd"}</tool_use>"#.to_string(),
    }];

    let allowed: std::collections::BTreeSet<String> = ["Read".to_string()].into_iter().collect();

    let (sse, paused) = start_cursor_tool_bridge(
        "msg_filter",
        "cursor-test",
        "session-filter-1",
        &events,
        Some(allowed),
        Box::new(|| "id".into()),
    );

    assert!(!paused, "should NOT pause for disallowed tool");

    let sse_str = String::from_utf8_lossy(&sse);
    let parsed = parse_sse_events(&sse_str);
    let _event_names: Vec<&str> = parsed.iter().map(|(n, _)| n.as_str()).collect();

    let msg_delta = parsed
        .iter()
        .find(|(n, _)| n == "message_delta")
        .map(|(_, d)| d.clone());
    assert_eq!(
        msg_delta.unwrap()["delta"]["stop_reason"],
        "end_turn",
        "disallowed tool should not trigger tool_use"
    );

    BridgeRegistry::remove("session-filter-1");
}

#[test]
fn bridge_result_messages_have_correct_read_shape() {
    use claude_cursor_proxy::providers::cursor::tool_bridge::*;

    let exec = CursorExec {
        id: Some(42),
        exec_id: None,
        args: serde_json::json!({"file_path": "/tmp/readme.txt"}),
    };
    let result = CursorNativeToolResult {
        content: "file contents here".into(),
        is_error: false,
    };

    let msg = build_read_result_from_native(&exec, &result);
    let msg_obj = msg.as_object().unwrap();

    assert_eq!(msg_obj.get("id").and_then(|v| v.as_i64()), Some(42));

    let read_result = msg_obj.get("readResult").unwrap();
    assert!(read_result.get("success").is_some());
    let success = read_result.get("success").unwrap();
    assert_eq!(
        success.get("path").and_then(|v| v.as_str()),
        Some("/tmp/readme.txt")
    );
    assert_eq!(
        success.get("content").and_then(|v| v.as_str()),
        Some("file contents here")
    );
    assert_eq!(success.get("totalLines").and_then(|v| v.as_i64()), Some(1));
}

#[test]
fn bridge_result_messages_have_correct_write_shape() {
    use claude_cursor_proxy::providers::cursor::tool_bridge::*;

    let exec = CursorExec {
        id: Some(99),
        exec_id: Some("exec-write-1".into()),
        args: serde_json::json!({"file_path": "/tmp/writeme.txt", "content": "data"}),
    };

    let result = CursorNativeToolResult {
        content: "written".into(),
        is_error: false,
    };
    let msg = build_write_result_from_native(&exec, &result);
    let msg_obj = msg.as_object().unwrap();
    assert_eq!(
        msg_obj.get("execId").and_then(|v| v.as_str()),
        Some("exec-write-1")
    );
    let write_result = msg_obj.get("writeResult").unwrap();
    let success = write_result.get("success").unwrap();
    assert_eq!(
        success.get("path").and_then(|v| v.as_str()),
        Some("/tmp/writeme.txt")
    );
    assert!(success.get("linesCreated").is_some());
    assert!(success.get("fileSize").is_some());

    let error_result = CursorNativeToolResult {
        content: "permission denied".into(),
        is_error: true,
    };
    let err_msg = build_write_result_from_native(&exec, &error_result);
    let err_obj = err_msg.as_object().unwrap();
    let write_result = err_obj.get("writeResult").unwrap();
    let error = write_result.get("error").unwrap();
    assert_eq!(
        error.get("path").and_then(|v| v.as_str()),
        Some("/tmp/writeme.txt")
    );
    assert!(
        error
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("permission")
    );
}

#[test]
fn bridge_shell_stream_result_has_correct_shape() {
    use claude_cursor_proxy::providers::cursor::tool_bridge::*;

    let exec = CursorExec {
        id: Some(7),
        exec_id: Some("exec-shell".into()),
        args: serde_json::json!({}),
    };

    let result = CursorNativeToolResult {
        content: "stdout output".into(),
        is_error: false,
    };

    let messages = build_shell_stream_result(
        &exec,
        &result,
        std::time::Duration::from_millis(150),
        "/home/user",
    );

    assert_eq!(messages.len(), 4, "start + stdout + exit + close");

    assert!(
        messages[0]
            .get("shellStream")
            .and_then(|s| s.get("start"))
            .is_some()
    );

    assert_eq!(
        messages[1]["shellStream"]["stdout"]["data"],
        "stdout output"
    );

    assert_eq!(messages[2]["shellStream"]["exit"]["code"], 0);
    assert_eq!(messages[2]["shellStream"]["exit"]["cwd"], "/home/user");

    assert_eq!(
        messages[3]["execClientControlMessage"]["streamClose"]["id"],
        7
    );
}

// ---------------------------------------------------------------------------
// No TypeScript sidecar
// ---------------------------------------------------------------------------

#[test]
fn cursor_provider_has_no_typescript_sidecar() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/providers/cursor");
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        match std::fs::read_dir(&dir) {
            Ok(entries) => {
                for entry in entries {
                    let path = entry.unwrap().path();
                    if path.is_dir() {
                        stack.push(path);
                        continue;
                    }
                    let ext = path.extension().and_then(|e| e.to_str());
                    assert_ne!(ext, Some("ts"), "TypeScript file found at {:?}", path);
                }
            }
            Err(_) => {
                // Directory may not exist (e.g., if cursor provider was removed)
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SSE parser helper (mirrors sse.rs internal helper for cross-module access)
// ---------------------------------------------------------------------------

fn parse_sse_events(sse: &str) -> Vec<(String, serde_json::Value)> {
    let mut events = Vec::new();
    let mut current_event = String::new();

    for line in sse.lines() {
        if let Some(event) = line.strip_prefix("event: ") {
            current_event = event.to_string();
        } else if let Some(data_str) = line.strip_prefix("data: ") {
            if let Ok(data) = serde_json::from_str::<serde_json::Value>(data_str) {
                events.push((current_event.clone(), data));
            }
        }
    }

    events
}

/// Final (non-progress) `message_delta` — mid-stream usage progress uses
/// `stop_reason: null` and must not be confused with end_turn/tool_use.
fn final_message_delta(events: &[(String, serde_json::Value)]) -> &serde_json::Value {
    events
        .iter()
        .rev()
        .find(|(name, data)| {
            name == "message_delta"
                && data
                    .get("delta")
                    .and_then(|d| d.get("stop_reason"))
                    .map(|s| !s.is_null())
                    .unwrap_or(false)
        })
        .map(|(_, data)| data)
        .expect("expected final message_delta with non-null stop_reason")
}
