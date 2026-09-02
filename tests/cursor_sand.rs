//! Protocol-level fixtures for the Cursor Desktop Sand transport.
//!
//! The real endpoint is HTTP/2 + Connect-JSON. Keeping this test separate
//! from the AgentService fixtures makes an accidental Run/RunSSE fallback
//! visible and protects the Fable long-context parameters from regressions.

use axum::{Router, body::Body, extract::Request, http::Version, routing::post};
use claude_cursor_proxy::providers::cursor::connect::{
    ConnectFrameDecoder, FLAG_END, encode_connect_frame,
};
use claude_cursor_proxy::providers::cursor::response::CursorStreamEvent;
use claude_cursor_proxy::providers::cursor::sand_inference::{
    SandInferenceClient, SandInferenceMessage, SandInferenceRequest,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct ObservedRequest {
    version: Version,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sand_fable_stream_uses_h2_connect_json_and_long_context_parameters() {
    let observed: Arc<Mutex<Option<ObservedRequest>>> = Arc::new(Mutex::new(None));
    let observed_handler = Arc::clone(&observed);

    let response_body = {
        let text = encode_connect_frame(
            serde_json::to_vec(&json!({
                "textPart": {"text": "sand-ok"},
                "usage": {
                    "promptTokens": 17,
                    "completionTokens": 3,
                    "cacheReadTokens": 2,
                    "cacheWriteTokens": 1
                }
            }))
            .expect("response JSON"),
            0,
        );
        let end = encode_connect_frame(Vec::new(), FLAG_END);
        [text.as_ref(), end.as_ref()].concat()
    };

    let app = Router::new().route(
        "/aiserver.v1.InferenceService/Stream",
        post(move |request: Request<Body>| {
            let observed_handler = Arc::clone(&observed_handler);
            let response_body = response_body.clone();
            async move {
                let version = request.version();
                let headers = request.headers().clone();
                let body = axum::body::to_bytes(request.into_body(), 8 * 1024 * 1024)
                    .await
                    .expect("read Sand request");
                *observed_handler.lock().expect("observed lock") = Some(ObservedRequest {
                    version,
                    headers,
                    body: body.to_vec(),
                });
                (
                    [(axum::http::header::CONTENT_TYPE, "application/connect+json")],
                    response_body,
                )
            }
        }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fixture");
    let url = format!("http://{}", listener.local_addr().expect("fixture address"));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve fixture");
    });

    let client = SandInferenceClient::with_base_url_timeout(url, 5).expect("Sand client");
    let request = SandInferenceRequest::new(
        "claude-fable-5",
        "conversation-fixture",
        "invocation-fixture",
        vec![SandInferenceMessage::user("hello")],
    )
    .with_parameter_model_id("claude-fable-5-thinking-max")
    .with_max_mode(true)
    .with_max_tokens(Some(4096));

    let mut stream = client
        .open("fixture-token", &request)
        .await
        .expect("Sand fixture should accept the stream");
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("Sand event"));
    }

    server.abort();

    let observed = observed
        .lock()
        .expect("observed lock")
        .clone()
        .expect("request captured");
    assert_eq!(observed.version, Version::HTTP_2, "Sand must stay on h2");
    assert_eq!(
        observed
            .headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/connect+json")
    );
    assert_eq!(
        observed
            .headers
            .get("x-cursor-client-type")
            .and_then(|value| value.to_str().ok()),
        Some("sand")
    );
    assert_eq!(
        observed
            .headers
            .get("connect-protocol-version")
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );

    let mut decoder = ConnectFrameDecoder::new();
    let frames = decoder.push(&observed.body).expect("decode request frame");
    assert_eq!(frames.len(), 1, "one request frame is expected");
    let body: Value = serde_json::from_slice(&frames[0].payload).expect("request JSON");
    assert_eq!(body["modelId"], "claude-fable-5");
    assert_eq!(body["requestedModel"]["modelId"], "claude-fable-5");
    assert_eq!(body["requestedModel"]["maxMode"], true);
    assert_eq!(body["conversationId"], "conversation-fixture");
    assert_eq!(body["invocationId"], "invocation-fixture");
    let parameters = body["requestedModel"]["parameters"]
        .as_array()
        .expect("requested model parameters");
    assert!(
        parameters
            .iter()
            .any(|parameter| parameter["id"] == "thinking" && parameter["value"] == "true")
    );
    assert!(
        parameters
            .iter()
            .any(|parameter| parameter["id"] == "effort" && parameter["value"] == "max")
    );
    assert!(
        parameters
            .iter()
            .any(|parameter| parameter["id"] == "context" && parameter["value"] == "1m")
    );
    assert_eq!(body["modelConfig"]["maxTokens"], 4096);

    assert!(
        events.iter().any(
            |event| matches!(event, CursorStreamEvent::TextDelta { text } if text == "sand-ok")
        )
    );
    assert!(events.iter().any(|event| matches!(
        event,
        CursorStreamEvent::Usage {
            input_tokens: 17,
            output_tokens: 3,
            cache_read_tokens: 2,
            cache_write_tokens: 1,
            ..
        }
    )));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, CursorStreamEvent::End))
            .count(),
        1,
        "terminal event must be emitted once"
    );
}
