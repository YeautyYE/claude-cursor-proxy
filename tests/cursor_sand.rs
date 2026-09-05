//! Protocol-level fixtures for the Cursor Desktop Sand transport.
//!
//! The real endpoint is HTTP/2 + Connect-JSON. Keeping this test separate
//! from the AgentService fixtures makes an accidental Run/RunSSE fallback
//! visible and protects the Fable long-context parameters from regressions.

use axum::{Router, body::Body, extract::Request, http::Version, routing::post};
use bytes::Bytes;
use claude_cursor_proxy::providers::cursor::connect::{
    ConnectFrameDecoder, FLAG_END, encode_connect_frame,
};
use claude_cursor_proxy::providers::cursor::response::CursorStreamEvent;
use claude_cursor_proxy::providers::cursor::sand_inference::{
    SandInferenceClient, SandInferenceMessage, SandInferenceRequest,
};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

/// Exercise the Sand transport fan-out against a local HTTP/2 fixture.
///
/// This deliberately bypasses the real Cursor service: every request is
/// accepted by an in-process Axum endpoint and held at a barrier until all
/// 512 handlers have arrived. This catches a hidden 3/4/32 transport stream
/// cap without spending quota or depending on network timing. Provider-level
/// admission and retry behavior is covered in `sand_load_tests`.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn sand_transport_fixture_supports_512_concurrent_streams() {
    const FANOUT: usize = 512;

    let barrier = Arc::new(tokio::sync::Barrier::new(FANOUT + 1));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));

    let response_body = {
        let text = encode_connect_frame(
            serde_json::to_vec(&json!({
                "textPart": {"text": "fixture-ok"},
                "usage": {"promptTokens": 1, "completionTokens": 1}
            }))
            .expect("response JSON"),
            0,
        );
        let end = encode_connect_frame(Vec::new(), FLAG_END);
        [text.as_ref(), end.as_ref()].concat()
    };

    let app = Router::new().route(
        "/aiserver.v1.InferenceService/Stream",
        post({
            let barrier = Arc::clone(&barrier);
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            let completed = Arc::clone(&completed);
            move |request: Request<Body>| {
                let barrier = Arc::clone(&barrier);
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let completed = Arc::clone(&completed);
                let response_body = response_body.clone();
                async move {
                    // Read the request body so the test covers the full
                    // Connect request upload, not only response headers.
                    let _ = axum::body::to_bytes(request.into_body(), 8 * 1024 * 1024)
                        .await
                        .expect("read fixture request");

                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now, Ordering::SeqCst);
                    // Return headers immediately. The body stream is held at
                    // the barrier so `open()` can complete and all clients
                    // can enter the established-stream phase concurrently.
                    let body_barrier = Arc::clone(&barrier);
                    let body_active = Arc::clone(&active);
                    let body_completed = Arc::clone(&completed);
                    let body = futures_util::stream::once(async move {
                        body_barrier.wait().await;
                        body_active.fetch_sub(1, Ordering::SeqCst);
                        body_completed.fetch_add(1, Ordering::SeqCst);
                        Ok::<Bytes, Infallible>(Bytes::from(response_body))
                    });

                    (
                        [(axum::http::header::CONTENT_TYPE, "application/connect+json")],
                        Body::from_stream(body),
                    )
                }
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

    // Use several independent HTTP/2 pools. A single H2 connection commonly
    // advertises a peer stream limit below 512; production Sand uses sharded
    // clients for the same reason. The fixture should validate logical
    // fan-out rather than a peer-specific MAX_CONCURRENT_STREAMS setting.
    let clients = (0..8)
        .map(|_| SandInferenceClient::with_base_url_timeout(url.clone(), 10).expect("Sand client"))
        .collect::<Vec<_>>();
    let mut tasks = tokio::task::JoinSet::new();
    let run = async {
        for index in 0..FANOUT {
            let client = clients[index % clients.len()].clone();
            tasks.spawn(async move {
                let request = SandInferenceRequest::new(
                    "claude-fable-5",
                    format!("fixture-conversation-{index}"),
                    format!("fixture-invocation-{index}"),
                    vec![SandInferenceMessage::user("hello")],
                );
                let mut stream = client
                    .open("fixture-token", &request)
                    .await
                    .expect("fixture stream should open");
                let mut saw_text = false;
                while let Some(event) = stream.next().await {
                    match event.expect("fixture stream event") {
                        CursorStreamEvent::TextDelta { text } if text == "fixture-ok" => {
                            saw_text = true;
                        }
                        _ => {}
                    }
                }
                assert!(saw_text, "each stream must deliver its text delta");
            });
        }

        // The fixture holds every accepted request until the test task joins
        // the barrier.  If a local stream cap regresses below FANOUT, this
        // timeout fails instead of hanging the test indefinitely.
        tokio::time::timeout(Duration::from_secs(20), barrier.wait())
            .await
            .map_err(|_| "all 512 local Sand handlers should arrive".to_string())?;

        while let Some(result) = tasks.join_next().await {
            result.map_err(|error| error.to_string())?;
        }
        Ok::<_, String>(())
    };

    let outcome = tokio::time::timeout(Duration::from_secs(30), run).await;
    tasks.shutdown().await;
    server.abort();
    let _ = server.await;
    outcome
        .expect("512-way local Sand load should finish")
        .expect("fan-out task");
    assert_eq!(max_active.load(Ordering::SeqCst), FANOUT);
    assert_eq!(completed.load(Ordering::SeqCst), FANOUT);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}
