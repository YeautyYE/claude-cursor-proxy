//! Local provider-level Sand load fixtures.
//!
//! These tests exercise the same admission/retry driver used by production,
//! while keeping all traffic inside a loopback Axum server. They are separate
//! from the protocol tests because they intentionally touch private provider
//! state and retry budgets.

use super::*;
use crate::providers::cursor::connect::{FLAG_END, encode_connect_frame};
use crate::providers::cursor::sand_inference::{
    SandInferenceClient, SandInferenceMessage, SandInferenceRequest,
};
use axum::{Router, body::Body, extract::Request, routing::post};
use bytes::Bytes;
use futures_util::StreamExt;
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const FANOUT: usize = 512;
const LOAD_TOKEN: &str = "provider-load-fixture-token";
const LOAD_MODEL: &str = "provider-load-fixture-model";

fn local_response(text: &str) -> Vec<u8> {
    let frame = encode_connect_frame(
        serde_json::to_vec(&json!({"textPart": {"text": text}})).expect("response JSON"),
        0,
    );
    let end = encode_connect_frame(Vec::new(), FLAG_END);
    [frame.as_ref(), end.as_ref()].concat()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn provider_sand_admission_retries_512_local_requests() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(FANOUT + 1));

    let app = Router::new().route(
        "/aiserver.v1.InferenceService/Stream",
        post({
            let attempts = Arc::clone(&attempts);
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            let completed = Arc::clone(&completed);
            let barrier = Arc::clone(&barrier);
            move |request: Request<Body>| {
                let attempts = Arc::clone(&attempts);
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                let completed = Arc::clone(&completed);
                let barrier = Arc::clone(&barrier);
                async move {
                    let _ = axum::body::to_bytes(request.into_body(), 8 * 1024 * 1024)
                        .await
                        .expect("read fixture request");
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    // One deterministic pre-output failure exercises the
                    // provider retry path; all subsequent opens succeed.
                    if attempt == 0 {
                        return (
                            http::StatusCode::SERVICE_UNAVAILABLE,
                            [(axum::http::header::CONTENT_TYPE, "application/json")],
                            Body::from(r#"{"error":{"message":"synthetic pre-output 503"}}"#),
                        );
                    }

                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now, Ordering::SeqCst);
                    let body = futures_util::stream::once(async move {
                        barrier.wait().await;
                        active.fetch_sub(1, Ordering::SeqCst);
                        completed.fetch_add(1, Ordering::SeqCst);
                        Ok::<Bytes, Infallible>(Bytes::from(local_response("provider-fixture-ok")))
                    });
                    (
                        http::StatusCode::OK,
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

    // Keep the fixture independent of one peer's HTTP/2 stream limit. The
    // production client uses the same sharding strategy for large fan-outs.
    let clients = (0..8)
        .map(|_| {
            SandInferenceClient::with_base_url_timeout(url.clone(), 10).expect("fixture client")
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..FANOUT {
        let client = clients[index % clients.len()].clone();
        tasks.spawn(async move {
            let request = SandInferenceRequest::new(
                LOAD_MODEL,
                format!("provider-load-conversation-{index}"),
                format!("provider-load-invocation-{index}"),
                vec![SandInferenceMessage::user("hello")],
            );
            let started = Instant::now();
            let mut stream = open_sand_with_retries_until(
                &client,
                LOAD_TOKEN,
                &request,
                Instant::now() + Duration::from_secs(40),
                &SandAttemptBudget::new(),
                SandOpenAttemptKind::Transport,
            )
            .await
            .expect("provider retry path should recover");
            let open_time = started.elapsed();
            let mut text_count = 0;
            let mut end_count = 0;
            while let Some(event) = stream.next().await {
                match event.expect("fixture event") {
                    CursorStreamEvent::TextDelta { text } => {
                        assert_eq!(text, "provider-fixture-ok");
                        text_count += 1;
                    }
                    CursorStreamEvent::End => end_count += 1,
                    _ => {}
                }
            }
            assert_eq!(text_count, 1);
            assert_eq!(end_count, 1);
            open_time
        });
    }

    let outcome = tokio::time::timeout(Duration::from_secs(45), async {
        barrier.wait().await;
        let mut open_times = Vec::with_capacity(FANOUT);
        while let Some(result) = tasks.join_next().await {
            open_times.push(result.map_err(|error| error.to_string())?);
        }
        Ok::<_, String>(open_times)
    })
    .await;
    tasks.shutdown().await;
    server.abort();
    let _ = server.await;
    let mut open_times = outcome
        .expect("all recovered provider streams should finish within the fixture deadline")
        .expect("provider load task");
    open_times.sort_unstable();
    eprintln!(
        "Sand provider fixture: streams={FANOUT}, attempts={}, elapsed={:?}, open p50={:?}, p95={:?}, p99={:?}",
        attempts.load(Ordering::SeqCst),
        started.elapsed(),
        open_times[FANOUT * 50 / 100],
        open_times[FANOUT * 95 / 100],
        open_times[FANOUT * 99 / 100],
    );

    assert_eq!(max_active.load(Ordering::SeqCst), FANOUT);
    assert_eq!(completed.load(Ordering::SeqCst), FANOUT);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(attempts.load(Ordering::SeqCst), FANOUT + 1);
    assert_eq!(
        sand_inference::sand_open_available_permits(LOAD_TOKEN, LOAD_MODEL),
        Some(sand_inference::sand_open_account_capacity()),
        "completed opens must return all account admission permits"
    );
    assert!(started.elapsed() < Duration::from_secs(45));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_sand_admission_cancellation_releases_resources() {
    const TOKEN: &str = "fixture-cancel-open-token";
    const MODEL: &str = "fixture-cancel-open-model";
    let entered = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/aiserver.v1.InferenceService/Stream",
        post({
            let entered = Arc::clone(&entered);
            let calls = Arc::clone(&calls);
            move || {
                let entered = Arc::clone(&entered);
                let calls = Arc::clone(&calls);
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                    (
                        [(axum::http::header::CONTENT_TYPE, "application/connect+json")],
                        local_response("after-cancel"),
                    )
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind cancellation fixture");
    let url = format!("http://{}", listener.local_addr().expect("fixture address"));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve fixture");
    });
    let client = SandInferenceClient::with_base_url_timeout(url, 10).expect("fixture client");
    let request = SandInferenceRequest::new(
        MODEL,
        "cancel-conversation",
        "cancel-invocation",
        vec![SandInferenceMessage::user("hello")],
    );
    let mut opens = tokio::task::JoinSet::new();
    opens.spawn({
        let client = client.clone();
        let request = request.clone();
        async move {
            open_sand_with_retries_until(
                &client,
                TOKEN,
                &request,
                Instant::now() + Duration::from_secs(10),
                &SandAttemptBudget::new(),
                SandOpenAttemptKind::Transport,
            )
            .await
        }
    });
    let entered_result = tokio::time::timeout(Duration::from_secs(5), entered.notified()).await;
    opens.shutdown().await;
    let released = sand_inference::sand_open_available_permits(TOKEN, MODEL);
    let reopened = tokio::time::timeout(Duration::from_secs(5), async {
        let mut stream = open_sand_with_retries_until(
            &client,
            TOKEN,
            &request.with_fresh_ids(),
            Instant::now() + Duration::from_secs(5),
            &SandAttemptBudget::new(),
            SandOpenAttemptKind::Transport,
        )
        .await?;
        while let Some(event) = stream.next().await {
            event?;
        }
        Ok::<_, CursorError>(())
    })
    .await;
    server.abort();
    let _ = server.await;
    entered_result.expect("first request should reach the pending header fixture");
    assert_eq!(released, Some(sand_inference::sand_open_account_capacity()));
    reopened
        .expect("canceled opening must not stall the next request")
        .expect("next request should succeed");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
