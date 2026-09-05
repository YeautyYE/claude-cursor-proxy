use axum::{Router, body::Body, extract::State, http::HeaderMap, routing::post};
use bytes::Bytes;
use claude_cursor_proxy::{
    anthropic::schema::MessagesRequest,
    providers::cursor::{
        connect::{ConnectFrameDecoder, FLAG_GZIP, decode_gzip_frame},
        model::resolve_sand_model_id,
        response::{AnthropicJsonAcc, CursorStreamEvent},
        sand_inference::{
            SAND_INFERENCE_STREAM_PATH, SandInferenceClient, SandInferenceMessage,
            SandInferenceRequest, messages_from_anthropic, tools_from_anthropic,
        },
    },
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Deserialize)]
struct Auth {
    #[serde(rename = "accessToken")]
    access_token: String,
}

#[derive(Clone)]
struct Capture {
    client: reqwest::Client,
    endpoint: String,
    shapes: Arc<Mutex<BTreeMap<String, usize>>>,
}

fn record_shape(value: &Value, path: &str, shapes: &mut BTreeMap<String, usize>) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let child_path = format!("{path}.{key}");
                *shapes.entry(child_path.clone()).or_default() += 1;
                record_shape(child, &child_path, shapes);
            }
        }
        Value::Array(array) => {
            *shapes.entry(format!("{path}#items")).or_default() += array.len();
            for child in array {
                record_shape(child, &format!("{path}[]"), shapes);
            }
        }
        Value::String(value) => {
            *shapes.entry(format!("{path}#bytes")).or_default() += value.len();
            if path.ends_with(".errorMessage") && !value.is_empty() {
                eprintln!(
                    "synthetic_response_error={}",
                    value.chars().take(300).collect::<String>()
                );
            }
        }
        _ => {}
    }
}

async fn forward(
    State(capture): State<Capture>,
    mut headers: HeaderMap,
    body: Bytes,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    headers.remove("host");
    headers.remove("content-length");
    let response = capture
        .client
        .post(&capture.endpoint)
        .headers(headers)
        .body(body)
        .send()
        .await
        .map_err(|_| axum::http::StatusCode::BAD_GATEWAY)?;
    let status = response.status();
    let content_type = response.headers().get("content-type").cloned();
    let mut upstream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = upstream.next().await {
        let chunk = chunk.map_err(|_| axum::http::StatusCode::BAD_GATEWAY)?;
        if body.len() + chunk.len() > 4 * 1024 * 1024 {
            return Err(axum::http::StatusCode::BAD_GATEWAY);
        }
        body.extend_from_slice(&chunk);
    }
    let mut decoder = ConnectFrameDecoder::new();
    if let Ok(frames) = decoder.push(&body) {
        let mut shapes = capture.shapes.lock().unwrap();
        for frame in frames {
            *shapes
                .entry(format!("frame_flags_{}", frame.flags))
                .or_default() += 1;
            let payload = if frame.flags & FLAG_GZIP != 0 {
                decode_gzip_frame(&frame.payload).unwrap_or_default()
            } else {
                frame.payload.to_vec()
            };
            if let Ok(value) = serde_json::from_slice::<Value>(&payload) {
                record_shape(&value, "$", &mut shapes);
            }
        }
    }
    let mut response = axum::response::Response::builder().status(status);
    if let Some(content_type) = content_type {
        response = response.header("content-type", content_type);
    }
    Ok(response.body(Body::from(body)).unwrap())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let auth_path = std::env::var("CCP_CURSOR_AUTH_FILE").unwrap_or_else(|_| {
        format!(
            "{}/.config/claude-cursor-proxy/cursor/auth.json",
            std::env::var("HOME").unwrap()
        )
    });
    let auth: Auth = serde_json::from_slice(&std::fs::read(auth_path)?)?;
    let model = std::env::var("CCP_CURSOR_SAND_PROBE_MODEL")
        .unwrap_or_else(|_| "claude-fable-5-1-thinking-max".into());
    let timeout = Duration::from_secs(60);
    let capture = Capture {
        client: reqwest::Client::builder().timeout(timeout).build()?,
        endpoint: format!(
            "{}{}",
            std::env::var("CCP_CURSOR_SAND_BASE_URL")
                .unwrap_or_else(|_| "https://api2.cursor.sh".into())
                .trim_end_matches('/'),
            SAND_INFERENCE_STREAM_PATH
        ),
        shapes: Arc::new(Mutex::new(BTreeMap::new())),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let client = SandInferenceClient::with_base_url_timeout(
        format!("http://{}", listener.local_addr()?),
        60,
    )?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let app = Router::new()
        .route(SAND_INFERENCE_STREAM_PATH, post(forward))
        .with_state(capture.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let native: MessagesRequest = serde_json::from_value(json!({
        "model": model, "max_tokens": 16384,
        "messages": [
            {"role":"user","content":"What is the returned probe value?"},
            {"role":"assistant","content":[{"type":"tool_use","id":"toolu_probe","name":"Read","input":{"file_path":"/synthetic/probe.txt"}}]},
            {"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_probe","content":"PROBE_VALUE=17"}]}
        ],
        "tools":[{"name":"Read","description":"Read a synthetic fixture", "input_schema":{"type":"object","properties":{"file_path":{"type":"string"}},"required":["file_path"]}}]
    }))?;
    let native_messages = messages_from_anthropic(&native, false);
    let mut native_bridge = native_messages.clone();
    native_bridge.insert(0, SandInferenceMessage::system("Answer the user's question using the tool results already in the conversation. Do not call tools."));
    let filter = std::env::var("CCP_CURSOR_SAND_PROBE_CASE").ok();
    let first_prompt = if filter.as_deref() == Some("signed_text_continuation") {
        "Calculate (8473 * 269) + (119 * 863). Reply only with the resulting integer."
    } else {
        "Reply exactly PONG."
    };
    let cases = vec![
        (
            "first_text",
            vec![SandInferenceMessage::user(first_prompt)],
            Vec::new(),
        ),
        (
            "text_continuation",
            vec![
                SandInferenceMessage::user("Reply exactly PONG."),
                SandInferenceMessage::assistant("PONG"),
                SandInferenceMessage::user("Now reply exactly READY."),
            ],
            Vec::new(),
        ),
        ("signed_text_continuation", Vec::new(), Vec::new()),
        ("native_history_no_catalog", native_bridge, Vec::new()),
        (
            "native_history_with_catalog",
            native_messages,
            tools_from_anthropic(&native, false),
        ),
        (
            "text_history_no_catalog",
            vec![
                SandInferenceMessage::system(
                    "Answer using the supplied tool result. Do not call tools.",
                ),
                SandInferenceMessage::user("What is the returned probe value?"),
                SandInferenceMessage::assistant(
                    "<tool_call><name>Read</name><arguments>{\"file_path\":\"/synthetic/probe.txt\"}</arguments></tool_call>",
                ),
                SandInferenceMessage::user(
                    "<tool_result tool_use_id=\"toolu_probe\" name=\"Read\">PROBE_VALUE=17</tool_result>",
                ),
            ],
            Vec::new(),
        ),
    ];
    let mut first_response = None;
    for (label, mut messages, tools) in cases {
        if filter.as_deref().is_some_and(|selected| selected != label) && label != "first_text" {
            continue;
        }
        if label == "signed_text_continuation" {
            let Some(answer) = first_response.as_ref() else {
                eprintln!("case={label} outcome=skipped_no_first_response");
                continue;
            };
            let history: MessagesRequest = serde_json::from_value(json!({
                "model": model,
                "messages": [
                    {"role": "user", "content": first_prompt},
                    {"role": "assistant", "content": answer},
                    {"role": "user", "content": "Now reply exactly READY."}
                ]
            }))?;
            messages = messages_from_anthropic(&history, false);
            if messages
                .iter()
                .all(|message| message.reasoning_parts.is_empty())
            {
                eprintln!("case={label} outcome=skipped_no_upstream_signature");
                continue;
            }
            eprintln!(
                "case={label} replayed_reasoning_parts={}",
                messages
                    .iter()
                    .map(|message| message.reasoning_parts.len())
                    .sum::<usize>()
            );
        }
        capture.shapes.lock().unwrap().clear();
        let request = SandInferenceRequest::new(
            resolve_sand_model_id(&model),
            uuid::Uuid::new_v4().to_string(),
            uuid::Uuid::new_v4().to_string(),
            messages,
        )
        .with_max_mode(true)
        .with_parameter_model_id(&model)
        .with_max_tokens(Some(16384))
        .with_tools(tools);
        let shape = request.to_json_value();
        let roles: Vec<_> = shape["messages"].as_array().unwrap().iter().map(|message| json!({
            "role": message["role"], "keys": message.as_object().unwrap().keys().collect::<Vec<_>>()
        })).collect();
        eprintln!(
            "case={label} request_model={model} messages={} tools={}",
            json!(roles),
            request.tools.len()
        );
        let start = Instant::now();
        let mut counts = BTreeMap::<String, usize>::new();
        let mut text_bytes = 0usize;
        let mut answer = AnthropicJsonAcc::new(0);
        let result = tokio::time::timeout(timeout, async {
            let mut stream = client.open(&auth.access_token, &request).await?;
            while let Some(event) = stream.next().await {
                let event = event?;
                answer.push(&event);
                let key = match event {
                    CursorStreamEvent::TextDelta { text } => {
                        text_bytes += text.len();
                        "text"
                    }
                    CursorStreamEvent::ThinkingDelta { .. } => "thinking",
                    CursorStreamEvent::ThinkingSignature { .. } => "signature",
                    CursorStreamEvent::ThinkingCompleted => "thinking_end",
                    CursorStreamEvent::NativeTool { .. } => "tool",
                    CursorStreamEvent::Session { .. } => "session",
                    CursorStreamEvent::Usage { .. } => "usage",
                    CursorStreamEvent::OutputTokenDelta { .. } => "output_token",
                    CursorStreamEvent::End => "end",
                };
                *counts.entry(key.into()).or_default() += 1;
            }
            Ok::<_, claude_cursor_proxy::providers::cursor::client::CursorError>(())
        })
        .await;
        let outcome = match result {
            Ok(Ok(())) if answer.has_useful() => "complete".to_string(),
            Ok(Ok(())) => "hollow".to_string(),
            Ok(Err(error)) => format!("error_status={}", error.status),
            Err(_) => "timeout".to_string(),
        };
        if label == "first_text" && outcome == "complete" {
            first_response = Some(answer.into_message_json("probe", &model)["content"].clone());
        }
        eprintln!(
            "case={label} outcome={outcome} seconds={:.2} events={} text_bytes={text_bytes} frame_shapes={}",
            start.elapsed().as_secs_f64(),
            json!(counts),
            json!(*capture.shapes.lock().unwrap())
        );
    }
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}
