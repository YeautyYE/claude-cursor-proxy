use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use claude_cursor_proxy::MessagesRequest;
use claude_cursor_proxy::media::{inbound_bearer, valid_video_id};
use claude_cursor_proxy::openai::{
    AnthropicToResponses, catalog_model, messages_json_to_responses, responses_to_messages,
};
use claude_cursor_proxy::providers::cursor::model::{
    apply_effort_to_cursor_model, resolve_cursor_model,
};
use claude_cursor_proxy::providers::grok::translate::model_allowlist::assert_allowed_model;
use claude_cursor_proxy::providers::grok::translate::reducer::reduce_upstream_bytes;
use claude_cursor_proxy::providers::grok::translate::request::translate_request;
use claude_cursor_proxy::providers::grok::{
    extract_upstream_error_message, grok_passthrough_failed_event,
    grok_passthrough_request_headers, mapped_upstream_status,
};
use claude_cursor_proxy::registry::Registry;
use claude_cursor_proxy::server::app;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::util::ServiceExt;

fn json_body(value: &Value) -> Body {
    Body::from(serde_json::to_vec(value).expect("json"))
}

async fn json_response(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let value = serde_json::from_slice(&bytes).unwrap_or(json!({}));
    (status, value)
}

#[tokio::test]
async fn models_catalog_exposes_grok_build_fields() {
    let app = app(Arc::new(Registry::with_default_alias()));
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/v1/models")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = json_response(response).await;
    assert_eq!(status, StatusCode::OK);
    let data = body["data"].as_array().expect("data array");
    assert!(
        data.iter()
            .any(|model| model["id"].as_str() == Some("grok-4.6")),
        "catalog must list grok-4.6: {body}"
    );
    for model in data {
        let id = model["id"].as_str().unwrap_or_default();
        let backend = model["api_backend"].as_str().unwrap_or_default();
        let context_window = model["context_window"].as_u64().unwrap_or(0);
        assert!(
            !model["model"].as_str().unwrap_or_default().is_empty(),
            "{id} missing model"
        );
        assert!(
            matches!(backend, "messages" | "responses"),
            "{id} api_backend={backend:?} must be messages or responses"
        );
        assert!(context_window > 0, "{id} context_window must be > 0");
        if id.starts_with("grok-") {
            assert_eq!(backend, "responses", "grok models must advertise responses");
            assert_ne!(
                backend, "chat_completions",
                "{id} must not fall through to chat_completions"
            );
        } else {
            assert_eq!(backend, "messages", "{id} non-grok models use messages");
        }
    }

    let grok46 = data
        .iter()
        .find(|model| model["id"].as_str() == Some("grok-4.6"))
        .expect("grok-4.6");
    assert_eq!(grok46["supports_reasoning_effort"], true);
    assert_eq!(grok46["reasoning_effort"], "high");
    assert_eq!(grok46["supports_backend_search"], true);
    assert_eq!(grok46["context_window"], 500_000);
    let efforts = grok46["reasoning_efforts"].as_array().expect("4.6 menu");
    let values: Vec<&str> = efforts
        .iter()
        .filter_map(|item| item["value"].as_str())
        .collect();
    assert_eq!(values, ["xhigh", "high", "medium", "low"]);
    assert!(
        efforts.iter().any(|item| item["value"] == "low"
            && item["description"]
                .as_str()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .contains("fast")),
        "grok-4.6 low must be the Fast menu entry: {grok46}"
    );
    assert!(
        efforts
            .iter()
            .any(|item| item["value"] == "high" && item["default"] == true),
        "grok-4.6 high must be default: {grok46}"
    );

    let grok45 = data
        .iter()
        .find(|model| model["id"].as_str() == Some("grok-4.5"))
        .expect("grok-4.5");
    assert_eq!(grok45["supports_reasoning_effort"], true);
    assert_eq!(grok45["reasoning_effort"], "high");
    assert_eq!(grok45["supports_backend_search"], false);
    let values45: Vec<&str> = grok45["reasoning_efforts"]
        .as_array()
        .expect("4.5 menu")
        .iter()
        .filter_map(|item| item["value"].as_str())
        .collect();
    assert_eq!(values45, ["high", "medium", "low"]);

    let catalog46 = catalog_model("grok-4.6", "grok");
    assert_eq!(catalog46["api_backend"], "responses");
    assert_eq!(catalog46["supports_reasoning_effort"], true);
    assert!(
        catalog46["reasoning_efforts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["value"] == "low")
    );

    let fable = catalog_model("claude-fable-5[1m]", "cursor");
    assert_eq!(fable["supports_reasoning_effort"], true);
    assert!(
        fable["reasoning_efforts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["value"] == "low")
    );
    let composer = catalog_model("composer-2.5", "cursor");
    assert_eq!(composer["supports_reasoning_effort"], true);
    let gpt = catalog_model("gpt-5.4", "cursor");
    assert_eq!(
        gpt["supports_reasoning_effort"], false,
        "gpt catalog ids must not advertise a no-op effort menu: {gpt}"
    );
    assert!(gpt.get("reasoning_efforts").is_none());
    let gemini = catalog_model("gemini-3-pro", "cursor");
    assert_eq!(gemini["supports_reasoning_effort"], false);
    let cursor_grok = catalog_model("cursor-grok-4.5-high-fast", "cursor");
    assert_eq!(
        cursor_grok["supports_reasoning_effort"], false,
        "cursor-grok catalog ids must not advertise a no-op effort menu: {cursor_grok}"
    );
    assert!(cursor_grok.get("reasoning_efforts").is_none());
    let auto = catalog_model("auto", "cursor");
    assert_eq!(
        auto["supports_reasoning_effort"], false,
        "auto must not advertise a no-op effort menu: {auto}"
    );
    assert!(auto.get("reasoning_efforts").is_none());
}

#[tokio::test]
async fn responses_unknown_model_is_400_without_auth() {
    let app = app(Arc::new(Registry::with_default_alias()));
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(json_body(&json!({
                    "model": "not-a-model",
                    "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],
                    "stream": true
                })))
                .unwrap(),
        )
        .await
        .unwrap();

    let (status, body) = json_response(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], json!("invalid_request_error"));
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("Unknown model"),
        "{body}"
    );
}

#[tokio::test]
async fn media_routes_reject_oversize_and_exist() {
    let app = app(Arc::new(Registry::with_default_alias()));
    let small = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/generations")
                .header("content-type", "application/json")
                .body(json_body(&json!({"prompt":"a cat","n":1})))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(
        small.status(),
        StatusCode::NOT_FOUND,
        "image generation route must exist"
    );

    for path in [
        "/v1/images/edits",
        "/v1/videos/generations",
        "/v1/videos/req_test",
    ] {
        let method = if path.starts_with("/v1/videos/req_") {
            Method::GET
        } else {
            Method::POST
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(if path.starts_with("/v1/videos/req_") {
                        Body::empty()
                    } else {
                        json_body(&json!({"prompt":"x"}))
                    })
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(response.status(), StatusCode::NOT_FOUND, "{path} missing");
    }

    let oversize = vec![b'x'; 20 * 1024 * 1024 + 1];
    let rejected = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/images/generations")
                .header("content-type", "application/json")
                .body(Body::from(oversize))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        matches!(
            rejected.status(),
            StatusCode::PAYLOAD_TOO_LARGE | StatusCode::BAD_REQUEST
        ),
        "oversize media must be rejected, got {}",
        rejected.status()
    );
}

#[test]
fn grok_reducer_ignores_unknown_events() {
    let input = concat!(
        "data: {\"type\":\"response.doom_loop_check\",\"doom_loop_check\":{}}\n\n",
        "data: {\"type\":\"response.queued\"}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
    );
    let events = reduce_upstream_bytes(input.as_bytes()).expect("unknown events must not fail");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, claude_cursor_proxy::providers::grok::translate::reducer::ReducerEvent::TextDelta(_, delta) if delta == "hi"))
    );
    assert!(matches!(
        events.last(),
        Some(claude_cursor_proxy::providers::grok::translate::reducer::ReducerEvent::Finish { .. })
    ));
}

fn sse_data_values(raw: &str) -> Vec<Value> {
    raw.split("\n\n")
        .filter_map(|block| {
            block
                .lines()
                .find_map(|line| line.strip_prefix("data: "))
                .filter(|data| *data != "[DONE]")
                .and_then(|data| serde_json::from_str(data).ok())
        })
        .collect()
}

#[test]
fn anthropic_to_responses_matches_grok_build_sse_contract() {
    let mut translator = AnthropicToResponses::new("resp_1".into(), "claude-fable-5[1m]".into());
    let first = translator.push(
        br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]"}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}

event: message_stop
data: {"type":"message_stop"}

"#,
    );
    let events = sse_data_values(&String::from_utf8(first).unwrap());
    assert!(
        events
            .iter()
            .any(|event| event["type"] == "response.created"
                && event["sequence_number"].as_u64() == Some(0)
                && event["response"]["id"] == "resp_1"),
        "{events:?}"
    );
    let delta = events
        .iter()
        .find(|event| event["type"] == "response.output_text.delta")
        .expect("text delta");
    assert!(delta["sequence_number"].as_u64().is_some(), "{delta}");
    assert!(!delta["item_id"].as_str().unwrap_or_default().is_empty());
    assert_eq!(delta["output_index"], 0);
    assert_eq!(delta["content_index"], 0);
    assert_eq!(delta["delta"], "hi");
    let created = events
        .iter()
        .find(|event| event["type"] == "response.created")
        .expect("created");
    assert_typed_usage(&created["response"]["usage"]);
    let part_done = events
        .iter()
        .find(|event| event["type"] == "response.content_part.done")
        .expect("content_part.done");
    assert_eq!(part_done["part"]["type"], "output_text");
    assert_eq!(part_done["part"]["text"], "hi");
    assert!(part_done["part"].get("annotations").is_some());
    let completed = events
        .iter()
        .find(|event| event["type"] == "response.completed")
        .expect("completed");
    assert!(completed["sequence_number"].as_u64().is_some());
    assert_eq!(completed["response"]["status"], "completed");
    assert_typed_usage(&completed["response"]["usage"]);
    assert!(
        completed["response"]["output"]
            .as_array()
            .is_some_and(|output| !output.is_empty()),
        "completed must include output: {completed}"
    );

    let mut tools = AnthropicToResponses::new("resp_tool".into(), "claude-fable-5[1m]".into());
    let tool_bytes = tools.push(
        br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]"}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_1","name":"lookup"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"1}"}}

event: message_stop
data: {"type":"message_stop"}

"#,
    );
    let tool_events = sse_data_values(&String::from_utf8(tool_bytes).unwrap());
    let tool_completed = tool_events
        .iter()
        .find(|event| event["type"] == "response.completed")
        .expect("tool completed");
    assert_eq!(
        tool_completed["response"]["output"][0]["arguments"], "{\"q\":1}",
        "final function_call arguments must accumulate deltas: {tool_completed}"
    );
    assert_typed_usage(&tool_completed["response"]["usage"]);
    let rest = translator.finish();
    assert!(
        !String::from_utf8(rest)
            .unwrap()
            .contains("response.completed")
    );

    let mut failed = AnthropicToResponses::new("resp_err".into(), "claude-fable-5[1m]".into());
    let error_bytes = failed.push(
        br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]"}}

event: error
data: {"type":"error","error":{"type":"api_error","message":"boom"}}

"#,
    );
    let error_events = sse_data_values(&String::from_utf8(error_bytes).unwrap());
    assert!(
        error_events
            .iter()
            .any(|event| event["type"] == "response.failed"
                && event["response"]["status"] == "failed"),
        "{error_events:?}"
    );
    assert!(
        !error_events
            .iter()
            .any(|event| event["type"] == "response.completed"),
        "errors must not be completed: {error_events:?}"
    );
    let after_fail = failed.finish();
    assert!(
        !String::from_utf8(after_fail)
            .unwrap()
            .contains("response.completed")
    );

    let mut broken = AnthropicToResponses::new("resp_brk".into(), "claude-fable-5[1m]".into());
    let _ = broken.push(
        br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]"}}

"#,
    );
    let failed_finish = String::from_utf8(broken.fail("upstream stream failed")).unwrap();
    let failed_events = sse_data_values(&failed_finish);
    assert!(
        failed_events
            .iter()
            .any(|event| event["type"] == "response.failed"),
        "{failed_events:?}"
    );
    assert!(
        !failed_events
            .iter()
            .any(|event| event["type"] == "response.completed")
    );
}

#[test]
fn responses_to_messages_preserves_sampling_and_tool_choice() {
    let request = responses_to_messages(&json!({
        "model": "claude-fable-5[1m]",
        "instructions": "rules",
        "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],
        "tools": [{"type":"function","name":"lookup","parameters":{"type":"object"}}],
        "tool_choice": {"type":"function","name":"lookup"},
        "temperature": 0.2,
        "top_p": 0.9,
        "previous_response_id": "resp_prev",
        "reasoning": {"effort": "low"},
        "stream": true
    }))
    .unwrap();
    assert_eq!(request.extra["output_config"]["effort"], "low");
    assert_eq!(request.extra["tool_choice"]["type"], "tool");
    assert_eq!(request.extra["tool_choice"]["name"], "lookup");
    assert_eq!(request.extra["temperature"], 0.2);
    assert_eq!(request.extra["top_p"], 0.9);
    assert_eq!(request.extra["previous_response_id"], "resp_prev");
}

#[test]
fn media_video_id_rejects_traversal() {
    assert!(valid_video_id("req_abc-1"));
    assert!(valid_video_id("req.abc:1"));
    assert!(!valid_video_id(""));
    assert!(!valid_video_id(".."));
    assert!(!valid_video_id("foo..bar"));
    assert!(!valid_video_id("../secret"));
    assert!(!valid_video_id("a/b"));
    assert!(!valid_video_id(&"a".repeat(129)));
}

#[test]
fn inbound_bearer_ignores_placeholders_and_non_bearer() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Basic abc"),
    );
    assert_eq!(inbound_bearer(&headers), None);

    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer unused"),
    );
    assert_eq!(inbound_bearer(&headers), None);

    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer placeholder"),
    );
    assert_eq!(inbound_bearer(&headers), None);

    headers.insert("x-api-key", HeaderValue::from_static("unused"));
    assert_eq!(inbound_bearer(&headers), None);

    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer xai-real-token"),
    );
    assert_eq!(inbound_bearer(&headers).as_deref(), Some("xai-real-token"));

    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static(
            "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJncm9rLXNlc3Npb24ifQ.signature",
        ),
    );
    assert_eq!(
        inbound_bearer(&headers),
        None,
        "grok-build session JWTs must not be forwarded as xAI media keys"
    );
}

#[test]
fn grok_45_and_46_are_allowed() {
    assert!(assert_allowed_model("grok-4.5").is_ok());
    assert!(assert_allowed_model("grok-4.6").is_ok());
    assert!(assert_allowed_model("grok-not-a-model").is_err());

    let request: MessagesRequest = serde_json::from_value(json!({
        "model": "grok-4.6",
        "messages": [{"role":"user","content":"hi"}],
        "output_config": {"effort":"low"}
    }))
    .unwrap();
    let translated =
        serde_json::to_value(translate_request(&request, "grok-4.6".into()).unwrap()).unwrap();
    assert_eq!(translated["model"], "grok-4.6");
    assert_eq!(translated["reasoning"]["effort"], "low");
    assert_eq!(translated["reasoning"]["summary"], "concise");
}

#[test]
fn invalid_effort_is_rejected() {
    let request: MessagesRequest = serde_json::from_value(json!({
        "model": "grok-4.6",
        "messages": [{"role":"user","content":"hi"}],
        "output_config": {"effort":"typo"}
    }))
    .unwrap();
    let error = translate_request(&request, "grok-4.6".into())
        .err()
        .expect("invalid effort must fail closed");
    assert!(
        error.to_string().contains("effort"),
        "translate must name the bad effort: {error}"
    );

    let minimal: MessagesRequest = serde_json::from_value(json!({
        "model": "grok-4.6",
        "messages": [{"role":"user","content":"hi"}],
        "output_config": {"effort":"minimal"}
    }))
    .unwrap();
    let translated =
        serde_json::to_value(translate_request(&minimal, "grok-4.6".into()).unwrap()).unwrap();
    assert_eq!(
        translated["reasoning"]["effort"], "low",
        "minimal must map to Grok low/fast: {translated}"
    );

    let converted = responses_to_messages(&json!({
        "model": "claude-fable-5[1m]",
        "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],
        "reasoning": {"effort": "typo"}
    }));
    assert!(
        converted.is_err(),
        "invalid reasoning.effort must fail before provider dispatch"
    );
}

#[tokio::test]
async fn invalid_effort_is_rejected_on_http() {
    let messages_app = app(Arc::new(Registry::with_default_alias()));
    let response = messages_app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/messages")
                .header("content-type", "application/json")
                .body(json_body(&json!({
                    "model": "claude-fable-5[1m]",
                    "messages": [{"role":"user","content":"hi"}],
                    "output_config": {"effort":"typo"},
                    "max_tokens": 16,
                    "stream": false
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = json_response(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("effort"),
        "messages 400 must name the bad effort: {body}"
    );

    let responses_app = app(Arc::new(Registry::with_default_alias()));
    let response = responses_app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/responses")
                .header("content-type", "application/json")
                .body(json_body(&json!({
                    "model": "claude-fable-5[1m]",
                    "input": [{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}],
                    "reasoning": {"effort": "typo"},
                    "stream": false
                })))
                .unwrap(),
        )
        .await
        .unwrap();
    let (status, body) = json_response(response).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("effort"),
        "responses 400 must name the bad effort: {body}"
    );
}

#[test]
fn grok_upstream_4xx_is_preserved() {
    assert_eq!(
        mapped_upstream_status(StatusCode::BAD_REQUEST),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        mapped_upstream_status(StatusCode::NOT_FOUND),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        mapped_upstream_status(StatusCode::UNAUTHORIZED),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        mapped_upstream_status(StatusCode::INTERNAL_SERVER_ERROR),
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        extract_upstream_error_message(
            br#"{"error":{"message":"effort xhigh is not supported"}}"#,
            "fallback"
        ),
        "effort xhigh is not supported"
    );
    let redacted = extract_upstream_error_message(
        br#"{"error":{"message":"bad key Bearer sk-secret123"}}"#,
        "fallback",
    );
    assert!(!redacted.contains("sk-secret123"));
}

#[test]
fn cursor_fable_honors_grok_build_effort() {
    assert_eq!(
        apply_effort_to_cursor_model("claude-fable-5[1m]", Some("low")),
        "claude-fable-5-thinking-low[1m]"
    );
    assert_eq!(
        apply_effort_to_cursor_model("claude-fable-5[1m]", Some("fast")),
        "claude-fable-5-thinking-low[1m]"
    );
    assert_eq!(
        apply_effort_to_cursor_model("claude-fable-5[1m]", Some("high")),
        "claude-fable-5-thinking-high[1m]"
    );
    assert_eq!(
        apply_effort_to_cursor_model("claude-fable-5[1m]", Some("max")),
        "claude-fable-5-thinking-max[1m]"
    );
    assert_eq!(
        apply_effort_to_cursor_model("claude-fable-5[1m]", None),
        "claude-fable-5[1m]"
    );

    let low = resolve_cursor_model(&apply_effort_to_cursor_model(
        "claude-fable-5[1m]",
        Some("low"),
    ))
    .unwrap();
    assert_eq!(low.model_id, "claude-fable-5-thinking-low");
    let default = resolve_cursor_model("claude-fable-5[1m]").unwrap();
    assert_eq!(default.model_id, "claude-fable-5-thinking-max");
}

#[test]
fn anthropic_to_responses_tool_before_text_keeps_output_index() {
    let mut translator =
        AnthropicToResponses::new("resp_order".into(), "claude-fable-5[1m]".into());
    let bytes = translator.push(
        br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]"}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"call_1","name":"lookup"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":1}"}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"ok"}}

event: message_stop
data: {"type":"message_stop"}

"#,
    );
    let events = sse_data_values(&String::from_utf8(bytes).unwrap());
    let tool_added = events
        .iter()
        .find(|event| {
            event["type"] == "response.output_item.added"
                && event["item"]["type"] == "function_call"
        })
        .expect("tool added");
    assert_eq!(
        tool_added["output_index"], 0,
        "tool-before-text must claim output_index 0: {tool_added}"
    );
    let arg_delta = events
        .iter()
        .find(|event| event["type"] == "response.function_call_arguments.delta")
        .expect("arg delta");
    assert_eq!(
        arg_delta["output_index"], tool_added["output_index"],
        "argument deltas must stay on the tool item index: {arg_delta}"
    );
    let text_added = events
        .iter()
        .find(|event| {
            event["type"] == "response.output_item.added" && event["item"]["type"] == "message"
        })
        .expect("text added");
    assert_eq!(
        text_added["output_index"], 1,
        "later text must not collide with the tool index: {text_added}"
    );
    let text_delta = events
        .iter()
        .find(|event| event["type"] == "response.output_text.delta")
        .expect("text delta");
    assert_eq!(text_delta["output_index"], 1, "{text_delta}");
    let arg_done = events
        .iter()
        .find(|event| event["type"] == "response.function_call_arguments.done")
        .expect("arg done");
    assert_eq!(arg_done["output_index"], 0, "{arg_done}");
}

#[test]
fn anthropic_to_responses_maps_cache_read_tokens() {
    let mut translator =
        AnthropicToResponses::new("resp_cache".into(), "claude-fable-5[1m]".into());
    let bytes = translator.push(
        br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]","usage":{"input_tokens":12,"output_tokens":0,"cache_read_input_tokens":80}}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":20,"output_tokens":4,"cache_read_input_tokens":80,"cache_creation_input_tokens":3}}

event: message_stop
data: {"type":"message_stop"}

"#,
    );
    let events = sse_data_values(&String::from_utf8(bytes).unwrap());
    let completed = events
        .iter()
        .find(|event| event["type"] == "response.completed")
        .expect("completed");
    let usage = &completed["response"]["usage"];
    assert_eq!(usage["input_tokens"], 20);
    assert_eq!(usage["output_tokens"], 4);
    assert_eq!(
        usage["input_tokens_details"]["cached_tokens"], 80,
        "cache_read_input_tokens must map to cached_tokens: {usage}"
    );
    assert_eq!(
        usage["total_tokens"], 104,
        "total_tokens must include cache reads for grok-build context: {usage}"
    );
}

fn assert_typed_usage(usage: &Value) {
    assert!(usage["input_tokens"].as_u64().is_some(), "{usage}");
    assert!(usage["output_tokens"].as_u64().is_some(), "{usage}");
    assert!(usage["total_tokens"].as_u64().is_some(), "{usage}");
    assert!(
        usage["input_tokens_details"]["cached_tokens"]
            .as_u64()
            .is_some(),
        "usage must include input_tokens_details.cached_tokens: {usage}"
    );
    assert!(
        usage["output_tokens_details"]["reasoning_tokens"]
            .as_u64()
            .is_some(),
        "usage must include output_tokens_details.reasoning_tokens: {usage}"
    );
}

#[test]
fn anthropic_to_responses_eof_and_max_tokens() {
    let mut truncated = AnthropicToResponses::new("resp_eof".into(), "claude-fable-5[1m]".into());
    let _ = truncated.push(
        br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]"}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"partial"}}

"#,
    );
    let eof = sse_data_values(&String::from_utf8(truncated.finish()).unwrap());
    assert!(
        eof.iter()
            .any(|event| event["type"] == "response.failed"
                && event["response"]["status"] == "failed"),
        "EOF without message_stop must fail: {eof:?}"
    );
    assert!(
        !eof.iter()
            .any(|event| event["type"] == "response.completed"),
        "EOF must not fabricate completed: {eof:?}"
    );
    assert_typed_usage(
        &eof.iter()
            .find(|event| event["type"] == "response.failed")
            .unwrap()["response"]["usage"],
    );

    let mut limited = AnthropicToResponses::new("resp_max".into(), "claude-fable-5[1m]".into());
    let limited_bytes = limited.push(
        br#"event: message_start
data: {"type":"message_start","message":{"model":"claude-fable-5[1m]"}}

event: content_block_delta
data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"cut"}}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"input_tokens":2,"output_tokens":3}}

event: message_stop
data: {"type":"message_stop"}

"#,
    );
    let limited_events = sse_data_values(&String::from_utf8(limited_bytes).unwrap());
    let incomplete = limited_events
        .iter()
        .find(|event| event["type"] == "response.incomplete")
        .expect("max_tokens must emit response.incomplete");
    assert_eq!(incomplete["response"]["status"], "incomplete");
    assert_eq!(
        incomplete["response"]["incomplete_details"]["reason"],
        "max_output_tokens"
    );
    assert_typed_usage(&incomplete["response"]["usage"]);
    assert!(
        !limited_events
            .iter()
            .any(|event| event["type"] == "response.completed"),
        "max_tokens must not look completed: {limited_events:?}"
    );
}

#[test]
fn messages_json_to_responses_matches_contract() {
    let converted = messages_json_to_responses(
        &json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "claude-fable-5[1m]",
            "content": [{"type":"text","text":"hi"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 4, "output_tokens": 2}
        }),
        "resp_json",
        "claude-fable-5[1m]",
    );
    assert_eq!(converted["object"], "response");
    assert_eq!(converted["id"], "resp_json");
    assert_eq!(converted["status"], "completed");
    assert_eq!(converted["output"][0]["content"][0]["text"], "hi");
    assert_eq!(converted["usage"]["input_tokens"], 4);
    assert_eq!(converted["usage"]["output_tokens"], 2);
    assert_typed_usage(&converted["usage"]);

    let incomplete = messages_json_to_responses(
        &json!({
            "id": "msg_2",
            "type": "message",
            "role": "assistant",
            "model": "claude-fable-5[1m]",
            "content": [{"type":"text","text":"cut"}],
            "stop_reason": "max_tokens",
            "usage": {"input_tokens": 1, "output_tokens": 8}
        }),
        "resp_json_max",
        "claude-fable-5[1m]",
    );
    assert_eq!(incomplete["status"], "incomplete");
    assert_eq!(
        incomplete["incomplete_details"]["reason"],
        "max_output_tokens"
    );
}

#[test]
fn responses_to_messages_preserves_images_system_and_reasoning() {
    let request = responses_to_messages(&json!({
        "model": "claude-fable-5[1m]",
        "input": [
            {"type":"message","role":"system","content":[{"type":"input_text","text":"be brief"}]},
            {"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"think first"}]},
            {"type":"message","role":"user","content":[
                {"type":"input_text","text":"see"},
                {"type":"input_image","image_url":"data:image/png;base64,aGVsbG8="}
            ]}
        ],
        "stream": true
    }))
    .unwrap();
    assert_eq!(request.extra["system"], "be brief");
    assert!(
        request
            .messages
            .iter()
            .all(|message| message.role != "system"),
        "system input must not stay as a chat role: {request:?}"
    );
    let thinking = request
        .messages
        .iter()
        .find(|message| message.role == "assistant")
        .expect("reasoning becomes assistant thinking");
    assert_eq!(thinking.content[0]["type"], "thinking");
    assert_eq!(thinking.content[0]["thinking"], "think first");
    let user = request
        .messages
        .iter()
        .find(|message| message.role == "user")
        .expect("user");
    let image = user
        .content
        .as_array()
        .unwrap()
        .iter()
        .find(|block| block["type"] == "image" && block["source"]["type"] == "base64");
    let image = image.expect("data-URI must become base64 source");
    assert_eq!(image["source"]["media_type"], "image/png");
    assert_eq!(image["source"]["data"], "aGVsbG8=");
}

#[test]
fn grok_passthrough_failed_event_is_typed() {
    let event = grok_passthrough_failed_event("grok-4.6");
    assert_eq!(event["type"], "response.failed");
    assert_eq!(event["response"]["object"], "response");
    assert_eq!(event["response"]["status"], "failed");
    assert_eq!(event["response"]["model"], "grok-4.6");
    assert!(event["response"]["created_at"].as_u64().is_some());
    assert!(event["response"]["output"].as_array().is_some());
    assert_typed_usage(&event["response"]["usage"]);
    assert_eq!(event["response"]["error"]["code"], "server_error");
}

#[test]
fn grok_upstream_error_message_is_preserved() {
    assert_eq!(
        extract_upstream_error_message(
            br#"{"error":{"message":"effort xhigh is not supported"}}"#,
            "fallback"
        ),
        "effort xhigh is not supported"
    );
    assert_eq!(
        extract_upstream_error_message(b"not-json", "fallback"),
        "fallback"
    );
    let redacted = extract_upstream_error_message(
        br#"{"error":{"message":"bad key Bearer sk-secret123"}}"#,
        "fallback",
    );
    assert!(
        !redacted.contains("sk-secret123"),
        "upstream error text must not leak credentials: {redacted}"
    );
    assert!(!redacted.contains("Bearer sk-secret123"));
}

#[test]
fn grok_passthrough_headers_are_allowlisted() {
    let mut headers = HeaderMap::new();
    headers.insert("x-grok-conv-id", HeaderValue::from_static("conv-1"));
    headers.insert("x-compaction-at", HeaderValue::from_static("100"));
    headers.insert("x-compactions-remaining", HeaderValue::from_static("1"));
    headers.insert(
        "x-grok-model-override",
        HeaderValue::from_static("grok-4.6"),
    );
    headers.insert("x-grok-doom-loop-check", HeaderValue::from_static("1024"));
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer secret-token"),
    );
    headers.insert("x-grok-user-id", HeaderValue::from_static("user-1"));
    headers.insert(
        axum::http::header::COOKIE,
        HeaderValue::from_static("sid=abc"),
    );
    let forwarded = grok_passthrough_request_headers(&headers);
    let names: Vec<&str> = forwarded.iter().map(|(name, _)| name.as_str()).collect();
    assert!(names.contains(&"x-grok-conv-id"), "{names:?}");
    assert!(names.contains(&"x-compaction-at"), "{names:?}");
    assert!(names.contains(&"x-compactions-remaining"), "{names:?}");
    assert!(names.contains(&"x-grok-model-override"), "{names:?}");
    assert!(names.contains(&"x-grok-doom-loop-check"), "{names:?}");
    assert_eq!(
        forwarded
            .iter()
            .find(|(name, _)| name == "x-compactions-remaining")
            .map(|(_, value)| value.as_str()),
        Some("1")
    );
    assert_eq!(
        forwarded
            .iter()
            .find(|(name, _)| name == "x-grok-model-override")
            .map(|(_, value)| value.as_str()),
        Some("grok-4.6")
    );

    let mut rejected = HeaderMap::new();
    rejected.insert(
        "x-compactions-remaining",
        HeaderValue::from_static("1; rm -rf /"),
    );
    rejected.insert(
        "x-grok-model-override",
        HeaderValue::from_static("../etc/passwd"),
    );
    let rejected_forwarded = grok_passthrough_request_headers(&rejected);
    let rejected_names: Vec<&str> = rejected_forwarded
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert!(
        !rejected_names.contains(&"x-compactions-remaining"),
        "non-digit compaction budget must be dropped: {rejected_names:?}"
    );
    assert!(
        !rejected_names.contains(&"x-grok-model-override"),
        "path-like model override must be dropped: {rejected_names:?}"
    );
    assert!(
        !names
            .iter()
            .any(|name| name.eq_ignore_ascii_case("authorization")),
        "Authorization must never be forwarded: {names:?}"
    );
    assert!(
        !names.iter().any(|name| *name == "x-grok-user-id"),
        "x-grok-user-id must never be forwarded: {names:?}"
    );
    assert!(
        !names.iter().any(|name| name.eq_ignore_ascii_case("cookie")),
        "Cookie must never be forwarded: {names:?}"
    );
    assert!(
        !forwarded
            .iter()
            .any(|(_, value)| value.contains("secret-token") || value.contains("sid=abc")),
        "{forwarded:?}"
    );
}
