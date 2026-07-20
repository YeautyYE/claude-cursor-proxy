//! Emulate Anthropic hosted `web_search_20250305` for Claude Code.
//!
//! Claude Code's `WebSearchTool` (and `/deep-research` agents) nest a
//! `/v1/messages` call with `tools: [{type:"web_search_20250305",…}]` and
//! `tool_choice: {type:"tool", name:"web_search"}`, then expect Anthropic
//! `server_tool_use` + `web_search_tool_result` SSE blocks. Cursor has no
//! equivalent hosted tool, so we run a lightweight HTML search and synthesize
//! that wire shape.

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream;
use http::StatusCode;
use std::convert::Infallible;

use crate::anthropic::schema::MessagesRequest;
use crate::providers::cursor::sse::{
    EVENT_CONTENT_BLOCK_DELTA, EVENT_CONTENT_BLOCK_START, EVENT_CONTENT_BLOCK_STOP,
    EVENT_MESSAGE_DELTA, EVENT_MESSAGE_START, EVENT_MESSAGE_STOP, format_sse_event_bytes,
};

#[derive(Debug, Clone)]
pub struct WebSearchHit {
    pub title: String,
    pub url: String,
}

/// True when this Messages request is Claude Code's nested hosted web_search.
///
/// Only matches **pure** hosted-search calls (no ordinary client tools like
/// Read/Bash). Main agent turns that happen to list `web_search_20250305`
/// alongside other tools still go through the Cursor live path.
pub fn is_hosted_web_search_request(req: &MessagesRequest) -> bool {
    let tools = req
        .extra
        .get("tools")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let is_hosted_tool = |tool: &serde_json::Value| -> bool {
        let ty = tool.get("type").and_then(|v| v.as_str()).unwrap_or("");
        ty.starts_with("web_search_20")
            || (tool.get("name").and_then(|v| v.as_str()) == Some("web_search")
                && ty.contains("web_search"))
    };

    let has_hosted = tools.iter().any(is_hosted_tool);
    let has_client_tools = tools.iter().any(|tool| {
        if is_hosted_tool(tool) {
            return false;
        }
        // Anthropic client tools always carry a name (+ usually input_schema).
        tool.get("name")
            .and_then(|v| v.as_str())
            .is_some_and(|n| !n.is_empty())
    });

    if has_hosted && !has_client_tools {
        return true;
    }

    // tool_choice forced to web_search with no client tools (nested WebSearchTool).
    let choice = req.extra.get("tool_choice");
    let forced = match choice {
        Some(serde_json::Value::Object(map)) => {
            map.get("type").and_then(|v| v.as_str()) == Some("tool")
                && map.get("name").and_then(|v| v.as_str()) == Some("web_search")
        }
        Some(serde_json::Value::String(s)) => s == "web_search",
        _ => false,
    };
    forced && !has_client_tools
}

/// Pull the search query Claude Code embeds in the nested user message.
pub fn extract_web_search_query(req: &MessagesRequest) -> Option<String> {
    for message in req.messages.iter().rev() {
        if message.role != "user" {
            continue;
        }
        let text = match &message.content {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Array(blocks) => blocks
                .iter()
                .filter_map(|b| {
                    if b.get("type").and_then(|t| t.as_str()) == Some("text") {
                        b.get("text").and_then(|t| t.as_str()).map(str::to_string)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("\n"),
            _ => continue,
        };
        let trimmed = text.trim();
        if trimmed.is_empty() {
            continue;
        }
        const PREFIXES: &[&str] = &[
            "Perform a web search for the query: ",
            "Perform a web search for the query:",
            "Search the web for: ",
            "Search the web for:",
        ];
        for prefix in PREFIXES {
            if let Some(rest) = trimmed.strip_prefix(prefix) {
                let q = rest.trim().trim_matches('"').trim();
                if !q.is_empty() {
                    return Some(q.to_string());
                }
            }
        }
        // Fallback: whole user text if short enough to be a query.
        if trimmed.len() <= 500 && !trimmed.contains('\n') {
            return Some(trimmed.to_string());
        }
        return Some(trimmed.chars().take(400).collect());
    }
    None
}

/// Run HTML search (DuckDuckGo) and return title/url hits.
pub async fn search_web(query: &str) -> Result<Vec<WebSearchHit>, String> {
    let query = query.trim();
    if query.is_empty() {
        return Err("empty web search query".into());
    }
    let client = reqwest::Client::builder()
        .user_agent("claude-cursor-bridge/web-search (compatible; +https://github.com/YeautyYE/claude-cursor-bridge)")
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|e| format!("web search client: {e}"))?;

    let url = format!(
        "https://html.duckduckgo.com/html/?q={}",
        urlencoding_encode(query)
    );
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("web search request failed: {e}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "web search upstream HTTP {}",
            response.status().as_u16()
        ));
    }
    let html = response
        .text()
        .await
        .map_err(|e| format!("web search body: {e}"))?;
    Ok(parse_duckduckgo_html(&html))
}

fn urlencoding_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char);
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Minimal DuckDuckGo HTML result parser (result__a anchors).
pub fn parse_duckduckgo_html(html: &str) -> Vec<WebSearchHit> {
    let mut hits = Vec::new();
    let mut rest = html;
    while let Some(idx) = rest.find("result__a") {
        let window = &rest[idx..];
        let href = match extract_attr(window, "href") {
            Some(h) => decode_ddg_redirect(&h),
            None => {
                rest = &rest[idx + 8..];
                continue;
            }
        };
        let title = extract_anchor_text(window).unwrap_or_else(|| href.clone());
        if !href.is_empty() && href.starts_with("http") {
            hits.push(WebSearchHit {
                title: html_unescape(&title),
                url: href,
            });
        }
        rest = &rest[idx + 8..];
        if hits.len() >= 8 {
            break;
        }
    }
    hits
}

fn extract_attr<'a>(s: &'a str, name: &str) -> Option<String> {
    let key = format!("{name}=\"");
    let start = s.find(&key)? + key.len();
    let end = s[start..].find('"')? + start;
    Some(s[start..end].to_string())
}

fn extract_anchor_text(s: &str) -> Option<String> {
    let start = s.find('>')? + 1;
    let end = s[start..].find('<')? + start;
    let text = s[start..end].trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

fn decode_ddg_redirect(href: &str) -> String {
    // //duckduckgo.com/l/?uddg=<urlencoded>&...
    if let Some(q) = href.find("uddg=") {
        let rest = &href[q + 5..];
        let enc = rest.split('&').next().unwrap_or(rest);
        if let Ok(decoded) = urlencoding_decode(enc) {
            return decoded;
        }
    }
    if href.starts_with("//") {
        return format!("https:{href}");
    }
    href.to_string()
}

fn urlencoding_decode(s: &str) -> Result<String, ()> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let h = std::str::from_utf8(&bytes[i + 1..i + 3]).map_err(|_| ())?;
                let v = u8::from_str_radix(h, 16).map_err(|_| ())?;
                out.push(v);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| ())
}

fn html_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

/// Build Anthropic SSE that Claude Code's WebSearchTool.call() understands.
pub fn hosted_web_search_sse_response(
    message_id: String,
    model: String,
    query: String,
    hits: Vec<WebSearchHit>,
    error: Option<String>,
) -> Response {
    let tool_use_id = format!("srvtoolu_{}", uuid::Uuid::new_v4().simple());
    let mut events: Vec<Bytes> = Vec::new();

    let message_start = serde_json::json!({
        "type": "message_start",
        "message": {
            "id": message_id,
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [],
            "stop_reason": null,
            "stop_sequence": null,
            "usage": {"input_tokens": (query.len() / 4).max(1), "output_tokens": 0}
        }
    });
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_MESSAGE_START,
        &message_start,
    )));

    let server_tool_start = serde_json::json!({
        "type": "content_block_start",
        "index": 0,
        "content_block": {
            "type": "server_tool_use",
            "id": tool_use_id,
            "name": "web_search",
            "input": {}
        }
    });
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_CONTENT_BLOCK_START,
        &server_tool_start,
    )));

    let partial = serde_json::json!({"query": query}).to_string();
    let input_delta = serde_json::json!({
        "type": "content_block_delta",
        "index": 0,
        "delta": {"type": "input_json_delta", "partial_json": partial}
    });
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_CONTENT_BLOCK_DELTA,
        &input_delta,
    )));
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_CONTENT_BLOCK_STOP,
        &serde_json::json!({"type": "content_block_stop", "index": 0}),
    )));

    let content = if let Some(err) = error {
        serde_json::json!({"error_code": err})
    } else {
        serde_json::Value::Array(
            hits.iter()
                .map(|h| {
                    serde_json::json!({
                        "type": "web_search_result",
                        "title": h.title,
                        "url": h.url,
                    })
                })
                .collect(),
        )
    };

    let result_start = serde_json::json!({
        "type": "content_block_start",
        "index": 1,
        "content_block": {
            "type": "web_search_tool_result",
            "tool_use_id": tool_use_id,
            "content": content
        }
    });
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_CONTENT_BLOCK_START,
        &result_start,
    )));
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_CONTENT_BLOCK_STOP,
        &serde_json::json!({"type": "content_block_stop", "index": 1}),
    )));

    let message_delta = serde_json::json!({
        "type": "message_delta",
        "delta": {"stop_reason": "end_turn", "stop_sequence": null},
        "usage": {
            "output_tokens": 32,
            "server_tool_use": {"web_search_requests": 1}
        }
    });
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_MESSAGE_DELTA,
        &message_delta,
    )));
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_MESSAGE_STOP,
        &serde_json::json!({"type": "message_stop"}),
    )));

    let stream = stream::iter(events.into_iter().map(Ok::<Bytes, Infallible>));
    let mut response = Body::from_stream(stream).into_response();
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/event-stream"),
    );
    response.headers_mut().insert(
        http::header::CACHE_CONTROL,
        http::HeaderValue::from_static("no-cache"),
    );
    response
}

/// Non-streaming JSON body for the same hosted web_search shape.
pub fn hosted_web_search_json_response(
    message_id: String,
    model: String,
    query: String,
    hits: Vec<WebSearchHit>,
    error: Option<String>,
) -> Response {
    let tool_use_id = format!("srvtoolu_{}", uuid::Uuid::new_v4().simple());
    let content = if let Some(err) = error {
        serde_json::json!([
            {
                "type": "server_tool_use",
                "id": tool_use_id,
                "name": "web_search",
                "input": {"query": query}
            },
            {
                "type": "web_search_tool_result",
                "tool_use_id": tool_use_id,
                "content": {"error_code": err}
            }
        ])
    } else {
        serde_json::json!([
            {
                "type": "server_tool_use",
                "id": tool_use_id,
                "name": "web_search",
                "input": {"query": query}
            },
            {
                "type": "web_search_tool_result",
                "tool_use_id": tool_use_id,
                "content": hits.iter().map(|h| serde_json::json!({
                    "type": "web_search_result",
                    "title": h.title,
                    "url": h.url,
                })).collect::<Vec<_>>()
            }
        ])
    };
    let body = serde_json::json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": (query.len() / 4).max(1),
            "output_tokens": 32,
            "server_tool_use": {"web_search_requests": 1}
        }
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::schema::Message;

    #[test]
    fn detects_hosted_web_search_tool() {
        let req = MessagesRequest {
            model: Some("claude-fable-5".into()),
            max_tokens: Some(1024),
            messages: vec![],
            stream: true,
            extra: serde_json::json!({
                "tools": [{"type":"web_search_20250305","name":"web_search"}]
            })
            .as_object()
            .cloned()
            .unwrap(),
        };
        assert!(is_hosted_web_search_request(&req));
    }

    #[test]
    fn ignores_mixed_client_and_hosted_tools() {
        let req = MessagesRequest {
            model: Some("claude-fable-5".into()),
            max_tokens: Some(1024),
            messages: vec![],
            stream: true,
            extra: serde_json::json!({
                "tools": [
                    {"type":"web_search_20250305","name":"web_search"},
                    {"name":"Read","description":"read","input_schema":{}}
                ]
            })
            .as_object()
            .cloned()
            .unwrap(),
        };
        assert!(!is_hosted_web_search_request(&req));
    }

    #[test]
    fn extracts_prefixed_query() {
        let req = MessagesRequest {
            model: Some("claude-fable-5".into()),
            max_tokens: Some(1024),
            messages: vec![Message {
                role: "user".into(),
                content: serde_json::json!(
                    "Perform a web search for the query: rust async channels"
                ),
            }],
            stream: true,
            extra: Default::default(),
        };
        assert_eq!(
            extract_web_search_query(&req).as_deref(),
            Some("rust async channels")
        );
    }

    #[test]
    fn parses_ddg_result_anchors() {
        let html = r#"
            <a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fpage&rut=1">Example Title</a>
            <a class="result__a" href="https://direct.example/x">Direct</a>
        "#;
        let hits = parse_duckduckgo_html(html);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].url, "https://example.com/page");
        assert_eq!(hits[0].title, "Example Title");
        assert_eq!(hits[1].url, "https://direct.example/x");
    }
}
