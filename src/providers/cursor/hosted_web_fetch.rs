//! Emulate Anthropic hosted `web_fetch_*` for Claude Code.
//!
//! Claude Code `/deep-research` Workflow agents nest a `/v1/messages` call with
//! `tools: [{type:"web_fetch_20250910"|"web_fetch_20260209", name:"web_fetch"}]`
//! (and sometimes `tool_choice: {type:"tool", name:"web_fetch"}`), then expect
//! Anthropic `server_tool_use` + `web_fetch_tool_result` SSE blocks. Cursor has
//! no equivalent hosted tool, so we GET the URL server-side and synthesize that
//! wire shape — nested-only, matching `hosted_web_search`.
//!
//! ## `mod.rs` wire-up
//!
//! After the hosted `web_search` gate in `handle_messages`:
//! ```ignore
//! if let Some(resp) =
//!     maybe_handle_hosted_web_fetch(&body, &message_id, &wire_model).await
//! {
//!     return resp;
//! }
//! ```
//! Plus `pub mod hosted_web_fetch;` and
//! `use crate::providers::cursor::hosted_web_fetch::maybe_handle_hosted_web_fetch;`.

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::stream;
use http::StatusCode;
use std::convert::Infallible;
use std::net::{IpAddr, ToSocketAddrs};
use std::time::Duration;

use crate::anthropic::error::json_error;
use crate::anthropic::schema::MessagesRequest;
use crate::providers::cursor::sse::{
    EVENT_CONTENT_BLOCK_DELTA, EVENT_CONTENT_BLOCK_START, EVENT_CONTENT_BLOCK_STOP,
    EVENT_MESSAGE_DELTA, EVENT_MESSAGE_START, EVENT_MESSAGE_STOP, format_sse_event_bytes,
};

const MAX_URL_LEN: usize = 250;
const MAX_BODY_BYTES: usize = 1_048_576;
const MAX_TEXT_CHARS: usize = 100_000;
const FETCH_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_REDIRECTS: usize = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchError {
    pub code: &'static str,
    pub message: String,
}

impl FetchError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FetchedPage {
    pub url: String,
    pub title: String,
    pub text: String,
    pub retrieved_at: String,
}

/// True when this Messages request is Claude Code's nested hosted web_fetch.
///
/// Only matches **pure** hosted-fetch calls (no ordinary client tools like
/// Read/Bash). A sibling hosted `web_search_*` without `tool_choice=web_fetch`
/// is left for the search emulator / Cursor live path.
pub fn is_hosted_web_fetch_request(req: &MessagesRequest) -> bool {
    let tools = req
        .extra
        .get("tools")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let has_hosted_fetch = tools.iter().any(is_hosted_fetch_tool);
    let has_hosted_search = tools.iter().any(is_hosted_search_tool);
    let has_client_tools = tools.iter().any(|tool| {
        if is_hosted_fetch_tool(tool) || is_hosted_search_tool(tool) {
            return false;
        }
        tool.get("name")
            .and_then(|v| v.as_str())
            .is_some_and(|n| !n.is_empty())
    });

    if has_client_tools {
        return false;
    }

    let forced = tool_choice_is_web_fetch(req);
    if forced {
        return true;
    }
    has_hosted_fetch && !has_hosted_search
}

fn is_hosted_fetch_tool(tool: &serde_json::Value) -> bool {
    let ty = tool.get("type").and_then(|v| v.as_str()).unwrap_or("");
    ty.starts_with("web_fetch_20")
        || (tool.get("name").and_then(|v| v.as_str()) == Some("web_fetch")
            && ty.contains("web_fetch"))
}

fn is_hosted_search_tool(tool: &serde_json::Value) -> bool {
    let ty = tool.get("type").and_then(|v| v.as_str()).unwrap_or("");
    ty.starts_with("web_search_20")
        || (tool.get("name").and_then(|v| v.as_str()) == Some("web_search")
            && ty.contains("web_search"))
}

fn tool_choice_is_web_fetch(req: &MessagesRequest) -> bool {
    match req.extra.get("tool_choice") {
        Some(serde_json::Value::Object(map)) => {
            map.get("type").and_then(|v| v.as_str()) == Some("tool")
                && map.get("name").and_then(|v| v.as_str()) == Some("web_fetch")
        }
        Some(serde_json::Value::String(s)) => s == "web_fetch",
        _ => false,
    }
}

/// Pull the fetch URL Claude Code embeds in the nested user message.
pub fn extract_web_fetch_url(req: &MessagesRequest) -> Option<String> {
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
        if let Some(url) = first_http_url(&text) {
            return Some(url);
        }
    }
    None
}

fn first_http_url(text: &str) -> Option<String> {
    let lower = text.to_ascii_lowercase();
    let https = lower.find("https://");
    let http = lower.find("http://");
    let start = match (https, http) {
        (Some(a), Some(b)) => a.min(b),
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => return None,
    };
    let after = &text[start..];
    let end_rel = after
        .find(|c: char| c.is_whitespace() || "<>\"'".contains(c))
        .unwrap_or(after.len());
    let raw = after[..end_rel].trim_end_matches(|c: char| {
        matches!(c, '.' | ',' | ';' | ':' | '!' | '?' | ')' | ']' | '}' | '(')
    });
    if raw.starts_with("http://") || raw.starts_with("https://") {
        Some(raw.to_string())
    } else {
        None
    }
}

/// Scheme / host SSRF gate (no DNS). Used by both the live GET and tests.
pub fn validate_fetch_url(url: &str) -> Result<url::Url, FetchError> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err(FetchError::new("invalid_input", "empty URL"));
    }
    if trimmed.len() > MAX_URL_LEN {
        return Err(FetchError::new(
            "url_too_long",
            format!("URL exceeds {MAX_URL_LEN} characters"),
        ));
    }
    let parsed = url::Url::parse(trimmed)
        .map_err(|_| FetchError::new("invalid_input", format!("Invalid URL format: {trimmed}")))?;
    match parsed.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(FetchError::new(
                "url_not_allowed",
                format!("scheme {} is not allowed", parsed.scheme()),
            ));
        }
    }
    let host = parsed.host_str().unwrap_or("");
    if host.is_empty() {
        return Err(FetchError::new("invalid_input", "URL is missing a host"));
    }
    if host_is_blocked(host) {
        return Err(FetchError::new(
            "url_not_allowed",
            format!("host {host} is not allowed"),
        ));
    }
    Ok(parsed)
}

fn host_is_blocked(host: &str) -> bool {
    let lower = host
        .trim_matches(|c| c == '[' || c == ']')
        .to_ascii_lowercase();
    if lower == "localhost"
        || lower.ends_with(".localhost")
        || lower.ends_with(".local")
        || lower == "metadata.google.internal"
        || lower == "metadata"
    {
        return true;
    }
    if let Ok(ip) = lower.parse::<IpAddr>() {
        return ip_is_blocked(ip);
    }
    false
}

fn ip_is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_link_local()
                || v4.is_private()
                || v4.octets()[0] == 0
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unicast_link_local()
                || v6.is_unique_local()
                || v6.to_ipv4_mapped().is_some_and(|v4| {
                    v4.is_loopback() || v4.is_private() || v4.is_link_local() || v4.is_unspecified()
                })
        }
    }
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
}

/// Process a mocked HTTP response the same way the live GET would.
pub fn fetch_web_mocked(
    url: &str,
    content_type: &str,
    body: &[u8],
) -> Result<FetchedPage, FetchError> {
    let parsed = validate_fetch_url(url)?;
    process_http_body(parsed.as_str(), content_type, body)
}

fn process_http_body(
    url: &str,
    content_type: &str,
    body: &[u8],
) -> Result<FetchedPage, FetchError> {
    if body.len() > MAX_BODY_BYTES {
        return Err(FetchError::new(
            "url_not_accessible",
            format!("response larger than {MAX_BODY_BYTES} bytes"),
        ));
    }
    let media = media_type(content_type);
    if !content_type_allowed(&media) {
        return Err(FetchError::new(
            "unsupported_content_type",
            format!("content type {media} is not supported"),
        ));
    }
    let raw = String::from_utf8_lossy(body);
    let (title, text) = if media == "text/html" || (media.is_empty() && looks_like_html(&raw)) {
        html_to_text(&raw)
    } else {
        (String::new(), raw.into_owned())
    };
    let text = truncate_chars(&text, MAX_TEXT_CHARS);
    Ok(FetchedPage {
        url: url.to_string(),
        title,
        text,
        retrieved_at: now_rfc3339(),
    })
}

fn media_type(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase()
}

fn content_type_allowed(media: &str) -> bool {
    matches!(
        media,
        "text/html" | "text/plain" | "text/markdown" | "text/x-markdown" | ""
    )
}

fn looks_like_html(s: &str) -> bool {
    let t = s.trim_start().to_ascii_lowercase();
    t.starts_with("<!doctype html") || t.starts_with("<html")
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    s.chars().take(max).collect()
}

fn html_to_text(html: &str) -> (String, String) {
    let stripped = strip_container(&strip_container(html, "script"), "style");
    let title = extract_title(&stripped);
    let with_breaks = insert_block_breaks(&stripped);
    let no_tags = strip_tags(&with_breaks);
    let text = collapse_ws(&html_unescape(&no_tags));
    (title, text)
}

fn strip_container(html: &str, tag: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut out = String::with_capacity(html.len());
    let mut i = 0;
    while i < html.len() {
        if let Some(rel) = lower[i..].find(&open) {
            let start = i + rel;
            out.push_str(&html[i..start]);
            let after_open = start + open.len();
            let close_at = lower[after_open..]
                .find(&close)
                .map(|c| after_open + c + close.len())
                .unwrap_or(html.len());
            i = close_at;
        } else {
            out.push_str(&html[i..]);
            break;
        }
    }
    out
}

fn extract_title(html: &str) -> String {
    let lower = html.to_ascii_lowercase();
    let Some(start) = lower.find("<title") else {
        return String::new();
    };
    let Some(gt) = html[start..].find('>') else {
        return String::new();
    };
    let after = start + gt + 1;
    let Some(end_rel) = lower[after..].find("</title>") else {
        return String::new();
    };
    collapse_ws(&html_unescape(&strip_tags(&html[after..after + end_rel])))
}

fn insert_block_breaks(html: &str) -> String {
    let mut out = String::with_capacity(html.len() + 32);
    let lower = html.to_ascii_lowercase();
    let mut i = 0;
    while i < html.len() {
        if html.as_bytes()[i] == b'<' {
            let rest_lower = &lower[i..];
            let is_break = rest_lower.starts_with("<p")
                || rest_lower.starts_with("<br")
                || rest_lower.starts_with("<div")
                || rest_lower.starts_with("<tr")
                || rest_lower.starts_with("<li")
                || rest_lower.starts_with("<h1")
                || rest_lower.starts_with("<h2")
                || rest_lower.starts_with("<h3")
                || rest_lower.starts_with("<h4")
                || rest_lower.starts_with("<h5")
                || rest_lower.starts_with("<h6")
                || rest_lower.starts_with("</p")
                || rest_lower.starts_with("</div")
                || rest_lower.starts_with("</tr")
                || rest_lower.starts_with("</li")
                || rest_lower.starts_with("</h");
            if is_break {
                out.push('\n');
            }
            if let Some(end) = html[i..].find('>') {
                i += end + 1;
                continue;
            }
        }
        out.push(html[i..].chars().next().unwrap_or(' '));
        i += html[i..].chars().next().map(|c| c.len_utf8()).unwrap_or(1);
    }
    out
}

fn strip_tags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out
}

fn html_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
}

fn collapse_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut newline_run = 0;
    let mut space = false;
    for c in s.chars() {
        if c == '\n' || c == '\r' {
            if newline_run < 2 {
                if space {
                    // drop pending space before newline
                }
                out.push('\n');
                newline_run += 1;
            }
            space = false;
        } else if c.is_whitespace() {
            space = true;
        } else {
            if space && !out.is_empty() && !out.ends_with('\n') {
                out.push(' ');
            }
            out.push(c);
            space = false;
            newline_run = 0;
        }
    }
    out.trim().to_string()
}

/// Live HTTP GET with redirect limit, timeout, and size cap.
pub async fn fetch_web(url: &str) -> Result<FetchedPage, FetchError> {
    let parsed = validate_fetch_url(url)?;
    reject_resolved_blocked_ips(parsed.host_str().unwrap_or(""))?;

    let client = reqwest::Client::builder()
        .user_agent("claude-cursor-proxy/web-fetch (compatible; +https://github.com/YeautyYE/claude-cursor-proxy)")
        .timeout(FETCH_TIMEOUT)
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= MAX_REDIRECTS {
                return attempt.error("too many redirects");
            }
            match validate_fetch_url(attempt.url().as_str()) {
                Ok(_) => attempt.follow(),
                Err(e) => attempt.error(e.message),
            }
        }))
        .build()
        .map_err(|e| FetchError::new("unavailable", format!("web fetch client: {e}")))?;

    let response = client.get(parsed.as_str()).send().await.map_err(|e| {
        FetchError::new(
            "url_not_accessible",
            format!("web fetch request failed: {e}"),
        )
    })?;

    if let Some(final_host) = response.url().host_str() {
        reject_resolved_blocked_ips(final_host)?;
        if host_is_blocked(final_host) {
            return Err(FetchError::new(
                "url_not_allowed",
                format!("redirect host {final_host} is not allowed"),
            ));
        }
    }

    if !response.status().is_success() {
        return Err(FetchError::new(
            "url_not_accessible",
            format!("web fetch upstream HTTP {}", response.status().as_u16()),
        ));
    }

    let final_url = response.url().as_str().to_string();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let bytes = response
        .bytes()
        .await
        .map_err(|e| FetchError::new("url_not_accessible", format!("web fetch body: {e}")))?;
    process_http_body(&final_url, &content_type, &bytes)
}

fn reject_resolved_blocked_ips(host: &str) -> Result<(), FetchError> {
    if host.is_empty() {
        return Ok(());
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip_is_blocked(ip) {
            return Err(FetchError::new(
                "url_not_allowed",
                format!("host {host} is not allowed"),
            ));
        }
        return Ok(());
    }
    let addrs = match (host, 0u16).to_socket_addrs() {
        Ok(a) => a,
        Err(_) => return Ok(()),
    };
    for addr in addrs {
        if ip_is_blocked(addr.ip()) {
            return Err(FetchError::new(
                "url_not_allowed",
                format!("host {host} resolved to a blocked address"),
            ));
        }
    }
    Ok(())
}

/// Build Anthropic SSE that Claude Code's nested web_fetch understands.
pub fn hosted_web_fetch_sse_response(
    message_id: String,
    model: String,
    url: String,
    page: Option<FetchedPage>,
    error: Option<FetchError>,
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
            "usage": {"input_tokens": (url.len() / 4).max(1), "output_tokens": 0}
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
            "name": "web_fetch",
            "input": {}
        }
    });
    events.push(Bytes::from(format_sse_event_bytes(
        EVENT_CONTENT_BLOCK_START,
        &server_tool_start,
    )));

    let partial = serde_json::json!({"url": url}).to_string();
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

    let content = if let Some(err) = error.as_ref() {
        serde_json::json!({
            "type": "web_fetch_tool_error",
            "error_code": err.code
        })
    } else if let Some(page) = page.as_ref() {
        serde_json::json!({
            "type": "web_fetch_result",
            "url": page.url,
            "retrieved_at": page.retrieved_at,
            "content": {
                "type": "document",
                "source": {
                    "type": "text",
                    "media_type": "text/plain",
                    "data": page.text
                },
                "title": page.title
            }
        })
    } else {
        serde_json::json!({
            "type": "web_fetch_tool_error",
            "error_code": "unavailable"
        })
    };

    let result_start = serde_json::json!({
        "type": "content_block_start",
        "index": 1,
        "content_block": {
            "type": "web_fetch_tool_result",
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
            "server_tool_use": {"web_fetch_requests": 1}
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

/// Non-streaming JSON body for the same hosted web_fetch shape.
pub fn hosted_web_fetch_json_response(
    message_id: String,
    model: String,
    url: String,
    page: Option<FetchedPage>,
    error: Option<FetchError>,
) -> Response {
    let tool_use_id = format!("srvtoolu_{}", uuid::Uuid::new_v4().simple());
    let result_content = if let Some(err) = error.as_ref() {
        serde_json::json!({
            "type": "web_fetch_tool_error",
            "error_code": err.code
        })
    } else if let Some(page) = page.as_ref() {
        serde_json::json!({
            "type": "web_fetch_result",
            "url": page.url,
            "retrieved_at": page.retrieved_at,
            "content": {
                "type": "document",
                "source": {
                    "type": "text",
                    "media_type": "text/plain",
                    "data": page.text
                },
                "title": page.title
            }
        })
    } else {
        serde_json::json!({
            "type": "web_fetch_tool_error",
            "error_code": "unavailable"
        })
    };
    let body = serde_json::json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [
            {
                "type": "server_tool_use",
                "id": tool_use_id,
                "name": "web_fetch",
                "input": {"url": url}
            },
            {
                "type": "web_fetch_tool_result",
                "tool_use_id": tool_use_id,
                "content": result_content
            }
        ],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": (url.len() / 4).max(1),
            "output_tokens": 32,
            "server_tool_use": {"web_fetch_requests": 1}
        }
    });
    (StatusCode::OK, axum::Json(body)).into_response()
}

/// Nested-only gate used by `cursor/mod.rs` `handle_messages`.
pub async fn maybe_handle_hosted_web_fetch(
    body: &MessagesRequest,
    message_id: &str,
    wire_model: &str,
) -> Option<Response> {
    if !is_hosted_web_fetch_request(body) {
        return None;
    }
    let url = extract_web_fetch_url(body).unwrap_or_default();
    if url.trim().is_empty() {
        return Some(json_error(
            StatusCode::BAD_REQUEST,
            "invalid_request_error",
            "web_fetch requires a non-empty URL",
        ));
    }
    let (page, error) = match fetch_web(&url).await {
        Ok(page) => (Some(page), None),
        Err(err) => (None, Some(err)),
    };
    if body.stream {
        Some(hosted_web_fetch_sse_response(
            message_id.to_string(),
            wire_model.to_string(),
            url,
            page,
            error,
        ))
    } else {
        Some(hosted_web_fetch_json_response(
            message_id.to_string(),
            wire_model.to_string(),
            url,
            page,
            error,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::schema::Message;

    fn req_with_tools(tools: serde_json::Value, messages: Vec<Message>) -> MessagesRequest {
        MessagesRequest {
            model: Some("claude-fable-5".into()),
            max_tokens: Some(1024),
            messages,
            stream: true,
            extra: serde_json::json!({ "tools": tools })
                .as_object()
                .cloned()
                .unwrap(),
        }
    }

    #[test]
    fn detects_hosted_web_fetch_20250910() {
        let req = req_with_tools(
            serde_json::json!([{"type":"web_fetch_20250910","name":"web_fetch"}]),
            vec![],
        );
        assert!(is_hosted_web_fetch_request(&req));
    }

    #[test]
    fn detects_hosted_web_fetch_20260209() {
        let req = req_with_tools(
            serde_json::json!([{"type":"web_fetch_20260209","name":"web_fetch"}]),
            vec![],
        );
        assert!(is_hosted_web_fetch_request(&req));
    }

    #[test]
    fn ignores_mixed_client_and_hosted_fetch_tools() {
        let req = req_with_tools(
            serde_json::json!([
                {"type":"web_fetch_20250910","name":"web_fetch"},
                {"name":"Read","description":"read","input_schema":{}}
            ]),
            vec![],
        );
        assert!(!is_hosted_web_fetch_request(&req));
    }

    #[test]
    fn ignores_hosted_web_search_only_request() {
        let req = req_with_tools(
            serde_json::json!([{"type":"web_search_20250305","name":"web_search"}]),
            vec![],
        );
        assert!(!is_hosted_web_fetch_request(&req));
    }

    #[test]
    fn ignores_mixed_hosted_search_and_fetch_without_tool_choice() {
        let req = req_with_tools(
            serde_json::json!([
                {"type":"web_search_20250305","name":"web_search"},
                {"type":"web_fetch_20250910","name":"web_fetch"}
            ]),
            vec![],
        );
        assert!(!is_hosted_web_fetch_request(&req));
    }

    #[test]
    fn detects_tool_choice_forced_web_fetch() {
        let mut req = req_with_tools(serde_json::json!([]), vec![]);
        req.extra.insert(
            "tool_choice".into(),
            serde_json::json!({"type":"tool","name":"web_fetch"}),
        );
        assert!(is_hosted_web_fetch_request(&req));
    }

    #[test]
    fn extracts_url_from_fetch_the_content_at_prefix() {
        let req = req_with_tools(
            serde_json::json!([{"type":"web_fetch_20250910","name":"web_fetch"}]),
            vec![Message {
                role: "user".into(),
                content: serde_json::json!(
                    "Fetch the content at https://example.com/research-paper and extract the key findings."
                ),
            }],
        );
        assert_eq!(
            extract_web_fetch_url(&req).as_deref(),
            Some("https://example.com/research-paper")
        );
    }

    #[test]
    fn extracts_url_from_markdown_url_field() {
        let req = req_with_tools(
            serde_json::json!([{"type":"web_fetch_20250910","name":"web_fetch"}]),
            vec![Message {
                role: "user".into(),
                content: serde_json::json!(
                    "**URL:** https://docs.example.org/page\n**Title:** Example"
                ),
            }],
        );
        assert_eq!(
            extract_web_fetch_url(&req).as_deref(),
            Some("https://docs.example.org/page")
        );
    }

    #[test]
    fn rejects_file_url() {
        let err = validate_fetch_url("file:///etc/passwd").unwrap_err();
        assert_eq!(err.code, "url_not_allowed");
    }

    #[test]
    fn rejects_non_http_scheme() {
        let err = validate_fetch_url("ftp://example.com/file").unwrap_err();
        assert_eq!(err.code, "url_not_allowed");
    }

    #[test]
    fn rejects_localhost_and_loopback() {
        assert_eq!(
            validate_fetch_url("http://localhost/secret")
                .unwrap_err()
                .code,
            "url_not_allowed"
        );
        assert_eq!(
            validate_fetch_url("http://127.0.0.1/secret")
                .unwrap_err()
                .code,
            "url_not_allowed"
        );
        assert_eq!(
            validate_fetch_url("http://[::1]/secret").unwrap_err().code,
            "url_not_allowed"
        );
    }

    #[test]
    fn rejects_link_local_metadata() {
        assert_eq!(
            validate_fetch_url("http://169.254.169.254/latest/meta-data/")
                .unwrap_err()
                .code,
            "url_not_allowed"
        );
    }

    #[test]
    fn accepts_https_public_url() {
        assert!(validate_fetch_url("https://example.com/article").is_ok());
    }

    #[test]
    fn mocked_html_happy_path_strips_tags() {
        let page = fetch_web_mocked(
            "https://example.com/article",
            "text/html; charset=utf-8",
            b"<html><head><title>Example Article</title></head><body><script>alert(1)</script><p>Hello <b>world</b>.</p></body></html>",
        )
        .expect("html fetch");
        assert_eq!(page.url, "https://example.com/article");
        assert_eq!(page.title, "Example Article");
        assert!(page.text.contains("Hello world"));
        assert!(!page.text.contains("alert"));
        assert!(!page.text.contains("<p>"));
    }

    #[test]
    fn mocked_plain_text_happy_path() {
        let page = fetch_web_mocked(
            "https://example.com/readme.md",
            "text/markdown",
            b"# Heading\n\nBody text",
        )
        .expect("markdown fetch");
        assert_eq!(page.text, "# Heading\n\nBody text");
    }

    #[test]
    fn mocked_unsupported_content_type_is_rejected() {
        let err =
            fetch_web_mocked("https://example.com/photo.png", "image/png", b"\x89PNG").unwrap_err();
        assert_eq!(err.code, "unsupported_content_type");
    }

    #[tokio::test]
    async fn sse_response_uses_anthropic_web_fetch_shape() {
        let page = FetchedPage {
            url: "https://example.com/article".into(),
            title: "Example Article".into(),
            text: "Full text content of the article...".into(),
            retrieved_at: "2025-08-25T10:30:00Z".into(),
        };
        let response = hosted_web_fetch_sse_response(
            "msg_test".into(),
            "claude-fable-5".into(),
            page.url.clone(),
            Some(page),
            None,
        );
        assert_eq!(response.status(), http::StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("sse body");
        let sse = String::from_utf8_lossy(&body);
        assert!(sse.contains("event: message_start"));
        assert!(sse.contains("\"type\":\"server_tool_use\""));
        assert!(sse.contains("\"name\":\"web_fetch\""));
        assert!(sse.contains("\"type\":\"web_fetch_tool_result\""));
        assert!(sse.contains("\"type\":\"web_fetch_result\""));
        assert!(sse.contains("\"type\":\"document\""));
        assert!(sse.contains("Full text content of the article..."));
        assert!(sse.contains("\"web_fetch_requests\":1"));
    }
}
