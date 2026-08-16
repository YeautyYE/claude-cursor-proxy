use crate::anthropic::json_error;
use crate::providers::grok::auth::manager::GrokAuthManager;
use crate::providers::grok::auth::token_store::file_store;
use axum::{
    body::Body,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use std::time::Duration;

pub const MEDIA_MAX_BODY_BYTES: usize = 20 * 1024 * 1024;
const MEDIA_TIMEOUT_SECS: u64 = 300;

pub fn valid_video_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && !id.contains("..")
        && id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | ':'))
}

fn looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    let Some(header) = parts.next() else {
        return false;
    };
    let Some(payload) = parts.next() else {
        return false;
    };
    let Some(signature) = parts.next() else {
        return false;
    };
    parts.next().is_none()
        && header.starts_with("eyJ")
        && !payload.is_empty()
        && !signature.is_empty()
        && token
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

fn is_placeholder_key(token: &str) -> bool {
    matches!(
        token.to_ascii_lowercase().as_str(),
        "unused"
            | "none"
            | "null"
            | "placeholder"
            | "your-api-key"
            | "changeme"
            | "dummy"
            | "test"
            | "sk-placeholder"
    )
}

pub fn inbound_bearer(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        if let Some(token) = value
            .strip_prefix("Bearer ")
            .or_else(|| value.strip_prefix("bearer "))
        {
            let token = token.trim();
            if !token.is_empty() && !is_placeholder_key(token) && !looks_like_jwt(token) {
                return Some(token.to_string());
            }
        }
    }
    if let Some(value) = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
    {
        let token = value.trim();
        if !token.is_empty() && !is_placeholder_key(token) && !looks_like_jwt(token) {
            return Some(token.to_string());
        }
    }
    None
}

pub async fn proxy_media(
    method: Method,
    upstream_path: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() > MEDIA_MAX_BODY_BYTES {
        return json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "invalid_request_error",
            "media request exceeds the 20MiB limit",
        );
    }
    let base = crate::config::grok_media_base_url();
    let url = match join_url(&base, upstream_path) {
        Ok(url) => url,
        Err(_) => {
            return json_error(
                StatusCode::BAD_GATEWAY,
                "api_error",
                "Grok media base URL is invalid",
            );
        }
    };
    let (token, session_auth) = match resolve_token(headers).await {
        Ok(value) => value,
        Err(_) => {
            return json_error(
                StatusCode::UNAUTHORIZED,
                "authentication_error",
                "Grok media requires Authorization Bearer, x-api-key, or `grok auth login`",
            );
        }
    };
    let client = match reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(MEDIA_TIMEOUT_SECS))
        .build()
    {
        Ok(client) => client,
        Err(_) => {
            return json_error(StatusCode::BAD_GATEWAY, "api_error", "media client failed");
        }
    };
    let mut request = client.request(method, url);
    request = request
        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(
            http::header::CONTENT_TYPE,
            headers
                .get(http::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("application/json"),
        )
        .header("x-grok-client-identifier", "grok-shell")
        .header(
            "x-grok-client-version",
            crate::config::grok_client_version(),
        );
    if session_auth {
        request = request.header("x-xai-token-auth", "xai-grok-cli");
    }
    if !body.is_empty() {
        request = request.body(body);
    }
    match request.send().await {
        Ok(upstream) => forward_upstream(upstream).await,
        Err(_) => json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            "Grok media upstream request failed",
        ),
    }
}

async fn resolve_token(headers: &HeaderMap) -> anyhow::Result<(String, bool)> {
    if let Some(token) = inbound_bearer(headers) {
        return Ok((token, false));
    }
    let auth = GrokAuthManager::new(file_store())?.get_auth().await?;
    Ok((auth.access, true))
}

fn join_url(base: &str, path: &str) -> anyhow::Result<String> {
    let mut url = reqwest::Url::parse(base)?;
    let prefix = url.path().trim_end_matches('/');
    let suffix = path.trim_start_matches('/');
    url.set_path(&format!("{prefix}/{suffix}"));
    Ok(url.to_string())
}

async fn forward_upstream(upstream: reqwest::Response) -> Response {
    let status =
        StatusCode::from_u16(upstream.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let content_type = upstream
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    match read_limited_body(upstream).await {
        Ok(bytes) => (
            status,
            [(http::header::CONTENT_TYPE, content_type)],
            Body::from(bytes),
        )
            .into_response(),
        Err(oversize) if oversize => json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            "Grok media upstream response exceeds the 20MiB limit",
        ),
        Err(_) => json_error(
            StatusCode::BAD_GATEWAY,
            "api_error",
            "Grok media upstream body failed",
        ),
    }
}

async fn read_limited_body(upstream: reqwest::Response) -> Result<bytes::Bytes, bool> {
    use futures_util::StreamExt;
    let mut stream = upstream.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| false)?;
        if bytes.len().saturating_add(chunk.len()) > MEDIA_MAX_BODY_BYTES {
            return Err(true);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes::Bytes::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn video_id_rejects_path_traversal() {
        assert!(valid_video_id("req_abc-1"));
        assert!(valid_video_id("req.abc:1"));
        assert!(!valid_video_id("../secret"));
        assert!(!valid_video_id(".."));
        assert!(!valid_video_id("foo..bar"));
        assert!(!valid_video_id(""));
        assert!(!valid_video_id(&"a".repeat(129)));
    }
}
