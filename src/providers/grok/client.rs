use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use http::StatusCode;

use super::auth::manager::GrokAuthManager;
use super::auth::token_store::{StoredAuth, file_store};
use super::translate::request::GrokResponsesRequest;
use crate::traffic::TrafficCapture;

const DEFAULT_BASE_URL: &str = "https://cli-chat-proxy.grok.com/v1";
const MAX_BUFFERED_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

pub struct GrokClient {
    client: Arc<reqwest::Client>,
    auth: Arc<GrokAuthManager<crate::auth::FileAuthStore<StoredAuth>>>,
    url: String,
    client_version: String,
}

pub struct GrokResponse {
    response: reqwest::Response,
}
pub struct GrokError {
    pub status: StatusCode,
    pub retry_after: Option<String>,
    pub message: String,
}

impl GrokResponse {
    pub fn into_response(self) -> reqwest::Response {
        self.response
    }

    pub fn into_stream(
        self,
    ) -> impl futures_util::Stream<Item = Result<bytes::Bytes, GrokError>> + Send {
        self.response.bytes_stream().map(|chunk| {
            chunk.map_err(|_| GrokError {
                status: StatusCode::BAD_GATEWAY,
                retry_after: None,
                message: "Grok upstream stream failed".into(),
            })
        })
    }

    pub async fn into_bytes(self) -> Result<Vec<u8>, GrokError> {
        let mut stream = self.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if bytes.len().saturating_add(chunk.len()) > MAX_BUFFERED_RESPONSE_BYTES {
                return Err(GrokError {
                    status: StatusCode::BAD_GATEWAY,
                    retry_after: None,
                    message: "Grok upstream response exceeds the size limit".into(),
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

impl GrokClient {
    pub fn new(base_url: String, client_version: String) -> anyhow::Result<Self> {
        let client = Arc::new(
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .build()?,
        );
        let auth = Arc::new(GrokAuthManager::new(file_store())?);
        Ok(Self::with_shared(
            url_for(base_url)?,
            client_version,
            client,
            auth,
        ))
    }

    fn with_shared(
        url: String,
        client_version: String,
        client: Arc<reqwest::Client>,
        auth: Arc<GrokAuthManager<crate::auth::FileAuthStore<StoredAuth>>>,
    ) -> Self {
        Self {
            client,
            auth,
            url,
            client_version,
        }
    }

    pub async fn post(
        &self,
        body: &GrokResponsesRequest,
        traffic: Option<Arc<TrafficCapture>>,
    ) -> Result<GrokResponse, GrokError> {
        let bytes = serde_json::to_vec(body).unwrap_or_default();
        self.post_bytes(&bytes, traffic).await
    }

    pub async fn post_bytes(
        &self,
        body: &[u8],
        traffic: Option<Arc<TrafficCapture>>,
    ) -> Result<GrokResponse, GrokError> {
        self.post_bytes_with_headers(body, traffic, &[]).await
    }

    pub async fn post_bytes_with_headers(
        &self,
        body: &[u8],
        traffic: Option<Arc<TrafficCapture>>,
        extra_headers: &[(String, String)],
    ) -> Result<GrokResponse, GrokError> {
        if let Some(capture) = traffic.as_ref() {
            let body_value = serde_json::from_slice::<serde_json::Value>(body)
                .unwrap_or(serde_json::Value::Null);
            capture.write_json("020-upstream-request", &body_value);
            capture.write_json("021-upstream-request-metadata", &serde_json::json!({
                "method": "POST", "url": safe_url(&self.url), "provider": "grok", "transport": "http",
                "headers": {"accept":"text/event-stream", "content-type":"application/json", "authorization":"[redacted]", "x-xai-token-auth":"[redacted]"},
                "body_bytes": body.len(),
            }));
        }
        let auth = match self.auth.get_auth().await {
            Ok(auth) => auth,
            Err(error) => {
                capture_failure(traffic.as_deref(), "auth", "authentication", 0);
                return Err(auth_error(error));
            }
        };
        let mut access = auth.access;
        let mut attempt = 0_u32;
        let response = loop {
            match self
                .attempt(
                    &access,
                    body,
                    (attempt + 1) as u8,
                    traffic.as_deref(),
                    extra_headers,
                )
                .await
            {
                Ok(response) if response.status() == StatusCode::UNAUTHORIZED => {
                    let refreshed = self.auth.force_refresh(&access).await.map_err(|error| {
                        capture_failure(traffic.as_deref(), "auth", "refresh", 1);
                        auth_error(error)
                    })?;
                    access = refreshed.access;
                    let replay = self
                        .attempt(&access, body, 2, traffic.as_deref(), extra_headers)
                        .await?;
                    if replay.status() == StatusCode::UNAUTHORIZED {
                        capture_failure(traffic.as_deref(), "auth", "unauthorized", 2);
                        return Err(auth_error(anyhow::anyhow!("unauthorized")));
                    }
                    break replay;
                }
                Ok(response) => break response,
                Err(error)
                    if attempt < crate::retry::MAX_RATE_LIMIT_RETRIES
                        && crate::retry::should_retry_upstream(
                            error.status.as_u16(),
                            &error.message,
                        ) =>
                {
                    crate::retry::sleep(
                        crate::retry::compute_backoff_delay(attempt, error.retry_after.as_deref())
                            .wait_ms,
                    )
                    .await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        };
        Ok(self.captured_response(response, traffic.as_deref()))
    }

    fn captured_response(
        &self,
        response: reqwest::Response,
        traffic: Option<&TrafficCapture>,
    ) -> GrokResponse {
        if let Some(capture) = traffic.as_ref() {
            capture.write_json("030-upstream-response-headers", &serde_json::json!({
                "status": response.status().as_u16(), "headers": safe_headers(response.headers()),
            }));
        }
        GrokResponse { response }
    }

    async fn attempt(
        &self,
        access: &str,
        body: &[u8],
        attempt: u8,
        traffic: Option<&TrafficCapture>,
        extra_headers: &[(String, String)],
    ) -> Result<reqwest::Response, GrokError> {
        let started = Instant::now();
        let mut request = self
            .client
            .post(&self.url)
            .header("accept", "text/event-stream")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {access}"))
            .header("x-xai-token-auth", "xai-grok-cli")
            .header("x-grok-client-identifier", "grok-shell")
            .header("x-grok-client-version", &self.client_version);
        for (name, value) in extra_headers {
            if super::is_passthrough_request_header(name) {
                request = request.header(name.as_str(), value.as_str());
            }
        }
        let response = request.body(body.to_vec()).send().await.map_err(|_| {
            capture_failure(traffic, "transport", "transport", attempt);
            GrokError {
                status: StatusCode::BAD_GATEWAY,
                retry_after: None,
                message: "Grok upstream request failed".into(),
            }
        })?;
        let status = response.status();
        if let Some(capture) = traffic {
            capture.write_json("022-upstream-attempt", &serde_json::json!({"attempt":attempt,"status":status.as_u16(),"elapsed_ms":started.elapsed().as_millis(),"headers":safe_headers(response.headers())}));
        }
        if !status.is_success() && status != StatusCode::UNAUTHORIZED {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let (body, truncated) = read_rejected_body(response, 64 * 1024).await;
            if let Some(capture) = traffic {
                let detail = serde_json::from_slice::<serde_json::Value>(&body)
                    .unwrap_or_else(|_| serde_json::json!({"body_bytes": body.len()}));
                capture.write_json(
                    "031-upstream-error-body",
                    &serde_json::json!({"attempt":attempt,"status":status.as_u16(),"truncated":truncated,"body":detail}),
                );
            }
            return Err(GrokError {
                status,
                retry_after,
                message: extract_upstream_error_message(
                    &body,
                    "Grok upstream rejected the request",
                ),
            });
        }
        Ok(response)
    }
}

pub fn extract_upstream_error_message(body: &[u8], fallback: &str) -> String {
    let parsed = serde_json::from_slice::<serde_json::Value>(body).ok();
    let raw = parsed
        .as_ref()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .or_else(|| value.get("message"))
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or(fallback);
    sanitize_error_message(raw)
}

fn sanitize_error_message(raw: &str) -> String {
    let truncated: String = raw.chars().take(512).collect();
    let mut out = String::new();
    let mut words = truncated.split_whitespace().peekable();
    while let Some(word) = words.next() {
        if !out.is_empty() {
            out.push(' ');
        }
        if word.eq_ignore_ascii_case("bearer")
            && let Some(next) = words.peek()
            && looks_like_secret(next)
        {
            out.push_str("Bearer [redacted]");
            words.next();
            continue;
        }
        if looks_like_secret(word) {
            out.push_str("[redacted]");
            continue;
        }
        out.push_str(word);
    }
    if out.is_empty() {
        "Grok upstream rejected the request".into()
    } else {
        out
    }
}

fn looks_like_secret(token: &str) -> bool {
    let trimmed =
        token.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_');
    (trimmed.starts_with("sk-") && trimmed.len() > 8)
        || (trimmed.starts_with("xai-") && trimmed.len() > 12)
        || {
            let mut parts = token.split('.');
            matches!(
                (parts.next(), parts.next(), parts.next(), parts.next()),
                (Some(header), Some(payload), Some(signature), None)
                    if header.starts_with("eyJ") && !payload.is_empty() && !signature.is_empty()
            )
        }
}

async fn read_rejected_body(response: reqwest::Response, limit: usize) -> (Vec<u8>, bool) {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else { break };
        let remaining = limit.saturating_sub(body.len());
        if chunk.len() > remaining {
            body.extend_from_slice(&chunk[..remaining]);
            return (body, true);
        }
        body.extend_from_slice(&chunk);
    }
    (body, false)
}

pub(super) fn capture_failure(
    traffic: Option<&TrafficCapture>,
    stage: &str,
    kind: &str,
    attempt: u8,
) {
    if let Some(capture) = traffic {
        capture.write_json(
            "060-grok-stream-error",
            &serde_json::json!({"stage":stage,"kind":kind,"attempt":attempt}),
        );
    }
}

fn safe_headers(headers: &reqwest::header::HeaderMap) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    for name in [
        "content-type",
        "content-length",
        "retry-after",
        "x-request-id",
    ] {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            result.insert(
                name.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    }
    serde_json::Value::Object(result)
}

fn safe_url(raw: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(raw) else {
        return "[invalid-url]".into();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.to_string()
}

fn url_for(base_url: String) -> anyhow::Result<String> {
    responses_url(&base_url)
}
fn responses_url(base_url: &str) -> anyhow::Result<String> {
    let base_url = if base_url.trim().is_empty() {
        DEFAULT_BASE_URL
    } else {
        base_url.trim()
    };
    let mut url = reqwest::Url::parse(base_url)?;
    let path = url.path().trim_end_matches('/');
    if !path.ends_with("/responses") {
        url.set_path(&format!("{path}/responses"));
    }
    Ok(url.to_string().trim_end_matches('/').to_string())
}

fn auth_error(_: anyhow::Error) -> GrokError {
    GrokError {
        status: StatusCode::UNAUTHORIZED,
        retry_after: None,
        message: "Grok authentication requires official CLI login and proxy import".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::responses_url;
    #[test]
    fn responses_url_appends_responses_to_base_path() {
        assert_eq!(
            responses_url("http://127.0.0.1:8080/v1").unwrap(),
            "http://127.0.0.1:8080/v1/responses"
        );
    }
    #[test]
    fn responses_url_preserves_responses_endpoint() {
        assert_eq!(
            responses_url("https://example.com/custom/responses/").unwrap(),
            "https://example.com/custom/responses"
        );
    }
    #[test]
    fn responses_url_rejects_invalid_url() {
        assert!(responses_url(":invalid").is_err());
    }
}
