//! HTTP/1.1 Agent transport: `RunSSE` read channel + `BidiAppend` write channel.
//!
//! Official CLI (`network.useHttp1ForAgent: true`) rewrites BiDi `Run` to
//! server-streaming `RunSSE`. Client messages after the open go through
//! unary `aiserver.v1.BidiService/BidiAppend` (hex-encoded `AgentClientMessage`).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use prost::Message;
use serde_json::json;
use tokio::sync::Semaphore;

use super::client::CursorError;
use super::connect::encode_connect_frame;
use super::proto::{AgentClientMessage, BidiRequestId};

/// Max in-flight BidiAppend calls (CLI uses 16).
const MAX_IN_FLIGHT: usize = 16;

/// Transient BidiAppend retries (CLI turn-runner: transport ≤10, server ≥3 throws).
const APPEND_MAX_ATTEMPTS: u32 = 4;

/// Session that appends client messages onto an open RunSSE stream.
#[derive(Clone)]
pub struct BidiAppendSession {
    client: reqwest::Client,
    base_url: String,
    token: String,
    request_id: String,
    seqno: Arc<AtomicI64>,
    in_flight: Arc<Semaphore>,
    identity_headers: Arc<Vec<(String, String)>>,
}

impl BidiAppendSession {
    pub fn new(
        client: reqwest::Client,
        base_url: String,
        token: String,
        request_id: String,
        identity_headers: Vec<(String, String)>,
    ) -> Self {
        Self {
            client,
            base_url,
            token,
            request_id,
            seqno: Arc::new(AtomicI64::new(0)),
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
            identity_headers: Arc::new(identity_headers),
        }
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Append a raw `AgentClientMessage` protobuf payload (not Connect-framed).
    ///
    /// Retries transient transport / 5xx failures with CLI-like exponential
    /// backoff + ~20% jitter (base 200ms, cap 4s) so a blip does not kill the
    /// open RunSSE session.
    pub async fn append_raw(&self, payload: &[u8]) -> Result<(), CursorError> {
        let _permit = self
            .in_flight
            .acquire()
            .await
            .map_err(|_| CursorError::internal("BidiAppend semaphore closed"))?;

        let seq = self.seqno.fetch_add(1, Ordering::SeqCst);
        let url = format!(
            "{}/aiserver.v1.BidiService/BidiAppend",
            self.base_url.trim_end_matches('/')
        );
        let body = json!({
            "data": hex_encode(payload),
            "requestId": { "requestId": self.request_id },
            "appendSeqno": seq.to_string(),
        });

        let mut last_err = None;
        for attempt in 0..APPEND_MAX_ATTEMPTS {
            if attempt > 0 {
                let base_ms = 200u64 << (attempt - 1).min(4);
                let jitter = (base_ms as f64 * 0.2 * fastrand_unit()).round() as u64;
                tokio::time::sleep(Duration::from_millis((base_ms + jitter).min(4_000))).await;
            }

            let mut req = self
                .client
                .post(&url)
                .bearer_auth(&self.token)
                .header("content-type", "application/json")
                .header("connect-protocol-version", "1")
                .header("user-agent", "connect-es/1.6.1")
                .header("x-request-id", &self.request_id)
                .header("x-original-request-id", &self.request_id)
                .json(&body);

            for (name, value) in self.identity_headers.iter() {
                req = req.header(name.as_str(), value.as_str());
            }

            let resp = match req.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    last_err = Some(CursorError::from_reqwest(e, 30));
                    continue;
                }
            };
            let status = resp.status().as_u16();
            if (200..300).contains(&status) {
                return Ok(());
            }
            let detail = resp.text().await.unwrap_or_default();
            let err = CursorError::new(
                status,
                format!("BidiAppend failed with HTTP {status}"),
                Some(detail.chars().take(500).collect()),
            );
            // Do not retry client errors (except 408/429).
            if !matches!(status, 408 | 429 | 500..=599) {
                return Err(err);
            }
            last_err = Some(err);
        }
        Err(last_err.unwrap_or_else(|| CursorError::internal("BidiAppend retries exhausted")))
    }

    pub async fn append_message(&self, message: &AgentClientMessage) -> Result<(), CursorError> {
        let mut payload = Vec::new();
        message
            .encode(&mut payload)
            .map_err(|e| CursorError::internal(format!("BidiAppend encode: {e}")))?;
        self.append_raw(&payload).await
    }

    /// Accept either a Connect frame or raw protobuf bytes.
    pub async fn append_connect_or_raw(&self, frame_or_raw: &[u8]) -> Result<(), CursorError> {
        let payload = strip_connect_frame(frame_or_raw).unwrap_or(frame_or_raw);
        self.append_raw(payload).await
    }
}

/// Encode `RunSSE` request body: Connect envelope of `BidiRequestId`.
pub fn encode_run_sse_request(request_id: &str) -> Result<Bytes, CursorError> {
    let msg = BidiRequestId {
        request_id: request_id.to_string(),
    };
    let mut payload = Vec::new();
    msg.encode(&mut payload)
        .map_err(|e| CursorError::internal(format!("RunSSE encode: {e}")))?;
    Ok(encode_connect_frame(payload, 0))
}

/// Whether the agent transport should use HTTP/1 RunSSE + BidiAppend.
pub fn prefer_http1_agent() -> bool {
    std::env::var("CCP_CURSOR_HTTP1")
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Cheap [0,1) unit without pulling `rand` — jitter only, not crypto.
fn fastrand_unit() -> f64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut h = DefaultHasher::new();
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos()
        .hash(&mut h);
    std::thread::current().id().hash(&mut h);
    (h.finish() % 10_000) as f64 / 10_000.0
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

pub fn hex_decode(hex: &str) -> Result<Vec<u8>, String> {
    let hex = hex.trim();
    if hex.len() % 2 != 0 {
        return Err("odd hex length".into());
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = from_hex(bytes[i])?;
        let lo = from_hex(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn from_hex(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("invalid hex digit {}", b as char)),
    }
}

/// If `data` is a Connect frame (`flags + len_be + payload`), return payload.
pub fn strip_connect_frame(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 5 {
        return None;
    }
    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
    if data.len() == 5 + len {
        Some(&data[5..])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn hex_roundtrip() {
        let raw = b"\x0a\x03foo";
        let enc = hex_encode(raw);
        assert_eq!(enc, "0a03666f6f");
        assert_eq!(hex_decode(&enc).unwrap(), raw);
    }

    #[test]
    fn strip_connect_frame_extracts_payload() {
        let frame = encode_connect_frame(b"abc", 0);
        assert_eq!(strip_connect_frame(&frame).unwrap(), b"abc");
        assert!(strip_connect_frame(b"rawproto").is_none());
    }

    #[test]
    fn run_sse_request_encodes_bidi_request_id() {
        let frame = encode_run_sse_request("req-123").unwrap();
        let payload = strip_connect_frame(&frame).unwrap();
        let decoded = BidiRequestId::decode(payload).unwrap();
        assert_eq!(decoded.request_id, "req-123");
    }

    #[test]
    fn bidi_append_json_shape() {
        let payload = b"\x3a\x00"; // empty client_heartbeat field tag 7
        let body = json!({
            "data": hex_encode(payload),
            "requestId": { "requestId": "abc" },
            "appendSeqno": "0",
        });
        assert_eq!(body["data"], "3a00");
        assert_eq!(body["requestId"]["requestId"], "abc");
        assert_eq!(body["appendSeqno"], "0");
    }
}
