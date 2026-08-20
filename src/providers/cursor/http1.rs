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

/// Whole-request cap so a stalled unary append cannot freeze the live driver.
const APPEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Session that appends client messages onto an open RunSSE stream.
#[derive(Clone)]
pub struct BidiAppendSession {
    client: reqwest::Client,
    base_url: String,
    token: String,
    request_id: String,
    original_request_id: String,
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
        let original_request_id = request_id.clone();
        Self::new_with_original(
            client,
            base_url,
            token,
            request_id,
            original_request_id,
            identity_headers,
        )
    }

    pub fn new_with_original(
        client: reqwest::Client,
        base_url: String,
        token: String,
        request_id: String,
        original_request_id: String,
        identity_headers: Vec<(String, String)>,
    ) -> Self {
        Self {
            client,
            base_url,
            token,
            request_id,
            original_request_id,
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
    /// Each append is attempted exactly once. A transport error, timeout, or
    /// 5xx can arrive after Cursor accepted the payload; replaying it with a
    /// new attempt can duplicate tool results and control messages.
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
        let http_request_id = uuid::Uuid::new_v4().to_string();

        let mut req = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .header("content-type", "application/json")
            .header("connect-protocol-version", "1")
            .header("user-agent", "connect-es/1.6.1")
            .header("x-request-id", http_request_id)
            .header("x-original-request-id", &self.original_request_id)
            .json(&body);

        for (name, value) in self.identity_headers.iter() {
            req = req.header(name.as_str(), value.as_str());
        }

        let resp = match tokio::time::timeout(APPEND_TIMEOUT, req.send()).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(error)) => return Err(CursorError::from_reqwest(error, 30)),
            Err(_) => return Err(CursorError::new(408, "BidiAppend timed out", None)),
        };
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let detail = resp.text().await.unwrap_or_default();
        Err(CursorError::new(
            status,
            format!("BidiAppend failed with HTTP {status}"),
            Some(detail.chars().take(500).collect()),
        ))
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
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    #[tokio::test]
    async fn bidi_append_does_not_retry_ambiguous_server_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock BidiAppend server");
        let address = listener.local_addr().expect("mock server address");
        let hits = Arc::new(AtomicUsize::new(0));
        let server_hits = Arc::clone(&hits);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let mut request = [0u8; 4096];
                if socket.read(&mut request).await.is_err() {
                    continue;
                }
                server_hits.fetch_add(1, Ordering::SeqCst);
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
            }
        });
        let session = BidiAppendSession::new(
            reqwest::Client::new(),
            format!("http://{address}"),
            "token".into(),
            "request-id".into(),
            vec![],
        );

        assert!(session.append_raw(b"\x3a\x00").await.is_err());
        server.abort();
        let _ = server.await;
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "an acknowledged-or-ambiguous append failure must never be replayed"
        );
    }

    #[tokio::test]
    async fn bidi_append_uses_fresh_http_ids_with_stable_run_lineage() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock BidiAppend server");
        let address = listener.local_addr().expect("mock server address");
        let (request_tx, mut request_rx) = tokio::sync::mpsc::channel(2);
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.expect("accept BidiAppend");
                let mut request = Vec::new();
                loop {
                    let mut chunk = [0u8; 2048];
                    let bytes_read = socket.read(&mut chunk).await.expect("read request");
                    if bytes_read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..bytes_read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                request_tx
                    .send(String::from_utf8_lossy(&request).into_owned())
                    .await
                    .expect("capture request");
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await
                    .expect("reply");
            }
        });
        let session = BidiAppendSession::new_with_original(
            reqwest::Client::new(),
            format!("http://{address}"),
            "token".into(),
            "logical-run-id".into(),
            "original-operation-id".into(),
            vec![],
        );

        session.append_raw(b"\x3a\x00").await.expect("first append");
        session
            .append_raw(b"\x3a\x00")
            .await
            .expect("second append");
        let first = request_rx.recv().await.expect("first request");
        let second = request_rx.recv().await.expect("second request");
        server.await.expect("mock server");

        let header = |request: &str, name: &str| {
            request
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case(name)
                        .then(|| value.trim().to_string())
                })
                .unwrap_or_else(|| panic!("missing {name} in {request}"))
        };
        let first_request_id = header(&first, "x-request-id");
        let second_request_id = header(&second, "x-request-id");
        assert_ne!(
            first_request_id, second_request_id,
            "each unary append is a distinct HTTP attempt"
        );
        assert_eq!(
            header(&first, "x-original-request-id"),
            "original-operation-id"
        );
        assert_eq!(
            header(&second, "x-original-request-id"),
            "original-operation-id",
            "all appends must retain the logical Run lineage"
        );
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
