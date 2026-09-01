use bytes::{Bytes, BytesMut};

// Connect frame flags
pub const FLAG_GZIP: u8 = 0x01;
pub const FLAG_END: u8 = 0x02;

/// Cap gzip Connect payloads so a compressed envelope cannot expand without bound.
pub const MAX_GZIP_DECODE_BYTES: usize = 8 * 1024 * 1024;

/// A single Connect frame with flags and payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectFrame {
    pub flags: u8,
    pub payload: Bytes,
}

/// Encode a payload into a Connect frame: 1 byte flags, 4 byte big-endian
/// payload length, then the payload bytes.
pub fn encode_connect_frame(payload: impl AsRef<[u8]>, flags: u8) -> Bytes {
    let payload = payload.as_ref();
    let mut out = BytesMut::with_capacity(5 + payload.len());
    out.extend_from_slice(&[flags]);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out.freeze()
}

/// Streaming decoder for Connect frames from a byte source.
///
/// Handles split chunks, multiple frames in a single chunk, and malformed
/// (oversized) lengths. Does NOT handle gzip decompression inline -- the
/// caller checks `FLAG_GZIP` and decompresses if desired.
///
/// End frames (FLAG_END set) with an empty or JSON payload are returned
/// as ConnectFrames. The caller inspects the payload to determine whether
/// it conveys a Connect error.
#[derive(Default)]
pub struct ConnectFrameDecoder {
    buffer: BytesMut,
}

impl ConnectFrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed bytes into the decoder. Returns all complete frames found.
    ///
    /// Returns an error if a frame header advertises a length that exceeds
    /// `max_frame_payload` (default 64 MiB).
    pub fn push(&mut self, chunk: impl AsRef<[u8]>) -> Result<Vec<ConnectFrame>, ConnectError> {
        self.buffer.extend_from_slice(chunk.as_ref());
        self.drain(64 * 1024 * 1024) // 64 MiB max payload
    }

    /// Same as `push` but with an explicit `max_payload` limit for testing.
    pub fn push_with_limit(
        &mut self,
        chunk: impl AsRef<[u8]>,
        max_payload: usize,
    ) -> Result<Vec<ConnectFrame>, ConnectError> {
        self.buffer.extend_from_slice(chunk.as_ref());
        self.drain(max_payload)
    }

    fn drain(&mut self, max_payload: usize) -> Result<Vec<ConnectFrame>, ConnectError> {
        let mut out = Vec::new();
        loop {
            if self.buffer.len() < 5 {
                break;
            }
            let len = u32::from_be_bytes([
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
                self.buffer[4],
            ]) as usize;

            if len > max_payload {
                return Err(ConnectError::PayloadTooLarge {
                    length: len,
                    max: max_payload,
                });
            }

            if self.buffer.len() < 5 + len {
                break;
            }

            let mut raw = self.buffer.split_to(5 + len);
            out.push(ConnectFrame {
                flags: raw[0],
                payload: raw.split_off(5).freeze(),
            });
        }
        Ok(out)
    }

    /// Return the number of buffered bytes (incomplete frame data).
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

/// Decode gzipped payload bytes. The caller decides when to call this based
/// on frame flags & FLAG_GZIP.
pub fn decode_gzip_frame(payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(payload).take(MAX_GZIP_DECODE_BYTES as u64 + 1);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    if out.len() > MAX_GZIP_DECODE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "gzip payload exceeds limit",
        ));
    }
    Ok(out)
}

/// Map a live/Connect error string onto Anthropic SSE `error.type`.
pub fn anthropic_error_type_from_live_error(error: &str) -> &'static str {
    crate::retry::anthropic_error_kind_for_status(502, error)
}

pub fn cursor_connect_error_is_missing_image(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("image not found") || lower.contains("imagenotfound")
}

pub fn cursor_connect_error_is_missing_conversation_data(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("conversation data missing")
        || lower.contains("conversation's data is missing")
        || lower.contains("conversation’s data is missing")
}

/// Convert a Connect/Sand error value to a diagnostic string.
///
/// Connect JSON normally encodes `code` as a string, while the Sand
/// `InferenceStreamErrorType` is an integer (and a few gateway revisions
/// serialize either field as a boolean/stringified number).  Keeping this
/// conversion in one place prevents a numeric value from making the whole
/// end-frame look like an unrelated transport failure.
fn error_value_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

/// Map the numeric `InferenceStreamErrorType` enum to an HTTP-ish status.
///
/// These values are deliberately separate from HTTP status codes.  In
/// particular, `4` means rate limit and `7` means overloaded in the Sand
/// stream protocol.  Unknown values are left for the textual/status fallback.
fn sand_error_type_status(value: &serde_json::Value) -> Option<u16> {
    let number = match value {
        serde_json::Value::Number(number) => number.as_u64()?,
        serde_json::Value::String(text) => text.trim().parse::<u64>().ok()?,
        _ => return None,
    };
    Some(match number {
        2 | 3 | 8 => 400,
        4 => 429,
        5 => 401,
        6 => 403,
        7 => 503,
        _ => return None,
    })
}

/// Accept a numeric HTTP status when a gateway serializes `code` as a number
/// instead of the usual gRPC name. Keep this separate from the Sand enum
/// mapping because the two numeric domains overlap at small values.
fn numeric_http_status(value: &serde_json::Value) -> Option<u16> {
    let number = match value {
        serde_json::Value::Number(number) => number.as_u64()?,
        serde_json::Value::String(text) => text.trim().parse::<u64>().ok()?,
        _ => return None,
    };
    (400..600).contains(&number).then_some(number as u16)
}

/// Map a textual Connect/Sand error code to an HTTP-ish status.
///
/// Cursor has emitted both lower-case gRPC names (`resource_exhausted`) and
/// upper-case aiserver diagnostics (`ERROR_RATE_LIMIT`,
/// `ERROR_OUTDATED_CLIENT`).  Normalize punctuation and match the meaningful
/// token rather than requiring one exact spelling.
fn textual_error_status(value: &str) -> Option<u16> {
    let normalized = value
        .trim()
        .to_ascii_uppercase()
        .replace(['-', ' ', '.'], "_");
    if normalized.is_empty() {
        return None;
    }

    // Match the most specific policy names first.  `RATE_LIMITED_CHANGEABLE`
    // is still a 429; the caller's retry policy decides whether it is
    // terminal for the account.
    if normalized.contains("RATE_LIMIT")
        || normalized.contains("RESOURCE_EXHAUSTED")
        || normalized == "TOO_MANY_REQUESTS"
        || normalized == "RATE_LIMITED"
    {
        return Some(429);
    }
    if normalized.contains("OUTDATED_CLIENT")
        || normalized.contains("AUTHENTICATION")
        || normalized == "UNAUTHENTICATED"
        || normalized == "UNAUTHORIZED"
    {
        // OUTDATED_CLIENT is intentionally represented as 403 here.  The
        // response layer recognizes the diagnostic and presents it as a
        // client-update (400) error without treating it as a login failure.
        if normalized.contains("OUTDATED_CLIENT") {
            return Some(403);
        }
        return Some(401);
    }
    if normalized.contains("PERMISSION")
        || normalized == "FORBIDDEN"
        || normalized == "ACCESS_DENIED"
    {
        return Some(403);
    }
    if normalized.contains("BAD_MODEL_NAME")
        || normalized.contains("INPUT_TOKEN_LIMIT")
        || normalized.contains("OUTPUT_TOKEN_LIMIT")
        || normalized.contains("CONTENT_FILTER")
        || normalized == "INVALID_ARGUMENT"
        || normalized == "FAILED_PRECONDITION"
        || normalized == "BAD_REQUEST"
    {
        return Some(400);
    }
    if normalized.contains("OVERLOADED") {
        return Some(503);
    }
    if normalized == "NOT_FOUND" {
        return Some(404);
    }
    None
}

/// Classify a Connect error using Sand's `errorType`, the ordinary `code`,
/// and finally human-readable diagnostics.
pub(crate) fn connect_error_status(
    code: Option<&serde_json::Value>,
    error_type: Option<&serde_json::Value>,
    message: &str,
) -> u16 {
    // A recognized Sand enum is authoritative even when a gateway leaves a
    // generic `code` such as `internal` beside it.
    if let Some(value) = error_type
        && let Some(status) = sand_error_type_status(value)
    {
        return status;
    }
    if let Some(value) = error_type
        && let Some(text) = error_value_string(value)
        && let Some(status) = textual_error_status(&text)
    {
        return status;
    }
    if let Some(value) = code {
        if let Some(status) = numeric_http_status(value) {
            return status;
        }
        if let Some(status) = sand_error_type_status(value) {
            return status;
        }
        if let Some(text) = error_value_string(value)
            && let Some(status) = textual_error_status(&text)
        {
            return status;
        }
    }
    textual_error_status(message).unwrap_or(502)
}

/// Parse a Connect end-frame JSON error payload into a structured error.
///
/// Returns `None` if the payload is empty or not valid Connect error JSON.
pub fn parse_connect_error(payload: &[u8]) -> Option<ConnectEndError> {
    if payload.is_empty() {
        return None;
    }
    let parsed: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let error = parsed.get("error")?;
    if error.is_null() {
        return None;
    }
    let error_object = error.as_object();
    let code_value = error_object.and_then(|object| object.get("code"));
    let error_type_value = error_object
        .and_then(|object| object.get("errorType"))
        .or_else(|| error_object.and_then(|object| object.get("error_type")));
    let raw_code = code_value
        .and_then(error_value_string)
        .filter(|value| !value.trim().is_empty());
    let raw_error_type = error_type_value
        .and_then(error_value_string)
        .filter(|value| !value.trim().is_empty());
    let mut message = error_object
        .and_then(|object| object.get("message"))
        .and_then(error_value_string)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| error_value_string(error))
        .unwrap_or_else(|| "Connect error".to_string());

    // Prefer human-readable aiserver ErrorDetails when present (e.g.
    // ERROR_OUTDATED_CLIENT / "Update Required").
    if let Some(detail) = extract_aiserver_detail(&parsed) {
        message = detail;
    }

    // Keep an enum/code visible in the short display string when the server
    // only sent a generic message.  The full serialized envelope remains in
    // `detail` for callers that need exact fields.
    if let Some(error_type) = raw_error_type.as_deref()
        && !message
            .to_ascii_lowercase()
            .contains(&error_type.to_ascii_lowercase())
    {
        message.push_str(" [errorType=");
        message.push_str(error_type);
        message.push(']');
    }

    let status = if error_object.is_some_and(|object| {
        object
            .get("isInputTokenLimitError")
            .or_else(|| object.get("is_input_token_limit_error"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
            || object
                .get("isOutputTokenLimitError")
                .or_else(|| object.get("is_output_token_limit_error"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
    }) {
        400
    } else {
        connect_error_status(code_value, error_type_value, &message)
    };
    let code = raw_code
        .or(raw_error_type)
        .unwrap_or_else(|| "upstream_error".to_string());
    Some(ConnectEndError {
        code,
        message,
        detail: parsed.to_string(),
        status,
    })
}

fn extract_aiserver_detail(parsed: &serde_json::Value) -> Option<String> {
    let details = parsed.pointer("/error/details")?.as_array()?;
    for entry in details {
        let Some(debug) = entry.get("debug") else {
            continue;
        };
        let code = debug
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("ERROR");
        let title = debug
            .pointer("/details/title")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let detail = debug
            .pointer("/details/detail")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !title.is_empty() || !detail.is_empty() {
            return Some(format!("{code}: {title} — {detail}"));
        }
        if code != "ERROR" {
            return Some(code.to_string());
        }
    }
    None
}

#[derive(Debug, Clone)]
pub struct ConnectEndError {
    pub code: String,
    pub message: String,
    pub detail: String,
    pub status: u16,
}

impl std::fmt::Display for ConnectEndError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Connect error {}: {} [{}]",
            self.status, self.message, self.code
        )
    }
}

impl std::error::Error for ConnectEndError {}

#[derive(Debug, Clone)]
pub enum ConnectError {
    PayloadTooLarge { length: usize, max: usize },
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::PayloadTooLarge { length, max } => {
                write!(f, "Connect frame payload {length} exceeds max {max}")
            }
        }
    }
}

impl std::error::Error for ConnectError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_roundtrip() {
        let frame = encode_connect_frame(b"hello", 0);
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(&frame).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].flags, 0);
        assert_eq!(&frames[0].payload[..], b"hello");
    }

    #[test]
    fn encode_with_gzip_flag() {
        let frame = encode_connect_frame(b"gzip-data", FLAG_GZIP);
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(&frame).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].flags, FLAG_GZIP);
    }

    #[test]
    fn encode_with_end_flag() {
        let frame = encode_connect_frame(b"", FLAG_END);
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(&frame).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].flags, FLAG_END);
        assert!(frames[0].payload.is_empty());
    }

    #[test]
    fn encode_with_gzip_and_end_flags() {
        let payload = b"end-data";
        let frame = encode_connect_frame(payload, FLAG_GZIP | FLAG_END);
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(&frame).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].flags, FLAG_GZIP | FLAG_END);
        assert_eq!(&frames[0].payload[..], payload);
    }

    #[test]
    fn multiple_frames_in_single_chunk() {
        let f1 = encode_connect_frame(b"first", 0);
        let f2 = encode_connect_frame(b"second", 0);
        let mut combined = BytesMut::new();
        combined.extend_from_slice(&f1);
        combined.extend_from_slice(&f2);

        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(combined).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(&frames[0].payload[..], b"first");
        assert_eq!(&frames[1].payload[..], b"second");
    }

    #[test]
    fn split_chunks_are_assembled() {
        let frame = encode_connect_frame(b"split-test", 0);
        let (a, b) = frame.split_at(3);

        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(a).unwrap();
        assert!(frames.is_empty());

        let frames = decoder.push(b).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].payload[..], b"split-test");
    }

    #[test]
    fn split_at_header_boundary() {
        let frame = encode_connect_frame(b"split-at-5", 0);
        // Split after the flags byte but before the length bytes are complete
        let (a, b) = frame.split_at(1);

        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(a).unwrap();
        assert!(frames.is_empty());

        let frames = decoder.push(b).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(&frames[0].payload[..], b"split-at-5");
    }

    #[test]
    fn oversized_length_is_rejected() {
        let mut decoder = ConnectFrameDecoder::new();
        // Encode a frame with 1M payload (will exceed our 10-byte max)
        let oversized = encode_connect_frame(vec![0u8; 100], 0);
        let result = decoder.push_with_limit(&oversized, 10);
        assert!(result.is_err());
        match result.unwrap_err() {
            ConnectError::PayloadTooLarge { length, max } => {
                assert_eq!(length, 100);
                assert_eq!(max, 10);
            }
        }
    }

    #[test]
    fn empty_chunk_produces_no_frames() {
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(b"").unwrap();
        assert!(frames.is_empty());
    }

    #[test]
    fn buf_returns_buffered_bytes() {
        let mut decoder = ConnectFrameDecoder::new();
        // Push part of a frame header
        decoder.push(b"\x00\x00").unwrap();
        assert_eq!(decoder.buffered(), 2);
    }

    #[test]
    fn clean_end_frame_empty_payload() {
        let frame = encode_connect_frame(b"", FLAG_END);
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(frame).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].flags, FLAG_END);
        assert!(frames[0].payload.is_empty());
        // Parse error from empty payload
        assert!(parse_connect_error(&frames[0].payload).is_none());
    }

    #[test]
    fn connect_json_error_parsing() {
        let json_err = serde_json::json!({
            "error": {
                "code": "resource_exhausted",
                "message": "quota exceeded",
                "details": []
            }
        });
        let payload = serde_json::to_vec(&json_err).unwrap();
        let frame = encode_connect_frame(&payload, FLAG_END);
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(frame).unwrap();
        assert_eq!(frames.len(), 1);

        let err = parse_connect_error(&frames[0].payload).unwrap();
        assert_eq!(err.code, "resource_exhausted");
        assert_eq!(err.status, 429);
        assert_eq!(err.message, "quota exceeded");
    }

    #[test]
    fn missing_image_connect_error_is_detected() {
        assert!(cursor_connect_error_is_missing_image("Image not found"));
        assert!(cursor_connect_error_is_missing_image(
            "Connect error 502: Image not found [internal]"
        ));
        assert!(!cursor_connect_error_is_missing_image(
            "unexpected internal error"
        ));
    }

    #[test]
    fn missing_conversation_data_connect_error_is_detected() {
        assert!(cursor_connect_error_is_missing_conversation_data(
            "ERROR_CUSTOM_MESSAGE: Conversation data missing"
        ));
        assert!(cursor_connect_error_is_missing_conversation_data(
            "This conversation’s data is missing and can’t be restored"
        ));
        assert!(!cursor_connect_error_is_missing_conversation_data(
            "temporary connection failure"
        ));
    }

    #[test]
    fn connect_json_unavailable_error() {
        let json_err = serde_json::json!({
            "error": {
                "code": "unavailable",
                "message": "service unavailable"
            }
        });
        let payload = serde_json::to_vec(&json_err).unwrap();
        let err = parse_connect_error(&payload).unwrap();
        assert_eq!(err.code, "unavailable");
        assert_eq!(err.status, 502);
    }

    #[test]
    fn sand_numeric_error_type_maps_to_http_statuses() {
        let cases = [
            (2, 400),
            (3, 400),
            (4, 429),
            (5, 401),
            (6, 403),
            (7, 503),
            (8, 400),
        ];
        for (error_type, status) in cases {
            let payload = serde_json::to_vec(&serde_json::json!({
                "error": {
                    "errorType": error_type,
                    "message": format!("sand error {error_type}")
                }
            }))
            .unwrap();
            let error = parse_connect_error(&payload).unwrap();
            assert_eq!(error.status, status, "errorType={error_type}");
            assert_eq!(error.code, error_type.to_string());
            assert!(error.detail.contains("errorType"));
        }
    }

    #[test]
    fn sand_string_error_type_and_numeric_code_are_supported() {
        let cases = [
            ("ERROR_RATE_LIMIT", 429),
            ("ERROR_OUTDATED_CLIENT", 403),
            ("ERROR_BAD_MODEL_NAME", 400),
            ("ERROR_AUTHENTICATION", 401),
            ("ERROR_PERMISSION", 403),
            ("ERROR_OVERLOADED", 503),
            ("ERROR_INPUT_TOKEN_LIMIT", 400),
            ("ERROR_OUTPUT_TOKEN_LIMIT", 400),
            ("ERROR_CONTENT_FILTER", 400),
        ];
        for (error_type, status) in cases {
            let payload = serde_json::to_vec(&serde_json::json!({
                "error": {
                    "code": "internal",
                    "error_type": error_type,
                    "message": "gateway message"
                }
            }))
            .unwrap();
            let error = parse_connect_error(&payload).unwrap();
            assert_eq!(error.status, status, "error_type={error_type}");
            assert_eq!(error.code, "internal");
            assert!(error.message.contains(error_type));
        }
    }

    #[test]
    fn connect_error_without_code_keeps_message_and_defaults_to_gateway() {
        let payload = br#"{"error":{"message":"opaque gateway failure"}}"#;
        let error = parse_connect_error(payload).unwrap();
        assert_eq!(error.status, 502);
        assert_eq!(error.code, "upstream_error");
        assert_eq!(error.message, "opaque gateway failure");
    }

    #[test]
    fn connect_error_accepts_numeric_code_and_message_fallback() {
        let payload = br#"{"error":{"code":429}}"#;
        let error = parse_connect_error(payload).unwrap();
        assert_eq!(error.status, 429);
        assert_eq!(error.code, "429");
        assert_eq!(error.message, "Connect error");
    }

    #[test]
    fn token_limit_boolean_flags_are_client_errors() {
        for key in ["isInputTokenLimitError", "is_input_token_limit_error"] {
            let payload = serde_json::to_vec(&serde_json::json!({
                "error": { key: true, "message": "prompt too long" }
            }))
            .unwrap();
            assert_eq!(parse_connect_error(&payload).unwrap().status, 400);
        }
        for key in ["isOutputTokenLimitError", "is_output_token_limit_error"] {
            let payload = serde_json::to_vec(&serde_json::json!({
                "error": { key: true, "message": "completion too long" }
            }))
            .unwrap();
            assert_eq!(parse_connect_error(&payload).unwrap().status, 400);
        }
    }

    #[test]
    fn frame_fixture_matches_reference_layout() {
        // Connect frame: flags=0x00, length=3 (0x00000003), payload="abc"
        // Wire format: [0x00, 0x00, 0x00, 0x00, 0x03, 0x61, 0x62, 0x63]
        let frame = encode_connect_frame(b"abc", 0);
        assert_eq!(hex::encode(frame), "0000000003616263");
    }

    #[test]
    fn frame_fixture_with_flags() {
        // flags=0x01, length=3
        let frame = encode_connect_frame(b"xyz", 0x01);
        assert_eq!(hex::encode(frame), "010000000378797a");
    }

    #[test]
    fn gzip_frame_decompress() {
        let payload = b"hello gzip";
        let mut compressed = Vec::new();
        {
            use std::io::Write;
            let mut encoder =
                flate2::write::GzEncoder::new(&mut compressed, flate2::Compression::fast());
            encoder.write_all(payload).unwrap();
            encoder.finish().unwrap();
        }

        let frame = encode_connect_frame(&compressed, FLAG_GZIP);
        let mut decoder = ConnectFrameDecoder::new();
        let frames = decoder.push(frame).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].flags, FLAG_GZIP);

        let decompressed = decode_gzip_frame(&frames[0].payload).unwrap();
        assert_eq!(decompressed, b"hello gzip");
    }

    #[test]
    fn gzip_frame_rejects_unbounded_expansion() {
        let mut compressed = Vec::new();
        {
            use std::io::Write;
            let mut encoder =
                flate2::write::GzEncoder::new(&mut compressed, flate2::Compression::fast());
            let chunk = vec![0u8; 64 * 1024];
            for _ in 0..(MAX_GZIP_DECODE_BYTES / chunk.len() + 2) {
                encoder.write_all(&chunk).unwrap();
            }
            encoder.finish().unwrap();
        }
        assert!(decode_gzip_frame(&compressed).is_err());
    }
}
