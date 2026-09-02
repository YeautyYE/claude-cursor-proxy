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
    let provider = extract_provider_error_metadata(&parsed);
    Some(ConnectEndError {
        code,
        message,
        detail: parsed.to_string(),
        status,
        provider_error_code: provider.error_code,
        provider_status_code: provider.status_code,
        provider_is_retryable: provider.is_retryable,
    })
}

/// Metadata emitted by Cursor's aiserver provider diagnostics.
///
/// The outer Connect error frequently says `resource_exhausted`/429 even when
/// the provider rejected the request with a deterministic 4xx. Keeping the
/// inner values lets the live router distinguish an account-specific provider
/// failure from a retryable provider outage.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProviderErrorMetadata {
    pub error_code: Option<String>,
    pub status_code: Option<u16>,
    pub is_retryable: Option<bool>,
}

fn extract_provider_error_metadata(parsed: &serde_json::Value) -> ProviderErrorMetadata {
    let Some(details) = parsed
        .pointer("/error/details")
        .and_then(|value| value.as_array())
    else {
        return ProviderErrorMetadata::default();
    };

    for entry in details {
        let Some(debug) = entry.get("debug") else {
            continue;
        };
        let error_code = debug
            .get("error")
            .and_then(|value| value.as_str())
            .map(str::to_owned);
        let details = debug.get("details");
        let status_code = details
            .and_then(|value| {
                value
                    .get("additionalInfo")
                    .or_else(|| value.get("additional_info"))
            })
            .and_then(|value| {
                value
                    .get("providerStatusCode")
                    .or_else(|| value.get("provider_status_code"))
            })
            .and_then(parse_status_code_value);
        let is_retryable = details
            .and_then(|value| {
                value
                    .get("isRetryable")
                    .or_else(|| value.get("is_retryable"))
            })
            .and_then(serde_json::Value::as_bool);

        // A debug entry without provider metadata may describe another
        // diagnostic (for example a region or auth error). Continue looking
        // so a later provider entry remains authoritative.
        if error_code.is_some() || status_code.is_some() || is_retryable.is_some() {
            return ProviderErrorMetadata {
                error_code,
                status_code,
                is_retryable,
            };
        }
    }
    ProviderErrorMetadata::default()
}

fn parse_status_code_value(value: &serde_json::Value) -> Option<u16> {
    let number = match value {
        serde_json::Value::Number(value) => value.as_u64()?,
        serde_json::Value::String(value) => value.trim().parse::<u64>().ok()?,
        _ => return None,
    };
    (400..600).contains(&number).then_some(number as u16)
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
    /// Inner provider diagnostic code, when Cursor includes `ErrorDetails`.
    pub provider_error_code: Option<String>,
    /// Provider HTTP-ish status hidden inside Cursor's outer 429 envelope.
    pub provider_status_code: Option<u16>,
    /// Provider retry hint. `false` means retrying the same provider request
    /// cannot repair the failure.
    pub provider_is_retryable: Option<bool>,
}

impl ConnectEndError {
    /// True for the provider rejection shape observed in Cursor's current
    /// Sand/CLI responses: outer resource exhaustion, inner deterministic 4xx.
    pub fn is_non_retryable_provider_error(&self) -> bool {
        // Cursor occasionally reuses the same inner 400 envelope for a short
        // provider outage.  The human diagnostic is more useful than the
        // outer status in that case; keep the outage on the transport retry
        // path instead of treating it as an account-specific allowance.
        if is_transient_provider_error_message(&format!("{} {}", self.message, self.detail)) {
            return false;
        }
        self.provider_error_code
            .as_deref()
            .is_some_and(|code| code.eq_ignore_ascii_case("ERROR_PROVIDER_ERROR"))
            && self
                .provider_status_code
                .is_some_and(|status| (400..500).contains(&status))
            && self.provider_is_retryable == Some(false)
    }
}

/// Detect Cursor's provider-connectivity diagnostic after an error has crossed
/// a transport/string boundary.
///
/// The provider adapter has emitted this text with several outer statuses
/// (`400`, `429`, and `502`) and with both JSON and flattened key/value
/// metadata.  The outer status is therefore not sufficient to classify it.
/// Require a provider marker (or Cursor's strong current connectivity
/// sentence) plus temporary wording, while explicitly excluding quota,
/// billing, and capacity-shed language.  This helper is intentionally shared
/// by the Connect parser, retry classifier,
/// account breaker, Sand stream, and Agent live paths.
pub fn is_transient_provider_error_message(message: &str) -> bool {
    // Error text crosses several serialization boundaries before it reaches
    // the retry policy (Connect JSON -> a flattened Cursor diagnostic -> an
    // SSE/Responses string).  Normalize case, escaped line breaks, and
    // repeated whitespace once so a line-wrapped provider sentence is treated
    // the same as the one-line message emitted by Cursor.
    let lower = normalize_provider_error_text(message);

    // Do not classify ordinary `ERROR_OPENAI: Unable to reach the model
    // provider` messages here. Those already have the generic transport
    // retry semantics. This helper is for the newer nested provider adapter
    // envelope, whose stable marker is `ERROR_PROVIDER_ERROR` (or its
    // flattened `providerErrorCode` spelling).
    let provider_marker = lower.contains("error_provider_error")
        || lower.contains("providererrorcode=error_provider_error")
        || lower.contains("providererrorcode\":\"error_provider_error")
        || lower.contains("provider_error_code=error_provider_error");

    // These markers describe an account/plan or a deliberate capacity shed;
    // retrying the same account does not repair them even if the envelope
    // also contains "try again" wording.
    let terminal_policy = [
        "out of usage",
        "usage exhausted",
        "usage has been exhausted",
        "usage limit",
        "usage-limit",
        "quota",
        "rate limit exceeded",
        "rate_limited",
        "rate-limit exceeded",
        "resource exhausted by",
        "unpaid invoice",
        "pay your invoice",
        "billing",
        "entitlement",
        "free plans",
        "upgrade plan",
        "increase limits",
        "switch to auto",
        "high load",
        "high demand",
    ]
    .iter()
    .any(|marker| lower.contains(marker));
    if terminal_policy {
        return false;
    }

    // Keep the list explicit so generic model text containing the word
    // "temporary" cannot disable account policy handling.
    let temporary_connectivity = [
        "provider unavailable",
        "provider is unavailable",
        "provider temporarily unavailable",
        "temporarily unavailable",
        "temporary provider",
        "temporary trouble connecting",
        // Cursor's current provider adapter wording (the leading "having"
        // is present in the live error but was absent from older fixtures).
        "having trouble connecting to the model provider",
        "trouble connecting to the model provider",
        "trouble connecting with the model provider",
        "trouble connecting to provider",
        "unable to reach the model provider",
        "unable to connect to the model provider",
        "upstream connection failed",
        "upstream connection reset",
        "upstream connection closed",
        "upstream connection unavailable",
        "upstream connection",
        "connection to the model provider",
        "provider connection",
        "temporary outage",
        "temporarily unable to reach",
        "try again in a moment",
        "try again later",
        "might be temporary",
        "may be temporary",
    ];
    let has_temporary_connectivity = temporary_connectivity
        .iter()
        .any(|marker| lower.contains(marker));

    if !has_temporary_connectivity {
        return false;
    }

    // The nested provider adapter normally supplies ERROR_PROVIDER_ERROR.
    // A few Cursor revisions instead expose the legacy ERROR_OPENAI wrapper
    // (or only a `Connect error <status>` prefix) around the same sentence.
    // Accept those *specific* strong connectivity phrases, but do not turn a
    // generic `ERROR_OPENAI: Unable to reach ...` response into this class;
    // that path already has its own retry handling and must retain existing
    // quota semantics.
    provider_marker
        || lower.contains("having trouble connecting to the model provider")
        || lower.contains("trouble connecting to the model provider")
        || lower.contains("trouble connecting with the model provider")
}

/// Lower-case and collapse transport/JSON formatting differences in a Cursor
/// provider diagnostic.  `str::to_ascii_lowercase` is sufficient for the
/// stable English markers, while replacing escaped control sequences makes a
/// serialized `detail` field match the original multi-line diagnostic.
fn normalize_provider_error_text(message: &str) -> String {
    message
        .to_ascii_lowercase()
        .replace("\\r", " ")
        .replace("\\n", " ")
        .replace("\\t", " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Backwards-compatible spelling used by a few transport-specific callers.
/// Keep the implementation in `is_transient_provider_error_message` so all
/// paths share exactly the same policy precedence.
pub fn is_temporary_provider_error_message(message: &str) -> bool {
    is_transient_provider_error_message(message)
}

/// Detect the same provider diagnostic after an error has crossed a string
/// boundary (for example `CursorError::client_message`). This intentionally
/// accepts both camelCase and snake_case JSON spellings and tolerates spaces.
pub fn is_non_retryable_provider_error_message(message: &str) -> bool {
    if is_transient_provider_error_message(message) {
        return false;
    }
    let compact = message
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    // The diagnostic has appeared as JSON, key=value text, and a mixture of
    // both.  Read the first numeric provider status after either spelling so
    // deterministic 4xx responses (not only the observed 400) can trigger
    // account failover.
    if compact.contains("error_provider_error")
        && (compact.contains("isretryable\":false")
            || compact.contains("is_retryable\":false")
            || compact.contains("isretryable=false")
            || compact.contains("is_retryable=false"))
    {
        let status_marker = compact
            .find("providerstatuscode")
            .or_else(|| compact.find("provider_status_code"));
        if let Some(marker) = status_marker {
            let digits = compact[marker..]
                .chars()
                .skip_while(|character| !character.is_ascii_digit())
                .take_while(|character| character.is_ascii_digit())
                .collect::<String>();
            if digits.len() == 3
                && digits
                    .parse::<u16>()
                    .ok()
                    .is_some_and(|status| (400..500).contains(&status))
            {
                return true;
            }
        }
    }
    compact.contains("error_provider_error")
        && (compact.contains("providerstatuscode\":\"400\"")
            || compact.contains("providerstatuscode\":400")
            || compact.contains("provider_status_code\":\"400\"")
            || compact.contains("provider_status_code\":400")
            || compact.contains("providerstatuscode=400")
            || compact.contains("provider_status_code=400"))
        && (compact.contains("isretryable\":false")
            || compact.contains("is_retryable\":false")
            || compact.contains("isretryable=false")
            || compact.contains("is_retryable=false"))
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
    fn provider_error_metadata_survives_outer_resource_exhausted_wrapper() {
        let payload = serde_json::json!({
            "error": {
                "code": "resource_exhausted",
                "details": [{
                    "debug": {
                        "error": "ERROR_PROVIDER_ERROR",
                        "details": {
                            "title": "Provider Error",
                            "detail": "provider unavailable",
                            "additionalInfo": {"providerStatusCode": "400"},
                            "isRetryable": false
                        }
                    }
                }],
                "message": "Error"
            }
        });
        let error = parse_connect_error(&serde_json::to_vec(&payload).unwrap()).unwrap();
        assert_eq!(error.status, 429, "outer status remains observable");
        assert_eq!(
            error.provider_error_code.as_deref(),
            Some("ERROR_PROVIDER_ERROR")
        );
        assert_eq!(error.provider_status_code, Some(400));
        assert_eq!(error.provider_is_retryable, Some(false));
        assert!(is_transient_provider_error_message(&error.detail));
        assert!(!error.is_non_retryable_provider_error());
        assert!(!is_non_retryable_provider_error_message(&error.detail));
    }

    #[test]
    fn provider_error_metadata_accepts_numeric_status_and_snake_case_fields() {
        let payload = br#"{"error":{"code":"resource_exhausted","details":[{"debug":{"error":"ERROR_PROVIDER_ERROR","details":{"additional_info":{"provider_status_code":400},"is_retryable":false}}}]}}"#;
        let error = parse_connect_error(payload).unwrap();
        assert_eq!(error.provider_status_code, Some(400));
        assert_eq!(error.provider_is_retryable, Some(false));
        assert!(is_non_retryable_provider_error_message(&error.detail));
    }

    #[test]
    fn provider_error_message_requires_deterministic_inner_status() {
        let retryable = r#"{"error":{"details":[{"debug":{"error":"ERROR_PROVIDER_ERROR","details":{"additionalInfo":{"providerStatusCode":"400"},"isRetryable":true}}}]}}"#;
        assert!(!is_non_retryable_provider_error_message(retryable));
        let other_status = r#"{"error":{"details":[{"debug":{"error":"ERROR_PROVIDER_ERROR","details":{"additionalInfo":{"providerStatusCode":"503"},"isRetryable":false}}}]}}"#;
        assert!(!is_non_retryable_provider_error_message(other_status));
        let deterministic_forbidden = r#"{"error":{"details":[{"debug":{"error":"ERROR_PROVIDER_ERROR","details":{"additionalInfo":{"providerStatusCode":403},"isRetryable":false}}}]}}"#;
        assert!(is_non_retryable_provider_error_message(
            deterministic_forbidden
        ));
    }

    #[test]
    fn transient_provider_connectivity_diagnostic_overrides_outer_429() {
        let messages = [
            r#"{"error":{"code":"resource_exhausted","details":[{"debug":{"error":"ERROR_PROVIDER_ERROR","details":{"detail":"temporary trouble connecting to the model provider","additionalInfo":{"providerStatusCode":400},"isRetryable":false}}}]}}"#,
            "Connect error 429: ERROR_PROVIDER_ERROR: Provider Error — provider unavailable [providerStatusCode=400,isRetryable=false]",
            "Cursor error 400: ERROR_PROVIDER_ERROR upstream connection reset; try again in a moment",
            // Current Cursor wording, including the legacy ERROR_OPENAI
            // wrapper, must stay on the bounded provider-retry path.
            "Connect error 502: ERROR_OPENAI: Unable to reach the model provider — We're having trouble connecting to the model provider. This might be temporary - please try again in a moment. [unavailable]",
            // `detail` is sometimes passed as serialized JSON, where line
            // breaks become escaped `\\n` sequences.
            r#"{"error":"ERROR_PROVIDER_ERROR: We're having trouble connecting to the\nmodel provider. This might be temporary - please try again in a moment."}"#,
        ];
        for message in messages {
            assert!(is_transient_provider_error_message(message), "{message}");
            assert!(
                !is_non_retryable_provider_error_message(message),
                "temporary provider failures must not trip the account breaker: {message}"
            );
        }
    }

    #[test]
    fn provider_policy_wording_wins_over_temporary_connectivity_hint() {
        for message in [
            "Connect error 429: ERROR_PROVIDER_ERROR: provider unavailable; you're out of usage",
            "ERROR_PROVIDER_ERROR temporary trouble connecting; unpaid invoice",
            "ERROR_PROVIDER_ERROR provider unavailable; High Load — switch to Auto",
            "ERROR_PROVIDER_ERROR: We're having trouble connecting to the model provider; usage exhausted",
            "ERROR_PROVIDER_ERROR: We're having trouble connecting to the model provider; usage limit reached",
        ] {
            assert!(
                !is_transient_provider_error_message(message),
                "policy wording must remain terminal: {message}"
            );
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
