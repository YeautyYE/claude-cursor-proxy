use std::time::Duration;

pub const RETRY_INITIAL_DELAY_MS: u64 = 2000;
pub const RETRY_MAX_DELAY_MS: u64 = 30_000;
pub const RETRY_BACKOFF_FACTOR: u64 = 2;
pub const MAX_RATE_LIMIT_RETRIES: u32 = 3;

#[derive(Debug, Clone, Copy)]
pub struct BackoffOutcome {
    pub wait_ms: u64,
    pub exceeds_budget: bool,
}

pub fn should_retry_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Billing / invoice 429s will not succeed on retry. Transient provider
/// exhaustion and gateway 502s will.
pub fn is_billing_block(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("unpaid invoice")
        || lower.contains("pay your invoice")
        || (lower.contains("error_rate_limited") && lower.contains("invoice"))
}

/// Cursor capacity shed: the model pool is full and the message tells the
/// client to switch models. Same-request retries turn one 429 into a flood.
pub fn is_capacity_shed(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    (lower.contains("high load") || lower.contains("high demand"))
        && (lower.contains("switch to")
            || lower.contains("another model")
            || lower.contains("try again in a few"))
}

pub fn should_retry_upstream(status: u16, message: &str) -> bool {
    let status = classify_proxy_error_status(status, message);
    should_retry_status(status) && !is_billing_block(message) && !is_capacity_shed(message)
}

/// Cursor Connect often records `status: 502` while the message is still
/// `Connect error 429`. grok-build maps 500/502 to "Server error (our side)".
pub fn is_upstream_rate_limit(message: &str) -> bool {
    is_billing_block(message)
        || message.contains("[resource_exhausted]")
        || message.contains("ERROR_RATE_LIMITED")
        || message.contains("ERROR_RESOURCE_EXHAUSTED")
        || message.contains("Connect error 429")
        || message.contains("Cursor error 429")
}

/// Model / provider geo fences. Cursor often wraps these as Connect 502
/// `[internal]`, which grok-build then shows as "our side".
pub fn is_geo_policy_block(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    let place = lower.contains("country")
        || lower.contains("region")
        || lower.contains("territor")
        || message.contains("国家")
        || message.contains("区域")
        || message.contains("地区");
    if !place {
        return false;
    }
    lower.contains("not available")
        || lower.contains("unsupported")
        || lower.contains("not supported")
        || lower.contains("restricted")
        || lower.contains("blocked")
        || message.contains("不支持")
}

fn embedded_connect_http_status(message: &str) -> Option<u16> {
    for label in [
        "Connect error ",
        "Cursor error ",
        "Cursor upstream HTTP ",
        "Cursor RunSSE HTTP ",
    ] {
        let mut rest = message;
        while let Some(idx) = rest.find(label) {
            rest = &rest[idx + label.len()..];
            let digits: String = rest.chars().take_while(|ch| ch.is_ascii_digit()).collect();
            if digits.len() == 3
                && let Ok(status) = digits.parse::<u16>()
                && (400..600).contains(&status)
            {
                return Some(status);
            }
        }
    }
    None
}

pub fn classify_proxy_error_status(status: u16, message: &str) -> u16 {
    if is_upstream_rate_limit(message) || status == 429 {
        return 429;
    }
    if is_geo_policy_block(message)
        || message.contains("[permission_denied]")
        || message.contains("Connect error 403")
        || message.contains("Cursor error 403")
    {
        return 403;
    }
    if message.contains("[unauthenticated]")
        || message.contains("Connect error 401")
        || message.contains("Cursor error 401")
    {
        return 401;
    }
    if message.contains("[not_found]") || message.contains("Connect error 404") {
        return 404;
    }
    if message.contains("[invalid_argument]")
        || message.contains("[failed_precondition]")
        || message.contains("Connect error 400")
        || message.contains("Cursor error 400")
    {
        return 400;
    }
    if is_ambiguous_live_accept(message) {
        return 409;
    }
    if let Some(embedded) = embedded_connect_http_status(message)
        && (400..500).contains(&embedded)
    {
        return embedded;
    }
    status
}

/// `.send()` / ResumeAction timed out: Cursor may already have the Run.
/// grok-build retries 5xx; 409 fail-closes without duplicating the turn.
pub fn is_ambiguous_live_accept(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    if lower.contains("connect failed") {
        return false;
    }
    if lower.contains("error sending request for url")
        && !lower.contains("connection reset")
        && !lower.contains("connection closed")
    {
        return false;
    }
    lower.contains("live open timed out")
        || lower.contains("response-less resumeaction")
        || lower.contains("acceptance is ambiguous")
        || lower.contains("resume produced no progress")
}

pub fn anthropic_error_kind_for_status(status: u16, message: &str) -> &'static str {
    match classify_proxy_error_status(status, message) {
        429 => "rate_limit_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        400 | 409 => "invalid_request_error",
        other if (400..500).contains(&other) => "invalid_request_error",
        _ => "api_error",
    }
}

/// Responses `error.code` grok-build maps to HTTP 500 when this is
/// `server_error`. Client/policy failures must stay off that path.
pub fn responses_error_code(kind: Option<&str>, message: &str) -> &'static str {
    match kind {
        Some("rate_limit_error") => "rate_limit",
        Some("authentication_error") => "invalid_api_key",
        Some("permission_error") => "invalid_request",
        Some("invalid_request_error" | "not_found_error") => "invalid_request",
        _ => match classify_proxy_error_status(502, message) {
            429 => "rate_limit",
            401 => "invalid_api_key",
            400 | 403 | 404 | 409 => "invalid_request",
            _ => "server_error",
        },
    }
}

pub fn compute_backoff_delay(attempt: u32, retry_after: Option<&str>) -> BackoffOutcome {
    if let Some(raw) = retry_after
        && let Ok(raw_secs) = raw.parse::<f64>()
    {
        let target_ms = (raw_secs * 1000.0).ceil() as u64;
        return BackoffOutcome {
            wait_ms: target_ms.min(RETRY_MAX_DELAY_MS),
            exceeds_budget: target_ms > RETRY_MAX_DELAY_MS,
        };
    }

    let mut exp =
        RETRY_INITIAL_DELAY_MS.saturating_mul(RETRY_BACKOFF_FACTOR.saturating_pow(attempt));
    if exp > RETRY_MAX_DELAY_MS {
        exp = RETRY_MAX_DELAY_MS;
    }
    let jitter = exp / 2;
    let wait_ms = (exp / 2) + (jitter / 2);
    BackoffOutcome {
        wait_ms,
        exceeds_budget: false,
    }
}

pub async fn sleep(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[cfg(test)]
pub async fn retry_on_statuses<T, E, F>(mut next: F) -> Result<T, E>
where
    E: std::fmt::Debug,
    F: FnMut(u32) -> Result<T, E>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        if attempt > MAX_RATE_LIMIT_RETRIES + 1 {
            break;
        }
        match next(attempt) {
            Ok(value) => return Ok(value),
            Err(err) if attempt <= MAX_RATE_LIMIT_RETRIES + 1 => {
                if attempt > MAX_RATE_LIMIT_RETRIES {
                    return Err(err);
                }
                sleep(compute_backoff_delay(attempt, None).wait_ms).await;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_transient_502_and_429_but_not_unpaid_invoice() {
        assert!(should_retry_upstream(
            502,
            "Connect error 502: Conversation data missing"
        ));
        assert!(should_retry_upstream(
            429,
            "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]"
        ));
        assert!(should_retry_upstream(503, "Connect error 503: unavailable"));
        assert!(!should_retry_upstream(
            429,
            "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — Visit cursor.com/dashboard and pay your invoice in Stripe to resume requests. [resource_exhausted]"
        ));
        assert!(
            !should_retry_upstream(
                429,
                "Connect error 429: ERROR_RESOURCE_EXHAUSTED: High Load — We're experiencing high demand for Cursor Grok 4.5 right now. Please switch to Auto, another model, or try again in a few moments. [resource_exhausted]"
            ),
            "Cursor High Load is a capacity shed; same-request retries make the 429 flood worse"
        );
        assert!(!should_retry_upstream(400, "bad request"));
        assert!(!should_retry_upstream(401, "unauthorized"));
    }

    #[test]
    fn unpaid_invoice_502_is_classified_as_429() {
        let message = "Connect error 429: ERROR_RATE_LIMITED: You have an unpaid invoice — Your team has an unpaid invoice. Please contact your team administrator to pay your invoice and continue using Cursor. [resource_exhausted]";
        assert_eq!(classify_proxy_error_status(502, message), 429);
        assert_eq!(
            anthropic_error_kind_for_status(429, message),
            "rate_limit_error"
        );
        assert_eq!(
            classify_proxy_error_status(502, "Connect error 502: Conversation data missing"),
            502
        );
    }

    #[test]
    fn geo_restriction_502_is_classified_as_403() {
        let en = "Connect error 502: ERROR_OPENAI: This model is not available in your country or region [internal]";
        assert_eq!(classify_proxy_error_status(502, en), 403);
        assert_eq!(anthropic_error_kind_for_status(502, en), "permission_error");
        assert!(!should_retry_upstream(502, en));

        let zh = "Connect error 403: 不支持的国家/区域";
        assert_eq!(classify_proxy_error_status(502, zh), 403);
        assert_eq!(
            classify_proxy_error_status(
                502,
                "Connect error 403: ERROR_CUSTOM_MESSAGE: Model not available in your region [permission_denied]"
            ),
            403
        );
        assert_eq!(
            classify_proxy_error_status(502, "Connect error 400: invalid model"),
            400
        );
        assert_eq!(
            classify_proxy_error_status(
                502,
                "Connect error 502: model slug is not supported [invalid_argument]"
            ),
            400
        );
        assert_eq!(
            classify_proxy_error_status(502, "Connect error 502: blob missing [not_found]"),
            404
        );
        assert!(!should_retry_upstream(
            502,
            "Connect error 502: model slug is not supported [invalid_argument]"
        ));
    }

    #[test]
    fn regional_outage_wording_is_not_a_geo_policy_block() {
        let outage = "Connect error 502: regional endpoint unavailable [unavailable]";
        assert_eq!(
            classify_proxy_error_status(502, outage),
            502,
            "gRPC unavailable + 'region' must not become a terminal 403"
        );
        assert!(should_retry_upstream(502, outage));
        assert!(!is_geo_policy_block(outage));
    }

    #[test]
    fn direct_cursor_http_status_in_message_is_classified() {
        assert_eq!(
            classify_proxy_error_status(502, "Cursor upstream HTTP 403"),
            403
        );
        assert_eq!(
            classify_proxy_error_status(502, "Cursor RunSSE HTTP 429"),
            429
        );
        assert_eq!(
            classify_proxy_error_status(502, "Cursor error 451: legal restriction"),
            451
        );
        assert_eq!(
            anthropic_error_kind_for_status(502, "Cursor error 451: legal restriction"),
            "invalid_request_error"
        );
    }

    #[test]
    fn unpaid_invoice_in_http_body_text_is_429() {
        let message = "Cursor error 502: Cursor upstream HTTP 502 You have an unpaid invoice — pay your invoice in Stripe";
        assert_eq!(classify_proxy_error_status(502, message), 429);
        assert!(!should_retry_upstream(502, message));
    }

    #[test]
    fn ambiguous_live_open_timeout_is_classified_as_409() {
        let messages = [
            "Cursor live open timed out after 20s",
            "Cursor error 504: Cursor live open timed out after 20s",
            "Cursor live open timed out after 10s (response-less ResumeAction send is ambiguous)",
            "Cursor BidiAppend timed out; acceptance is ambiguous",
        ];
        for message in messages {
            assert_eq!(
                classify_proxy_error_status(504, message),
                409,
                "ambiguous accept must be 409 so grok-build does not 5xx-retry: {message}"
            );
            assert_eq!(classify_proxy_error_status(502, message), 409, "{message}");
            assert!(
                !should_retry_upstream(504, message),
                "ambiguous 504 must not be same-request retryable: {message}"
            );
            assert_eq!(
                anthropic_error_kind_for_status(504, message),
                "invalid_request_error"
            );
        }
        assert_eq!(
            classify_proxy_error_status(504, "Gateway Timeout"),
            504,
            "generic 504 is not a live-open accept ambiguity"
        );
        assert_eq!(
            classify_proxy_error_status(429, "Cursor live open concurrency saturated"),
            429,
            "saturation is retryable overload, not an ambiguous accept"
        );
        assert!(should_retry_status(429));
        assert!(
            !is_ambiguous_live_accept("Cursor live open concurrency saturated"),
            "do not remap saturation onto 409"
        );
    }

    #[test]
    fn hollow_resume_produced_no_progress_is_classified_as_409() {
        let messages = [
            "Cursor resume produced no progress before the stream stalled",
            "Cursor resume produced no progress before the recovery deadline",
            "Cursor resume produced no progress before the stream ended",
        ];
        for message in messages {
            assert_eq!(
                classify_proxy_error_status(502, message),
                409,
                "hollow ResumeAction must be 409 so grok-build does not 5xx-retry: {message}"
            );
            assert!(
                !should_retry_upstream(502, message),
                "hollow resume must not be same-request retryable: {message}"
            );
        }
        assert_eq!(
            classify_proxy_error_status(502, "Cursor live run cancelled"),
            502,
            "an explicit cancel is not a hollow-resume accept ambiguity"
        );
    }

    #[test]
    fn pre_connect_failure_is_not_classified_as_409() {
        let messages = [
            "Cursor upstream connect failed",
            "Cursor BidiAppend initial Run failed; acceptance is ambiguous: Cursor upstream connect failed",
            "Cursor stream produced no useful progress (reconnect failed: Cursor BidiAppend initial Run failed; acceptance is ambiguous: Cursor upstream connect failed)",
            "Cursor KV reply send failed: Cursor error 502: Cursor BidiAppend send failed; acceptance is ambiguous: Cursor upstream connect failed",
            "error sending request for url (https://api2.cursor.sh/aiserver.v1.BidiService/BidiAppend)",
        ];
        for message in messages {
            assert!(
                !is_ambiguous_live_accept(message),
                "a request that never reached Cursor is not an accept: {message}"
            );
            assert_eq!(
                classify_proxy_error_status(502, message),
                502,
                "grok-build must 5xx-retry a pre-connect miss, not 409: {message}"
            );
            assert!(
                should_retry_upstream(502, message),
                "pre-connect 502 must stay same-request retryable: {message}"
            );
        }
        assert!(
            is_ambiguous_live_accept("Cursor BidiAppend timed out; acceptance is ambiguous"),
            "a timed-out HTTP/1 append is still an accept ambiguity"
        );
    }
}
