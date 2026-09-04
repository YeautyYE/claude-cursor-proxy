use std::time::Duration;

use rand::Rng as _;

pub const RETRY_INITIAL_DELAY_MS: u64 = 2000;
pub const RETRY_MAX_DELAY_MS: u64 = 30_000;
pub const RETRY_BACKOFF_FACTOR: u64 = 2;
pub const MAX_RATE_LIMIT_RETRIES: u32 = 3;
const RETRY_AFTER_JITTER_MAX_MS: u64 = 250;

/// Shared provider-outage classifier used by all Cursor transport paths.
///
/// Keep this forwarding API in the retry module as well as the Connect module
/// so callers that only depend on retry policy do not need to know which wire
/// parser produced the diagnostic.
pub fn is_transient_provider_error_message(message: &str) -> bool {
    crate::providers::cursor::connect::is_transient_provider_error_message(message)
}

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

/// Cursor's provider adapter sometimes reports an account/model allowance
/// failure as a generic provider error instead of a normal gRPC 429.  The
/// diagnostic looks like:
/// `ERROR_PROVIDER_ERROR providerStatusCode=400 resource_exhausted
/// isRetryable=false`.
///
/// Treat only the explicit non-retryable/provider-4xx form as an account
/// policy result.  A plain `resource_exhausted` from a 429/503 transport is
/// still transient capacity and must remain eligible for the normal backoff
/// path.
pub fn is_provider_resource_exhausted(message: &str) -> bool {
    // The provider adapter can put a temporary connectivity failure inside
    // the same `resource_exhausted`/429 envelope used for account quota.
    // Preserve the temporary provider disposition before inspecting the outer
    // resource token so it remains eligible for bounded transport retries.
    if crate::providers::cursor::connect::is_transient_provider_error_message(message) {
        return false;
    }
    let lower = message.to_ascii_lowercase();
    let resource_exhausted =
        lower.contains("resource_exhausted") || lower.contains("resource exhausted");
    if !resource_exhausted {
        return false;
    }
    // The outer Connect error may carry a generic 429/resource_exhausted
    // status, so require the provider diagnostic itself before treating this
    // as an account-specific policy result.  Do not infer it from a bare
    // `providerStatusCode=400`: unrelated gateway diagnostics can contain the
    // same number.
    let compact = lower
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect::<String>();
    let provider_error =
        compact.contains("error_provider_error") || compact.contains("providererror");
    if !provider_error || !provider_status_is_non_retryable_4xx(&compact) {
        return false;
    }
    true
}

/// Return whether a provider diagnostic contains an explicit non-retryable
/// 4xx status.  Cursor has emitted JSON, `key=value`, and human-readable forms
/// with both camelCase and snake_case field names; scan the normalized text so
/// all of those forms share one classification path.
fn provider_status_is_non_retryable_4xx(compact_lower: &str) -> bool {
    let retryable_false = ["isretryable", "is_retryable"].iter().any(|key| {
        let mut offset = 0usize;
        while let Some(relative) = compact_lower[offset..].find(key) {
            let start = offset + relative + key.len();
            let tail = compact_lower[start..].trim_start_matches(|character: char| {
                matches!(character, '"' | '\'' | ':' | '=' | ',')
            });
            if tail.starts_with("false")
                && !tail
                    .chars()
                    .nth("false".len())
                    .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
            {
                return true;
            }
            offset = start;
        }
        false
    });
    if !retryable_false {
        return false;
    }

    ["providerstatuscode", "provider_status_code"]
        .iter()
        .any(|key| {
            let mut offset = 0usize;
            while let Some(relative) = compact_lower[offset..].find(key) {
                let start = offset + relative + key.len();
                let tail = compact_lower[start..].trim_start_matches(|character: char| {
                    matches!(character, '"' | '\'' | ':' | '=' | ',')
                });
                let digits: String = tail
                    .chars()
                    .take_while(|character| character.is_ascii_digit())
                    .collect();
                if digits.len() == 3
                    && digits
                        .parse::<u16>()
                        .ok()
                        .is_some_and(|status| (400..500).contains(&status))
                {
                    return true;
                }
                offset = start;
            }
            false
        })
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

/// Definite policy 429s: plan/entitlement/account-quota rate limits that will
/// not succeed on a same-account retry (e.g. `ERROR_RATE_LIMITED_CHANGEABLE:
/// Free plans can only use Auto`, `ERROR_PRO_USER_RATE_LIMIT_EXCEEDED`). These
/// must pass through verbatim on the first upstream response instead of being
/// retried internally — and after a hot account switch they trigger an
/// automatic failover to the newly stored credentials.
pub fn is_policy_rate_limit(message: &str) -> bool {
    if is_billing_block(message) {
        return true;
    }
    if crate::providers::cursor::connect::is_transient_provider_error_message(message) {
        return false;
    }
    let lower = message.to_ascii_lowercase();
    // Cursor's Grok Bot vision meter uses a legacy OpenAI-shaped code and
    // does not include the usual ERROR_RATE_LIMITED/provider metadata.  The
    // accompanying "out of usage" message is account-local quota, not a
    // transient gateway 429; classify it before the generic resource marker
    // path so Sand does not replay the same exhausted request.
    if is_grok_bot_vision_quota(&lower) {
        return true;
    }
    is_provider_resource_exhausted(message)
        || lower.contains("error_rate_limited_changeable")
        // ERROR_PRO_USER_/ERROR_FREE_USER_/ERROR_USER_RATE_LIMIT_EXCEEDED:
        // the account's own quota window, not pool capacity.
        || lower.contains("user_rate_limit_exceeded")
        // The named-model/API allowance uses a distinct code from the
        // account-wide user meter. It is equally terminal for this account;
        // retrying it internally only creates another empty-turn wave.
        || lower.contains("api_rate_limit_exceeded")
        || (lower.contains("error_rate_limited")
            && (lower.contains("free plans")
                || lower.contains("upgrade plans")
                || lower.contains("increase limits")
                // Cursor's generic policy response is currently emitted as
                // `ERROR_RATE_LIMITED: You're out of usage. Switch to Auto`.
                // It omits the older plan/quota wording, but still denotes a
                // terminal account meter. Treating it as transient causes
                // every Claude Code retry to open another empty Run.
                || lower.contains("out of usage")
                || lower.contains("switch to auto")))
}

/// Cursor has reused the `ERROR_GPT_4_VISION_PREVIEW_RATE_LIMIT` enum for an
/// outdated-client diagnostic in older gateway revisions.  Only the variant
/// carrying the explicit Bot usage/paid-plan language is an account quota;
/// the update-required form must continue through the client-version mapper.
pub fn is_grok_bot_vision_quota(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    // Newer Sand gateways include the machine-readable allowance reason in
    // `additionalInfo` and may omit the legacy GPT-4 vision enum from the
    // flattened message.  This marker is account-local quota even when the
    // outer Connect status is a generic 429/resource_exhausted.
    if (lower.contains("sand_included_limit")
        || lower.contains("sand-included-limit")
        || lower.contains("sand included limit"))
        && !lower.contains("update required")
        && !lower.contains("version of cursor is no longer supported")
    {
        return true;
    }
    // Some responses retain only the human-readable title/detail after the
    // nested ErrorDetails object has been flattened.  Keep this independent
    // of the legacy enum so those responses do not enter Sand replay.
    if lower.contains("grok bot usage limit")
        && (lower.contains("reached")
            || lower.contains("out of usage")
            || lower.contains("included"))
        && !lower.contains("update required")
        && !lower.contains("version of cursor is no longer supported")
    {
        return true;
    }
    lower.contains("error_gpt_4_vision_preview_rate_limit")
        && (lower.contains("out of usage")
            || lower.contains("grok bot")
            || lower.contains("paid plan"))
        && !lower.contains("update required")
        && !lower.contains("version of cursor is no longer supported")
}

pub fn should_retry_upstream(status: u16, message: &str) -> bool {
    let status = classify_proxy_error_status(status, message);
    should_retry_status(status)
        && !is_billing_block(message)
        && !is_capacity_shed(message)
        && !is_policy_rate_limit(message)
        // Cursor may wrap a deterministic provider 4xx in an outer 429
        // without the usual `resource_exhausted` marker.  Retrying that same
        // request only repeats the provider rejection and amplifies a 429
        // wave; the structured diagnostic must fail closed on every route.
        && !crate::providers::cursor::connect::is_non_retryable_provider_error_message(message)
}

/// Cursor Connect often records `status: 502` while the message is still
/// `Connect error 429`. grok-build maps 500/502 to "Server error (our side)".
pub fn is_upstream_rate_limit(message: &str) -> bool {
    // The same diagnostic is wrapped by Connect, Responses, and SSE paths;
    // some of those lower-case the body before forwarding it.  Matching one
    // normalized copy prevents a lowercase 429 from becoming a misleading
    // 502 and entering the generic transport retry loop.
    let lower = message.to_ascii_lowercase();
    if is_billing_block(message) {
        return true;
    }
    if crate::providers::cursor::connect::is_transient_provider_error_message(message) {
        return false;
    }
    is_provider_resource_exhausted(message)
        || lower.contains("[resource_exhausted]")
        || lower.contains("error_rate_limited")
        || lower.contains("error_resource_exhausted")
        || lower.contains("connect error 429")
        || lower.contains("cursor error 429")
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
    // Error wrappers are not consistent about casing. `to_ascii_lowercase`
    // preserves byte offsets for the ASCII labels below, so we can search the
    // normalized view and still slice the original message for digits.
    let normalized = message.to_ascii_lowercase();
    for label in [
        "connect error ",
        "cursor error ",
        "cursor upstream http ",
        "cursor runsse http ",
        // Some Responses/CLI wrappers discard the `Cursor error <code>`
        // prefix and retain only the human-readable status phrase.
        "request too large (",
        "payload too large (",
    ] {
        let mut offset = 0usize;
        while let Some(relative) = normalized[offset..].find(label) {
            let idx = offset + relative;
            let end = idx + label.len();
            let rest = &message[end..];
            let digits: String = rest.chars().take_while(|ch| ch.is_ascii_digit()).collect();
            if digits.len() == 3
                && let Ok(status) = digits.parse::<u16>()
                && (400..600).contains(&status)
            {
                return Some(status);
            }
            offset = end;
        }
    }
    None
}

/// Local admission backpressure (semaphore queues and the per-session busy
/// gate). These start as 503 + Retry-After but the late-retry engine folds
/// them into event strings ("Cursor error 503: ..."), which would otherwise
/// surface as 502 — grok-build then reports "Server error (our side)"
/// instead of backing off.
pub fn is_local_admission_backpressure(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("sand accepted-stream admission")
        || lower.contains("sand stream capacity is unavailable")
        || lower.contains("sand open admission queue timed out")
        || lower.contains("sand inference open admission deadline exhausted")
        || lower.contains("cursor live generation admission queue timed out")
        || lower.contains("cursor live run admission queue timed out")
        || lower.contains("already active for this session")
}

/// Detect an idle timeout where the upstream produced no client-visible
/// progress. Cursor and the different transport paths use several slightly
/// different diagnostics for this condition (for example
/// `idle timeout after 45s with no useful progress` and
/// `Stream idle timeout - no chunks received`).  These errors are different
/// from a normal post-output idle completion: the request may already have
/// been accepted by Cursor, so replaying it as a generic 5xx can create a
/// second live Run and a 409/503 retry storm.
pub fn is_idle_no_progress(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();

    // Keep the matcher deliberately conjunctive.  `stream idle` by itself
    // also appears in diagnostics after useful output and must remain a
    // normal completion/transport signal rather than an acceptance ambiguity.
    let idle = lower.contains("idle timeout")
        || lower.contains("stream idle")
        || lower.contains("setup idle")
        || lower.contains("idle stall");
    if !idle {
        return false;
    }
    lower.contains("no useful progress")
        || lower.contains("no chunks received")
        || lower.contains("no response bytes")
        || lower.contains("0 response bytes")
        || lower.contains("no decodable text")
        || lower.contains("no text yet")
        || lower.contains("without text or tool calls")
        || lower.contains("without useful output")
}

pub fn classify_proxy_error_status(status: u16, message: &str) -> u16 {
    // Once Cursor may have accepted any part of an operation, replay safety
    // dominates a nested child status such as 429/503. Mapping the child first
    // would invite the caller to start a second Run.
    if is_ambiguous_live_accept(message) {
        return 409;
    }
    if is_local_admission_backpressure(message) {
        return 503;
    }
    // A provider-connectivity diagnostic may be wrapped in an outer 400/429
    // (`resource_exhausted`). Expose it as a retryable service outage rather
    // than an account rate limit; this also makes Anthropic responses use
    // `api_error` and lets bounded Sand/CLI retry loops run.
    if crate::providers::cursor::connect::is_transient_provider_error_message(message) {
        return 503;
    }
    if is_upstream_rate_limit(message) || status == 429 {
        return 429;
    }
    let lower = message.to_ascii_lowercase();
    if is_geo_policy_block(message)
        || lower.contains("[permission_denied]")
        || lower.contains("connect error 403")
        || lower.contains("cursor error 403")
    {
        return 403;
    }
    if lower.contains("[unauthenticated]")
        || lower.contains("connect error 401")
        || lower.contains("cursor error 401")
    {
        return 401;
    }
    if lower.contains("[not_found]") || lower.contains("connect error 404") {
        return 404;
    }
    if lower.contains("[invalid_argument]")
        || lower.contains("[failed_precondition]")
        || lower.contains("connect error 400")
        || lower.contains("cursor error 400")
    {
        return 400;
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
    if lower.contains("tool-result batch partially sent")
        || lower.contains("tool result batch partially sent")
    {
        return true;
    }
    if lower.contains("connect failed") {
        return false;
    }
    if lower.contains("error sending request for url")
        && !lower.contains("connection reset")
        && !lower.contains("connection closed")
    {
        return false;
    }
    is_idle_no_progress(message)
        || lower.contains("live open timed out")
        || lower.contains("response-less resumeaction")
        || lower.contains("acceptance is ambiguous")
        || lower.contains("completion is ambiguous")
        || lower.contains("resume produced no progress")
        || lower.contains("stream produced no useful progress")
        || lower.contains("tool result wait expired")
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
            status if (400..500).contains(&status) => "invalid_request",
            _ => "server_error",
        },
    }
}

pub fn compute_backoff_delay(attempt: u32, retry_after: Option<&str>) -> BackoffOutcome {
    compute_backoff_delay_with_sampler(attempt, retry_after, |upper| {
        if upper == 0 {
            0
        } else {
            rand::thread_rng().gen_range(0..=upper)
        }
    })
}

fn compute_backoff_delay_with_sampler<F>(
    attempt: u32,
    retry_after: Option<&str>,
    mut sample_inclusive: F,
) -> BackoffOutcome
where
    F: FnMut(u64) -> u64,
{
    if let Some(raw) = retry_after
        && let Ok(raw_secs) = raw.parse::<f64>()
        && raw_secs.is_finite()
        && raw_secs >= 0.0
    {
        let target_ms = (raw_secs * 1000.0).ceil() as u64;
        let jitter_cap = retry_after_jitter_cap_ms(target_ms);
        let jitter = sample_inclusive(jitter_cap);
        let proposed = target_ms.saturating_add(jitter);
        return BackoffOutcome {
            // Do not shorten an explicit server cooldown. Callers that have
            // a 30s logical retry budget use `exceeds_budget` to stop before
            // sleeping; callers that honor Retry-After directly still wait
            // at least the service-provided duration.
            wait_ms: proposed,
            exceeds_budget: proposed > RETRY_MAX_DELAY_MS,
        };
    }

    let mut exp =
        RETRY_INITIAL_DELAY_MS.saturating_mul(RETRY_BACKOFF_FACTOR.saturating_pow(attempt));
    if exp > RETRY_MAX_DELAY_MS {
        exp = RETRY_MAX_DELAY_MS;
    }
    let lower = exp / 2;
    let upper = exp.saturating_sub(lower);
    let wait_ms = lower.saturating_add(sample_inclusive(upper));
    BackoffOutcome {
        wait_ms,
        exceeds_budget: false,
    }
}

fn retry_after_jitter_cap_ms(target_ms: u64) -> u64 {
    target_ms
        .saturating_div(10)
        .clamp(1, RETRY_AFTER_JITTER_MAX_MS)
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
    fn deterministic_provider_4xx_wrapped_in_429_is_not_retried() {
        let message =
            "Connect error 429: ERROR_PROVIDER_ERROR providerStatusCode=400 isRetryable=false";
        assert!(
            crate::providers::cursor::connect::is_non_retryable_provider_error_message(message)
        );
        assert_eq!(classify_proxy_error_status(429, message), 429);
        assert!(!should_retry_upstream(429, message));
    }

    #[test]
    fn local_admission_backpressure_stays_503_even_when_wrapped() {
        // The late-retry engine folds local 503s into event strings; the
        // classifier must map them back to 503 so clients back off instead of
        // reporting "Server error (our side)" from a 502.
        for text in [
            "Sand accepted-stream admission deadline exhausted; retry after active streams drain",
            "Sand stream capacity is unavailable",
            "Sand open admission queue timed out; retry after upstream capacity recovers",
            "Sand inference open admission deadline exhausted; retry after upstream capacity recovers",
            "Cursor error 503: Cursor live generation admission queue timed out",
            "Cursor live generation admission queue timed out",
            "Cursor error 503: Cursor live run admission queue timed out",
            "Cursor error 503: A Cursor live run is already active for this session; retry after it advances",
        ] {
            assert!(is_local_admission_backpressure(text), "{text}");
            assert_eq!(classify_proxy_error_status(502, text), 503, "{text}");
            assert_eq!(anthropic_error_kind_for_status(502, text), "api_error");
        }
        assert!(!is_local_admission_backpressure(
            "Connect error 503: upstream unavailable"
        ));
    }

    #[test]
    fn equal_jitter_backoff_stays_within_expected_range() {
        let min = compute_backoff_delay_with_sampler(0, None, |_| 0);
        let max = compute_backoff_delay_with_sampler(0, None, |upper| upper);
        assert_eq!(min.wait_ms, RETRY_INITIAL_DELAY_MS / 2);
        assert_eq!(max.wait_ms, RETRY_INITIAL_DELAY_MS);
        assert!(!min.exceeds_budget);
        assert!(!max.exceeds_budget);

        let capped = compute_backoff_delay_with_sampler(10, None, |upper| upper);
        assert!(capped.wait_ms >= RETRY_MAX_DELAY_MS / 2);
        assert!(capped.wait_ms <= RETRY_MAX_DELAY_MS);
    }

    #[test]
    fn retry_after_waits_at_least_server_value_and_can_add_small_jitter() {
        let base = compute_backoff_delay_with_sampler(0, Some("2.5"), |_| 0);
        let jittered = compute_backoff_delay_with_sampler(0, Some("2.5"), |upper| upper);
        assert_eq!(base.wait_ms, 2500);
        assert_eq!(jittered.wait_ms, 2750);
        assert!(!base.exceeds_budget);
        assert!(!jittered.exceeds_budget);

        let near_budget = compute_backoff_delay_with_sampler(0, Some("29.9"), |upper| upper);
        assert!(near_budget.wait_ms >= 29_900);
    }

    #[test]
    fn retry_after_over_budget_reports_exceeds_budget() {
        let over = compute_backoff_delay_with_sampler(0, Some("31"), |_| 0);
        assert_eq!(over.wait_ms, 31_000);
        assert!(over.exceeds_budget);
    }

    #[test]
    fn policy_rate_limit_is_terminal_and_never_internally_retried() {
        let free_plan = "Connect error 429: ERROR_RATE_LIMITED_CHANGEABLE: Named models unavailable — Free plans can only use Auto. [resource_exhausted]";
        assert!(is_policy_rate_limit(free_plan));
        assert!(
            !should_retry_upstream(429, free_plan),
            "a plan-entitlement 429 will never succeed on retry; hidden retries multiply the flood"
        );
        assert_eq!(
            classify_proxy_error_status(502, free_plan),
            429,
            "the verbatim policy 429 must reach the client as HTTP 429"
        );

        let limits = "Connect error 429: ERROR_RATE_LIMITED: Increase limits for faster responses at cursor.com/dashboard [resource_exhausted]";
        assert!(is_policy_rate_limit(limits));
        assert!(!should_retry_upstream(429, limits));

        // Cursor's newer generic wording does not mention a plan or an
        // explicit `increase limits` action. It is still the same terminal
        // account meter and must not enter the transport retry loop.
        let generic_out_of_usage = "Connect error 429: ERROR_RATE_LIMITED: You're out of usage. Switch to Auto, or ask your admin to increase your limit to continue. [resource_exhausted]";
        assert!(is_policy_rate_limit(generic_out_of_usage));
        assert!(!should_retry_upstream(429, generic_out_of_usage));

        let invoice = "You have an unpaid invoice — pay your invoice to continue";
        assert!(is_policy_rate_limit(invoice), "billing blocks are a subset");

        // Per-account quota windows (observed 2026-08-23 after an account
        // switch): must not be hidden-retried on the same login.
        let pro_quota = "Connect error 429: ERROR_PRO_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded. — You have exceeded your usage limit. [resource_exhausted]";
        assert!(is_policy_rate_limit(pro_quota));
        assert!(!should_retry_upstream(429, pro_quota));
        assert_eq!(classify_proxy_error_status(502, pro_quota), 429);
        assert!(is_policy_rate_limit(
            "Connect error 429: ERROR_FREE_USER_RATE_LIMIT_EXCEEDED: Rate limit exceeded [resource_exhausted]"
        ));

        let api_quota = "Connect error 429: ERROR_CURSOR_API_RATE_LIMIT_EXCEEDED: Cursor API usage meter is 100% [resource_exhausted]";
        assert!(is_policy_rate_limit(api_quota));
        assert!(!should_retry_upstream(429, api_quota));
        assert_eq!(classify_proxy_error_status(502, api_quota), 429);

        // Newer Cursor provider responses use HTTP 400 for the same
        // account/model meter.  The nested provider diagnostic is the only
        // reliable signal; it must enter the policy breaker and account
        // failover path instead of being retried as an invalid request.
        let provider_quota =
            "ERROR_PROVIDER_ERROR providerStatusCode=400 resource_exhausted isRetryable=false";
        assert!(is_provider_resource_exhausted(provider_quota));
        assert!(is_policy_rate_limit(provider_quota));
        assert_eq!(classify_proxy_error_status(400, provider_quota), 429);
        assert!(!should_retry_upstream(400, provider_quota));

        // Deterministic provider rejections are account-specific for every
        // HTTP 4xx, not only the observed 400 variant.
        for status in [403, 404, 429, 499] {
            let message = format!(
                "{{\"error\":{{\"code\":\"resource_exhausted\",\"details\":[{{\"debug\":{{\"error\":\"ERROR_PROVIDER_ERROR\",\"details\":{{\"additionalInfo\":{{\"providerStatusCode\":{status}}},\"isRetryable\":false}}}}}}]}}}}"
            );
            assert!(
                is_provider_resource_exhausted(&message),
                "provider status {status} should be account-terminal"
            );
            assert!(!should_retry_upstream(status, &message));
        }

        // A retryable provider-capacity response must retain its transient
        // semantics even when it mentions the same resource token.
        for message in [
            "ERROR_PROVIDER_ERROR providerStatusCode=500 resource_exhausted isRetryable=false",
            "ERROR_PROVIDER_ERROR providerStatusCode=502 resource_exhausted isRetryable=false",
            "ERROR_PROVIDER_ERROR providerStatusCode=503 resource_exhausted isRetryable=false",
            "ERROR_PROVIDER_ERROR providerStatusCode=504 resource_exhausted isRetryable=false",
            "ERROR_PROVIDER_ERROR providerStatusCode=503 resource_exhausted isRetryable=true",
            "ERROR_PROVIDER_ERROR resource_exhausted isRetryable=false",
        ] {
            assert!(!is_provider_resource_exhausted(message), "{message}");
        }
        assert!(should_retry_upstream(
            503,
            "ERROR_PROVIDER_ERROR providerStatusCode=503 resource_exhausted isRetryable=false"
        ));

        // JSON and key/value spellings use the same parser. A field named
        // `isRetryableExtra` must not be mistaken for the retry hint.
        assert!(is_provider_resource_exhausted(
            r#"{"error": {"code": "resource_exhausted", "details": [{"debug": {"error": "ERROR_PROVIDER_ERROR", "details": {"additional_info": {"provider_status_code": "400"}, "is_retryable": false}}}]}}"#
        ));
        assert!(!is_provider_resource_exhausted(
            "ERROR_PROVIDER_ERROR providerStatusCode=400 resource_exhausted isRetryableExtra=false"
        ));

        let transient = "Connect error 429: ERROR_RESOURCE_EXHAUSTED: Unable to reach the model provider [resource_exhausted]";
        assert!(
            !is_policy_rate_limit(transient),
            "transient provider exhaustion must stay retryable"
        );
        assert!(should_retry_upstream(429, transient));
    }

    #[test]
    fn grok_bot_vision_quota_is_terminal_policy_limit() {
        let message = "Connect error 429: ERROR_GPT_4_VISION_PREVIEW_RATE_LIMIT: You are out of usage — Upgrade to a paid plan to use more Grok Bot. [resource_exhausted]";
        assert!(is_policy_rate_limit(message));
        assert!(!should_retry_upstream(429, message));
        for message in [
            // Current Sand response after the ErrorDetails object is
            // flattened into a short client diagnostic.
            "Connect error 429: resource_exhausted rateLimitReason=sand_included_limit isRetryable=false",
            "Cursor error 429: You've reached your Grok Bot usage limit — included Grok Bot usage limit",
            // JSON spelling emitted by the dashboard/provider adapter.
            r#"{"error":{"code":"resource_exhausted","details":[{"debug":{"details":{"additionalInfo":{"rateLimitReason":"sand_included_limit"}}}}]}}"#,
        ] {
            assert!(is_grok_bot_vision_quota(message), "{message}");
            assert!(is_policy_rate_limit(message), "{message}");
            assert!(!should_retry_upstream(429, message), "{message}");
        }
        let outdated = "Connect error 429: ERROR_GPT_4_VISION_PREVIEW_RATE_LIMIT: Update Required — Your version of Cursor is no longer supported. [resource_exhausted]";
        assert!(!is_grok_bot_vision_quota(outdated));
        assert!(!is_policy_rate_limit(outdated));
    }

    #[test]
    fn nested_temporary_provider_error_is_service_outage_not_account_quota() {
        let messages = [
            "Connect error 429: ERROR_PROVIDER_ERROR: Provider Error — temporary trouble connecting to the model provider [providerStatusCode=400,isRetryable=false]",
            "Cursor error 400: ERROR_PROVIDER_ERROR provider unavailable; try again in a moment [provider_status_code=400,is_retryable=false]",
            "{\"error\":{\"code\":\"resource_exhausted\",\"details\":[{\"debug\":{\"error\":\"ERROR_PROVIDER_ERROR\",\"details\":{\"detail\":\"upstream connection reset\",\"additionalInfo\":{\"providerStatusCode\":400},\"isRetryable\":false}}}]}}",
        ];
        for message in messages {
            assert!(is_transient_provider_error_message(message), "{message}");
            assert!(!is_provider_resource_exhausted(message), "{message}");
            assert!(!is_policy_rate_limit(message), "{message}");
            assert!(!is_upstream_rate_limit(message), "{message}");
            assert_eq!(
                classify_proxy_error_status(429, message),
                503,
                "temporary provider failures should be surfaced as api_error/503"
            );
            assert!(should_retry_upstream(429, message), "{message}");
        }
    }

    #[test]
    fn provider_quota_stays_terminal_even_with_provider_error_envelope() {
        let message = "Connect error 429: ERROR_PROVIDER_ERROR provider unavailable; out of usage resource_exhausted [providerStatusCode=400,isRetryable=false]";
        assert!(!is_transient_provider_error_message(message));
        assert!(is_provider_resource_exhausted(message));
        assert!(is_policy_rate_limit(message));
        assert_eq!(classify_proxy_error_status(429, message), 429);
        assert!(!should_retry_upstream(429, message));
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

        // Responses/SSE wrappers can normalize the diagnostic labels. Keep
        // status and error-kind mapping stable regardless of that casing.
        assert_eq!(
            classify_proxy_error_status(502, "cursor upstream http 403"),
            403
        );
        assert_eq!(
            classify_proxy_error_status(502, "CURSOR RUNSSE HTTP 429"),
            429
        );
        assert_eq!(
            responses_error_code(
                None,
                "Request too large (413): Cursor KV blob store limit exceeded"
            ),
            "invalid_request",
            "pre-output 413s must not be emitted as Responses server_error"
        );
    }

    #[test]
    fn rate_limit_wrappers_are_case_insensitive() {
        for message in [
            "connect error 429: error_resource_exhausted [resource_exhausted]",
            "Cursor error 429: Error_Rate_Limited: quota window",
            "CURSOR ERROR 429: [RESOURCE_EXHAUSTED]",
        ] {
            assert!(is_upstream_rate_limit(message), "{message}");
            assert_eq!(classify_proxy_error_status(502, message), 429, "{message}");
            assert!(should_retry_upstream(502, message), "{message}");
        }
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
    fn heartbeat_live_ambiguous_completion_is_classified_as_409_immediately() {
        let message = "Cursor stream produced no useful progress; upstream transport remained live, so completion is ambiguous";

        assert_eq!(
            classify_proxy_error_status(502, message),
            409,
            "the first response must agree with the ambiguous live-run tombstone"
        );
        assert!(
            !should_retry_upstream(502, message),
            "a still-live upstream Run must not be replayed by the HTTP client"
        );
        assert_eq!(
            anthropic_error_kind_for_status(502, message),
            "invalid_request_error"
        );
    }

    #[test]
    fn idle_no_progress_variants_are_ambiguous_without_replaying_the_run() {
        let messages = [
            "idle timeout after 45s with no useful progress (0 response bytes — check Surge node / auth)",
            "Cursor error 502: idle timeout after 45s with no useful progress",
            "Stream idle timeout - no chunks received",
            "Cursor stream idle (no response bytes)",
            "idle timeout after 20s with no useful progress (got 3 Connect frames / 69 bytes; no decodable text/thinking yet)",
            "idle timeout after 12s with thinking but no text yet",
        ];
        for message in messages {
            assert!(is_idle_no_progress(message), "{message}");
            assert!(is_ambiguous_live_accept(message), "{message}");
            assert_eq!(
                classify_proxy_error_status(502, message),
                409,
                "an accepted-but-hollow idle run must not be exposed as retryable 5xx: {message}"
            );
            assert!(
                !should_retry_upstream(502, message),
                "the downstream client must not create another Run for {message}"
            );
        }
        let pre_connect =
            "idle timeout after 45s with no useful progress (error sending request for url)";
        assert!(is_idle_no_progress(pre_connect));
        assert!(
            !is_ambiguous_live_accept(pre_connect),
            "a pre-connect transport miss must stay retryable"
        );
        assert_eq!(classify_proxy_error_status(502, pre_connect), 502);
        assert!(should_retry_upstream(502, pre_connect));
    }

    #[test]
    fn idle_marker_without_no_progress_detail_is_not_misclassified() {
        for message in [
            "stream idle after useful text",
            "idle timeout after 2s while completing a response",
            "Cursor stream idle; 12 chunks received",
        ] {
            assert!(
                !is_idle_no_progress(message),
                "ordinary post-output idle must not become an acceptance ambiguity: {message}"
            );
            assert!(!is_ambiguous_live_accept(message), "{message}");
        }
    }

    #[test]
    fn unresolved_live_failures_never_become_retryable_from_nested_statuses() {
        let messages = [
            "Cursor stream produced no useful progress",
            "Cursor tool result wait expired",
            "Cursor error 429: Cursor tool-result batch partially sent (1/2); acceptance is ambiguous: ERROR_RESOURCE_EXHAUSTED",
            "Cursor error 503: Cursor tool-result batch partially sent (1/2); acceptance is ambiguous: upstream unavailable",
        ];

        for message in messages {
            assert_eq!(
                classify_proxy_error_status(502, message),
                409,
                "unknown or partial acceptance must dominate nested HTTP status: {message}"
            );
            assert!(
                !should_retry_upstream(502, message),
                "grok-build must not replay an unresolved operation: {message}"
            );
        }
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
