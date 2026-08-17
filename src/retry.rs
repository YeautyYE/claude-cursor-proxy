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

pub fn should_retry_upstream(status: u16, message: &str) -> bool {
    should_retry_status(status) && !is_billing_block(message)
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
        assert!(!should_retry_upstream(400, "bad request"));
        assert!(!should_retry_upstream(401, "unauthorized"));
    }
}
