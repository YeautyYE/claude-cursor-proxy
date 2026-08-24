//! Official Cursor dashboard usage (read-only).
//!
//! These endpoints are the same ones Cursor's website and tools like CodexBar
//! use to render Auto / API / Grok Bot bars. They do **not** change
//! `x-cursor-client-type`; Sand request routing is handled independently by the
//! Cursor provider and never by the dashboard poller.

use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::monitor::{AccountUsageEvent, AccountUsageSnapshot, AccountUsageState};
use crate::providers::cursor::auth::{CursorAuth, load_cursor_auth, load_cursor_desktop_auth};

const DASHBOARD_ORIGIN: &str = "https://cursor.com";
const USAGE_SUMMARY_PATH: &str = "/api/usage-summary";
const AUTH_ME_PATH: &str = "/api/auth/me";
const AGGREGATED_USAGE_PATH: &str = "/api/dashboard/get-aggregated-usage-events";
const FILTERED_USAGE_PATH: &str = "/api/dashboard/get-filtered-usage-events";
const SAND_USAGE_PATH: &str = "/api/dashboard/get-sand-usage-status";
const FETCH_TIMEOUT: Duration = Duration::from_secs(8);
const SAND_TIMEOUT: Duration = Duration::from_secs(5);
const EVENTS_TIMEOUT: Duration = Duration::from_secs(5);

pub fn fetch_account_usage_state() -> AccountUsageState {
    let auth = match load_cursor_auth() {
        Ok(Some(auth)) => Some(auth),
        Ok(None) => load_cursor_desktop_auth().ok().flatten(),
        Err(err) => match load_cursor_desktop_auth().ok().flatten() {
            Some(auth) => Some(auth),
            None => return AccountUsageState::Failed(truncate_error(&err.to_string())),
        },
    };
    match auth {
        Some(auth) => match fetch_account_usage(&auth) {
            Ok(snapshot) => AccountUsageState::Ready(snapshot),
            Err(err) => AccountUsageState::Failed(truncate_error(&err.to_string())),
        },
        None => AccountUsageState::MissingAuth,
    }
}

pub async fn poll_cursor_account_usage(monitor: crate::monitor::MonitorHandle) {
    loop {
        let state = match tokio::task::spawn_blocking(fetch_account_usage_state).await {
            Ok(state) => state,
            Err(_) => AccountUsageState::Failed("usage poller cancelled".into()),
        };
        monitor.set_account_usage(state);
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

pub fn fetch_account_usage(auth: &CursorAuth) -> anyhow::Result<AccountUsageSnapshot> {
    let cookie = workos_session_cookie(auth);
    let client = reqwest::blocking::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .build()?;
    fetch_account_usage_with(auth, DASHBOARD_ORIGIN, &client, &cookie)
}

fn fetch_account_usage_with(
    auth: &CursorAuth,
    origin: &str,
    client: &reqwest::blocking::Client,
    cookie: &str,
) -> anyhow::Result<AccountUsageSnapshot> {
    // `usage-summary` is the richest response, but it has not been enabled
    // for every account/dashboard deployment. Keep the independent identity
    // and Sand meters useful when that endpoint is absent.
    let summary_result = dashboard_get(client, origin, USAGE_SUMMARY_PATH, cookie, FETCH_TIMEOUT);
    let me = dashboard_get(client, origin, AUTH_ME_PATH, cookie, FETCH_TIMEOUT).ok();
    let aggregated = dashboard_post(
        client,
        origin,
        AGGREGATED_USAGE_PATH,
        cookie,
        r#"{"teamId":0}"#,
        EVENTS_TIMEOUT,
    )
    .ok();
    let filtered = dashboard_post(
        client,
        origin,
        FILTERED_USAGE_PATH,
        cookie,
        r#"{"teamId":0,"page":1,"pageSize":30}"#,
        EVENTS_TIMEOUT,
    )
    .ok();
    let sand = dashboard_post(client, origin, SAND_USAGE_PATH, cookie, "{}", SAND_TIMEOUT).ok();
    let summary = match summary_result {
        Ok(summary) => summary,
        Err(_error)
            if me.is_some() || aggregated.is_some() || filtered.is_some() || sand.is_some() =>
        {
            Value::Object(Default::default())
        }
        Err(error) => return Err(error),
    };
    Ok(parse_account_usage_with_events(
        auth,
        &summary,
        me.as_ref(),
        sand.as_ref(),
        aggregated.as_ref(),
        filtered.as_ref(),
    ))
}

pub(crate) fn workos_session_cookie(auth: &CursorAuth) -> String {
    let raw = match auth
        .user_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(user_id) => format!("{}::{}", user_id, auth.access_token),
        None => auth.access_token.clone(),
    };
    format!("WorkosCursorSessionToken={}", percent_encode_cookie(&raw))
}

fn percent_encode_cookie(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn dashboard_get(
    client: &reqwest::blocking::Client,
    origin: &str,
    path: &str,
    cookie: &str,
    timeout: Duration,
) -> anyhow::Result<Value> {
    let url = format!("{}{path}", origin.trim_end_matches('/'));
    let resp = client
        .get(url)
        .header("Accept", "application/json")
        .header("Origin", origin)
        .header("Referer", format!("{origin}/dashboard"))
        .header(
            "User-Agent",
            format!("claude-cursor-proxy/{}", env!("CARGO_PKG_VERSION")),
        )
        .header("Cookie", cookie)
        .timeout(timeout)
        .send()?;
    parse_dashboard_response(resp)
}

fn dashboard_post(
    client: &reqwest::blocking::Client,
    origin: &str,
    path: &str,
    cookie: &str,
    body: &str,
    timeout: Duration,
) -> anyhow::Result<Value> {
    let url = format!("{}{path}", origin.trim_end_matches('/'));
    let resp = client
        .post(url)
        .header("Accept", "application/json")
        .header("Content-Type", "application/json")
        .header("Origin", origin)
        .header("Referer", format!("{origin}/dashboard"))
        .header(
            "User-Agent",
            format!("claude-cursor-proxy/{}", env!("CARGO_PKG_VERSION")),
        )
        .header("Cookie", cookie)
        .timeout(timeout)
        .body(body.to_string())
        .send()?;
    parse_dashboard_response(resp)
}

fn parse_dashboard_response(resp: reqwest::blocking::Response) -> anyhow::Result<Value> {
    let status = resp.status();
    let text = resp.text().unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("cursor dashboard {status}: {}", truncate_error(&text));
    }
    serde_json::from_str(&text)
        .map_err(|err| anyhow::anyhow!("cursor dashboard JSON: {err}: {}", truncate_error(&text)))
}

#[cfg(test)]
pub(crate) fn parse_account_usage(
    auth: &CursorAuth,
    summary: &Value,
    me: Option<&Value>,
    sand: Option<&Value>,
) -> AccountUsageSnapshot {
    parse_account_usage_with_events(auth, summary, me, sand, None, None)
}

fn parse_account_usage_with_events(
    auth: &CursorAuth,
    summary: &Value,
    me: Option<&Value>,
    sand: Option<&Value>,
    aggregated: Option<&Value>,
    filtered: Option<&Value>,
) -> AccountUsageSnapshot {
    let plan = summary.pointer("/individualUsage/plan");
    let overall = summary.pointer("/individualUsage/overall");
    let pooled = summary.pointer("/teamUsage/pooled");
    let auto_percent = json_f64(plan.and_then(|p| p.get("autoPercentUsed")));
    let api_percent = json_f64(plan.and_then(|p| p.get("apiPercentUsed")));
    let total_percent = json_f64(plan.and_then(|p| p.get("totalPercentUsed"))).or_else(|| {
        match (auto_percent, api_percent) {
            (Some(auto), Some(api)) => Some((auto + api) / 2.0),
            (Some(auto), None) => Some(auto),
            (None, Some(api)) => Some(api),
            (None, None) => percent_from_cents(plan)
                .or_else(|| percent_from_cents(overall))
                .or_else(|| percent_from_cents(pooled)),
        }
    });
    let (plan_used_usd, plan_limit_usd) = usd_pair(plan)
        .or_else(|| usd_pair(overall))
        .or_else(|| usd_pair(pooled))
        .unwrap_or((None, None));
    let on_demand = summary.pointer("/individualUsage/onDemand");
    let (on_demand_used_usd, on_demand_limit_usd) = usd_pair(on_demand).unwrap_or((None, None));

    let email = string_field(me.and_then(|v| v.get("email"))).or_else(|| auth.email.clone());
    let membership = string_field(summary.get("membershipType"))
        .or_else(|| string_field(summary.pointer("/individualUsage/membershipType")))
        .or_else(|| string_field(me.and_then(|value| value.get("membershipType"))))
        .or_else(|| string_field(me.and_then(|value| value.get("membership"))));

    let grok_bot = parse_grok_bot(sand);
    let total_cost_usd = json_f64(aggregated.and_then(|value| value.get("totalCostCents")))
        .map(|cents| cents / 100.0);
    let usage_event_count = json_u64(
        filtered
            .and_then(|value| value.get("totalUsageEventsCount"))
            .or_else(|| aggregated.and_then(|value| value.get("totalUsageEventsCount"))),
    );

    AccountUsageSnapshot {
        email,
        membership,
        auto_percent,
        api_percent,
        total_percent,
        plan_used_usd,
        plan_limit_usd,
        on_demand_used_usd,
        on_demand_limit_usd,
        grok_bot_percent: grok_bot.0,
        grok_bot_period_start: parse_grok_bot_period_start(sand),
        grok_bot_reset: grok_bot.1,
        total_cost_usd,
        usage_event_count,
        usage_events: parse_usage_events(filtered),
        fetched_at: SystemTime::now(),
    }
}

fn parse_grok_bot_period_start(sand: Option<&Value>) -> Option<String> {
    sand.and_then(|value| dashboard_timestamp(value.get("currentPeriodStart")))
}

fn parse_usage_events(filtered: Option<&Value>) -> Vec<AccountUsageEvent> {
    let Some(events) = filtered
        .and_then(|value| value.get("usageEventsDisplay"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    events
        .iter()
        .filter_map(|event| {
            if !event.is_object() {
                return None;
            }
            let timestamp = dashboard_timestamp(event.get("timestamp"));
            let model = string_field(event.get("model"));
            let charged_usd = json_f64(event.get("chargedCents")).map(|cents| cents / 100.0);
            let kind = string_field(event.get("kind"))
                .map(|kind| kind.trim_start_matches("USAGE_EVENT_KIND_").to_string());
            if timestamp.is_none() && model.is_none() && charged_usd.is_none() && kind.is_none() {
                None
            } else {
                Some(AccountUsageEvent {
                    timestamp,
                    model,
                    charged_usd,
                    kind,
                })
            }
        })
        .collect()
}

fn parse_grok_bot(sand: Option<&Value>) -> (Option<f64>, Option<String>) {
    let Some(sand) = sand else {
        return (None, None);
    };
    let percent = json_f64(sand.get("usagePercent"));
    if percent.is_none() {
        return (None, None);
    }
    let reset = dashboard_timestamp(sand.get("nextResetTimestampUtc"));
    (percent, reset)
}

fn usd_pair(node: Option<&Value>) -> Option<(Option<f64>, Option<f64>)> {
    let node = node?;
    let used = json_f64(node.get("used")).map(|cents| cents / 100.0);
    let limit = json_f64(node.get("limit")).map(|cents| cents / 100.0);
    if used.is_none() && limit.is_none() {
        None
    } else {
        Some((used, limit))
    }
}

fn percent_from_cents(node: Option<&Value>) -> Option<f64> {
    let node = node?;
    let used = json_f64(node.get("used"))?;
    let limit = json_f64(node.get("limit"))?;
    if limit <= 0.0 {
        return None;
    }
    Some((used / limit) * 100.0)
}

fn json_f64(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    let number = value
        .as_f64()
        .or_else(|| value.as_i64().map(|n| n as f64))
        .or_else(|| value.as_u64().map(|n| n as f64))
        .or_else(|| {
            value
                .as_str()
                .and_then(|raw| raw.trim().parse::<f64>().ok())
        });
    number.filter(|number| number.is_finite())
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    let value = value?;
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
        .or_else(|| {
            value
                .as_str()
                .and_then(|raw| raw.trim().parse::<u64>().ok())
        })
}

/// Cursor dashboard timestamps are returned as ISO strings by some deployments
/// and epoch seconds/milliseconds by others. Normalize both to an ISO string so
/// the TUI keeps showing period and event times across deployments.
fn dashboard_timestamp(value: Option<&Value>) -> Option<String> {
    let value = value?;
    if let Some(raw) = value.as_str() {
        let raw = raw.trim();
        if raw.is_empty() {
            return None;
        }
        if let Ok(number) = raw.parse::<f64>() {
            return dashboard_timestamp_number(number);
        }
        return Some(raw.to_string());
    }
    let number = json_f64(Some(value))?;
    dashboard_timestamp_number(number)
}

fn dashboard_timestamp_number(number: f64) -> Option<String> {
    let millis = if number.abs() < 10_000_000_000.0 {
        number * 1_000.0
    } else {
        number
    };
    if !millis.is_finite() || millis.abs() > 9.0e15 {
        return Some(format!("{number:.0}"));
    }
    let nanos = (millis * 1_000_000.0).round() as i128;
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|timestamp| {
            timestamp
                .format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .or_else(|| Some(format!("{number:.0}")))
}

fn string_field(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn truncate_error(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= 80 {
        collapsed
    } else {
        collapsed.chars().take(77).collect::<String>() + "..."
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::cursor::auth::CursorAuth;

    fn auth(user: &str, token: &str) -> CursorAuth {
        CursorAuth {
            access_token: token.into(),
            refresh_token: None,
            api_key: None,
            expires: None,
            user_id: Some(user.into()),
            email: Some("dev@example.com".into()),
            source: "test".into(),
        }
    }

    #[test]
    fn workos_cookie_urlencodes_user_and_token() {
        let cookie = workos_session_cookie(&auth("user_1", "tok:en"));
        assert_eq!(cookie, "WorkosCursorSessionToken=user_1%3A%3Atok%3Aen");
    }

    #[test]
    fn parse_usage_maps_official_dashboard_buckets() {
        let summary = serde_json::json!({
            "membershipType": "ultra",
            "individualUsage": {
                "plan": {
                    "used": 4200,
                    "limit": 20000,
                    "autoPercentUsed": 12.4,
                    "apiPercentUsed": 48.0,
                    "totalPercentUsed": 30.2
                },
                "onDemand": { "used": 150, "limit": 1000 }
            }
        });
        let sand = serde_json::json!({
            "hasNonZeroIncludedLimit": true,
            "usagePercent": 8.25,
            "currentPeriodStart": "2026-08-01T00:00:00.000Z",
            "nextResetTimestampUtc": "2026-08-31T00:00:00.000Z"
        });
        let parsed = parse_account_usage(&auth("user_1", "tok"), &summary, None, Some(&sand));
        assert_eq!(parsed.email.as_deref(), Some("dev@example.com"));
        assert_eq!(parsed.membership.as_deref(), Some("ultra"));
        assert_eq!(parsed.auto_percent, Some(12.4));
        assert_eq!(parsed.api_percent, Some(48.0));
        assert_eq!(parsed.total_percent, Some(30.2));
        assert_eq!(parsed.plan_used_usd, Some(42.0));
        assert_eq!(parsed.plan_limit_usd, Some(200.0));
        assert_eq!(parsed.on_demand_used_usd, Some(1.5));
        assert_eq!(parsed.grok_bot_percent, Some(8.25));
        assert_eq!(
            parsed.grok_bot_period_start,
            Some("2026-08-01T00:00:00.000Z".into())
        );
        assert_eq!(
            parsed.grok_bot_reset.as_deref(),
            Some("2026-08-31T00:00:00.000Z")
        );
        let line = parsed.header_line();
        assert!(line.contains("ultra"), "{line}");
        assert!(line.contains("auto"), "{line}");
        assert!(line.contains("api"), "{line}");
        assert!(line.contains("bot"), "{line}");
    }

    #[test]
    fn sand_usage_percent_is_kept_without_included_limit_flag() {
        let summary = serde_json::json!({"individualUsage":{"plan":{"autoPercentUsed":1.0}}});
        let sand = serde_json::json!({
            "hasNonZeroIncludedLimit": false,
            "usagePercent": 99.0
        });
        let parsed = parse_account_usage(&auth("user_1", "tok"), &summary, None, Some(&sand));
        assert_eq!(parsed.grok_bot_percent, Some(99.0));
        assert!(parsed.header_line().contains("bot"));
    }

    #[test]
    fn cents_ratio_fills_total_when_percents_missing() {
        let summary = serde_json::json!({
            "individualUsage": { "overall": { "used": 25, "limit": 100 } }
        });
        let parsed = parse_account_usage(&auth("user_1", "tok"), &summary, None, None);
        assert_eq!(parsed.total_percent, Some(25.0));
        assert_eq!(parsed.plan_used_usd, Some(0.25));
        assert_eq!(parsed.plan_limit_usd, Some(1.0));
    }

    #[test]
    fn usage_numbers_may_be_encoded_as_strings() {
        let summary = serde_json::json!({
            "individualUsage": {
                "plan": {
                    "autoPercentUsed": "12.5",
                    "apiPercentUsed": "25",
                    "used": "1250",
                    "limit": "10000"
                },
                "onDemand": { "used": "50", "limit": "500" }
            }
        });
        let sand = serde_json::json!({
            "hasNonZeroIncludedLimit": true,
            "usagePercent": "6.25"
        });
        let parsed = parse_account_usage(&auth("user_1", "tok"), &summary, None, Some(&sand));
        assert_eq!(parsed.auto_percent, Some(12.5));
        assert_eq!(parsed.api_percent, Some(25.0));
        assert_eq!(parsed.plan_used_usd, Some(12.5));
        assert_eq!(parsed.plan_limit_usd, Some(100.0));
        assert_eq!(parsed.on_demand_used_usd, Some(0.5));
        assert_eq!(parsed.grok_bot_percent, Some(6.25));
    }

    #[test]
    fn parse_usage_events_maps_dashboard_costs_and_labels() {
        let summary = serde_json::json!({});
        let aggregated = serde_json::json!({"totalCostCents": "275"});
        let filtered = serde_json::json!({
            "totalUsageEventsCount": "3",
            "usageEventsDisplay": [
                {
                    "timestamp": "2026-08-25T12:00:00Z",
                    "model": "claude-fable-5",
                    "chargedCents": "125",
                    "kind": "USAGE_EVENT_KIND_INCLUDED"
                },
                {"model": "gpt-5.5", "chargedCents": 150, "kind": "API"}
            ]
        });
        let parsed = parse_account_usage_with_events(
            &auth("user_1", "tok"),
            &summary,
            None,
            None,
            Some(&aggregated),
            Some(&filtered),
        );
        assert_eq!(parsed.total_cost_usd, Some(2.75));
        assert_eq!(parsed.usage_event_count, Some(3));
        assert_eq!(parsed.usage_events.len(), 2);
        assert_eq!(parsed.usage_events[0].charged_usd, Some(1.25));
        assert_eq!(parsed.usage_events[0].kind.as_deref(), Some("INCLUDED"));
    }

    #[test]
    fn dashboard_numeric_timestamps_are_normalized() {
        let summary = serde_json::json!({});
        let sand = serde_json::json!({
            "usagePercent": "4.5",
            "currentPeriodStart": 1_754_006_400_000_i64,
            "nextResetTimestampUtc": 1_756_684_800_000_i64
        });
        let filtered = serde_json::json!({
            "usageEventsDisplay": [{
                "timestamp": 1_754_066_400_000_i64,
                "model": "gpt-5.5"
            }]
        });
        let parsed = parse_account_usage_with_events(
            &auth("user_1", "tok"),
            &summary,
            None,
            Some(&sand),
            None,
            Some(&filtered),
        );
        assert_eq!(parsed.grok_bot_percent, Some(4.5));
        assert!(
            parsed
                .grok_bot_period_start
                .as_deref()
                .is_some_and(|value| value.contains("2025-08"))
        );
        assert!(
            parsed
                .grok_bot_reset
                .as_deref()
                .is_some_and(|value| value.contains("2025-09"))
        );
        assert!(
            parsed.usage_events[0]
                .timestamp
                .as_deref()
                .is_some_and(|value| value.contains("2025-08"))
        );
    }
}
