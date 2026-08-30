//! Official Cursor dashboard usage (read-only).
//!
//! These endpoints are the same ones Cursor's website and tools like CodexBar
//! use to render Auto / API / Grok Bot bars. They do **not** change
//! `x-cursor-client-type`; Sand request routing is handled independently by the
//! Cursor provider and never by the dashboard poller.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::monitor::{AccountUsageEvent, AccountUsageSnapshot, AccountUsageState};
use crate::providers::cursor::auth::{
    CursorAccountProfile, CursorAuth, load_cursor_auth, load_cursor_desktop_auth,
};

const DASHBOARD_ORIGIN: &str = "https://cursor.com";
const USAGE_SUMMARY_PATH: &str = "/api/usage-summary";
const AUTH_ME_PATH: &str = "/api/auth/me";
const AGGREGATED_USAGE_PATH: &str = "/api/dashboard/get-aggregated-usage-events";
const FILTERED_USAGE_PATH: &str = "/api/dashboard/get-filtered-usage-events";
const SAND_USAGE_PATH: &str = "/api/dashboard/get-sand-usage-status";
const FETCH_TIMEOUT: Duration = Duration::from_secs(8);
const SAND_TIMEOUT: Duration = Duration::from_secs(5);
const EVENTS_TIMEOUT: Duration = Duration::from_secs(5);
const USAGE_POLL_INTERVAL: Duration = Duration::from_secs(60);
/// The dashboard poller runs once per minute. Keep two missed polls worth of
/// evidence, then stop using it for request classification: a stale 100% meter
/// must not make a newly reset Sand period look exhausted.
const SAND_USAGE_EVIDENCE_TTL: Duration = Duration::from_secs(180);
const SAND_USAGE_EVIDENCE_MAX_ACCOUNTS: usize = 64;
const ACCOUNT_USAGE_CACHE_VERSION: u64 = 1;
const ACCOUNT_USAGE_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024;
const ACCOUNT_USAGE_CACHE_MAX_ACCOUNTS: usize = 64;
const ACCOUNT_USAGE_CACHE_MAX_EVENTS: usize = 64;
const ACCOUNT_USAGE_CACHE_MAX_STRING_CHARS: usize = 512;
const ACCOUNT_USAGE_CACHE_MAX_ID_CHARS: usize = 512;

static ACCOUNT_USAGE_CACHE_IO: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct CachedAccountUsageEvent {
    timestamp: Option<String>,
    model: Option<String>,
    charged_usd: Option<f64>,
    kind: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct CachedAccountUsageSnapshot {
    email: Option<String>,
    membership: Option<String>,
    auto_percent: Option<f64>,
    api_percent: Option<f64>,
    total_percent: Option<f64>,
    plan_used_usd: Option<f64>,
    plan_limit_usd: Option<f64>,
    on_demand_used_usd: Option<f64>,
    on_demand_limit_usd: Option<f64>,
    grok_bot_percent: Option<f64>,
    grok_bot_period_start: Option<String>,
    grok_bot_reset: Option<String>,
    total_cost_usd: Option<f64>,
    usage_event_count: Option<u64>,
    usage_events: Vec<CachedAccountUsageEvent>,
    fetched_at_ms: Option<u64>,
}

impl CachedAccountUsageSnapshot {
    fn from_snapshot(snapshot: &AccountUsageSnapshot) -> Self {
        Self {
            email: bounded_cache_string(snapshot.email.as_deref()),
            membership: bounded_cache_string(snapshot.membership.as_deref()),
            auto_percent: finite(snapshot.auto_percent),
            api_percent: finite(snapshot.api_percent),
            total_percent: finite(snapshot.total_percent),
            plan_used_usd: finite(snapshot.plan_used_usd),
            plan_limit_usd: finite(snapshot.plan_limit_usd),
            on_demand_used_usd: finite(snapshot.on_demand_used_usd),
            on_demand_limit_usd: finite(snapshot.on_demand_limit_usd),
            grok_bot_percent: finite(snapshot.grok_bot_percent),
            grok_bot_period_start: bounded_cache_string(snapshot.grok_bot_period_start.as_deref()),
            grok_bot_reset: bounded_cache_string(snapshot.grok_bot_reset.as_deref()),
            total_cost_usd: finite(snapshot.total_cost_usd),
            usage_event_count: snapshot.usage_event_count,
            usage_events: snapshot
                .usage_events
                .iter()
                .take(ACCOUNT_USAGE_CACHE_MAX_EVENTS)
                .map(|event| CachedAccountUsageEvent {
                    timestamp: bounded_cache_string(event.timestamp.as_deref()),
                    model: bounded_cache_string(event.model.as_deref()),
                    charged_usd: finite(event.charged_usd),
                    kind: bounded_cache_string(event.kind.as_deref()),
                })
                .collect(),
            fetched_at_ms: Some(
                snapshot
                    .fetched_at
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            ),
        }
    }

    fn into_snapshot(self) -> Option<AccountUsageSnapshot> {
        let fetched_at = UNIX_EPOCH.checked_add(Duration::from_millis(self.fetched_at_ms?))?;
        Some(AccountUsageSnapshot {
            email: self.email,
            membership: self.membership,
            auto_percent: finite(self.auto_percent),
            api_percent: finite(self.api_percent),
            total_percent: finite(self.total_percent),
            plan_used_usd: finite(self.plan_used_usd),
            plan_limit_usd: finite(self.plan_limit_usd),
            on_demand_used_usd: finite(self.on_demand_used_usd),
            on_demand_limit_usd: finite(self.on_demand_limit_usd),
            grok_bot_percent: finite(self.grok_bot_percent),
            grok_bot_period_start: self.grok_bot_period_start,
            grok_bot_reset: self.grok_bot_reset,
            total_cost_usd: finite(self.total_cost_usd),
            usage_event_count: self.usage_event_count,
            usage_events: self
                .usage_events
                .into_iter()
                .take(ACCOUNT_USAGE_CACHE_MAX_EVENTS)
                .map(|event| AccountUsageEvent {
                    timestamp: event.timestamp,
                    model: event.model,
                    charged_usd: finite(event.charged_usd),
                    kind: event.kind,
                })
                .collect(),
            fetched_at,
        })
    }
}

fn finite(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite())
}

fn bounded_cache_string(value: Option<&str>) -> Option<String> {
    value.map(|value| {
        let value = value.trim();
        if value.chars().count() <= ACCOUNT_USAGE_CACHE_MAX_STRING_CHARS {
            value.to_string()
        } else {
            value
                .chars()
                .take(ACCOUNT_USAGE_CACHE_MAX_STRING_CHARS)
                .collect()
        }
    })
}

fn valid_cache_account_id(account_id: &str) -> bool {
    !account_id.trim().is_empty() && account_id.chars().count() <= ACCOUNT_USAGE_CACHE_MAX_ID_CHARS
}

/// Load the last successful dashboard snapshot for each account. Cache
/// corruption is treated as a miss; credentials and routing do not depend on
/// this file.
pub fn load_account_usage_cache() -> HashMap<String, AccountUsageSnapshot> {
    let deps = crate::paths::DirResolverEnv::default();
    load_account_usage_cache_from(
        &crate::paths::cursor_usage_cache_file(&deps),
        &crate::paths::cursor_usage_cache_lock_file(&deps),
    )
    .unwrap_or_default()
}

/// Persist one successful account snapshot without retaining any credential
/// material. Updates are serialized across threads and processes so workers
/// finishing out of order cannot replace another account's entry.
pub fn persist_account_usage(
    account_id: &str,
    snapshot: &AccountUsageSnapshot,
) -> anyhow::Result<()> {
    let deps = crate::paths::DirResolverEnv::default();
    persist_account_usage_to(
        &crate::paths::cursor_usage_cache_file(&deps),
        &crate::paths::cursor_usage_cache_lock_file(&deps),
        account_id,
        snapshot,
    )
}

/// Persist usage for a profile after any credential refresh. Registry-backed
/// profiles keep their stable configured id. A legacy single-account profile
/// is synthetic and may be keyed by an opaque bearer digest, so migrate its
/// cache row when refreshing the bearer changes that digest.
pub fn persist_account_usage_for_profile(
    profile: &CursorAccountProfile,
    refreshed_auth: &CursorAuth,
    snapshot: &AccountUsageSnapshot,
) -> anyhow::Result<()> {
    let registry_backed =
        profile.auth.source == crate::providers::cursor::auth::cursor_accounts_location();
    if registry_backed {
        let Some(result) =
            crate::providers::cursor::auth::with_cursor_account_present(&profile.id, || {
                persist_account_usage(&profile.id, snapshot)
            })
        else {
            // The account was deleted while this worker was fetching. Do not
            // resurrect its cache row with a late successful response.
            return Ok(());
        };
        result?;
        return Ok(());
    }
    persist_account_usage(&profile.id, snapshot)?;
    let refreshed_id =
        crate::providers::cursor::auth::cursor_account_id_for_token(&refreshed_auth.access_token);
    if refreshed_id == profile.id {
        return Ok(());
    }
    persist_account_usage(&refreshed_id, snapshot)?;
    remove_account_usage(&profile.id)
}

/// Remove a deleted account's durable usage snapshot. Credentials are stored
/// separately, so cache cleanup is best effort and never affects account
/// removal itself.
pub fn remove_account_usage(account_id: &str) -> anyhow::Result<()> {
    let deps = crate::paths::DirResolverEnv::default();
    remove_account_usage_from(
        &crate::paths::cursor_usage_cache_file(&deps),
        &crate::paths::cursor_usage_cache_lock_file(&deps),
        account_id,
    )
}

fn load_account_usage_cache_from(
    cache_path: &Path,
    lock_path: &Path,
) -> anyhow::Result<HashMap<String, AccountUsageSnapshot>> {
    with_account_usage_cache_lock(lock_path, || read_account_usage_cache(cache_path))
}

fn persist_account_usage_to(
    cache_path: &Path,
    lock_path: &Path,
    account_id: &str,
    snapshot: &AccountUsageSnapshot,
) -> anyhow::Result<()> {
    if !valid_cache_account_id(account_id) {
        anyhow::bail!("invalid Cursor account id for usage cache");
    }
    with_account_usage_cache_lock(lock_path, || {
        let mut cache = read_cached_usage_entries(cache_path)?;
        let next = CachedAccountUsageSnapshot::from_snapshot(snapshot);
        let next_fetched_at = next.fetched_at_ms.unwrap_or_default();
        let should_replace = cache
            .get(account_id)
            .is_none_or(|previous| previous.fetched_at_ms.unwrap_or_default() <= next_fetched_at);
        if should_replace {
            cache.insert(account_id.to_string(), next);
        }
        write_account_usage_cache(cache_path, account_id, cache)
    })
}

fn remove_account_usage_from(
    cache_path: &Path,
    lock_path: &Path,
    account_id: &str,
) -> anyhow::Result<()> {
    if !valid_cache_account_id(account_id) {
        anyhow::bail!("invalid Cursor account id for usage cache");
    }
    with_account_usage_cache_lock(lock_path, || {
        let mut cache = read_cached_usage_entries(cache_path)?;
        if cache.remove(account_id).is_some() {
            write_account_usage_cache(cache_path, account_id, cache)?;
        }
        Ok(())
    })
}

fn with_account_usage_cache_lock<T>(
    lock_path: &Path,
    operation: impl FnOnce() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let _process_guard = ACCOUNT_USAGE_CACHE_IO
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let directory = lock_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("invalid Cursor usage cache lock path"))?;
    fs::create_dir_all(directory)?;
    #[cfg(unix)]
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    #[cfg(unix)]
    let lock_file = {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(lock_path)?;
        let _ = fs::set_permissions(lock_path, fs::Permissions::from_mode(0o600));
        file
    };
    #[cfg(not(unix))]
    let lock_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)?;
    fs2::FileExt::lock_exclusive(&lock_file)?;
    let result = operation();
    let unlock_result = fs2::FileExt::unlock(&lock_file);
    match (result, unlock_result) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

fn read_account_usage_cache(
    cache_path: &Path,
) -> anyhow::Result<HashMap<String, AccountUsageSnapshot>> {
    let entries = read_cached_usage_entries(cache_path)?;
    Ok(entries
        .into_iter()
        .filter_map(|(id, snapshot)| snapshot.into_snapshot().map(|snapshot| (id, snapshot)))
        .collect())
}

fn read_cached_usage_entries(
    cache_path: &Path,
) -> anyhow::Result<HashMap<String, CachedAccountUsageSnapshot>> {
    let metadata = match fs::metadata(cache_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() > ACCOUNT_USAGE_CACHE_MAX_BYTES {
        return Ok(HashMap::new());
    }
    let raw: Value = match serde_json::from_slice(&fs::read(cache_path)?) {
        Ok(raw) => raw,
        Err(_) => return Ok(HashMap::new()),
    };
    if raw.get("version").and_then(Value::as_u64) != Some(ACCOUNT_USAGE_CACHE_VERSION) {
        return Ok(HashMap::new());
    }
    let Some(accounts) = raw.get("accounts").and_then(Value::as_object) else {
        return Ok(HashMap::new());
    };
    let mut entries = accounts
        .iter()
        .filter(|(id, _)| valid_cache_account_id(id))
        .filter_map(|(id, value)| {
            serde_json::from_value::<CachedAccountUsageSnapshot>(value.clone())
                .ok()
                .filter(|snapshot| snapshot.fetched_at_ms.is_some_and(|value| value > 0))
                .map(|snapshot| (id.clone(), snapshot))
        })
        .collect::<Vec<_>>();
    entries
        .sort_by_key(|(_, snapshot)| std::cmp::Reverse(snapshot.fetched_at_ms.unwrap_or_default()));
    entries.truncate(ACCOUNT_USAGE_CACHE_MAX_ACCOUNTS);
    Ok(entries.into_iter().collect())
}

fn write_account_usage_cache(
    cache_path: &Path,
    current_account_id: &str,
    mut accounts: HashMap<String, CachedAccountUsageSnapshot>,
) -> anyhow::Result<()> {
    while accounts.len() > ACCOUNT_USAGE_CACHE_MAX_ACCOUNTS {
        let oldest = accounts
            .iter()
            .filter(|(id, _)| id.as_str() != current_account_id)
            .min_by_key(|(_, snapshot)| snapshot.fetched_at_ms.unwrap_or_default())
            .map(|(id, _)| id.clone())
            .or_else(|| accounts.keys().next().cloned());
        let Some(oldest) = oldest else {
            break;
        };
        accounts.remove(&oldest);
    }

    loop {
        let ordered = accounts
            .iter()
            .map(|(id, snapshot)| (id.clone(), snapshot.clone()))
            .collect::<BTreeMap<_, _>>();
        let document = serde_json::json!({
            "version": ACCOUNT_USAGE_CACHE_VERSION,
            "accounts": ordered,
        });
        if serde_json::to_vec_pretty(&document)?.len() as u64 <= ACCOUNT_USAGE_CACHE_MAX_BYTES {
            crate::auth::write_atomically(&cache_path.to_string_lossy(), &document)?;
            #[cfg(unix)]
            if let Some(directory) = cache_path.parent() {
                fs::File::open(directory)?.sync_all()?;
            }
            return Ok(());
        }
        let oldest = accounts
            .iter()
            .filter(|(id, _)| id.as_str() != current_account_id)
            .min_by_key(|(_, snapshot)| snapshot.fetched_at_ms.unwrap_or_default())
            .map(|(id, _)| id.clone());
        let Some(oldest) = oldest else {
            anyhow::bail!("Cursor account usage snapshot exceeds cache size limit");
        };
        accounts.remove(&oldest);
    }
}

/// Account-scoped dashboard evidence used only to disambiguate Cursor's
/// otherwise-successful, payload-less Sand `FLAG_END`. Some exhausted Sand
/// accounts return that frame instead of a typed 429. The bearer itself is
/// never retained; the map is keyed by a SHA-256 digest.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SandUsageEvidence {
    pub usage_percent: f64,
    pub has_available_usage: Option<bool>,
    pub next_reset: Option<String>,
    observed_at: Instant,
}

impl SandUsageEvidence {
    pub(crate) fn retry_after_secs(&self) -> Option<u64> {
        retry_after_secs_for_reset(self.next_reset.as_deref())
    }
}

static SAND_USAGE_EVIDENCE: LazyLock<Mutex<HashMap<String, SandUsageEvidence>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Account-scoped evidence for Cursor's named-model/API allowance. Cursor can
/// acknowledge an exhausted CLI Run with HTTP 200 and an empty `FLAG_END`, so
/// the live transport needs a recent API meter to distinguish that policy
/// response from a transient hollow turn. Keep this cache separate from the
/// Sand meter because the two allowances have independent reset windows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ApiUsageEvidence {
    pub usage_percent: f64,
    pub next_reset: Option<String>,
    observed_at: Instant,
}

impl ApiUsageEvidence {
    pub(crate) fn retry_after_secs(&self) -> Option<u64> {
        retry_after_secs_for_reset(self.next_reset.as_deref())
    }
}

fn retry_after_secs_for_reset(reset: Option<&str>) -> Option<u64> {
    let reset = reset?;
    let reset =
        time::OffsetDateTime::parse(reset, &time::format_description::well_known::Rfc3339).ok()?;
    Some(
        (reset - time::OffsetDateTime::now_utc())
            .whole_seconds()
            .max(1) as u64,
    )
}

static API_USAGE_EVIDENCE: LazyLock<Mutex<HashMap<String, ApiUsageEvidence>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn sand_usage_account_key(token: &str) -> String {
    // Keep the usage cache partitioning identical to the policy breaker:
    // refreshed JWTs for one Cursor account must retain the same dashboard
    // evidence, while opaque environment tokens still fall back to a digest
    // of the token itself. The raw bearer is never stored.
    super::auth::cursor_account_digest(token)
}

fn store_sand_usage_evidence(auth: &CursorAuth, sand: Option<&Value>) {
    let account_key = sand_usage_account_key(&auth.access_token);
    let mut cache = SAND_USAGE_EVIDENCE
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let now = Instant::now();
    cache.retain(|_, evidence| {
        now.saturating_duration_since(evidence.observed_at) < SAND_USAGE_EVIDENCE_TTL
    });
    let Some(sand) = sand else {
        // A transient dashboard failure does not erase a recent successful
        // poll. Its evidence naturally expires after the short TTL above.
        return;
    };
    let Some(usage_percent) = json_f64(sand.get("usagePercent")) else {
        cache.remove(&account_key);
        return;
    };
    if cache.len() >= SAND_USAGE_EVIDENCE_MAX_ACCOUNTS && !cache.contains_key(&account_key) {
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, evidence)| evidence.observed_at)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(
        account_key,
        SandUsageEvidence {
            usage_percent,
            has_available_usage: sand.get("hasAvailableUsage").and_then(Value::as_bool),
            next_reset: dashboard_timestamp(sand.get("nextResetTimestampUtc")),
            observed_at: now,
        },
    );
}

fn store_api_usage_evidence(auth: &CursorAuth, summary: Option<&Value>) {
    let account_key = sand_usage_account_key(&auth.access_token);
    let mut cache = API_USAGE_EVIDENCE
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let now = Instant::now();
    cache.retain(|_, evidence| {
        now.saturating_duration_since(evidence.observed_at) < SAND_USAGE_EVIDENCE_TTL
    });
    let Some(summary) = summary else {
        // Keep recent evidence across a single dashboard timeout. It expires
        // naturally rather than turning an outage into a false quota signal.
        return;
    };
    let Some(api_percent) = json_f64(summary.pointer("/individualUsage/plan/apiPercentUsed"))
    else {
        // A successful dashboard response without an API meter means this
        // account/deployment does not expose the classifier signal.
        cache.remove(&account_key);
        return;
    };
    if cache.len() >= SAND_USAGE_EVIDENCE_MAX_ACCOUNTS && !cache.contains_key(&account_key) {
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, evidence)| evidence.observed_at)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
    }
    let next_reset = summary
        .get("billingCycleEnd")
        .or_else(|| summary.get("billing_cycle_end"))
        .and_then(|value| dashboard_timestamp(Some(value)));
    cache.insert(
        account_key,
        ApiUsageEvidence {
            usage_percent: api_percent,
            next_reset,
            observed_at: now,
        },
    );
}

/// Return recent Sand usage evidence only for the exact credential that was
/// used to open the Run. This prevents a hot account switch from inheriting a
/// previous login's exhausted meter.
pub(crate) fn cached_sand_usage_evidence(token: &str) -> Option<SandUsageEvidence> {
    let account_key = sand_usage_account_key(token);
    let mut cache = SAND_USAGE_EVIDENCE
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let now = Instant::now();
    cache.retain(|_, evidence| {
        now.saturating_duration_since(evidence.observed_at) < SAND_USAGE_EVIDENCE_TTL
    });
    cache.get(&account_key).cloned()
}

/// Return recent API allowance evidence for the exact credential used by a
/// live Run. The cache key is the stable account digest, never the bearer.
pub(crate) fn cached_api_usage_evidence(token: &str) -> Option<ApiUsageEvidence> {
    let account_key = sand_usage_account_key(token);
    let mut cache = API_USAGE_EVIDENCE
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let now = Instant::now();
    cache.retain(|_, evidence| {
        now.saturating_duration_since(evidence.observed_at) < SAND_USAGE_EVIDENCE_TTL
    });
    cache.get(&account_key).cloned()
}

#[cfg(test)]
pub(crate) fn store_sand_usage_evidence_for_test(
    token: &str,
    usage_percent: f64,
    has_available_usage: Option<bool>,
    next_reset: Option<&str>,
) {
    let auth = CursorAuth {
        access_token: token.to_string(),
        refresh_token: None,
        api_key: None,
        expires: None,
        user_id: None,
        email: None,
        source: "test".into(),
    };
    let sand = serde_json::json!({
        "usagePercent": usage_percent,
        "hasAvailableUsage": has_available_usage,
        "nextResetTimestampUtc": next_reset,
    });
    store_sand_usage_evidence(&auth, Some(&sand));
}

#[cfg(test)]
pub(crate) fn store_api_usage_evidence_for_test(
    token: &str,
    usage_percent: f64,
    next_reset: Option<&str>,
) {
    let auth = CursorAuth {
        access_token: token.to_string(),
        refresh_token: None,
        api_key: None,
        expires: None,
        user_id: None,
        email: None,
        source: "test".into(),
    };
    let summary = serde_json::json!({
        "individualUsage": {
            "plan": { "apiPercentUsed": usage_percent }
        },
        "billingCycleEnd": next_reset,
    });
    store_api_usage_evidence(&auth, Some(&summary));
}

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
            Ok(snapshot) => {
                persist_snapshot_for_auth(&auth, &snapshot);
                AccountUsageState::Ready(snapshot)
            }
            Err(err) => AccountUsageState::Failed(truncate_error(&err.to_string())),
        },
        None => AccountUsageState::MissingAuth,
    }
}

fn persist_snapshot_for_auth(auth: &CursorAuth, snapshot: &AccountUsageSnapshot) {
    // The monitor poller receives only a credential, while the durable cache
    // is keyed by the stable account id. Resolve that id from the registry and
    // compare token digests so a desktop fallback cannot write another row's
    // snapshot. Failure to discover the id is non-fatal for monitoring.
    let Ok(accounts) = crate::providers::cursor::auth::list_cursor_accounts() else {
        return;
    };
    let auth_digest = super::auth::cursor_account_digest(&auth.access_token);
    if let Some(profile) = accounts.into_iter().find(|profile| {
        super::auth::cursor_account_digest(&profile.auth.access_token) == auth_digest
    }) {
        let _ = persist_account_usage_for_profile(&profile, auth, snapshot);
    }
}

pub async fn poll_cursor_account_usage(monitor: crate::monitor::MonitorHandle) {
    loop {
        let state = match tokio::task::spawn_blocking(fetch_account_usage_state).await {
            Ok(state) => state,
            Err(_) => AccountUsageState::Failed("usage poller cancelled".into()),
        };
        monitor.set_account_usage(state);
        tokio::time::sleep(USAGE_POLL_INTERVAL).await;
    }
}

/// Keep the account-scoped Sand meter warm when the proxy runs without the
/// monitor TUI. Cursor can report an exhausted Sand Run as HTTP 200 followed by
/// an empty `FLAG_END`; recent dashboard evidence lets the live transport map
/// that wire shape to a useful 429 instead of a misleading stale-conversation
/// error. This intentionally calls only the Sand endpoint once per poll rather
/// than duplicating the monitor's full dashboard sweep.
pub async fn poll_cursor_sand_usage_evidence() {
    loop {
        let _ = tokio::task::spawn_blocking(refresh_sand_usage_evidence).await;
        tokio::time::sleep(USAGE_POLL_INTERVAL).await;
    }
}

fn refresh_sand_usage_evidence() -> anyhow::Result<()> {
    let auth = match load_cursor_auth() {
        Ok(Some(auth)) => Some(auth),
        Ok(None) | Err(_) => load_cursor_desktop_auth().ok().flatten(),
    };
    let Some(auth) = auth else {
        return Ok(());
    };
    let cookie = workos_session_cookie(&auth);
    let client = reqwest::blocking::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .build()?;
    let sand_result = fetch_sand_usage_evidence_with(&auth, DASHBOARD_ORIGIN, &client, &cookie);
    let summary_result = dashboard_get(
        &client,
        DASHBOARD_ORIGIN,
        USAGE_SUMMARY_PATH,
        &cookie,
        FETCH_TIMEOUT,
    );
    if let Ok(summary) = summary_result.as_ref() {
        store_api_usage_evidence(&auth, Some(summary));
    }
    match (sand_result, summary_result) {
        (Ok(()), _) | (_, Ok(_)) => Ok(()),
        (Err(sand), Err(summary)) => Err(anyhow::anyhow!(
            "cursor usage evidence: sand: {sand}; api: {summary}"
        )),
    }
}

fn fetch_sand_usage_evidence_with(
    auth: &CursorAuth,
    origin: &str,
    client: &reqwest::blocking::Client,
    cookie: &str,
) -> anyhow::Result<()> {
    let sand = dashboard_post(client, origin, SAND_USAGE_PATH, cookie, "{}", SAND_TIMEOUT)?;
    store_sand_usage_evidence(auth, Some(&sand));
    Ok(())
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
    // Fetch and publish the classifier evidence first. The remaining dashboard
    // endpoints are independent and may each spend their timeout budget; Sand
    // requests arriving while the TUI snapshot is still loading should not
    // miss an otherwise available exhausted-meter signal.
    let sand = dashboard_post(client, origin, SAND_USAGE_PATH, cookie, "{}", SAND_TIMEOUT).ok();
    store_sand_usage_evidence(auth, sand.as_ref());
    // `usage-summary` is the richest response, but it has not been enabled
    // for every account/dashboard deployment. Keep the independent identity
    // and Sand meters useful when that endpoint is absent.
    let summary_result = dashboard_get(client, origin, USAGE_SUMMARY_PATH, cookie, FETCH_TIMEOUT);
    if let Ok(summary) = summary_result.as_ref() {
        store_api_usage_evidence(auth, Some(summary));
    }
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
    use std::io::{Read, Write};
    use std::net::TcpListener;

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
    fn sand_usage_evidence_is_scoped_to_the_exact_account_token() {
        let token_a = format!("usage-evidence-a-{}", uuid::Uuid::new_v4());
        let token_b = format!("usage-evidence-b-{}", uuid::Uuid::new_v4());
        store_sand_usage_evidence_for_test(
            &token_a,
            100.0,
            Some(true),
            Some("2099-09-02T20:12:42Z"),
        );

        let evidence = cached_sand_usage_evidence(&token_a).expect("account A evidence");
        assert_eq!(evidence.usage_percent, 100.0);
        // Cursor currently reports this true even when an exhausted Sand Run
        // immediately returns an empty END. Classification therefore combines
        // the meter with observed wire behavior instead of trusting this flag.
        assert_eq!(evidence.has_available_usage, Some(true));
        assert!(evidence.retry_after_secs().is_some_and(|secs| secs > 0));
        assert!(cached_sand_usage_evidence(&token_b).is_none());
    }

    #[test]
    fn api_usage_evidence_is_scoped_and_reads_plan_meter() {
        let token_a = format!("api-usage-evidence-a-{}", uuid::Uuid::new_v4());
        let token_b = format!("api-usage-evidence-b-{}", uuid::Uuid::new_v4());
        store_api_usage_evidence_for_test(&token_a, 100.0, Some("2099-09-02T20:12:42Z"));

        let evidence = cached_api_usage_evidence(&token_a).expect("account A API evidence");
        assert_eq!(evidence.usage_percent, 100.0);
        assert!(evidence.retry_after_secs().is_some_and(|secs| secs > 0));
        assert!(cached_api_usage_evidence(&token_b).is_none());
    }

    #[test]
    fn api_usage_evidence_without_meter_retires_old_value() {
        let token = format!("api-usage-evidence-retire-{}", uuid::Uuid::new_v4());
        store_api_usage_evidence_for_test(&token, 100.0, None);
        assert!(cached_api_usage_evidence(&token).is_some());
        let auth = auth("user", &token);
        store_api_usage_evidence(
            &auth,
            Some(&serde_json::json!({
                "individualUsage": { "plan": {} }
            })),
        );
        assert!(cached_api_usage_evidence(&token).is_none());
    }

    #[test]
    fn successful_sand_meter_without_percent_retires_old_evidence() {
        let token = format!("usage-evidence-retire-{}", uuid::Uuid::new_v4());
        store_sand_usage_evidence_for_test(&token, 100.0, Some(false), None);
        assert!(cached_sand_usage_evidence(&token).is_some());
        store_sand_usage_evidence(
            &auth("user", &token),
            Some(&serde_json::json!({"hasAvailableUsage": true})),
        );
        assert!(cached_sand_usage_evidence(&token).is_none());
    }

    #[test]
    fn sand_only_refresh_hits_one_dashboard_endpoint_and_populates_evidence() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2048];
            loop {
                let read = stream.read(&mut chunk).unwrap();
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&chunk[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n")
                    && request.ends_with(b"{}")
                {
                    break;
                }
            }
            let body = r#"{"usagePercent":100,"hasAvailableUsage":false}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            String::from_utf8(request).unwrap()
        });

        let token = format!("sand-only-refresh-{}", uuid::Uuid::new_v4());
        let auth = auth("user", &token);
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .unwrap();
        fetch_sand_usage_evidence_with(&auth, &origin, &client, "cookie=value").unwrap();

        let request = server.join().unwrap();
        assert!(
            request.starts_with("POST /api/dashboard/get-sand-usage-status HTTP/1.1\r\n"),
            "{request}"
        );
        let evidence = cached_sand_usage_evidence(&token).expect("fresh Sand evidence");
        assert_eq!(evidence.usage_percent, 100.0);
        assert_eq!(evidence.has_available_usage, Some(false));
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

    fn cache_snapshot(seed: u64) -> AccountUsageSnapshot {
        AccountUsageSnapshot {
            email: Some(format!("account-{seed}@example.com")),
            membership: Some("ultra".into()),
            auto_percent: Some(seed as f64),
            api_percent: Some(2.0),
            total_percent: Some(3.0),
            plan_used_usd: Some(4.0),
            plan_limit_usd: Some(5.0),
            on_demand_used_usd: Some(6.0),
            on_demand_limit_usd: Some(7.0),
            grok_bot_percent: Some(8.0),
            grok_bot_period_start: Some("2026-08-01T00:00:00Z".into()),
            grok_bot_reset: Some("2026-09-01T00:00:00Z".into()),
            total_cost_usd: Some(9.0),
            usage_event_count: Some(seed),
            usage_events: vec![AccountUsageEvent {
                timestamp: Some("2026-08-30T00:00:00Z".into()),
                model: Some("claude-fable-5".into()),
                charged_usd: Some(0.1),
                kind: Some("INCLUDED".into()),
            }],
            fetched_at: UNIX_EPOCH + Duration::from_millis(seed),
        }
    }

    #[test]
    fn account_usage_cache_round_trips_without_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cursor").join("account-usage.json");
        let lock = dir.path().join("cursor").join("account-usage.lock");
        let snapshot = cache_snapshot(1234);
        persist_account_usage_to(&cache, &lock, "account-a", &snapshot).unwrap();

        let loaded = load_account_usage_cache_from(&cache, &lock).unwrap();
        assert_eq!(loaded.get("account-a"), Some(&snapshot));
        let raw = fs::read_to_string(cache).unwrap();
        assert!(!raw.contains("access_token"));
        assert!(!raw.contains("refresh_token"));
    }

    #[test]
    fn account_usage_cache_does_not_replace_newer_snapshot_with_older_result() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cursor").join("account-usage.json");
        let lock = dir.path().join("cursor").join("account-usage.lock");
        persist_account_usage_to(&cache, &lock, "account-a", &cache_snapshot(2000)).unwrap();
        persist_account_usage_to(&cache, &lock, "account-a", &cache_snapshot(1000)).unwrap();

        let loaded = load_account_usage_cache_from(&cache, &lock).unwrap();
        assert_eq!(
            loaded["account-a"].fetched_at,
            UNIX_EPOCH + Duration::from_millis(2000)
        );
        assert_eq!(loaded["account-a"].usage_event_count, Some(2000));
    }

    #[test]
    fn account_usage_cache_removes_deleted_account_only() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cursor").join("account-usage.json");
        let lock = dir.path().join("cursor").join("account-usage.lock");
        persist_account_usage_to(&cache, &lock, "account-a", &cache_snapshot(1)).unwrap();
        persist_account_usage_to(&cache, &lock, "account-b", &cache_snapshot(2)).unwrap();

        remove_account_usage_from(&cache, &lock, "account-a").unwrap();

        let loaded = load_account_usage_cache_from(&cache, &lock).unwrap();
        assert!(!loaded.contains_key("account-a"));
        assert!(loaded.contains_key("account-b"));
    }

    #[test]
    fn account_usage_cache_read_ignores_bad_entries_and_unknown_schema() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cursor").join("account-usage.json");
        let lock = dir.path().join("cursor").join("account-usage.lock");
        fs::create_dir_all(cache.parent().unwrap()).unwrap();
        fs::write(
            &cache,
            serde_json::to_vec(&serde_json::json!({
                "version": ACCOUNT_USAGE_CACHE_VERSION,
                "accounts": {
                    "good": serde_json::to_value(CachedAccountUsageSnapshot::from_snapshot(&cache_snapshot(7))).unwrap(),
                    "bad": {"fetchedAtMs": "not-a-number"},
                    "": serde_json::json!({})
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let loaded = load_account_usage_cache_from(&cache, &lock).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded["good"].usage_event_count, Some(7));

        fs::write(
            &cache,
            serde_json::to_vec(&serde_json::json!({"version": 99, "accounts": {}})).unwrap(),
        )
        .unwrap();
        assert!(
            load_account_usage_cache_from(&cache, &lock)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn account_usage_cache_concurrent_updates_keep_each_account() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cursor").join("account-usage.json");
        let lock = dir.path().join("cursor").join("account-usage.lock");
        let mut threads = Vec::new();
        for seed in 1..=8_u64 {
            let cache = cache.clone();
            let lock = lock.clone();
            threads.push(std::thread::spawn(move || {
                persist_account_usage_to(
                    &cache,
                    &lock,
                    &format!("account-{seed}"),
                    &cache_snapshot(seed),
                )
                .unwrap();
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let loaded = load_account_usage_cache_from(&cache, &lock).unwrap();
        assert_eq!(loaded.len(), 8);
        for seed in 1..=8_u64 {
            assert!(loaded.contains_key(&format!("account-{seed}")));
        }
    }
}
