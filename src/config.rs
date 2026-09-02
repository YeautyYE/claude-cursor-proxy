use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::paths;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AliasProvider {
    Codex,
    Kimi,
    Cursor,
}

impl AliasProvider {
    pub fn as_str(&self) -> &str {
        match self {
            AliasProvider::Codex => "codex",
            AliasProvider::Kimi => "kimi",
            AliasProvider::Cursor => "cursor",
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub bind_address: String,
    pub port: u16,
    pub alias_provider: AliasProvider,
    pub log_verbose: bool,
    pub log_stderr: bool,
    pub config_dir: PathBuf,
}

#[derive(Deserialize)]
struct FileConfig {
    #[serde(rename = "bindAddress")]
    pub bind_address: Option<String>,
    pub port: Option<u16>,
    #[serde(rename = "aliasProvider")]
    pub alias_provider: Option<String>,
    pub log: Option<FileLog>,
    pub kimi: Option<KimiConfig>,
    pub codex: Option<CodexConfig>,
    pub cursor: Option<CursorConfig>,
    pub grok: Option<GrokConfig>,
}

#[derive(Deserialize, Clone)]
struct CodexConfig {
    #[serde(rename = "baseUrl")]
    pub base_url: Option<String>,
    #[serde(rename = "originator")]
    pub originator: Option<String>,
    #[serde(rename = "userAgent")]
    pub user_agent: Option<String>,
    #[serde(rename = "previousResponseId")]
    pub previous_response_id: Option<bool>,
    #[serde(rename = "serviceTier")]
    pub service_tier: Option<String>,
    #[serde(rename = "reasoningSummary")]
    pub reasoning_summary: Option<String>,
    #[serde(rename = "effort")]
    pub effort: Option<String>,
    #[serde(rename = "model")]
    pub model: Option<String>,
    pub transport: Option<String>,
}

#[derive(Deserialize, Clone)]
struct CursorConfig {
    #[serde(rename = "baseUrl")]
    pub base_url: Option<String>,
    /// Base URL for the Desktop Sand `InferenceService/Stream` route.  Keep
    /// this separate from the ordinary AgentService URL because deployments
    /// may front the two services with different gateways.
    #[serde(rename = "sandBaseUrl")]
    pub sand_base_url: Option<String>,
    #[serde(rename = "clientVersion")]
    pub client_version: Option<String>,
    #[serde(rename = "clientType")]
    pub client_type: Option<String>,
    /// Desktop Cursor identity fields used by the patched Sand client path.
    /// They remain optional so the default CLI profile keeps its historical
    /// wire shape.  Environment variables with the `CCP_CURSOR_*` prefix take
    /// precedence over these values.
    #[serde(rename = "clientLayout")]
    pub client_layout: Option<String>,
    #[serde(rename = "clientOsVersion")]
    pub client_os_version: Option<String>,
    #[serde(rename = "canary")]
    pub canary: Option<bool>,
    #[serde(rename = "configVersion")]
    pub config_version: Option<String>,
    #[serde(rename = "sandClientVersion")]
    pub sand_client_version: Option<String>,
    #[serde(rename = "localClientMode")]
    pub local_client_mode: Option<bool>,
    #[serde(rename = "clientCommit")]
    pub client_commit: Option<String>,
    #[serde(rename = "ghostMode")]
    pub ghost_mode: Option<bool>,
    /// Value for Cursor Desktop's `x-new-onboarding-completed` common
    /// header.  The patched Sand client intentionally disables snippet
    /// eligibility, so the desktop helper normally emits `false`; expose an
    /// override for installations whose local eligibility state differs.
    #[serde(rename = "newOnboardingCompleted")]
    pub new_onboarding_completed: Option<bool>,
    /// Optional IANA timezone override used by the desktop identity helper.
    #[serde(rename = "timezone")]
    pub timezone: Option<String>,
    #[serde(rename = "agentBundle")]
    pub agent_bundle: Option<String>,
    #[serde(rename = "sandModels")]
    pub sand_models: Option<SandModelsConfig>,
    #[serde(rename = "modelAccounts")]
    pub model_accounts: Option<ModelAccountsConfig>,
}

/// The persisted form accepts either a JSON array or a comma/newline-separated
/// string. The latter is convenient for hand-edited configs and mirrors the env
/// override without making the rest of the config parser more permissive.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum SandModelsConfig {
    List(Vec<String>),
    Text(String),
}

impl SandModelsConfig {
    fn into_patterns(self) -> Vec<String> {
        match self {
            Self::List(values) => values,
            Self::Text(value) => split_sand_models(&value),
        }
    }
}

/// Persisted model-to-account assignments.  The object form is the canonical
/// representation (`{"model-pattern":"account-id"}`), while the list form
/// is accepted for callers that prefer an explicit editable shape.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum ModelAccountsConfig {
    Map(std::collections::BTreeMap<String, String>),
    List(Vec<CursorModelAccountRule>),
}

impl ModelAccountsConfig {
    fn into_rules(self) -> Vec<CursorModelAccountRule> {
        match self {
            Self::Map(values) => values
                .into_iter()
                .map(|(model, account)| CursorModelAccountRule { model, account })
                .collect(),
            Self::List(values) => values,
        }
    }
}

/// One model pattern assigned to one Cursor account selector.  `account` can
/// be the stable account id printed by `cursor auth list`, or a unique account
/// label/email.  Model patterns use the same case-insensitive glob syntax as
/// `cursor.sandModels`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorModelAccountRule {
    pub model: String,
    pub account: String,
}

impl CursorModelAccountRule {
    pub fn new(model: impl Into<String>, account: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            account: account.into(),
        }
    }
}

/// Model-to-account routing policy.  Rules are evaluated in their persisted
/// order; duplicate normalized model patterns are replaced by the last value.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct CursorAccountRoutingPolicy {
    routes: Vec<CursorModelAccountRule>,
}

impl CursorAccountRoutingPolicy {
    pub fn new<I>(rules: I) -> Self
    where
        I: IntoIterator<Item = CursorModelAccountRule>,
    {
        let mut routes: Vec<CursorModelAccountRule> = Vec::new();
        for rule in rules {
            let model = normalize_sand_model(&rule.model);
            let account = rule.account.trim();
            if model.is_empty() || account.is_empty() {
                continue;
            }
            let normalized = CursorModelAccountRule {
                model,
                account: account.to_string(),
            };
            if let Some(existing) = routes
                .iter_mut()
                .find(|item| item.model == normalized.model)
            {
                *existing = normalized;
            } else {
                routes.push(normalized);
            }
        }
        Self { routes }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn routes(&self) -> &[CursorModelAccountRule] {
        &self.routes
    }

    pub fn into_routes(self) -> Vec<CursorModelAccountRule> {
        self.routes
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    /// Return whether a model has a direct model-account route.
    ///
    /// This deliberately does not resolve Cursor aliases or consult the live
    /// catalog.  The registry uses it as an explicit opt-in signal for a
    /// custom model id, and calling [`account_for_model`] here would recurse
    /// through model resolution for unknown ids.
    pub fn matches_direct(&self, model: &str) -> bool {
        let candidates = account_route_model_candidates(model);
        !candidates.is_empty()
            && self.routes.iter().any(|rule| {
                candidates
                    .iter()
                    .any(|candidate| sand_pattern_matches(&rule.model, candidate))
            })
    }

    /// Return the concrete model spellings that represent the same logical
    /// request as `model` for account-route editing.  Cursor/Anthropic clients
    /// commonly alternate between a public alias and a concrete Fable tier,
    /// and some catalog responses append `-preview`; treating those spellings
    /// as one edit target prevents a stale exact rule from resurfacing after a
    /// TUI assignment is rotated or cleared.
    ///
    /// Wildcard patterns are deliberately not returned here.  Callers can use
    /// this set to remove equivalent *literal* rules while retaining broad
    /// fallback rules such as `*` or `gemini-*`.
    pub fn equivalent_model_candidates(model: &str) -> Vec<String> {
        let mut candidates = Self::resolved_model_candidates(model);
        if candidates.is_empty() {
            return candidates;
        }

        // A preview suffix is a catalog/display variant, not a separate
        // account target.  Add both directions so editing either spelling
        // cleans up the other exact rule as well.
        let originals = candidates.clone();
        for candidate in originals {
            let base = candidate.strip_suffix("-preview").unwrap_or(&candidate);
            push_unique_candidate(&mut candidates, base.to_string());
            push_unique_candidate(&mut candidates, format!("{base}-preview"));
        }
        candidates
    }

    /// Return a copy of this policy with the selected model assigned to (or
    /// cleared from) one account.  Assignment edits remove only semantically
    /// equivalent literal rules; wildcard fallbacks remain in place and still
    /// apply when the exact mapping is cleared.
    pub fn with_model_assignment(&self, model: &str, account: Option<&str>) -> Self {
        let normalized = normalize_sand_model(model);
        if normalized.is_empty() {
            return self.clone();
        }
        let candidates = Self::equivalent_model_candidates(&normalized);
        let selected_is_wildcard = model_pattern_has_wildcards(&normalized);
        let mut routes = self
            .routes
            .iter()
            .filter(|rule| {
                let rule_is_wildcard = model_pattern_has_wildcards(&rule.model);
                if selected_is_wildcard {
                    // A wildcard row is edited as that exact row.  For a
                    // concrete row, broad rules are always retained.
                    rule.model != normalized
                } else {
                    rule_is_wildcard || !candidates.iter().any(|candidate| candidate == &rule.model)
                }
            })
            .cloned()
            .collect::<Vec<_>>();

        if let Some(account) = account.map(str::trim).filter(|value| !value.is_empty()) {
            // Keep exact assignments ahead of a hand-authored wildcard.  The
            // resolver also scores literals first, but this ordering makes the
            // persisted file deterministic and easy to inspect.
            routes.insert(
                0,
                CursorModelAccountRule::new(normalized, account.to_string()),
            );
        }
        Self::new(routes)
    }

    /// Build the alias/resolution chain used by account lookup.  This is kept
    /// separate from [`equivalent_model_candidates`] because runtime matching
    /// should retain its existing one-way `-preview` semantics, while the TUI
    /// editor needs symmetric cleanup of preview spellings.
    fn resolved_model_candidates(model: &str) -> Vec<String> {
        let normalized_model = normalize_sand_model(model);
        if normalized_model.is_empty() {
            return Vec::new();
        }
        let mut candidates = vec![normalized_model.clone()];
        // Resolve a preview spelling through its base id as well.  The model
        // resolver knows `fable`/`claude-fable-5`, but not every catalog's
        // `-preview` display suffix; retaining both forms keeps Fable alias
        // cleanup symmetric when the selected TUI row is `fable-preview`.
        let mut candidate = normalized_model
            .strip_suffix("-preview")
            .map(str::to_string)
            .unwrap_or_else(|| normalized_model.clone());
        push_unique_candidate(&mut candidates, candidate.clone());
        for _ in 0..2 {
            let Some(resolved) =
                crate::providers::cursor::model::resolve_cursor_model(&candidate).ok()
            else {
                break;
            };
            if resolved.model_id == candidate {
                break;
            }
            candidate = resolved.model_id;
            push_unique_candidate(&mut candidates, candidate.clone());
        }
        // Bare Fable aliases resolve to a concrete thinking tier upstream,
        // while users commonly configure the public `fable`/`claude-fable-5`
        // name. Include both aliases so either form edits one target.
        if candidates
            .iter()
            .any(|candidate| is_fable_sand_family(candidate))
        {
            for alias in ["claude-fable-5", "fable"] {
                push_unique_candidate(&mut candidates, alias.to_string());
            }
        }
        // Cursor's live catalog uses `cursor-grok-*` ids while the public
        // registry and Claude Code commonly send `grok-*` (and vice versa).
        // Treat the two spellings as one account-route target.  Walk every
        // presentation/resolution candidate so a trailing `-preview` marker
        // is handled symmetrically as well; the helper keeps effort tiers
        // distinct (xhigh must never be treated as high).
        for source in candidates.clone() {
            for candidate in account_route_model_candidates(&source) {
                push_unique_candidate(&mut candidates, candidate);
            }
        }
        candidates
    }

    /// Return the configured account selector for a model, if any.
    pub fn account_for_model(&self, model: &str) -> Option<&str> {
        let candidates = Self::resolved_model_candidates(model);
        if candidates.is_empty() {
            return None;
        }

        // Prefer an exact rule over a wildcard regardless of JSON/object
        // ordering. This keeps a TUI assignment deterministic even when a
        // hand-authored `*` fallback appears before it in config.json.
        let mut best: Option<(usize, usize, usize, usize, usize)> = None;
        for (index, rule) in self.routes.iter().enumerate() {
            let Some(candidate_index) = candidates
                .iter()
                .position(|candidate| sand_pattern_matches(&rule.model, candidate))
            else {
                continue;
            };
            let wildcard_count = rule
                .model
                .chars()
                .filter(|ch| matches!(ch, '*' | '?'))
                .count();
            let exact_rank = usize::from(wildcard_count == 0);
            // A literal rule for the model string the caller actually sent is
            // the strongest signal.  This must outrank a longer literal that
            // only matches after alias resolution (for example
            // `claude-fable-5` versus `claude-fable-5-thinking-max`).
            let direct_exact = usize::from(
                candidate_index == 0 && wildcard_count == 0 && rule.model == candidates[0],
            );
            // When no direct rule exists, prefer a literal alias/catalog match
            // over a wildcard, then prefer an earlier candidate in the alias
            // chain, and finally use pattern length/insertion order as stable
            // tie breakers.
            let candidate_rank = candidates.len().saturating_sub(candidate_index);
            let score = (
                direct_exact,
                exact_rank,
                candidate_rank,
                rule.model.len(),
                usize::MAX - index,
            );
            if best.is_none_or(|current| score > current) {
                best = Some(score);
            }
            // A literal match on the original normalized model cannot be
            // improved by any later alias or wildcard rule.  A rule that only
            // matches because the incoming id has a `-preview` suffix must
            // keep scanning: a later `*-preview`-specific literal is more
            // precise and should win.
            if direct_exact == 1 {
                break;
            }
        }
        best.map(|(_, _, _, _, encoded_index)| {
            let index = usize::MAX - encoded_index;
            self.routes[index].account.as_str()
        })
    }
}

fn push_unique_candidate(candidates: &mut Vec<String>, candidate: String) {
    if !candidate.is_empty() && !candidates.iter().any(|existing| existing == &candidate) {
        candidates.push(candidate);
    }
}

/// Return model spellings that Cursor uses interchangeably for account
/// routing.  The public registry exposes `grok-4.6`, while the Cursor live
/// catalog and older TUI snapshots use `cursor-grok-4.6-high-fast` (or an
/// `xhigh-fast` variant).  A route written using either namespace must select
/// the same account when a request arrives through the other one.
///
/// The first item is always the normalized input. For an explicit effort tier,
/// aliases include only the same tier in the other namespace plus the bare
/// family names. For a bare family, aliases include Cursor's default
/// `high-fast` spelling. We intentionally never collapse `high-fast` and
/// `xhigh-fast`: those rows can be assigned to different accounts.
fn account_route_model_candidates(model: &str) -> Vec<String> {
    let normalized = normalize_sand_model(model);
    if normalized.is_empty() {
        return Vec::new();
    }
    let mut candidates = vec![normalized.clone()];

    // Keep preview as a presentation marker on generated aliases. Matching
    // still accepts the base spelling through `sand_pattern_matches`, while
    // retaining an explicit `*-preview` route when one was persisted.
    let (base, preview) = normalized
        .strip_suffix("-preview")
        .map_or((normalized.as_str(), ""), |base| (base, "-preview"));

    // `cursor-grok-*` is a catalog prefix, not a distinct model family.
    let (namespace, rest) = if let Some(rest) = base.strip_prefix("cursor-grok-") {
        ("cursor", rest)
    } else if let Some(rest) = base.strip_prefix("grok-") {
        ("public", rest)
    } else {
        return candidates;
    };

    // The rest starts with a version (`4.5`, `4.6`, ...), followed by an
    // optional effort/transport suffix. Keep this parser conservative: an
    // opaque future `grok-*` id is still matched in its original namespace,
    // but we do not collapse unrelated hyphenated names into one account.
    let Some(version_end) = rest.find('-') else {
        // Bare `grok-4.6`/`cursor-grok-4.6`: add the counterpart and the
        // catalog's default high-fast spelling below.
        let counterpart = if namespace == "cursor" {
            format!("grok-{rest}{preview}")
        } else {
            format!("cursor-grok-{rest}{preview}")
        };
        push_unique_candidate(&mut candidates, counterpart);
        let family = format!("grok-{rest}");
        let cursor_family = format!("cursor-grok-{rest}");
        // Bare family ids resolve to Cursor's normal high-fast catalog tier.
        // Do not add xhigh/medium/etc here: a bare request has no effort
        // signal and must not steal a deliberately pinned tier account.
        push_unique_candidate(&mut candidates, format!("{family}{preview}"));
        push_unique_candidate(&mut candidates, format!("{cursor_family}{preview}"));
        push_grok_default_variants(&mut candidates, &family, &cursor_family, preview);
        return candidates;
    };
    let version = &rest[..version_end];
    if version.is_empty()
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return candidates;
    }

    let suffix = &rest[version_end + 1..];
    let public_family = format!("grok-{version}");
    let cursor_family = format!("cursor-grok-{version}");
    let public_variant = if suffix.is_empty() {
        public_family.clone()
    } else {
        format!("grok-{version}-{suffix}")
    };
    let cursor_variant = if suffix.is_empty() {
        cursor_family.clone()
    } else {
        format!("cursor-grok-{version}-{suffix}")
    };

    // Same-tier counterpart first. This handles, for example,
    // `grok-4.6-xhigh-fast` -> `cursor-grok-4.6-xhigh-fast` without changing
    // a deliberate high/xhigh account split.
    push_unique_candidate(
        &mut candidates,
        if namespace == "cursor" {
            format!("{public_variant}{preview}")
        } else {
            format!("{cursor_variant}{preview}")
        },
    );
    // A configured bare family is a useful fallback for every explicit tier,
    // but no *other* explicit tier is an alias. This is what keeps a high
    // request from selecting an xhigh account (and vice versa).
    push_unique_candidate(&mut candidates, format!("{public_family}{preview}"));
    push_unique_candidate(&mut candidates, format!("{cursor_family}{preview}"));

    if suffix.is_empty() {
        // The parsed form is a bare family (the `rest.find('-')` branch above
        // handles the common numeric-only case); keep this guard for a future
        // catalog spelling whose version parser yields an empty suffix.
        push_grok_default_variants(&mut candidates, &public_family, &cursor_family, preview);
    }
    candidates
}

/// Add the default Cursor catalog tier for a bare Grok family. The helper is
/// deliberately narrow: callers handling an explicit `xhigh-fast` (or any
/// other tier) must not receive unrelated account-route candidates.
fn push_grok_default_variants(
    candidates: &mut Vec<String>,
    public_family: &str,
    cursor_family: &str,
    preview: &str,
) {
    // Keep the default order deterministic across namespaces. Append the
    // presentation marker *after* the tier (`...-high-fast-preview`), which
    // mirrors Cursor catalog responses and lets `sand_pattern_matches` strip
    // it safely when a base rule was persisted.
    let public = format!("{public_family}-high-fast{preview}");
    let cursor = format!("{cursor_family}-high-fast{preview}");
    push_unique_candidate(candidates, public);
    push_unique_candidate(candidates, cursor);
}

fn model_pattern_has_wildcards(pattern: &str) -> bool {
    pattern.chars().any(|ch| matches!(ch, '*' | '?'))
}

/// Parse the environment representation of model/account routes.  JSON is
/// preferred (`{"grok-build":"work"}`); a compact `model=account` list is
/// also accepted for shell usage.
fn parse_model_accounts_env(raw: &str) -> Vec<CursorModelAccountRule> {
    let trimmed = raw.trim();
    if trimmed.starts_with('{') {
        if let Ok(values) =
            serde_json::from_str::<std::collections::BTreeMap<String, String>>(trimmed)
        {
            return ModelAccountsConfig::Map(values).into_rules();
        }
    }
    if trimmed.starts_with('[')
        && let Ok(values) = serde_json::from_str::<Vec<CursorModelAccountRule>>(trimmed)
    {
        return values;
    }
    trimmed
        .split([',', '\n', '\r'])
        .filter_map(|entry| {
            let (model, account) = entry.split_once('=')?;
            Some(CursorModelAccountRule::new(model.trim(), account.trim()))
        })
        .collect()
}

/// Resolve model-to-account assignments.  An explicitly present environment
/// variable, including an empty value, overrides the file configuration.
pub fn cursor_account_routing_policy() -> CursorAccountRoutingPolicy {
    if let Some(raw) = std::env::var_os("CCP_CURSOR_MODEL_ACCOUNTS") {
        return CursorAccountRoutingPolicy::new(parse_model_accounts_env(&raw.to_string_lossy()));
    }
    let configured = read_file_config(&paths::config_dir())
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.model_accounts)
        .map(ModelAccountsConfig::into_rules)
        .unwrap_or_default();
    CursorAccountRoutingPolicy::new(configured)
}

/// Alias used by request-routing call sites.
pub fn cursor_model_account_routes() -> CursorAccountRoutingPolicy {
    cursor_account_routing_policy()
}

/// Return the account selector configured for a model, if one exists.
pub fn cursor_account_for_model(model: &str) -> Option<String> {
    cursor_account_routing_policy()
        .account_for_model(model)
        .map(str::to_string)
}

/// Return whether `model` is explicitly covered by a model-account route.
/// Unlike [`cursor_account_for_model`], this performs only direct glob
/// matching and therefore remains safe to call from Cursor model resolution
/// for an otherwise unknown custom id.
pub fn cursor_model_account_route_matches(model: &str) -> bool {
    cursor_account_routing_policy().matches_direct(model)
}

/// Persist only `cursor.modelAccounts`, preserving unrelated configuration
/// keys.  The same-directory temporary file and rename make TUI edits atomic.
pub fn persist_cursor_account_routes(policy: &CursorAccountRoutingPolicy) -> io::Result<()> {
    static WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = WRITE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| io::Error::other("config write lock poisoned"))?;

    let path = config_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut root = match fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str::<serde_json::Value>(&raw).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid config JSON: {err}"),
            )
        })?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => serde_json::json!({}),
        Err(err) => return Err(err),
    };
    if !root.is_object() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "config root must be a JSON object",
        ));
    }
    let cursor = root
        .as_object_mut()
        .expect("checked object above")
        .entry("cursor")
        .or_insert_with(|| serde_json::json!({}));
    if !cursor.is_object() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "config cursor must be a JSON object",
        ));
    }
    let mut map = serde_json::Map::new();
    for rule in policy.routes() {
        map.insert(
            rule.model.clone(),
            serde_json::Value::String(rule.account.clone()),
        );
    }
    cursor
        .as_object_mut()
        .expect("checked object above")
        .insert("modelAccounts".to_string(), serde_json::Value::Object(map));

    let encoded = serde_json::to_vec_pretty(&root).map_err(io::Error::other)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.json");
    let tmp = path.with_file_name(format!(".{file_name}.tmp-{}", std::process::id()));
    let write_result = (|| {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(&encoded)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, &path)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    write_result
}

/// Convenience wrapper for TUI callers editing a route list.
pub fn save_cursor_model_accounts<I>(rules: I) -> io::Result<()>
where
    I: IntoIterator<Item = CursorModelAccountRule>,
{
    persist_cursor_account_routes(&CursorAccountRoutingPolicy::new(rules))
}

/// Match an account selector against id, label, or email.  This is public so
/// request routing and the TUI account editor share exactly the same lookup
/// semantics, including case-insensitive globs.
pub fn account_selector_matches(
    selector: &str,
    id: &str,
    label: Option<&str>,
    email: Option<&str>,
) -> bool {
    let selector = selector.trim().to_ascii_lowercase();
    if selector.is_empty() {
        return false;
    }
    [Some(id), label, email].into_iter().flatten().any(|value| {
        let value = value.trim().to_ascii_lowercase();
        !value.is_empty() && glob_matches(&selector, &value)
    })
}

/// Remove model-account rules that select one account.  Selectors may use the
/// account id, label, email, or a glob over any of those fields.  This legacy
/// convenience wrapper assumes the account is the only known identity; account
/// deletion should prefer [`remove_cursor_model_account_routes_for_account_with_remaining`]
/// so a broad selector is retained when it still matches another account.
pub fn remove_cursor_model_account_routes_for_account(
    id: &str,
    label: Option<&str>,
    email: Option<&str>,
) -> io::Result<bool> {
    remove_cursor_model_account_routes_for_account_with_remaining(id, label, email, |_| false)
}

/// Remove model-account rules that select a deleted account unless the same
/// selector still matches one of the accounts that remains in the registry.
/// The callback receives the rule's account selector and should return true
/// when that selector remains valid for another account.  Keeping this check
/// at the config layer ensures TUI/CLI deletion paths apply identical glob
/// semantics without coupling the config module to account-storage structs.
/// Environment overrides are authoritative and are never rewritten here.
pub fn remove_cursor_model_account_routes_for_account_with_remaining<F>(
    id: &str,
    label: Option<&str>,
    email: Option<&str>,
    selector_matches_remaining: F,
) -> io::Result<bool>
where
    F: Fn(&str) -> bool,
{
    if std::env::var_os("CCP_CURSOR_MODEL_ACCOUNTS").is_some() {
        return Ok(false);
    }
    let policy = cursor_account_routing_policy();
    let filtered = policy
        .routes()
        .iter()
        .filter(|rule| {
            !account_selector_matches(&rule.account, id, label, email)
                || selector_matches_remaining(&rule.account)
        })
        .cloned()
        .collect::<Vec<_>>();
    if filtered.len() == policy.routes().len() {
        return Ok(false);
    }
    persist_cursor_account_routes(&CursorAccountRoutingPolicy::new(filtered))?;
    Ok(true)
}

#[derive(Deserialize, Clone)]
struct KimiConfig {
    #[serde(rename = "userAgent")]
    pub user_agent: Option<String>,
    #[serde(rename = "oauthHost")]
    pub oauth_host: Option<String>,
    #[serde(rename = "baseUrl")]
    pub base_url: Option<String>,
}

#[derive(Deserialize, Clone)]
struct GrokConfig {
    #[serde(rename = "baseUrl")]
    pub base_url: Option<String>,
    #[serde(rename = "clientVersion")]
    pub client_version: Option<String>,
}

#[derive(Deserialize)]
struct FileLog {
    pub verbose: Option<bool>,
    pub stderr: Option<bool>,
}

fn parse_alias(raw: &str) -> Option<AliasProvider> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "codex" => Some(AliasProvider::Codex),
        "kimi" => Some(AliasProvider::Kimi),
        "cursor" => Some(AliasProvider::Cursor),
        _ => None,
    }
}

/// Model-selection policy for Cursor requests that should use the `sand`
/// client surface. Patterns are case-insensitive and support `*` (any number
/// of characters) and `?` (one character).
///
/// Model ids are normalized before matching, so a rule for `claude-fable-5`
/// also matches the `[1m]` listing suffix and the `cursor:`/`cursor-agent:`/
/// `cursor-plan:`/`cursor-ask:` aliases used by the proxy.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct SandRoutingPolicy {
    patterns: Vec<String>,
}

impl SandRoutingPolicy {
    pub fn new<I, S>(patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut normalized = Vec::new();
        for raw in patterns {
            let pattern = normalize_sand_model(raw.as_ref());
            if pattern.is_empty() || normalized.iter().any(|item| item == &pattern) {
                continue;
            }
            normalized.push(pattern);
        }
        Self {
            patterns: normalized,
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn patterns(&self) -> &[String] {
        &self.patterns
    }

    pub fn into_patterns(self) -> Vec<String> {
        self.patterns
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn matches(&self, model: &str) -> bool {
        let model = normalize_sand_model(model);
        if model.is_empty() {
            return false;
        }

        // Cursor exposes Grok through two equivalent namespaces: the public
        // `grok-*` spelling used by Claude Code/grok-build and the
        // `cursor-grok-*` spelling returned by the Cursor catalog.  Match the
        // exact same tier in either namespace, but never collapse effort
        // tiers (in particular `high` and `xhigh`) into one another.  Keeping
        // the alias list to the original id plus its namespace counterpart
        // also means a bare family does not silently enable a variant tier.
        let mut candidates = vec![model.clone()];
        if let Some(counterpart) = grok_namespace_counterpart(&model) {
            push_unique_candidate(&mut candidates, counterpart);
        }

        self.patterns.iter().any(|pattern| {
            candidates
                .iter()
                .any(|candidate| sand_pattern_matches(pattern, candidate))
        })
    }

    pub fn matches_model(&self, model: &str) -> bool {
        if self.matches(model) {
            return true;
        }

        // Fable's public alias can be routed to low/medium/high/max thinking
        // catalog ids by request effort. Selecting any concrete Fable id in
        // the TUI therefore enables the whole Fable family; otherwise a
        // Claude Code effort change would unexpectedly fall back to `cli`.
        if is_fable_sand_family(model)
            && self
                .patterns
                .iter()
                .any(|pattern| is_fable_sand_family(pattern))
        {
            return true;
        }

        // The public Anthropic id is often an alias (for example
        // `claude-fable-5[1m]`) while Cursor receives a concrete catalog id
        // (`claude-fable-5-thinking-max`).  Check that resolved id as well so
        // a model selected in the TUI remains Sand-routed across both forms.
        let mut candidate = model.to_string();
        // Prefix wrappers (`cursor:`/`cursor-plan:`/...) are resolved before
        // Anthropic aliases, so walk the short alias chain once more when
        // needed (`cursor:claude-fable-5` -> `claude-fable-5` -> thinking-max).
        for _ in 0..2 {
            let Some(resolved) =
                crate::providers::cursor::model::resolve_cursor_model(&candidate).ok()
            else {
                break;
            };
            if self.matches(&resolved.model_id) {
                return true;
            }
            if resolved.model_id == candidate {
                break;
            }
            candidate = resolved.model_id;
        }
        false
    }
}

fn is_fable_sand_family(model: &str) -> bool {
    let normalized = normalize_sand_model(model);
    if normalized == "fable" || normalized == "claude-fable-5" {
        return true;
    }
    let Some(suffix) = normalized.strip_prefix("claude-fable-5-") else {
        return false;
    };
    // Fable 5.1 is a separate family (`claude-fable-5-1`). Only treat
    // recognized effort/display suffixes as variants of Fable 5.
    let mut saw_variant = false;
    for token in suffix.split('-') {
        if token.is_empty() {
            return false;
        }
        if matches!(
            token,
            "minimal"
                | "none"
                | "low"
                | "medium"
                | "high"
                | "xhigh"
                | "max"
                | "thinking"
                | "fast"
                | "preview"
        ) {
            saw_variant = true;
        } else {
            return false;
        }
    }
    saw_variant
}

/// Normalize a model id for Sand policy matching.
pub fn normalize_sand_model(model: &str) -> String {
    let mut normalized = model.trim().to_ascii_lowercase();
    for suffix in ["[1m]", "[2m]", "[1M]", "[2M]"] {
        if normalized.ends_with(&suffix.to_ascii_lowercase()) {
            normalized.truncate(normalized.len().saturating_sub(suffix.len()));
            break;
        }
    }
    // Claude Code and a few OpenAI-compatible clients use a human-readable
    // context marker instead of the bracketed `[1m]`/`[2m]` spelling.  Keep
    // account and Sand selectors equivalent across both wire forms so a
    // model bound in the TUI still resolves when the client reports
    // `claude-fable-5 (1M context)`.
    if let Some(open) = normalized.rfind('(')
        && normalized.ends_with(')')
    {
        let inner = normalized[open + 1..normalized.len() - 1]
            .chars()
            .filter(|ch| !ch.is_ascii_whitespace())
            .collect::<String>();
        if inner == "1mcontext" || inner == "2mcontext" {
            normalized.truncate(open);
            normalized = normalized.trim_end().to_string();
        }
    }
    for prefix in ["cursor-plan:", "cursor-ask:", "cursor-agent:", "cursor:"] {
        if let Some(rest) = normalized.strip_prefix(prefix) {
            normalized = rest.to_string();
            break;
        }
    }
    normalized.trim().to_string()
}

fn split_sand_models(raw: &str) -> Vec<String> {
    raw.split([',', '\n', '\r'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

/// Parse the env representation. A JSON array is accepted in addition to the
/// documented comma-separated form, which makes shell quoting less fragile.
fn parse_sand_models_env(raw: &str) -> Vec<String> {
    let trimmed = raw.trim();
    if trimmed.starts_with('[')
        && let Ok(values) = serde_json::from_str::<Vec<String>>(trimmed)
    {
        return values;
    }
    split_sand_models(raw)
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    // Iterative wildcard matching with backtracking to the most recent `*`.
    // Model ids are short, so this stays both simple and allocation-free.
    let pattern = pattern.as_bytes();
    let text = text.as_bytes();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star = None;
    let mut star_text = 0usize;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star = Some(pi);
            star_text = ti;
            pi += 1;
        } else if let Some(star_index) = star {
            pi = star_index + 1;
            star_text += 1;
            ti = star_text;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }
    pi == pattern.len()
}

/// Match a Sand rule against a model id and its common upstream preview alias.
///
/// Cursor's live catalog and Anthropic-compatible clients do not always agree
/// on whether a Gemini id carries a trailing `-preview` marker. Treating that
/// marker as a display/catalog variant keeps a rule for `gemini-3.1-pro`
/// effective when a request arrives as `gemini-3.1-pro-preview`, while still
/// requiring the rest of the model id to match the configured glob.
fn sand_pattern_matches(pattern: &str, model: &str) -> bool {
    if glob_matches(pattern, model) {
        return true;
    }

    // Only strip the marker from the incoming model.  Doing the reverse for a
    // wildcard rule such as `*-preview` would accidentally make every model
    // match (`*`), effectively turning Sand on globally.
    model
        .strip_suffix("-preview")
        .is_some_and(|candidate| glob_matches(pattern, candidate))
}

/// Return the same Grok model spelling in Cursor's alternate namespace.
///
/// The public Grok provider uses ids such as `grok-4.6-xhigh-fast`, whereas
/// Cursor's live catalog commonly calls that row
/// `cursor-grok-4.6-xhigh-fast`.  Namespace conversion is intentionally
/// conservative: only a numeric version (`4.5`, `4.6`, or a future dotted
/// version) is converted, and the suffix is copied verbatim.  As a result,
/// `high` can only match `high`, `xhigh` can only match `xhigh`, and opaque
/// unrelated ids are never promoted into Sand aliases.
fn grok_namespace_counterpart(model: &str) -> Option<String> {
    let normalized = normalize_sand_model(model);
    let (namespace, rest) = if let Some(rest) = normalized.strip_prefix("cursor-grok-") {
        ("grok-", rest)
    } else {
        let rest = normalized.strip_prefix("grok-")?;
        ("cursor-grok-", rest)
    };

    let version = rest.split('-').next().unwrap_or_default();
    if version.is_empty()
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return None;
    }
    Some(format!("{namespace}{rest}"))
}

/// Resolve the current model policy. The env var is an explicit override,
/// including an empty value (which disables all Sand matches).
pub fn cursor_sand_policy() -> SandRoutingPolicy {
    if let Some(raw) = std::env::var_os("CCP_CURSOR_SAND_MODELS") {
        // Sand is opt-in per model.  An environment override must describe
        // the complete policy so an unmatched Fable request remains visibly
        // on the configured default identity (normally CLI).
        return SandRoutingPolicy::new(parse_sand_models_env(&raw.to_string_lossy()));
    }

    let Some(cursor) = read_file_config(&paths::config_dir()).and_then(|file| file.cursor) else {
        // A clean install starts on the normal CLI identity.  Sand must be
        // selected explicitly in the TUI or by a model policy.
        return SandRoutingPolicy::empty();
    };
    let Some(models) = cursor.sand_models else {
        // An unrelated Cursor config does not imply that any model should use
        // the Sand/Bot identity.
        return SandRoutingPolicy::empty();
    };
    SandRoutingPolicy::new(models.into_patterns())
}

/// Alias retained for call sites that prefer the field's name.
pub fn cursor_sand_models() -> SandRoutingPolicy {
    cursor_sand_policy()
}

pub fn cursor_model_uses_sand(model: &str) -> bool {
    cursor_sand_policy().matches_model(model)
}

/// Persist only `cursor.sandModels`, preserving all unrelated config keys.
/// The write uses a same-directory temporary file and rename, so a TUI update
/// cannot leave a partially written JSON document after a process interruption.
pub fn persist_cursor_sand_policy(policy: &SandRoutingPolicy) -> io::Result<()> {
    static WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = WRITE_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .map_err(|_| io::Error::other("config write lock poisoned"))?;

    let path = config_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut root = match fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str::<serde_json::Value>(&raw).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid config JSON: {err}"),
            )
        })?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => serde_json::json!({}),
        Err(err) => return Err(err),
    };
    if !root.is_object() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "config root must be a JSON object",
        ));
    }
    let cursor = root
        .as_object_mut()
        .expect("checked object above")
        .entry("cursor")
        .or_insert_with(|| serde_json::json!({}));
    if !cursor.is_object() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "config cursor must be a JSON object",
        ));
    }
    cursor
        .as_object_mut()
        .expect("checked object above")
        .insert(
            "sandModels".to_string(),
            serde_json::to_value(policy.patterns()).map_err(io::Error::other)?,
        );

    let encoded = serde_json::to_vec_pretty(&root).map_err(io::Error::other)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.json");
    let tmp = path.with_file_name(format!(".{file_name}.tmp-{}", std::process::id()));
    let write_result = (|| {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        file.write_all(&encoded)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, &path)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    write_result
}

/// Convenience wrapper for TUI callers that own the editable pattern list.
pub fn save_cursor_sand_models<I, S>(patterns: I) -> io::Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    persist_cursor_sand_policy(&SandRoutingPolicy::new(patterns))
}

fn read_file_config(config_dir: &Path) -> Option<FileConfig> {
    let path = config_dir.join("config.json");
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

pub fn load_config() -> LoadedConfig {
    let config_dir = paths::config_dir();
    let file = read_file_config(&config_dir);
    let env: HashMap<_, _> = std::env::vars().collect();

    let mut out = LoadedConfig {
        bind_address: "127.0.0.1".to_string(),
        port: 18765,
        // Default Anthropic-style aliases (haiku/sonnet/fable/…) through Cursor so
        // Claude Code's stock model names work without a separate Codex login.
        alias_provider: AliasProvider::Cursor,
        log_verbose: false,
        log_stderr: false,
        config_dir: config_dir.clone(),
    };

    if let Some(raw) = env.get("CCP_BIND_ADDRESS") {
        out.bind_address = raw.clone();
    } else if let Some(bind_address) = file.as_ref().and_then(|f| f.bind_address.clone()) {
        out.bind_address = bind_address;
    }

    if let Some(raw) = env.get("CCP_ALIAS_PROVIDER") {
        if let Some(alias) = parse_alias(raw) {
            out.alias_provider = alias;
        }
    } else if let Some(alias_provider) = file
        .as_ref()
        .and_then(|f| f.alias_provider.as_deref())
        .and_then(parse_alias)
    {
        out.alias_provider = alias_provider;
    }

    if let Some(raw) = env.get("PORT") {
        if let Ok(port) = raw.parse::<u16>() {
            out.port = port;
        }
    } else if let Some(port) = file.as_ref().and_then(|f| f.port) {
        out.port = port;
    }

    if env.contains_key("CCP_LOG_VERBOSE") {
        out.log_verbose = true;
    } else if let Some(value) = file
        .as_ref()
        .and_then(|f| f.log.as_ref().and_then(|v| v.verbose))
    {
        out.log_verbose = value;
    }

    if env.contains_key("CCP_LOG_STDERR") {
        out.log_stderr = true;
    } else if let Some(value) = file
        .as_ref()
        .and_then(|f| f.log.as_ref().and_then(|v| v.stderr))
    {
        out.log_stderr = value;
    }

    out
}

pub fn config_path() -> PathBuf {
    paths::config_dir().join("config.json")
}

pub fn port() -> u16 {
    load_config().port
}

pub fn bind_address() -> String {
    load_config().bind_address
}

pub fn alias_provider() -> AliasProvider {
    load_config().alias_provider
}

pub fn log_verbose() -> bool {
    load_config().log_verbose
}

pub fn log_stderr() -> bool {
    load_config().log_stderr
}

pub fn config_override_summary_lines(cfg: &LoadedConfig) -> Vec<String> {
    let file = read_file_config(&cfg.config_dir);
    let env: HashMap<_, _> = std::env::vars().collect();
    let mut out = Vec::new();
    if env.contains_key("CCP_BIND_ADDRESS") {
        out.push("bindAddress (env)".to_string());
    }
    if env.contains_key("PORT") {
        out.push("port (env)".to_string());
    }
    if env.contains_key("CCP_ALIAS_PROVIDER") {
        out.push("aliasProvider (env)".to_string());
    }
    if env.contains_key("CCP_LOG_VERBOSE") {
        out.push("log.verbose (env)".to_string());
    }
    if env.contains_key("CCP_LOG_STDERR") {
        out.push("log.stderr (env)".to_string());
    }
    if env.contains_key("CCP_KIMI_OAUTH_HOST") {
        out.push("kimi.oauthHost (env)".to_string());
    }
    if env.contains_key("CCP_KIMI_BASE_URL") {
        out.push("kimi.baseUrl (env)".to_string());
    }
    if env.contains_key("CCP_CURSOR_BASE_URL") {
        out.push("cursor.baseUrl (env)".to_string());
    }
    if env.contains_key("CCP_CURSOR_CLIENT_VERSION") {
        out.push("cursor.clientVersion (env)".to_string());
    }
    if env.contains_key("CCP_CURSOR_CLIENT_TYPE") {
        out.push("cursor.clientType (env)".to_string());
    }
    if env.contains_key("CCP_CURSOR_CLIENT_COMMIT") {
        out.push("cursor.clientCommit (env)".to_string());
    }
    if env.contains_key("CCP_CURSOR_GHOST_MODE") {
        out.push("cursor.ghostMode (env)".to_string());
    }
    if env.contains_key("CCP_CURSOR_SAND_MODELS") {
        out.push("cursor.sandModels (env)".to_string());
    }
    if env.contains_key("CCP_CURSOR_MODEL_ACCOUNTS") {
        out.push("cursor.modelAccounts (env)".to_string());
    }
    if env.contains_key("CCP_KIMI_USER_AGENT") {
        out.push("kimi.userAgent (env)".to_string());
    }
    if env.contains_key("CCP_GROK_BASE_URL") {
        out.push("grok.baseUrl (env)".to_string());
    }
    if env.contains_key("CCP_GROK_CLIENT_VERSION") {
        out.push("grok.clientVersion (env)".to_string());
    }
    if env
        .get("CCP_CODEX_REASONING_SUMMARY")
        .is_some_and(|raw| !raw.is_empty())
    {
        out.push("CCP_CODEX_REASONING_SUMMARY (env)".to_string());
    }
    if let Some(file_cfg) = file {
        if let Some(bind_address) = file_cfg.bind_address {
            out.push(format!("bindAddress: {bind_address}"));
        }
        if let Some(p) = file_cfg.port {
            out.push(format!("port: {p}"));
        }
        if let Some(alias) = file_cfg.alias_provider {
            out.push(format!("aliasProvider: {alias}"));
        }
        if let Some(log) = file_cfg.log {
            if let Some(v) = log.verbose {
                out.push(format!("log.verbose: {v}"));
            }
            if let Some(v) = log.stderr {
                out.push(format!("log.stderr: {v}"));
            }
        }
        if let Some(codex) = file_cfg.codex
            && let Some(summary) = codex.reasoning_summary
            && !summary.is_empty()
        {
            out.push("codex.reasoningSummary (config)".to_string());
        }
        if let Some(ref cursor) = file_cfg.cursor
            && let Some(models) = cursor.sand_models.as_ref()
            && !models.clone().into_patterns().is_empty()
        {
            out.push("cursor.sandModels (config)".to_string());
        }
        if let Some(ref cursor) = file_cfg.cursor
            && let Some(routes) = cursor.model_accounts.as_ref()
            && !routes.clone().into_rules().is_empty()
        {
            out.push("cursor.modelAccounts (config)".to_string());
        }
    }
    out
}

pub fn grok_base_url() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_GROK_BASE_URL") {
        return raw.clone();
    }
    if let Some(grok) = read_file_config(&paths::config_dir()).and_then(|f| f.grok)
        && let Some(url) = grok.base_url
    {
        return url;
    }
    "https://cli-chat-proxy.grok.com/v1".to_string()
}

pub fn grok_media_base_url() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_GROK_MEDIA_BASE_URL") {
        return raw.clone();
    }
    "https://api.x.ai/v1".to_string()
}

pub fn grok_client_version() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_GROK_CLIENT_VERSION") {
        return raw.clone();
    }
    if let Some(grok) = read_file_config(&paths::config_dir()).and_then(|f| f.grok)
        && let Some(version) = grok.client_version
    {
        return version;
    }
    "0.2.93".to_string()
}

pub fn is_verbose() -> bool {
    log_verbose()
}

pub fn kimi_oauth_host() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_KIMI_OAUTH_HOST") {
        return raw.clone();
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(kimi) = file.kimi
        && let Some(host) = kimi.oauth_host
    {
        return host;
    }
    "https://auth.kimi.com".to_string()
}

pub fn kimi_base_url() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_KIMI_BASE_URL") {
        return raw.clone();
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(kimi) = file.kimi
        && let Some(url) = kimi.base_url
    {
        return url;
    }
    "https://api.kimi.com/coding/v1".to_string()
}

pub fn kimi_user_agent(default: &str) -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_KIMI_USER_AGENT") {
        return raw.clone();
    }
    if let Some(raw) = env.get("CCP_USER_AGENT") {
        return raw.clone();
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(kimi) = file.kimi
        && let Some(ua) = kimi.user_agent
    {
        return ua;
    }
    default.to_string()
}

// ---------------------------------------------------------------------------
// Codex config
// ---------------------------------------------------------------------------

pub fn codex_base_url(default: &str) -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CODEX_BASE_URL") {
        return raw.clone();
    }
    if let Some(raw) = env.get("CLAUDE_CODE_PROXY_CODEX_BASE_URL") {
        return raw.clone();
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
        && let Some(url) = codex.base_url
    {
        return url;
    }
    default.to_string()
}

pub fn codex_originator(default: &str) -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CODEX_ORIGINATOR") {
        return raw.clone();
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
        && let Some(val) = codex.originator
    {
        return val;
    }
    default.to_string()
}

pub fn codex_user_agent(default: &str) -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CODEX_USER_AGENT") {
        return raw.clone();
    }
    if let Some(raw) = env.get("CCP_USER_AGENT") {
        return raw.clone();
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
        && let Some(ua) = codex.user_agent
    {
        return ua;
    }
    default.to_string()
}

pub fn codex_previous_response_id() -> bool {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CODEX_PREVIOUS_RESPONSE_ID") {
        return matches!(raw.to_ascii_lowercase().as_str(), "1" | "true" | "yes");
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
        && let Some(val) = codex.previous_response_id
    {
        return val;
    }
    false
}

pub fn codex_service_tier() -> Option<String> {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CODEX_SERVICE_TIER") {
        return Some(raw.clone());
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
    {
        return codex.service_tier;
    }
    None
}

pub fn codex_effort() -> Option<String> {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CODEX_EFFORT") {
        return Some(raw.clone());
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
    {
        return codex.effort;
    }
    None
}

pub fn codex_reasoning_summary() -> Option<String> {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env
        .get("CCP_CODEX_REASONING_SUMMARY")
        .filter(|raw| !raw.is_empty())
    {
        return Some(raw.clone());
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
        && let Some(summary) = codex.reasoning_summary.filter(|raw| !raw.is_empty())
    {
        return Some(summary);
    }
    None
}

pub fn codex_model() -> Option<String> {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CODEX_MODEL") {
        return Some(raw.clone());
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
    {
        return codex.model;
    }
    None
}

// ---------------------------------------------------------------------------
// Codex transport config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexTransport {
    Http,
    WebSocket,
    Auto,
}

impl CodexTransport {
    pub fn as_str(self) -> &'static str {
        match self {
            CodexTransport::Http => "http",
            CodexTransport::WebSocket => "websocket",
            CodexTransport::Auto => "auto",
        }
    }
}

fn parse_codex_transport(raw: &str) -> Option<CodexTransport> {
    match raw {
        "http" => Some(CodexTransport::Http),
        "websocket" => Some(CodexTransport::WebSocket),
        "auto" => Some(CodexTransport::Auto),
        _ => None,
    }
}

pub fn codex_transport() -> CodexTransport {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CODEX_TRANSPORT")
        && let Some(transport) = parse_codex_transport(raw)
    {
        return transport;
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(codex) = file.codex
        && let Some(transport) = codex.transport.as_deref().and_then(parse_codex_transport)
    {
        return transport;
    }
    CodexTransport::WebSocket
}

// ---------------------------------------------------------------------------
// Cursor config
// ---------------------------------------------------------------------------

pub fn cursor_base_url() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CURSOR_BASE_URL") {
        return raw.clone();
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(cursor) = file.cursor
        && let Some(url) = cursor.base_url
    {
        return url;
    }
    "https://api2.cursor.sh".to_string()
}

/// URL used by the current Sand inference transport.  Sand moved from the
/// AgentService endpoint to `aiserver.v1.InferenceService/Stream`; an explicit
/// override is useful for regional gateways and local protocol fixtures while
/// leaving normal CLI/IDE traffic on `cursor_base_url()`.
pub fn cursor_sand_base_url() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CURSOR_SAND_BASE_URL") {
        let value = raw.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(cursor) = file.cursor
        && let Some(url) = cursor.sand_base_url
    {
        let value = url.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }
    // Keep the ordinary endpoint as the fallback so existing
    // `CCP_CURSOR_BASE_URL` fixtures and regional overrides continue to work
    // when no Sand-specific URL is configured.
    cursor_base_url()
}

pub fn cursor_client_version() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CURSOR_CLIENT_VERSION") {
        let t = raw.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(cursor) = file.cursor
        && let Some(version) = cursor.client_version
    {
        let t = version.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    // Official Cursor CLI sends `cli-<install-version>` e.g. cli-2026.07.16-899851b.
    // Auto-detect from ~/.local/share/cursor-agent/versions when present.
    if let Some(detected) = detect_installed_cursor_cli_version() {
        return format!("cli-{detected}");
    }
    "cli-2026.07.16-899851b".to_string()
}

/// Return the client-version value used by a request identity.
///
/// Cursor's desktop Sand path uses the product version (for example
/// `3.18.9`), while the standalone Agent CLI uses `cli-<version>`.  Sending
/// the CLI-prefixed value together with `x-cursor-client-type: sand` can make
/// the server classify the request as a legacy CLI stream and skip the local
/// runtime route.  Keep the Sand override request-scoped and deterministic:
/// `CCP_CURSOR_SAND_CLIENT_VERSION` wins, then `cursor.sandClientVersion`, then
/// a locally installed Cursor product version, and finally the normal client
/// version as an offline fallback.
pub fn cursor_client_version_for_type(client_type: &str) -> String {
    if !client_type.trim().eq_ignore_ascii_case("sand") {
        return cursor_client_version();
    }

    if let Ok(raw) = std::env::var("CCP_CURSOR_SAND_CLIENT_VERSION") {
        let value = raw.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }

    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(cursor) = file.cursor
        && let Some(version) = cursor.sand_client_version
    {
        let value = version.trim();
        if !value.is_empty() {
            return value.to_string();
        }
    }

    detect_installed_cursor_desktop_version().unwrap_or_else(cursor_client_version)
}

/// Best-effort detection of the installed Cursor desktop product version.
///
/// The helper intentionally reads only small JSON metadata files and never
/// starts or mutates Cursor.  It is useful for Sand identity headers on macOS
/// and Linux; callers can override it with `CCP_CURSOR_SAND_CLIENT_VERSION`.
pub fn detect_installed_cursor_desktop_version() -> Option<String> {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(raw) = std::env::var_os("CCP_CURSOR_APP") {
        let root = PathBuf::from(raw);
        candidates.push(root.join("Contents/Resources/app/package.json"));
        candidates.push(root.join("resources/app/package.json"));
        candidates.push(root.join("package.json"));
    }
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        candidates.extend([
            home.join("Applications/Cursor.app/Contents/Resources/app/package.json"),
            home.join(".local/share/cursor/package.json"),
            home.join(".local/share/cursor/resources/app/package.json"),
        ]);
    }
    candidates.push(PathBuf::from(
        "/Applications/Cursor.app/Contents/Resources/app/package.json",
    ));
    candidates.push(PathBuf::from(
        "/Applications/Cursor.app/Contents/Resources/app/product.json",
    ));

    for path in candidates {
        let Ok(raw) = fs::read_to_string(path) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
            continue;
        };
        let Some(version) = value.get("version").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let version = version.trim();
        if !version.is_empty() {
            return Some(version.to_string());
        }
    }
    None
}

/// Cursor `x-cursor-client-type` header.
/// Official agent CLI defaults to `cli` (see surface:"cli" in cursor-agent index.js).
/// Set `CCP_CURSOR_CLIENT_TYPE=ide` only when intentionally spoofing the desktop app.
pub fn cursor_client_type() -> String {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CURSOR_CLIENT_TYPE") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(cursor) = file.cursor
        && let Some(client_type) = cursor.client_type
    {
        let trimmed = client_type.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    "cli".to_string()
}

/// Select the Cursor identity for one request. Sand routing is deliberately
/// resolved without mutating process-wide configuration, so concurrent model
/// requests cannot leak their client type into one another.
pub fn cursor_client_type_for_model(model: &str) -> String {
    if cursor_sand_policy().matches_model(model) {
        "sand".to_string()
    } else {
        cursor_client_type()
    }
}

/// Request identities whose live `GetUsableModels` catalogs should be kept
/// warm for the current routing policy.  Cursor can expose a different model
/// catalog through the managed-local Sand identity than through the ordinary
/// CLI identity, so enabling any Sand rule requires a second, identity-scoped
/// catalog lookup.
///
/// The process-wide fallback identity remains first.  When it is already Sand
/// we deliberately avoid a duplicate request; otherwise Sand is appended only
/// when at least one model can select that route.
pub fn cursor_catalog_client_types() -> Vec<String> {
    let default_client_type = cursor_client_type();
    let mut client_types = vec![default_client_type];
    if !cursor_sand_policy().is_empty()
        && !client_types
            .iter()
            .any(|client_type| client_type.trim().eq_ignore_ascii_case("sand"))
    {
        client_types.push("sand".to_string());
    }
    client_types
}

/// Detect installed Cursor Agent CLI version directory name
/// (e.g. `2026.07.16-899851b` under `~/.local/share/cursor-agent/versions/`).
pub fn detect_installed_cursor_cli_version() -> Option<String> {
    let home = std::env::var_os("HOME")?;
    let versions_dir = std::path::PathBuf::from(home).join(".local/share/cursor-agent/versions");
    let mut best: Option<String> = None;
    if let Ok(rd) = std::fs::read_dir(&versions_dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.is_empty() || name.starts_with('.') {
                continue;
            }
            // Prefer lexicographically latest (YYYY.MM.DD-hash sorts well).
            if best
                .as_ref()
                .map(|b| name.as_str() > b.as_str())
                .unwrap_or(true)
            {
                best = Some(name);
            }
        }
    }
    best
}

/// Optional `x-cursor-client-commit` (Cursor IDE sends the app commit hash).
pub fn cursor_client_commit() -> Option<String> {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CURSOR_CLIENT_COMMIT") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(cursor) = file.cursor
        && let Some(commit) = cursor.client_commit
    {
        let trimmed = commit.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    // Regular (non-Anysphere) IDE builds omit commit; only send when env/config set.
    None
}

/// Cursor `x-ghost-mode` header.
/// Official CLI defaults to `true` when privacyCache.ghostMode is unset
/// (`return typeof r !== "boolean" || r`).
pub fn cursor_ghost_mode() -> bool {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CURSOR_GHOST_MODE") {
        return parse_env_bool(raw);
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(cursor) = file.cursor
        && let Some(ghost) = cursor.ghost_mode
    {
        return ghost;
    }
    true
}

/// Wire representation used by Cursor Desktop's common-header helper.  An
/// unset privacy value is deliberately distinct from an explicit `true` or
/// `false`: the desktop helper emits `implicit-false` for the unset case.
/// Keeping that distinction avoids Sand requests being classified as the
/// legacy CLI profile by the gateway.
pub fn cursor_ghost_mode_header() -> String {
    if let Ok(raw) = std::env::var("CCP_CURSOR_GHOST_MODE") {
        let value = raw.trim();
        if !value.is_empty() {
            return if parse_env_bool(value) {
                "true".to_string()
            } else {
                "false".to_string()
            };
        }
    }
    let config_value = read_file_config(&paths::config_dir())
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.ghost_mode);
    match config_value {
        Some(true) => "true".to_string(),
        Some(false) => "false".to_string(),
        None => "implicit-false".to_string(),
    }
}

/// Cursor Desktop always sends `x-new-onboarding-completed` on common-header
/// requests.  Its value is the conjunction of snippet eligibility and privacy
/// state; SandClientMode forces eligibility off, therefore `false` is the
/// correct headless default.  Keep an explicit env/config override for users
/// who mirror a different Desktop profile.
pub fn cursor_new_onboarding_completed() -> bool {
    if let Ok(raw) = std::env::var("CCP_CURSOR_NEW_ONBOARDING_COMPLETED") {
        return parse_env_bool(&raw);
    }
    read_file_config(&paths::config_dir())
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.new_onboarding_completed)
        .unwrap_or(false)
}

/// Whether to attach IDE-only fingerprint headers (device-type/os/arch/checksum…).
/// Official CLI Agent path does NOT set these; only the IDE `ccf()` helper does.
/// Profiles: `cli` (default) | `ide`
pub fn cursor_client_profile() -> String {
    if let Ok(raw) = std::env::var("CCP_CURSOR_CLIENT_PROFILE") {
        let t = raw.trim().to_ascii_lowercase();
        if !t.is_empty() {
            return t;
        }
    }
    "cli".to_string()
}

/// Optional desktop layout marker (`ide`/`glass`) copied by Cursor's common
/// header interceptor.  Sand clients may need this marker when an upstream
/// gateway applies desktop eligibility rules; leaving it unset preserves the
/// normal CLI request shape.
pub fn cursor_client_layout() -> Option<String> {
    if let Ok(raw) = std::env::var("CCP_CURSOR_CLIENT_LAYOUT") {
        let value = raw.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    let config_dir = paths::config_dir();
    read_file_config(&config_dir)
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.client_layout)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Optional operating-system version used by the desktop identity helper.
pub fn cursor_client_os_version() -> Option<String> {
    if let Ok(raw) = std::env::var("CCP_CURSOR_CLIENT_OS_VERSION") {
        let value = raw.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    let config_dir = paths::config_dir();
    read_file_config(&config_dir)
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.client_os_version)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Optional Cursor canary marker.  The desktop app only sends this header for
/// Anysphere/canary builds; it is omitted unless explicitly configured.
pub fn cursor_canary() -> bool {
    if let Ok(raw) = std::env::var("CCP_CURSOR_CANARY") {
        return parse_env_bool(&raw);
    }
    read_file_config(&paths::config_dir())
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.canary)
        .unwrap_or(false)
}

/// Optional Cursor configuration-version marker used by the desktop common
/// header helper.  Empty values are treated as absent.
pub fn cursor_config_version() -> Option<String> {
    if let Ok(raw) = std::env::var("CCP_CURSOR_CONFIG_VERSION") {
        let value = raw.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    read_file_config(&paths::config_dir())
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.config_version)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Whether an identity should advertise Cursor's local-client mode.  Sand is
/// local-runtime backed by definition; an explicit environment/config value
/// can opt a custom identity into the same header for troubleshooting.
pub fn cursor_local_client_mode(client_type: &str) -> bool {
    if let Ok(raw) = std::env::var("CCP_CURSOR_LOCAL_CLIENT_MODE") {
        return parse_env_bool(&raw);
    }
    read_file_config(&paths::config_dir())
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.local_client_mode)
        .unwrap_or_else(|| client_type.trim().eq_ignore_ascii_case("sand"))
}

/// Request timeout for Cursor Agent runs (seconds).
/// Default 90s: long enough for a short Fable reply, short enough to surface hangs
/// (BiDi waiting for tools) instead of sitting on "upstream" for 5+ minutes.
pub fn cursor_request_timeout_secs() -> u64 {
    if let Ok(raw) = std::env::var("CCP_CURSOR_TIMEOUT_SECS")
        && let Ok(n) = raw.trim().parse::<u64>()
        && n > 0
    {
        return n.min(3600);
    }
    90
}

pub fn cursor_client_os() -> String {
    if let Ok(raw) = std::env::var("CCP_CURSOR_CLIENT_OS") {
        let t = raw.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    match std::env::consts::OS {
        "macos" => "darwin".to_string(),
        other => other.to_string(),
    }
}

pub fn cursor_client_arch() -> String {
    if let Ok(raw) = std::env::var("CCP_CURSOR_CLIENT_ARCH") {
        let t = raw.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    match std::env::consts::ARCH {
        "aarch64" => "arm64".to_string(),
        "x86_64" => "x64".to_string(),
        other => other.to_string(),
    }
}

pub fn cursor_timezone() -> Option<String> {
    if let Ok(raw) = std::env::var("CCP_CURSOR_TIMEZONE") {
        let t = raw.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    let config_dir = paths::config_dir();
    if let Some(value) = read_file_config(&config_dir)
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.timezone)
    {
        let value = value.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }

    // Cursor's desktop helper uses `Intl.DateTimeFormat().resolvedOptions()`.
    // Reproduce the common local-zone resolution without spawning a process:
    // `TZ` is honored first, then Unix zoneinfo links/files.  Keep this
    // best-effort because minimal containers may intentionally omit timezone
    // data; the header is optional on the server.
    if let Ok(raw) = std::env::var("TZ") {
        let value = raw.trim();
        if !value.is_empty() && !value.starts_with(':') {
            return Some(value.to_string());
        }
    }
    #[cfg(unix)]
    {
        use std::path::Path;
        let localtime = Path::new("/etc/localtime");
        if let Ok(target) = std::fs::canonicalize(localtime)
            && let Some(path) = target.to_str()
            && let Some((_, zone)) = path.split_once("/zoneinfo/")
        {
            let zone = zone.trim_matches('/');
            if !zone.is_empty() && !zone.contains("..") {
                return Some(zone.to_string());
            }
        }
        if let Ok(raw) = std::fs::read_to_string("/etc/timezone") {
            let value = raw.trim();
            if !value.is_empty() && !value.starts_with('#') {
                return Some(value.to_string());
            }
        }
    }
    None
}

pub fn cursor_client_key() -> Option<String> {
    std::env::var("CCP_CURSOR_CLIENT_KEY")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn cursor_session_id() -> Option<String> {
    std::env::var("CCP_CURSOR_SESSION_ID")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn parse_env_bool(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

pub fn cursor_agent_bundle() -> Option<String> {
    let env: HashMap<_, _> = std::env::vars().collect();
    if let Some(raw) = env.get("CCP_CURSOR_AGENT_BUNDLE") {
        return Some(raw.clone());
    }
    let config_dir = paths::config_dir();
    if let Some(file) = read_file_config(&config_dir)
        && let Some(cursor) = file.cursor
        && let Some(bundle) = cursor.agent_bundle
    {
        return Some(bundle);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use std::sync::Mutex;

    static ENV_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    fn clear_env() {
        unsafe {
            std::env::remove_var("CCP_BIND_ADDRESS");
            std::env::remove_var("CCP_CODEX_TRANSPORT");
            std::env::remove_var("CCP_CONFIG_DIR");
            std::env::remove_var("CCP_LOG_VERBOSE");
            std::env::remove_var("CCP_LOG_STDERR");
            std::env::remove_var("CCP_CODEX_REASONING_SUMMARY");
            std::env::remove_var("CCP_CURSOR_SAND_MODELS");
            std::env::remove_var("CCP_CURSOR_MODEL_ACCOUNTS");
            std::env::remove_var("CCP_CURSOR_CLIENT_TYPE");
            std::env::remove_var("CCP_CURSOR_TIMEZONE");
        }
    }

    #[test]
    fn bind_address_defaults_to_loopback() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        assert_eq!(load_config().bind_address, "127.0.0.1");
    }

    #[test]
    fn cursor_timezone_prefers_explicit_environment_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let _timezone = EnvGuard::set("CCP_CURSOR_TIMEZONE", "Asia/Shanghai");
        assert_eq!(cursor_timezone().as_deref(), Some("Asia/Shanghai"));
    }

    #[cfg(unix)]
    #[test]
    fn cursor_timezone_detects_local_zoneinfo_when_no_override_is_set() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let detected = cursor_timezone();
        // CI images may omit /etc/localtime and /etc/timezone, so accept an
        // absent value; when present it must be a relative IANA zone name.
        if let Some(value) = detected {
            assert!(!value.starts_with('/'));
            assert!(!value.contains(".."));
            assert!(!value.trim().is_empty());
        }
    }

    #[test]
    fn bind_address_reads_config_and_env_takes_precedence() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"bindAddress":"192.0.2.10"}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        assert_eq!(load_config().bind_address, "192.0.2.10");
        let _bind_env = EnvGuard::set("CCP_BIND_ADDRESS", "0.0.0.0");
        assert_eq!(load_config().bind_address, "0.0.0.0");
    }

    struct EnvGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous.take() {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    fn codex_transport_defaults_to_websocket() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let result = codex_transport();
        assert_eq!(result, CodexTransport::WebSocket);
    }

    #[test]
    fn codex_transport_reads_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("CCP_CODEX_TRANSPORT", "auto");
        }
        assert_eq!(codex_transport(), CodexTransport::Auto);
    }

    #[test]
    fn codex_transport_env_websocket() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("CCP_CODEX_TRANSPORT", "websocket");
        }
        assert_eq!(codex_transport(), CodexTransport::WebSocket);
    }

    #[test]
    fn codex_transport_invalid_env_falls_back_to_websocket() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("CCP_CODEX_TRANSPORT", "invalid");
        }
        assert_eq!(codex_transport(), CodexTransport::WebSocket);
    }

    #[test]
    fn codex_transport_empty_env_falls_back_to_websocket() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("CCP_CODEX_TRANSPORT", "");
        }
        assert_eq!(codex_transport(), CodexTransport::WebSocket);
    }

    #[test]
    fn parse_codex_transport_variants() {
        assert_eq!(parse_codex_transport("http"), Some(CodexTransport::Http));
        assert_eq!(
            parse_codex_transport("websocket"),
            Some(CodexTransport::WebSocket)
        );
        assert_eq!(parse_codex_transport("auto"), Some(CodexTransport::Auto));
        assert_eq!(parse_codex_transport(""), None);
        assert_eq!(parse_codex_transport("HTTP"), None);
        assert_eq!(parse_codex_transport("ws"), None);
    }

    #[test]
    fn codex_transport_as_str() {
        assert_eq!(CodexTransport::Http.as_str(), "http");
        assert_eq!(CodexTransport::WebSocket.as_str(), "websocket");
        assert_eq!(CodexTransport::Auto.as_str(), "auto");
    }

    #[test]
    fn log_env_presence_enables_legacy_verbose_and_stderr() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _verbose_env = EnvGuard::set("CCP_LOG_VERBOSE", "0");
        let _stderr_env = EnvGuard::set("CCP_LOG_STDERR", "");

        let loaded = load_config();
        assert!(loaded.log_verbose);
        assert!(loaded.log_stderr);
    }

    #[test]
    fn log_config_values_apply_without_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"log":{"verbose":true,"stderr":true}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        let loaded = load_config();
        assert!(loaded.log_verbose);
        assert!(loaded.log_stderr);
    }

    #[test]
    fn codex_reasoning_summary_reads_config() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"codex":{"reasoningSummary":"off"}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        assert_eq!(codex_reasoning_summary().as_deref(), Some("off"));
    }

    #[test]
    fn codex_reasoning_summary_env_overrides_config_and_empty_falls_through() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"codex":{"reasoningSummary":"off"}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        {
            let _summary_env = EnvGuard::set("CCP_CODEX_REASONING_SUMMARY", "auto");
            assert_eq!(codex_reasoning_summary().as_deref(), Some("auto"));
        }
        {
            let _summary_env = EnvGuard::set("CCP_CODEX_REASONING_SUMMARY", "");
            assert_eq!(codex_reasoning_summary().as_deref(), Some("off"));
        }
    }

    #[test]
    fn sand_policy_normalizes_aliases_and_supports_globs() {
        let policy = SandRoutingPolicy::new([
            "claude-fable-5",
            "gpt-5.?",
            "cursor:duplicate",
            "cursor:duplicate[1m]",
        ]);
        assert_eq!(
            policy.patterns(),
            &["claude-fable-5", "gpt-5.?", "duplicate"]
        );
        assert!(policy.matches("claude-fable-5[1m]"));
        assert!(policy.matches("cursor-plan:claude-fable-5"));
        assert!(policy.matches("cursor-agent:claude-fable-5"));
        assert!(policy.matches("gpt-5.4"));
        assert!(!policy.matches("gpt-5.42"));
    }

    #[test]
    fn sand_policy_matches_resolved_cursor_aliases() {
        let policy = SandRoutingPolicy::new(["claude-fable-5-thinking-max"]);
        assert!(policy.matches_model("claude-fable-5[1m]"));
        assert!(policy.matches_model("fable"));
        assert!(policy.matches_model("cursor:claude-fable-5"));
        assert!(policy.matches_model("claude-fable-5-thinking-high"));
        assert!(policy.matches_model("claude-fable-5-thinking-low[1m]"));
        assert!(!policy.matches_model("claude-sonnet-5"));
        assert!(!policy.matches_model("claude-fable-50-thinking-max"));
    }

    #[test]
    fn sand_policy_keeps_fable_5_1_separate_from_fable_5() {
        let fable_51 = SandRoutingPolicy::new(["claude-fable-5-1-thinking-max"]);
        assert!(fable_51.matches_model("claude-fable-5-1-thinking-max"));
        assert!(!fable_51.matches_model("claude-fable-5[1m]"));

        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":["claude-fable-5-1-thinking-max"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let policy = cursor_sand_policy();
        assert!(!policy.matches_model("claude-fable-5[1m]"));
        assert!(policy.matches_model("claude-fable-5-1-thinking-max"));
    }

    #[test]
    fn sand_policy_matches_grok_namespace_counterpart_at_same_tier() {
        let high = SandRoutingPolicy::new(["cursor-grok-4.6-high-fast"]);
        assert!(high.matches_model("cursor-grok-4.6-high-fast"));
        assert!(high.matches_model("grok-4.6-high-fast"));
        assert!(high.matches_model("cursor:grok-4.6-high-fast"));
        assert!(high.matches_model("cursor:cursor-grok-4.6-high-fast"));

        // The namespace alias must not turn a high-tier rule into an xhigh
        // rule (or vice versa).  These rows may be assigned to different
        // accounts and consume distinct quota entries upstream.
        assert!(!high.matches_model("grok-4.6-xhigh-fast"));
        assert!(!high.matches_model("cursor-grok-4.6-xhigh-fast"));

        let xhigh = SandRoutingPolicy::new(["grok-4.6-xhigh-fast"]);
        assert!(xhigh.matches_model("cursor-grok-4.6-xhigh-fast"));
        assert!(xhigh.matches_model("cursor:grok-4.6-xhigh-fast"));
        assert!(!xhigh.matches_model("cursor-grok-4.6-high-fast"));
    }

    #[test]
    fn sand_policy_matches_bare_grok_namespaces_without_enabling_variants() {
        let policy = SandRoutingPolicy::new(["grok-4.6"]);
        assert!(policy.matches_model("grok-4.6"));
        assert!(policy.matches_model("cursor-grok-4.6"));
        assert!(policy.matches_model("cursor:grok-4.6"));

        // A bare family selector is not an implicit wildcard.  Variant rows
        // must be selected explicitly so a high/xhigh account or quota lane
        // cannot be chosen accidentally.
        assert!(!policy.matches_model("grok-4.6-high-fast"));
        assert!(!policy.matches_model("cursor-grok-4.6-xhigh-fast"));
        assert!(!policy.matches_model("cursor-grok-4.5"));
    }

    #[test]
    fn sand_policy_grok_glob_alias_is_scoped_to_grok_namespace() {
        let policy = SandRoutingPolicy::new(["cursor-grok-4.6-*"]);
        assert!(policy.matches_model("grok-4.6-high-fast"));
        assert!(policy.matches_model("grok-4.6-xhigh-fast"));
        assert!(!policy.matches_model("grok-4.5-high-fast"));
        assert!(!policy.matches_model("gemini-4.6-high-fast"));
        assert!(!policy.matches_model("cursor-sonnet-4.6-high-fast"));
    }

    #[test]
    fn sand_policy_reads_config_and_env_overrides_it() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":["claude-fable-*","gpt-5.4"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        let from_file = cursor_sand_policy();
        assert!(from_file.matches("claude-fable-5"));
        assert!(from_file.matches("gpt-5.4"));
        assert!(!from_file.matches("gpt-5.5"));

        let _sand_env = EnvGuard::set("CCP_CURSOR_SAND_MODELS", "grok-*, [1m-invalid]");
        let from_env = cursor_sand_policy();
        assert!(from_env.matches("grok-4.5"));
        // Environment rules are explicit. An unmatched Fable request stays on
        // the configured default identity instead of being promoted to Sand.
        assert!(!from_env.matches("claude-fable-5[1m]"));
        assert!(!from_env.matches("gpt-5.4"));
    }

    #[test]
    fn nonempty_sand_policy_does_not_add_fable_for_config_and_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":["gemini-3.1-pro"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        let from_file = cursor_sand_policy();
        assert!(from_file.matches_model("gemini-3.1-pro[1m]"));
        assert!(!from_file.matches_model("claude-fable-5[1m]"));

        let _sand_env = EnvGuard::set("CCP_CURSOR_SAND_MODELS", "grok-4.6-xhigh-fast");
        let from_env = cursor_sand_policy();
        assert!(from_env.matches_model("grok-4.6-xhigh-fast"));
        assert!(!from_env.matches_model("claude-fable-5[1m]"));
        assert!(!from_env.matches_model("gemini-3.1-pro"));

        // Empty overrides retain the explicit opt-out behavior.
        let _empty_env = EnvGuard::set("CCP_CURSOR_SAND_MODELS", "");
        let disabled = cursor_sand_policy();
        assert!(disabled.is_empty());
        assert!(!disabled.matches_model("claude-fable-5[1m]"));
    }

    #[test]
    fn account_policy_normalizes_models_and_replaces_duplicate_rules() {
        let policy = CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("*", "fallback"),
            CursorModelAccountRule::new(" Gemini-3.1-Pro[1m] ", "work"),
            CursorModelAccountRule::new("gemini-3.1-pro", "updated"),
            CursorModelAccountRule::new("claude-fable-*", "fable-account"),
            CursorModelAccountRule::new("claude-fable-5", "fable-exact"),
        ]);
        assert_eq!(policy.routes().len(), 4);
        assert_eq!(
            policy.account_for_model("gemini-3.1-pro-preview"),
            Some("updated")
        );
        assert_eq!(
            policy.account_for_model("gemini-3.1-pro[2m]"),
            Some("updated")
        );
        assert_eq!(
            policy.account_for_model("cursor:claude-fable-5[1m]"),
            Some("fable-exact")
        );
        assert_eq!(policy.account_for_model("grok-4.6"), Some("fallback"));
    }

    #[test]
    fn account_policy_matches_public_and_cursor_grok_namespaces() {
        let policy = CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("cursor-grok-4.6-high-fast", "high-account"),
            CursorModelAccountRule::new("cursor-grok-4.6-xhigh-fast", "xhigh-account"),
        ]);

        // A Claude Code request may use the public model id while the TUI
        // persisted the catalog spelling. The same-tier alias must select the
        // pinned account in either direction.
        assert_eq!(
            policy.account_for_model("grok-4.6-high-fast"),
            Some("high-account")
        );
        assert_eq!(
            policy.account_for_model("cursor-grok-4.6-high-fast"),
            Some("high-account")
        );
        assert_eq!(
            policy.account_for_model("grok-4.6-xhigh-fast"),
            Some("xhigh-account")
        );
        assert_eq!(
            policy.account_for_model("cursor:grok-4.6-xhigh-fast"),
            Some("xhigh-account")
        );
    }

    #[test]
    fn account_policy_bare_grok_uses_default_catalog_alias_when_needed() {
        let policy = CursorAccountRoutingPolicy::new([CursorModelAccountRule::new(
            "cursor-grok-4.6-high-fast",
            "default-account",
        )]);
        assert_eq!(
            policy.account_for_model("grok-4.6"),
            Some("default-account")
        );
        assert!(policy.matches_direct("grok-4.6"));
        assert!(policy.matches_direct("cursor:grok-4.6"));
    }

    #[test]
    fn account_policy_keeps_grok_effort_bindings_distinct() {
        let policy = CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("grok-4.6-high-fast", "high-account"),
            CursorModelAccountRule::new("grok-4.6-xhigh-fast", "xhigh-account"),
        ]);
        assert_eq!(
            policy.account_for_model("cursor-grok-4.6-high-fast"),
            Some("high-account")
        );
        assert_eq!(
            policy.account_for_model("cursor-grok-4.6-xhigh-fast"),
            Some("xhigh-account")
        );
        // A bare family has Cursor's high-fast default only. It must not
        // silently select the xhigh account when the high row is absent.
        let xhigh_only = CursorAccountRoutingPolicy::new([CursorModelAccountRule::new(
            "cursor-grok-4.6-xhigh-fast",
            "xhigh-account",
        )]);
        assert_eq!(xhigh_only.account_for_model("grok-4.6"), None);

        // Editing one tier removes its namespace/bare aliases but keeps the
        // other effort tier intact. This guards the TUI assignment path.
        let edited = policy.with_model_assignment("grok-4.6-high-fast", Some("new-high"));
        assert_eq!(
            edited.account_for_model("grok-4.6-high-fast"),
            Some("new-high")
        );
        assert_eq!(
            edited.account_for_model("grok-4.6-xhigh-fast"),
            Some("xhigh-account")
        );
    }

    #[test]
    fn account_policy_normalizes_human_readable_context_suffixes() {
        let policy = CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("claude-fable-5", "long-context"),
            CursorModelAccountRule::new("gemini-3.1-pro (2M context)", "gemini-account"),
        ]);
        assert_eq!(
            policy.account_for_model("claude-fable-5 (1M context)"),
            Some("long-context")
        );
        assert_eq!(
            policy.account_for_model("gemini-3.1-pro[2m]"),
            Some("gemini-account")
        );
    }

    #[test]
    fn account_policy_prefers_direct_alias_over_resolved_catalog_id() {
        let policy = CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("claude-fable-5-thinking-max", "catalog-account"),
            CursorModelAccountRule::new("claude-fable-5", "alias-account"),
        ]);
        assert_eq!(
            policy.account_for_model("claude-fable-5"),
            Some("alias-account")
        );
        assert_eq!(
            policy.account_for_model("claude-fable-5-thinking-max"),
            Some("catalog-account")
        );
    }

    #[test]
    fn account_policy_edit_removes_alias_equivalent_literals_but_keeps_wildcards() {
        let policy = CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("*", "fallback"),
            CursorModelAccountRule::new("gemini-*", "gemini-fallback"),
            CursorModelAccountRule::new("fable", "old-alias"),
            CursorModelAccountRule::new("claude-fable-5-thinking-max", "old-concrete"),
            CursorModelAccountRule::new("claude-fable-5-preview", "old-preview"),
        ]);

        let assigned = policy.with_model_assignment("claude-fable-5", Some("new-account"));
        assert_eq!(
            assigned.account_for_model("claude-fable-5"),
            Some("new-account")
        );
        assert!(assigned.routes().iter().any(|rule| rule.model == "*"));
        assert!(
            assigned
                .routes()
                .iter()
                .any(|rule| rule.model == "gemini-*")
        );
        assert!(!assigned.routes().iter().any(|rule| {
            matches!(
                rule.model.as_str(),
                "fable"
                    | "claude-fable-5"
                    | "claude-fable-5-thinking-max"
                    | "claude-fable-5-preview"
            ) && rule.account != "new-account"
        }));

        let cleared = assigned.with_model_assignment("claude-fable-5", None);
        assert_eq!(
            cleared.account_for_model("claude-fable-5"),
            Some("fallback")
        );
        assert!(cleared.routes().iter().any(|rule| rule.model == "*"));
        assert!(cleared.routes().iter().any(|rule| rule.model == "gemini-*"));
        assert!(!cleared.routes().iter().any(|rule| {
            matches!(
                rule.model.as_str(),
                "fable"
                    | "claude-fable-5"
                    | "claude-fable-5-thinking-max"
                    | "claude-fable-5-preview"
            )
        }));
    }

    #[test]
    fn account_policy_edit_treats_preview_and_base_as_one_literal_target() {
        let policy = CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("*", "fallback"),
            CursorModelAccountRule::new("gemini-3.1-pro-preview", "preview-account"),
        ]);

        let assigned = policy.with_model_assignment("gemini-3.1-pro", Some("base-account"));
        assert_eq!(
            assigned.account_for_model("gemini-3.1-pro-preview"),
            Some("base-account")
        );
        assert!(
            !assigned
                .routes()
                .iter()
                .any(|rule| rule.model == "gemini-3.1-pro-preview")
        );
        assert!(assigned.routes().iter().any(|rule| rule.model == "*"));

        let cleared = assigned.with_model_assignment("gemini-3.1-pro-preview", None);
        assert_eq!(
            cleared.account_for_model("gemini-3.1-pro"),
            Some("fallback")
        );
        assert!(cleared.routes().iter().any(|rule| rule.model == "*"));
    }

    #[test]
    fn account_policy_direct_match_accepts_unresolved_custom_ids() {
        let policy = CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("frontier-account-model", "work"),
            CursorModelAccountRule::new("vendor-*", "backup"),
        ]);
        assert!(policy.matches_direct("frontier-account-model"));
        assert!(policy.matches_direct("vendor-preview"));
        assert!(!policy.matches_direct("other-model"));
    }

    #[test]
    fn configured_custom_account_route_is_accepted_by_cursor_model_resolution() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"modelAccounts":{"frontier-account-model":"work"}}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        let resolved =
            crate::providers::cursor::model::resolve_cursor_model("frontier-account-model")
                .expect("an explicitly routed custom id should reach Cursor");
        assert_eq!(resolved.model_id, "frontier-account-model");
    }

    #[test]
    fn model_account_route_overrides_alias_provider_for_anthropic_alias() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"aliasProvider":"codex","cursor":{"modelAccounts":{"claude-fable-5":"work"}}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        // The explicit model→account binding is a Cursor routing signal; it
        // must not silently follow the process-wide Anthropic alias provider.
        let registry = crate::registry::Registry::new(AliasProvider::Codex);
        let provider = registry
            .provider_for_model("claude-fable-5[1m]", None)
            .expect("bound alias should route to Cursor");
        assert_eq!(provider.name(), "cursor");
    }

    #[test]
    fn concrete_model_account_route_overrides_alias_provider_for_public_fable_alias() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"aliasProvider":"codex","cursor":{"modelAccounts":{"claude-fable-5-thinking-max":"work"}}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        let registry = crate::registry::Registry::new(AliasProvider::Codex);
        let provider = registry
            .provider_for_model("claude-fable-5[1m]", None)
            .expect("concrete bound Fable tier should route alias to Cursor");
        assert_eq!(provider.name(), "cursor");
    }

    #[test]
    fn account_policy_reads_config_and_environment_override() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"modelAccounts":{"gemini-3.1-pro":"work","grok-*":"backup"}}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        let from_file = cursor_account_routing_policy();
        assert_eq!(from_file.account_for_model("gemini-3.1-pro"), Some("work"));
        assert_eq!(from_file.account_for_model("grok-4.6"), Some("backup"));

        let _route_env = EnvGuard::set("CCP_CURSOR_MODEL_ACCOUNTS", "{\"grok-4.6\":\"env\"}");
        let from_env = cursor_account_routing_policy();
        assert_eq!(from_env.account_for_model("grok-4.6"), Some("env"));
        assert_eq!(from_env.account_for_model("gemini-3.1-pro"), None);
    }

    #[test]
    fn persist_account_routes_preserves_existing_cursor_settings() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"port":18765,"cursor":{"sandModels":["gemini-3.1-pro"],"clientType":"cli"}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        persist_cursor_account_routes(&CursorAccountRoutingPolicy::new([
            CursorModelAccountRule::new("grok-4.6", "backup"),
        ]))
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(config.path().join("config.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(value["port"], 18765);
        assert_eq!(value["cursor"]["clientType"], "cli");
        assert_eq!(value["cursor"]["sandModels"][0], "gemini-3.1-pro");
        assert_eq!(value["cursor"]["modelAccounts"]["grok-4.6"], "backup");
    }

    #[test]
    fn remove_account_routes_matches_id_label_email_and_preserves_other_rules() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"modelAccounts":{"grok-*":"work","gemini-*":"backup","fable":"other"}}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        assert!(
            remove_cursor_model_account_routes_for_account(
                "account-work",
                Some("Work"),
                Some("work@example.com"),
            )
            .unwrap()
        );
        let value: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(config.path().join("config.json")).unwrap(),
        )
        .unwrap();
        assert!(value["cursor"]["modelAccounts"].get("grok-*").is_none());
        assert_eq!(value["cursor"]["modelAccounts"]["gemini-*"], "backup");
        assert_eq!(value["cursor"]["modelAccounts"]["fable"], "other");
    }

    #[test]
    fn remove_account_routes_keeps_selector_that_still_matches_remaining_account() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"modelAccounts":{"grok-*":"team*","gemini-*":"account-a"}}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let remaining = [(
            "account-b".to_string(),
            Some("Team Beta".to_string()),
            Some("beta@example.com".to_string()),
        )];

        assert!(
            remove_cursor_model_account_routes_for_account_with_remaining(
                "account-a",
                Some("Team Alpha"),
                Some("alpha@example.com"),
                |selector| remaining.iter().any(|(id, label, email)| {
                    account_selector_matches(selector, id, label.as_deref(), email.as_deref())
                }),
            )
            .unwrap(),
            "the direct account-a selector should be removed"
        );

        let policy = cursor_account_routing_policy();
        assert_eq!(policy.account_for_model("grok-4.6"), Some("team*"));
        assert_eq!(policy.account_for_model("gemini-3.1-pro"), None);
    }

    #[test]
    fn remove_account_routes_clears_selector_when_no_remaining_account_matches() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"modelAccounts":{"grok-*":"team*","gemini-*":"account-a"}}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let remaining = [(
            "account-b".to_string(),
            Some("Other Team".to_string()),
            Some("beta@example.com".to_string()),
        )];

        assert!(
            remove_cursor_model_account_routes_for_account_with_remaining(
                "account-a",
                Some("Team Alpha"),
                Some("alpha@example.com"),
                |selector| remaining.iter().any(|(id, label, email)| {
                    account_selector_matches(selector, id, label.as_deref(), email.as_deref())
                }),
            )
            .unwrap()
        );

        let policy = cursor_account_routing_policy();
        assert_eq!(policy.account_for_model("grok-4.6"), None);
        assert_eq!(policy.account_for_model("gemini-3.1-pro"), None);
    }

    #[test]
    fn remove_account_routes_does_not_write_when_environment_override_is_active() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        let path = config.path().join("config.json");
        let original = r#"{"cursor":{"modelAccounts":{"grok-*":"work"}}}"#;
        std::fs::write(&path, original).unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _route_env = EnvGuard::set("CCP_CURSOR_MODEL_ACCOUNTS", "grok-*=work");

        assert!(
            !remove_cursor_model_account_routes_for_account(
                "account-work",
                Some("Work"),
                Some("work@example.com"),
            )
            .unwrap()
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), original);
    }

    #[test]
    fn sand_route_selects_gemini_aliases_without_changing_unmatched_models() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":["gemini-3.1-pro"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "cli");

        assert_eq!(
            cursor_client_type_for_model("gemini-3.1-pro"),
            "sand",
            "exact Gemini id selected in TUI must use Sand"
        );
        assert_eq!(
            cursor_client_type_for_model("gemini-3.1-pro[1m]"),
            "sand",
            "Claude Code context suffix must not change the route"
        );
        assert_eq!(
            cursor_client_type_for_model("cursor:gemini-3.1-pro"),
            "sand",
            "Cursor prefix aliases must retain the selected route"
        );
        assert_eq!(
            cursor_client_type_for_model("gemini-3.6-flash-high"),
            "cli",
            "unmatched Gemini models must keep the normal CLI identity"
        );
    }

    #[test]
    fn sand_route_matches_gemini_preview_variant() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":["gemini-3.1-pro"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "cli");

        assert_eq!(
            cursor_client_type_for_model("gemini-3.1-pro-preview"),
            "sand",
            "the base Gemini rule must cover Cursor's preview catalog spelling"
        );
        assert_eq!(
            cursor_client_type_for_model("cursor:gemini-3.1-pro-preview"),
            "sand",
            "prefix aliases must retain the preview route"
        );
        assert_eq!(
            cursor_client_type_for_model("gemini-3.1-pro-preview-high"),
            "cli",
            "only a trailing preview marker is an alias; effort variants stay explicit"
        );
    }

    #[test]
    fn sand_route_matches_cursor_grok_namespace_at_the_selected_effort() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":["cursor-grok-4.6-xhigh-fast"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "cli");

        assert_eq!(
            cursor_client_type_for_model("cursor-grok-4.6-xhigh-fast"),
            "sand",
            "the exact Cursor catalog spelling must select the Sand lane"
        );
        assert_eq!(
            cursor_client_type_for_model("grok-4.6-xhigh-fast"),
            "sand",
            "public and Cursor Grok namespace spellings must share the Sand lane"
        );
        assert_eq!(
            cursor_client_type_for_model("cursor-grok-4.6-high-fast"),
            "cli",
            "an xhigh Sand rule must not enable the high tier"
        );
    }

    #[test]
    fn arbitrary_sand_policy_model_is_cursor_routable() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"aliasProvider":"codex","cursor":{"sandModels":["vendor-sand-*"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "cli");

        let model = "vendor-sand-2026-09";
        assert_eq!(cursor_client_type_for_model(model), "sand");

        // Sand policy entries are declarations of Cursor ownership even when
        // the id is not in a built-in family or a just-fetched catalog.
        let resolved = crate::providers::cursor::model::resolve_cursor_model(model)
            .expect("arbitrary Sand model should resolve without a hardcoded list");
        assert_eq!(resolved.model_id, model);

        let registry = crate::registry::Registry::new(AliasProvider::Codex);
        assert_eq!(
            registry
                .provider_for_model(model, None)
                .expect("Sand policy model should select Cursor")
                .name(),
            "cursor"
        );
        assert!(
            registry
                .provider_for_model("vendor-cli-only", None)
                .is_none(),
            "an unrelated opaque id must remain unknown"
        );
    }

    #[test]
    fn explicit_default_client_type_only_applies_to_unmatched_models() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":["gemini-3.1-pro"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "ide");

        assert_eq!(
            cursor_client_type_for_model("gemini-3.1-pro"),
            "sand",
            "Sand policy takes precedence for the selected model"
        );
        assert_eq!(
            cursor_client_type_for_model("gemini-3.6-flash-high"),
            "ide",
            "explicit default identity remains available for other models"
        );
    }

    #[test]
    fn catalog_warmup_fetches_cli_and_sand_when_sand_policy_is_enabled() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":["gemini-3.1-pro"]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "cli");

        assert_eq!(cursor_catalog_client_types(), vec!["cli", "sand"]);

        let _sand_default = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "SAND");
        assert_eq!(
            cursor_catalog_client_types(),
            vec!["SAND"],
            "a global Sand identity must not issue a duplicate Sand catalog request"
        );
    }

    #[test]
    fn catalog_warmup_does_not_probe_sand_without_a_sand_rule() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":[]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "ide");

        assert_eq!(cursor_catalog_client_types(), vec!["ide"]);
    }

    #[test]
    fn fable_defaults_to_cli_when_no_policy_has_been_saved() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "cli");

        assert_eq!(cursor_client_type_for_model("claude-fable-5[1m]"), "cli");
        assert_eq!(cursor_client_type_for_model("fable"), "cli");
        assert_eq!(cursor_catalog_client_types(), vec!["cli"]);
    }

    #[test]
    fn explicit_empty_sand_policy_keeps_fable_on_cli() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"cursor":{"sandModels":[]}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());
        let _client_type_env = EnvGuard::set("CCP_CURSOR_CLIENT_TYPE", "cli");

        assert_eq!(cursor_client_type_for_model("claude-fable-5[1m]"), "cli");
        assert_eq!(cursor_catalog_client_types(), vec!["cli"]);
    }

    #[test]
    fn persist_sand_policy_preserves_unknown_config_fields() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        let config = tempfile::TempDir::new().unwrap();
        std::fs::write(
            config.path().join("config.json"),
            r#"{"port":1234,"future":{"keep":true},"cursor":{"clientType":"cli"}}"#,
        )
        .unwrap();
        let _config_env = EnvGuard::set("CCP_CONFIG_DIR", config.path());

        persist_cursor_sand_policy(&SandRoutingPolicy::new(["claude-fable-*", "gpt-5.4"])).unwrap();
        let value: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(config.path().join("config.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(value["port"], 1234);
        assert_eq!(value["future"]["keep"], true);
        assert_eq!(value["cursor"]["clientType"], "cli");
        assert_eq!(
            value["cursor"]["sandModels"],
            serde_json::json!(["claude-fable-*", "gpt-5.4"])
        );
    }
}
