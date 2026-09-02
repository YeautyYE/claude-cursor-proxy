//! Cursor model catalog -- resolves incoming model names to Cursor model IDs.
//!
//! Resolution rules:
//! - `cursor:`, `cursor-plan:`, `cursor-ask:` prefixes are stripped and mapped
//!   to the corresponding agent mode.
//! - Legacy names like `cursor`, `cursor-agent`, `cursor-composer`,
//!   `cursor-composer-fast`, `cursor-plan`, `cursor-ask`, `composer-2.5`,
//!   `composer-2.5-fast` are recognized.
//! - `cursor-agent:` is also supported for agent mode routing.

use sha2::Digest;

pub const CURSOR_LEGACY_MODELS: &[&str] = &[
    "claude-opus-4-7",
    "claude-opus-4-8",
    "claude-opus-5",
    "claude-opus-5-thinking-high",
    "claude-sonnet-5",
    "cursor",
    "cursor-agent",
    "cursor-composer",
    "cursor-composer-fast",
    "cursor-plan",
    "cursor-ask",
    "composer-2.5",
    "composer-2.5-fast",
];

/// Agent mode derived from model prefix or name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorAgentMode {
    Agent,
    Plan,
    Ask,
}

impl CursorAgentMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            CursorAgentMode::Agent => "AGENT_MODE_AGENT",
            CursorAgentMode::Plan => "AGENT_MODE_PLAN",
            CursorAgentMode::Ask => "AGENT_MODE_ASK",
        }
    }

    /// Wire value for agent.v1.AgentMode (Cursor 3.12+).
    pub fn as_proto_enum(&self) -> i32 {
        match self {
            CursorAgentMode::Agent => 1, // AGENT_MODE_AGENT
            CursorAgentMode::Ask => 2,   // AGENT_MODE_ASK
            CursorAgentMode::Plan => 3,  // AGENT_MODE_PLAN
        }
    }
}

fn cursor_haiku_model_id(configured: Option<&str>) -> String {
    configured
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("claude-haiku-4-5")
        .to_string()
}

/// Apply grok-build / Anthropic `output_config.effort` onto a Cursor model id.
///
/// Bare Fable aliases otherwise always resolve to `thinking-max`. Fast in
/// grok-build is `low`.
pub fn apply_effort_to_cursor_model(model: &str, effort: Option<&str>) -> String {
    let Some(effort) = effort else {
        return model.to_string();
    };
    let stripped = strip_anthropic_context_suffix(model);
    let marker = if model.contains("[1m]") || model.contains("[1M]") {
        "[1m]"
    } else if model.contains("[2m]") || model.contains("[2M]") {
        "[2m]"
    } else {
        ""
    };
    if is_fable_family(&stripped) {
        let tier = match effort {
            "low" | "fast" | "minimal" => "claude-fable-5-thinking-low",
            "medium" => "claude-fable-5-thinking-medium",
            "high" => "claude-fable-5-thinking-high",
            "xhigh" | "max" => "claude-fable-5-thinking-max",
            _ => return model.to_string(),
        };
        return format!("{tier}{marker}");
    }
    if let Some(grok_model) = grok_effort_model(&stripped, effort) {
        // Cursor's Agent catalog uses the `cursor-grok-*` namespace for
        // effort variants.  Keep a mode prefix when the caller supplied one,
        // but canonicalize the model portion so account/Sand policies see
        // the same spelling as the live catalog and TUI.
        let prefix = ["cursor-agent:", "cursor-plan:", "cursor-ask:", "cursor:"]
            .iter()
            .find_map(|prefix| model.strip_prefix(prefix).map(|_| *prefix))
            .unwrap_or("");
        return format!("{prefix}{grok_model}");
    }
    if matches!(
        stripped.as_str(),
        "composer-2.5" | "cursor-composer" | "cursor" | "cursor-agent"
    ) && matches!(effort, "low" | "fast")
    {
        return "composer-2.5-fast".into();
    }
    model.to_string()
}

/// Map a Grok family/variant to Cursor's explicit effort slug.
///
/// Public Grok requests commonly use `grok-4.6` with the effort in
/// `output_config`; Cursor's account and Sand policies, however, are keyed by
/// concrete catalog rows such as `cursor-grok-4.6-xhigh-fast`.  Returning the
/// Cursor namespace here makes that request-scoped setting available to the
/// policy resolver without changing native Grok requests (this function is
/// only reached from the Cursor provider).
fn grok_effort_model(model: &str, effort: &str) -> Option<String> {
    let mut id = model.trim();
    for prefix in ["cursor-agent:", "cursor-plan:", "cursor-ask:", "cursor:"] {
        if let Some(rest) = id.strip_prefix(prefix) {
            id = rest.trim();
            break;
        }
    }

    let rest = id
        .strip_prefix("grok-")
        .or_else(|| id.strip_prefix("cursor-grok-"))?;
    let version_end = rest.find('-').unwrap_or(rest.len());
    let version = &rest[..version_end];
    if version.is_empty()
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return None;
    }

    let tier = match effort {
        "low" | "fast" | "minimal" => "low",
        "medium" => "medium",
        "high" => "high",
        // Grok 4.6 exposes xhigh as its top tier. `max` is accepted by the
        // shared Anthropic surface, so map it to the closest Cursor row.
        "xhigh" | "max" => "xhigh",
        _ => return None,
    };
    Some(format!("cursor-grok-{version}-{tier}-fast"))
}

/// Resolve a model id for the Desktop Sand `InferenceService` wire.
///
/// Cursor exposes two related, but distinct, model namespaces.  The CLI
/// catalog contains one id for every effort tier (for example
/// `gemini-3.6-flash-high` and `claude-fable-5-thinking-max`), while the
/// Desktop/Sand composer sends the family id and puts the selected tier in
/// `requestedModel.parameters` (for example `gemini-3.6-flash` and
/// `claude-fable-5`).  Sending a CLI tier id to InferenceService can therefore
/// result in `ERROR_BAD_MODEL_NAME` even though the same id is present in
/// `GetUsableModels`.
///
/// Keep this conversion separate from [`resolve_cursor_model`].  The latter
/// is used by AgentService and must continue to select explicit CLI tiers.
/// Unknown/custom ids are preserved byte-for-byte (apart from presentation
/// suffixes and routing prefixes), so newly added server models remain usable
/// without a proxy release.
pub fn resolve_sand_model_id(model: &str) -> String {
    let mut id = strip_anthropic_context_suffix(model.trim());

    // A model may arrive wrapped in one of the Cursor mode prefixes.  Sand's
    // InferenceService has no mode-prefixed ids; mode is selected by the
    // caller's request surface instead.
    for prefix in ["cursor-agent:", "cursor-plan:", "cursor-ask:", "cursor:"] {
        if let Some(rest) = id.strip_prefix(prefix) {
            id = rest.trim().to_string();
            break;
        }
    }
    if id.is_empty() {
        return id;
    }

    let lower = id.to_ascii_lowercase();

    // Desktop's selected model ids (verified from the current Cursor
    // renderer logs) use the short provider-family names below.  The live
    // Agent catalog, in contrast, advertises effort variants and cursor-
    // prefixed Grok ids.  Normalize only recognized variant suffixes; an
    // opaque custom id is intentionally left untouched.
    if lower == "fable" || sand_family_variant(&lower, "claude-fable-5") {
        return "claude-fable-5".to_string();
    }
    // Fable 5.1 is a separate family.  Check it before the `claude-fable-5`
    // rule above so its `-1-*` suffix is never mistaken for an effort tier.
    if sand_family_variant(&lower, "claude-fable-5-1") {
        return "claude-fable-5-1".to_string();
    }
    if sand_family_variant(&lower, "claude-opus-5") {
        return "claude-opus-5".to_string();
    }
    if sand_family_variant(&lower, "claude-sonnet-5") {
        return "claude-sonnet-5".to_string();
    }
    if sand_family_variant(&lower, "claude-opus-4-7") {
        return "claude-opus-4-7".to_string();
    }
    if sand_family_variant(&lower, "claude-opus-4-8") {
        return "claude-opus-4-8".to_string();
    }
    if sand_family_variant(&lower, "gemini-3.6-flash") {
        return "gemini-3.6-flash".to_string();
    }
    if sand_family_variant(&lower, "gemini-3.7-flash") {
        return "gemini-3.7-flash".to_string();
    }
    if sand_family_variant(&lower, "grok-4.5") || sand_family_variant(&lower, "cursor-grok-4.5") {
        return "grok-4.5".to_string();
    }
    if sand_family_variant(&lower, "grok-4.6") || sand_family_variant(&lower, "cursor-grok-4.6") {
        return "grok-4.6".to_string();
    }

    id
}

/// Return true when `id` is a known effort/transport variant of `base`.
///
/// Do not use a plain `starts_with` here: `claude-fable-5-1` is a distinct
/// model family, and arbitrary custom model ids may legitimately contain a
/// hyphenated suffix.
fn sand_family_variant(id: &str, base: &str) -> bool {
    if id == base {
        return true;
    }
    let Some(rest) = id
        .strip_prefix(base)
        .and_then(|rest| rest.strip_prefix('-'))
    else {
        return false;
    };
    let mut saw_variant = false;
    for token in rest.split('-') {
        if token.is_empty() {
            return false;
        }
        if matches!(
            token,
            "minimal" | "none" | "low" | "medium" | "high" | "xhigh" | "max" | "thinking" | "fast"
        ) {
            saw_variant = true;
        } else {
            return false;
        }
    }
    saw_variant
}

/// Resolve a model string into a (model_id, mode) pair.
///
/// Returns an error if the model is not recognized.
pub fn resolve_cursor_model(model: &str) -> Result<CursorModelResolution, String> {
    let model = strip_anthropic_context_suffix(model.trim());

    // Strip known prefixes
    if let Some(rest) = model.strip_prefix("cursor-agent:") {
        return resolve_prefixed_cursor_model(rest, CursorAgentMode::Agent);
    }
    if let Some(rest) = model.strip_prefix("cursor-plan:") {
        return resolve_prefixed_cursor_model(rest, CursorAgentMode::Plan);
    }
    if let Some(rest) = model.strip_prefix("cursor-ask:") {
        return resolve_prefixed_cursor_model(rest, CursorAgentMode::Ask);
    }
    if let Some(rest) = model.strip_prefix("cursor:") {
        return resolve_prefixed_cursor_model(rest, CursorAgentMode::Agent);
    }

    // Legacy exact names + Anthropic-style aliases.
    // Wire ids must match Cursor CLI `agent models` catalog (2026.07+), not
    // display names. Bare "cursor" is NOT a valid upstream model id.
    match model.as_str() {
        "cursor" | "cursor-agent" | "auto" => Ok(CursorModelResolution {
            // CLI default is Auto; Composer is a safe concrete Agent model.
            model_id: "composer-2.5".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        "cursor-composer" => Ok(CursorModelResolution {
            model_id: "composer-2.5".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        "cursor-composer-fast" => Ok(CursorModelResolution {
            model_id: "composer-2.5-fast".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        "cursor-plan" => Ok(CursorModelResolution {
            model_id: "composer-2.5".to_string(),
            mode: CursorAgentMode::Plan,
        }),
        "cursor-ask" => Ok(CursorModelResolution {
            model_id: "composer-2.5-fast".to_string(),
            mode: CursorAgentMode::Ask,
        }),
        // Composer is a model id under Agent mode in CLI (not Plan/Ask).
        "composer-2.5" | "composer-2.5-fast" => Ok(CursorModelResolution {
            model_id: model.to_string(),
            mode: CursorAgentMode::Agent,
        }),
        // User-selected default: fable → claude-fable-5-thinking-max
        "fable" | "claude-fable-5" => Ok(CursorModelResolution {
            model_id: "claude-fable-5-thinking-max".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        // Claude Desktop probes the Haiku alias even when another model is
        // selected. Some Cursor pools do not expose Haiku, so deployments may
        // route the probe to another usable catalog id without changing the
        // public Anthropic alias.
        "haiku" | "claude-haiku-4-5" | "claude-haiku-4-5-20251001" => Ok(CursorModelResolution {
            model_id: cursor_haiku_model_id(
                std::env::var("CCP_CURSOR_HAIKU_MODEL").ok().as_deref(),
            ),
            mode: CursorAgentMode::Agent,
        }),
        "sonnet" | "claude-sonnet-5" => Ok(CursorModelResolution {
            model_id: "claude-sonnet-5-high".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        // Claude Desktop sends the canonical Anthropic id and ignores local
        // modelOverrides when the desktop host manages the provider. Cursor's
        // catalog requires an explicit reasoning tier for Opus 5.
        "claude-opus-5" => Ok(CursorModelResolution {
            model_id: "claude-opus-5-thinking-high".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        // Cursor advertises canonical family ids for discovery, but live runs
        // require an explicit effort tier. Keep the public ids concise while
        // routing them to the verified high-effort catalog entries.
        "claude-opus-4-7" => Ok(CursorModelResolution {
            model_id: "claude-opus-4-7-high".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        "opus" | "claude-opus-4-8" => Ok(CursorModelResolution {
            model_id: "claude-opus-4-8-high".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        other => {
            // GetUsableModels is the authority for account-specific catalog
            // ids. Preserve the exact upstream spelling so newly introduced
            // model families work from the TUI and `/v1/models` without a
            // proxy release, while an account switch immediately retires the
            // previous account's ids through the scoped cache below.
            if let Some(model_id) = current_account_live_catalog_model(other) {
                return Ok(CursorModelResolution {
                    model_id,
                    mode: CursorAgentMode::Agent,
                });
            }

            // A model-account rule is an explicit request to route this id to
            // Cursor, including ids introduced by a server-side catalog that
            // has not been fetched into this process yet.  Keep the check
            // direct/non-recursive: `account_for_model` itself consults this
            // resolver while walking aliases.
            if crate::config::cursor_model_account_route_matches(other) {
                return Ok(CursorModelResolution {
                    model_id: other.to_string(),
                    mode: CursorAgentMode::Agent,
                });
            }

            // A Sand rule is also an explicit Cursor routing declaration.  A
            // managed-local catalog can introduce an otherwise opaque model
            // id before this process has refreshed `GetUsableModels`; honor
            // the configured rule without requiring a hardcoded family
            // prefix.  Use the direct matcher (rather than `matches_model`)
            // to avoid recursing through this resolver's alias walk.
            if crate::config::cursor_sand_policy().matches(other) {
                return Ok(CursorModelResolution {
                    model_id: other.to_string(),
                    mode: CursorAgentMode::Agent,
                });
            }

            // Keep known catalog families usable before the live catalog has
            // been fetched (startup/offline fallback).
            if other.starts_with("claude-")
                || other.starts_with("gpt-")
                || other.starts_with("composer-")
                || other.starts_with("gemini-")
                || other.starts_with("cursor-grok-")
                || other.starts_with("kimi-")
                || other.starts_with("glm-")
            {
                return Ok(CursorModelResolution {
                    model_id: other.to_string(),
                    mode: CursorAgentMode::Agent,
                });
            }

            Err(format!(
                "unknown cursor model: {model}. Use cursor:<id> with a current CLI catalog id (e.g. cursor:claude-fable-5-thinking-max, cursor:composer-2.5)"
            ))
        }
    }
}

fn resolve_prefixed_cursor_model(
    rest: &str,
    mode: CursorAgentMode,
) -> Result<CursorModelResolution, String> {
    let resolved = resolve_cursor_model(rest)?;
    Ok(CursorModelResolution {
        model_id: resolved.model_id,
        mode,
    })
}

/// Claude Code / ccstatusline treat bare `fable` as a ~200k window unless the
/// Anthropic-facing id carries a `[1m]` / `(1M context)` marker. Cursor's Fable 5
/// run is always long-context, so echo that marker back on the Messages wire.
///
/// Response/`message_start` ids collapse to `claude-fable-5[1m]` so the statusline
/// model display stays stable across thinking-* variants.
pub fn anthropic_wire_model(request_model: &str) -> String {
    let raw = request_model.trim();
    let base = strip_anthropic_context_suffix(raw);
    let base_ref = base.as_str();
    if is_fable_family(base_ref) || is_fable_family(raw) {
        return "claude-fable-5[1m]".to_string();
    }
    // Preserve an explicit long-context marker the client already sent.
    if raw.contains("[1m]")
        || raw.contains("[2m]")
        || raw.to_ascii_lowercase().contains("1m context")
    {
        return raw.to_string();
    }
    raw.to_string()
}

/// Id for Anthropic `/v1/models` (picker / gateway discovery).
///
/// Unlike [`anthropic_wire_model`], preserves catalog specificity
/// (`claude-fable-5-thinking-max[1m]`) so effort tiers remain selectable, while
/// always attaching `[1m]` for Claude Code `PE` when the proxy host is not
/// `api.anthropic.com`.
pub fn anthropic_list_model_id(catalog_or_alias: &str) -> String {
    let raw = catalog_or_alias.trim();
    let base = strip_anthropic_context_suffix(raw);
    if is_fable_family(&base) || is_fable_family(raw) {
        let catalog = match base.as_str() {
            "fable" | "claude-fable-5" => "claude-fable-5",
            other => other,
        };
        return format!("{catalog}[1m]");
    }
    raw.to_string()
}

fn is_fable_family(model: &str) -> bool {
    let m = model.trim();
    m == "fable"
        || m == "claude-fable-5"
        || m.starts_with("claude-fable-5-")
        || m.starts_with("cursor:claude-fable-5")
        || m.starts_with("cursor-agent:claude-fable-5")
}

/// Strip Claude Code long-context suffixes (`[1m]`, `[2m]`, `(1M context)`) so
/// Cursor upstream receives a real catalog id.
pub fn strip_anthropic_context_suffix(model: &str) -> String {
    let mut out = model.trim().to_string();
    for suffix in ["[1m]", "[2m]", "[1M]", "[2M]"] {
        if let Some(stripped) = out.strip_suffix(suffix) {
            out = stripped.trim_end().to_string();
        }
    }
    // "(1M context)" / "(1m context)" variants
    if let Some(open) = out.rfind('(')
        && out.ends_with(')')
    {
        let inner = &out[open + 1..out.len() - 1];
        let normalized = inner.to_ascii_lowercase().replace(' ', "");
        if normalized == "1mcontext" || normalized == "2mcontext" {
            out = out[..open].trim_end().to_string();
        }
    }
    out
}

#[derive(Debug, Clone)]
pub struct CursorModelResolution {
    pub model_id: String,
    pub mode: CursorAgentMode,
}

/// Map catalog id suffixes onto RequestedModel.parameters (CLI config semantics).
///
/// Keeps the full catalog `model_id` (already validated live) and additionally
/// sends `thinking` / `effort` / `context` when derivable, matching CLI
/// `cli-config.json` selectedModel.parameters.
///
/// Anthropic Messages `thinking` / `max_tokens` / `tool_choice` are **not**
/// inputs: AgentRunRequest has no such fields (see proto.rs). Overlaying
/// `thinking.enabled` here would duplicate catalog-id effort (e.g.
/// `claude-fable-5-thinking-max` already sets `thinking=true`, `effort=max`).
pub fn requested_model_parameters(
    model_id: &str,
) -> Vec<crate::providers::cursor::proto::ModelParameter> {
    use crate::providers::cursor::proto::ModelParameter;

    let mut params: Vec<ModelParameter> = Vec::new();
    let lower = model_id.to_ascii_lowercase();

    if lower.contains("thinking") {
        params.push(ModelParameter {
            id: "thinking".into(),
            value: "true".into(),
        });
    }

    let effort = if lower.contains("-xhigh") || lower.ends_with("xhigh") {
        Some("xhigh")
    } else if lower.contains("-max") || lower.ends_with("-max") || lower.contains("thinking-max") {
        Some("max")
    } else if lower.contains("-high") || lower.ends_with("-high") {
        Some("high")
    } else if lower.contains("-medium") || lower.ends_with("-medium") {
        Some("medium")
    } else if lower.contains("-low") || lower.ends_with("-low") {
        Some("low")
    } else if lower.contains("-fast") {
        Some("fast")
    } else {
        None
    };
    if let Some(effort) = effort {
        params.push(ModelParameter {
            id: "effort".into(),
            value: effort.into(),
        });
    }

    let context = std::env::var("CCP_CURSOR_CONTEXT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            if lower.contains("fable") || lower.contains("[1m]") {
                Some("1m".into())
            } else {
                None
            }
        });
    if let Some(context) = context {
        params.push(ModelParameter {
            id: "context".into(),
            value: context,
        });
    }

    params
}

/// Process-wide live catalog from `GetUsableModels` (filled by the HTTP client).
/// Merged into [`cursor_supported_models`] for listing and used by
/// [`resolve_cursor_model`] to validate exact account-specific catalog ids.
///
/// The cache is scoped by both account and Cursor request identity. Cursor
/// account switches happen in a separate CLI process while `serve` remains
/// alive; a process-wide, unkeyed catalog would otherwise keep returning the
/// previous account's model list until the TTL elapsed. Sand can expose a
/// different catalog from the CLI identity for the same account, so a CLI
/// snapshot must never satisfy a Sand lookup. We retain only a short token
/// digest, never the bearer itself.
#[derive(Debug, Clone)]
struct LiveCatalogSnapshot {
    fetched_at: std::time::Instant,
    models: Vec<String>,
}

#[derive(Debug)]
struct AccountLiveCatalog {
    /// Generation captured by in-flight fetches for this account. It changes
    /// whenever the account becomes active again after another account, so a
    /// late A response cannot overwrite a newer A -> B -> A observation.
    generation: u64,
    last_observed_at: std::time::Instant,
    /// One fresh snapshot per request identity (`cli`, `sand`, ...).
    snapshots: std::collections::BTreeMap<String, LiveCatalogSnapshot>,
}

impl AccountLiveCatalog {
    fn new(generation: u64) -> Self {
        Self {
            generation,
            last_observed_at: std::time::Instant::now(),
            snapshots: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Default)]
struct LiveCatalogCache {
    /// Account currently observed by the auth loader. Other account catalogs
    /// remain cached for fast TUI switching, but process-wide model discovery
    /// reads only this account.
    active_account_key: Option<String>,
    /// Process-wide monotonic source for per-account generations.
    generation: u64,
    /// Catalogs are keyed by a digest of the bearer, never by credential
    /// material. Each account then partitions snapshots by request identity.
    accounts: std::collections::BTreeMap<String, AccountLiveCatalog>,
    /// Compatibility snapshots written before any auth account was observed.
    /// They are listing-only and never authorize a dynamic model id.
    legacy_snapshots: std::collections::BTreeMap<String, LiveCatalogSnapshot>,
}

fn live_catalog_cache() -> &'static std::sync::Mutex<LiveCatalogCache> {
    use std::sync::{Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<LiveCatalogCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(LiveCatalogCache::default()))
}

const LIVE_CATALOG_TTL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
const LIVE_CATALOG_MAX_ACCOUNTS: usize = 64;
const LEGACY_CATALOG_IDENTITY: &str = "__legacy__";

fn live_catalog_snapshot_is_fresh(snapshot: &LiveCatalogSnapshot) -> bool {
    snapshot.fetched_at.elapsed() < LIVE_CATALOG_TTL
}

/// Drop expired snapshots and bound stale account identities. The active
/// account is retained even before its first successful fetch so generation
/// checks remain effective for the request currently in flight.
fn prune_live_catalog_cache(cache: &mut LiveCatalogCache) {
    cache
        .legacy_snapshots
        .retain(|_, snapshot| live_catalog_snapshot_is_fresh(snapshot));

    let active_account_key = cache.active_account_key.clone();
    cache.accounts.retain(|account_key, account| {
        account
            .snapshots
            .retain(|_, snapshot| live_catalog_snapshot_is_fresh(snapshot));
        !account.snapshots.is_empty() || active_account_key.as_deref() == Some(account_key.as_str())
    });

    if cache.accounts.len() <= LIVE_CATALOG_MAX_ACCOUNTS {
        return;
    }

    let mut eviction_candidates = cache
        .accounts
        .iter()
        .filter(|(account_key, _)| active_account_key.as_deref() != Some(account_key.as_str()))
        .map(|(account_key, account)| (account_key.clone(), account.last_observed_at))
        .collect::<Vec<_>>();
    eviction_candidates.sort_unstable_by_key(|(_, last_observed_at)| *last_observed_at);
    let remove_count = cache
        .accounts
        .len()
        .saturating_sub(LIVE_CATALOG_MAX_ACCOUNTS);
    for (account_key, _) in eviction_candidates.into_iter().take(remove_count) {
        cache.accounts.remove(&account_key);
    }
}

/// Normalize the part of the Cursor request identity that determines catalog
/// eligibility. Sand is case-insensitive on the wire and always canonicalized
/// to lowercase by the HTTP client; other custom client types remain
/// byte-for-byte distinct after whitespace trimming.
pub(crate) fn live_catalog_identity_key(client_type: &str) -> String {
    let client_type = client_type.trim();
    if client_type.eq_ignore_ascii_case("sand") {
        "sand".to_string()
    } else if client_type.is_empty() {
        "cli".to_string()
    } else {
        client_type.to_string()
    }
}

/// Store a freshly fetched GetUsableModels catalog (5-minute TTL).
pub fn store_live_usable_models(models: Vec<String>) {
    if let Ok(mut guard) = live_catalog_cache().lock() {
        prune_live_catalog_cache(&mut guard);
        // This legacy helper has no account identity. It is safe only before
        // the auth loader has observed an account; account-aware fetches use
        // `store_live_usable_models_for_account_at_generation` below.
        if guard.active_account_key.is_some() {
            return;
        }
        guard.legacy_snapshots.insert(
            LEGACY_CATALOG_IDENTITY.to_string(),
            LiveCatalogSnapshot {
                fetched_at: std::time::Instant::now(),
                models,
            },
        );
    }
}

/// Store a catalog only when the same account generation that began the fetch
/// is still current. The response may arrive after that account becomes
/// inactive; retaining it under its own digest is safe and lets a later TUI
/// switch reuse it. A -> B -> A transition still advances A's generation, so
/// a response from the first A observation cannot overwrite the later one.
#[allow(dead_code)]
pub(crate) fn store_live_usable_models_for_account_at_generation(
    token: &str,
    generation: u64,
    models: Vec<String>,
) {
    store_live_usable_models_for_account_and_identity_at_generation(
        token,
        &crate::config::cursor_client_type(),
        generation,
        models,
    );
}

/// Store a catalog only when both the account and request identity that began
/// the fetch still match. This prevents a CLI catalog response from replacing
/// (or being reused as) a Sand catalog for the same account.
pub(crate) fn store_live_usable_models_for_account_and_identity_at_generation(
    token: &str,
    client_type: &str,
    generation: u64,
    models: Vec<String>,
) {
    let account_key = account_catalog_key(token);
    let identity_key = live_catalog_identity_key(client_type);
    if let Ok(mut guard) = live_catalog_cache().lock() {
        prune_live_catalog_cache(&mut guard);
        let Some(account) = guard.accounts.get_mut(&account_key) else {
            return;
        };
        if account.generation != generation {
            return;
        }
        account.last_observed_at = std::time::Instant::now();
        account.snapshots.insert(
            identity_key,
            LiveCatalogSnapshot {
                fetched_at: std::time::Instant::now(),
                models,
            },
        );
    }
}

/// Mark the account whose credentials are currently active. Switching changes
/// which catalog is visible process-wide while retaining other fresh account
/// snapshots for later TUI/model-account reuse.
pub(crate) fn observe_live_usable_models_account(token: &str) -> u64 {
    let account_key = account_catalog_key(token);
    if let Ok(mut guard) = live_catalog_cache().lock() {
        prune_live_catalog_cache(&mut guard);
        let switched = guard.active_account_key.as_deref() != Some(account_key.as_str());
        if switched {
            guard.generation = guard.generation.wrapping_add(1);
            guard.active_account_key = Some(account_key.clone());
        }
        let generation = guard.generation;
        let account = guard
            .accounts
            .entry(account_key)
            .or_insert_with(|| AccountLiveCatalog::new(generation));
        if switched {
            account.generation = generation;
        }
        account.last_observed_at = std::time::Instant::now();
        let account_generation = account.generation;
        // The newly active account is protected from eviction; applying the
        // bound after insertion keeps a large saved-account registry from
        // retaining a 65th stale catalog until the next request.
        prune_live_catalog_cache(&mut guard);
        return account_generation;
    }
    // A poisoned cache is treated as a new generation. Callers still proceed
    // with the request, while the normal lock-recovery path prevents stale
    // data from being returned.
    0
}

/// Clear account identity and any catalog when authentication disappears.
pub(crate) fn clear_live_usable_models_account() {
    if let Ok(mut guard) = live_catalog_cache().lock() {
        if guard.active_account_key.is_some()
            || !guard.accounts.is_empty()
            || !guard.legacy_snapshots.is_empty()
        {
            guard.generation = guard.generation.wrapping_add(1);
        }
        guard.active_account_key = None;
        guard.accounts.clear();
        guard.legacy_snapshots.clear();
    }
}

/// Return cached live model ids if still within TTL.
pub fn cached_live_usable_models() -> Option<Vec<String>> {
    let mut guard = live_catalog_cache().lock().ok()?;
    prune_live_catalog_cache(&mut guard);
    let mut models = std::collections::BTreeSet::new();
    if let Some(active_account_key) = guard.active_account_key.as_deref() {
        if let Some(account) = guard.accounts.get(active_account_key) {
            for snapshot in account.snapshots.values() {
                models.extend(snapshot.models.iter().cloned());
            }
        }
    } else {
        for snapshot in guard.legacy_snapshots.values() {
            models.extend(snapshot.models.iter().cloned());
        }
    }
    (!models.is_empty()).then(|| models.into_iter().collect())
}

/// Resolve one exact id only from the catalog belonging to the currently
/// observed account. The legacy unkeyed cache is deliberately excluded: it
/// predates hot account switching and is useful for listing tests, but is not
/// strong enough evidence to authorize an otherwise unknown wire id.
fn current_account_live_catalog_model(model: &str) -> Option<String> {
    let mut guard = live_catalog_cache().lock().ok()?;
    prune_live_catalog_cache(&mut guard);
    let active_account_key = guard.active_account_key.as_deref()?;
    let account = guard.accounts.get(active_account_key)?;
    // Model resolution deliberately accepts ids from every current identity
    // snapshot so the picker can expose both CLI and Sand-specific entries.
    // Dispatch still chooses one request identity per model; the fetch cache
    // APIs below never substitute a CLI snapshot for a Sand request.
    account
        .snapshots
        .values()
        .flat_map(|snapshot| snapshot.models.iter())
        .find(|id| {
            // Claude Code may append a context-window marker to a model that
            // Cursor's catalog stores without it (and a few catalog responses
            // do the inverse).  Strip only that presentation suffix while
            // retaining the exact, case-sensitive upstream id otherwise.
            id.as_str() == model
                || strip_anthropic_context_suffix(id.as_str()) == model
                || strip_anthropic_context_suffix(model) == id.as_str()
        })
        .cloned()
}

/// Compatibility helper for the configured request identity. The supplied
/// token still selects the exact account snapshot, including an inactive
/// model-bound account; process-wide discovery remains active-account-only.
#[allow(dead_code)]
pub(crate) fn cached_live_usable_models_for_account(token: &str) -> Option<Vec<String>> {
    cached_live_usable_models_for_account_and_identity(token, &crate::config::cursor_client_type())
}

/// Return a cached catalog only when it was fetched under the exact account
/// and request identity. The account may be inactive: callers possess its
/// token and can use this for model-bound routing or a multi-account picker,
/// while process-wide listing remains scoped to `active_account_key`.
pub(crate) fn cached_live_usable_models_for_account_and_identity(
    token: &str,
    client_type: &str,
) -> Option<Vec<String>> {
    let mut guard = live_catalog_cache().lock().ok()?;
    prune_live_catalog_cache(&mut guard);
    let identity_key = live_catalog_identity_key(client_type);
    let account_key = account_catalog_key(token);
    let account = guard.accounts.get(&account_key)?;
    let snapshot = account.snapshots.get(&identity_key)?;
    Some(snapshot.models.clone())
}

fn account_catalog_key(token: &str) -> String {
    let digest = sha2::Sha256::digest(token.as_bytes());
    // A compact digest is sufficient for cache partitioning and avoids
    // retaining or logging bearer credentials in process state.
    format!("{digest:x}")
}

/// Build the list of supported Cursor model names.
///
/// Includes legacy aliases plus any still-fresh live catalog ids from
/// GetUsableModels. Exact ids from the current account's snapshot are also
/// accepted by [`resolve_cursor_model`].
pub fn cursor_supported_models() -> Vec<String> {
    let mut out: Vec<String> = CURSOR_LEGACY_MODELS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    if let Some(live) = cached_live_usable_models() {
        for id in live {
            if !out.iter().any(|existing| existing == &id) {
                out.push(id);
            }
        }
    }
    out.sort_unstable();
    out
}

/// Anthropic `/v1/models` surface ids.
///
/// Fable-family catalog ids are rewritten through [`anthropic_list_model_id`] so
/// Claude Code's model picker / gateway discovery always sees a `[1m]` marker
/// (needed for 1M `PE` when `ANTHROPIC_BASE_URL` is not api.anthropic.com).
/// Non-fable ids pass through unchanged.
pub fn cursor_anthropic_surface_models() -> Vec<String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    for id in cursor_supported_models() {
        let surface = anthropic_list_model_id(&id);
        if seen.insert(surface.clone()) {
            out.push(surface);
        }
    }
    // Always advertise the Fable 1M wire id even if the live catalog is empty.
    let fable_wire = anthropic_list_model_id("claude-fable-5");
    if seen.insert(fable_wire.clone()) {
        out.push(fable_wire);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // The live catalog is process-global; serialize tests that mutate it so
    // parallel test execution cannot make one account's snapshot look stale.
    fn cache_test_guard() -> std::sync::MutexGuard<'static, ()> {
        use std::sync::{Mutex, OnceLock};
        static CACHE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        CACHE_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    #[test]
    fn fable_thinking_max_gets_thinking_effort_context_params() {
        let params = requested_model_parameters("claude-fable-5-thinking-max");
        let map: std::collections::BTreeMap<_, _> =
            params.into_iter().map(|p| (p.id, p.value)).collect();
        assert_eq!(map.get("thinking").map(String::as_str), Some("true"));
        assert_eq!(map.get("effort").map(String::as_str), Some("max"));
        assert_eq!(map.get("context").map(String::as_str), Some("1m"));
    }

    #[test]
    fn non_thinking_catalog_id_does_not_invent_thinking_param() {
        // Anthropic Messages `thinking` is not an input to this function.
        // A catalog id without a thinking/effort suffix must not grow those
        // parameters (do not duplicate Anthropic generation controls here).
        let params = requested_model_parameters("composer-2.5");
        let ids: Vec<&str> = params.iter().map(|p| p.id.as_str()).collect();
        assert!(
            !ids.contains(&"thinking"),
            "composer-2.5 must not get thinking=true from a fake Anthropic overlay: {ids:?}"
        );
        assert!(
            !ids.contains(&"effort"),
            "composer-2.5 must not get an invented effort param: {ids:?}"
        );
    }

    #[test]
    fn resolve_legacy_cursor() {
        let r = resolve_cursor_model("cursor").unwrap();
        // Bare "cursor" is not a valid upstream id; map to Composer.
        assert_eq!(r.model_id, "composer-2.5");
        assert_eq!(r.mode, CursorAgentMode::Agent);
    }

    #[test]
    fn resolve_legacy_cursor_agent() {
        let r = resolve_cursor_model("cursor-agent").unwrap();
        assert_eq!(r.mode, CursorAgentMode::Agent);
    }

    #[test]
    fn resolve_legacy_cursor_plan() {
        let r = resolve_cursor_model("cursor-plan").unwrap();
        assert_eq!(r.mode, CursorAgentMode::Plan);
    }

    #[test]
    fn resolve_legacy_cursor_ask() {
        let r = resolve_cursor_model("cursor-ask").unwrap();
        assert_eq!(r.mode, CursorAgentMode::Ask);
    }

    #[test]
    fn resolve_prefixed_cursor() {
        let r = resolve_cursor_model("cursor:gpt-5.5").unwrap();
        assert_eq!(r.model_id, "gpt-5.5");
        assert_eq!(r.mode, CursorAgentMode::Agent);
    }

    #[test]
    fn resolve_prefixed_fable_alias_to_catalog_id() {
        let r = resolve_cursor_model("cursor:claude-fable-5[1m]").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");
        assert_eq!(r.mode, CursorAgentMode::Agent);

        let r = resolve_cursor_model("cursor-plan:fable[1m]").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");
        assert_eq!(r.mode, CursorAgentMode::Plan);
    }

    #[test]
    fn apply_effort_remaps_fable_alias_for_grok_build_fast() {
        assert_eq!(
            apply_effort_to_cursor_model("claude-fable-5[1m]", Some("low")),
            "claude-fable-5-thinking-low[1m]"
        );
        assert_eq!(
            apply_effort_to_cursor_model("claude-fable-5[1m]", Some("fast")),
            "claude-fable-5-thinking-low[1m]"
        );
        let resolved = resolve_cursor_model(&apply_effort_to_cursor_model(
            "claude-fable-5[1m]",
            Some("low"),
        ))
        .unwrap();
        assert_eq!(resolved.model_id, "claude-fable-5-thinking-low");
    }

    #[test]
    fn apply_effort_remaps_grok_family_to_cursor_catalog_tier() {
        let xhigh = apply_effort_to_cursor_model("grok-4.6", Some("xhigh"));
        assert_eq!(xhigh, "cursor-grok-4.6-xhigh-fast");
        assert_eq!(resolve_cursor_model(&xhigh).unwrap().model_id, xhigh);
        assert_eq!(
            apply_effort_to_cursor_model("grok-4.6", Some("high")),
            "cursor-grok-4.6-high-fast"
        );
        assert_eq!(
            apply_effort_to_cursor_model("grok-4.6", Some("medium")),
            "cursor-grok-4.6-medium-fast"
        );
        assert_eq!(
            apply_effort_to_cursor_model("grok-4.6", Some("low")),
            "cursor-grok-4.6-low-fast"
        );
        assert_eq!(
            apply_effort_to_cursor_model("grok-4.6", Some("fast")),
            "cursor-grok-4.6-low-fast"
        );
        assert_eq!(
            apply_effort_to_cursor_model("grok-4.6", Some("max")),
            "cursor-grok-4.6-xhigh-fast"
        );
    }

    #[test]
    fn apply_effort_preserves_cursor_grok_mode_prefix_and_replaces_existing_tier() {
        assert_eq!(
            apply_effort_to_cursor_model("cursor:cursor-grok-4.6-high-fast", Some("xhigh")),
            "cursor:cursor-grok-4.6-xhigh-fast"
        );
        assert_eq!(
            apply_effort_to_cursor_model("cursor-plan:grok-4.6", Some("high")),
            "cursor-plan:cursor-grok-4.6-high-fast"
        );
        assert_eq!(
            apply_effort_to_cursor_model("cursor-grok-4.6-xhigh-fast", Some("low")),
            "cursor-grok-4.6-low-fast"
        );
    }

    #[test]
    fn sand_resolver_uses_desktop_family_ids() {
        let cases = [
            ("fable", "claude-fable-5"),
            ("claude-fable-5[1m]", "claude-fable-5"),
            ("claude-fable-5-thinking-max", "claude-fable-5"),
            ("claude-fable-5-thinking-low[1m]", "claude-fable-5"),
            ("gemini-3.6-flash-high", "gemini-3.6-flash"),
            ("gemini-3.6-flash-medium", "gemini-3.6-flash"),
            ("cursor:grok-4.6-high-fast", "grok-4.6"),
            ("cursor-grok-4.5-high", "grok-4.5"),
            ("claude-opus-5-thinking-high", "claude-opus-5"),
            ("claude-sonnet-5-max", "claude-sonnet-5"),
        ];
        for (input, expected) in cases {
            assert_eq!(
                resolve_sand_model_id(input),
                expected,
                "unexpected Sand id for {input}"
            );
        }
    }

    #[test]
    fn sand_resolver_preserves_unknown_and_distinct_families() {
        assert_eq!(
            resolve_sand_model_id("vendor.custom-model-v2"),
            "vendor.custom-model-v2"
        );
        // Fable 5.1 must not collapse into the Fable 5 family.
        assert_eq!(
            resolve_sand_model_id("claude-fable-5-1-thinking-max"),
            "claude-fable-5-1"
        );
        assert_eq!(
            resolve_sand_model_id("composer-2.5-fast"),
            "composer-2.5-fast"
        );
    }

    #[test]
    fn resolve_anthropic_aliases_for_cursor_alias_provider() {
        assert_eq!(cursor_haiku_model_id(None), "claude-haiku-4-5");
        assert_eq!(
            cursor_haiku_model_id(Some(" claude-opus-5-thinking-high ")),
            "claude-opus-5-thinking-high"
        );

        let r = resolve_cursor_model("fable").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");
        assert_eq!(r.mode, CursorAgentMode::Agent);

        let r = resolve_cursor_model("claude-fable-5").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");

        let r = resolve_cursor_model("claude-fable-5-thinking-high").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-high");

        let r = resolve_cursor_model("haiku").unwrap();
        assert_eq!(r.model_id, "claude-haiku-4-5");

        let r = resolve_cursor_model("claude-haiku-4-5").unwrap();
        assert_eq!(r.model_id, "claude-haiku-4-5");

        let r = resolve_cursor_model("claude-opus-5").unwrap();
        assert_eq!(r.model_id, "claude-opus-5-thinking-high");

        let r = resolve_cursor_model("claude-opus-4-7").unwrap();
        assert_eq!(r.model_id, "claude-opus-4-7-high");

        let r = resolve_cursor_model("claude-opus-4-8").unwrap();
        assert_eq!(r.model_id, "claude-opus-4-8-high");
    }

    #[test]
    fn strips_1m_suffix_before_cursor_resolution() {
        let r = resolve_cursor_model("claude-fable-5[1m]").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");
        let r = resolve_cursor_model("fable[1m]").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");
        let r = resolve_cursor_model("claude-fable-5-thinking-max[1m]").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");
    }

    #[test]
    fn anthropic_wire_model_marks_fable_as_1m() {
        assert_eq!(anthropic_wire_model("fable"), "claude-fable-5[1m]");
        assert_eq!(anthropic_wire_model("claude-fable-5"), "claude-fable-5[1m]");
        assert_eq!(
            anthropic_wire_model("claude-fable-5-thinking-max"),
            "claude-fable-5[1m]"
        );
        assert_eq!(
            anthropic_wire_model("claude-fable-5[1m]"),
            "claude-fable-5[1m]"
        );
        assert_eq!(
            anthropic_wire_model("claude-fable-5-thinking-high[1m]"),
            "claude-fable-5[1m]"
        );
        assert_eq!(anthropic_wire_model("composer-2.5"), "composer-2.5");
    }

    #[test]
    fn anthropic_list_model_id_keeps_effort_tier_with_1m() {
        assert_eq!(anthropic_list_model_id("fable"), "claude-fable-5[1m]");
        assert_eq!(
            anthropic_list_model_id("claude-fable-5"),
            "claude-fable-5[1m]"
        );
        assert_eq!(
            anthropic_list_model_id("claude-fable-5-thinking-max"),
            "claude-fable-5-thinking-max[1m]"
        );
        assert_eq!(
            anthropic_list_model_id("claude-fable-5-thinking-high[1m]"),
            "claude-fable-5-thinking-high[1m]"
        );
        assert_eq!(anthropic_list_model_id("composer-2.5"), "composer-2.5");
    }

    #[test]
    fn anthropic_surface_models_advertise_fable_1m() {
        let models = cursor_anthropic_surface_models();
        assert!(models.iter().any(|m| m == "claude-opus-5"));
        assert!(models.iter().any(|m| m == "claude-opus-5-thinking-high"));
        assert!(
            models.iter().any(|m| m == "claude-fable-5[1m]"),
            "missing claude-fable-5[1m] in {models:?}"
        );
        // Bare fable catalog ids must not appear without the wire marker —
        // Claude Code gateway PE falls back to 200k without `[1m]`.
        assert!(!models.iter().any(|m| {
            let lower = m.to_ascii_lowercase();
            (lower.contains("fable") || lower == "claude-fable-5") && !lower.contains("[1m]")
        }));
    }

    #[test]
    fn wire_and_list_ids_round_trip_through_resolve() {
        for listed in [
            "claude-fable-5[1m]",
            "claude-fable-5-thinking-max[1m]",
            "claude-fable-5-thinking-high[1m]",
        ] {
            let resolved = resolve_cursor_model(listed).unwrap();
            assert!(
                resolved.model_id.starts_with("claude-fable-5"),
                "{listed} → {}",
                resolved.model_id
            );
            assert_eq!(anthropic_wire_model(listed), "claude-fable-5[1m]");
        }
    }

    #[test]
    fn resolve_composer_as_agent_model_id() {
        let r = resolve_cursor_model("composer-2.5").unwrap();
        assert_eq!(r.model_id, "composer-2.5");
        assert_eq!(r.mode, CursorAgentMode::Agent);
    }

    #[test]
    fn resolve_prefixed_cursor_plan() {
        let r = resolve_cursor_model("cursor-plan:gpt-5.5").unwrap();
        assert_eq!(r.model_id, "gpt-5.5");
        assert_eq!(r.mode, CursorAgentMode::Plan);
    }

    #[test]
    fn resolve_prefixed_cursor_ask() {
        let r = resolve_cursor_model("cursor-ask:gpt-5.5").unwrap();
        assert_eq!(r.model_id, "gpt-5.5");
        assert_eq!(r.mode, CursorAgentMode::Ask);
    }

    #[test]
    fn resolve_prefixed_cursor_agent() {
        let r = resolve_cursor_model("cursor-agent:gpt-5.5").unwrap();
        assert_eq!(r.model_id, "gpt-5.5");
        assert_eq!(r.mode, CursorAgentMode::Agent);
    }

    #[test]
    fn resolve_unknown_model_errors() {
        let r = resolve_cursor_model("unknown-model");
        assert!(r.is_err());
    }

    #[test]
    fn resolve_composer_models() {
        let r = resolve_cursor_model("composer-2.5").unwrap();
        assert_eq!(r.model_id, "composer-2.5");
        assert_eq!(r.mode, CursorAgentMode::Agent);

        let r = resolve_cursor_model("composer-2.5-fast").unwrap();
        assert_eq!(r.model_id, "composer-2.5-fast");
        assert_eq!(r.mode, CursorAgentMode::Agent);
    }

    #[test]
    fn supported_models_includes_all_legacy() {
        let models = cursor_supported_models();
        for m in CURSOR_LEGACY_MODELS {
            assert!(models.contains(&m.to_string()), "missing {m}");
        }
    }

    #[test]
    fn live_catalog_cache_does_not_cross_account_switches() {
        let _guard = cache_test_guard();

        clear_live_usable_models_account();
        let account_a_generation = observe_live_usable_models_account("account-a-token");
        store_live_usable_models_for_account_at_generation(
            "account-a-token",
            account_a_generation,
            vec!["gemini-a".into()],
        );
        assert_eq!(
            cached_live_usable_models_for_account("account-a-token"),
            Some(vec!["gemini-a".into()])
        );

        let account_b_generation = observe_live_usable_models_account("account-b-token");
        assert!(
            cached_live_usable_models_for_account("account-b-token").is_none(),
            "switching accounts must retire the previous snapshot before refetch"
        );
        assert!(
            cached_live_usable_models().is_none(),
            "unkeyed listing must not expose the previous account's catalog"
        );

        // A response started under account A can finish after the switch; it
        // must not repopulate the account-B cache.
        store_live_usable_models_for_account_at_generation(
            "account-a-token",
            account_a_generation,
            vec!["stale-a".into()],
        );
        assert!(cached_live_usable_models().is_none());

        store_live_usable_models_for_account_at_generation(
            "account-b-token",
            account_b_generation,
            vec!["gemini-b".into()],
        );
        assert_eq!(
            cached_live_usable_models_for_account("account-b-token"),
            Some(vec!["gemini-b".into()])
        );
        assert_eq!(cached_live_usable_models(), Some(vec!["gemini-b".into()]));
        clear_live_usable_models_account();
    }

    #[test]
    fn live_catalog_cache_is_partitioned_by_request_identity() {
        let _guard = cache_test_guard();

        clear_live_usable_models_account();
        let generation = observe_live_usable_models_account("account-a-token");
        store_live_usable_models_for_account_and_identity_at_generation(
            "account-a-token",
            "cli",
            generation,
            vec!["cli-only-model".into(), "shared-model".into()],
        );
        store_live_usable_models_for_account_and_identity_at_generation(
            "account-a-token",
            " SAND ",
            generation,
            vec!["sand-only-model".into(), "shared-model".into()],
        );

        assert_eq!(
            cached_live_usable_models_for_account_and_identity("account-a-token", "cli"),
            Some(vec!["cli-only-model".into(), "shared-model".into()]),
            "the CLI lookup must not consume Sand's catalog"
        );
        assert_eq!(
            cached_live_usable_models_for_account_and_identity("account-a-token", "sand"),
            Some(vec!["sand-only-model".into(), "shared-model".into()]),
            "the Sand lookup must not consume the CLI catalog"
        );
        assert_eq!(
            cached_live_usable_models_for_account_and_identity("account-a-token", "SAND"),
            Some(vec!["sand-only-model".into(), "shared-model".into()]),
            "Sand identity matching must follow its lowercase wire spelling"
        );

        assert_eq!(
            cached_live_usable_models(),
            Some(vec![
                "cli-only-model".into(),
                "sand-only-model".into(),
                "shared-model".into(),
            ]),
            "the model picker may expose the union, while dispatch remains partitioned"
        );
        clear_live_usable_models_account();
    }

    #[test]
    fn live_catalog_cache_retains_inactive_accounts_without_exposing_them() {
        let _guard = cache_test_guard();

        clear_live_usable_models_account();
        let generation_a = observe_live_usable_models_account("account-a-token");
        store_live_usable_models_for_account_and_identity_at_generation(
            "account-a-token",
            "sand",
            generation_a,
            vec!["sand-a-only".into()],
        );

        let generation_b = observe_live_usable_models_account("account-b-token");
        store_live_usable_models_for_account_and_identity_at_generation(
            "account-b-token",
            "sand",
            generation_b,
            vec!["sand-b-only".into()],
        );

        assert_eq!(
            cached_live_usable_models_for_account_and_identity("account-a-token", "sand"),
            Some(vec!["sand-a-only".into()]),
            "an inactive model-bound account keeps its own fresh Sand catalog"
        );
        assert_eq!(
            cached_live_usable_models_for_account_and_identity("account-b-token", "sand"),
            Some(vec!["sand-b-only".into()])
        );
        assert_eq!(
            cached_live_usable_models(),
            Some(vec!["sand-b-only".into()]),
            "global discovery must expose only the active account"
        );
        assert!(
            resolve_cursor_model("sand-a-only").is_err(),
            "an inactive account catalog must not authorize an active request"
        );

        let generation_a_again = observe_live_usable_models_account("account-a-token");
        assert_ne!(generation_a, generation_a_again);
        assert_eq!(
            cached_live_usable_models(),
            Some(vec!["sand-a-only".into()]),
            "switching back may reuse the exact account catalog"
        );
        store_live_usable_models_for_account_and_identity_at_generation(
            "account-a-token",
            "sand",
            generation_a,
            vec!["stale-a".into()],
        );
        assert_eq!(
            cached_live_usable_models(),
            Some(vec!["sand-a-only".into()]),
            "a pre-switch A request cannot overwrite the newer A generation"
        );
        clear_live_usable_models_account();
    }

    #[test]
    fn current_account_live_catalog_ids_resolve_exactly_with_all_modes() {
        let _guard = cache_test_guard();

        clear_live_usable_models_account();
        let generation = observe_live_usable_models_account("account-a-token");
        store_live_usable_models_for_account_at_generation(
            "account-a-token",
            generation,
            vec!["frontier-account-model".into()],
        );

        for (requested, expected_mode) in [
            ("frontier-account-model", CursorAgentMode::Agent),
            ("cursor:frontier-account-model", CursorAgentMode::Agent),
            (
                "cursor-agent:frontier-account-model",
                CursorAgentMode::Agent,
            ),
            ("cursor-plan:frontier-account-model", CursorAgentMode::Plan),
            ("cursor-ask:frontier-account-model", CursorAgentMode::Ask),
        ] {
            let resolved = resolve_cursor_model(requested).unwrap();
            assert_eq!(resolved.model_id, "frontier-account-model");
            assert_eq!(resolved.mode, expected_mode);
        }

        assert!(
            resolve_cursor_model("Frontier-Account-Model").is_err(),
            "live catalog ids must retain Cursor's exact case-sensitive spelling"
        );

        observe_live_usable_models_account("account-b-token");
        assert!(
            resolve_cursor_model("frontier-account-model").is_err(),
            "switching accounts must immediately retire account A's dynamic ids"
        );
        clear_live_usable_models_account();
    }

    #[test]
    fn arbitrary_sand_catalog_id_routes_through_registry() {
        let _guard = cache_test_guard();

        clear_live_usable_models_account();
        let generation = observe_live_usable_models_account("sand-catalog-token");
        store_live_usable_models_for_account_and_identity_at_generation(
            "sand-catalog-token",
            "sand",
            generation,
            vec!["vendor.sand.model.v9".into()],
        );

        let registry = crate::registry::Registry::new(crate::config::AliasProvider::Codex);
        assert_eq!(
            registry
                .provider_for_model("vendor.sand.model.v9", None)
                .expect("server-discovered Sand id should select Cursor")
                .name(),
            "cursor"
        );
        assert_eq!(
            resolve_cursor_model("vendor.sand.model.v9")
                .expect("server-discovered Sand id should resolve")
                .model_id,
            "vendor.sand.model.v9"
        );
        clear_live_usable_models_account();
    }

    #[test]
    fn legacy_unkeyed_catalog_does_not_authorize_unknown_model_ids() {
        let _guard = cache_test_guard();

        clear_live_usable_models_account();
        store_live_usable_models(vec!["legacy-frontier-model".into()]);
        assert!(
            cursor_supported_models().contains(&"legacy-frontier-model".to_string()),
            "the legacy helper remains available to listing-only callers"
        );
        assert!(
            resolve_cursor_model("legacy-frontier-model").is_err(),
            "an unkeyed catalog must not authorize an otherwise unknown wire id"
        );
        clear_live_usable_models_account();
    }

    #[test]
    fn stale_catalog_completion_is_rejected_after_account_switch_back() {
        let _guard = cache_test_guard();

        clear_live_usable_models_account();
        let generation_a = observe_live_usable_models_account("account-a-token");
        let _generation_b = observe_live_usable_models_account("account-b-token");
        let generation_a_again = observe_live_usable_models_account("account-a-token");
        assert_ne!(
            generation_a, generation_a_again,
            "each account identity transition must advance the cache generation"
        );

        // The first A request completed after A -> B -> A. A key-only check
        // would incorrectly accept this response; the captured generation
        // must reject it.
        store_live_usable_models_for_account_at_generation(
            "account-a-token",
            generation_a,
            vec!["stale-a".into()],
        );
        assert!(cached_live_usable_models().is_none());

        store_live_usable_models_for_account_at_generation(
            "account-a-token",
            generation_a_again,
            vec!["fresh-a".into()],
        );
        assert_eq!(cached_live_usable_models(), Some(vec!["fresh-a".into()]));
        clear_live_usable_models_account();
    }

    #[test]
    fn unkeyed_catalog_is_not_visible_after_account_observation() {
        let _guard = cache_test_guard();

        clear_live_usable_models_account();
        store_live_usable_models(vec!["legacy-model".into()]);
        assert_eq!(
            cached_live_usable_models(),
            Some(vec!["legacy-model".into()])
        );
        observe_live_usable_models_account("account-a-token");
        assert!(cached_live_usable_models().is_none());
        clear_live_usable_models_account();
    }

    #[test]
    fn resolve_gemini_31_pro_as_cursor_catalog_model() {
        let resolved = resolve_cursor_model("gemini-3.1-pro").unwrap();
        assert_eq!(resolved.model_id, "gemini-3.1-pro");
        assert_eq!(resolved.mode, CursorAgentMode::Agent);
    }
}
