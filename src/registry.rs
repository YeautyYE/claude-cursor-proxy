use crate::{
    anthropic::{json_error, schema::MessagesRequest},
    config::AliasProvider,
    provider::{CliHandlers, Provider, RequestContext},
};
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use axum::{http::StatusCode, response::Response};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

pub const ANTHROPIC_STYLE_ALIASES: &[&str] = &[
    "haiku",
    "claude-haiku-4-5",
    "claude-haiku-4-5-20251001",
    "sonnet",
    "claude-sonnet-4-6",
    "claude-sonnet-5",
    "opus",
    "claude-opus-5",
    "claude-opus-5-thinking-high",
    "claude-opus-4-7",
    "claude-opus-4-8",
    "fable",
    "claude-fable-5",
];

pub const CURSOR_PREFIXES: &[&str] = &["cursor:", "cursor-plan:", "cursor-ask:"];

pub(crate) const CODEX_MODELS: &[&str] = &[
    "gpt-5.2",
    "gpt-5.3-codex",
    "gpt-5.3-codex-spark",
    "gpt-5.4",
    "gpt-5.4-mini",
    "gpt-5.5",
    "gpt-5.6-luna",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
];

pub(crate) const KIMI_MODELS: &[&str] = &["kimi-for-coding", "kimi-k2.6", "k2.6"];
pub(crate) const GROK_MODELS: &[&str] = &["grok-composer-2.5-fast", "grok-4.5", "grok-4.6"];

pub struct Registry {
    alias_provider: AliasProvider,
    models: BTreeMap<String, Vec<String>>,
    handlers: BTreeMap<String, Arc<dyn Provider>>,
}

impl Registry {
    pub fn new(alias_provider: AliasProvider) -> Self {
        let mut models: BTreeMap<String, Vec<String>> = BTreeMap::new();
        models.insert("codex".into(), expand_codex_models());
        models.insert(
            "kimi".into(),
            KIMI_MODELS.iter().map(|m| (*m).to_string()).collect(),
        );
        models.insert("cursor".into(), build_cursor_models());
        models.insert(
            "grok".into(),
            GROK_MODELS
                .iter()
                .map(|model| (*model).to_string())
                .collect(),
        );

        let mut handlers = BTreeMap::new();
        for (name, entries) in &models {
            let handler: Arc<dyn Provider> = match name.as_str() {
                "codex" => Arc::new(crate::providers::codex::CodexProvider::new()),
                "kimi" => Arc::new(crate::providers::kimi::KimiProvider::new()),
                "cursor" => Arc::new(crate::providers::cursor::CursorProvider::new()),
                "grok" => Arc::new(crate::providers::grok::GrokProvider::new()),
                _ => Arc::new(PlaceholderProvider::new(name, entries.clone())),
            };
            handlers.insert(name.clone(), handler);
        }

        Self {
            alias_provider,
            models,
            handlers,
        }
    }

    pub fn with_default_alias() -> Self {
        Self::new(crate::config::alias_provider())
    }

    pub fn list_provider_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.handlers.keys().cloned().collect();
        names.sort_unstable();
        names
    }

    pub fn provider(&self, name: &str) -> Option<Arc<dyn Provider>> {
        self.handlers.get(name).cloned()
    }

    pub fn supported_models_for(&self, provider: &str) -> Vec<String> {
        let mut models = self.models.get(provider).cloned().unwrap_or_default();
        if provider == self.alias_provider.as_str() {
            for alias in ANTHROPIC_STYLE_ALIASES {
                if !models.iter().any(|value| value == alias) {
                    models.push((*alias).to_string());
                }
            }
        }
        models.sort_unstable();
        models
    }

    pub fn all_supported_models(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for provider in self.handlers.keys() {
            for model in self.supported_models_for(provider) {
                out.push((model, provider.clone()));
            }
        }
        out
    }

    pub fn grouped_models(&self) -> BTreeMap<String, Vec<String>> {
        let mut out = BTreeMap::new();
        for provider in self.handlers.keys() {
            out.insert(provider.clone(), self.supported_models_for(provider));
        }
        out
    }

    pub fn provider_for_model(
        &self,
        raw_model: &str,
        session_affinity: Option<&AliasProvider>,
    ) -> Option<Arc<dyn Provider>> {
        // Read both routing policies once per lookup.  Besides avoiding two
        // independent config-file reads during a TUI edit, passing the
        // snapshots into the resolver keeps provider classification
        // deterministic when the file is atomically replaced between calls.
        let account_routes = crate::config::cursor_account_routing_policy();
        let sand_policy = crate::config::cursor_sand_policy();
        self.provider_for_model_with_policies(
            raw_model,
            session_affinity,
            &account_routes,
            &sand_policy,
        )
    }

    /// Resolve a request whose effort setting can select a concrete Cursor
    /// catalog tier.  Provider selection happens before the Cursor provider
    /// gets the parsed Messages body, so looking only at a bare `grok-4.6`
    /// would incorrectly send `effort=xhigh` to the native Grok backend.
    ///
    /// The effective tier is promoted only when an explicit Cursor account or
    /// Sand rule matches it (or the request already carries a Cursor prefix).
    /// An unbound public Grok request therefore keeps its historical native
    /// provider and quota lane.
    pub fn resolve_model_for_request(
        &self,
        raw_model: &str,
        effort: Option<&str>,
        session_affinity: Option<&AliasProvider>,
    ) -> Option<(String, Arc<dyn Provider>)> {
        let account_routes = crate::config::cursor_account_routing_policy();
        let sand_policy = crate::config::cursor_sand_policy();
        self.resolve_model_for_request_with_policies(
            raw_model,
            effort,
            session_affinity,
            &account_routes,
            &sand_policy,
        )
    }

    fn resolve_model_for_request_with_policies(
        &self,
        raw_model: &str,
        effort: Option<&str>,
        session_affinity: Option<&AliasProvider>,
        account_routes: &crate::config::CursorAccountRoutingPolicy,
        sand_policy: &crate::config::SandRoutingPolicy,
    ) -> Option<(String, Arc<dyn Provider>)> {
        let normalized = normalize_incoming_model(raw_model);
        let effective =
            crate::providers::cursor::model::apply_effort_to_cursor_model(&normalized, effort);

        // Keep the ordinary lookup as the fallback. This is important for
        // native Grok: a model without a matching Cursor policy must not be
        // promoted merely because its effort maps to a known Cursor slug.
        let fallback = || {
            self.provider_for_model_with_policies(
                &normalized,
                session_affinity,
                account_routes,
                sand_policy,
            )
            .map(|provider| (normalized.clone(), provider))
        };

        if effective == normalized {
            return fallback();
        }

        let explicit_cursor_route = is_cursor_model(&normalized)
            || account_routes.account_for_model(&effective).is_some()
            || sand_policy.matches_model(&effective);
        if explicit_cursor_route
            && let Some(provider) = self.provider_for_model_with_policies(
                &effective,
                session_affinity,
                account_routes,
                sand_policy,
            )
            && provider.name() == "cursor"
        {
            return Some((effective, provider));
        }

        let fallback = fallback();
        // A bare Grok family has a compatibility alias to Cursor's default
        // `high-fast` row.  When the request explicitly asks for another
        // effort, that compatibility alias is a tier mismatch rather than a
        // valid Cursor route.  Fall back to the native Grok provider instead
        // of consuming the wrong Cursor account allowance.  An explicit
        // `cursor:` request remains Cursor-bound by design.
        if is_grok_model_namespace(&normalized)
            && !is_cursor_model(&normalized)
            && fallback
                .as_ref()
                .is_some_and(|provider| provider.1.name() == "cursor")
        {
            return self
                .provider_for_model_without_account_routes_normalized(&normalized, session_affinity)
                .map(|provider| (normalized, provider));
        }
        fallback
    }

    /// Resolve a model with caller-supplied Cursor routing snapshots.
    ///
    /// The public [`provider_for_model`] method obtains these snapshots from
    /// the process configuration.  Keeping the policy-aware part separate
    /// makes the precedence contract testable without mutating process-global
    /// environment variables (and avoids a race with parallel tests).
    fn provider_for_model_with_policies(
        &self,
        raw_model: &str,
        session_affinity: Option<&AliasProvider>,
        account_routes: &crate::config::CursorAccountRoutingPolicy,
        sand_policy: &crate::config::SandRoutingPolicy,
    ) -> Option<Arc<dyn Provider>> {
        let normalized = normalize_incoming_model(raw_model);
        // A model-account assignment is an explicit request to use Cursor for
        // that model, even when the same Anthropic-style alias would normally
        // follow the process-wide `aliasProvider` (for example `fable` with
        // `aliasProvider: codex`).  Account routing is intentionally checked
        // before the alias branch so the TUI's model→account binding cannot
        // silently send the request to another provider.  Keep the known
        // provider table below authoritative for concrete ids such as
        // `gpt-5.5`; users can still force those through Cursor with the
        // explicit `cursor:` prefix.
        if is_anthropic_alias(&normalized) {
            // Check the full alias/resolution chain as well as a direct rule.
            // The TUI commonly persists a concrete Fable tier (for example
            // `claude-fable-5-thinking-max`) while Claude Code sends the
            // public `claude-fable-5[1m]` alias.  A direct-only check would
            // let `aliasProvider: codex` steal that request before the
            // account-bound Cursor route had a chance to select its token.
            // Sand is another explicit Cursor ownership declaration. Check
            // it before the process-wide Anthropic alias provider so a
            // configured Sand model rule (including a TUI-selected alias)
            // reaches Cursor's InferenceService transport.
            if sand_policy.matches_model(&normalized)
                || account_routes.account_for_model(&normalized).is_some()
            {
                return self.handlers.get("cursor").cloned();
            }
        }
        // `grok-4.5`/`grok-4.6` are also exposed by the native Grok
        // provider.  An explicit `cursor.modelAccounts` binding is the
        // unambiguous opt-in for Cursor's account-backed Grok catalog, so it
        // must be checked before the static Grok table.  Keep this exception
        // scoped to the Grok namespaces: a broad account rule must not steal
        // a native Codex/Kimi id, and an unbound public Grok id keeps its
        // historical Grok provider route.
        if self.handlers.contains_key("cursor")
            && is_grok_model_namespace(&normalized)
            && account_routes.account_for_model(&normalized).is_some()
        {
            return self.handlers.get("cursor").cloned();
        }
        // Keep the static/provider catalog authoritative for all non-Grok
        // ids (for example `gpt-5.5` remains Codex even if someone adds a
        // broad account rule).  Grok has one intentional exception above:
        // an explicit model→account route opts into Cursor's account-backed
        // Grok catalog.  If no provider knows an id yet, an explicit route
        // is also a declaration that the id belongs to Cursor.  This is
        // important for newly added server-side catalog ids and for the
        // TUI's manual `a` entry: they must work before the next
        // `GetUsableModels` refresh.
        if let Some(provider) =
            self.provider_for_model_without_account_routes_normalized(&normalized, session_affinity)
        {
            return Some(provider);
        }
        if self.handlers.contains_key("cursor") && account_routes.matches_direct(&normalized) {
            return self.handlers.get("cursor").cloned();
        }
        // Sand policy entries are provider declarations too.  Keep this after
        // the static/live catalog lookup so an overlapping concrete id such
        // as `gpt-5.5` remains owned by Codex; only otherwise-unclaimed ids
        // are promoted to Cursor.  The direct matcher supports arbitrary
        // server model ids and wildcards without a hardcoded model list.
        if self.handlers.contains_key("cursor") && sand_policy.matches(&normalized) {
            return self.handlers.get("cursor").cloned();
        }
        None
    }

    /// Resolve a model using only the static/live provider catalog and alias
    /// affinity. This deliberately omits `modelAccounts` so callers that are
    /// inspecting a separate policy (such as `/v1/models` discovery tests)
    /// can classify route keys without relying on process-global config.
    pub(crate) fn provider_for_model_without_account_routes(
        &self,
        raw_model: &str,
        session_affinity: Option<&AliasProvider>,
    ) -> Option<Arc<dyn Provider>> {
        let normalized = normalize_incoming_model(raw_model);
        self.provider_for_model_without_account_routes_normalized(&normalized, session_affinity)
    }

    fn provider_for_model_without_account_routes_normalized(
        &self,
        normalized: &str,
        session_affinity: Option<&AliasProvider>,
    ) -> Option<Arc<dyn Provider>> {
        if is_anthropic_alias(normalized) {
            let target = session_affinity.unwrap_or(&self.alias_provider);
            return self.handlers.get(target.as_str()).cloned();
        }
        if is_cursor_model(normalized) {
            return self.handlers.get("cursor").cloned();
        }

        for (name, models) in &self.models {
            if models.iter().any(|candidate| candidate == normalized) {
                return self.handlers.get(name).cloned();
            }
        }

        // `/v1/models` lists live Cursor catalog ids (e.g. claude-fable-5-thinking-high[1m])
        // that are not in the static registry. Route those to Cursor only when no other
        // provider already claimed the exact id (gpt-5.5 stays Codex).
        if self.handlers.contains_key("cursor") && is_unclaimed_cursor_catalog_id(normalized) {
            return self.handlers.get("cursor").cloned();
        }

        None
    }

    pub fn unknown_model_message(&self) -> String {
        let mut parts = Vec::new();
        for (provider, models) in self.grouped_models() {
            let mut models = models;
            models.sort_unstable();
            parts.push(format!("{}: {}", provider, models.join(", ")));
        }
        format!("Supported: {}.", parts.join("; "))
    }
}

/// Return whether a normalized request belongs to one of Cursor's Grok model
/// namespaces.  The public registry and xAI clients use `grok-*`, while
/// Cursor's live catalog uses `cursor-grok-*`; mode prefixes are routing
/// controls and are ignored for this classification.
fn is_grok_model_namespace(model: &str) -> bool {
    let mut id = model.trim();
    for prefix in ["cursor-agent:", "cursor-plan:", "cursor-ask:", "cursor:"] {
        if let Some(rest) = id.strip_prefix(prefix) {
            id = rest.trim();
            break;
        }
    }
    id.starts_with("grok-") || id.starts_with("cursor-grok-")
}

pub fn normalize_incoming_model(model: &str) -> String {
    // Claude Code appends a context-window marker to the model id.  The
    // registry must remove every supported spelling before checking the
    // provider catalog; otherwise a harmless `[2m]`/`(1M context)` suffix can
    // make a known Codex model look like an unclaimed Cursor id.  Keep this
    // helper deliberately narrower than `config::normalize_sand_model`: the
    // `cursor:` prefixes are routing controls and must remain available to
    // `is_cursor_model` below.
    let mut normalized = model.trim().to_string();
    for suffix in ["[1m]", "[2m]"] {
        if normalized.len() >= suffix.len()
            && normalized[normalized.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
        {
            normalized.truncate(normalized.len() - suffix.len());
            normalized = normalized.trim_end().to_string();
            break;
        }
    }
    if let Some(open) = normalized.rfind('(')
        && normalized.ends_with(')')
    {
        let inner = normalized[open + 1..normalized.len() - 1]
            .chars()
            .filter(|ch| !ch.is_ascii_whitespace())
            .collect::<String>();
        if inner.eq_ignore_ascii_case("1mcontext") || inner.eq_ignore_ascii_case("2mcontext") {
            normalized.truncate(open);
            normalized = normalized.trim_end().to_string();
        }
    }
    normalized
}

pub fn is_anthropic_alias(model: &str) -> bool {
    ANTHROPIC_STYLE_ALIASES.contains(&model)
}

pub fn is_cursor_model(model: &str) -> bool {
    if crate::providers::cursor::model::CURSOR_LEGACY_MODELS.contains(&model) {
        return true;
    }

    CURSOR_PREFIXES
        .iter()
        .any(|prefix| model.starts_with(prefix))
}

fn is_unclaimed_cursor_catalog_id(model: &str) -> bool {
    if let Some(live) = crate::providers::cursor::model::cached_live_usable_models()
        && live.iter().any(|id| id == model)
    {
        return true;
    }
    crate::providers::cursor::model::resolve_cursor_model(model).is_ok()
}

struct PlaceholderProvider {
    name: &'static str,
    models: Vec<String>,
}

impl PlaceholderProvider {
    fn new(name: &str, models: Vec<String>) -> Self {
        let name = match name {
            "codex" => "codex",
            "kimi" => "kimi",
            "cursor" => "cursor",
            "grok" => "grok",
            _ => "codex",
        };
        Self { name, models }
    }
}

#[async_trait]
impl Provider for PlaceholderProvider {
    fn name(&self) -> &'static str {
        self.name
    }

    fn supported_models(&self) -> Vec<String> {
        self.models.clone()
    }

    fn cli(&self) -> &'static dyn CliHandlers {
        match self.name {
            "codex" => &CODEX_CLI,
            "kimi" => &KIMI_CLI,
            "cursor" => &CURSOR_CLI,
            "grok" => &GROK_CLI,
            _ => &CODEX_CLI,
        }
    }

    async fn handle_messages(&self, _body: MessagesRequest, ctx: RequestContext) -> Response {
        placeholder_provider_response("messages", &ctx.provider)
    }

    async fn handle_count_tokens(&self, _body: MessagesRequest, ctx: RequestContext) -> Response {
        placeholder_provider_response("count_tokens", &ctx.provider)
    }
}

fn placeholder_provider_response(route: &str, provider: &str) -> Response {
    let _ = route;
    json_error(
        StatusCode::NOT_IMPLEMENTED,
        "unsupported_provider_error",
        format!("provider '{}' is not yet implemented", provider),
    )
}

#[derive(Clone, Copy)]
struct PlaceholderCli {
    provider: &'static str,
}

impl CliHandlers for PlaceholderCli {
    fn login(&self) -> Result<()> {
        Err(anyhow!("{}: browser login not supported", self.provider))
    }

    fn device(&self) -> Result<()> {
        Err(anyhow!("{}: device login not supported", self.provider))
    }

    fn status(&self) -> Result<()> {
        use serde_json::Value;
        let path = crate::paths::provider_auth_file(self.provider);
        let legacy = crate::paths::provider_legacy_auth_file(self.provider);
        if crate::auth::load_auth_file_with_legacy::<Value>(&path, &legacy).is_some() {
            Ok(())
        } else {
            Err(anyhow!("Not authenticated"))
        }
    }

    fn logout(&self) -> Result<()> {
        let path = crate::paths::provider_auth_file(self.provider);
        let legacy = crate::paths::provider_legacy_auth_file(self.provider);
        let _ = crate::auth::delete_auth_file(&path, &legacy);
        Ok(())
    }
}

const CODEX_CLI: PlaceholderCli = PlaceholderCli { provider: "codex" };
const KIMI_CLI: PlaceholderCli = PlaceholderCli { provider: "kimi" };
const CURSOR_CLI: PlaceholderCli = PlaceholderCli { provider: "cursor" };
const GROK_CLI: PlaceholderCli = PlaceholderCli { provider: "grok" };

fn expand_codex_models() -> Vec<String> {
    let mut set = HashSet::new();
    let mut out = Vec::new();
    for model in CODEX_MODELS {
        if set.insert((*model).to_string()) {
            out.push((*model).to_string());
        }
        let fast = format!("{model}-fast");
        if set.insert(fast.clone()) {
            out.push(fast);
        }
    }
    out.sort_unstable();
    out
}

fn build_cursor_models() -> Vec<String> {
    let mut out: Vec<String> = crate::providers::cursor::model::CURSOR_LEGACY_MODELS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_model_trims_hint() {
        assert_eq!(normalize_incoming_model("gpt-5.4-fast[1m]"), "gpt-5.4-fast");
        assert_eq!(normalize_incoming_model("gpt-5.4-fast"), "gpt-5.4-fast");
    }

    #[test]
    fn normalize_model_strips_all_context_hint_spellings_without_prefixes() {
        for (raw, expected) in [
            ("gpt-5.5[2m]", "gpt-5.5"),
            ("gpt-5.5[2M]", "gpt-5.5"),
            ("gpt-5.5 (1M context)", "gpt-5.5"),
            ("gpt-5.5 ( 2m Context )", "gpt-5.5"),
            (" cursor:gpt-5.5[2m] ", "cursor:gpt-5.5"),
        ] {
            assert_eq!(normalize_incoming_model(raw), expected, "raw={raw:?}");
        }
    }

    #[test]
    fn context_hints_do_not_change_static_provider_selection() {
        let registry = Registry::new(AliasProvider::Codex);
        for model in ["gpt-5.5[2m]", "gpt-5.5 (1M context)"] {
            assert_eq!(
                registry
                    .provider_for_model(model, None)
                    .expect("known Codex model")
                    .name(),
                "codex",
                "context marker must not make {model} look like Cursor"
            );
        }
    }

    #[test]
    fn alias_routes_to_configured_provider() {
        let registry = Registry::new(AliasProvider::Kimi);
        let p = registry.provider_for_model("haiku", None);
        assert!(p.is_some());
        assert_eq!(p.expect("provider").name(), "kimi");
    }

    #[test]
    fn opus_4_8_routes_to_configured_provider() {
        let registry = Registry::new(AliasProvider::Codex);
        let p = registry.provider_for_model("claude-opus-4-8", None);
        assert!(p.is_some());
        assert_eq!(p.expect("provider").name(), "codex");
    }

    #[test]
    fn claude_5_aliases_route_to_configured_provider() {
        let registry = Registry::new(AliasProvider::Codex);
        let empty_accounts = crate::config::CursorAccountRoutingPolicy::empty();
        // Keep this alias-provider contract independent of any runtime Sand
        // model policy.
        let empty_sand = crate::config::SandRoutingPolicy::empty();
        for model in ["claude-sonnet-5", "fable", "claude-fable-5"] {
            let p = registry.provider_for_model_with_policies(
                model,
                None,
                &empty_accounts,
                &empty_sand,
            );
            assert!(p.is_some(), "{model} should route to a provider");
            assert_eq!(p.expect("provider").name(), "codex");
        }
    }

    #[test]
    fn claude_aliases_route_to_cursor_when_configured() {
        let registry = Registry::new(AliasProvider::Cursor);
        for model in [
            "claude-sonnet-5",
            "claude-opus-5",
            "claude-opus-5-thinking-high",
            "fable",
            "claude-fable-5",
            "haiku",
        ] {
            let p = registry.provider_for_model(model, None);
            assert!(p.is_some(), "{model} should route to a provider");
            assert_eq!(p.expect("provider").name(), "cursor");
        }
    }

    #[test]
    fn sand_policy_routes_fable_alias_before_global_alias_provider() {
        let registry = Registry::new(AliasProvider::Codex);
        let account_routes = crate::config::CursorAccountRoutingPolicy::empty();
        let sand_policy = crate::config::SandRoutingPolicy::new(["claude-fable-5"]);

        for model in ["fable", "claude-fable-5", "claude-fable-5[1m]"] {
            let provider = registry
                .provider_for_model_with_policies(model, None, &account_routes, &sand_policy)
                .expect("Sand-selected Fable alias should be routable");
            assert_eq!(provider.name(), "cursor", "{model} must use Cursor Sand");
        }
    }

    #[test]
    fn cursor_prefix_routes() {
        let registry = Registry::new(AliasProvider::Codex);
        assert_eq!(
            registry
                .provider_for_model("cursor:gpt-5.5", None)
                .unwrap()
                .name(),
            "cursor"
        );
        assert_eq!(
            registry
                .provider_for_model("cursor-plan:gpt-5.5", None)
                .unwrap()
                .name(),
            "cursor"
        );
        assert_eq!(
            registry
                .provider_for_model("cursor-ask:gpt-5.5", None)
                .unwrap()
                .name(),
            "cursor"
        );
    }

    #[test]
    fn live_cursor_catalog_ids_route_to_cursor() {
        let registry = Registry::new(AliasProvider::Codex);
        for model in [
            "claude-fable-5-thinking-high",
            "claude-fable-5-thinking-high[1m]",
            "claude-fable-5-thinking-max",
            "composer-2.5-fast",
        ] {
            let p = registry.provider_for_model(model, None);
            assert_eq!(
                p.expect(model).name(),
                "cursor",
                "{model} is advertised by Cursor and must be routable"
            );
        }
        assert_eq!(
            registry.provider_for_model("gpt-5.5", None).unwrap().name(),
            "codex",
            "overlapping gpt-5.5 without cursor: prefix stays Codex"
        );
        assert_eq!(
            registry
                .provider_for_model("cursor:gpt-5.5", None)
                .unwrap()
                .name(),
            "cursor"
        );
    }

    #[test]
    fn unbound_public_grok_models_keep_native_grok_provider() {
        let registry = Registry::new(AliasProvider::Codex);
        let empty_accounts = crate::config::CursorAccountRoutingPolicy::empty();
        let empty_sand = crate::config::SandRoutingPolicy::empty();
        for model in ["grok-4.5", "grok-4.6"] {
            let provider = registry
                .provider_for_model_with_policies(model, None, &empty_accounts, &empty_sand)
                .expect("native Grok model should remain routable");
            assert_eq!(provider.name(), "grok", "{model} must not be promoted");
        }
    }

    #[test]
    fn explicit_grok_account_routes_preempt_native_provider() {
        let registry = Registry::new(AliasProvider::Codex);
        let account_routes = crate::config::CursorAccountRoutingPolicy::new([
            crate::config::CursorModelAccountRule::new("grok-4.6", "family-account"),
            crate::config::CursorModelAccountRule::new("cursor-grok-4.6-high-fast", "high-account"),
            crate::config::CursorModelAccountRule::new(
                "cursor-grok-4.6-xhigh-fast",
                "xhigh-account",
            ),
        ]);
        let empty_sand = crate::config::SandRoutingPolicy::empty();

        for model in [
            // Public family id and its Cursor catalog spelling.
            "grok-4.6",
            "cursor-grok-4.6",
            // Explicit effort variants must resolve through the same-tier
            // account binding, including a mode prefix from Claude clients.
            "grok-4.6-high-fast",
            "cursor:grok-4.6-high-fast",
            "grok-4.6-xhigh-fast",
            "cursor:cursor-grok-4.6-xhigh-fast",
        ] {
            let provider = registry
                .provider_for_model_with_policies(model, None, &account_routes, &empty_sand)
                .expect("explicit Grok account route should be routable");
            assert_eq!(provider.name(), "cursor", "{model} must use Cursor");
        }
    }

    #[test]
    fn grok_wildcard_account_route_is_an_explicit_cursor_opt_in() {
        let registry = Registry::new(AliasProvider::Codex);
        let account_routes = crate::config::CursorAccountRoutingPolicy::new([
            crate::config::CursorModelAccountRule::new("grok-*", "grok-account"),
        ]);
        let empty_sand = crate::config::SandRoutingPolicy::empty();
        for model in ["grok-4.5", "grok-4.6"] {
            let provider = registry
                .provider_for_model_with_policies(model, None, &account_routes, &empty_sand)
                .expect("wildcard Grok route should be routable");
            assert_eq!(provider.name(), "cursor", "{model} wildcard route");
        }
    }

    #[test]
    fn account_route_does_not_preempt_native_codex_model() {
        let registry = Registry::new(AliasProvider::Codex);
        let account_routes = crate::config::CursorAccountRoutingPolicy::new([
            crate::config::CursorModelAccountRule::new("*", "fallback-account"),
        ]);
        let empty_sand = crate::config::SandRoutingPolicy::empty();
        let provider = registry
            .provider_for_model_with_policies("gpt-5.5", None, &account_routes, &empty_sand)
            .expect("known Codex model should remain routable");
        assert_eq!(provider.name(), "codex");
    }

    #[test]
    fn bare_grok_effort_uses_matching_cursor_tier_account() {
        let registry = Registry::new(AliasProvider::Codex);
        let account_routes = crate::config::CursorAccountRoutingPolicy::new([
            crate::config::CursorModelAccountRule::new(
                "cursor-grok-4.6-xhigh-fast",
                "xhigh-account",
            ),
        ]);
        let empty_sand = crate::config::SandRoutingPolicy::empty();
        let (model, provider) = registry
            .resolve_model_for_request_with_policies(
                "grok-4.6",
                Some("xhigh"),
                None,
                &account_routes,
                &empty_sand,
            )
            .expect("xhigh account route should promote Grok to Cursor");
        assert_eq!(model, "cursor-grok-4.6-xhigh-fast");
        assert_eq!(provider.name(), "cursor");
    }

    #[test]
    fn bare_grok_effort_uses_matching_sand_tier_without_crossing_high() {
        let registry = Registry::new(AliasProvider::Codex);
        let empty_accounts = crate::config::CursorAccountRoutingPolicy::empty();
        let sand = crate::config::SandRoutingPolicy::new(["cursor-grok-4.6-xhigh-fast"]);
        let (model, provider) = registry
            .resolve_model_for_request_with_policies(
                "grok-4.6",
                Some("xhigh"),
                None,
                &empty_accounts,
                &sand,
            )
            .expect("xhigh Sand rule should promote Grok to Cursor");
        assert_eq!(model, "cursor-grok-4.6-xhigh-fast");
        assert_eq!(provider.name(), "cursor");

        let high_only = crate::config::CursorAccountRoutingPolicy::new([
            crate::config::CursorModelAccountRule::new("cursor-grok-4.6-high-fast", "high-account"),
        ]);
        let empty_sand = crate::config::SandRoutingPolicy::empty();
        let provider = registry
            .resolve_model_for_request_with_policies(
                "grok-4.6",
                Some("xhigh"),
                None,
                &high_only,
                &empty_sand,
            )
            .expect("native Grok remains available without xhigh Cursor route");
        assert_eq!(provider.1.name(), "grok");
        assert_eq!(provider.0, "grok-4.6");

        let xhigh_only = crate::config::CursorAccountRoutingPolicy::new([
            crate::config::CursorModelAccountRule::new(
                "cursor-grok-4.6-xhigh-fast",
                "xhigh-account",
            ),
        ]);
        let provider = registry
            .resolve_model_for_request_with_policies(
                "grok-4.6",
                Some("high"),
                None,
                &xhigh_only,
                &empty_sand,
            )
            .expect("native Grok remains available without high Cursor route");
        assert_eq!(provider.1.name(), "grok");
        assert_eq!(provider.0, "grok-4.6");
    }
}
