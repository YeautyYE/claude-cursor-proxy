//! Cursor model catalog -- resolves incoming model names to Cursor model IDs.
//!
//! Resolution rules:
//! - `cursor:`, `cursor-plan:`, `cursor-ask:` prefixes are stripped and mapped
//!   to the corresponding agent mode.
//! - Legacy names like `cursor`, `cursor-agent`, `cursor-composer`,
//!   `cursor-composer-fast`, `cursor-plan`, `cursor-ask`, `composer-2.5`,
//!   `composer-2.5-fast` are recognized.
//! - `cursor-agent:` is also supported for agent mode routing.

pub const CURSOR_LEGACY_MODELS: &[&str] = &[
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

/// Resolve a model string into a (model_id, mode) pair.
///
/// Returns an error if the model is not recognized.
pub fn resolve_cursor_model(model: &str) -> Result<CursorModelResolution, String> {
    let model = strip_anthropic_context_suffix(model.trim());

    // Strip known prefixes
    if let Some(rest) = model.strip_prefix("cursor-agent:") {
        return Ok(CursorModelResolution {
            model_id: strip_anthropic_context_suffix(rest).to_string(),
            mode: CursorAgentMode::Agent,
        });
    }
    if let Some(rest) = model.strip_prefix("cursor-plan:") {
        return Ok(CursorModelResolution {
            model_id: strip_anthropic_context_suffix(rest).to_string(),
            mode: CursorAgentMode::Plan,
        });
    }
    if let Some(rest) = model.strip_prefix("cursor-ask:") {
        return Ok(CursorModelResolution {
            model_id: strip_anthropic_context_suffix(rest).to_string(),
            mode: CursorAgentMode::Ask,
        });
    }
    if let Some(rest) = model.strip_prefix("cursor:") {
        return Ok(CursorModelResolution {
            model_id: strip_anthropic_context_suffix(rest).to_string(),
            mode: CursorAgentMode::Agent,
        });
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
        "haiku" => Ok(CursorModelResolution {
            model_id: "claude-4.5-sonnet".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        "sonnet" | "claude-sonnet-5" => Ok(CursorModelResolution {
            model_id: "claude-sonnet-5-high".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        "opus" => Ok(CursorModelResolution {
            model_id: "claude-opus-4-8-high".to_string(),
            mode: CursorAgentMode::Agent,
        }),
        // Pass through full Cursor catalog ids (claude-fable-5-thinking-high, …).
        other
            if other.starts_with("claude-")
                || other.starts_with("gpt-")
                || other.starts_with("composer-")
                || other.starts_with("gemini-")
                || other.starts_with("cursor-grok-")
                || other.starts_with("kimi-")
                || other.starts_with("glm-") =>
        {
            Ok(CursorModelResolution {
                model_id: other.to_string(),
                mode: CursorAgentMode::Agent,
            })
        }
        _ => Err(format!(
            "unknown cursor model: {model}. Use cursor:<id> with a CLI catalog id (e.g. cursor:claude-fable-5-thinking-max, cursor:composer-2.5)"
        )),
    }
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
/// Merged into [`cursor_supported_models`] for listing only — does not affect
/// [`resolve_cursor_model`].
fn live_catalog_cache() -> &'static std::sync::Mutex<Option<(std::time::Instant, Vec<String>)>> {
    use std::sync::{Mutex, OnceLock};
    #[allow(clippy::type_complexity)]
    static CACHE: OnceLock<Mutex<Option<(std::time::Instant, Vec<String>)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

const LIVE_CATALOG_TTL: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// Store a freshly fetched GetUsableModels catalog (5-minute TTL).
pub fn store_live_usable_models(models: Vec<String>) {
    if let Ok(mut guard) = live_catalog_cache().lock() {
        *guard = Some((std::time::Instant::now(), models));
    }
}

/// Return cached live model ids if still within TTL.
pub fn cached_live_usable_models() -> Option<Vec<String>> {
    let guard = live_catalog_cache().lock().ok()?;
    let (at, models) = guard.as_ref()?;
    if at.elapsed() < LIVE_CATALOG_TTL {
        Some(models.clone())
    } else {
        None
    }
}

/// Build the list of supported Cursor model names.
///
/// Includes legacy aliases plus any still-fresh live catalog ids from
/// GetUsableModels. Resolution via [`resolve_cursor_model`] is unchanged.
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
    fn resolve_anthropic_aliases_for_cursor_alias_provider() {
        let r = resolve_cursor_model("fable").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");
        assert_eq!(r.mode, CursorAgentMode::Agent);

        let r = resolve_cursor_model("claude-fable-5").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-max");

        let r = resolve_cursor_model("claude-fable-5-thinking-high").unwrap();
        assert_eq!(r.model_id, "claude-fable-5-thinking-high");

        let r = resolve_cursor_model("haiku").unwrap();
        assert_eq!(r.model_id, "claude-4.5-sonnet");
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
}
