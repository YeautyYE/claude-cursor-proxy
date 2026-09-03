//! Dynamic Cursor model catalog.
//!
//! `AgentService/GetUsableModels` exposes the legacy CLI variant slugs (for
//! example `gemini-3.6-flash-high`).  The Desktop/Sand endpoint accepts the
//! family name (`gemini-3.6-flash`) and receives effort/context settings in
//! `requestedModel.parameters`.  Cursor publishes the relationship through
//! `AiService/AvailableModels`; this module keeps a small account/identity
//! scoped snapshot of that response so model routing can follow server-side
//! catalog changes without a proxy release.

use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// One parameter attached to a Desktop model variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogParameter {
    pub id: String,
    pub value: String,
}

/// A CLI/variant spelling and its Desktop parameter representation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatalogVariant {
    pub legacy_slug: Option<String>,
    pub variant_string: Option<String>,
    pub parameters: Vec<CatalogParameter>,
}

/// One family from `aiserver.v1.AvailableModelsResponse.models[]`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatalogModel {
    /// Client-facing family id (`models[].name`).
    pub name: String,
    /// Provider-facing family id (`models[].serverModelName`).
    pub server_model_name: Option<String>,
    pub legacy_slugs: Vec<String>,
    pub id_aliases: Vec<String>,
    pub variants: Vec<CatalogVariant>,
}

/// Result of matching an arbitrary incoming model spelling against a family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogMatch {
    pub family_id: String,
    pub parameters: Vec<CatalogParameter>,
    /// The matched spelling, useful for diagnostics and preserving the
    /// requested variant when a caller needs to select a default.
    pub matched_id: String,
}

const CATALOG_TTL: Duration = Duration::from_secs(5 * 60);
const CATALOG_MAX_ACCOUNTS: usize = 64;
// Invalidation epochs are deliberately kept a little larger than the live
// snapshot bound.  They normally contain one entry per saved account; the
// generous cap also covers opaque-token rotations before the old identities
// are pruned.  If an unusually large credential pool exceeds it, resetting
// the whole metadata cache is safer than dropping an epoch and allowing an
// already-running request to repopulate stale data.
const CATALOG_MAX_EPOCHS: usize = CATALOG_MAX_ACCOUNTS * 16;

#[derive(Debug, Clone)]
struct Snapshot {
    fetched_at: Instant,
    models: Vec<CatalogModel>,
}

#[derive(Debug, Default)]
struct Cache {
    entries: BTreeMap<String, Snapshot>,
    /// Per-account invalidation counters.  A clear/re-login increments the
    /// counter, allowing an in-flight request that started before the clear
    /// to be discarded when it eventually completes.
    account_epochs: BTreeMap<String, u64>,
    /// Global epoch used by `clear()`.  Keeping it separate means a late
    /// response from any account cannot resurrect a snapshot after logout.
    global_epoch: u64,
}

/// Opaque generation captured immediately before a catalog request.  The
/// bearer/account identity is never retained in this value; it is only a pair
/// of monotonic counters checked while storing the response.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CacheGeneration {
    global: u64,
    account: u64,
}

fn cache() -> &'static Mutex<Cache> {
    static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(Cache::default()))
}

fn cache_key(token: &str, client_type: &str) -> String {
    // Access JWTs rotate during a normal session.  Partition by Cursor's
    // stable account identity instead of the transient bearer so a refresh
    // does not discard a still-fresh five-minute catalog snapshot.
    let digest = crate::providers::cursor::auth::cursor_account_digest(token);
    format!("{digest}:{}", normalize_identity(client_type))
}

fn normalize_identity(client_type: &str) -> String {
    let trimmed = client_type.trim();
    if trimmed.is_empty() {
        "cli".to_string()
    } else if trimmed.eq_ignore_ascii_case("sand") {
        "sand".to_string()
    } else {
        trimmed.to_ascii_lowercase()
    }
}

fn prune_locked(cache: &mut Cache) {
    cache
        .entries
        .retain(|_, snapshot| snapshot.fetched_at.elapsed() < CATALOG_TTL);
    if cache.entries.len() > CATALOG_MAX_ACCOUNTS {
        let mut order: Vec<(String, Instant)> = cache
            .entries
            .iter()
            .map(|(key, snapshot)| (key.clone(), snapshot.fetched_at))
            .collect();
        order.sort_unstable_by_key(|(_, at)| *at);
        for (key, _) in order
            .into_iter()
            .take(cache.entries.len().saturating_sub(CATALOG_MAX_ACCOUNTS))
        {
            cache.entries.remove(&key);
        }
    }

    if cache.account_epochs.len() > CATALOG_MAX_EPOCHS {
        // Do not evict an arbitrary epoch: a request may still be in flight
        // for an account that has no snapshot yet. A global generation bump
        // invalidates every such response before dropping the oversized map.
        cache.global_epoch = cache.global_epoch.wrapping_add(1);
        cache.account_epochs.clear();
        cache.entries.clear();
    }
}

/// Capture the current invalidation generation for one account. The caller
/// should invoke this after acquiring the per-key single-flight lease and
/// immediately before starting its network request.
pub(crate) fn generation_for_account(token: &str) -> CacheGeneration {
    let account_key = crate::providers::cursor::auth::cursor_account_digest(token);
    let Ok(mut guard) = cache().lock() else {
        // A poisoned cache is treated as an unusable generation. The store
        // path also refuses to write when it cannot lock, so this cannot let
        // stale data through.
        return CacheGeneration::default();
    };
    prune_locked(&mut guard);
    CacheGeneration {
        global: guard.global_epoch,
        account: *guard.account_epochs.get(&account_key).unwrap_or(&0),
    }
}

/// Store a freshly fetched catalog under the account identity and request
/// identity. Only a stable digest is retained in process memory.
pub fn store_for_account(token: &str, client_type: &str, models: Vec<CatalogModel>) {
    let generation = generation_for_account(token);
    store_for_account_at_generation(token, client_type, generation, models);
}

/// Store a response only when no account/global invalidation happened after
/// the request began. This prevents a late AvailableModels response from
/// resurrecting entitlements after re-login, account removal, or logout.
pub(crate) fn store_for_account_at_generation(
    token: &str,
    client_type: &str,
    generation: CacheGeneration,
    models: Vec<CatalogModel>,
) {
    if models.is_empty() {
        return;
    }
    let account_key = crate::providers::cursor::auth::cursor_account_digest(token);
    if let Ok(mut guard) = cache().lock() {
        prune_locked(&mut guard);
        let current_account_epoch = *guard.account_epochs.get(&account_key).unwrap_or(&0);
        if guard.global_epoch != generation.global || current_account_epoch != generation.account {
            return;
        }
        guard.entries.insert(
            cache_key(token, client_type),
            Snapshot {
                fetched_at: Instant::now(),
                models,
            },
        );
        prune_locked(&mut guard);
    }
}

/// Return a fresh account/identity-scoped snapshot.
pub fn cached_for_account(token: &str, client_type: &str) -> Option<Vec<CatalogModel>> {
    let mut guard = cache().lock().ok()?;
    prune_locked(&mut guard);
    guard
        .entries
        .get(&cache_key(token, client_type))
        .map(|snapshot| snapshot.models.clone())
}

/// Clear all dynamic catalog state.  This is used when the auth loader loses
/// its active account and in deterministic tests.
pub fn clear() {
    if let Ok(mut guard) = cache().lock() {
        guard.global_epoch = guard.global_epoch.wrapping_add(1);
        guard.entries.clear();
        guard.account_epochs.clear();
    }
}

/// Invalidate all catalog identities for one account while retaining fresh
/// snapshots belonging to other saved accounts.  A normal re-login or account
/// removal should not leave the just-updated account's model entitlements in
/// the five-minute cache, but clearing the whole process cache would make
/// multi-account TUI switching needlessly expensive.
pub fn clear_for_account(token: &str) {
    let account_key = crate::providers::cursor::auth::cursor_account_digest(token);
    let prefix = format!("{account_key}:");
    if let Ok(mut guard) = cache().lock() {
        guard.entries.retain(|key, _| !key.starts_with(&prefix));
        let epoch = guard.account_epochs.entry(account_key).or_default();
        *epoch = epoch.wrapping_add(1);
        prune_locked(&mut guard);
    }
}

/// Find a family by canonical id, alias, legacy slug, or variant string.
/// Matching is case-insensitive because Cursor treats model ids as such on
/// the Desktop wire; the canonical spelling from the server is returned.
pub fn resolve(models: &[CatalogModel], requested: &str) -> Option<CatalogMatch> {
    let requested = requested.trim();
    if requested.is_empty() {
        return None;
    }
    let requested_lower = requested.to_ascii_lowercase();
    for model in models {
        let family = model
            .server_model_name
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .unwrap_or(&model.name)
            .trim();
        if family.is_empty() {
            continue;
        }
        if family.eq_ignore_ascii_case(requested) || model.name.eq_ignore_ascii_case(requested) {
            return Some(CatalogMatch {
                family_id: family.to_string(),
                parameters: Vec::new(),
                matched_id: requested.to_string(),
            });
        }
        if let Some(alias) = model
            .id_aliases
            .iter()
            .find(|alias| alias.eq_ignore_ascii_case(requested))
        {
            return Some(CatalogMatch {
                family_id: family.to_string(),
                parameters: Vec::new(),
                matched_id: alias.clone(),
            });
        }
        if let Some(variant) = model.variants.iter().find(|variant| {
            variant
                .legacy_slug
                .as_deref()
                .is_some_and(|slug| slug.eq_ignore_ascii_case(requested))
                || variant
                    .variant_string
                    .as_deref()
                    .is_some_and(|slug| slug.eq_ignore_ascii_case(requested))
        }) {
            return Some(CatalogMatch {
                family_id: family.to_string(),
                parameters: variant.parameters.clone(),
                matched_id: requested.to_string(),
            });
        }
        if model
            .legacy_slugs
            .iter()
            .any(|slug| slug.eq_ignore_ascii_case(requested))
        {
            return Some(CatalogMatch {
                family_id: family.to_string(),
                parameters: infer_parameters_from_variant_id(requested),
                matched_id: requested.to_string(),
            });
        }
        // Some server builds omit `variants[]` but still expose a bracketed
        // variant string in a legacy slug.  A family-prefix check is safe only
        // when the suffix consists entirely of known variant tokens.
        if requested_lower.starts_with(&format!("{}-", family.to_ascii_lowercase()))
            && !infer_parameters_from_variant_id(requested).is_empty()
        {
            return Some(CatalogMatch {
                family_id: family.to_string(),
                parameters: infer_parameters_from_variant_id(requested),
                matched_id: requested.to_string(),
            });
        }
    }
    None
}

/// Parse `AvailableModelsResponse` JSON.  The parser accepts both protobuf
/// JSON names and snake_case, and also tolerates Connect wrappers (`result`,
/// `response`, `data`, `payload`).
pub fn parse_json(body: &str) -> Result<Vec<CatalogModel>, String> {
    let value: Value = serde_json::from_str(body).map_err(|error| error.to_string())?;
    let models = find_models_array(&value)
        .ok_or_else(|| "AvailableModels JSON missing models[]".to_string())?;
    let mut output = Vec::with_capacity(models.len());
    for value in models {
        let Some(object) = value.as_object() else {
            continue;
        };
        let name = string_field(object, &["name", "modelName", "model_name"]).unwrap_or_default();
        let server_model_name = string_field(object, &["serverModelName", "server_model_name"]);
        let mut model = CatalogModel {
            name,
            server_model_name,
            legacy_slugs: string_array_field(object, &["legacySlugs", "legacy_slugs"]),
            id_aliases: string_array_field(object, &["idAliases", "id_aliases"]),
            variants: Vec::new(),
        };
        if let Some(variants) = object
            .get("variants")
            .or_else(|| object.get("modelVariants"))
            .or_else(|| object.get("model_variants"))
            .and_then(Value::as_array)
        {
            for variant in variants {
                let Some(variant_object) = variant.as_object() else {
                    continue;
                };
                let parameters = parse_parameter_values(variant_object);
                let variant_string = string_field(
                    variant_object,
                    &[
                        "variantStringRepresentation",
                        "variant_string_representation",
                    ],
                );
                let legacy_slug = string_field(variant_object, &["legacySlug", "legacy_slug"]);
                model.variants.push(CatalogVariant {
                    legacy_slug,
                    variant_string: variant_string.clone(),
                    parameters: if parameters.is_empty() {
                        variant_string
                            .as_deref()
                            .map(parse_variant_string)
                            .unwrap_or_default()
                    } else {
                        parameters
                    },
                });
            }
        }
        // A few responses only carry `legacySlugs[]` and no variant objects.
        // Retain those as variants so parameter inference still works.
        for slug in &model.legacy_slugs {
            if !model.variants.iter().any(|variant| {
                variant
                    .legacy_slug
                    .as_deref()
                    .is_some_and(|known| known.eq_ignore_ascii_case(slug))
            }) {
                model.variants.push(CatalogVariant {
                    legacy_slug: Some(slug.clone()),
                    variant_string: None,
                    parameters: infer_parameters_from_variant_id(slug),
                });
            }
        }
        if !model.name.trim().is_empty()
            || model
                .server_model_name
                .as_deref()
                .is_some_and(|id| !id.trim().is_empty())
        {
            output.push(model);
        }
    }
    Ok(output)
}

fn find_models_array(value: &Value) -> Option<&Vec<Value>> {
    if let Some(models) = value.get("models").and_then(Value::as_array) {
        return Some(models);
    }
    let Value::Object(object) = value else {
        return None;
    };
    for key in ["result", "response", "data", "payload"] {
        if let Some(child) = object.get(key)
            && let Some(models) = find_models_array(child)
        {
            return Some(models);
        }
    }
    None
}

fn string_field(object: &serde_json::Map<String, Value>, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| object.get(*name).and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn string_array_field(object: &serde_json::Map<String, Value>, names: &[&str]) -> Vec<String> {
    let mut output: Vec<String> = Vec::new();
    for name in names {
        let Some(values) = object.get(*name).and_then(Value::as_array) else {
            continue;
        };
        for value in values {
            if let Some(value) = value
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                && !output.iter().any(|known| known.eq_ignore_ascii_case(value))
            {
                output.push(value.to_string());
            }
        }
    }
    output
}

fn parse_parameter_values(object: &serde_json::Map<String, Value>) -> Vec<CatalogParameter> {
    let Some(values) = object
        .get("parameterValues")
        .or_else(|| object.get("parameter_values"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(|value| {
            let object = value.as_object()?;
            let id = string_field(object, &["id", "parameterId", "parameter_id"])?;
            let value = object
                .get("value")
                .and_then(|value| match value {
                    Value::String(value) => Some(value.clone()),
                    Value::Number(value) => Some(value.to_string()),
                    Value::Bool(value) => Some(value.to_string()),
                    _ => None,
                })
                .filter(|value| !value.trim().is_empty())?;
            Some(CatalogParameter { id, value })
        })
        .collect()
}

/// Parse the compact Desktop representation, e.g.
/// `grok-4.6[effort=high,fast=true]`.
pub fn parse_variant_string(value: &str) -> Vec<CatalogParameter> {
    let Some((_, inner)) = value.split_once('[') else {
        return Vec::new();
    };
    let inner = inner.strip_suffix(']').unwrap_or(inner);
    inner
        .split(',')
        .filter_map(|pair| {
            let (id, value) = pair.split_once('=')?;
            let id = id.trim();
            let value = value.trim();
            if id.is_empty() || value.is_empty() {
                None
            } else {
                Some(CatalogParameter {
                    id: id.to_string(),
                    value: value.to_string(),
                })
            }
        })
        .collect()
}

/// Conservative fallback for legacy slugs when the server omitted variant
/// metadata.  It intentionally does not strip arbitrary suffixes.
pub fn infer_parameters_from_variant_id(value: &str) -> Vec<CatalogParameter> {
    let base = value
        .split_once('[')
        .map(|(base, _)| base)
        .unwrap_or(value)
        .to_ascii_lowercase();
    let tokens: Vec<&str> = base.split('-').collect();
    let mut output = Vec::new();
    let effort = ["minimal", "none", "low", "medium", "high", "xhigh", "max"]
        .iter()
        .find(|tier| tokens.last().is_some_and(|token| token == *tier));
    if let Some(effort) = effort {
        output.push(CatalogParameter {
            id: "effort".into(),
            value: (*effort).into(),
        });
    }
    if tokens.contains(&"thinking") {
        output.push(CatalogParameter {
            id: "thinking".into(),
            value: "true".into(),
        });
    }
    if tokens.last().is_some_and(|token| *token == "fast") {
        output.push(CatalogParameter {
            id: "fast".into(),
            value: "true".into(),
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    // Keep cache-structure tests independent of the process-global cache. The
    // production cache is intentionally private and process-wide, while a
    // local value lets us exercise expiry/eviction deterministically without
    // sleeping for the five-minute TTL.
    fn snapshot_at(age: Duration, name: &str) -> Snapshot {
        Snapshot {
            fetched_at: Instant::now() - age,
            models: vec![CatalogModel {
                name: name.to_string(),
                ..CatalogModel::default()
            }],
        }
    }

    #[test]
    fn parses_family_aliases_variants_and_parameters() {
        let body = r#"{"models":[{"name":"grok-4.6","serverModelName":"grok-4.6","legacySlugs":["cursor-grok-4.6-high-fast"],"idAliases":["grok"],"variants":[{"legacySlug":"cursor-grok-4.6-high-fast","variantStringRepresentation":"grok-4.6[effort=high,fast=true]","parameterValues":[{"id":"effort","value":"high"},{"id":"fast","value":"true"}]}]}]}"#;
        let models = parse_json(body).unwrap();
        assert_eq!(models[0].name, "grok-4.6");
        let matched = resolve(&models, "cursor-grok-4.6-high-fast").unwrap();
        assert_eq!(matched.family_id, "grok-4.6");
        assert_eq!(matched.parameters.len(), 2);
        assert_eq!(resolve(&models, "GROK").unwrap().family_id, "grok-4.6");
    }

    #[test]
    fn parses_snake_case_and_variant_string_fallback() {
        let body = r#"{"result":{"models":[{"name":"gpt-5.5","server_model_name":"gpt-5.5","legacy_slugs":["gpt-5.5-high-fast"],"variants":[{"legacy_slug":"gpt-5.5-high-fast","variant_string_representation":"gpt-5.5[reasoning=high,fast=true]"}]}]}}"#;
        let models = parse_json(body).unwrap();
        let matched = resolve(&models, "gpt-5.5-high-fast").unwrap();
        assert_eq!(matched.family_id, "gpt-5.5");
        assert_eq!(matched.parameters[0].id, "reasoning");
        assert_eq!(matched.parameters[1].value, "true");
    }

    #[test]
    fn variant_string_parser_is_bounded() {
        assert_eq!(
            parse_variant_string("grok-4.6[]"),
            Vec::<CatalogParameter>::new()
        );
        assert_eq!(
            parse_variant_string("grok-4.6[effort=high,fast=true]"),
            vec![
                CatalogParameter {
                    id: "effort".into(),
                    value: "high".into()
                },
                CatalogParameter {
                    id: "fast".into(),
                    value: "true".into()
                },
            ]
        );
    }

    #[test]
    fn cache_prunes_expired_snapshots_without_waiting_for_ttl() {
        let mut local = Cache::default();
        local.entries.insert(
            "expired".into(),
            snapshot_at(CATALOG_TTL + Duration::from_secs(1), "expired-model"),
        );
        local.entries.insert(
            "fresh".into(),
            snapshot_at(Duration::from_secs(0), "fresh-model"),
        );

        prune_locked(&mut local);

        assert!(!local.entries.contains_key("expired"));
        assert!(local.entries.contains_key("fresh"));
    }

    #[test]
    fn cache_eviction_keeps_newest_entries_when_bound_is_exceeded() {
        let mut local = Cache::default();
        // Use sub-second ages so every entry remains inside the TTL. The
        // oldest entry should be evicted once the bounded map is pruned.
        for index in 0..(CATALOG_MAX_ACCOUNTS + 3) {
            local.entries.insert(
                format!("account-{index}"),
                snapshot_at(
                    Duration::from_millis(index as u64),
                    &format!("model-{index}"),
                ),
            );
        }

        prune_locked(&mut local);

        assert_eq!(local.entries.len(), CATALOG_MAX_ACCOUNTS);
        assert!(local.entries.contains_key("account-0"));
        assert!(local.entries.contains_key("account-1"));
        assert!(local.entries.contains_key("account-2"));
        assert!(!local.entries.contains_key("account-66"));
    }

    #[test]
    fn cache_key_is_credential_free_and_identity_partitioned() {
        let bearer = "catalog-secret-token";
        let sand = cache_key(bearer, " SAND ");
        let cli = cache_key(bearer, "cli");

        assert!(!sand.contains(bearer));
        assert!(sand.ends_with(":sand"));
        assert!(cli.ends_with(":cli"));
        assert_ne!(sand, cli);
        assert_eq!(cache_key(bearer, "Sand"), sand);
        assert_eq!(cache_key(bearer, ""), cli);
    }

    #[test]
    fn cache_key_survives_access_token_rotation_for_same_account() {
        use base64::Engine as _;

        fn token(sub: &str, issued_at: u64) -> String {
            let payload =
                serde_json::json!({"sub": sub, "email": "User@Example.com", "iat": issued_at});
            let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&payload).expect("JWT payload"));
            format!("header.{encoded}.signature")
        }

        let first = token("stable-account", 1);
        let rotated = token("stable-account", 2);
        assert_eq!(
            cache_key(&first, "sand"),
            cache_key(&rotated, "sand"),
            "a refresh-token rotation must keep the five-minute catalog entry"
        );
    }

    #[test]
    fn clear_for_account_keeps_other_account_snapshots() {
        let account_a = format!("catalog-clear-a-{}", uuid::Uuid::new_v4());
        let account_b = format!("catalog-clear-b-{}", uuid::Uuid::new_v4());
        store_for_account(
            &account_a,
            "cli",
            vec![CatalogModel {
                name: "model-a".into(),
                ..CatalogModel::default()
            }],
        );
        store_for_account(
            &account_b,
            "sand",
            vec![CatalogModel {
                name: "model-b".into(),
                ..CatalogModel::default()
            }],
        );

        clear_for_account(&account_a);
        assert!(cached_for_account(&account_a, "cli").is_none());
        assert_eq!(
            cached_for_account(&account_b, "sand").expect("another account must remain cached")[0]
                .name,
            "model-b"
        );

        // Avoid retaining this test's entry for subsequent tests.
        clear_for_account(&account_b);
    }

    #[test]
    fn invalidation_epoch_rejects_late_store_after_account_clear() {
        let account_a = format!("catalog-epoch-a-{}", uuid::Uuid::new_v4());
        let account_b = format!("catalog-epoch-b-{}", uuid::Uuid::new_v4());

        // Capture the generation as if a network request had just started,
        // then invalidate the account before its response arrives.
        let stale_generation = generation_for_account(&account_a);
        clear_for_account(&account_a);
        store_for_account_at_generation(
            &account_a,
            "sand",
            stale_generation,
            vec![CatalogModel {
                name: "stale-model".into(),
                ..CatalogModel::default()
            }],
        );
        assert!(
            cached_for_account(&account_a, "sand").is_none(),
            "a response started before clear_for_account must not resurrect stale data"
        );

        // The other account remains independently cacheable while account A
        // is being refreshed.
        let fresh_b = generation_for_account(&account_b);
        store_for_account_at_generation(
            &account_b,
            "cli",
            fresh_b,
            vec![CatalogModel {
                name: "fresh-model".into(),
                ..CatalogModel::default()
            }],
        );
        assert_eq!(
            cached_for_account(&account_b, "cli")
                .expect("unrelated account snapshot should remain available")[0]
                .name,
            "fresh-model"
        );

        // A request that starts after the clear gets the new generation and
        // is allowed to populate the account normally.
        let current_a = generation_for_account(&account_a);
        store_for_account_at_generation(
            &account_a,
            "sand",
            current_a,
            vec![CatalogModel {
                name: "fresh-a".into(),
                ..CatalogModel::default()
            }],
        );
        assert_eq!(
            cached_for_account(&account_a, "sand")
                .expect("post-clear response should be cacheable")[0]
                .name,
            "fresh-a"
        );

        clear_for_account(&account_a);
        clear_for_account(&account_b);
    }
}
