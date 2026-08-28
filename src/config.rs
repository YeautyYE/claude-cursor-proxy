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
    #[serde(rename = "clientVersion")]
    pub client_version: Option<String>,
    #[serde(rename = "clientType")]
    pub client_type: Option<String>,
    #[serde(rename = "clientCommit")]
    pub client_commit: Option<String>,
    #[serde(rename = "ghostMode")]
    pub ghost_mode: Option<bool>,
    #[serde(rename = "agentBundle")]
    pub agent_bundle: Option<String>,
    #[serde(rename = "sandModels")]
    pub sand_models: Option<SandModelsConfig>,
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
        !model.is_empty()
            && self
                .patterns
                .iter()
                .any(|pattern| glob_matches(pattern, &model))
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
    normalized == "fable"
        || normalized == "claude-fable-5"
        || normalized.starts_with("claude-fable-5-")
}

/// Normalize a model id for Sand policy matching.
pub fn normalize_sand_model(model: &str) -> String {
    let mut normalized = model.trim().to_ascii_lowercase();
    if normalized.ends_with("[1m]") {
        normalized.truncate(normalized.len().saturating_sub(4));
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

/// Resolve the current model policy. The env var is an explicit override,
/// including an empty value (which disables all Sand matches).
pub fn cursor_sand_policy() -> SandRoutingPolicy {
    if let Some(raw) = std::env::var_os("CCP_CURSOR_SAND_MODELS") {
        return SandRoutingPolicy::new(parse_sand_models_env(&raw.to_string_lossy()));
    }

    let configured = read_file_config(&paths::config_dir())
        .and_then(|file| file.cursor)
        .and_then(|cursor| cursor.sand_models)
        .map(SandModelsConfig::into_patterns)
        .unwrap_or_default();
    SandRoutingPolicy::new(configured)
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
        if let Some(cursor) = file_cfg.cursor
            && let Some(models) = cursor.sand_models
            && !models.into_patterns().is_empty()
        {
            out.push("cursor.sandModels (config)".to_string());
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
    // Best-effort: leave unset if we cannot resolve; IDE uses Intl timezone.
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
            std::env::remove_var("CCP_CURSOR_CLIENT_TYPE");
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
        assert!(!from_env.matches("claude-fable-5"));
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
