use anyhow::Context;
use base64::Engine;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::auth::{AuthStorage, KeychainFileAuthStore, SystemKeychain};
use crate::{config, paths};

pub const KEYCHAIN_SERVICE: &str = "claude-cursor-proxy.cursor";
pub const KEYCHAIN_ACCOUNT: &str = "auth";

/// Refresh when access JWT is within this window of expiry (align with Codex 5min).
const REFRESH_EXPIRY_SKEW_MS: u64 = 5 * 60_000;
const CURSOR_WEBSITE_URL: &str = "https://cursor.com";
const DESKTOP_STATE_DB_QUERY_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StoredCursorAuth {
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorAuth {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub api_key: Option<String>,
    pub expires: Option<u64>,
    pub user_id: Option<String>,
    pub email: Option<String>,
    pub source: String,
}

pub type DefaultCursorAuthStore = KeychainFileAuthStore<StoredCursorAuth, SystemKeychain>;

pub struct CursorTokenStore<S: AuthStorage<StoredCursorAuth>> {
    store: S,
}

impl<S: AuthStorage<StoredCursorAuth>> CursorTokenStore<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn load_auth(&self) -> anyhow::Result<Option<CursorAuth>> {
        let Some(stored) = self.store.load()? else {
            return Ok(None);
        };
        if stored.access_token.trim().is_empty() {
            return Ok(None);
        }
        let auth = enrich(stored, self.auth_path());
        self.refresh_if_needed(auth)
    }

    pub fn save_auth(&self, auth: StoredCursorAuth) -> anyhow::Result<CursorAuth> {
        if auth.access_token.trim().is_empty() {
            anyhow::bail!("Cursor auth accessToken is required");
        }
        self.store.save(auth.clone())?;
        Ok(enrich(auth, self.auth_path()))
    }

    pub fn clear_auth(&self) -> anyhow::Result<()> {
        self.store.clear()
    }

    pub fn auth_path(&self) -> String {
        self.store.path()
    }

    fn refresh_if_needed(&self, auth: CursorAuth) -> anyhow::Result<Option<CursorAuth>> {
        let Some(refresh_token) = auth.refresh_token.clone() else {
            return Ok(Some(auth));
        };
        let Some(expires) = auth.expires else {
            return Ok(Some(auth));
        };
        if expires > now_ms() + REFRESH_EXPIRY_SKEW_MS {
            return Ok(Some(auth));
        }

        // Single-flight: concurrent near-expiry loads must not race the
        // rotation (a second refresh can invalidate the first's tokens).
        let _flight = refresh_single_flight();
        // Re-check under the lock: another request may have refreshed, or
        // `cursor auth login` may have hot-swapped the account.
        match self.store.load()? {
            Some(current) if current.access_token != auth.access_token => {
                return Ok(Some(enrich(current, self.auth_path())));
            }
            Some(_) => {}
            None => return Ok(None),
        }

        match refresh_cursor_auth(&refresh_token) {
            Ok(Some(refreshed)) => {
                // Account CAS before save: never overwrite a hot-swapped login
                // with a refresh of the account it replaced.
                if let Some(current) = self.store.load()?
                    && current.access_token != auth.access_token
                {
                    return Ok(Some(enrich(current, self.auth_path())));
                }
                let new_refresh = if refreshed.refresh_token.is_empty() {
                    auth.refresh_token.clone()
                } else {
                    Some(refreshed.refresh_token)
                };
                self.save_auth(StoredCursorAuth {
                    access_token: refreshed.access_token,
                    refresh_token: new_refresh,
                    api_key: auth.api_key.clone(),
                })
                .map(Some)
            }
            Ok(None) => {
                // Refresh rejected — only hard-fail if already expired.
                if expires <= now_ms() {
                    anyhow::bail!(
                        "Cursor access token expired and refresh failed. Run `claude-cursor-proxy cursor auth login`."
                    );
                }
                Ok(Some(auth))
            }
            Err(err) => {
                if expires <= now_ms() {
                    Err(err).context("Cursor token refresh failed after access token expiry")
                } else {
                    // Still usable for a short while; surface on next hard expiry.
                    Ok(Some(auth))
                }
            }
        }
    }

    /// Unconditional refresh using the stored refresh token (upstream 401
    /// recovery). `failed_access_token` is the bearer that just 401'd: when
    /// the store already holds a different token (another flight refreshed,
    /// or a hot account switch happened), that stored auth is returned
    /// without spending a rotation.
    pub fn force_refresh(
        &self,
        failed_access_token: Option<&str>,
    ) -> anyhow::Result<Option<CursorAuth>> {
        let _flight = refresh_single_flight();
        let Some(stored) = self.store.load()? else {
            return Ok(None);
        };
        if stored.access_token.trim().is_empty() {
            return Ok(None);
        }
        if let Some(failed) = failed_access_token
            && stored.access_token != failed
        {
            return Ok(Some(enrich(stored, self.auth_path())));
        }
        let auth = enrich(stored.clone(), self.auth_path());
        let Some(refresh_token) = auth.refresh_token.as_deref() else {
            anyhow::bail!(
                "No Cursor refresh token available (env tokens cannot auto-renew). Run `claude-cursor-proxy cursor auth login`."
            );
        };
        let refreshed = refresh_cursor_auth(refresh_token)?
            .ok_or_else(|| anyhow::anyhow!("Cursor /auth/refresh returned non-success"))?;
        // Account CAS before save (hot login is a separate process; the
        // in-process single-flight cannot serialize it).
        if let Some(current) = self.store.load()?
            && current.access_token != stored.access_token
        {
            return Ok(Some(enrich(current, self.auth_path())));
        }
        let new_refresh = if refreshed.refresh_token.is_empty() {
            auth.refresh_token.clone()
        } else {
            Some(refreshed.refresh_token)
        };
        self.save_auth(StoredCursorAuth {
            access_token: refreshed.access_token,
            refresh_token: new_refresh,
            api_key: auth.api_key,
        })
        .map(Some)
    }
}

/// Process-wide refresh serialization. Held across the blocking refresh HTTP
/// call; waiters re-check the store and usually return without a second
/// rotation.
fn refresh_single_flight() -> std::sync::MutexGuard<'static, ()> {
    static REFRESH_FLIGHT: std::sync::Mutex<()> = std::sync::Mutex::new(());
    REFRESH_FLIGHT.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorLogin {
    pub login_url: String,
    pub uuid: String,
    pub verifier: String,
}

#[derive(Debug, Deserialize)]
pub struct RefreshResponse {
    #[serde(rename = "accessToken")]
    access_token: String,
    #[serde(rename = "refreshToken")]
    refresh_token: String,
}

pub fn file_store() -> CursorTokenStore<DefaultCursorAuthStore> {
    let primary = paths::provider_auth_file("cursor");
    let legacy = paths::provider_legacy_auth_file("cursor");
    CursorTokenStore::new(KeychainFileAuthStore::new(
        primary.to_string_lossy().to_string(),
        legacy.to_string_lossy().to_string(),
        KEYCHAIN_SERVICE,
        KEYCHAIN_ACCOUNT,
        use_macos_keychain(),
        SystemKeychain,
    ))
}

pub fn load_cursor_auth() -> anyhow::Result<Option<CursorAuth>> {
    let result = (|| {
        if let Some(token) = env_cursor_token() {
            return Ok(Some(enrich(
                StoredCursorAuth {
                    access_token: token,
                    refresh_token: None,
                    api_key: None,
                },
                "environment".to_string(),
            )));
        }
        if let Some(auth) = file_store().load_auth()? {
            return Ok(Some(auth));
        }
        // Optional: reuse official Cursor CLI keychain when proxy store is empty.
        if cli_keychain_fallback_enabled()
            && let Some(auth) = load_official_cli_keychain_auth()?
        {
            return Ok(Some(auth));
        }
        // Non-macOS / file-store CLI credentials (~/.config/cursor/auth.json).
        if cli_keychain_fallback_enabled()
            && let Some(auth) = load_official_cli_auth_json()?
        {
            return Ok(Some(auth));
        }
        Ok(None)
    })();

    // Keep model discovery aligned with the credentials used by requests. A
    // hot account switch retires the old catalog before any unkeyed listing
    // helper (TUI/registry) can read it.
    match &result {
        Ok(Some(auth)) => {
            crate::providers::cursor::model::observe_live_usable_models_account(&auth.access_token);
        }
        Ok(None) => crate::providers::cursor::model::clear_live_usable_models_account(),
        Err(_) => {}
    }
    result
}

/// Load the login state written by the Cursor desktop app on macOS.
///
/// This is intentionally a read-only, best-effort fallback for dashboard
/// consumers. The proxy's own auth store and the official Agent credentials
/// remain higher priority, so merely having Cursor Desktop open never changes
/// the credentials used by an existing proxy session.
pub fn load_cursor_desktop_auth() -> anyhow::Result<Option<CursorAuth>> {
    let Some(path) = cursor_desktop_state_db_path() else {
        return Ok(None);
    };
    if !path.is_file() {
        return Ok(None);
    }

    let sqlite = if std::path::Path::new("/usr/bin/sqlite3").is_file() {
        "/usr/bin/sqlite3"
    } else if std::path::Path::new("/opt/homebrew/bin/sqlite3").is_file() {
        "/opt/homebrew/bin/sqlite3"
    } else if std::path::Path::new("/usr/local/bin/sqlite3").is_file() {
        "/usr/local/bin/sqlite3"
    } else {
        "sqlite3"
    };
    let output = run_desktop_state_query(
        sqlite,
        &path,
        "SELECT hex(key), hex(value) FROM ItemTable WHERE key LIKE 'cursorAuth/%';",
    );
    let Some(output) = output else {
        return Ok(None);
    };
    if !output.status.success() {
        return Ok(None);
    }

    let mut values = std::collections::HashMap::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some((encoded_key, encoded_value)) = line.split_once('\t') else {
            continue;
        };
        let Some(key) = decode_hex_text(encoded_key) else {
            continue;
        };
        let Some(value) = decode_hex_text(encoded_value) else {
            continue;
        };
        let Some(key) = key.strip_prefix("cursorAuth/") else {
            continue;
        };
        values.insert(key.to_string(), value);
    }
    let access_token = values
        .get("accessToken")
        .map(String::as_str)
        .filter(|token| !token.trim().is_empty())
        .map(str::to_string);
    let Some(access_token) = access_token else {
        return Ok(None);
    };
    let refresh_token = values
        .get("refreshToken")
        .map(String::as_str)
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_string);

    let mut auth = enrich(
        StoredCursorAuth {
            access_token,
            refresh_token,
            api_key: None,
        },
        format!("cursor-desktop:{}", path.display()),
    );
    if let Some(email) = values
        .get("cachedEmail")
        .map(String::as_str)
        .map(str::trim)
        .filter(|email| !email.is_empty())
    {
        auth.email = Some(email.to_string());
    }
    Ok(Some(auth))
}

fn run_desktop_state_query(
    sqlite: &str,
    path: &std::path::Path,
    query: &str,
) -> Option<std::process::Output> {
    let mut child = Command::new(sqlite)
        .args(["-readonly", "-batch", "-noheader", "-separator", "\t"])
        .arg(path)
        .arg(query)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;
    let deadline = Instant::now() + DESKTOP_STATE_DB_QUERY_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

fn decode_hex_text(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(raw.len() / 2);
    for index in (0..raw.len()).step_by(2) {
        bytes.push(u8::from_str_radix(&raw[index..index + 2], 16).ok()?);
    }
    String::from_utf8(bytes).ok()
}

fn cursor_desktop_state_db_path() -> Option<std::path::PathBuf> {
    if let Some(path) = std::env::var_os("CCP_CURSOR_STATE_DB") {
        let path = std::path::PathBuf::from(path);
        return (!path.as_os_str().is_empty()).then_some(path);
    }
    #[cfg(target_os = "macos")]
    {
        dirs_home().map(|home| {
            home.join("Library/Application Support/Cursor/User/globalStorage/state.vscdb")
        })
    }
    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

fn cli_keychain_fallback_enabled() -> bool {
    match std::env::var("CCP_CURSOR_CLI_KEYCHAIN_FALLBACK") {
        Ok(raw) => matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on" | ""
        ),
        // Default on so `agent` login / CLI auth.json can power the proxy without re-login.
        Err(_) => true,
    }
}

/// Read Cursor Agent's Keychain item (`cursor-access-token` / `cursor-user`).
fn load_official_cli_keychain_auth() -> anyhow::Result<Option<CursorAuth>> {
    #[cfg(target_os = "macos")]
    {
        use crate::auth::{Keychain, SystemKeychain};
        let raw = match SystemKeychain.read("cursor-access-token", "cursor-user") {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };
        let Some(token) = raw.filter(|t| !t.trim().is_empty()) else {
            return Ok(None);
        };
        // CLI sometimes stores a bare JWT, sometimes JSON with accessToken.
        let stored = if token.trim_start().starts_with('{') {
            match serde_json::from_str::<StoredCursorAuth>(&token) {
                Ok(s) if !s.access_token.trim().is_empty() => s,
                _ => {
                    // Try common CLI shapes.
                    let parsed: serde_json::Value = match serde_json::from_str(&token) {
                        Ok(v) => v,
                        Err(_) => return Ok(None),
                    };
                    let access = parsed
                        .get("accessToken")
                        .or_else(|| parsed.get("access_token"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    if access.is_empty() {
                        return Ok(None);
                    }
                    StoredCursorAuth {
                        access_token: access,
                        refresh_token: parsed
                            .get("refreshToken")
                            .or_else(|| parsed.get("refresh_token"))
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        api_key: None,
                    }
                }
            }
        } else {
            StoredCursorAuth {
                access_token: token,
                refresh_token: None,
                api_key: None,
            }
        };
        Ok(Some(enrich(
            stored,
            "macos-keychain:cursor-access-token".to_string(),
        )))
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(None)
    }
}

/// Read official Cursor CLI `auth.json` (Linux/Windows / file credential store).
fn load_official_cli_auth_json() -> anyhow::Result<Option<CursorAuth>> {
    let candidates = official_cli_auth_json_candidates();
    for path in candidates {
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let parsed: serde_json::Value = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let access = parsed
            .get("accessToken")
            .or_else(|| parsed.get("access_token"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if access.is_empty() {
            continue;
        }
        let stored = StoredCursorAuth {
            access_token: access,
            refresh_token: parsed
                .get("refreshToken")
                .or_else(|| parsed.get("refresh_token"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
            api_key: parsed
                .get("apiKey")
                .or_else(|| parsed.get("api_key"))
                .and_then(|v| v.as_str())
                .map(str::to_string),
        };
        return Ok(Some(enrich(
            stored,
            format!("cli-auth.json:{}", path.display()),
        )));
    }
    Ok(None)
}

fn official_cli_auth_json_candidates() -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Some(home) = dirs_home() {
        out.push(home.join(".config/cursor/auth.json"));
        out.push(home.join(".cursor/auth.json"));
    }
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        out.insert(0, std::path::PathBuf::from(xdg).join("cursor/auth.json"));
    }
    if let Ok(appdata) = std::env::var("APPDATA") {
        out.push(std::path::PathBuf::from(appdata).join("Cursor/auth.json"));
    }
    out
}

fn dirs_home() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)
}

/// Force a refresh from the file/keychain store (ignores env-only tokens).
/// Pass the bearer token that 401'd so an already-rotated or hot-swapped
/// store is returned as-is instead of burning another rotation.
pub fn force_refresh_cursor_auth(
    failed_access_token: Option<&str>,
) -> anyhow::Result<Option<CursorAuth>> {
    if env_cursor_token().is_some() {
        anyhow::bail!(
            "CCP_CURSOR_AUTH_TOKEN/CURSOR_AUTH_TOKEN is set; those tokens cannot be refreshed. Unset env and use `claude-cursor-proxy cursor auth login`, or supply a fresh token."
        );
    }
    let result = file_store().force_refresh(failed_access_token);
    match &result {
        Ok(Some(auth)) => {
            crate::providers::cursor::model::observe_live_usable_models_account(&auth.access_token);
        }
        Ok(None) => crate::providers::cursor::model::clear_live_usable_models_account(),
        Err(_) => {}
    }
    result
}

/// Load only the bearer token for call sites that do not need auth metadata.
pub fn load_cursor_token() -> Option<String> {
    load_cursor_auth()
        .ok()
        .flatten()
        .map(|auth| auth.access_token)
}

pub fn save_cursor_auth(auth: StoredCursorAuth) -> anyhow::Result<CursorAuth> {
    let saved = file_store().save_auth(auth);
    if let Ok(ref auth) = saved {
        crate::providers::cursor::model::observe_live_usable_models_account(&auth.access_token);
    }
    saved
}

pub fn clear_cursor_auth() -> anyhow::Result<()> {
    let result = file_store().clear_auth();
    if result.is_ok() {
        crate::providers::cursor::model::clear_live_usable_models_account();
    }
    result
}

pub fn cursor_auth_location() -> String {
    file_store().auth_path()
}

pub fn missing_auth_message() -> String {
    [
        "Cursor authentication was not found.",
        "Run `claude-cursor-proxy cursor auth login`, or set CCP_CURSOR_AUTH_TOKEN/CURSOR_AUTH_TOKEN.",
        "On macOS the proxy also falls back to Cursor Agent Keychain (cursor-access-token) when CCP_CURSOR_CLI_KEYCHAIN_FALLBACK is on (default).",
        "The TUI usage view can also read Cursor Desktop state.vscdb on macOS when the proxy and Agent stores are empty.",
        "On Linux/Windows it also reads ~/.config/cursor/auth.json when that fallback is enabled.",
    ]
    .join(" ")
}

pub fn expired_auth_message(auth: &CursorAuth) -> String {
    let expires = auth
        .expires
        .map(format_unix_ms)
        .unwrap_or_else(|| "unknown".to_string());
    format!(
        "Cursor access token from {} is expired or near expiry ({}). Run `claude-cursor-proxy cursor auth login` again or set CCP_CURSOR_AUTH_TOKEN.",
        auth.source, expires
    )
}

pub fn create_cursor_login() -> CursorLogin {
    let verifier = random_base64_url(32);
    let challenge = base64_url(Sha256::digest(verifier.as_bytes()).as_ref());
    let uuid = uuid::Uuid::new_v4().to_string();
    let login_url = format!(
        "{CURSOR_WEBSITE_URL}/loginDeepControl?challenge={challenge}&uuid={uuid}&mode=login&redirectTarget=cli"
    );
    CursorLogin {
        login_url,
        uuid,
        verifier,
    }
}

pub fn run_cursor_login() -> anyhow::Result<Option<CursorAuth>> {
    let login = create_cursor_login();
    println!("Open this URL to authenticate with Cursor:");
    println!("{}", login.login_url);
    println!();
    if let Err(err) = open_cursor_login_url(&login.login_url) {
        println!("Could not open browser automatically: {err}");
    }
    println!("Waiting for Cursor login...");
    let result = wait_for_cursor_login(&login, 150, |attempt| {
        if attempt > 0 && attempt % 10 == 0 {
            print!(".");
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
    })?;
    let Some(result) = result else {
        return Ok(None);
    };
    save_cursor_auth(StoredCursorAuth {
        access_token: result.access_token,
        refresh_token: Some(result.refresh_token),
        api_key: None,
    })
    .map(Some)
}

pub fn wait_for_cursor_login(
    login: &CursorLogin,
    max_attempts: usize,
    mut on_progress: impl FnMut(usize),
) -> anyhow::Result<Option<RefreshResponse>> {
    let client = reqwest::blocking::Client::new();
    let base = config::cursor_base_url().trim_end_matches('/').to_string();
    let mut consecutive_errors = 0usize;

    for attempt in 0..max_attempts {
        let delay =
            Duration::from_millis((1000.0 * 1.2_f64.powi(attempt as i32)).min(10_000.0) as u64);
        let url = format!(
            "{base}/auth/poll?uuid={}&verifier={}",
            login.uuid, login.verifier
        );
        match client
            .get(url)
            .header("content-type", "application/json")
            .send()
        {
            Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => {
                consecutive_errors = 0;
                on_progress(attempt);
                std::thread::sleep(delay);
            }
            Ok(resp) if resp.status().is_success() => {
                let parsed: serde_json::Value = resp.json()?;
                return Ok(parse_cursor_auth_tokens(&parsed));
            }
            Ok(_) | Err(_) => {
                consecutive_errors += 1;
                if consecutive_errors >= 3 {
                    return Ok(None);
                }
                std::thread::sleep(delay);
            }
        }
    }
    Ok(None)
}

fn refresh_cursor_auth(refresh_token: &str) -> anyhow::Result<Option<RefreshResponse>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let url = format!(
        "{}/auth/refresh",
        config::cursor_base_url().trim_end_matches('/')
    );
    let resp = client
        .post(url)
        .header("content-type", "application/json")
        .bearer_auth(refresh_token)
        .body("{}")
        .send()?;
    if !resp.status().is_success() {
        return Ok(None);
    }
    let parsed: serde_json::Value = resp.json()?;
    Ok(parse_cursor_auth_tokens(&parsed))
}

fn parse_cursor_auth_tokens(parsed: &serde_json::Value) -> Option<RefreshResponse> {
    let access_token = parsed
        .get("accessToken")
        .or_else(|| parsed.get("access_token"))?
        .as_str()?
        .to_string();
    if access_token.is_empty() {
        return None;
    }
    // Refresh responses sometimes omit a rotated refresh token — keep empty and
    // let callers preserve the previous one.
    let refresh_token = parsed
        .get("refreshToken")
        .or_else(|| parsed.get("refresh_token"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    Some(RefreshResponse {
        access_token,
        refresh_token,
    })
}

fn env_cursor_token() -> Option<String> {
    env_cursor_token_from(|key| std::env::var(key).ok())
}

/// True when an env token shadows the persistent store. A `cursor auth login`
/// hot-swap will not take effect for a running `serve` in that case.
pub fn env_cursor_token_present() -> bool {
    env_cursor_token().is_some()
}

fn env_cursor_token_from(get: impl Fn(&str) -> Option<String>) -> Option<String> {
    get("CCP_CURSOR_AUTH_TOKEN")
        .filter(|token| !token.trim().is_empty())
        .or_else(|| get("CURSOR_AUTH_TOKEN").filter(|token| !token.trim().is_empty()))
}

fn enrich(stored: StoredCursorAuth, source: String) -> CursorAuth {
    let claims = parse_jwt_claims(&stored.access_token);
    CursorAuth {
        expires: token_expiry_ms(&stored.access_token),
        user_id: claims
            .as_ref()
            .and_then(|claims| claims.get("sub"))
            .and_then(|sub| sub.as_str())
            .map(str::to_string),
        email: claims
            .as_ref()
            .and_then(|claims| claims.get("email"))
            .and_then(|email| email.as_str())
            .map(str::to_string),
        source,
        access_token: stored.access_token,
        refresh_token: stored.refresh_token,
        api_key: stored.api_key,
    }
}

fn token_expiry_ms(token: &str) -> Option<u64> {
    parse_jwt_claims(token)?
        .get("exp")?
        .as_u64()
        .map(|exp| exp * 1000)
}

/// Stable, one-way identity for account-scoped runtime state.
///
/// Cursor rotates access JWTs for the same login. Keying cooldowns by the raw
/// bearer would therefore forget an active account limit after every refresh.
/// Prefer the stable JWT subject, then email for older tokens, and retain a
/// token-digest fallback for opaque credentials. Domain separators prevent the
/// same bytes in two different identity classes from colliding semantically.
pub(crate) fn cursor_account_digest(token: &str) -> String {
    let claims = parse_jwt_claims(token);
    let subject = claims
        .as_ref()
        .and_then(|claims| claims.get("sub"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let email = claims
        .as_ref()
        .and_then(|claims| claims.get("email"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase);

    let mut digest = Sha256::new();
    digest.update(b"claude-cursor-proxy:cursor-account:v1\0");
    if let Some(subject) = subject {
        digest.update(b"sub\0");
        digest.update(subject.as_bytes());
    } else if let Some(email) = email {
        digest.update(b"email\0");
        digest.update(email.as_bytes());
    } else {
        digest.update(b"token\0");
        digest.update(token.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn parse_jwt_claims(token: &str) -> Option<serde_json::Value> {
    let mut parts = token.split('.');
    let _header = parts.next()?;
    let payload = parts.next()?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| {
            let padded = format!("{payload}{}", "=".repeat((4 - payload.len() % 4) % 4));
            base64::engine::general_purpose::URL_SAFE.decode(padded)
        })
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

fn open_cursor_login_url(url: &str) -> anyhow::Result<()> {
    let status = if cfg!(target_os = "macos") {
        std::process::Command::new("open").arg(url).status()?
    } else if cfg!(target_os = "windows") {
        std::process::Command::new("cmd")
            .args(["/c", "start", "", url])
            .status()?
    } else {
        std::process::Command::new("xdg-open").arg(url).status()?
    };
    if !status.success() {
        anyhow::bail!("open command exited with {status}");
    }
    Ok(())
}

fn random_base64_url(len: usize) -> String {
    let mut bytes = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64_url(&bytes)
}

fn base64_url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn use_macos_keychain() -> bool {
    cfg!(target_os = "macos") && std::env::var_os("CCP_CONFIG_DIR").is_none()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn format_unix_ms(ms: u64) -> String {
    let secs = (ms / 1000) as i64;
    match time::OffsetDateTime::from_unix_timestamp(secs) {
        Ok(ts) => ts
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| ms.to_string()),
        Err(_) => ms.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::InMemoryAuthStore;

    #[test]
    fn auth_uses_cursor_auth_token_env() {
        let token = env_cursor_token_from(|key| match key {
            "CURSOR_AUTH_TOKEN" => Some("tok_from_cursor".into()),
            _ => None,
        });
        assert_eq!(token.as_deref(), Some("tok_from_cursor"));
    }

    #[test]
    fn auth_prioritizes_ccp_env_over_cursor_env() {
        let token = env_cursor_token_from(|key| match key {
            "CCP_CURSOR_AUTH_TOKEN" => Some("tok_ccp".into()),
            "CURSOR_AUTH_TOKEN" => Some("tok_cursor".into()),
            _ => None,
        });
        assert_eq!(token.as_deref(), Some("tok_ccp"));
    }

    #[test]
    fn auth_returns_none_when_not_set() {
        assert!(env_cursor_token_from(|_| None).is_none());
    }

    #[test]
    fn stored_auth_uses_camel_case_fields() {
        let auth: StoredCursorAuth = serde_json::from_value(serde_json::json!({
            "accessToken": "access",
            "refreshToken": "refresh",
            "apiKey": "api"
        }))
        .unwrap();
        assert_eq!(auth.access_token, "access");
        assert_eq!(auth.refresh_token.as_deref(), Some("refresh"));

        let value = serde_json::to_value(auth).unwrap();
        assert_eq!(value["accessToken"], "access");
        assert_eq!(value["refreshToken"], "refresh");
        assert!(value.get("access_token").is_none());
    }

    #[test]
    fn cursor_token_store_enriches_jwt_claims() {
        let store = CursorTokenStore::new(InMemoryAuthStore::new());
        let auth = store
            .save_auth(StoredCursorAuth {
                access_token: test_jwt(4_102_444_800, Some("user_1"), Some("me@example.com")),
                refresh_token: Some("refresh".into()),
                api_key: None,
            })
            .unwrap();

        assert_eq!(auth.user_id.as_deref(), Some("user_1"));
        assert_eq!(auth.email.as_deref(), Some("me@example.com"));
        assert_eq!(auth.expires, Some(4_102_444_800_000));
    }

    #[test]
    fn account_digest_survives_jwt_rotation_and_isolates_accounts() {
        let first = test_jwt(4_102_444_800, Some("user_1"), Some("old@example.com"));
        let rotated = test_jwt(4_102_444_900, Some("user_1"), Some("new@example.com"));
        let other = test_jwt(4_102_444_900, Some("user_2"), Some("old@example.com"));

        assert_eq!(
            cursor_account_digest(&first),
            cursor_account_digest(&rotated),
            "a refreshed JWT for the same subject must retain account cooldowns"
        );
        assert_ne!(cursor_account_digest(&first), cursor_account_digest(&other));
    }

    #[test]
    fn account_digest_uses_normalized_email_then_opaque_token_fallback() {
        let upper = test_jwt(4_102_444_800, None, Some(" Person@Example.COM "));
        let lower = test_jwt(4_102_444_900, None, Some("person@example.com"));
        assert_eq!(cursor_account_digest(&upper), cursor_account_digest(&lower));
        assert_ne!(
            cursor_account_digest("opaque-token-a"),
            cursor_account_digest("opaque-token-b")
        );
    }

    #[test]
    fn create_login_matches_cursor_deep_control_shape() {
        let login = create_cursor_login();
        assert!(
            login
                .login_url
                .starts_with("https://cursor.com/loginDeepControl?challenge=")
        );
        assert!(login.login_url.contains("&uuid="));
        assert!(login.login_url.contains("&mode=login&redirectTarget=cli"));
        assert!(!login.verifier.contains('='));
    }

    #[test]
    fn desktop_state_hex_decoder_handles_text_and_invalid_rows() {
        assert_eq!(
            decode_hex_text("636163686564456d61696c"),
            Some("cachedEmail".into())
        );
        assert_eq!(decode_hex_text("746f6b3a656e"), Some("tok:en".into()));
        assert!(decode_hex_text("0").is_none());
        assert!(decode_hex_text("zz").is_none());
    }

    fn test_jwt(exp: u64, sub: Option<&str>, email: Option<&str>) -> String {
        let mut payload = serde_json::json!({ "exp": exp });
        if let Some(sub) = sub {
            payload["sub"] = serde_json::Value::String(sub.to_string());
        }
        if let Some(email) = email {
            payload["email"] = serde_json::Value::String(email.to_string());
        }
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        format!("{header}.{payload}.sig")
    }
}
