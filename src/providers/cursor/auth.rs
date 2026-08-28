use anyhow::Context;
use base64::Engine;
use fs2::FileExt;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{
    fs::{self, File, OpenOptions},
    path::PathBuf,
};

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

/// One named Cursor login in the persistent multi-account registry.
///
/// The registry intentionally lives beside the legacy `auth.json`: that file
/// remains the active credential consumed by existing request code and by
/// older proxy binaries.  `accounts.json` is only an index of credentials that
/// can be selected with `cursor auth use` or from the TUI.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoredCursorAccount {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub auth: StoredCursorAuth,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct StoredCursorAccounts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_id: Option<String>,
    #[serde(default)]
    pub accounts: Vec<StoredCursorAccount>,
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

/// Public account view used by the TUI and account-management commands.
/// Credentials are present because usage fetches need the account's bearer;
/// callers should avoid displaying or logging `auth.access_token`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorAccountProfile {
    pub id: String,
    pub label: Option<String>,
    pub auth: CursorAuth,
    pub active: bool,
}

impl CursorAccountProfile {
    pub fn email(&self) -> Option<&str> {
        self.auth.email.as_deref()
    }

    pub fn display_name(&self) -> &str {
        self.label
            .as_deref()
            .filter(|label| !label.trim().is_empty())
            .or(self.auth.email.as_deref())
            .unwrap_or(&self.id)
    }
}

fn normalize_account_label(label: Option<String>) -> Option<String> {
    label.and_then(|label| {
        let trimmed = label.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

pub type DefaultCursorAuthStore = KeychainFileAuthStore<StoredCursorAuth, SystemKeychain>;

pub struct CursorTokenStore<S: AuthStorage<StoredCursorAuth>> {
    store: S,
    coordinate_account_registry: bool,
}

impl<S: AuthStorage<StoredCursorAuth>> CursorTokenStore<S> {
    pub fn new(store: S) -> Self {
        Self {
            store,
            coordinate_account_registry: false,
        }
    }

    fn with_account_registry_coordination(mut self) -> Self {
        self.coordinate_account_registry = true;
        self
    }

    fn lock_refresh_commit(&self) -> anyhow::Result<Option<File>> {
        self.coordinate_account_registry
            .then(lock_account_registry)
            .transpose()
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

    /// Load the raw stored credential without attempting a refresh. Account
    /// migration must preserve an expired login so a later explicit switch or
    /// refresh can still use it.
    pub fn load_stored_auth(&self) -> anyhow::Result<Option<StoredCursorAuth>> {
        self.store.load()
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
        // The process-wide mutex above does not coordinate separate proxy
        // processes. Hold the file lock across the re-check and refresh too,
        // so two `serve` processes cannot rotate one-shot refresh tokens
        // concurrently. Mutating account paths use the same lock but perform
        // only raw reads while holding it.
        let _registry_lock = self.lock_refresh_commit()?;
        // Re-check under the lock: another request may have refreshed, or
        // `cursor auth login` may have hot-swapped the account.
        match self.store.load()? {
            Some(current)
                if current.access_token != auth.access_token
                    || current.refresh_token != auth.refresh_token
                    || current.api_key != auth.api_key =>
            {
                return Ok(Some(enrich(current, self.auth_path())));
            }
            Some(_) => {}
            None => return Ok(None),
        }

        match refresh_cursor_auth(&refresh_token) {
            Ok(Some(refreshed)) => {
                // Account CAS and the write must share the cross-process
                // registry lock.  Otherwise a concurrent `auth use` can
                // write its selected account between these two operations,
                // after which this refresh would overwrite the hot switch
                // with the old account's rotated credential.
                if let Some(current) = self.store.load()?
                    && (current.access_token != auth.access_token
                        || current.refresh_token != auth.refresh_token
                        || current.api_key != auth.api_key)
                {
                    return Ok(Some(enrich(current, self.auth_path())));
                }
                let new_refresh = if refreshed.refresh_token.is_empty() {
                    auth.refresh_token.clone()
                } else {
                    Some(refreshed.refresh_token)
                };
                let next = StoredCursorAuth {
                    access_token: refreshed.access_token,
                    refresh_token: new_refresh,
                    api_key: auth.api_key.clone(),
                };
                let saved = self.save_auth(next.clone())?;
                sync_registry_after_auth_save_locked(
                    self.auth_path().as_str(),
                    &auth.access_token,
                    &next,
                );
                Ok(Some(saved))
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
        let _registry_lock = self.lock_refresh_commit()?;
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
        // Account CAS and the write share the cross-process registry lock
        // acquired above. Cursor refresh tokens are often one-shot, so the
        // lock also serializes refreshes across separate proxy processes.
        if let Some(current) = self.store.load()?
            && (current.access_token != stored.access_token
                || current.refresh_token != stored.refresh_token
                || current.api_key != stored.api_key)
        {
            return Ok(Some(enrich(current, self.auth_path())));
        }
        let new_refresh = if refreshed.refresh_token.is_empty() {
            auth.refresh_token.clone()
        } else {
            Some(refreshed.refresh_token)
        };
        let next = StoredCursorAuth {
            access_token: refreshed.access_token,
            refresh_token: new_refresh,
            api_key: auth.api_key,
        };
        let saved = self.save_auth(next.clone())?;
        sync_registry_after_auth_save_locked(
            self.auth_path().as_str(),
            &stored.access_token,
            &next,
        );
        Ok(Some(saved))
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
    .with_account_registry_coordination()
}

fn accounts_path() -> PathBuf {
    paths::provider_accounts_file("cursor")
}

fn accounts_lock_path() -> PathBuf {
    accounts_path()
        .parent()
        .map(|parent| parent.join("accounts.lock"))
        .unwrap_or_else(|| PathBuf::from("accounts.lock"))
}

/// Serialize account-pool mutations across proxy processes. The lock file is
/// deliberately separate from `accounts.json`; replacing the JSON atomically
/// while holding this lock keeps `auth.json` and its activeId mirror together.
fn lock_account_registry() -> anyhow::Result<File> {
    let path = accounts_lock_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&path)?
    };
    #[cfg(not(unix))]
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    file.lock_exclusive()?;
    Ok(file)
}

/// Human-readable location of the multi-account registry.
pub fn cursor_accounts_location() -> String {
    accounts_path().to_string_lossy().into_owned()
}

/// Return the persistent multi-account registry path for callers that need to
/// inspect or back up the account pool without duplicating path resolution.
pub fn cursor_accounts_path() -> PathBuf {
    accounts_path()
}

fn load_stored_accounts() -> anyhow::Result<Option<StoredCursorAccounts>> {
    let path = accounts_path();
    let raw = match fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(anyhow::Error::from(error).context(format!(
                "Failed to read Cursor accounts file {}",
                path.display()
            )));
        }
    };
    let mut stored: StoredCursorAccounts = serde_json::from_str(&raw).map_err(|error| {
        anyhow::anyhow!(
            "Failed to parse Cursor accounts file {}: {error}",
            path.display()
        )
    })?;
    // Ignore malformed/empty entries rather than letting one interrupted
    // manual edit make all other accounts unavailable.
    stored.accounts.retain(|account| {
        !account.id.trim().is_empty() && !account.auth.access_token.trim().is_empty()
    });
    stored.accounts.sort_by(|a, b| a.id.cmp(&b.id));
    if stored
        .active_id
        .as_ref()
        .is_some_and(|active| !stored.accounts.iter().any(|item| &item.id == active))
    {
        stored.active_id = stored.accounts.first().map(|account| account.id.clone());
    }
    Ok(Some(stored))
}

fn save_stored_accounts(stored: &StoredCursorAccounts) -> anyhow::Result<()> {
    let path = accounts_path();
    crate::auth::write_atomically(&path.to_string_lossy(), stored)
}

fn stored_account_id(auth: &StoredCursorAuth) -> String {
    cursor_account_digest(&auth.access_token)
}

fn stored_account_profile(
    account: &StoredCursorAccount,
    active_id: Option<&str>,
) -> CursorAccountProfile {
    CursorAccountProfile {
        id: account.id.clone(),
        label: account.label.clone(),
        auth: enrich(
            account.auth.clone(),
            accounts_path().to_string_lossy().into_owned(),
        ),
        active: active_id == Some(account.id.as_str()),
    }
}

fn synthetic_account_profile(auth: CursorAuth, active: bool) -> CursorAccountProfile {
    CursorAccountProfile {
        id: cursor_account_digest(&auth.access_token),
        label: auth.email.clone(),
        auth,
        active,
    }
}

/// Recover the selected account when the compatibility mirror was removed or
/// was written by an older binary that did not know about `accounts.json`.
/// The regular auth store remains the preferred source, so this is only a
/// fallback for an explicitly marked account.
fn load_active_registry_auth() -> anyhow::Result<Option<CursorAuth>> {
    let store = file_store();
    // Restore the compatibility mirror under the same lock used by account
    // switches. Drop the lock before calling `load_auth`: an expired registry
    // token may trigger refresh, whose commit path takes this lock again.
    {
        let _registry_lock = lock_account_registry()?;
        let Some(stored) = load_stored_accounts()? else {
            return Ok(None);
        };
        let Some(active_id) = stored.active_id.as_deref() else {
            return Ok(None);
        };
        let Some(account) = stored
            .accounts
            .iter()
            .find(|account| account.id == active_id)
        else {
            return Ok(None);
        };
        store.save_auth(account.auth.clone())?;
    }
    // Refresh (and persist any rotated refresh token) through the normal store
    // path. A concurrent switch is protected by the refresh CAS in
    // `CursorTokenStore`.
    store.load_auth()
}

fn upsert_stored_account(stored: &mut StoredCursorAccounts, account: StoredCursorAccount) {
    if let Some(existing) = stored
        .accounts
        .iter_mut()
        .find(|item| item.id == account.id)
    {
        *existing = account;
    } else {
        stored.accounts.push(account);
    }
}

fn load_store_auth_for_registry<S>(store: &CursorTokenStore<S>) -> Option<CursorAuth>
where
    S: AuthStorage<StoredCursorAuth>,
{
    // This helper is only called before account mutations take the registry
    // lock, so allow the normal refresh path to rotate an expiring token. If
    // refresh fails, preserve the raw credential so migration/listing still
    // shows the account and its refresh token.
    store.load_auth().ok().flatten().or_else(|| {
        store
            .load_stored_auth()
            .ok()
            .flatten()
            .filter(|auth| !auth.access_token.trim().is_empty())
            .map(|auth| enrich(auth, store.auth_path()))
    })
}

fn load_auth_for_registry_sync() -> Option<CursorAuth> {
    // Deliberately bypass `load_cursor_auth`: an environment bearer shadows
    // the persistent store and must never be copied into accounts.json during
    // login/add migration. The raw fallback also preserves an expired access
    // token (and its refresh token) when the normal refresh attempt fails.
    let store = file_store();
    load_store_auth_for_registry(&store).or_else(|| {
        if !cli_keychain_fallback_enabled() {
            return None;
        }
        load_official_cli_keychain_auth()
            .ok()
            .flatten()
            .or_else(|| load_official_cli_auth_json().ok().flatten())
    })
}

/// Synchronize the legacy active credential (including refresh-token
/// rotation) into the registry before a read-modify-write operation.
fn load_auth_for_registry_raw() -> Option<CursorAuth> {
    // This variant intentionally skips refresh. It is used while
    // `accounts.lock` is held; calling `load_auth_for_registry_sync` there
    // would recurse into the same lock when an access token is near expiry.
    let store = file_store();
    store
        .load_stored_auth()
        .ok()
        .flatten()
        .filter(|auth| !auth.access_token.trim().is_empty())
        .map(|auth| enrich(auth, store.auth_path()))
        .or_else(|| {
            if !cli_keychain_fallback_enabled() {
                return None;
            }
            load_official_cli_keychain_auth()
                .ok()
                .flatten()
                .or_else(|| load_official_cli_auth_json().ok().flatten())
        })
}

fn sync_active_registry(stored: &mut StoredCursorAccounts, active: Option<CursorAuth>) {
    let Some(active) = active else {
        return;
    };
    if active.access_token.trim().is_empty() {
        return;
    }
    let auth = StoredCursorAuth {
        access_token: active.access_token,
        refresh_token: active.refresh_token,
        api_key: active.api_key,
    };
    let digest = stored_account_id(&auth);
    let id = stored
        .active_id
        .clone()
        .filter(|id| {
            stored
                .accounts
                .iter()
                .find(|account| &account.id == id)
                .is_some_and(|account| stored_auth_identity_matches(&account.auth, &auth))
        })
        .or_else(|| {
            stored
                .accounts
                .iter()
                .find(|account| stored_auth_identity_matches(&account.auth, &auth))
                .map(|account| account.id.clone())
        })
        .unwrap_or(digest);
    let label = stored
        .accounts
        .iter()
        .find(|account| account.id == id)
        .and_then(|account| account.label.clone())
        .or(active.email);
    upsert_stored_account(
        stored,
        StoredCursorAccount {
            id: id.clone(),
            label,
            auth,
        },
    );
    stored.active_id = Some(id);
}

fn stored_auth_identity_matches(a: &StoredCursorAuth, b: &StoredCursorAuth) -> bool {
    if cursor_account_digest(&a.access_token) == cursor_account_digest(&b.access_token) {
        return true;
    }
    // Opaque access tokens do not carry a subject. A stable refresh token is
    // still enough to recognize the same account during a migration or an
    // explicit re-login, without treating an unrelated bearer as a rotation.
    matches!(
        (a.refresh_token.as_deref(), b.refresh_token.as_deref()),
        (Some(left), Some(right))
            if !left.trim().is_empty()
                && !right.trim().is_empty()
                && left.trim() == right.trim()
    )
}

fn sync_registry_after_auth_save_locked(
    auth_path: &str,
    previous_access_token: &str,
    next: &StoredCursorAuth,
) {
    // CursorTokenStore is generic for unit-testability. Only the production
    // file/keychain store owns this registry; custom in-memory stores must not
    // mutate the user's account pool as a side effect of a test refresh.
    let primary = paths::provider_auth_file("cursor");
    let is_default_store = auth_path == "macOS Keychain"
        || auth_path == primary.to_string_lossy()
        || auth_path == paths::provider_legacy_auth_file("cursor").to_string_lossy();
    if !is_default_store {
        return;
    }
    let Ok(Some(mut stored)) = load_stored_accounts() else {
        return;
    };
    if sync_registry_account_credentials(&mut stored, previous_access_token, next) {
        let _ = save_stored_accounts(&stored);
    }
}

/// Update the active registry entry after a successful access/refresh-token
/// rotation. Returning whether a write is needed keeps the filesystem wrapper
/// small and makes the identity/CAS rules directly testable.
fn sync_registry_account_credentials(
    stored: &mut StoredCursorAccounts,
    previous_access_token: &str,
    next: &StoredCursorAuth,
) -> bool {
    let Some(active_id) = stored.active_id.as_deref() else {
        return false;
    };
    let previous_digest = cursor_account_digest(previous_access_token);
    // A refresh must retain the account identity. If an upstream response
    // unexpectedly contains another subject, leave the registry untouched
    // rather than attaching that credential to the old account.
    if (cursor_token_has_stable_identity(previous_access_token)
        || cursor_token_has_stable_identity(&next.access_token))
        && cursor_account_digest(&next.access_token) != previous_digest
    {
        return false;
    }
    let Some(account) = stored
        .accounts
        .iter_mut()
        .find(|account| account.id == active_id)
    else {
        return false;
    };
    if account.id != previous_digest && stored_account_id(&account.auth) != previous_digest {
        return false;
    }
    if account.auth == *next {
        return false;
    }
    account.auth = next.clone();
    true
}

fn cursor_token_has_stable_identity(token: &str) -> bool {
    let Some(claims) = parse_jwt_claims(token) else {
        return false;
    };
    ["sub", "email"].iter().any(|key| {
        claims
            .get(*key)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
    })
}

/// List all persisted Cursor accounts.  When upgrading from a single-account
/// install, the active `auth.json` is exposed as an ephemeral profile and is
/// migrated to `accounts.json` on the next login/add/switch write.
pub fn list_cursor_accounts() -> anyhow::Result<Vec<CursorAccountProfile>> {
    let env_auth = env_cursor_token().map(|token| {
        synthetic_account_profile(
            enrich(
                StoredCursorAuth {
                    access_token: token,
                    refresh_token: None,
                    api_key: None,
                },
                "environment".to_string(),
            ),
            true,
        )
    });
    // Hold the registry lock for the complete persistent read/sync operation.
    // Read only the current raw mirror while locked: using a pre-lock auth
    // snapshot here could resurrect an account after a concurrent logout.
    let _registry_lock = if env_auth.is_none() {
        Some(lock_account_registry()?)
    } else {
        None
    };
    let active_auth = if env_auth.is_none() {
        // Include official Cursor CLI keychain/file fallback credentials. An
        // install with only Agent credentials must still appear in the TUI.
        load_auth_for_registry_raw()
    } else {
        None
    };
    let Some(mut stored) = load_stored_accounts()? else {
        return Ok(active_auth
            .map(|auth| vec![synthetic_account_profile(auth, true)])
            .or_else(|| env_auth.map(|auth| vec![auth]))
            .unwrap_or_default());
    };

    // Keep a rotated active JWT (same subject, new bearer) current in the
    // registry. The refresh path writes auth.json directly; syncing here
    // avoids restoring an expired token when the user later switches away and
    // back to this account. `sync_active_registry` also handles a
    // regular login performed by an older proxy binary.
    if env_auth.is_none() && active_auth.is_some() {
        let active = active_auth;
        let before = stored.clone();
        sync_active_registry(&mut stored, active);
        if stored != before {
            save_stored_accounts(&stored)?;
        }
    }
    let env_active = env_auth.is_some();
    let mut profiles: Vec<_> = stored
        .accounts
        .iter()
        .map(|account| {
            stored_account_profile(
                account,
                if env_active {
                    None
                } else {
                    stored.active_id.as_deref()
                },
            )
        })
        .collect();
    if let Some(env_auth) = env_auth {
        profiles.insert(0, env_auth);
    }
    profiles.sort_by_key(|profile| (!profile.active, profile.display_name().to_ascii_lowercase()));
    Ok(profiles)
}

/// Return the currently selected persisted account (or the env-backed account
/// when an environment token shadows persistent credentials).
pub fn current_cursor_account() -> anyhow::Result<Option<CursorAccountProfile>> {
    Ok(list_cursor_accounts()?
        .into_iter()
        .find(|profile| profile.active))
}

/// Return the active id without exposing the credential.  This is useful for
/// TUI selection state and for diagnostics.
pub fn active_cursor_account_id() -> anyhow::Result<Option<String>> {
    Ok(current_cursor_account()?.map(|profile| profile.id))
}

/// Persist `auth` as the active credential.  This preserves existing
/// `cursor auth login` semantics (new requests use the new login immediately)
/// while retaining the previous login in the registry so it can be selected
/// again later.  `cursor auth add` appends without changing the active login.
pub fn save_cursor_auth_as_active(
    auth: StoredCursorAuth,
    label: Option<String>,
) -> anyhow::Result<CursorAuth> {
    if auth.access_token.trim().is_empty() {
        anyhow::bail!("Cursor auth accessToken is required");
    }
    // A failed/expired previous login must not prevent a new browser login
    // from replacing it.
    // Refresh before taking the registry lock; the lock is also used by the
    // refresh commit path and is therefore not re-entrant.
    // Warm a near-expiry token before taking the registry lock. The value is
    // deliberately not used as a fallback below: if logout wins the race,
    // the locked raw read must remain empty rather than resurrecting the old
    // account from this pre-lock snapshot.
    let _ = load_auth_for_registry_sync();
    let _registry_lock = lock_account_registry()?;
    let mut stored = load_stored_accounts()?.unwrap_or_default();
    let previous = load_auth_for_registry_raw();
    sync_active_registry(&mut stored, previous.clone());
    // Preserve the account being replaced.  This is what makes a regular
    // `cursor auth login` behave like the historical account switch while
    // still allowing the user to switch back from the new registry.
    if let Some(previous) = previous.as_ref()
        && !previous.access_token.trim().is_empty()
    {
        let previous_stored = StoredCursorAuth {
            access_token: previous.access_token.clone(),
            refresh_token: previous.refresh_token.clone(),
            api_key: previous.api_key.clone(),
        };
        // Prefer the active registry id (including a user-supplied custom id)
        // when it still identifies this credential. A re-login should update
        // that entry rather than adding a second digest-keyed profile.
        let previous_id = stored
            .active_id
            .clone()
            .filter(|id| {
                stored
                    .accounts
                    .iter()
                    .find(|account| &account.id == id)
                    .is_some_and(|account| {
                        stored_auth_identity_matches(&account.auth, &previous_stored)
                    })
            })
            .or_else(|| {
                stored
                    .accounts
                    .iter()
                    .find(|account| stored_auth_identity_matches(&account.auth, &previous_stored))
                    .map(|account| account.id.clone())
            })
            .unwrap_or_else(|| stored_account_id(&previous_stored));
        let previous_label = stored
            .accounts
            .iter()
            .find(|account| account.id == previous_id)
            .and_then(|account| account.label.clone())
            .or_else(|| previous.email.clone());
        upsert_stored_account(
            &mut stored,
            StoredCursorAccount {
                id: previous_id.clone(),
                label: previous_label,
                auth: previous_stored,
            },
        );
        if stored.active_id.is_none() {
            stored.active_id = Some(previous_id);
        }
    }
    let proposed_id = stored_account_id(&auth);
    // Preserve hand-authored/legacy ids when a normal login rotates the same
    // account's credentials, just as `auth add` does.
    let id = stored
        .accounts
        .iter()
        .find(|account| {
            account.id == proposed_id || stored_auth_identity_matches(&account.auth, &auth)
        })
        .map(|account| account.id.clone())
        .unwrap_or(proposed_id);
    let saved = file_store().save_auth(auth.clone())?;
    let account_label = normalize_account_label(label)
        .or_else(|| {
            stored
                .accounts
                .iter()
                .find(|account| account.id == id)
                .and_then(|account| normalize_account_label(account.label.clone()))
        })
        .or_else(|| saved.email.clone());
    let account = StoredCursorAccount {
        id: id.clone(),
        label: account_label,
        auth,
    };
    upsert_stored_account(&mut stored, account);
    stored.active_id = Some(id);
    save_stored_accounts(&stored)?;
    crate::providers::cursor::model::observe_live_usable_models_account(&saved.access_token);
    Ok(saved)
}

/// Append a Cursor login to the account registry without changing the active
/// credential. Re-adding the same identity updates its credentials instead of
/// creating duplicates. The first account is activated automatically.
pub fn add_cursor_auth(
    auth: StoredCursorAuth,
    label: Option<String>,
) -> anyhow::Result<CursorAuth> {
    if auth.access_token.trim().is_empty() {
        anyhow::bail!("Cursor auth accessToken is required");
    }
    if env_cursor_token().is_some() {
        anyhow::bail!(
            "An environment Cursor token is active; unset CCP_CURSOR_AUTH_TOKEN/CURSOR_AUTH_TOKEN before adding accounts"
        );
    }
    // An expired active token is still a valid registry entry to migrate; its
    // refresh failure should not block adding another account.
    // Include credentials supplied by the official Cursor CLI fallback when
    // the proxy's own active file is empty; adding a second account should
    // never silently drop the first login.
    // Read/refresh before locking, then prefer the raw mirror after locking.
    // Warm a near-expiry token before taking the registry lock, but never use
    // this pre-lock snapshot as a fallback after a concurrent logout.
    let _ = load_auth_for_registry_sync();
    let _registry_lock = lock_account_registry()?;
    let mut stored = load_stored_accounts()?.unwrap_or_default();
    let previous = load_auth_for_registry_raw();
    sync_active_registry(&mut stored, previous.clone());
    // Migrate a pre-multi-account active login before replacing it with the
    // newly added account.
    if stored.accounts.is_empty()
        && let Some(previous) = previous.as_ref()
        && !previous.access_token.trim().is_empty()
    {
        let previous_stored = StoredCursorAuth {
            access_token: previous.access_token.clone(),
            refresh_token: previous.refresh_token.clone(),
            api_key: previous.api_key.clone(),
        };
        // Preserve a user-facing/legacy id when the account already exists in
        // the registry.  Falling back to the bearer digest is only needed for
        // a pre-registry login; otherwise a normal account switch could leave
        // a duplicate entry for the same identity when it had a custom id.
        let previous_id = stored
            .accounts
            .iter()
            .find(|account| stored_auth_identity_matches(&account.auth, &previous_stored))
            .map(|account| account.id.clone())
            .unwrap_or_else(|| stored_account_id(&previous_stored));
        stored.accounts.push(StoredCursorAccount {
            id: previous_id.clone(),
            label: previous.email.clone(),
            auth: previous_stored,
        });
        stored.active_id = Some(previous_id);
    }
    let proposed_id = stored_account_id(&auth);
    // Keep hand-authored/legacy ids stable when the bearer rotates. New
    // registry entries use the digest, while an existing entry may have a
    // shorter id from an earlier version of the proxy.
    let id = stored
        .accounts
        .iter()
        .find(|account| {
            account.id == proposed_id || stored_auth_identity_matches(&account.auth, &auth)
        })
        .map(|account| account.id.clone())
        .unwrap_or(proposed_id);
    // `add` keeps the selected account active. When the newly authenticated
    // credentials resolve to that same account identity, however, the active
    // compatibility mirror must also receive the rotated access/refresh
    // tokens. Otherwise the next registry sync would overwrite this fresh
    // entry with the stale `auth.json` credential.
    let replaces_active = stored.active_id.as_deref() == Some(id.as_str());
    let account_label = normalize_account_label(label)
        .or_else(|| {
            stored
                .accounts
                .iter()
                .find(|account| account.id == id)
                .and_then(|account| normalize_account_label(account.label.clone()))
        })
        .or_else(|| enrich(auth.clone(), accounts_path().to_string_lossy().into_owned()).email);
    let account = StoredCursorAccount {
        id: id.clone(),
        label: account_label,
        auth,
    };
    upsert_stored_account(&mut stored, account);
    let first_account = stored.active_id.is_none();
    if first_account {
        stored.active_id = Some(id.clone());
    }
    save_stored_accounts(&stored)?;
    if first_account || replaces_active {
        let saved = file_store().save_auth(
            stored
                .accounts
                .iter()
                .find(|item| item.id == id)
                .map(|item| item.auth.clone())
                .expect("just inserted first Cursor account"),
        )?;
        crate::providers::cursor::model::observe_live_usable_models_account(&saved.access_token);
        Ok(saved)
    } else {
        Ok(enrich(
            stored
                .accounts
                .iter()
                .find(|item| item.id == id)
                .map(|item| item.auth.clone())
                .expect("just inserted Cursor account"),
            accounts_path().to_string_lossy().into_owned(),
        ))
    }
}

/// Switch the active account by id and hot-write `auth.json`; running proxy
/// requests pick this up on their next request without a process restart.
pub fn switch_cursor_account(id: &str) -> anyhow::Result<CursorAccountProfile> {
    let id = id.trim();
    if id.is_empty() {
        anyhow::bail!("Cursor account id is required");
    }
    if env_cursor_token().is_some() {
        anyhow::bail!(
            "An environment Cursor token is active; unset CCP_CURSOR_AUTH_TOKEN/CURSOR_AUTH_TOKEN before switching accounts"
        );
    }
    let _registry_lock = lock_account_registry()?;
    let mut stored = load_stored_accounts()?.unwrap_or_default();
    let active = load_auth_for_registry_raw();
    sync_active_registry(&mut stored, active.clone());
    if stored.accounts.is_empty()
        && let Some(active) = active
        && !active.access_token.trim().is_empty()
    {
        let active_stored = StoredCursorAuth {
            access_token: active.access_token,
            refresh_token: active.refresh_token,
            api_key: active.api_key,
        };
        let active_id = stored_account_id(&active_stored);
        stored.active_id = Some(active_id.clone());
        stored.accounts.push(StoredCursorAccount {
            id: active_id,
            label: active.email,
            auth: active_stored,
        });
    }
    let account = stored
        .accounts
        .iter()
        .find(|account| account.id == id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Cursor account not found: {id}"))?;
    let saved = file_store().save_auth(account.auth.clone())?;
    stored.active_id = Some(account.id.clone());
    save_stored_accounts(&stored)?;
    crate::providers::cursor::model::observe_live_usable_models_account(&saved.access_token);
    Ok(CursorAccountProfile {
        id: account.id,
        label: account.label,
        auth: saved,
        active: true,
    })
}

/// Remove a persisted account and return the account that remains active.
/// Removing the active account immediately activates the first remaining
/// account, if any; removing an inactive account leaves the current account
/// and its model catalog untouched.
pub fn remove_cursor_account(id: &str) -> anyhow::Result<Option<CursorAccountProfile>> {
    let id = id.trim();
    if id.is_empty() {
        anyhow::bail!("Cursor account id is required");
    }
    if env_cursor_token().is_some() {
        anyhow::bail!(
            "An environment Cursor token is active; unset CCP_CURSOR_AUTH_TOKEN/CURSOR_AUTH_TOKEN before removing accounts"
        );
    }
    let _registry_lock = lock_account_registry()?;
    let mut stored = load_stored_accounts()?.unwrap_or_default();
    let active = load_auth_for_registry_raw();
    sync_active_registry(&mut stored, active);
    let before = stored.accounts.len();
    stored.accounts.retain(|account| account.id != id);
    if stored.accounts.len() == before {
        anyhow::bail!("Cursor account not found: {id}");
    }
    let was_active = stored.active_id.as_deref() == Some(id);
    let mut active_account = stored
        .active_id
        .as_deref()
        .and_then(|active| stored.accounts.iter().find(|account| account.id == active))
        .cloned();
    if was_active {
        active_account = stored.accounts.first().cloned();
        stored.active_id = active_account.as_ref().map(|account| account.id.clone());
        if let Some(account) = active_account.as_ref() {
            let _ = file_store().save_auth(account.auth.clone())?;
        } else {
            file_store().clear_auth()?;
        }
    } else if stored
        .active_id
        .as_ref()
        .is_some_and(|active| !stored.accounts.iter().any(|account| &account.id == active))
    {
        stored.active_id = stored.accounts.first().map(|account| account.id.clone());
        active_account = stored.accounts.first().cloned();
    }
    save_stored_accounts(&stored)?;
    if let Some(account) = active_account.as_ref() {
        crate::providers::cursor::model::observe_live_usable_models_account(
            &account.auth.access_token,
        );
    } else {
        crate::providers::cursor::model::clear_live_usable_models_account();
    }
    Ok(active_account.map(|account| CursorAccountProfile {
        id: account.id,
        label: account.label,
        auth: enrich(account.auth, accounts_path().to_string_lossy().into_owned()),
        active: true,
    }))
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
        if let Some(auth) = load_active_registry_auth()? {
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

/// Refresh one account without switching the process-wide active account.
///
/// Account usage is intentionally allowed to inspect inactive profiles. A
/// profile can outlive its access JWT while its refresh token remains valid;
/// callers should use this helper before dashboard requests so one stale
/// account does not require a global `auth use` round trip. Rotated
/// credentials are persisted back to `accounts.json`, and to `auth.json` only
/// when the account is still active at commit time.
pub fn refresh_cursor_account_for_usage(
    profile: &CursorAccountProfile,
) -> anyhow::Result<CursorAuth> {
    // Environment credentials have no refresh token and intentionally shadow
    // the persistent account pool.
    if profile.auth.source == "environment" {
        return Ok(profile.auth.clone());
    }
    let Some(profile_expires) = profile.auth.expires else {
        return Ok(profile.auth.clone());
    };
    if profile_expires > now_ms() + REFRESH_EXPIRY_SKEW_MS {
        return Ok(profile.auth.clone());
    }

    // Serialize refresh-token rotation in this process. Cursor invalidates a
    // previous refresh token in some deployments, so concurrent workers for
    // the same account must not spend two rotations.
    let _flight = refresh_single_flight();

    // Serialize the complete re-check/network/CAS sequence across proxy
    // processes. Cursor refresh tokens can be one-shot; taking the registry
    // lock before the request prevents two TUI/CLI workers from consuming the
    // same refresh token concurrently. Account management uses the same lock
    // and only performs raw reads while holding it, so this cannot recurse.
    let _registry_lock = lock_account_registry()?;
    // Prefer the latest registry entry in case a previous usage worker or a
    // concurrent account switch already updated this profile.
    let stored_before_refresh = load_stored_accounts()?.and_then(|stored| {
        stored
            .accounts
            .into_iter()
            .find(|item| item.id == profile.id)
    });
    let (old, registry_backed) = match stored_before_refresh {
        Some(account) => (
            enrich(account.auth, accounts_path().to_string_lossy().into_owned()),
            true,
        ),
        None if profile.active => (profile.auth.clone(), false),
        None => {
            anyhow::bail!("Cursor account no longer exists: {}", profile.id);
        }
    };
    let Some(refresh_token) = old.refresh_token.as_deref() else {
        return Ok(old);
    };
    if let Some(expires) = old.expires
        && expires > now_ms() + REFRESH_EXPIRY_SKEW_MS
    {
        return Ok(old);
    }

    let refreshed = match refresh_cursor_auth(refresh_token) {
        Ok(Some(refreshed)) => refreshed,
        Ok(None) if old.expires.is_some_and(|expires| expires <= now_ms()) => {
            anyhow::bail!(
                "Cursor access token expired and account refresh was rejected for {}",
                profile.id
            )
        }
        Ok(None) => return Ok(old),
        Err(error) if old.expires.is_some_and(|expires| expires <= now_ms()) => {
            return Err(error).context(format!(
                "Cursor account refresh failed after access token expiry ({})",
                profile.id
            ));
        }
        Err(_) => return Ok(old),
    };
    let next = StoredCursorAuth {
        access_token: refreshed.access_token,
        refresh_token: if refreshed.refresh_token.is_empty() {
            old.refresh_token.clone()
        } else {
            Some(refreshed.refresh_token)
        },
        api_key: old.api_key.clone(),
    };
    // A refresh response must not silently move an account's identity to a
    // different JWT subject. Opaque tokens have no stable claims, so the
    // account id/CAS below remains the authority for that legacy shape.
    if (cursor_token_has_stable_identity(&old.access_token)
        || cursor_token_has_stable_identity(&next.access_token))
        && cursor_account_digest(&old.access_token) != cursor_account_digest(&next.access_token)
    {
        anyhow::bail!("Cursor account refresh returned a different account identity");
    }

    let mut stored = load_stored_accounts()?.unwrap_or_default();
    if registry_backed {
        let account = stored
            .accounts
            .iter_mut()
            .find(|account| account.id == profile.id)
            .ok_or_else(|| anyhow::anyhow!("Cursor account no longer exists: {}", profile.id))?;
        // Compare the bearer captured before the network call. If another
        // worker rotated or switched this account, its result is already the
        // freshest credential and must win over this response.
        if account.auth.access_token != old.access_token {
            return Ok(enrich(
                account.auth.clone(),
                accounts_path().to_string_lossy().into_owned(),
            ));
        }
        account.auth = next.clone();
        let active = stored.active_id.as_deref() == Some(profile.id.as_str());
        if active {
            file_store().save_auth(next.clone())?;
        }
        save_stored_accounts(&stored)?;
        let saved = enrich(next, accounts_path().to_string_lossy().into_owned());
        if active {
            crate::providers::cursor::model::observe_live_usable_models_account(
                &saved.access_token,
            );
        }
        Ok(saved)
    } else {
        // Upgrade installs can expose one active auth.json profile before the
        // registry has been created. Refresh that mirror with a CAS so a hot
        // `auth use` cannot be overwritten by this network response.
        let store = file_store();
        if let Some(current) = store.load_stored_auth()?
            && current.access_token != old.access_token
        {
            return Ok(enrich(current, store.auth_path()));
        }
        let saved = store.save_auth(next)?;
        Ok(saved)
    }
}

/// Load only the bearer token for call sites that do not need auth metadata.
pub fn load_cursor_token() -> Option<String> {
    load_cursor_auth()
        .ok()
        .flatten()
        .map(|auth| auth.access_token)
}

pub fn save_cursor_auth(auth: StoredCursorAuth) -> anyhow::Result<CursorAuth> {
    save_cursor_auth_as_active(auth, None)
}

pub fn clear_cursor_auth() -> anyhow::Result<()> {
    let _registry_lock = lock_account_registry()?;
    // Write the inactive marker first. If clearing the compatibility mirror
    // fails, the still-valid auth file remains usable and the next registry
    // sync can restore its active marker; a successful logout can never be
    // resurrected from accounts.json.
    let previous_active_id = if let Some(mut stored) = load_stored_accounts()? {
        let previous_active_id = stored.active_id.clone();
        stored.active_id = None;
        save_stored_accounts(&stored)?;
        Some(previous_active_id)
    } else {
        None
    };
    if let Err(error) = file_store().clear_auth() {
        // Preserve the pre-logout state when the mirror cannot be removed;
        // callers can retry without silently losing the selected account.
        if let Some(previous_active_id) = previous_active_id
            && let Ok(Some(mut stored)) = load_stored_accounts()
        {
            stored.active_id = previous_active_id;
            let _ = save_stored_accounts(&stored);
        }
        return Err(error);
    }
    crate::providers::cursor::model::clear_live_usable_models_account();
    Ok(())
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
    run_cursor_login_with_mode(false, None)
}

/// Browser login used by `cursor auth add`.  The login flow is identical to
/// normal login, but the resulting credentials are appended to the registry.
pub fn run_cursor_login_add() -> anyhow::Result<Option<CursorAuth>> {
    run_cursor_login_with_mode(true, None)
}

/// Browser login used by `cursor auth add --label`.  Keeping the label in the
/// same transaction avoids writing the freshly authenticated account once
/// without a label and then issuing a second read/modify/write just to attach
/// metadata.
pub fn run_cursor_login_add_with_label(
    label: Option<String>,
) -> anyhow::Result<Option<CursorAuth>> {
    run_cursor_login_with_mode(true, label)
}

fn run_cursor_login_with_mode(
    append: bool,
    label: Option<String>,
) -> anyhow::Result<Option<CursorAuth>> {
    if append && env_cursor_token().is_some() {
        anyhow::bail!(
            "An environment Cursor token is active; unset CCP_CURSOR_AUTH_TOKEN/CURSOR_AUTH_TOKEN before adding accounts"
        );
    }
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
    let stored = StoredCursorAuth {
        access_token: result.access_token,
        refresh_token: Some(result.refresh_token),
        api_key: None,
    };
    let saved = if append {
        add_cursor_auth(stored, label)?
    } else {
        save_cursor_auth(stored)?
    };
    Ok(Some(saved))
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

/// Return the stable opaque id used by the account registry for a bearer.
/// Access tokens are never returned or logged; this helper is useful to
/// correlate the result of a login/refresh with a profile after Cursor rotates
/// the JWT.
pub fn cursor_account_id_for_token(token: &str) -> String {
    cursor_account_digest(token)
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
    use crate::auth::{AuthStorage, InMemoryAuthStore};
    use std::sync::{Arc, atomic::AtomicUsize};

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
    fn stored_accounts_use_stable_registry_shape() {
        let value = serde_json::to_value(StoredCursorAccounts {
            active_id: Some("account-a".into()),
            accounts: vec![StoredCursorAccount {
                id: "account-a".into(),
                label: Some("work".into()),
                auth: StoredCursorAuth {
                    access_token: "access".into(),
                    refresh_token: Some("refresh".into()),
                    api_key: None,
                },
            }],
        })
        .unwrap();
        assert_eq!(value["activeId"], "account-a");
        assert_eq!(value["accounts"][0]["id"], "account-a");
        assert_eq!(value["accounts"][0]["auth"]["accessToken"], "access");
        assert!(value["accounts"][0]["auth"].get("access_token").is_none());
    }

    #[test]
    fn account_profile_display_name_prefers_label_then_email_then_id() {
        let auth = CursorAuth {
            access_token: "token".into(),
            refresh_token: None,
            api_key: None,
            expires: None,
            user_id: None,
            email: Some("mail@example.com".into()),
            source: "test".into(),
        };
        let mut profile = CursorAccountProfile {
            id: "account-id".into(),
            label: Some("Work".into()),
            auth: auth.clone(),
            active: false,
        };
        assert_eq!(profile.display_name(), "Work");
        profile.label = None;
        assert_eq!(profile.display_name(), "mail@example.com");
        profile.auth.email = None;
        assert_eq!(profile.display_name(), "account-id");
    }

    #[test]
    fn account_usage_refresh_leaves_fresh_profile_untouched() {
        let auth = enrich(
            StoredCursorAuth {
                access_token: test_jwt(
                    4_102_444_800,
                    Some("usage-user"),
                    Some("usage@example.com"),
                ),
                refresh_token: Some("refresh-token".into()),
                api_key: None,
            },
            "accounts.json".into(),
        );
        let profile = CursorAccountProfile {
            id: cursor_account_digest(&auth.access_token),
            label: Some("usage".into()),
            auth: auth.clone(),
            active: false,
        };

        let refreshed = refresh_cursor_account_for_usage(&profile).unwrap();
        assert_eq!(refreshed.access_token, auth.access_token);
        assert_eq!(refreshed.refresh_token, auth.refresh_token);
    }

    #[test]
    fn account_usage_refresh_does_not_rotate_environment_credentials() {
        let auth = enrich(
            StoredCursorAuth {
                access_token: test_jwt(1, Some("env-user"), Some("env@example.com")),
                refresh_token: None,
                api_key: None,
            },
            "environment".into(),
        );
        let profile = CursorAccountProfile {
            id: cursor_account_digest(&auth.access_token),
            label: None,
            auth: auth.clone(),
            active: true,
        };

        let refreshed = refresh_cursor_account_for_usage(&profile).unwrap();
        assert_eq!(refreshed, auth);
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
    fn registry_sync_falls_back_to_raw_auth_after_refresh_load_error() {
        // Simulate an expired credential whose refresh attempt failed. The
        // second raw read must still migrate both the access and refresh token.
        let raw = StoredCursorAuth {
            access_token: test_jwt(1, Some("legacy-user"), Some("legacy@example.com")),
            refresh_token: Some("legacy-refresh".to_string()),
            api_key: Some("legacy-api".to_string()),
        };
        let store = CursorTokenStore::new(FailFirstLoadStore {
            value: raw.clone(),
            loads: Arc::new(AtomicUsize::new(0)),
        });

        let recovered = load_store_auth_for_registry(&store).expect("raw credential fallback");
        assert_eq!(recovered.access_token, raw.access_token);
        assert_eq!(recovered.refresh_token, raw.refresh_token);
        assert_eq!(recovered.api_key, raw.api_key);
        assert_eq!(recovered.email.as_deref(), Some("legacy@example.com"));
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
    fn registry_refresh_updates_active_account_and_preserves_id() {
        let old = test_jwt(4_102_444_800, Some("user_1"), Some("old@example.com"));
        let next = test_jwt(4_102_444_900, Some("user_1"), Some("new@example.com"));
        let mut stored = StoredCursorAccounts {
            active_id: Some("stable-account".to_string()),
            accounts: vec![StoredCursorAccount {
                id: "stable-account".to_string(),
                label: Some("work".to_string()),
                auth: StoredCursorAuth {
                    access_token: old.clone(),
                    refresh_token: Some("refresh-old".to_string()),
                    api_key: None,
                },
            }],
        };
        let changed = sync_registry_account_credentials(
            &mut stored,
            &old,
            &StoredCursorAuth {
                access_token: next.clone(),
                refresh_token: Some("refresh-new".to_string()),
                api_key: None,
            },
        );
        assert!(changed);
        assert_eq!(stored.active_id.as_deref(), Some("stable-account"));
        assert_eq!(stored.accounts[0].auth.access_token, next);
        assert_eq!(stored.accounts[0].label.as_deref(), Some("work"));
    }

    #[test]
    fn registry_refresh_rejects_identity_change() {
        let old = test_jwt(4_102_444_800, Some("user_1"), Some("old@example.com"));
        let other = test_jwt(4_102_444_900, Some("user_2"), Some("other@example.com"));
        let mut stored = StoredCursorAccounts {
            active_id: Some(cursor_account_digest(&old)),
            accounts: vec![StoredCursorAccount {
                id: cursor_account_digest(&old),
                label: None,
                auth: StoredCursorAuth {
                    access_token: old.clone(),
                    refresh_token: Some("refresh-old".to_string()),
                    api_key: None,
                },
            }],
        };
        assert!(!sync_registry_account_credentials(
            &mut stored,
            &old,
            &StoredCursorAuth {
                access_token: other,
                refresh_token: Some("refresh-other".to_string()),
                api_key: None,
            },
        ));
        assert_eq!(stored.accounts[0].auth.access_token, old);
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

    #[derive(Clone)]
    struct FailFirstLoadStore {
        value: StoredCursorAuth,
        loads: Arc<AtomicUsize>,
    }

    impl AuthStorage<StoredCursorAuth> for FailFirstLoadStore {
        fn load(&self) -> anyhow::Result<Option<StoredCursorAuth>> {
            if self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                anyhow::bail!("simulated refresh load failure");
            }
            Ok(Some(self.value.clone()))
        }

        fn save(&self, _value: StoredCursorAuth) -> anyhow::Result<()> {
            Ok(())
        }

        fn clear(&self) -> anyhow::Result<()> {
            Ok(())
        }

        fn path(&self) -> String {
            "test-auth".to_string()
        }
    }
}
