//! Read-only diagnostics for the Cursor SandClientMode route.
//!
//! The Sand client mode is deliberately request-scoped: a single proxy can
//! send some model turns through the managed-local/H2 surface and leave other
//! turns on the ordinary CLI surface.  This module exposes the effective
//! policy and the identity/transport markers without opening a Cursor request
//! or printing credential material.  It is used by `cursor sand-status` and is
//! also useful to callers embedding the proxy as a library.

use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{config, paths};

/// Sand transport/header capabilities compiled into this proxy.
///
/// These are local implementation facts, not a desktop bundle probe.  The
/// separate `desktop_bundle` report below is populated by a read-only scan of
/// Cursor's JavaScript files and must be used when checking the patched app.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandProtocolMarkers {
    /// Route eligible Sand requests through Cursor's managed-local runtime.
    pub managed_local_route: bool,
    /// Force the local runtime/Agent Host load path.
    pub local_runtime_load: bool,
    /// Use the direct inference stream selected by the local runtime.
    pub direct_stream: bool,
    /// Advertise the desktop Agent Host identity (`clientType: "sand"`).
    pub agent_host_identity: bool,
    /// Keep the local exec/resource bridge enabled for native tools.
    pub exec_resource_bridge: bool,
}

impl SandProtocolMarkers {
    /// Return the capabilities implemented by this proxy.
    pub const fn current() -> Self {
        Self {
            managed_local_route: true,
            local_runtime_load: true,
            direct_stream: true,
            agent_host_identity: true,
            exec_resource_bridge: true,
        }
    }

    pub const fn all_enabled(self) -> bool {
        self.managed_local_route
            && self.local_runtime_load
            && self.direct_stream
            && self.agent_host_identity
            && self.exec_resource_bridge
    }
}

/// Read-only status of the separately installed Cursor Desktop bundle.
///
/// The reference SandClientMode/SandStreamToolkit tools patch Cursor's desktop
/// JavaScript bundle. The proxy implements its own request-scoped route and
/// does not depend on that patch; this separate probe is informational only.
/// All paths and marker counts here come from local files, and no app is
/// started or modified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DesktopSandPatchStatus {
    /// Always false for this proxy. Exposed so JSON diagnostics cannot confuse
    /// an unpatched Desktop bundle with a broken proxy Sand route.
    pub required_for_proxy: bool,
    pub detected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub managed_local_route_markers: usize,
    pub local_runtime_load_markers: usize,
    pub direct_stream_markers: usize,
    pub agent_host_enablement_markers: usize,
    pub agent_host_identity_markers: usize,
    /// SandStreamToolkit v1.3.2's background/multitask route marker.
    pub multitask_route_markers: usize,
    /// Aggregate of its four required task-session markers: features,
    /// capture, config, and task.
    pub subagent_task_markers: usize,
    /// Aggregate of its two required subagent-route markers: run-options and
    /// background-completion.
    pub subagent_route_markers: usize,
    pub exec_bridge_markers: usize,
    pub move_exec_markers: usize,
    pub patched_files: Vec<String>,
    pub stream_mode_ready: bool,
    /// Legacy SandClientMode readiness for the explicit exec/resource bridge.
    pub exec_bridge_ready: bool,
    /// SandStreamToolkit v1.3.2 readiness for its multitask/subagent marker
    /// family. This is intentionally separate from the legacy bridge check:
    /// the toolkit does not inject the latter's two bridge markers.
    pub stream_toolkit_ready: bool,
    /// The scanned Desktop patch is ready under either supported marker
    /// family. This remains informational; the proxy itself never requires a
    /// Desktop patch.
    pub ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

impl DesktopSandPatchStatus {
    fn not_detected(diagnostic: impl Into<String>) -> Self {
        Self {
            required_for_proxy: false,
            detected: false,
            app_path: None,
            version: None,
            managed_local_route_markers: 0,
            local_runtime_load_markers: 0,
            direct_stream_markers: 0,
            agent_host_enablement_markers: 0,
            agent_host_identity_markers: 0,
            multitask_route_markers: 0,
            subagent_task_markers: 0,
            subagent_route_markers: 0,
            exec_bridge_markers: 0,
            move_exec_markers: 0,
            patched_files: Vec::new(),
            stream_mode_ready: false,
            exec_bridge_ready: false,
            stream_toolkit_ready: false,
            ready: false,
            diagnostic: Some(diagnostic.into()),
        }
    }
}

/// Credential-free account information included in Sand diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandAccountStatus {
    pub id: String,
    pub label: Option<String>,
    pub email: Option<String>,
    pub active: bool,
    /// Whether a successful dashboard snapshot exists for this account.
    pub cached_usage: bool,
    /// Unix timestamp (milliseconds) of the cached snapshot, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_usage_fetched_at_ms: Option<u64>,
}

/// Effective Sand configuration and read-only account/cache diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SandStatusSnapshot {
    /// Stable identifier for scripts consuming `--json` output.
    pub protocol: &'static str,
    pub markers: SandProtocolMarkers,
    /// `markers_ready` describes only this proxy's request transport.
    pub markers_ready: bool,
    /// Read-only inspection result for the separate Cursor Desktop app.
    pub desktop_bundle: DesktopSandPatchStatus,
    /// Whether at least one model pattern currently selects Sand.
    pub enabled: bool,
    pub policy_source: &'static str,
    pub model_patterns: Vec<String>,
    /// Process-wide fallback identity for Cursor models not matched by the
    /// Sand policy.  Matched requests always use `sand` regardless of this.
    pub default_client_type: String,
    pub client_profile: String,
    pub sand_client_version: String,
    pub base_url: String,
    /// `h2-only` for HTTPS and `h2-prior-knowledge` for cleartext fixtures.
    pub transport: &'static str,
    pub local_client_mode: bool,
    pub desktop_identity_headers: bool,
    pub ghost_mode_header: String,
    pub new_onboarding_completed: bool,
    pub model_account_routes: Vec<config::CursorModelAccountRule>,
    pub accounts: Vec<SandAccountStatus>,
    pub usage_cache_path: String,
    pub usage_cache_accounts: usize,
    /// If account discovery failed, status remains useful and reports the
    /// error here instead of turning a read-only diagnostic into a hard fail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_error: Option<String>,
}

/// Build a status snapshot without contacting Cursor's API.
pub fn snapshot() -> SandStatusSnapshot {
    let policy = config::cursor_sand_policy();
    let model_patterns = policy.patterns().to_vec();
    let policy_source = if std::env::var_os("CCP_CURSOR_SAND_MODELS").is_some() {
        "environment"
    } else {
        "config"
    };
    let base_url = config::cursor_base_url();
    let cleartext = base_url.starts_with("http://");
    let markers = SandProtocolMarkers::current();
    let desktop_bundle = inspect_desktop_bundle();
    let default_client_type = config::cursor_client_type();
    let globally_sand = default_client_type.trim().eq_ignore_ascii_case("sand");

    let usage_cache_path = paths::cursor_usage_cache_file(&paths::DirResolverEnv::default());
    let usage_cache = crate::providers::cursor::usage::load_account_usage_cache();
    let usage_metadata = crate::providers::cursor::usage::load_account_usage_cache_metadata();

    let (accounts, account_error) = match crate::providers::cursor::auth::list_cursor_accounts() {
        Ok(profiles) => {
            let rows = profiles
                .into_iter()
                .map(|profile| {
                    let metadata = usage_metadata.get(&profile.id);
                    let cached_usage = usage_cache.contains_key(&profile.id);
                    SandAccountStatus {
                        id: profile.id,
                        label: profile.label,
                        email: profile.auth.email,
                        active: profile.active,
                        cached_usage,
                        cached_usage_fetched_at_ms: metadata
                            .and_then(|item| epoch_ms(item.fetched_at)),
                    }
                })
                .collect();
            (rows, None)
        }
        Err(error) => (Vec::new(), Some(error.to_string())),
    };

    SandStatusSnapshot {
        protocol: "sand-client-mode",
        markers,
        markers_ready: markers.all_enabled(),
        desktop_bundle,
        enabled: globally_sand || !model_patterns.is_empty(),
        policy_source,
        model_patterns,
        default_client_type,
        client_profile: config::cursor_client_profile(),
        sand_client_version: config::cursor_client_version_for_type("sand"),
        base_url,
        transport: if cleartext {
            "h2-prior-knowledge"
        } else {
            "h2-only"
        },
        local_client_mode: config::cursor_local_client_mode("sand"),
        // Sand always takes the desktop common-header path, even when the
        // process-wide profile remains `cli`.
        desktop_identity_headers: true,
        ghost_mode_header: config::cursor_ghost_mode_header(),
        new_onboarding_completed: config::cursor_new_onboarding_completed(),
        model_account_routes: config::cursor_account_routing_policy().routes().to_vec(),
        accounts,
        usage_cache_path: usage_cache_path.to_string_lossy().into_owned(),
        usage_cache_accounts: usage_cache.len(),
        account_error,
    }
}

const SAND_MANAGED_LOCAL_ROUTE_MARKER: &str = "/*SAND_MANAGED_LOCAL_ROUTE_V1*/";
const SAND_LOCAL_RUNTIME_LOAD_MARKER: &str = "/*SAND_LOCAL_RUNTIME_LOAD_V1*/";
const SAND_DIRECT_STREAM_MARKER: &str = "/*SAND_DIRECT_INFERENCE_STREAM_V1*/";
const SAND_AGENT_HOST_ENABLEMENT_MARKER: &str = "/*SAND_AGENT_HOST_ENABLEMENT_V1*/";
const SAND_AGENT_HOST_IDENTITY_MARKER: &str = "/*SAND_AGENT_HOST_IDENTITY_V1*/";
const SAND_EXEC_BRIDGE_MARKER: &str = "/*SAND_EXEC_BRIDGE_V1*/";
const SAND_BR_RESOURCE_BRIDGE_MARKER: &str = "/*SAND_BR_RESOURCE_BRIDGE_V1*/";
const SAND_MOVE_EXEC_MARKER: &str = "/*SAND_MOVE_EXEC_V1*/";
const SAND_MULTITASK_ROUTE_MARKER: &str = "/*SAND_MULTITASK_ROUTE_V1*/";
const SAND_SUBAGENT_FEATURES_MARKER: &str = "/*SAND_SUBAGENT_FEATURES_V1*/";
const SAND_SUBAGENT_CAPTURE_MARKER: &str = "/*SAND_SUBAGENT_CAPTURE_V1*/";
const SAND_SUBAGENT_CONFIG_MARKER: &str = "/*SAND_SUBAGENT_CONFIG_V1*/";
const SAND_SUBAGENT_TASK_MARKER: &str = "/*SAND_SUBAGENT_TASK_V1*/";
const SAND_SUBAGENT_RUN_OPTIONS_MARKER: &str = "/*SAND_SUBAGENT_RUN_OPTIONS_V1*/";
const SAND_BACKGROUND_COMPLETION_MARKER: &str = "/*SAND_BACKGROUND_COMPLETION_V1*/";

const DESKTOP_TARGETS: &[&str] = &[
    "out/main.js",
    "out/vs/workbench/api/worker/extensionHostWorkerMain.js",
    "out/vs/workbench/api/node/extensionHostProcess.js",
    "out/vs/workbench/workbench.glass.main.js",
    "out/vs/workbench/workbench.desktop.main.js",
    "extensions/cursor-always-local/dist/main.js",
    "extensions/cursor-local-agent-runtime/dist/main.js",
    "extensions/cursor-agent-host/dist/main.js",
    "extensions/cursor-agent-exec/dist/main.js",
];

fn inspect_desktop_bundle() -> DesktopSandPatchStatus {
    inspect_desktop_bundle_candidates(desktop_app_candidates())
}

fn inspect_desktop_bundle_candidates(candidates: Vec<PathBuf>) -> DesktopSandPatchStatus {
    let Some(app_root) = candidates.into_iter().find_map(resolve_desktop_app_root) else {
        return DesktopSandPatchStatus::not_detected(
            "Cursor Desktop bundle was not found; set CCP_CURSOR_APP to inspect a custom install",
        );
    };

    let version = desktop_bundle_version(&app_root);
    let mut counts = MarkerCounts::default();
    let mut patched_files = Vec::new();
    let mut unreadable_files = 0usize;
    let mut scanned = Vec::new();
    for relative in DESKTOP_TARGETS {
        scanned.push(app_root.join(relative));
    }
    // Agent Host uses versioned chunks for some bridge code.  Scan only this
    // known extension directory, rather than walking the whole 1GB app.
    let chunks = app_root.join("extensions/cursor-agent-host/dist");
    if let Ok(entries) = fs::read_dir(chunks) {
        for entry in entries.flatten().take(512) {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("js") {
                scanned.push(path);
            }
        }
    }

    for path in scanned {
        let Ok(metadata) = fs::metadata(&path) else {
            unreadable_files += 1;
            continue;
        };
        // A malformed fixture should not make a status command allocate an
        // unbounded buffer.  Cursor chunks are normally well below this cap.
        if !metadata.is_file() || metadata.len() > 64 * 1024 * 1024 {
            unreadable_files += 1;
            continue;
        }
        let Ok(content) = fs::read_to_string(&path) else {
            unreadable_files += 1;
            continue;
        };
        let file_counts = MarkerCounts::from_content(&content);
        if file_counts.total() > 0 {
            counts += file_counts;
            if let Ok(relative) = path.strip_prefix(&app_root) {
                patched_files.push(relative.to_string_lossy().into_owned());
            } else {
                patched_files.push(path.to_string_lossy().into_owned());
            }
        }
    }
    patched_files.sort();
    patched_files.dedup();
    let stream_mode_ready = counts.managed_local_route > 0
        && counts.local_runtime_load > 0
        && counts.direct_stream > 0
        && counts.agent_host_enablement > 0
        && counts.agent_host_identity > 0;
    // SandClientMode and SandStreamToolkit use mutually different desktop
    // completion markers. The older installer adds exec/resource bridges;
    // v1.3.2 adds the full multitask/subagent route instead. Do not require
    // both, otherwise a healthy Toolkit installation is reported as partial.
    let exec_bridge_ready = counts.move_exec > 0 && counts.exec_bridge >= 2;
    let stream_toolkit_ready = stream_mode_ready
        && counts.move_exec > 0
        && counts.multitask_route > 0
        && counts.subagent_task == 4
        && counts.subagent_route == 2;
    let diagnostic = (unreadable_files > 0).then(|| {
        format!(
            "{unreadable_files} Cursor Desktop target file(s) could not be read; marker readiness may be incomplete"
        )
    });
    DesktopSandPatchStatus {
        required_for_proxy: false,
        detected: true,
        app_path: Some(app_root.to_string_lossy().into_owned()),
        version,
        managed_local_route_markers: counts.managed_local_route,
        local_runtime_load_markers: counts.local_runtime_load,
        direct_stream_markers: counts.direct_stream,
        agent_host_enablement_markers: counts.agent_host_enablement,
        agent_host_identity_markers: counts.agent_host_identity,
        multitask_route_markers: counts.multitask_route,
        subagent_task_markers: counts.subagent_task,
        subagent_route_markers: counts.subagent_route,
        exec_bridge_markers: counts.exec_bridge,
        move_exec_markers: counts.move_exec,
        patched_files,
        stream_mode_ready,
        exec_bridge_ready,
        stream_toolkit_ready,
        ready: (stream_mode_ready && exec_bridge_ready) || stream_toolkit_ready,
        diagnostic,
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct MarkerCounts {
    managed_local_route: usize,
    local_runtime_load: usize,
    direct_stream: usize,
    agent_host_enablement: usize,
    agent_host_identity: usize,
    multitask_route: usize,
    subagent_task: usize,
    subagent_route: usize,
    exec_bridge: usize,
    move_exec: usize,
}

impl MarkerCounts {
    fn from_content(content: &str) -> Self {
        Self {
            managed_local_route: content.matches(SAND_MANAGED_LOCAL_ROUTE_MARKER).count(),
            local_runtime_load: content.matches(SAND_LOCAL_RUNTIME_LOAD_MARKER).count(),
            direct_stream: content.matches(SAND_DIRECT_STREAM_MARKER).count(),
            agent_host_enablement: content.matches(SAND_AGENT_HOST_ENABLEMENT_MARKER).count(),
            agent_host_identity: content.matches(SAND_AGENT_HOST_IDENTITY_MARKER).count(),
            multitask_route: content.matches(SAND_MULTITASK_ROUTE_MARKER).count(),
            subagent_task: [
                SAND_SUBAGENT_FEATURES_MARKER,
                SAND_SUBAGENT_CAPTURE_MARKER,
                SAND_SUBAGENT_CONFIG_MARKER,
                SAND_SUBAGENT_TASK_MARKER,
            ]
            .into_iter()
            .map(|marker| content.matches(marker).count())
            .sum(),
            subagent_route: [
                SAND_SUBAGENT_RUN_OPTIONS_MARKER,
                SAND_BACKGROUND_COMPLETION_MARKER,
            ]
            .into_iter()
            .map(|marker| content.matches(marker).count())
            .sum(),
            exec_bridge: content.matches(SAND_EXEC_BRIDGE_MARKER).count()
                + content.matches(SAND_BR_RESOURCE_BRIDGE_MARKER).count(),
            move_exec: content.matches(SAND_MOVE_EXEC_MARKER).count(),
        }
    }

    fn total(self) -> usize {
        self.managed_local_route
            + self.local_runtime_load
            + self.direct_stream
            + self.agent_host_enablement
            + self.agent_host_identity
            + self.multitask_route
            + self.subagent_task
            + self.subagent_route
            + self.exec_bridge
            + self.move_exec
    }
}

impl std::ops::AddAssign for MarkerCounts {
    fn add_assign(&mut self, other: Self) {
        self.managed_local_route += other.managed_local_route;
        self.local_runtime_load += other.local_runtime_load;
        self.direct_stream += other.direct_stream;
        self.agent_host_enablement += other.agent_host_enablement;
        self.agent_host_identity += other.agent_host_identity;
        self.multitask_route += other.multitask_route;
        self.subagent_task += other.subagent_task;
        self.subagent_route += other.subagent_route;
        self.exec_bridge += other.exec_bridge;
        self.move_exec += other.move_exec;
    }
}

fn desktop_app_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(path) = std::env::var_os("CCP_CURSOR_APP") {
        candidates.push(PathBuf::from(path));
    }
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        candidates.push(home.join("Applications/Cursor.app"));
        candidates.push(home.join(".local/share/cursor"));
    }
    candidates.push(PathBuf::from("/Applications/Cursor.app"));
    candidates
}

fn resolve_desktop_app_root(candidate: PathBuf) -> Option<PathBuf> {
    let candidate = candidate.canonicalize().ok()?;
    let roots = [
        candidate.clone(),
        candidate.join("Contents/Resources/app"),
        candidate.join("resources/app"),
        candidate.join("Resources/app"),
    ];
    roots.into_iter().find(|root| {
        root.is_dir() && (root.join("out").is_dir() || root.join("extensions").is_dir())
    })
}

fn desktop_bundle_version(app_root: &Path) -> Option<String> {
    for path in [
        app_root.join("package.json"),
        app_root.join("product.json"),
        app_root.join("Contents/Resources/app/package.json"),
    ] {
        let Ok(raw) = fs::read_to_string(path) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&raw) else {
            continue;
        };
        if let Some(version) = value.get("version").and_then(serde_json::Value::as_str) {
            let version = version.trim();
            if !version.is_empty() {
                return Some(version.to_string());
            }
        }
    }
    None
}

fn epoch_ms(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
}

/// Human-readable status intended for a terminal, with no secrets.
pub fn render_text(status: &SandStatusSnapshot) -> String {
    let mut out = String::new();
    out.push_str("SandClientMode status\n");
    let routing_detail = if status.model_patterns.is_empty()
        && status
            .default_client_type
            .trim()
            .eq_ignore_ascii_case("sand")
    {
        "global default: sand".to_string()
    } else if status.model_patterns.is_empty() {
        "no model patterns".to_string()
    } else {
        status.model_patterns.join(", ")
    };
    out.push_str(&format!(
        "  routing: {} ({})\n",
        if status.enabled {
            "enabled"
        } else {
            "disabled"
        },
        routing_detail
    ));
    out.push_str(&format!(
        "  policy source: {}\n  default client: {}\n  profile: {}\n",
        status.policy_source, status.default_client_type, status.client_profile
    ));
    out.push_str(&format!(
        "  transport: {}\n  Sand client version: {}\n  base URL: {}\n",
        status.transport, status.sand_client_version, status.base_url
    ));
    out.push_str(&format!(
        "  proxy transport: {}\n  proxy markers: managed-local: {}  local-runtime: {}  direct-stream: {}\n",
        marker(status.markers_ready),
        marker(status.markers.managed_local_route),
        marker(status.markers.local_runtime_load),
        marker(status.markers.direct_stream)
    ));
    out.push_str(&format!(
        "  proxy headers: Agent Host identity: {}  exec/resource bridge: {}\n",
        marker(status.markers.agent_host_identity),
        marker(status.markers.exec_resource_bridge)
    ));
    let desktop_state = if !status.desktop_bundle.detected {
        "not detected"
    } else if status.desktop_bundle.ready {
        "ready"
    } else if status.desktop_bundle.stream_mode_ready
        || status.desktop_bundle.exec_bridge_ready
        || status.desktop_bundle.stream_toolkit_ready
    {
        "partial"
    } else {
        "detected, unpatched"
    };
    out.push_str(&format!(
        "  Desktop bundle (optional; not required by proxy): {desktop_state}"
    ));
    if let Some(path) = status.desktop_bundle.app_path.as_deref() {
        out.push_str(&format!(" ({path})"));
    }
    if let Some(version) = status.desktop_bundle.version.as_deref() {
        out.push_str(&format!("  version: {version}"));
    }
    out.push('\n');
    out.push_str(&format!(
        "  Desktop patch: stream-mode: {}  SandClientMode exec/resource: {}  SandStreamToolkit multitask/subagents: {}  files: {}\n",
        marker(status.desktop_bundle.stream_mode_ready),
        marker(status.desktop_bundle.exec_bridge_ready),
        marker(status.desktop_bundle.stream_toolkit_ready),
        status.desktop_bundle.patched_files.len()
    ));
    if let Some(diagnostic) = status.desktop_bundle.diagnostic.as_deref() {
        out.push_str(&format!("  Desktop diagnostic: {diagnostic}\n"));
    }
    out.push_str(&format!(
        "  local-client-mode: {}  desktop headers: {}  ghost: {}  onboarding: {}\n",
        marker(status.local_client_mode),
        marker(status.desktop_identity_headers),
        status.ghost_mode_header,
        status.new_onboarding_completed
    ));
    out.push_str(&format!(
        "  accounts: {}  usage cache: {} account(s)\n  usage cache path: {}\n",
        status.accounts.len(),
        status.usage_cache_accounts,
        status.usage_cache_path
    ));
    for account in &status.accounts {
        let active = if account.active { '*' } else { ' ' };
        let name = account
            .label
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .or(account.email.as_deref())
            .unwrap_or(account.id.as_str());
        let cached = match account.cached_usage_fetched_at_ms {
            Some(fetched_at) => format!("cached at {fetched_at}ms"),
            None if account.cached_usage => "cached".to_string(),
            None => "no-cache".to_string(),
        };
        out.push_str(&format!(
            "    {active} {name}  ({})  {cached}\n",
            account.id
        ));
    }
    if let Some(error) = status.account_error.as_deref() {
        out.push_str(&format!("  account discovery: {error}\n"));
    }
    out
}

fn marker(value: bool) -> &'static str {
    if value { "ready" } else { "missing" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn current_markers_cover_sand_client_mode_injection_points() {
        let markers = SandProtocolMarkers::current();
        assert!(markers.all_enabled());
        assert!(markers.managed_local_route);
        assert!(markers.local_runtime_load);
        assert!(markers.direct_stream);
        assert!(markers.agent_host_identity);
        assert!(markers.exec_resource_bridge);
    }

    #[test]
    fn text_status_is_secret_free_and_mentions_h2_transport() {
        let status = SandStatusSnapshot {
            protocol: "sand-client-mode",
            markers: SandProtocolMarkers::current(),
            markers_ready: true,
            desktop_bundle: DesktopSandPatchStatus::not_detected("test"),
            enabled: true,
            policy_source: "environment",
            model_patterns: vec!["claude-fable-5".into()],
            default_client_type: "cli".into(),
            client_profile: "cli".into(),
            sand_client_version: "3.17.19".into(),
            base_url: "https://api2.cursor.sh".into(),
            transport: "h2-only",
            local_client_mode: true,
            desktop_identity_headers: true,
            ghost_mode_header: "implicit-false".into(),
            new_onboarding_completed: false,
            model_account_routes: Vec::new(),
            accounts: Vec::new(),
            usage_cache_path: "/tmp/account-usage.json".into(),
            usage_cache_accounts: 0,
            account_error: None,
        };
        let text = render_text(&status);
        assert!(text.contains("SandClientMode status"));
        assert!(text.contains("h2-only"));
        assert!(text.contains("managed-local: ready"));
        assert!(text.contains("not required by proxy"));
        assert!(!text.contains("accessToken"));
        assert!(!text.contains("refreshToken"));
    }

    #[test]
    fn global_sand_default_is_rendered_as_an_enabled_route() {
        let mut status = SandStatusSnapshot {
            protocol: "sand-client-mode",
            markers: SandProtocolMarkers::current(),
            markers_ready: true,
            desktop_bundle: DesktopSandPatchStatus::not_detected("test"),
            enabled: true,
            policy_source: "config",
            model_patterns: Vec::new(),
            default_client_type: "SAND".into(),
            client_profile: "cli".into(),
            sand_client_version: "3.17.19".into(),
            base_url: "https://api2.cursor.sh".into(),
            transport: "h2-only",
            local_client_mode: true,
            desktop_identity_headers: true,
            ghost_mode_header: "implicit-false".into(),
            new_onboarding_completed: false,
            model_account_routes: Vec::new(),
            accounts: Vec::new(),
            usage_cache_path: "/tmp/account-usage.json".into(),
            usage_cache_accounts: 0,
            account_error: None,
        };
        assert!(render_text(&status).contains("routing: enabled (global default: sand)"));

        status.default_client_type = "cli".into();
        status.enabled = false;
        assert!(render_text(&status).contains("routing: disabled (no model patterns)"));
    }

    #[test]
    fn desktop_scan_distinguishes_detected_unpatched_bundle() {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("Cursor.app/Contents/Resources/app");
        fs::create_dir_all(root.join("out")).expect("out");
        fs::write(root.join("package.json"), r#"{"version":"9.9.9"}"#).expect("package");

        let status = inspect_desktop_bundle_candidates(vec![temp.path().join("Cursor.app")]);
        assert!(!status.required_for_proxy);
        assert!(status.detected);
        assert_eq!(status.version.as_deref(), Some("9.9.9"));
        assert!(!status.ready);
        assert!(!status.stream_mode_ready);
        assert!(!status.exec_bridge_ready);
        assert!(!status.stream_toolkit_ready);
        assert!(status.patched_files.is_empty());
    }

    #[test]
    fn desktop_scan_counts_reference_markers_without_mutating_files() {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("Cursor.app/Contents/Resources/app");
        fs::create_dir_all(root.join("out/vs/workbench")).expect("workbench");
        fs::create_dir_all(root.join("extensions/cursor-agent-host/dist")).expect("agent host");
        fs::write(root.join("package.json"), r#"{"version":"1.2.3"}"#).expect("package");
        let main = format!(
            "{SAND_MANAGED_LOCAL_ROUTE_MARKER}{SAND_LOCAL_RUNTIME_LOAD_MARKER}{SAND_DIRECT_STREAM_MARKER}{SAND_AGENT_HOST_ENABLEMENT_MARKER}{SAND_AGENT_HOST_IDENTITY_MARKER}"
        );
        let bridge = format!(
            "{SAND_MOVE_EXEC_MARKER}{SAND_EXEC_BRIDGE_MARKER}{SAND_BR_RESOURCE_BRIDGE_MARKER}"
        );
        fs::write(root.join("out/main.js"), &main).expect("main");
        fs::write(
            root.join("extensions/cursor-agent-host/dist/123.js"),
            &bridge,
        )
        .expect("bridge");

        let status = inspect_desktop_bundle_candidates(vec![temp.path().join("Cursor.app")]);
        assert!(!status.required_for_proxy);
        assert!(status.detected);
        assert!(status.ready);
        assert!(status.stream_mode_ready);
        assert!(status.exec_bridge_ready);
        assert!(!status.stream_toolkit_ready);
        assert_eq!(status.managed_local_route_markers, 1);
        assert_eq!(status.exec_bridge_markers, 2);
        assert_eq!(status.patched_files.len(), 2);
        assert_eq!(fs::read_to_string(root.join("out/main.js")).unwrap(), main);
    }

    #[test]
    fn desktop_scan_accepts_complete_sand_stream_toolkit_v1_3_2_markers() {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("Cursor.app/Contents/Resources/app");
        fs::create_dir_all(root.join("out")).expect("out");
        fs::create_dir_all(root.join("extensions/cursor-agent-host/dist")).expect("agent host");
        fs::write(root.join("package.json"), r#"{"version":"3.18.9"}"#).expect("package");
        let core = format!(
            "{SAND_MANAGED_LOCAL_ROUTE_MARKER}{SAND_LOCAL_RUNTIME_LOAD_MARKER}{SAND_DIRECT_STREAM_MARKER}{SAND_AGENT_HOST_ENABLEMENT_MARKER}{SAND_AGENT_HOST_IDENTITY_MARKER}"
        );
        // This is the exact v1.3.2 set recovered from the Windows bundle's
        // PatchStatus.stream_mode_installed property. It deliberately has no
        // SandClientMode exec/resource bridge markers.
        let toolkit = format!(
            "{SAND_MOVE_EXEC_MARKER}{SAND_MULTITASK_ROUTE_MARKER}{SAND_SUBAGENT_FEATURES_MARKER}{SAND_SUBAGENT_CAPTURE_MARKER}{SAND_SUBAGENT_CONFIG_MARKER}{SAND_SUBAGENT_TASK_MARKER}{SAND_SUBAGENT_RUN_OPTIONS_MARKER}{SAND_BACKGROUND_COMPLETION_MARKER}"
        );
        fs::write(root.join("out/main.js"), core).expect("main");
        fs::write(
            root.join("extensions/cursor-agent-host/dist/675.js"),
            toolkit,
        )
        .expect("toolkit");

        let status = inspect_desktop_bundle_candidates(vec![temp.path().join("Cursor.app")]);
        assert!(!status.required_for_proxy);
        assert!(status.detected);
        assert!(status.stream_mode_ready);
        assert!(!status.exec_bridge_ready);
        assert!(status.stream_toolkit_ready);
        assert!(status.ready);
        assert_eq!(status.move_exec_markers, 1);
        assert_eq!(status.multitask_route_markers, 1);
        assert_eq!(status.subagent_task_markers, 4);
        assert_eq!(status.subagent_route_markers, 2);
        assert_eq!(status.exec_bridge_markers, 0);
    }

    #[test]
    fn desktop_scan_requires_the_complete_toolkit_subagent_marker_counts() {
        let temp = tempfile::tempdir().expect("temp dir");
        let root = temp.path().join("Cursor.app/Contents/Resources/app");
        fs::create_dir_all(root.join("out")).expect("out");
        fs::create_dir_all(root.join("extensions/cursor-agent-host/dist")).expect("agent host");
        let core = format!(
            "{SAND_MANAGED_LOCAL_ROUTE_MARKER}{SAND_LOCAL_RUNTIME_LOAD_MARKER}{SAND_DIRECT_STREAM_MARKER}{SAND_AGENT_HOST_ENABLEMENT_MARKER}{SAND_AGENT_HOST_IDENTITY_MARKER}"
        );
        // Five task markers do not meet the Toolkit's exact `== 4` check.
        let incomplete_toolkit = format!(
            "{SAND_MOVE_EXEC_MARKER}{SAND_MULTITASK_ROUTE_MARKER}{SAND_SUBAGENT_FEATURES_MARKER}{SAND_SUBAGENT_CAPTURE_MARKER}{SAND_SUBAGENT_CONFIG_MARKER}{SAND_SUBAGENT_TASK_MARKER}{SAND_SUBAGENT_TASK_MARKER}{SAND_SUBAGENT_RUN_OPTIONS_MARKER}{SAND_BACKGROUND_COMPLETION_MARKER}"
        );
        fs::write(root.join("out/main.js"), core).expect("main");
        fs::write(
            root.join("extensions/cursor-agent-host/dist/657.js"),
            incomplete_toolkit,
        )
        .expect("toolkit");

        let status = inspect_desktop_bundle_candidates(vec![temp.path().join("Cursor.app")]);
        assert!(status.stream_mode_ready);
        assert_eq!(status.subagent_task_markers, 5);
        assert_eq!(status.subagent_route_markers, 2);
        assert!(!status.stream_toolkit_ready);
        assert!(!status.ready);
    }
}
