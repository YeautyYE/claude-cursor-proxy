//! Diagnostics for Claude Code MCP state.
//!
//! This module deliberately has no process-control side effects.  It reads
//! Claude Code's project registry, discovers the locally installed Lobster
//! plugin, samples matching processes, and summarizes the Lobster log.  The
//! optional repair operation only removes Lobster entries from the selected
//! project's `disabledMcpServers` array.  A backup is made before an atomic
//! replacement of `~/.claude.json`.

use anyhow::{Context, Result, anyhow};
use clap::{Args, Parser};
use once_cell::sync::Lazy;
use regex_lite::Regex;
use serde::Serialize;
use serde_json::{Map, Value};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Canonical Claude Code server id used by the Lobster channel plugin.
pub const LOBSTER_SERVER_ID: &str = "plugin:lobster-channel:lobster-channel";

/// CLI options that can be flattened into the application's command parser.
///
/// `claude_config` is intentionally not a CLI argument.  It is a test and
/// embedding seam so callers can point the doctor at a fixture without
/// changing the user's real `~/.claude.json`.
#[derive(Debug, Clone, Args, Default)]
pub struct McpDoctorOptions {
    /// Project directory whose Claude Code state should be inspected.
    #[arg(long, value_name = "DIR")]
    pub cwd: Option<PathBuf>,

    /// Remove only Lobster entries from this project's disabledMcpServers.
    #[arg(long)]
    pub repair: bool,

    /// Emit a machine-readable JSON report.
    #[arg(long)]
    pub json: bool,

    #[arg(skip)]
    pub claude_config: Option<PathBuf>,
}

impl McpDoctorOptions {
    /// Point the doctor at a fixture or an alternate Claude config file.
    pub fn with_config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.claude_config = Some(path.into());
        self
    }

    /// Parse the doctor-only flags.  This is useful before the command is
    /// wired into the main binary and keeps the option contract testable.
    pub fn parse_from<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        // A tiny wrapper gives clap a stable command name while retaining the
        // public options type for embedding.
        #[derive(Debug, clap::Parser)]
        struct Wrapper {
            #[command(flatten)]
            options: McpDoctorOptions,
        }
        Ok(Wrapper::try_parse_from(args)?.options)
    }
}

/// Top-level report returned by [`inspect`] and [`run`].
#[derive(Debug, Clone, Serialize)]
pub struct McpDoctorReport {
    pub cwd: String,
    pub claude_config: ConfigReport,
    pub project: ProjectMcpReport,
    pub lobster: LobsterInstallReport,
    pub processes: ProcessReport,
    pub log: LogReport,
    pub issues: Vec<String>,
    pub repair: Option<RepairReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigReport {
    pub path: String,
    pub exists: bool,
    pub readable: bool,
    pub parse_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProjectMcpReport {
    pub requested_cwd: String,
    pub matched_project_key: Option<String>,
    pub project_present: bool,
    pub disabled_mcp_servers: Vec<String>,
    pub lobster_disabled_servers: Vec<String>,
    pub enabled_mcpjson_servers: Vec<String>,
    pub disabled_mcpjson_servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LobsterInstallReport {
    pub plugin_roots: Vec<PluginRootReport>,
    pub files: Vec<PathReport>,
    /// Compiled Lobster MCP entrypoints whose session-superseded (bridge
    /// close 4405) branch terminates the stdio server process.  When multiple
    /// Claude sessions share a binding, a newer instance can take ownership
    /// and leave the older Claude session with a permanently disconnected MCP
    /// registry entry.
    pub session_superseded_exit_servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginRootReport {
    pub path: String,
    pub exists: bool,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PathReport {
    pub path: String,
    pub exists: bool,
    pub kind: Option<String>,
    pub size_bytes: Option<u64>,
    pub modified_unix_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProcessReport {
    pub available: bool,
    pub error: Option<String>,
    pub matching: Vec<ProcessInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProcessInfo {
    pub pid: u32,
    pub ppid: u32,
    pub state: String,
    pub elapsed: String,
    pub command: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LogReport {
    pub path: String,
    pub exists: bool,
    pub size_bytes: u64,
    pub line_count: usize,
    pub not_connected_count: usize,
    pub shutdown_count: usize,
    /// Number of explicit stdin EOF/close shutdowns. A stdio MCP server
    /// normally exits on this event; Claude can keep the hook registry entry
    /// around and report the server as disconnected for the rest of a session.
    pub stdin_end_count: usize,
    pub error_count: usize,
    pub unpaired_count: usize,
    /// Explicit fatal close records for bridge code 4405.  This is narrower
    /// than counting the digits `4405`, which may also occur in message ids.
    pub fatal_session_superseded_count: usize,
    /// Pairing handshakes displaced by another local process (bridge 4407).
    pub pairing_takeover_count: usize,
    pub latest_line: Option<String>,
    pub tail: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepairReport {
    pub requested: bool,
    pub changed: bool,
    pub removed_servers: Vec<String>,
    pub backup_path: Option<String>,
    pub atomic_write_path: Option<String>,
    pub message: String,
}

/// Inspect the default Claude config and local Lobster installation.
pub fn inspect(options: &McpDoctorOptions) -> Result<McpDoctorReport> {
    let config_path = config_path(options);
    inspect_at(options, &config_path)
}

/// Inspect and, when requested, perform the narrowly scoped repair.
pub fn run(options: &McpDoctorOptions) -> Result<McpDoctorReport> {
    let config_path = config_path(options);
    if !options.repair {
        return inspect_at(options, &config_path);
    }

    let mut report = inspect_at(options, &config_path)?;
    let repair = repair_config(&config_path, &report.project, options.cwd.as_deref())?;
    report.repair = Some(repair);

    // Re-read the project after a successful write so JSON output is a useful
    // postcondition rather than a pre-repair snapshot.
    if report.repair.as_ref().is_some_and(|r| r.changed) {
        let refreshed = inspect_at(options, &config_path)?;
        report.project = refreshed.project;
        report.claude_config = refreshed.claude_config;
        report.issues = build_issues(&report);
    }
    Ok(report)
}

/// Render a report for a human or machine consumer.
pub fn render(report: &McpDoctorReport, json: bool) -> Result<String> {
    if json {
        return serde_json::to_string_pretty(report).context("serialize MCP doctor report");
    }
    Ok(render_text(report))
}

/// Convenience wrapper honoring `options.json`.
pub fn run_and_render(options: &McpDoctorOptions) -> Result<String> {
    let report = run(options)?;
    render(&report, options.json)
}

fn config_path(options: &McpDoctorOptions) -> PathBuf {
    let home = home_dir();
    config_path_from_parts(
        options.claude_config.as_deref(),
        std::env::var_os("CLAUDE_CONFIG_DIR").as_deref(),
        &home,
    )
}

/// Resolve Claude Code's config file using the same directory convention as
/// the client: an explicit fixture wins, otherwise a non-empty
/// CLAUDE_CONFIG_DIR is treated as a directory containing .claude.json.
///
/// Older local wrappers used .config.json; when that file is the only
/// candidate, keep it usable as a compatibility fallback. The canonical
/// .claude.json path always wins when both files exist.
fn config_path_from_parts(
    explicit: Option<&Path>,
    config_dir: Option<&OsStr>,
    home: &Path,
) -> PathBuf {
    if let Some(path) = explicit {
        return path.to_path_buf();
    }
    let root = config_dir
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.to_path_buf());
    let canonical = root.join(".claude.json");
    let legacy = root.join(".config.json");
    if canonical.exists() || !legacy.exists() {
        canonical
    } else {
        legacy
    }
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var_os("USERPROFILE").filter(|value| !value.is_empty()))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn inspect_at(options: &McpDoctorOptions, config_path: &Path) -> Result<McpDoctorReport> {
    let cwd = resolve_cwd(options.cwd.as_deref())?;
    let config_meta = fs::metadata(config_path).ok();
    let config_exists = config_meta.is_some();
    let mut config_report = ConfigReport {
        path: path_string(config_path),
        exists: config_exists,
        readable: false,
        parse_error: None,
    };

    let (root, read_error) = if config_exists {
        match fs::read_to_string(config_path) {
            Ok(text) => {
                config_report.readable = true;
                match serde_json::from_str::<Value>(&text) {
                    Ok(value) => (Some(value), None),
                    Err(err) => {
                        let msg = err.to_string();
                        config_report.parse_error = Some(msg.clone());
                        (None, Some(msg))
                    }
                }
            }
            Err(err) => {
                let msg = err.to_string();
                (None, Some(msg))
            }
        }
    } else {
        (None, Some("file does not exist".to_string()))
    };

    let project = root
        .as_ref()
        .map(|value| project_report(value, &cwd))
        .unwrap_or_else(|| ProjectMcpReport {
            requested_cwd: path_string(&cwd),
            ..ProjectMcpReport::default()
        });
    let home = config_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let lobster = discover_lobster(&home);
    let log = summarize_log(&home.join(".lobster").join("channel.log"));
    let processes = collect_processes();

    let mut report = McpDoctorReport {
        cwd: path_string(&cwd),
        claude_config: config_report,
        project,
        lobster,
        processes,
        log,
        issues: Vec::new(),
        repair: None,
    };
    if let Some(err) = read_error {
        report.issues.push(format!("Claude config: {err}"));
    }
    report.issues.extend(build_issues(&report));
    report.issues.sort();
    report.issues.dedup();
    Ok(report)
}

fn resolve_cwd(input: Option<&Path>) -> Result<PathBuf> {
    let raw = input
        .map(Path::to_path_buf)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    if let Ok(canonical) = fs::canonicalize(&raw) {
        return Ok(canonical);
    }
    if raw.is_absolute() {
        Ok(raw)
    } else {
        Ok(std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(raw))
    }
}

fn project_report(root: &Value, cwd: &Path) -> ProjectMcpReport {
    let requested_cwd = path_string(cwd);
    let Some(projects) = root.get("projects").and_then(Value::as_object) else {
        return ProjectMcpReport {
            requested_cwd,
            ..ProjectMcpReport::default()
        };
    };
    let key = find_project_key(projects, cwd);
    let Some(key) = key else {
        return ProjectMcpReport {
            requested_cwd,
            ..ProjectMcpReport::default()
        };
    };
    let Some(project) = projects.get(&key).and_then(Value::as_object) else {
        return ProjectMcpReport {
            requested_cwd,
            matched_project_key: Some(key),
            ..ProjectMcpReport::default()
        };
    };
    let disabled = string_array(project.get("disabledMcpServers"));
    ProjectMcpReport {
        requested_cwd,
        matched_project_key: Some(key),
        project_present: true,
        lobster_disabled_servers: disabled
            .iter()
            .filter(|name| is_lobster_server_name(name))
            .cloned()
            .collect(),
        disabled_mcp_servers: disabled,
        enabled_mcpjson_servers: string_array(project.get("enabledMcpjsonServers")),
        disabled_mcpjson_servers: string_array(project.get("disabledMcpjsonServers")),
    }
}

fn find_project_key(projects: &Map<String, Value>, cwd: &Path) -> Option<String> {
    let requested = path_string(cwd);
    if projects.contains_key(&requested) {
        return Some(requested);
    }
    // Claude normally stores absolute paths, but older versions could retain
    // a symlink spelling. Compare canonical paths without suffix matching.
    let requested_canonical = fs::canonicalize(cwd).ok();
    let mut candidates = projects.keys().filter_map(|key| {
        let key_path = PathBuf::from(key);
        let canonical = fs::canonicalize(&key_path).ok();
        if (canonical.is_some() && canonical == requested_canonical)
            || (canonical.is_none() && key_path == cwd)
        {
            Some(key.clone())
        } else {
            None
        }
    });
    candidates.next()
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Match only server identifiers belonging to the Lobster channel plugin.
/// Generic MCP names are intentionally left untouched by repair.
pub fn is_lobster_server_name(name: &str) -> bool {
    let normalized = name.trim().to_ascii_lowercase();
    normalized == LOBSTER_SERVER_ID
        || normalized == "lobster-channel"
        || normalized.starts_with("plugin:lobster-channel:")
        || normalized.starts_with("lobster-channel:")
}

fn repair_config(
    config_path: &Path,
    project: &ProjectMcpReport,
    requested_cwd: Option<&Path>,
) -> Result<RepairReport> {
    let Some(project_key) = project.matched_project_key.as_deref() else {
        return Ok(RepairReport {
            requested: true,
            changed: false,
            removed_servers: Vec::new(),
            backup_path: None,
            atomic_write_path: None,
            message: "No matching Claude project entry; configuration was left unchanged.".into(),
        });
    };
    // Re-read rather than mutating the inspection snapshot. This closes the
    // time-of-check/time-of-use gap if another process edited the file between
    // inspection and repair, and ensures only the selected key is touched.
    let text = fs::read_to_string(config_path)
        .with_context(|| format!("read Claude config {}", config_path.display()))?;
    let mut root: Value = serde_json::from_str(&text)
        .with_context(|| format!("parse Claude config {}", config_path.display()))?;
    let projects = root
        .get_mut("projects")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("Claude config has no projects object"))?;
    let project_value = projects
        .get_mut(project_key)
        .ok_or_else(|| anyhow!("Claude project disappeared during repair: {project_key}"))?;
    let project_object = project_value
        .as_object_mut()
        .ok_or_else(|| anyhow!("Claude project entry is not an object: {project_key}"))?;
    let disabled_value = project_object
        .get_mut("disabledMcpServers")
        .and_then(Value::as_array_mut);
    let Some(disabled) = disabled_value else {
        return Ok(RepairReport {
            requested: true,
            changed: false,
            removed_servers: Vec::new(),
            backup_path: None,
            atomic_write_path: None,
            message:
                "Target project has no disabledMcpServers array; configuration was left unchanged."
                    .into(),
        });
    };
    let mut removed_servers = Vec::new();
    disabled.retain(|value| {
        let remove = value.as_str().is_some_and(is_lobster_server_name);
        if remove {
            if let Some(name) = value.as_str() {
                removed_servers.push(name.to_string());
            }
        }
        !remove
    });
    if removed_servers.is_empty() {
        return Ok(RepairReport {
            requested: true,
            changed: false,
            removed_servers,
            backup_path: None,
            atomic_write_path: None,
            message: "No Lobster disabled server was present; configuration was left unchanged."
                .into(),
        });
    }

    let backup = unique_backup_path(config_path)?;
    fs::copy(config_path, &backup).with_context(|| {
        format!(
            "create Claude config backup {} before repair",
            backup.display()
        )
    })?;
    atomic_write_json(config_path, &root)?;
    let cwd_note = requested_cwd
        .map(|path| format!(" for {}", path.display()))
        .unwrap_or_default();
    Ok(RepairReport {
        requested: true,
        changed: true,
        removed_servers,
        backup_path: Some(path_string(&backup)),
        atomic_write_path: Some(path_string(config_path)),
        message: format!("Removed Lobster disabled entries{cwd_note}; backup created first."),
    })
}

fn unique_backup_path(config_path: &Path) -> Result<PathBuf> {
    let stamp = unix_ms();
    let file_name = config_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("claude.json");
    let parent = config_path.parent().unwrap_or_else(|| Path::new("."));
    for suffix in 0..1000u32 {
        let candidate = parent.join(format!(
            ".{file_name}.mcp-doctor.bak.{stamp}.{}.{}",
            std::process::id(),
            suffix
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(anyhow!(
        "could not allocate a unique MCP doctor backup path"
    ))
}

fn atomic_write_json(config_path: &Path, root: &Value) -> Result<()> {
    let parent = config_path.parent().unwrap_or_else(|| Path::new("."));
    let temp = parent.join(format!(
        ".{}.mcp-doctor.tmp.{}.{}",
        config_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("claude.json"),
        std::process::id(),
        unix_ms()
    ));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temp)
            .with_context(|| format!("create temporary config {}", temp.display()))?;
        serde_json::to_writer_pretty(&mut file, root)
            .context("serialize repaired Claude config")?;
        file.write_all(b"\n")?;
        file.sync_all()
            .with_context(|| format!("sync temporary config {}", temp.display()))?;
        // rename(2) replaces the destination atomically on Unix. We never
        // remove the destination first, so a failed replacement leaves the
        // original config intact.
        fs::rename(&temp, config_path).with_context(|| {
            format!(
                "atomically replace {} with {}",
                config_path.display(),
                temp.display()
            )
        })?;
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn discover_lobster(home: &Path) -> LobsterInstallReport {
    let claude_dir = home.join(".claude");
    let mut roots = Vec::new();
    let cache = claude_dir
        .join("plugins")
        .join("cache")
        .join("lobster-lab")
        .join("lobster-channel");
    if let Ok(entries) = fs::read_dir(&cache) {
        let mut versions: Vec<PathBuf> = entries
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
        versions.sort();
        roots.extend(versions);
    }
    roots.push(
        claude_dir
            .join("lobster-lab")
            .join("marketplace")
            .join("lobster-channel"),
    );
    // Include an install path recorded by Claude even when the marketplace
    // layout changes. This file is data-only; no plugin code is executed.
    let installed = claude_dir.join("plugins").join("installed_plugins.json");
    if let Ok(text) = fs::read_to_string(installed)
        && let Ok(value) = serde_json::from_str::<Value>(&text)
        && let Some(entries) = value
            .get("plugins")
            .and_then(Value::as_object)
            .and_then(|plugins| plugins.get("lobster-channel@lobster-lab"))
            .and_then(Value::as_array)
    {
        roots.extend(entries.iter().filter_map(|entry| {
            entry
                .get("installPath")
                .and_then(Value::as_str)
                .map(PathBuf::from)
        }));
    }
    roots.sort();
    roots.dedup();

    let plugin_roots = roots
        .iter()
        .map(|path| PluginRootReport {
            path: path_string(path),
            exists: path.exists(),
            version: lobster_version(path),
        })
        .collect();
    let mut files = Vec::new();
    let mut session_superseded_exit_servers = Vec::new();
    for root in roots {
        for relative in [
            ".claude-plugin/plugin.json",
            "hooks/hooks.json",
            "dist/server.js",
        ] {
            let path = root.join(relative);
            if relative == "dist/server.js" && lobster_4405_exits_stdio_server(&path) {
                session_superseded_exit_servers.push(path_string(&path));
            }
            files.push(path_report(&path));
        }
    }
    session_superseded_exit_servers.sort();
    session_superseded_exit_servers.dedup();
    LobsterInstallReport {
        plugin_roots,
        files,
        session_superseded_exit_servers,
    }
}

fn lobster_version(root: &Path) -> Option<String> {
    for relative in [".claude-plugin/plugin.json", "package.json"] {
        let Ok(text) = fs::read_to_string(root.join(relative)) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        if let Some(version) = value.get("version").and_then(Value::as_str) {
            return Some(version.to_string());
        }
    }
    None
}

/// Detect the old Lobster lifecycle contract that turns bridge close 4405
/// into `process.exit(1)` for the whole stdio MCP server.
///
/// This intentionally looks for a conjunction rather than a single textual
/// marker.  Current and future builds may still define close code 4405 while
/// handling it dormantly; those must not be diagnosed as exit-prone merely
/// because `bridge-client.js` documents the code.
fn lobster_4405_exits_stdio_server(path: &Path) -> bool {
    let Ok(source) = fs::read_to_string(path) else {
        return false;
    };
    lobster_source_4405_exits_stdio_server(&source)
}

fn lobster_source_4405_exits_stdio_server(source: &str) -> bool {
    let has_superseded_branch = source.contains("CLOSE_SESSION_SUPERSEDED")
        || (source.contains("4405") && source.to_ascii_lowercase().contains("supersed"));
    if !has_superseded_branch {
        return false;
    }

    // Limit the exit test to the superseded branch. Generic process exits for
    // CLI failures or normal stdin teardown are unrelated and should not make
    // a dormant 4405 implementation look vulnerable.
    let branch_start = source
        .find("case bridge_client_js_1.CLOSE_SESSION_SUPERSEDED")
        .or_else(|| source.find("case CLOSE_SESSION_SUPERSEDED"))
        .or_else(|| source.find("code === CLOSE_SESSION_SUPERSEDED"))
        .or_else(|| source.find("code === 4405"));
    let Some(branch_start) = branch_start else {
        return false;
    };
    let tail = &source[branch_start..];
    let branch_end = ["case ", "default:", "\n    }\n", "\n  }\n"]
        .into_iter()
        .filter_map(|marker| tail.get(1..)?.find(marker).map(|offset| offset + 1))
        .min()
        .unwrap_or_else(|| tail.len().min(4096));
    let branch = &tail[..branch_end];
    branch.contains("process.exit(")
}

fn collect_processes() -> ProcessReport {
    #[cfg(unix)]
    {
        let output = Command::new("ps")
            .args(["-axo", "pid=,ppid=,stat=,etime=,command="])
            .output();
        let Ok(output) = output else {
            return ProcessReport {
                available: false,
                error: Some("failed to execute ps".into()),
                matching: Vec::new(),
            };
        };
        if !output.status.success() {
            return ProcessReport {
                available: false,
                error: Some(String::from_utf8_lossy(&output.stderr).trim().to_string()),
                matching: Vec::new(),
            };
        }
        let mut matching = Vec::new();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let mut fields = line.split_whitespace();
            let Some(pid) = fields.next().and_then(|v| v.parse::<u32>().ok()) else {
                continue;
            };
            let Some(ppid) = fields.next().and_then(|v| v.parse::<u32>().ok()) else {
                continue;
            };
            let Some(state) = fields.next() else { continue };
            let Some(elapsed) = fields.next() else {
                continue;
            };
            let command = fields.collect::<Vec<_>>().join(" ");
            if is_lobster_process_command(&command) {
                matching.push(ProcessInfo {
                    pid,
                    ppid,
                    state: state.to_string(),
                    elapsed: elapsed.to_string(),
                    command: redact_process_command(&command),
                });
            }
        }
        matching.sort_by_key(|process| process.pid);
        ProcessReport {
            available: true,
            error: None,
            matching,
        }
    }
    #[cfg(not(unix))]
    {
        ProcessReport {
            available: false,
            error: Some("process inspection is not implemented on this platform".into()),
            matching: Vec::new(),
        }
    }
}

/// Return whether a `ps` command line is an actual Lobster MCP server.
///
/// Matching arbitrary command text (for example `find ... lobster-channel` or
/// a shell running `rg lobster-lab`) makes the doctor report its own diagnostic
/// commands as live servers. Restrict this to the shipped stdio entrypoint or
/// a known Lobster executable name. The helper is intentionally pure so the
/// process classification stays testable without spawning processes.
fn is_lobster_process_command(command: &str) -> bool {
    let mut words = command.split_whitespace();
    let executable = words.next().unwrap_or_default();
    let executable_name = Path::new(executable)
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    // All supported plugin layouts install this exact compiled entrypoint,
    // with a version directory between the plugin name and `dist` (for
    // example `lobster-channel/1.23.0/dist/server.js`). Inspect each argument
    // rather than matching the whole command so a shell's source-search text
    // cannot be mistaken for a running server.
    let server_entrypoint = words.clone().any(|arg| {
        let normalized = arg.trim_matches(['"', '\'']).replace('\\', "/");
        let arg_lower = normalized.to_ascii_lowercase();
        (arg_lower.contains("/lobster-channel/") || arg_lower.contains("/lobster_channel/"))
            && arg_lower.ends_with("/dist/server.js")
    });
    if server_entrypoint {
        return matches!(
            executable_name.as_str(),
            "node" | "nodejs" | "bun" | "deno" | "tsx" | "ts-node"
        );
    }

    // Future native builds may expose a dedicated executable rather than a
    // Node entrypoint. Keep this allow-list narrow for the same false-positive
    // reason as above.
    matches!(
        executable_name.as_str(),
        "channel-mcp" | "lobster-channel" | "lobster-agent"
    )
}

fn redact_process_command(command: &str) -> String {
    // Process arguments can contain bridge tokens. Keep enough command text
    // for diagnosis while avoiding accidental credential disclosure.
    redact_secrets(command)
}

fn summarize_log(path: &Path) -> LogReport {
    let mut report = LogReport {
        path: path_string(path),
        ..LogReport::default()
    };
    let Ok(bytes) = fs::read(path) else {
        return report;
    };
    report.exists = true;
    report.size_bytes = bytes.len() as u64;
    let text = String::from_utf8_lossy(&bytes);
    let lines: Vec<&str> = text.lines().collect();
    report.line_count = lines.len();
    report.not_connected_count = count_case_insensitive(&text, "not connected");
    report.shutdown_count = count_case_insensitive(&text, "shutdown");
    report.stdin_end_count = count_case_insensitive(&text, "stdin_end");
    report.error_count = count_case_insensitive(&text, "error");
    report.unpaired_count = count_case_insensitive(&text, "unpaired");
    report.fatal_session_superseded_count = text
        .lines()
        .filter(|line| {
            let lower = line.to_ascii_lowercase();
            lower.contains("fatal close code=4405") || lower.contains("bridge fatal 4405")
        })
        .count();
    report.pairing_takeover_count = text
        .lines()
        .filter(|line| {
            let lower = line.to_ascii_lowercase();
            lower.contains("handshake taken over by another process")
                || (lower.contains("4407") && lower.contains("takeover"))
        })
        .count();
    report.latest_line = lines.last().map(|line| redact_log_line(line));
    const TAIL_LINES: usize = 20;
    report.tail = lines
        .iter()
        .rev()
        .take(TAIL_LINES)
        .rev()
        .map(|line| redact_log_line(line))
        .collect();
    report
}

fn count_case_insensitive(haystack: &str, needle: &str) -> usize {
    let lower = haystack.to_ascii_lowercase();
    lower.match_indices(&needle.to_ascii_lowercase()).count()
}

fn redact_log_line(line: &str) -> String {
    // Log lines are normally already token-free; redact common key/value forms
    // before exposing the tail through `--json`.
    redact_secrets(line)
}

// Cover the credential spellings seen in process tables and structured MCP
// logs. In particular, `--token VALUE`, `Authorization: Bearer VALUE`, and
// JSON fields do not contain the `token=` marker handled by the original
// doctor implementation.
static BEARER_SECRET_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/=-]+")
        .expect("Bearer redaction regex must compile")
});
static TOKEN_FLAG_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(--(?:agent[-_]?token|token)(?:\s*=\s*|\s+))(?:"[^"]*"|'[^']*'|[^\s]+)"#)
        .expect("token flag redaction regex must compile")
});
static NAMED_SECRET_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?i)((?:"(?:agenttoken|agent_token|symmetrickeyb64|token|authorization)"|'(?:agenttoken|agent_token|symmetrickeyb64|token|authorization)'|(?:agenttoken|agent_token|symmetrickeyb64|token|authorization))\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,}]+)"#,
    )
    .expect("named secret redaction regex must compile")
});

fn redact_secrets(input: &str) -> String {
    // Redact Bearer payloads first. For an unquoted `Authorization=Bearer X`,
    // the named-field pass sees only the first word; doing this pass first
    // ensures the second word has already been removed.
    let bearer = BEARER_SECRET_RE.replace_all(input, "${1}REDACTED");
    let flags = TOKEN_FLAG_RE.replace_all(&bearer, "${1}REDACTED");
    NAMED_SECRET_RE
        .replace_all(&flags, "${1}REDACTED")
        .into_owned()
}

fn path_report(path: &Path) -> PathReport {
    let metadata = fs::metadata(path).ok();
    PathReport {
        path: path_string(path),
        exists: metadata.is_some(),
        kind: metadata.as_ref().map(|meta| {
            if meta.is_dir() {
                "directory".to_string()
            } else if meta.is_file() {
                "file".to_string()
            } else {
                "other".to_string()
            }
        }),
        size_bytes: metadata.as_ref().map(|meta| meta.len()),
        modified_unix_ms: metadata
            .and_then(|meta| meta.modified().ok())
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis()),
    }
}

fn build_issues(report: &McpDoctorReport) -> Vec<String> {
    let mut issues = Vec::new();
    if !report.project.lobster_disabled_servers.is_empty() {
        issues.push(format!(
            "Target project disables Lobster MCP: {}",
            report.project.lobster_disabled_servers.join(", ")
        ));
    }
    if report
        .lobster
        .files
        .iter()
        .any(|file| !file.exists && file.path.ends_with("dist/server.js"))
    {
        issues.push("Lobster plugin server.js is missing from a discovered install root.".into());
    }
    if !report.lobster.session_superseded_exit_servers.is_empty() {
        issues.push(format!(
            "Lobster MCP runtime exits its stdio process on bridge close 4405 (session superseded): {}. A newer local Claude session can take over the shared binding and leave the older session's MCP entry permanently disconnected; update the Lobster runtime to a build with dormant 4405 handling.",
            report
                .lobster
                .session_superseded_exit_servers
                .join(", ")
        ));
    }
    if report.processes.available && report.processes.matching.len() > 1 {
        issues.push(format!(
            "{} Lobster MCP processes are running for the shared binding; concurrent instances can trigger bridge 4405/4407 takeovers and repeated 'not connected' reports. Keep one channel owner per binding or isolate Claude sessions with separate config/workspace state.",
            report.processes.matching.len()
        ));
    }
    if report.log.not_connected_count > 0 {
        issues.push(format!(
            "Lobster log contains {} 'not connected' entries.",
            report.log.not_connected_count
        ));
    }
    if report.log.stdin_end_count > 0 {
        issues.push(format!(
            "Lobster stdio recorded {} stdin EOF/close shutdown event(s); a headless Claude run can leave a stale disconnected MCP registry. Start a fresh session or isolate it with CLAUDE_CONFIG_DIR and an empty MCP config.",
            report.log.stdin_end_count
        ));
    }
    if report.log.fatal_session_superseded_count > 0 {
        issues.push(format!(
            "Lobster log contains {} bridge 4405 session-superseded close event(s); each event can terminate the MCP stdio child when using an exit-prone runtime.",
            report.log.fatal_session_superseded_count
        ));
    }
    if report.log.pairing_takeover_count > 0 {
        issues.push(format!(
            "Lobster log contains {} pairing takeover event(s) (bridge 4407); another local process is competing for the same pending handshake.",
            report.log.pairing_takeover_count
        ));
    }
    if report.processes.available && report.processes.matching.is_empty() {
        issues.push("No running Lobster channel process was found.".into());
    }
    issues
}

fn render_text(report: &McpDoctorReport) -> String {
    let mut lines = vec![
        format!("MCP doctor for {}", report.cwd),
        format!(
            "Claude config: {} ({})",
            report.claude_config.path,
            if report.claude_config.readable {
                "readable"
            } else if report.claude_config.exists {
                "unreadable"
            } else {
                "missing"
            }
        ),
        format!(
            "Project: {}",
            report
                .project
                .matched_project_key
                .as_deref()
                .unwrap_or("not found")
        ),
        format!(
            "disabledMcpServers: {}",
            if report.project.disabled_mcp_servers.is_empty() {
                "(none)".into()
            } else {
                report.project.disabled_mcp_servers.join(", ")
            }
        ),
        format!(
            "Lobster disabled: {}",
            if report.project.lobster_disabled_servers.is_empty() {
                "no".into()
            } else {
                report.project.lobster_disabled_servers.join(", ")
            }
        ),
        format!(
            "Lobster processes: {}{}",
            report.processes.matching.len(),
            if report.lobster.session_superseded_exit_servers.is_empty() {
                String::new()
            } else {
                format!(
                    ", {} exit-prone 4405 runtime(s)",
                    report.lobster.session_superseded_exit_servers.len()
                )
            }
        ),
        format!(
            "channel.log: {} lines, {} not-connected, {} shutdown ({} stdin EOF), {} 4405 superseded, {} 4407 takeover",
            report.log.line_count,
            report.log.not_connected_count,
            report.log.shutdown_count,
            report.log.stdin_end_count,
            report.log.fatal_session_superseded_count,
            report.log.pairing_takeover_count
        ),
    ];
    if let Some(repair) = &report.repair {
        lines.push(format!("Repair: {}", repair.message));
        if let Some(backup) = &repair.backup_path {
            lines.push(format!("Backup: {backup}"));
        }
    }
    if !report.issues.is_empty() {
        lines.push("Issues:".into());
        lines.extend(report.issues.iter().map(|issue| format!("- {issue}")));
    }
    lines.join("\n")
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn fixture_config(dir: &Path, target: &Path) -> PathBuf {
        let config = dir.join(".claude.json");
        let value = serde_json::json!({
            "projects": {
                target.to_string_lossy(): {
                    "disabledMcpServers": [
                        "plugin:lobster-channel:lobster-channel",
                        "context7",
                        "lobster-channel:other"
                    ],
                    "enabledMcpjsonServers": ["serena"],
                    "disabledMcpjsonServers": []
                }
            },
            "mcpServers": {}
        });
        fs::write(&config, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        config
    }

    #[test]
    fn parses_doctor_flags() {
        let options = McpDoctorOptions::parse_from([
            "mcp-doctor",
            "--cwd",
            "/tmp/project",
            "--repair",
            "--json",
        ])
        .unwrap();
        assert_eq!(options.cwd, Some(PathBuf::from("/tmp/project")));
        assert!(options.repair);
        assert!(options.json);
    }

    #[test]
    fn config_path_defaults_to_home_claude_json() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("home");
        fs::create_dir_all(&home).unwrap();

        let path = config_path_from_parts(None, None, &home);

        assert_eq!(path, home.join(".claude.json"));
    }

    #[test]
    fn config_path_uses_non_empty_config_dir_as_directory() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("home");
        let config_dir = dir.path().join("claude-state");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&config_dir).unwrap();

        let path = config_path_from_parts(None, Some(config_dir.as_os_str()), &home);

        assert_eq!(path, config_dir.join(".claude.json"));
    }

    #[test]
    fn config_path_ignores_empty_config_dir_and_prefers_canonical_file() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("home");
        fs::create_dir_all(&home).unwrap();
        fs::write(home.join(".config.json"), b"{}").unwrap();

        let path = config_path_from_parts(None, Some(OsStr::new("")), &home);

        assert_eq!(path, home.join(".config.json"));

        fs::write(home.join(".claude.json"), b"{}").unwrap();
        let canonical = config_path_from_parts(None, None, &home);
        assert_eq!(canonical, home.join(".claude.json"));
    }

    #[test]
    fn config_path_falls_back_to_legacy_file_only_when_needed() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("home");
        let config_dir = dir.path().join("claude-state");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&config_dir).unwrap();
        fs::write(config_dir.join(".config.json"), b"{}").unwrap();

        let legacy = config_path_from_parts(None, Some(config_dir.as_os_str()), &home);
        assert_eq!(legacy, config_dir.join(".config.json"));

        fs::write(config_dir.join(".claude.json"), b"{}").unwrap();
        let canonical = config_path_from_parts(None, Some(config_dir.as_os_str()), &home);
        assert_eq!(canonical, config_dir.join(".claude.json"));
    }

    #[test]
    fn explicit_config_path_overrides_environment_directory() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("home");
        let config_dir = dir.path().join("claude-state");
        let explicit = dir.path().join("fixture.json");

        let path = config_path_from_parts(Some(&explicit), Some(config_dir.as_os_str()), &home);

        assert_eq!(path, explicit);
    }

    #[test]
    fn recognizes_only_lobster_server_ids() {
        assert!(is_lobster_server_name(LOBSTER_SERVER_ID));
        assert!(is_lobster_server_name("lobster-channel"));
        assert!(is_lobster_server_name("plugin:lobster-channel:legacy"));
        assert!(!is_lobster_server_name("plugin:other:lobster-channel"));
        assert!(!is_lobster_server_name("context7"));
    }

    #[test]
    fn process_classifier_ignores_diagnostic_commands() {
        assert!(is_lobster_process_command(
            "node /Users/me/.claude/plugins/cache/lobster-lab/lobster-channel/1.23.0/dist/server.js"
        ));
        assert!(is_lobster_process_command("channel-mcp --stdio"));
        assert!(!is_lobster_process_command(
            "/bin/zsh -c find /Users/me -path '*lobster-channel*' -print"
        ));
        assert!(!is_lobster_process_command(
            "rg -n lobster-lab src/mcp_doctor.rs"
        ));
        assert!(!is_lobster_process_command(
            "node /tmp/lobster-channel/src/server.ts"
        ));
    }

    #[test]
    fn process_redaction_covers_flags_bearer_and_json_tokens() {
        let command = concat!(
            "node server.js --token cli-secret --agent-token='agent secret' ",
            "Authorization='Bearer bearer-secret' ",
            r#"--metadata '{"token":"json-secret","symmetricKeyB64":"key-secret"}'"#,
        );

        let redacted = redact_process_command(command);

        for secret in [
            "cli-secret",
            "agent secret",
            "bearer-secret",
            "json-secret",
            "key-secret",
        ] {
            assert!(!redacted.contains(secret), "leaked {secret}: {redacted}");
        }
        assert!(redacted.contains("node server.js"));
        assert!(redacted.matches("REDACTED").count() >= 5, "{redacted}");
    }

    #[test]
    fn log_redaction_covers_unquoted_authorization_and_mixed_case_keys() {
        let line = concat!(
            "request Authorization=Bearer bearer-secret ",
            "AgentToken=agent-secret ",
            r#"payload={"TOKEN": "json-secret"}"#,
        );

        let redacted = redact_log_line(line);

        for secret in ["bearer-secret", "agent-secret", "json-secret"] {
            assert!(!redacted.contains(secret), "leaked {secret}: {redacted}");
        }
        assert!(redacted.contains("request"));
    }

    #[test]
    fn detects_only_session_superseded_branches_that_exit() {
        let old_runtime = r#"
            case bridge_client_js_1.CLOSE_SESSION_SUPERSEDED: {
                safeAppendLog(`fatal close code=${code}`);
                setTimeout(() => process.exit(1), 250);
                return;
            }
            default: { return; }
        "#;
        assert!(lobster_source_4405_exits_stdio_server(old_runtime));

        let dormant_runtime = r#"
            case bridge_client_js_1.CLOSE_SESSION_SUPERSEDED: {
                safeAppendLog(`bridge dormant code=${code}`);
                enterDormantMode();
                return;
            }
            default: { process.exit(1); }
        "#;
        assert!(!lobster_source_4405_exits_stdio_server(dormant_runtime));

        let unrelated_exit = "process.stdin.on('end', () => process.exit(0));";
        assert!(!lobster_source_4405_exits_stdio_server(unrelated_exit));
    }

    #[test]
    fn discovers_exit_prone_lobster_runtime_and_version() {
        let dir = tempdir().unwrap();
        let root = dir
            .path()
            .join(".claude/plugins/cache/lobster-lab/lobster-channel/1.23.0");
        fs::create_dir_all(root.join(".claude-plugin")).unwrap();
        fs::create_dir_all(root.join("dist")).unwrap();
        fs::write(
            root.join(".claude-plugin/plugin.json"),
            r#"{"name":"lobster-channel","version":"1.23.0"}"#,
        )
        .unwrap();
        fs::write(
            root.join("dist/server.js"),
            r#"
            case bridge_client_js_1.CLOSE_SESSION_SUPERSEDED: {
                setTimeout(() => process.exit(1), 250);
                return;
            }
            default: { return; }
            "#,
        )
        .unwrap();

        let discovered = discover_lobster(dir.path());
        assert_eq!(discovered.plugin_roots.len(), 2);
        let installed = discovered
            .plugin_roots
            .iter()
            .find(|entry| entry.path == path_string(&root))
            .unwrap();
        assert_eq!(installed.version.as_deref(), Some("1.23.0"));
        assert_eq!(
            discovered.session_superseded_exit_servers,
            vec![path_string(&root.join("dist/server.js"))]
        );
    }

    #[test]
    fn repair_backs_up_and_removes_only_lobster_entries() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("project");
        fs::create_dir_all(&target).unwrap();
        let config = fixture_config(dir.path(), &target);
        let options = McpDoctorOptions {
            cwd: Some(target.clone()),
            repair: true,
            json: true,
            claude_config: Some(config.clone()),
        };
        let report = run(&options).unwrap();
        let repair = report.repair.unwrap();
        assert!(repair.changed);
        assert_eq!(repair.removed_servers.len(), 2);
        let backup = PathBuf::from(repair.backup_path.unwrap());
        assert!(backup.exists());
        let updated: Value = serde_json::from_str(&fs::read_to_string(config).unwrap()).unwrap();
        let disabled = updated["projects"][target.to_string_lossy().as_ref()]["disabledMcpServers"]
            .as_array()
            .unwrap();
        assert_eq!(disabled, &[Value::String("context7".into())]);
        let backup_value: Value =
            serde_json::from_str(&fs::read_to_string(backup).unwrap()).unwrap();
        assert_eq!(
            backup_value["projects"][target.to_string_lossy().as_ref()]["disabledMcpServers"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn inspect_reports_log_counts_and_tail() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("project");
        fs::create_dir_all(&target).unwrap();
        let config = fixture_config(dir.path(), &target);
        let lobster = dir.path().join(".lobster");
        fs::create_dir_all(&lobster).unwrap();
        fs::write(
            lobster.join("channel.log"),
            "not connected\nshutdown signal=stdin_end\nnot connected\n",
        )
        .unwrap();
        let report = inspect(&McpDoctorOptions {
            cwd: Some(target),
            claude_config: Some(config),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(report.log.not_connected_count, 2);
        assert_eq!(report.log.shutdown_count, 1);
        assert_eq!(report.log.stdin_end_count, 1);
        assert_eq!(report.log.fatal_session_superseded_count, 0);
        assert_eq!(report.log.pairing_takeover_count, 0);
        assert_eq!(report.log.tail.len(), 3);
        assert!(!report.issues.is_empty());
    }

    #[test]
    fn log_summary_counts_only_explicit_4405_and_4407_takeovers() {
        let dir = tempdir().unwrap();
        let log = dir.path().join("channel.log");
        fs::write(
            &log,
            concat!(
                "bridge WS closed code=4405 reason=duplicate_conn\n",
                "fatal close code=4405 reason=duplicate_conn\n",
                "MessageDisplay msg=efde4405 deltas=1\n",
                "pre-pair watch: handshake taken over by another process (4407) — backing off\n",
                "activeRun end reason=superseded_by_new_run\n"
            ),
        )
        .unwrap();

        let summary = summarize_log(&log);
        assert_eq!(summary.fatal_session_superseded_count, 1);
        assert_eq!(summary.pairing_takeover_count, 1);
    }
}
