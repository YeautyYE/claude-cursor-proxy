use anyhow::Result;
use clap::{ArgAction, Parser, Subcommand};
use claude_cursor_proxy::{
    config, logging, mcp_doctor,
    monitor::MonitorHandle,
    paths, providers,
    registry::{ANTHROPIC_STYLE_ALIASES, Registry},
    server::{self, ServerConfig},
    tui::{self, MonitorExit, MonitorUiConfig},
};
use std::io::IsTerminal;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Parser)]
#[command(
    name = "claude-cursor-proxy",
    version = VERSION,
    about = "Local Anthropic-compatible proxy: Claude Code to Cursor (Fable) and other providers",
    disable_version_flag = true
)]
struct Cli {
    #[arg(long = "version", short = 'v', action = ArgAction::SetTrue)]
    version_flag: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Version,
    Serve {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long = "no-monitor", action = ArgAction::SetTrue)]
        no_monitor: bool,
    },
    /// Open the monitor TUI with mock data and no proxy server
    Demo,
    Models {
        #[arg(long)]
        full: bool,
    },
    /// Inspect Claude Code MCP/Lobster state and optionally repair a disabled entry.
    #[command(name = "mcp-doctor")]
    McpDoctor {
        #[command(flatten)]
        options: mcp_doctor::McpDoctorOptions,
    },
    Codex {
        #[command(subcommand)]
        command: ProviderGroup,
    },
    Kimi {
        #[command(subcommand)]
        command: ProviderGroup,
    },
    Cursor {
        #[command(subcommand)]
        command: ProviderGroup,
    },
    Grok {
        #[command(subcommand)]
        command: ProviderGroup,
    },
}

#[derive(Debug, Subcommand)]
enum ProviderGroup {
    Auth {
        #[command(subcommand)]
        command: claude_cursor_proxy::provider::AuthCommand,
    },
    /// Show the effective SandClientMode routing, identity, transport, and
    /// credential-free usage-cache state without making an upstream request.
    #[command(name = "sand-status")]
    SandStatus {
        /// Emit machine-readable JSON.
        #[arg(long)]
        json: bool,
    },
}

fn main() -> Result<()> {
    // macOS often starts CLI tools with a 256-file soft limit. Raise before
    // Tokio/reqwest open sockets so 64-way grok-cli waves do not fail as
    // "Cursor auth failed: /usr/bin/security: Too many open files".
    let _ = claude_cursor_proxy::fdlimit::raise_nofile_limit();

    let cli = Cli::parse();

    if cli.version_flag {
        println!("claude-cursor-proxy {}", VERSION);
        return Ok(());
    }

    let commands = cli.command.unwrap_or(Commands::Serve {
        port: None,
        no_monitor: false,
    });

    match commands {
        Commands::Version => {
            println!("claude-cursor-proxy {}", VERSION);
            Ok(())
        }
        Commands::Serve { port, no_monitor } => {
            let bind_address = config::bind_address();
            let effective_port = port.unwrap_or_else(config::port);
            let registry = Registry::with_default_alias();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            match select_serve_mode(std::io::stdout().is_terminal(), no_monitor) {
                ServeMode::Plain => {
                    print_server_banner(&bind_address, effective_port, &registry);
                    runtime.spawn(async {
                        providers::cursor::usage::poll_cursor_sand_usage_evidence().await;
                    });
                    spawn_cursor_catalog_warmup(&runtime);
                    runtime
                        .block_on(server::serve(ServerConfig {
                            bind_address,
                            port: effective_port,
                            monitor: None,
                        }))
                        .map_err(|err| anyhow::anyhow!(err))
                }
                ServeMode::Monitor => {
                    let _stderr_guard = logging::suppress_stderr();
                    let monitor = MonitorHandle::default();
                    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                    let (shutdown_complete_tx, shutdown_complete_rx) = std::sync::mpsc::channel();
                    let listener = runtime
                        .block_on(server::bind_proxy_listener(&bind_address, effective_port))?;
                    let local_addr = listener.local_addr()?;
                    let monitor_listen_url =
                        listen_url(&local_addr.ip().to_string(), local_addr.port());
                    let server_monitor = monitor.clone();
                    let usage_monitor = monitor.clone();
                    runtime.spawn(async move {
                        providers::cursor::usage::poll_cursor_account_usage(usage_monitor).await;
                    });
                    spawn_cursor_catalog_warmup(&runtime);
                    let server_task = runtime.spawn(async move {
                        let result =
                            server::serve_listener(listener, Some(server_monitor), async move {
                                let _ = shutdown_rx.await;
                            })
                            .await;
                        let _ = shutdown_complete_tx.send(());
                        result
                    });
                    let ui_result = tui::run_monitor(
                        monitor,
                        MonitorUiConfig {
                            listen_url: monitor_listen_url,
                            port: effective_port,
                            registry: &registry,
                            shutdown: Some(shutdown_tx),
                            shutdown_complete: Some(shutdown_complete_rx),
                        },
                    );
                    if matches!(&ui_result, Ok(MonitorExit::ForceQuit)) {
                        server_task.abort();
                        let _ = runtime.block_on(server_task);
                        std::process::exit(130);
                    }
                    let server_result = runtime.block_on(server_task)?;
                    ui_result?;
                    server_result.map_err(|err| anyhow::anyhow!(err))
                }
            }
        }
        Commands::Demo => {
            let registry = Registry::with_default_alias();
            tui::run_mock_monitor(config::port(), &registry)
        }
        Commands::Models { full } => {
            print_models(&Registry::with_default_alias(), full);
            Ok(())
        }
        Commands::McpDoctor { options } => {
            println!("{}", mcp_doctor::run_and_render(&options)?);
            Ok(())
        }
        Commands::Codex { command } => run_provider_cli("codex", command),
        Commands::Kimi { command } => run_provider_cli("kimi", command),
        Commands::Cursor { command } => run_provider_cli("cursor", command),
        Commands::Grok { command } => run_provider_cli("grok", command),
    }
}

/// Populate the process-wide Cursor model cache before the TUI opens its
/// selector. `/v1/models` also refreshes this cache on demand; this warm-up
/// keeps the first `s` press useful even when the client has not queried that
/// endpoint yet.
fn spawn_cursor_catalog_warmup(runtime: &tokio::runtime::Runtime) {
    runtime.spawn(async {
        let auth =
            tokio::task::spawn_blocking(|| match providers::cursor::auth::load_cursor_auth() {
                Ok(Some(auth)) => Some(auth),
                Ok(None) | Err(_) => providers::cursor::auth::load_cursor_desktop_auth()
                    .ok()
                    .flatten(),
            })
            .await
            .ok()
            .flatten();
        let Some(auth) = auth else {
            return;
        };
        let client = providers::cursor::client::CursorHttpClient::new();
        // Cursor can return a different catalog under its managed-local Sand
        // identity than it does for the ordinary CLI identity.  Warm every
        // identity selected by the current policy so the first TUI `s` press
        // has the same choices as a later request turn. The HTTP client keeps
        // these snapshots identity-scoped, so concurrent fetches cannot
        // overwrite one another.
        let client_types = config::cursor_catalog_client_types();
        let fetches = client_types.into_iter().map(|client_type| {
            let client = client.clone();
            let token = auth.access_token.clone();
            async move {
                let _ = client
                    .fetch_usable_models_for_client_type(&token, &client_type)
                    .await;
            }
        });
        futures_util::future::join_all(fetches).await;
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServeMode {
    Monitor,
    Plain,
}

fn select_serve_mode(stdout_is_tty: bool, no_monitor: bool) -> ServeMode {
    if stdout_is_tty && !no_monitor {
        ServeMode::Monitor
    } else {
        ServeMode::Plain
    }
}

fn run_provider_cli(name: &str, command: ProviderGroup) -> Result<()> {
    let registry = Registry::with_default_alias();
    let provider = registry
        .provider(name)
        .ok_or_else(|| anyhow::anyhow!("unknown provider: {name}"))?;
    let handlers = provider.cli();
    match command {
        ProviderGroup::Auth { command } => match command {
            claude_cursor_proxy::provider::AuthCommand::Login => {
                if let Err(err) = handlers.login() {
                    eprintln!("{err}");
                    std::process::exit(2);
                }
                Ok(())
            }
            claude_cursor_proxy::provider::AuthCommand::Device => {
                if let Err(err) = handlers.device() {
                    eprintln!("{err}");
                    std::process::exit(2);
                }
                Ok(())
            }
            claude_cursor_proxy::provider::AuthCommand::Status => {
                if let Err(err) = handlers.status() {
                    println!("{err}");
                    if err.to_string() == "Not authenticated" {
                        std::process::exit(1);
                    }
                    std::process::exit(2);
                }
                Ok(())
            }
            claude_cursor_proxy::provider::AuthCommand::Logout => {
                handlers.logout()?;
                Ok(())
            }
            command @ (claude_cursor_proxy::provider::AuthCommand::Add { .. }
            | claude_cursor_proxy::provider::AuthCommand::List
            | claude_cursor_proxy::provider::AuthCommand::Use { .. }
            | claude_cursor_proxy::provider::AuthCommand::Remove { .. }
            | claude_cursor_proxy::provider::AuthCommand::Usage { .. }) => {
                if name != "cursor" {
                    anyhow::bail!(
                        "{name} auth {} is only available for the cursor provider",
                        auth_command_name(&command)
                    );
                }
                run_cursor_account_cli(command)
            }
        },
        ProviderGroup::SandStatus { json } => {
            if name != "cursor" {
                anyhow::bail!("{name} sand-status is only available for the cursor provider");
            }
            let status = claude_cursor_proxy::providers::cursor::sand_status::snapshot();
            if json {
                println!("{}", serde_json::to_string_pretty(&status)?);
            } else {
                print!(
                    "{}",
                    claude_cursor_proxy::providers::cursor::sand_status::render_text(&status)
                );
            }
            Ok(())
        }
    }
}

fn auth_command_name(command: &claude_cursor_proxy::provider::AuthCommand) -> &'static str {
    use claude_cursor_proxy::provider::AuthCommand;
    match command {
        AuthCommand::Add { .. } => "add",
        AuthCommand::List => "list",
        AuthCommand::Use { .. } => "use",
        AuthCommand::Remove { .. } => "remove",
        AuthCommand::Usage { .. } => "usage",
        AuthCommand::Login => "login",
        AuthCommand::Device => "device",
        AuthCommand::Status => "status",
        AuthCommand::Logout => "logout",
    }
}

fn run_cursor_account_cli(command: claude_cursor_proxy::provider::AuthCommand) -> Result<()> {
    use claude_cursor_proxy::provider::AuthCommand;
    use claude_cursor_proxy::providers::cursor::auth as cursor_auth;

    match command {
        AuthCommand::Add { label } => {
            let auth = cursor_auth::run_cursor_login_add_with_label(label.clone())?
                .ok_or_else(|| anyhow::anyhow!("Cursor login timed out"))?;
            // Resolve the persisted profile once so the output names the
            // account just added (which can be inactive) rather than merely
            // echoing the currently selected account.
            let proposed_id = cursor_auth::cursor_account_id_for_token(&auth.access_token);
            let added = cursor_auth::list_cursor_accounts()?
                .into_iter()
                .find(|profile| {
                    profile.id == proposed_id
                        || profile
                            .auth
                            .email
                            .as_deref()
                            .zip(auth.email.as_deref())
                            .is_some_and(|(left, right)| {
                                left.trim().eq_ignore_ascii_case(right.trim())
                            })
                });
            if let Some(profile) = added {
                println!("Cursor account added: {}", profile.display_name());
                println!("Account id: {}", profile.id);
            } else {
                println!("Cursor account added: {}", account_display_name(&auth));
                println!("Account id: {proposed_id}");
            }
            if let Some(id) = cursor_auth::active_cursor_account_id()? {
                println!("Active account: {id}");
            }
            Ok(())
        }
        AuthCommand::List => {
            let accounts = cursor_auth::list_cursor_accounts()?;
            if accounts.is_empty() {
                println!("No Cursor accounts saved.");
                println!(
                    "Run `claude-cursor-proxy cursor auth login` to replace the current login, or `... cursor auth add` to keep it."
                );
                return Ok(());
            }
            println!("Cursor accounts ({}):", accounts.len());
            for account in accounts {
                let marker = if account.active { '*' } else { ' ' };
                let label = account.display_name();
                let email = account.auth.email.as_deref().unwrap_or("-");
                println!("{marker} {id}  {label}  ({email})", id = account.id);
            }
            Ok(())
        }
        AuthCommand::Use { account } => {
            let profile = resolve_cursor_account(&account)?;
            let selected = cursor_auth::switch_cursor_account(&profile.id)?;
            println!(
                "Active Cursor account: {}",
                account_display_name(&selected.auth)
            );
            println!("Account id: {}", selected.id);
            Ok(())
        }
        AuthCommand::Remove { account } => {
            let profile = resolve_cursor_account(&account)?;
            let replacement = cursor_auth::remove_cursor_account(&profile.id)?;
            // Usage snapshots live outside the credential registry. A removed
            // account must not leave stale quota data behind for a later TUI.
            let _ =
                claude_cursor_proxy::providers::cursor::usage::remove_account_usage(&profile.id);
            println!("Removed Cursor account: {}", profile.id);
            if let Some(replacement) = replacement {
                println!(
                    "Active Cursor account: {}",
                    account_display_name(&replacement.auth)
                );
                println!("Account id: {}", replacement.id);
            } else {
                println!("No Cursor account is active.");
            }
            Ok(())
        }
        AuthCommand::Usage { account, json } => {
            let accounts = cursor_auth::list_cursor_accounts()?;
            if accounts.is_empty() {
                anyhow::bail!("No Cursor accounts saved; run `cursor auth login` first");
            }
            let selected = match account {
                Some(ref selector) => {
                    vec![resolve_cursor_account_from(&accounts, selector)?.clone()]
                }
                None => accounts,
            };
            // Dashboard calls are blocking and can each spend several seconds.
            // Keep all-account usage responsive without opening an unbounded
            // wave of sockets, while retaining the deterministic account order
            // in both text and JSON output.
            let usage_results = fetch_cursor_usage_bounded(selected);
            let mut rows = Vec::with_capacity(usage_results.len());
            for (profile, usage) in usage_results {
                match usage {
                    Ok(snapshot) => {
                        if json {
                            rows.push(serde_json::json!({
                                "id": profile.id,
                                "label": profile.label,
                                "email": profile.auth.email,
                                "active": profile.active,
                                "usage": usage_snapshot_json(&snapshot),
                            }));
                        } else {
                            let marker = if profile.active { '*' } else { ' ' };
                            println!("{marker} {}  {}", profile.id, snapshot.header_line());
                        }
                    }
                    Err(error) if json => rows.push(serde_json::json!({
                        "id": profile.id,
                        "label": profile.label,
                        "email": profile.auth.email,
                        "active": profile.active,
                        "error": error.to_string(),
                    })),
                    Err(error) => {
                        let marker = if profile.active { '*' } else { ' ' };
                        println!("{marker} {}  usage failed: {error}", profile.id);
                    }
                }
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&rows)?);
            }
            Ok(())
        }
        _ => unreachable!("cursor account command dispatched separately"),
    }
}

fn account_display_name(auth: &claude_cursor_proxy::providers::cursor::auth::CursorAuth) -> String {
    auth.email
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Cursor account".to_string())
}

fn resolve_cursor_account(
    selector: &str,
) -> Result<claude_cursor_proxy::providers::cursor::auth::CursorAccountProfile> {
    let accounts = claude_cursor_proxy::providers::cursor::auth::list_cursor_accounts()?;
    resolve_cursor_account_from(&accounts, selector).cloned()
}

fn resolve_cursor_account_from<'a>(
    accounts: &'a [claude_cursor_proxy::providers::cursor::auth::CursorAccountProfile],
    selector: &str,
) -> Result<&'a claude_cursor_proxy::providers::cursor::auth::CursorAccountProfile> {
    let selector = selector.trim();
    if selector.is_empty() {
        anyhow::bail!("Cursor account id or email is required");
    }
    let matches: Vec<_> = accounts
        .iter()
        .filter(|account| {
            account.id.trim().eq_ignore_ascii_case(selector)
                || account
                    .auth
                    .email
                    .as_deref()
                    .is_some_and(|email| email.trim().eq_ignore_ascii_case(selector))
                || account
                    .label
                    .as_deref()
                    .is_some_and(|label| label.trim().eq_ignore_ascii_case(selector))
        })
        .collect();
    match matches.as_slice() {
        [account] => Ok(account),
        [] => anyhow::bail!("Cursor account not found: {selector}"),
        _ => anyhow::bail!("Cursor account selector is ambiguous: {selector}"),
    }
}

const MAX_CURSOR_USAGE_WORKERS: usize = 8;

fn fetch_cursor_usage_bounded(
    profiles: Vec<claude_cursor_proxy::providers::cursor::auth::CursorAccountProfile>,
) -> Vec<(
    claude_cursor_proxy::providers::cursor::auth::CursorAccountProfile,
    std::result::Result<claude_cursor_proxy::monitor::AccountUsageSnapshot, String>,
)> {
    let results = run_bounded_ordered(&profiles, MAX_CURSOR_USAGE_WORKERS, |profile| {
        let auth =
            claude_cursor_proxy::providers::cursor::auth::refresh_cursor_account_for_usage(profile)
                .map_err(|error| error.to_string());
        auth.and_then(|auth| {
            claude_cursor_proxy::providers::cursor::usage::fetch_account_usage(&auth)
                .map_err(|error| error.to_string())
                .inspect(|snapshot| {
                    // Keep CLI usage and the TUI on the same durable snapshot.
                    // The helper also migrates a legacy synthetic id when a
                    // token refresh changes its opaque bearer digest.
                    let _ = claude_cursor_proxy::providers::cursor::usage::persist_account_usage_for_profile(
                        profile,
                        &auth,
                        snapshot,
                    );
                })
        })
    });
    profiles.into_iter().zip(results).collect()
}

/// Execute one blocking task per item with a fixed worker ceiling. Results are
/// indexed by input position, so callers can render them in stable order even
/// when individual requests complete out of order.
fn run_bounded_ordered<T, R, F>(items: &[T], max_workers: usize, task: F) -> Vec<R>
where
    T: Sync,
    R: Send,
    F: Fn(&T) -> R + Sync,
{
    if items.is_empty() {
        return Vec::new();
    }
    let workers = items.len().min(max_workers.max(1));
    let next = AtomicUsize::new(0);
    let (tx, rx) = mpsc::channel();

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let tx = tx.clone();
            let next = &next;
            let task = &task;
            scope.spawn(move || {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= items.len() {
                        break;
                    }
                    if tx.send((index, task(&items[index]))).is_err() {
                        break;
                    }
                }
            });
        }
    });
    drop(tx);

    let mut results: Vec<Option<R>> = (0..items.len()).map(|_| None).collect();
    for (index, result) in rx {
        if index < results.len() {
            results[index] = Some(result);
        }
    }
    results
        .into_iter()
        .map(|result| result.expect("bounded worker did not return a result"))
        .collect()
}

fn usage_snapshot_json(
    snapshot: &claude_cursor_proxy::monitor::AccountUsageSnapshot,
) -> serde_json::Value {
    serde_json::json!({
        "email": snapshot.email,
        "membership": snapshot.membership,
        "autoPercent": snapshot.auto_percent,
        "apiPercent": snapshot.api_percent,
        "totalPercent": snapshot.total_percent,
        "planUsedUsd": snapshot.plan_used_usd,
        "planLimitUsd": snapshot.plan_limit_usd,
        "onDemandUsedUsd": snapshot.on_demand_used_usd,
        "onDemandLimitUsd": snapshot.on_demand_limit_usd,
        "grokBotPercent": snapshot.grok_bot_percent,
        "grokBotPeriodStart": snapshot.grok_bot_period_start,
        "grokBotReset": snapshot.grok_bot_reset,
        "totalCostUsd": snapshot.total_cost_usd,
        "usageEventCount": snapshot.usage_event_count,
        "usageEvents": snapshot.usage_events.iter().map(|event| serde_json::json!({
            "timestamp": event.timestamp,
            "model": event.model,
            "chargedUsd": event.charged_usd,
            "kind": event.kind,
        })).collect::<Vec<_>>(),
    })
}

fn print_models(registry: &Registry, full: bool) {
    let grouped = registry.grouped_models();
    for provider in ["codex", "kimi", "grok", "cursor"] {
        let Some(models) = grouped.get(provider) else {
            continue;
        };
        if full || provider != "cursor" {
            println!("{provider}: {}", models.join(", "));
        } else {
            println!("{provider}: {}", compact_cursor_list(models));
        }
    }
}

fn compact_cursor_list(models: &[String]) -> String {
    let mut legacy = Vec::new();
    let mut dynamic = Vec::new();
    for model in models {
        if !model.contains(':') {
            legacy.push(model.clone());
        } else {
            dynamic.push(model.clone());
        }
    }
    let mut out = String::new();
    if !legacy.is_empty() {
        out.push_str(&legacy.join(", "));
        out.push_str("; ");
    }
    out.push_str(&format!("{} cursor model aliases", dynamic.len()));
    if !dynamic.is_empty() {
        out.push_str(", example: cursor:gpt-5.5");
    }
    out.push_str(" run `claude-cursor-proxy models --full` for all aliases");
    out
}

fn listen_url(bind_address: &str, port: u16) -> String {
    match bind_address.parse::<std::net::IpAddr>() {
        Ok(ip) => format!("http://{}", std::net::SocketAddr::new(ip, port)),
        Err(_) => format!("http://{bind_address}:{port}"),
    }
}

fn print_server_banner(bind_address: &str, port: u16, registry: &Registry) {
    println!("Proxy listening on {}", listen_url(bind_address, port));
    println!("Logs: {}", paths::log_file().display());
    let cfg = paths::config_dir();
    if cfg.exists() {
        println!("Config: {}", cfg.display());
    }
    print_models(registry, false);
    println!();
    println!("Configure Claude Code (pick a model from above):");
    println!("  export ANTHROPIC_BASE_URL=\"http://localhost:{port}\"");
    println!("  export ANTHROPIC_AUTH_TOKEN=\"anything\"");
    println!("  export ANTHROPIC_MODEL=\"gpt-5.6-sol\"");
    println!("  export ANTHROPIC_SMALL_FAST_MODEL=\"gpt-5.6-luna\"");
    println!("  export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1");
}

#[allow(dead_code)]
fn alias_names() -> usize {
    ANTHROPIC_STYLE_ALIASES.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_serve_selects_monitor_on_tty() {
        assert_eq!(select_serve_mode(true, false), ServeMode::Monitor);
    }

    #[test]
    fn no_monitor_selects_plain_mode() {
        assert_eq!(select_serve_mode(true, true), ServeMode::Plain);
    }

    #[test]
    fn non_tty_stdout_selects_plain_mode() {
        assert_eq!(select_serve_mode(false, false), ServeMode::Plain);
    }

    #[test]
    fn demo_command_parses_without_server_options() {
        let cli = Cli::try_parse_from(["claude-cursor-proxy", "demo"]).unwrap();

        assert!(matches!(cli.command, Some(Commands::Demo)));
    }

    #[test]
    fn mcp_doctor_command_parses_options() {
        let cli = Cli::try_parse_from([
            "claude-cursor-proxy",
            "mcp-doctor",
            "--cwd",
            "/tmp/project",
            "--repair",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Some(Commands::McpDoctor { options }) => {
                assert_eq!(options.cwd, Some(std::path::PathBuf::from("/tmp/project")));
                assert!(options.repair);
                assert!(options.json);
            }
            other => panic!("expected mcp-doctor command, got {other:?}"),
        }
    }

    #[test]
    fn listen_url_brackets_ipv6_addresses() {
        assert_eq!(listen_url("::1", 18765), "http://[::1]:18765");
    }

    #[test]
    fn cursor_account_selector_ignores_case_and_outer_whitespace() {
        use claude_cursor_proxy::providers::cursor::auth::{CursorAccountProfile, CursorAuth};

        let profile = CursorAccountProfile {
            id: "ACCOUNT-ID".to_string(),
            label: Some("Work Laptop".to_string()),
            auth: CursorAuth {
                access_token: "token".to_string(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: None,
                email: Some("Person@Example.com".to_string()),
                source: "test".to_string(),
            },
            active: false,
        };
        let accounts = vec![profile];

        assert_eq!(
            resolve_cursor_account_from(&accounts, "  account-id  ")
                .unwrap()
                .id,
            "ACCOUNT-ID"
        );
        assert_eq!(
            resolve_cursor_account_from(&accounts, " person@example.COM ")
                .unwrap()
                .id,
            "ACCOUNT-ID"
        );
        assert_eq!(
            resolve_cursor_account_from(&accounts, "  work laptop ")
                .unwrap()
                .id,
            "ACCOUNT-ID"
        );
    }

    #[test]
    fn bounded_usage_worker_preserves_order_and_respects_ceiling() {
        use std::sync::Arc;
        use std::time::Duration;

        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak_observed = Arc::clone(&peak);
        let active_observed = Arc::clone(&active);
        let items: Vec<usize> = (0..24).collect();
        let results = run_bounded_ordered(&items, 3, move |item| {
            let now = active_observed.fetch_add(1, Ordering::SeqCst) + 1;
            peak_observed.fetch_max(now, Ordering::SeqCst);
            // Make completion order differ from input order while workers are
            // active, then release the slot.
            std::thread::sleep(Duration::from_millis((4 - item % 4) as u64));
            active_observed.fetch_sub(1, Ordering::SeqCst);
            item * 2
        });

        assert_eq!(
            results,
            items.iter().map(|item| item * 2).collect::<Vec<_>>()
        );
        assert!(peak.load(Ordering::SeqCst) <= 3);
        assert!(peak.load(Ordering::SeqCst) > 1);
    }
}
