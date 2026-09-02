mod layout;

use layout::{
    CODE_WIDTH, COUNT_WIDTH, ColumnSpec, DURATION_WIDTH, EFFORT_WIDTH, ENDPOINT_WIDTH, ERROR_WIDTH,
    ID_WIDTH, LayoutTier, MODEL_MEDIUM_WIDTH, MODEL_NARROW_WIDTH, MODEL_WIDE_WIDTH,
    PROJECT_MEDIUM_WIDTH, PROJECT_WIDE_WIDTH, PROVIDER_WIDTH, RATE_WIDTH, STATUS_WIDTH, TIME_WIDTH,
    TOKEN_WIDTH,
};

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    io::{self, Stdout},
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, LazyLock, Mutex, mpsc},
    thread,
    time::{Duration, Instant, SystemTime},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use jiff::{Timestamp, Zoned, tz::TimeZone};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Wrap},
};
use tokio::sync::oneshot;

use crate::config::{self, SandRoutingPolicy};
use crate::{
    monitor::{
        ActiveRequest, CompletedRequest, MockMonitor, MonitorHandle, MonitorState,
        SESSION_TOKEN_BUCKET_SECS, SessionSummary,
    },
    paths,
    registry::Registry,
};

const TEAL: Color = Color::Rgb(78, 201, 176);
const WHITE: Color = Color::Rgb(240, 244, 248);
const DIM_WHITE: Color = Color::Rgb(180, 190, 200);
const SEPARATOR: Color = Color::Rgb(72, 74, 82);
const BG: Color = Color::Rgb(18, 18, 22);
const PANEL_BG: Color = Color::Rgb(22, 22, 27);
const SELECTED_BG: Color = Color::Rgb(42, 45, 54);
const GREEN: Color = Color::Rgb(120, 200, 120);
const RED: Color = Color::Rgb(220, 120, 120);
const YELLOW: Color = Color::Rgb(220, 200, 100);
const BLUE: Color = Color::Rgb(120, 170, 230);
const DIM: Color = Color::Rgb(100, 104, 114);
const SESSION_SPARKLINE_MIN_WIDTH: u16 = 170;
const SESSION_SPARKLINE_MAX_TOKENS: u64 = 4_000;
/// A dashboard call normally completes within a few seconds. This watchdog
/// bounds the UI state even if a worker gets stuck outside the HTTP timeout.
const ACCOUNT_USAGE_JOB_WATCHDOG: Duration = Duration::from_secs(60);

pub struct MonitorUiConfig<'a> {
    pub listen_url: String,
    pub port: u16,
    pub registry: &'a Registry,
    pub shutdown: Option<oneshot::Sender<()>>,
    pub shutdown_complete: Option<mpsc::Receiver<()>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MonitorExit {
    ShutdownComplete,
    ForceQuit,
}

pub fn run_monitor(
    handle: MonitorHandle,
    config: MonitorUiConfig<'_>,
) -> Result<MonitorExit, anyhow::Error> {
    run_monitor_loop(|| handle.snapshot(), config, None)
}

pub fn run_mock_monitor(port: u16, registry: &Registry) -> Result<(), anyhow::Error> {
    let mut monitor = MockMonitor::new();
    run_monitor_loop(
        move || monitor.snapshot(),
        MonitorUiConfig {
            listen_url: "mock://tui-demo".to_string(),
            port,
            registry,
            shutdown: None,
            shutdown_complete: None,
        },
        Some(mock_setup_text(port, registry)),
    )
    .map(|_| ())
}

fn run_monitor_loop(
    mut snapshot: impl FnMut() -> MonitorState,
    config: MonitorUiConfig<'_>,
    setup_text_override: Option<String>,
) -> Result<MonitorExit, anyhow::Error> {
    // A process can open the monitor more than once (for example, after a
    // terminal resize/restart in an embedding application). Do not inherit a
    // previous account usage fan-out into the new event loop.
    cancel_account_usage_workers();
    let mut terminal = setup_terminal()?;
    let _guard = TerminalGuard;
    let mut app = MonitorApp {
        listen_url: config.listen_url,
        setup_text: setup_text_override.unwrap_or_else(|| setup_text(config.port, config.registry)),
        show_setup: false,
        show_sand_settings: false,
        show_help: false,
        detail: None,
        focus: FocusPane::Sessions,
        selected: 0,
        recent_selected: 0,
        tick: 0,
        phase: MonitorPhase::Running,
        shutdown: config.shutdown,
        shutdown_complete: config.shutdown_complete,
        sand_models: sand_model_choices(config.registry),
        sand_policy: config::cursor_sand_policy(),
        sand_selected: 0,
        sand_message: None,
        sand_input: None,
    };

    let run_result = run_monitor_events(&mut terminal, &mut snapshot, &mut app);
    // Usage calls run outside the event loop. Signal detached workers before
    // leaving the TUI so a force-quit stops any queued fan-out; a request that
    // is already inside reqwest finishes at its normal bounded timeout.
    cancel_account_usage_workers();
    if run_result.is_err() {
        app.begin_shutdown();
        let state = snapshot();
        let _ = terminal.draw(|frame| render(frame, &mut app, &state));
        app.wait_for_shutdown_completion();
    }
    let cursor_result = terminal.show_cursor();
    let exit = run_result?;
    cursor_result?;
    Ok(exit)
}

fn run_monitor_events(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    mut snapshot: impl FnMut() -> MonitorState,
    app: &mut MonitorApp,
) -> Result<MonitorExit, anyhow::Error> {
    loop {
        poll_account_usage_results();
        let state = snapshot();
        app.clamp_selection(state.sessions.len(), state.recent.len());
        app.tick = app.tick.wrapping_add(1);
        terminal.draw(|frame| render(frame, app, &state))?;
        if app.shutdown_is_complete() {
            return Ok(MonitorExit::ShutdownComplete);
        }
        app.merge_cached_sand_models();
        if event::poll(Duration::from_millis(250))? {
            match event::read()? {
                Event::Key(key) => match key.code {
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        if app.handle_ctrl_c() {
                            return Ok(MonitorExit::ForceQuit);
                        }
                    }
                    _ if app.phase == MonitorPhase::ShuttingDown => {}
                    KeyCode::Char('y') if app.phase == MonitorPhase::ConfirmingShutdown => {
                        app.begin_shutdown()
                    }
                    KeyCode::Char('n') | KeyCode::Esc | KeyCode::Char('q')
                        if app.phase == MonitorPhase::ConfirmingShutdown =>
                    {
                        app.cancel_shutdown_confirmation()
                    }
                    _ if app.phase == MonitorPhase::ConfirmingShutdown => {}
                    _ if app.show_sand_settings => app.handle_sand_key(key.code),
                    _ if app.detail == Some(DetailView::AccountRoutes) => {
                        handle_account_route_key(app, key.code)
                    }
                    _ if app.detail == Some(DetailView::Accounts) => {
                        handle_account_key(app, key.code)
                    }
                    KeyCode::Char('q') => app.request_shutdown_confirmation(),
                    KeyCode::Char('?') => app.show_help = !app.show_help,
                    KeyCode::Char('b') => app.show_setup = !app.show_setup,
                    KeyCode::Char('u') => {
                        app.detail = Some(DetailView::Usage);
                        app.show_setup = false;
                        app.show_sand_settings = false;
                        app.show_help = false;
                    }
                    KeyCode::Char('a') => open_accounts_view(app),
                    KeyCode::Char('m') => open_account_routes(app),
                    KeyCode::Char('s') => {
                        app.refresh_sand_models();
                        app.show_sand_settings = true;
                        app.show_setup = false;
                        app.show_help = false;
                    }
                    KeyCode::Tab => app.focus = app.focus.next(),
                    KeyCode::Down => app.move_down(state.sessions.len(), state.recent.len(), true),
                    KeyCode::Char('j') => {
                        app.move_down(state.sessions.len(), state.recent.len(), false)
                    }
                    KeyCode::Up => app.move_up(state.sessions.len(), state.recent.len(), true),
                    KeyCode::Char('k') => {
                        app.move_up(state.sessions.len(), state.recent.len(), false)
                    }
                    KeyCode::Right => app.focus = FocusPane::Recent,
                    KeyCode::Left => app.focus = FocusPane::Sessions,
                    KeyCode::Enter => {
                        app.detail = match app.focus {
                            FocusPane::Sessions if !state.sessions.is_empty() => {
                                Some(DetailView::Session)
                            }
                            FocusPane::Recent if !state.recent.is_empty() => {
                                Some(DetailView::Request)
                            }
                            _ => None,
                        }
                    }
                    KeyCode::Esc => {
                        if app.show_help {
                            app.show_help = false;
                        } else if app.show_setup {
                            app.show_setup = false;
                        } else if app.show_sand_settings {
                            app.show_sand_settings = false;
                        } else {
                            app.detail = None;
                        }
                    }
                    _ => {}
                },
                Event::Resize(_, _) => {}
                _ => {}
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FocusPane {
    Sessions,
    Recent,
}

impl FocusPane {
    fn next(self) -> Self {
        match self {
            Self::Sessions => Self::Recent,
            Self::Recent => Self::Sessions,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DetailView {
    Session,
    Request,
    Usage,
    Accounts,
    AccountRoutes,
}

/// State for the account manager overlay.  It is kept outside `MonitorApp` so
/// the monitor's request snapshot remains the single source of truth and the
/// account list can be refreshed without threading credentials through every
/// render/test fixture.
#[derive(Default)]
struct AccountUiState {
    accounts: Vec<crate::providers::cursor::auth::CursorAccountProfile>,
    selected: usize,
    usage: HashMap<String, crate::monitor::AccountUsageState>,
    /// Identity metadata hydrated from the durable usage cache.  The account
    /// registry remains authoritative for credentials; this map only fills in
    /// labels/emails while a registry refresh is unavailable.
    account_metadata: HashMap<String, crate::providers::cursor::usage::AccountUsageCacheMetadata>,
    usage_rx: Option<mpsc::Receiver<AccountUsageEvent>>,
    /// Sender/receiver stay shared while the account panel is open. Each
    /// account still carries its own generation and cancellation token, so a
    /// refresh for one row never invalidates another row's worker.
    usage_tx: Option<mpsc::Sender<AccountUsageEvent>>,
    // Aggregate fields remain for compatibility with existing fixtures;
    // runtime cancellation/generation is isolated in the maps below.
    usage_pending: usize,
    usage_generation: u64,
    usage_cancel: Option<Arc<AtomicBool>>,
    usage_scope: Option<AccountUsageScope>,
    usage_loading: HashSet<String>,
    usage_generations: HashMap<String, u64>,
    usage_cancels: HashMap<String, Arc<AtomicBool>>,
    /// Explicit `u`/`U` refreshes requested while an account worker is still
    /// running are coalesced here.  The underlying blocking reqwest call does
    /// not support immediate cancellation; starting a replacement before it
    /// unwinds would create two dashboard requests for one account and can
    /// trigger Cursor's `already active` response.  A pending id is launched
    /// as soon as the current lease is released.
    usage_refresh_pending: HashSet<String>,
    /// Account ids whose visible snapshot came from the durable disk cache
    /// rather than the latest dashboard request.  Keeping this bit separate
    /// from `AccountUsageState` lets us retain a useful Ready snapshot while
    /// a refresh is in flight and makes its provenance explicit in the UI.
    usage_cached: HashSet<String>,
    /// Network work can outlive its UI loading state after the watchdog fires.
    /// Keep the account leased until its result or worker completion arrives so
    /// retry input cannot create a second request for the same account.
    usage_in_flight: HashMap<String, AccountUsageLease>,
    usage_waves: HashMap<u64, AccountUsageWave>,
    usage_next_wave: u64,
    /// Usage status messages are owned by the wave that started them. This
    /// prevents a late error from an older wave replacing a newer wave's
    /// fetching/success message.
    usage_message_wave: Option<u64>,
    usage_errors: HashMap<String, String>,
    /// Account id awaiting an explicit delete confirmation. Keeping the id
    /// (rather than the row index) prevents a concurrent refresh from
    /// deleting a different account than the one the user selected.
    delete_confirm: Option<AccountDeleteRequest>,
    message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AccountUsageScope {
    Selected(String),
    All,
}

fn selected_index_after_account_delete(
    selected_index: usize,
    remaining_len: usize,
    replacement_active_index: Option<usize>,
    was_active: bool,
) -> usize {
    if remaining_len == 0 {
        return 0;
    }
    if was_active {
        return replacement_active_index
            .unwrap_or(selected_index.min(remaining_len - 1))
            .min(remaining_len - 1);
    }
    selected_index.min(remaining_len - 1)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AccountDeleteRequest {
    id: String,
    display_name: String,
    active: bool,
    selected_index: usize,
}

struct AccountUsageResult {
    account_id: String,
    state: crate::monitor::AccountUsageState,
    generation: u64,
}

enum AccountUsageEvent {
    Result {
        result: Box<AccountUsageResult>,
        wave_id: u64,
        source_credential_fingerprint: u64,
        credential_fingerprint: u64,
    },
    WaveComplete {
        wave_id: u64,
    },
}

struct AccountUsageWave {
    expected_workers: usize,
    completed_workers: usize,
    accounts: Vec<AccountUsageWaveAccount>,
    deadline: Instant,
}

#[derive(Clone)]
struct AccountUsageWaveAccount {
    account_id: String,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AccountUsageLease {
    wave_id: u64,
    generation: u64,
}

/// Every fan-out worker owns one guard. A panic unwinds through `Drop`, so a
/// retained UI sender can no longer strand the corresponding wave in loading.
struct AccountUsageWorkerGuard {
    tx: mpsc::Sender<AccountUsageEvent>,
    wave_id: u64,
    account_ids: Vec<String>,
}

impl Drop for AccountUsageWorkerGuard {
    fn drop(&mut self) {
        release_account_usage_leases(&self.account_ids, self.wave_id);
        let _ = self.tx.send(AccountUsageEvent::WaveComplete {
            wave_id: self.wave_id,
        });
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum AccountRoutePane {
    #[default]
    Models,
    Accounts,
}

/// State for the model-to-account routing editor. Like the account usage
/// state, this stays outside `MonitorApp` so existing monitor fixtures and
/// request snapshots remain unchanged while the overlay is open.
#[derive(Default)]
struct AccountRouteUiState {
    models: Vec<String>,
    policy: config::CursorAccountRoutingPolicy,
    /// Selected model row. Kept as `selected` for compatibility with the
    /// existing route tests and persisted UI state.
    selected: usize,
    /// Account option row. `0` is the explicit `automatic` option; saved
    /// accounts start at one. Keeping the automatic row in the index makes
    /// clearing a route discoverable instead of an implicit side effect.
    account_selected: usize,
    pane: AccountRoutePane,
    input: Option<String>,
    message: Option<String>,
}

static ACCOUNT_ROUTE_UI: LazyLock<Mutex<AccountRouteUiState>> =
    LazyLock::new(|| Mutex::new(AccountRouteUiState::default()));

fn account_route_ui_lock() -> std::sync::MutexGuard<'static, AccountRouteUiState> {
    ACCOUNT_ROUTE_UI
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

static ACCOUNT_UI: LazyLock<Mutex<AccountUiState>> =
    LazyLock::new(|| Mutex::new(AccountUiState::default()));

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MonitorPhase {
    Running,
    ConfirmingShutdown,
    ShuttingDown,
}

struct MonitorApp {
    listen_url: String,
    setup_text: String,
    show_setup: bool,
    show_sand_settings: bool,
    show_help: bool,
    detail: Option<DetailView>,
    focus: FocusPane,
    selected: usize,
    recent_selected: usize,
    tick: usize,
    phase: MonitorPhase,
    shutdown: Option<oneshot::Sender<()>>,
    shutdown_complete: Option<mpsc::Receiver<()>>,
    sand_models: Vec<String>,
    sand_policy: SandRoutingPolicy,
    sand_selected: usize,
    sand_message: Option<String>,
    sand_input: Option<String>,
}

impl MonitorApp {
    fn handle_ctrl_c(&mut self) -> bool {
        if self.phase == MonitorPhase::ShuttingDown {
            true
        } else {
            self.begin_shutdown();
            false
        }
    }

    fn request_shutdown_confirmation(&mut self) {
        if self.phase == MonitorPhase::Running {
            self.phase = MonitorPhase::ConfirmingShutdown;
        }
    }

    fn cancel_shutdown_confirmation(&mut self) {
        if self.phase == MonitorPhase::ConfirmingShutdown {
            self.phase = MonitorPhase::Running;
        }
    }

    fn begin_shutdown(&mut self) {
        if self.phase == MonitorPhase::ShuttingDown {
            return;
        }
        self.phase = MonitorPhase::ShuttingDown;
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    fn shutdown_is_complete(&self) -> bool {
        let Some(shutdown_complete) = &self.shutdown_complete else {
            return self.phase == MonitorPhase::ShuttingDown;
        };
        match shutdown_complete.try_recv() {
            Ok(()) | Err(mpsc::TryRecvError::Disconnected) => true,
            Err(mpsc::TryRecvError::Empty) => false,
        }
    }

    fn wait_for_shutdown_completion(&self) {
        if !self.shutdown_is_complete()
            && let Some(shutdown_complete) = &self.shutdown_complete
        {
            let _ = shutdown_complete.recv();
        }
    }

    fn handle_sand_key(&mut self, key: KeyCode) {
        if self.sand_input.is_some() {
            self.handle_sand_input_key(key);
            return;
        }
        match key {
            KeyCode::Esc | KeyCode::Char('s') => {
                self.show_sand_settings = false;
                self.sand_message = None;
            }
            // Keep the global usage shortcut available while the Sand model
            // list is open, so users do not have to close the overlay first.
            KeyCode::Char('u') => {
                self.detail = Some(DetailView::Usage);
                self.show_sand_settings = false;
                self.show_setup = false;
                self.show_help = false;
                self.sand_message = None;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.sand_selected = self.sand_selected.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.sand_selected = self
                    .sand_selected
                    .saturating_add(1)
                    .min(self.sand_models.len().saturating_sub(1));
            }
            KeyCode::Char(' ') | KeyCode::Enter => self.toggle_selected_sand_model(),
            KeyCode::Char('a') => {
                if std::env::var_os("CCP_CURSOR_SAND_MODELS").is_some() {
                    self.sand_message = Some(
                        "CCP_CURSOR_SAND_MODELS is active; unset it to add a model".to_string(),
                    );
                } else {
                    self.sand_input = Some(String::new());
                    self.sand_message = None;
                }
            }
            _ => {}
        }
    }

    fn handle_sand_input_key(&mut self, key: KeyCode) {
        match key {
            KeyCode::Esc => self.sand_input = None,
            KeyCode::Enter => self.save_custom_sand_model(),
            KeyCode::Backspace => {
                if let Some(input) = self.sand_input.as_mut() {
                    input.pop();
                }
            }
            KeyCode::Char(value) if !value.is_control() => {
                if let Some(input) = self.sand_input.as_mut()
                    && input.len() < 160
                {
                    input.push(value);
                }
            }
            _ => {}
        }
    }

    fn save_custom_sand_model(&mut self) {
        let raw = self.sand_input.clone().unwrap_or_default();
        let model = config::normalize_sand_model(&raw);
        if model.is_empty() {
            self.sand_message = Some("Model id cannot be empty".to_string());
            return;
        }
        if model.chars().any(char::is_whitespace) || model.contains(['*', '?']) {
            self.sand_message = Some("Enter one exact Cursor model id".to_string());
            return;
        }
        self.sand_input = None;

        self.sand_models.push(model.clone());
        self.sand_models.sort_unstable();
        self.sand_models.dedup();
        self.sand_selected = self
            .sand_models
            .iter()
            .position(|candidate| candidate == &model)
            .unwrap_or(0);

        if self.sand_policy.matches_model(&model) {
            self.sand_message = Some("Model already uses Sand".to_string());
            return;
        }
        let mut patterns = self.sand_policy.patterns().to_vec();
        patterns.push(model);
        let policy = SandRoutingPolicy::new(patterns);
        match config::persist_cursor_sand_policy(&policy) {
            Ok(()) => {
                self.sand_policy = policy;
                self.sand_message = Some("Added and enabled for Sand".to_string());
            }
            Err(error) => self.sand_message = Some(format!("Save failed: {error}")),
        }
    }

    fn refresh_sand_models(&mut self) {
        // Pick up edits made by another terminal before opening the editor.
        self.sand_policy = config::cursor_sand_policy();
        self.merge_cached_sand_models();
    }

    fn merge_cached_sand_models(&mut self) {
        self.sand_models
            .extend(crate::providers::cursor::model::cursor_supported_models());
        self.sand_models.extend(
            self.sand_policy
                .patterns()
                .iter()
                .filter(|pattern| !pattern.contains(['*', '?']))
                .cloned(),
        );
        self.sand_models.sort_unstable();
        self.sand_models.dedup();
        self.sand_selected = self
            .sand_selected
            .min(self.sand_models.len().saturating_sub(1));
    }

    fn toggle_selected_sand_model(&mut self) {
        if std::env::var_os("CCP_CURSOR_SAND_MODELS").is_some() {
            self.sand_message =
                Some("CCP_CURSOR_SAND_MODELS is active; unset it to edit config.json".to_string());
            return;
        }
        let Some(model) = self.sand_models.get(self.sand_selected) else {
            self.sand_message = Some("No Cursor models are available".to_string());
            return;
        };
        let normalized = config::normalize_sand_model(model);
        let mut patterns = self.sand_policy.patterns().to_vec();
        if let Some(index) = patterns.iter().position(|pattern| pattern == &normalized) {
            patterns.remove(index);
        } else if self.sand_policy.matches_model(&normalized) {
            self.sand_message = Some(
                "This model is covered by a wildcard Sand pattern; edit config.json to change it"
                    .to_string(),
            );
            return;
        } else {
            patterns.push(normalized);
        }
        let policy = SandRoutingPolicy::new(patterns);
        match config::persist_cursor_sand_policy(&policy) {
            Ok(()) => {
                self.sand_policy = policy;
                self.sand_message = Some("Saved; new requests use this policy".to_string());
            }
            Err(error) => {
                self.sand_message = Some(format!("Save failed: {error}"));
            }
        }
    }

    fn clamp_selection(&mut self, sessions: usize, recent: usize) {
        self.selected = self.selected.min(sessions.saturating_sub(1));
        self.recent_selected = self.recent_selected.min(recent.saturating_sub(1));
    }

    fn move_down(&mut self, sessions: usize, recent: usize, switch_panes: bool) {
        match self.focus {
            FocusPane::Sessions => {
                if switch_panes && self.selected >= sessions.saturating_sub(1) && recent > 0 {
                    self.focus = FocusPane::Recent;
                    self.recent_selected = 0;
                } else {
                    self.selected = self
                        .selected
                        .saturating_add(1)
                        .min(sessions.saturating_sub(1));
                }
            }
            FocusPane::Recent => {
                self.recent_selected = self
                    .recent_selected
                    .saturating_add(1)
                    .min(recent.saturating_sub(1));
            }
        }
    }

    fn move_up(&mut self, sessions: usize, recent: usize, switch_panes: bool) {
        match self.focus {
            FocusPane::Sessions => self.selected = self.selected.saturating_sub(1),
            FocusPane::Recent => {
                if switch_panes && self.recent_selected == 0 && sessions > 0 {
                    self.focus = FocusPane::Sessions;
                    self.selected = sessions.saturating_sub(1);
                } else {
                    self.recent_selected = self
                        .recent_selected
                        .saturating_sub(1)
                        .min(recent.saturating_sub(1));
                }
            }
        }
    }
}

fn account_ui_lock() -> std::sync::MutexGuard<'static, AccountUiState> {
    ACCOUNT_UI
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

fn release_account_usage_leases(account_ids: &[String], wave_id: u64) {
    let mut ui = account_ui_lock();
    for account_id in account_ids {
        if ui
            .usage_in_flight
            .get(account_id)
            .is_some_and(|lease| lease.wave_id == wave_id)
        {
            ui.usage_in_flight.remove(account_id);
        }
    }
}

fn release_account_usage_lease_locked(
    ui: &mut AccountUiState,
    account_id: &str,
    wave_id: u64,
    generation: u64,
) {
    if ui
        .usage_in_flight
        .get(account_id)
        .is_some_and(|lease| lease.wave_id == wave_id && lease.generation == generation)
    {
        ui.usage_in_flight.remove(account_id);
    }
}

fn cancel_usage_locked(ui: &mut AccountUiState) {
    // Keep the aggregate cancellation fields working for older callers and
    // fixtures; account-scoped workers use the map immediately below.
    if let Some(cancel) = ui.usage_cancel.take() {
        cancel.store(true, Ordering::Release);
    }
    ui.usage_generation = ui.usage_generation.wrapping_add(1);
    ui.usage_rx = None;
    ui.usage_tx = None;
    ui.usage_pending = 0;
    ui.usage_scope = None;
    ui.usage_message_wave = None;
    for cancel in ui.usage_cancels.drain().map(|(_, cancel)| cancel) {
        cancel.store(true, Ordering::Release);
    }
    for generation in ui.usage_generations.values_mut() {
        *generation = generation.wrapping_add(1);
    }
    ui.usage_waves.clear();
    // The receiver is discarded above, so late events from these cancelled
    // workers are intentionally unreachable. Drop their leases as well;
    // otherwise a worker that never returns could block a later panel open
    // forever. Wave ids are monotonic, so a late guard cannot clear a newer
    // lease after this reset.
    ui.usage_in_flight.clear();
    ui.usage_loading.clear();
    ui.usage_refresh_pending.clear();
}

fn cancel_account_usage_workers() {
    let mut ui = account_ui_lock();
    cancel_usage_locked(&mut ui);
}

fn cancel_account_usage_locked(ui: &mut AccountUiState, account_id: &str) {
    if let Some(cancel) = ui.usage_cancels.remove(account_id) {
        cancel.store(true, Ordering::Release);
    }
    let generation = ui
        .usage_generations
        .entry(account_id.to_string())
        .or_default();
    *generation = generation.wrapping_add(1);
    ui.usage_loading.remove(account_id);
}

/// Return whether an account already owns a live/retired worker lease.  A
/// force request is remembered for a follow-up wave rather than cancelling the
/// socket in place (blocking reqwest calls are only cooperatively cancellable).
fn account_usage_busy_locked(ui: &mut AccountUiState, account_id: &str, force: bool) -> bool {
    let busy = ui.usage_loading.contains(account_id) || ui.usage_in_flight.contains_key(account_id);
    if busy && force {
        ui.usage_refresh_pending.insert(account_id.to_string());
    }
    busy
}

fn ensure_account_usage_channel_locked(ui: &mut AccountUiState) -> mpsc::Sender<AccountUsageEvent> {
    if let Some(tx) = ui.usage_tx.as_ref() {
        return tx.clone();
    }
    let (tx, rx) = mpsc::channel();
    ui.usage_tx = Some(tx.clone());
    ui.usage_rx = Some(rx);
    tx
}

fn open_accounts_view(app: &mut MonitorApp) {
    app.detail = Some(DetailView::Accounts);
    app.show_setup = false;
    app.show_sand_settings = false;
    app.show_help = false;
    // A monitor instance can be reopened in the same process. Do not carry a
    // stale confirmation from a previous account panel into the new view.
    {
        let mut ui = account_ui_lock();
        ui.delete_confirm = None;
    }
    refresh_account_list();
    // Prime the selected account only.  Fetching every account is available
    // with `U`, while opening the panel stays responsive even with a large
    // account pool.
    request_account_usage(false);
}

/// Open the model-to-account editor from the account view.  Models already
/// known by the Sand selector are reused so a user can configure an account
/// for a live catalog model without typing its id; persisted route patterns
/// are merged as well to keep hand-authored rules visible.
fn open_account_routes(app: &mut MonitorApp) {
    // Re-read the registry whenever the editor opens. Accounts can be added,
    // removed, or switched from another terminal while this TUI remains
    // running; only refreshing an empty list leaves newly-added accounts
    // invisible in the chooser until the user manually presses `r`.
    refresh_account_list();
    let policy = config::cursor_account_routing_policy();
    let mut models = app.sand_models.clone();
    models.extend(policy.routes().iter().map(|rule| rule.model.clone()));
    models.sort_unstable();
    models.dedup();
    let accounts = account_ui_lock().accounts.clone();
    let mut ui = account_route_ui_lock();
    ui.models = models;
    ui.policy = policy;
    ui.selected = ui.selected.min(ui.models.len().saturating_sub(1));
    ui.account_selected = ui
        .models
        .get(ui.selected)
        .map(|model| account_route_account_index(&ui.policy, model, &accounts))
        .unwrap_or(0);
    ui.pane = AccountRoutePane::Models;
    ui.input = None;
    ui.message = None;
    app.show_setup = false;
    app.show_sand_settings = false;
    app.show_help = false;
    app.detail = Some(DetailView::AccountRoutes);
    drop(ui);
    // Usage is fetched for every account while this editor is open so the
    // account chooser can make quota decisions from current data. The work
    // runs on detached bounded workers and never blocks the TUI event loop.
    request_account_usage(true);
}

fn close_account_routes(app: &mut MonitorApp) {
    // Keep the account pane as the parent view so Esc/m behaves like the
    // other nested TUI details and the user can immediately inspect usage or
    // switch accounts after editing a route.
    app.detail = Some(DetailView::Accounts);
    let mut ui = account_route_ui_lock();
    ui.pane = AccountRoutePane::Models;
    ui.account_selected = 0;
    ui.input = None;
    ui.message = None;
}

fn account_routes_env_override_active() -> bool {
    std::env::var_os("CCP_CURSOR_MODEL_ACCOUNTS").is_some()
}

fn handle_account_route_key(app: &mut MonitorApp, key: KeyCode) {
    let input_active = account_route_ui_lock().input.is_some();
    if input_active {
        handle_account_route_input_key(key);
        return;
    }
    let pane = account_route_ui_lock().pane;
    match key {
        KeyCode::Esc => {
            close_account_routes(app);
        }
        KeyCode::Char('m') => close_account_routes(app),
        KeyCode::Tab | KeyCode::Right | KeyCode::Left => {
            let (next_pane, model, policy) = {
                let ui = account_route_ui_lock();
                let next_pane = match (ui.pane, key) {
                    (AccountRoutePane::Models, KeyCode::Tab | KeyCode::Right) => {
                        AccountRoutePane::Accounts
                    }
                    (AccountRoutePane::Accounts, KeyCode::Tab | KeyCode::Left) => {
                        AccountRoutePane::Models
                    }
                    (pane, _) => pane,
                };
                (
                    next_pane,
                    ui.models.get(ui.selected).cloned(),
                    ui.policy.clone(),
                )
            };
            let accounts = account_ui_lock().accounts.clone();
            let mut ui = account_route_ui_lock();
            ui.pane = next_pane;
            if ui.pane == AccountRoutePane::Accounts {
                ui.account_selected = model
                    .as_deref()
                    .map(|model| account_route_account_index(&policy, model, &accounts))
                    .unwrap_or(0);
            }
        }
        KeyCode::Up | KeyCode::Char('k') => move_account_route_selection(-1),
        KeyCode::Down | KeyCode::Char('j') => move_account_route_selection(1),
        KeyCode::Char('a') if pane == AccountRoutePane::Models => {
            let mut ui = account_route_ui_lock();
            if account_routes_env_override_active() {
                ui.message = Some(
                    "CCP_CURSOR_MODEL_ACCOUNTS is active; unset it to edit config.json".to_string(),
                );
            } else {
                ui.input = Some(String::new());
                ui.message = None;
            }
        }
        KeyCode::Char('r') => {
            let mut ui = account_route_ui_lock();
            ui.policy = config::cursor_account_routing_policy();
            let route_models = ui
                .policy
                .routes()
                .iter()
                .map(|rule| rule.model.clone())
                .collect::<Vec<_>>();
            ui.models.extend(route_models);
            ui.models.sort_unstable();
            ui.models.dedup();
            ui.selected = ui.selected.min(ui.models.len().saturating_sub(1));
            ui.message = Some("Model-account routes reloaded".to_string());
            drop(ui);
            refresh_account_list();
            request_account_usage_force(true);
            // Account order can change after a refresh. Re-select the account
            // matching the current model's persisted route rather than
            // leaving the cursor on a stale row.
            let (model, policy) = {
                let ui = account_route_ui_lock();
                (ui.models.get(ui.selected).cloned(), ui.policy.clone())
            };
            let accounts = account_ui_lock().accounts.clone();
            if let Some(model) = model {
                account_route_ui_lock().account_selected =
                    account_route_account_index(&policy, &model, &accounts);
            }
        }
        KeyCode::Char('u') => request_route_selected_usage(),
        KeyCode::Char('U') => request_account_usage_force(true),
        KeyCode::Char('x') | KeyCode::Delete | KeyCode::Backspace => {
            clear_selected_account_route();
            account_route_ui_lock().pane = AccountRoutePane::Models;
        }
        KeyCode::Enter | KeyCode::Char(' ') if pane == AccountRoutePane::Models => {
            let (model, policy) = {
                let ui = account_route_ui_lock();
                (ui.models.get(ui.selected).cloned(), ui.policy.clone())
            };
            let accounts = account_ui_lock().accounts.clone();
            let mut ui = account_route_ui_lock();
            ui.pane = AccountRoutePane::Accounts;
            ui.account_selected = model
                .as_deref()
                .map(|model| account_route_account_index(&policy, model, &accounts))
                .unwrap_or(0);
        }
        KeyCode::Enter | KeyCode::Char(' ') if pane == AccountRoutePane::Accounts => {
            assign_selected_account_route();
        }
        _ => {}
    }
}

fn request_route_selected_usage() {
    let option = account_route_ui_lock().account_selected;
    let account_index = option.checked_sub(1).or_else(|| {
        let ui = account_ui_lock();
        ui.accounts
            .iter()
            .position(|account| account.active)
            .or((ui.selected < ui.accounts.len()).then_some(ui.selected))
    });
    let Some(account_index) = account_index else {
        account_route_ui_lock().message = Some("No Cursor accounts are available".to_string());
        return;
    };
    let available = {
        let mut ui = account_ui_lock();
        if account_index >= ui.accounts.len() {
            false
        } else {
            // Reuse the existing usage worker and make the selected account
            // in the account manager follow the row inspected here.
            ui.selected = account_index;
            true
        }
    };
    if available {
        request_account_usage_force(false);
    } else {
        account_route_ui_lock().message =
            Some("Selected account is no longer available".to_string());
    }
}

fn handle_account_route_input_key(key: KeyCode) {
    match key {
        KeyCode::Esc => {
            let mut ui = account_route_ui_lock();
            ui.input = None;
        }
        KeyCode::Enter => save_custom_account_route_model(),
        KeyCode::Backspace => {
            let mut ui = account_route_ui_lock();
            if let Some(input) = ui.input.as_mut() {
                input.pop();
            }
        }
        KeyCode::Char(value) if !value.is_control() => {
            let mut ui = account_route_ui_lock();
            if let Some(input) = ui.input.as_mut()
                && input.len() < 160
            {
                input.push(value);
            }
        }
        _ => {}
    }
}

fn save_custom_account_route_model() {
    let raw = account_route_ui_lock().input.clone().unwrap_or_default();
    let model = config::normalize_sand_model(&raw);
    if model.is_empty() {
        account_route_ui_lock().message = Some("Model id cannot be empty".to_string());
        return;
    }
    if model.chars().any(char::is_whitespace) {
        account_route_ui_lock().message = Some("Model id cannot contain whitespace".to_string());
        return;
    }
    let mut ui = account_route_ui_lock();
    ui.input = None;
    if !ui.models.iter().any(|candidate| candidate == &model) {
        ui.models.push(model.clone());
        ui.models.sort_unstable();
    }
    ui.selected = ui
        .models
        .iter()
        .position(|candidate| candidate == &model)
        .unwrap_or(0);
    ui.message = Some("Model added; press Enter to choose an account".to_string());
}

fn selected_account_route_model() -> Option<String> {
    let ui = account_route_ui_lock();
    ui.models.get(ui.selected).cloned()
}

fn account_route_rules_with_assignment(
    policy: &config::CursorAccountRoutingPolicy,
    model: &str,
    account: Option<&str>,
) -> config::CursorAccountRoutingPolicy {
    policy.with_model_assignment(model, account)
}

/// Return the account option index for a model's currently persisted route.
/// Index zero is always the explicit automatic option. A selector that is
/// missing or resolves ambiguously intentionally falls back to automatic so
/// the chooser never highlights an account that would not be used.
fn account_route_account_index(
    policy: &config::CursorAccountRoutingPolicy,
    model: &str,
    accounts: &[crate::providers::cursor::auth::CursorAccountProfile],
) -> usize {
    let Some(selector) = policy.account_for_model(model) else {
        return 0;
    };
    let mut matches = accounts.iter().enumerate().filter(|(_, account)| {
        config::account_selector_matches(
            selector,
            &account.id,
            account.label.as_deref(),
            account.email(),
        )
    });
    let Some((index, _)) = matches.next() else {
        return 0;
    };
    if matches.next().is_some() {
        return 0;
    }
    index + 1
}

fn move_account_route_selection(delta: i8) {
    let account_len = account_ui_lock().accounts.len().saturating_add(1);
    let (pane, model_index, model_len, account_index, account_len) = {
        let ui = account_route_ui_lock();
        (
            ui.pane,
            ui.selected,
            ui.models.len(),
            ui.account_selected,
            account_len,
        )
    };
    let mut ui = account_route_ui_lock();
    match pane {
        AccountRoutePane::Models => {
            ui.selected = if delta.is_negative() {
                model_index.saturating_sub(delta.unsigned_abs() as usize)
            } else {
                model_index
                    .saturating_add(delta as usize)
                    .min(model_len.saturating_sub(1))
            };
            // Make the account pane immediately reflect the newly selected
            // model when the user tabs across.
            let model = ui.models.get(ui.selected).cloned();
            let policy = ui.policy.clone();
            drop(ui);
            if let Some(model) = model {
                let accounts = account_ui_lock().accounts.clone();
                let index = account_route_account_index(&policy, &model, &accounts);
                account_route_ui_lock().account_selected = index;
            }
        }
        AccountRoutePane::Accounts => {
            ui.account_selected = if delta.is_negative() {
                account_index.saturating_sub(delta.unsigned_abs() as usize)
            } else {
                account_index
                    .saturating_add(delta as usize)
                    .min(account_len.saturating_sub(1))
            };
        }
    }
}

fn assign_selected_account_route() {
    if account_routes_env_override_active() {
        account_route_ui_lock().message =
            Some("CCP_CURSOR_MODEL_ACCOUNTS is active; unset it to edit config.json".to_string());
        return;
    }
    let Some(model) = selected_account_route_model() else {
        account_route_ui_lock().message = Some("No models are available".to_string());
        return;
    };
    let (policy, selected) = {
        let ui = account_route_ui_lock();
        (ui.policy.clone(), ui.account_selected)
    };
    let accounts = account_ui_lock().accounts.clone();
    let selected_account = selected
        .checked_sub(1)
        .and_then(|index| accounts.get(index))
        .map(|account| account.id.clone());
    if selected > 0 && selected_account.is_none() {
        account_route_ui_lock().message =
            Some("Selected account is no longer available".to_string());
        return;
    }
    let next_policy =
        account_route_rules_with_assignment(&policy, &model, selected_account.as_deref());
    match config::persist_cursor_account_routes(&next_policy) {
        Ok(()) => {
            let mut ui = account_route_ui_lock();
            ui.policy = next_policy;
            ui.message = Some(match selected_account {
                Some(account) => {
                    let display = accounts
                        .iter()
                        .find(|candidate| candidate.id == account)
                        .map(|candidate| candidate.display_name().to_string())
                        .unwrap_or(account);
                    format!("{model} uses account {display}")
                }
                None => format!("{model} uses automatic account selection"),
            });
        }
        Err(error) => account_route_ui_lock().message = Some(format!("Save failed: {error}")),
    }
}

fn clear_selected_account_route() {
    if account_routes_env_override_active() {
        account_route_ui_lock().message =
            Some("CCP_CURSOR_MODEL_ACCOUNTS is active; unset it to edit config.json".to_string());
        return;
    }
    let Some(model) = selected_account_route_model() else {
        account_route_ui_lock().message = Some("No models are available".to_string());
        return;
    };
    let policy = account_route_ui_lock().policy.clone();
    let next_policy = account_route_rules_with_assignment(&policy, &model, None);
    match config::persist_cursor_account_routes(&next_policy) {
        Ok(()) => {
            let mut ui = account_route_ui_lock();
            ui.policy = next_policy;
            ui.message = Some(format!("{model} uses automatic account selection"));
        }
        Err(error) => account_route_ui_lock().message = Some(format!("Save failed: {error}")),
    }
}

fn refresh_account_list() {
    let result = crate::providers::cursor::auth::list_cursor_accounts();
    let persisted = crate::providers::cursor::usage::load_account_usage_cache();
    let persisted_metadata = crate::providers::cursor::usage::load_account_usage_cache_metadata();
    let mut ui = account_ui_lock();
    // Hydrate identity metadata even when the registry read below fails.  A
    // transient lock/parse error should not erase the names and emails that
    // were already learned from the last successful dashboard response.
    for (account_id, metadata) in &persisted_metadata {
        let should_replace = ui
            .account_metadata
            .get(account_id)
            .is_none_or(|current| metadata.fetched_at > current.fetched_at);
        if should_replace {
            ui.account_metadata
                .insert(account_id.clone(), metadata.clone());
        }
    }
    match result {
        Ok(accounts) => {
            let selected_index = ui.selected;
            let selected_id = ui
                .accounts
                .get(ui.selected)
                .map(|account| account.id.clone());
            ui.accounts = accounts;
            ui.selected = selected_id
                .and_then(|id| ui.accounts.iter().position(|account| account.id == id))
                .unwrap_or(selected_index)
                .min(ui.accounts.len().saturating_sub(1));
            if ui
                .delete_confirm
                .as_ref()
                .is_some_and(|pending| !ui.accounts.iter().any(|account| account.id == pending.id))
            {
                ui.delete_confirm = None;
            }
            let ids = ui
                .accounts
                .iter()
                .map(|account| account.id.clone())
                .collect::<std::collections::HashSet<_>>();
            ui.usage.retain(|id, _| ids.contains(id.as_str()));
            ui.usage_cached.retain(|id| ids.contains(id.as_str()));
            ui.usage_refresh_pending
                .retain(|id| ids.contains(id.as_str()));
            ui.account_metadata
                .retain(|id, _| ids.contains(id.as_str()));
            ui.usage_errors.retain(|id, _| ids.contains(id.as_str()));
            // A list refresh must not interrupt an account that is still
            // present. Only workers for removed accounts are invalidated.
            let removed = ui
                .usage_cancels
                .keys()
                .chain(ui.usage_loading.iter())
                .chain(ui.usage_generations.keys())
                .chain(ui.usage_in_flight.keys())
                .filter(|id| !ids.contains(id.as_str()))
                .cloned()
                .collect::<HashSet<_>>();
            for id in removed {
                cancel_account_usage_locked(&mut ui, &id);
                // The row no longer exists, so no future refresh can be
                // admitted for this id. Drop its detached-worker lease as
                // part of pruning state; a late guard is keyed by wave id
                // and cannot affect a different account row.
                ui.usage_in_flight.remove(&id);
                ui.usage.remove(&id);
                ui.usage_cached.remove(&id);
                ui.usage_refresh_pending.remove(&id);
                ui.account_metadata.remove(&id);
                ui.usage_errors.remove(&id);
            }
            // Successful snapshots survive process restarts and are loaded
            // before any fresh request starts. A live Ready value wins over a
            // stale disk value so an in-flight refresh remains visible.
            let account_ids = ui
                .accounts
                .iter()
                .map(|account| account.id.clone())
                .collect::<Vec<_>>();
            for account_id in account_ids {
                let should_load_cached = match ui.usage.get(&account_id) {
                    Some(crate::monitor::AccountUsageState::Ready(current)) => persisted
                        .get(&account_id)
                        .is_some_and(|cached| cached.fetched_at > current.fetched_at),
                    _ => persisted.contains_key(&account_id),
                };
                if should_load_cached && let Some(snapshot) = persisted.get(&account_id) {
                    ui.usage_cached.insert(account_id.clone());
                    ui.usage.insert(
                        account_id,
                        crate::monitor::AccountUsageState::Ready(snapshot.clone()),
                    );
                }
            }
            if ui.accounts.is_empty() {
                ui.message = Some(
                    "No Cursor accounts. Run `cursor auth login` or `cursor auth add`.".to_string(),
                );
            } else {
                ui.message = None;
            }
        }
        Err(error) => {
            // Keep the last account list and its cached meters visible when a
            // transient registry read fails. A failed refresh must not make a
            // usable account pool disappear from the TUI.
            if ui.accounts.is_empty() && !persisted.is_empty() {
                // A fresh TUI process has no in-memory rows to retain. Build
                // display-only rows from the credential-free cache so users
                // can still inspect names, meters, and fetch times while the
                // registry is temporarily unreadable. They are marked with a
                // cache source and are excluded from network refresh jobs
                // until a later `r` successfully reloads credentials.
                let mut ids = persisted.keys().cloned().collect::<Vec<_>>();
                ids.sort_unstable();
                ui.accounts = ids
                    .into_iter()
                    .map(|id| {
                        let metadata = persisted_metadata.get(&id);
                        let email = metadata.and_then(|entry| entry.email.clone()).or_else(|| {
                            persisted
                                .get(&id)
                                .and_then(|snapshot| snapshot.email.clone())
                        });
                        crate::providers::cursor::auth::CursorAccountProfile {
                            id,
                            label: metadata.and_then(|entry| entry.label.clone()),
                            auth: crate::providers::cursor::auth::CursorAuth {
                                access_token: String::new(),
                                refresh_token: None,
                                api_key: None,
                                expires: None,
                                user_id: None,
                                email,
                                source: "account-usage-cache".to_string(),
                            },
                            active: metadata.and_then(|entry| entry.active).unwrap_or(false),
                        }
                    })
                    .collect();
                ui.accounts.sort_by_key(|account| {
                    (!account.active, account.display_name().to_ascii_lowercase())
                });
                ui.usage = persisted
                    .iter()
                    .map(|(id, snapshot)| {
                        (
                            id.clone(),
                            crate::monitor::AccountUsageState::Ready(snapshot.clone()),
                        )
                    })
                    .collect();
                ui.usage_cached = persisted.keys().cloned().collect();
                ui.selected = ui.selected.min(ui.accounts.len().saturating_sub(1));
            }
            ui.message = Some(format!("Account list refresh failed: {error}"));
        }
    }
}

fn handle_account_key(app: &mut MonitorApp, key: KeyCode) {
    // Confirmation owns the account pane until it is resolved. In
    // particular, navigation must not change the target between `d` and
    // `y`.
    if account_ui_lock().delete_confirm.is_some() {
        match key {
            KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                confirm_account_delete();
            }
            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc | KeyCode::Char('d') => {
                cancel_account_delete();
            }
            _ => {}
        }
        return;
    }
    match key {
        KeyCode::Esc | KeyCode::Char('a') => {
            app.detail = None;
            cancel_account_usage_workers();
            let mut ui = account_ui_lock();
            ui.message = None;
        }
        KeyCode::Up | KeyCode::Char('k') => {
            let mut ui = account_ui_lock();
            ui.selected = ui.selected.saturating_sub(1);
        }
        KeyCode::Down | KeyCode::Char('j') => {
            let mut ui = account_ui_lock();
            ui.selected = ui
                .selected
                .saturating_add(1)
                .min(ui.accounts.len().saturating_sub(1));
        }
        KeyCode::Enter => switch_selected_account(),
        // Lowercase `u` is an explicit refresh of the selected row.  It must
        // be able to replace a request that has gone idle; otherwise opening
        // the panel's automatic warm-up can permanently occupy the row.
        KeyCode::Char('u') => request_account_usage_force(false),
        KeyCode::Char('U') => request_account_usage_force(true),
        KeyCode::Char('d') | KeyCode::Char('D') => request_account_delete(),
        KeyCode::Char('r') => {
            refresh_account_list();
            request_account_usage_force(true);
        }
        KeyCode::Char('m') => open_account_routes(app),
        _ => {}
    }
}

fn request_account_delete() {
    let mut ui = account_ui_lock();
    // Clone the immutable row data before mutating the UI state below. This
    // avoids holding an account-row borrow across `cancel_usage_locked` and
    // keeps a concurrent usage cancellation from tripping the borrow checker.
    let target = ui.accounts.get(ui.selected).map(|account| {
        (
            account.id.clone(),
            account_name_for_display(account, account.email()),
            account.active,
        )
    });
    let Some((id, display_name, active)) = target else {
        ui.message = Some("No Cursor accounts are available".to_string());
        return;
    };
    let selected_index = ui.selected;
    // Stop an in-flight usage fan-out before asking for confirmation. This
    // keeps a deleted account from receiving a late usage result while the
    // confirmation prompt is on screen.
    cancel_usage_locked(&mut ui);
    ui.message = None;
    ui.delete_confirm = Some(AccountDeleteRequest {
        id,
        display_name,
        active,
        selected_index,
    });
}

fn cancel_account_delete() {
    let mut ui = account_ui_lock();
    ui.delete_confirm = None;
    ui.message = None;
}

fn confirm_account_delete() {
    let target = {
        let mut ui = account_ui_lock();
        ui.delete_confirm.take()
    };
    let Some(target) = target else {
        return;
    };

    match crate::providers::cursor::auth::remove_cursor_account(&target.id) {
        Ok(replacement) => {
            // Usage snapshots are keyed separately from credentials. Remove
            // this row even when the cache write is unavailable; account
            // deletion itself has already succeeded and remains authoritative.
            let _ = crate::providers::cursor::usage::remove_account_usage(&target.id);
            refresh_account_list();
            {
                let mut ui = account_ui_lock();
                let replacement_index = replacement.as_ref().and_then(|replacement| {
                    ui.accounts
                        .iter()
                        .position(|account| account.id == replacement.id)
                });
                // Keep the active replacement selected when the removed
                // account was active; otherwise retain the nearest row so a
                // delete in a large pool does not jump to the top.
                ui.selected = selected_index_after_account_delete(
                    target.selected_index,
                    ui.accounts.len(),
                    replacement_index,
                    target.active,
                );
                ui.message = Some(if target.active {
                    match replacement {
                        Some(replacement) => format!(
                            "Removed {}; active account: {}",
                            target.display_name,
                            replacement.display_name()
                        ),
                        None => format!(
                            "Removed {}; no Cursor account is active",
                            target.display_name
                        ),
                    }
                } else {
                    format!("Removed {}", target.display_name)
                });
            }
            let has_accounts = {
                let ui = account_ui_lock();
                !ui.accounts.is_empty()
            };
            if has_accounts {
                request_account_usage(false);
            }
        }
        Err(error) => {
            let mut ui = account_ui_lock();
            ui.message = Some(format!("Account removal failed: {error}"));
        }
    }
}

fn switch_selected_account() {
    let selected = {
        let ui = account_ui_lock();
        ui.accounts.get(ui.selected).cloned()
    };
    let Some(account) = selected else {
        let mut ui = account_ui_lock();
        ui.message = Some("No Cursor accounts are available".to_string());
        return;
    };
    if account.active {
        let mut ui = account_ui_lock();
        ui.message = Some(format!("{} is already active", account.display_name()));
        return;
    }
    match crate::providers::cursor::auth::switch_cursor_account(&account.id) {
        Ok(switched) => {
            refresh_account_list();
            {
                let mut ui = account_ui_lock();
                ui.message = Some(format!("Active account: {}", switched.display_name()));
            }
            request_account_usage(false);
        }
        Err(error) => {
            let mut ui = account_ui_lock();
            ui.message = Some(format!("Account switch failed: {error}"));
        }
    }
}

fn request_account_usage(all: bool) {
    request_account_usage_inner(all, false);
}

fn request_account_usage_force(all: bool) {
    request_account_usage_inner(all, true);
}

fn request_account_usage_inner(all: bool, force: bool) {
    let requested_profiles = {
        let ui = account_ui_lock();
        if all {
            ui.accounts.clone()
        } else {
            ui.accounts.get(ui.selected).cloned().into_iter().collect()
        }
    };
    if requested_profiles.is_empty() {
        let mut ui = account_ui_lock();
        ui.message = Some("No Cursor accounts are available".to_string());
        return;
    }
    request_account_usage_profiles(requested_profiles, all, force);
}

/// Start usage workers for an explicit set of profiles. Keeping this separate
/// from [`request_account_usage_inner`] lets a coalesced force-refresh launch
/// the account that was selected earlier even after the user has navigated to
/// another row.
fn request_account_usage_profiles(
    requested_profiles: Vec<crate::providers::cursor::auth::CursorAccountProfile>,
    all: bool,
    force: bool,
) {
    // Rows reconstructed from the credential-free identity cache are useful
    // for inspection but intentionally cannot make dashboard requests.  Keep
    // them visible until the registry recovers instead of attempting a call
    // with an empty bearer.
    let profiles = requested_profiles
        .into_iter()
        .filter(|profile| !profile.auth.access_token.trim().is_empty())
        .collect::<Vec<_>>();
    if profiles.is_empty() {
        let mut ui = account_ui_lock();
        ui.message = Some(
            "Account credentials are unavailable; press r to reload the account list".to_string(),
        );
        return;
    }

    let mut jobs = Vec::new();
    let mut wave_id = None;
    let mut wave_tx = None;
    {
        let mut ui = account_ui_lock();
        let mut skipped = 0usize;
        for profile in profiles {
            let account_id = profile.id.clone();
            // A blocking dashboard request cannot be interrupted reliably
            // once reqwest has entered the socket read. Coalesce an explicit
            // force refresh behind the current lease instead of admitting an
            // overlapping request (which Cursor reports as 503 already-active).
            if account_usage_busy_locked(&mut ui, &account_id, force) {
                skipped += 1;
                continue;
            }
            // This id may have been queued by an earlier explicit refresh. It
            // is now being admitted, so remove the marker before spawning.
            ui.usage_refresh_pending.remove(&account_id);
            let generation = ui
                .usage_generations
                .entry(account_id.clone())
                .or_default()
                .wrapping_add(1);
            ui.usage_generations.insert(account_id.clone(), generation);
            let cancel = Arc::new(AtomicBool::new(false));
            ui.usage_cancels
                .insert(account_id.clone(), Arc::clone(&cancel));
            ui.usage_loading.insert(account_id.clone());
            ui.usage_errors.remove(&account_id);
            // Keep a previous Ready snapshot on screen while the network
            // request is running. Unknown is used only for a true cache miss.
            if !ui.usage.contains_key(&account_id) {
                ui.usage.insert(
                    account_id.clone(),
                    crate::monitor::AccountUsageState::Unknown,
                );
            }
            jobs.push((profile, generation, cancel));
        }
        ui.usage_pending = ui.usage_loading.len();
        ui.usage_scope = Some(if all {
            AccountUsageScope::All
        } else {
            AccountUsageScope::Selected(
                jobs.first()
                    .map(|(profile, _, _)| profile.id.clone())
                    .unwrap_or_default(),
            )
        });
        if !jobs.is_empty() {
            let next_wave = ui.usage_next_wave.wrapping_add(1);
            ui.usage_next_wave = next_wave;
            let worker_count = jobs.len().min(8);
            let jobs_per_worker = jobs.len().div_ceil(worker_count);
            ui.usage_waves.insert(
                next_wave,
                AccountUsageWave {
                    expected_workers: worker_count,
                    completed_workers: 0,
                    accounts: jobs
                        .iter()
                        .map(|(profile, generation, _)| AccountUsageWaveAccount {
                            account_id: profile.id.clone(),
                            generation: *generation,
                        })
                        .collect(),
                    deadline: Instant::now()
                        + ACCOUNT_USAGE_JOB_WATCHDOG
                            .saturating_mul(u32::try_from(jobs_per_worker).unwrap_or(u32::MAX)),
                },
            );
            for (profile, generation, _) in &jobs {
                ui.usage_in_flight.insert(
                    profile.id.clone(),
                    AccountUsageLease {
                        wave_id: next_wave,
                        generation: *generation,
                    },
                );
            }
            wave_id = Some(next_wave);
            wave_tx = Some(ensure_account_usage_channel_locked(&mut ui));
            ui.usage_message_wave = Some(next_wave);
            ui.message = Some(if all {
                format!("Fetching usage for {} account(s)...", jobs.len())
            } else {
                "Fetching account usage...".to_string()
            });
        } else if skipped > 0 {
            // Leave an existing status intact while the selected row is
            // already being fetched. A force call has been coalesced and will
            // start automatically after the worker releases its lease.
            ui.message = Some(if force {
                "Account usage refresh queued; keeping one request per account".to_string()
            } else {
                "Account usage is already being fetched".to_string()
            });
        }
    }

    if jobs.is_empty() {
        return;
    }

    // Keep usage fan-out bounded within this refresh wave. Separate waves may
    // coexist for different account ids; their generations isolate results.
    let worker_count = jobs.len().min(8);
    let wave_id = wave_id.expect("usage wave is assigned when jobs are present");
    let tx = wave_tx.expect("usage channel is assigned when jobs are present");
    let jobs = Arc::new(jobs);
    for worker in 0..worker_count {
        let jobs = Arc::clone(&jobs);
        let tx = tx.clone();
        let spawn_failure_tx = tx.clone();
        let worker_account_ids = jobs
            .iter()
            .enumerate()
            .filter(|(index, _)| index % worker_count == worker)
            .map(|(_, (profile, _, _))| profile.id.clone())
            .collect::<Vec<_>>();
        let spawn_failure_account_ids = worker_account_ids.clone();
        let spawn_result = thread::Builder::new()
            .name(format!("cursor-usage-{wave_id}-{worker}"))
            .spawn(move || {
                let _guard = AccountUsageWorkerGuard {
                    tx: tx.clone(),
                    wave_id,
                    account_ids: worker_account_ids,
                };
                for index in (worker..jobs.len()).step_by(worker_count) {
                    let (profile, generation, cancel) = &jobs[index];
                    if cancel.load(Ordering::Acquire) {
                        continue;
                    }
                    let (result, source_credential_fingerprint, credential_fingerprint) =
                        fetch_account_usage_result(profile, *generation);
                    let _ = tx.send(AccountUsageEvent::Result {
                        result: result.into(),
                        wave_id,
                        source_credential_fingerprint,
                        credential_fingerprint,
                    });
                }
            });
        // Thread creation is the one failure mode that cannot run the guard;
        // account for it as an immediately completed worker so loading is
        // still cleared and the user can retry.
        if spawn_result.is_err() {
            release_account_usage_leases(&spawn_failure_account_ids, wave_id);
            let _ = spawn_failure_tx.send(AccountUsageEvent::WaveComplete { wave_id });
        }
    }
}

/// Launch coalesced explicit refreshes whose previous worker has now released
/// its account lease. This is deliberately called outside the account mutex;
/// worker creation may allocate and can otherwise block result application.
fn drain_pending_account_usage() {
    let profiles = {
        let mut ui = account_ui_lock();
        let ready_ids = ui
            .usage_refresh_pending
            .iter()
            .filter(|account_id| {
                !ui.usage_loading.contains(*account_id)
                    && !ui.usage_in_flight.contains_key(*account_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        if ready_ids.is_empty() {
            return;
        }
        let mut profiles = Vec::new();
        for account_id in ready_ids {
            ui.usage_refresh_pending.remove(&account_id);
            if let Some(profile) = ui.accounts.iter().find(|profile| profile.id == account_id) {
                // Cache-only rows have no credentials and must wait for a
                // successful registry reload instead of spinning a worker.
                if !profile.auth.access_token.trim().is_empty() {
                    profiles.push(profile.clone());
                }
            }
        }
        profiles
    };
    if !profiles.is_empty() {
        request_account_usage_profiles(profiles, false, false);
    }
}

fn fetch_account_usage_result(
    profile: &crate::providers::cursor::auth::CursorAccountProfile,
    generation: u64,
) -> (AccountUsageResult, u64, u64) {
    let source_credential_fingerprint = account_credential_fingerprint(profile);
    let (state, credential_fingerprint) =
        match crate::providers::cursor::auth::refresh_cursor_account_for_usage(profile) {
            Ok(auth) => {
                let credential_fingerprint = account_auth_credential_fingerprint(&auth);
                let state = match crate::providers::cursor::usage::fetch_account_usage(&auth) {
                    Ok(snapshot) => {
                        // Keep a successful snapshot even when the panel closed while
                        // the request was in flight. Cache persistence checks account
                        // existence under the registry lock, so a late result cannot
                        // resurrect a row deleted concurrently.
                        let _ = crate::providers::cursor::usage::persist_account_usage_for_profile(
                            profile, &auth, &snapshot,
                        );
                        crate::monitor::AccountUsageState::Ready(snapshot)
                    }
                    Err(error) => crate::monitor::AccountUsageState::Failed(error.to_string()),
                };
                (state, credential_fingerprint)
            }
            Err(error) => (
                crate::monitor::AccountUsageState::Failed(error.to_string()),
                source_credential_fingerprint,
            ),
        };
    (
        AccountUsageResult {
            account_id: profile.id.clone(),
            state,
            generation,
        },
        source_credential_fingerprint,
        credential_fingerprint,
    )
}

fn account_credential_fingerprint(
    profile: &crate::providers::cursor::auth::CursorAccountProfile,
) -> u64 {
    account_auth_credential_fingerprint(&profile.auth)
}

fn account_auth_credential_fingerprint(auth: &crate::providers::cursor::auth::CursorAuth) -> u64 {
    // This is an in-process equality token only. Never persist or display it;
    // hashing avoids putting bearer/refresh material into the event queue.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    auth.access_token.hash(&mut hasher);
    auth.refresh_token.hash(&mut hasher);
    auth.api_key.hash(&mut hasher);
    auth.user_id.hash(&mut hasher);
    auth.email.hash(&mut hasher);
    hasher.finish()
}

fn mark_usage_account_failed(ui: &mut AccountUiState, account_id: &str, error: &str) {
    ui.usage_errors
        .insert(account_id.to_string(), error.to_string());
    if !matches!(
        ui.usage.get(account_id),
        Some(crate::monitor::AccountUsageState::Ready(_))
    ) {
        ui.usage.insert(
            account_id.to_string(),
            crate::monitor::AccountUsageState::Failed(error.to_string()),
        );
    }
}

/// Finish a wave and clear only the rows owned by that wave. A later wave for
/// another account (or a replacement generation for the same account) is left
/// untouched.
fn finish_account_usage_wave(
    ui: &mut AccountUiState,
    wave_id: u64,
    error: &str,
    invalidate_generation: bool,
) {
    let Some(wave) = ui.usage_waves.remove(&wave_id) else {
        return;
    };
    let mut stranded = false;
    for account in wave.accounts {
        let current_generation = ui
            .usage_generations
            .get(&account.account_id)
            .copied()
            .unwrap_or_default();
        if current_generation != account.generation
            || !ui.usage_loading.contains(&account.account_id)
        {
            continue;
        }
        if invalidate_generation {
            cancel_account_usage_locked(ui, &account.account_id);
            // Keep the lease until the detached worker's guard runs. The
            // watchdog only retires the UI generation; reqwest may still be
            // reading the socket, and admitting a replacement here would
            // overlap requests for one Cursor account (which commonly yields
            // 503 `already active`). A force `u` is coalesced in
            // `usage_refresh_pending` and drained after this lease is truly
            // released by the worker result/guard.
        } else {
            ui.usage_loading.remove(&account.account_id);
            ui.usage_cancels.remove(&account.account_id);
        }
        mark_usage_account_failed(ui, &account.account_id, error);
        stranded = true;
    }
    if stranded
        && ui.usage_message_wave == Some(wave_id)
        && ui.message.as_deref().is_some_and(is_usage_fetching_message)
    {
        ui.message = Some(format!("{error}; press u to retry"));
    }
}

fn is_usage_fetching_message(message: &str) -> bool {
    message.starts_with("Fetching account usage") || message.starts_with("Fetching usage for ")
}

fn complete_account_usage_wave(ui: &mut AccountUiState, wave_id: u64) {
    let complete = if let Some(wave) = ui.usage_waves.get_mut(&wave_id) {
        wave.completed_workers = wave.completed_workers.saturating_add(1);
        wave.completed_workers >= wave.expected_workers
    } else {
        false
    };
    if complete {
        // A worker guard sends this marker after all of that worker's results,
        // so once every marker is observed no result from this wave is still
        // in flight on the channel.
        finish_account_usage_wave(ui, wave_id, "Account usage worker stopped", false);
    }
}

fn expire_account_usage_waves(ui: &mut AccountUiState) {
    let now = Instant::now();
    let expired = ui
        .usage_waves
        .iter()
        .filter(|(_, wave)| wave.deadline <= now)
        .map(|(wave_id, _)| *wave_id)
        .collect::<Vec<_>>();
    for wave_id in expired {
        finish_account_usage_wave(ui, wave_id, "Account usage refresh timed out", true);
    }
}

#[cfg(test)]
fn apply_account_usage_event_result(
    ui: &mut AccountUiState,
    result: AccountUsageResult,
    credential_fingerprint: u64,
) {
    apply_account_usage_event_result_for_wave(
        ui,
        result,
        credential_fingerprint,
        credential_fingerprint,
        None,
    );
}

fn apply_account_usage_event_result_for_wave(
    ui: &mut AccountUiState,
    result: AccountUsageResult,
    source_credential_fingerprint: u64,
    credential_fingerprint: u64,
    wave_id: Option<u64>,
) {
    // Release the exact lease before any generation or credential checks. A
    // stale result still represents completion of its detached request; if
    // this is deferred until the current-generation branch, a timed-out wave
    // can keep an account blocked forever.
    if let Some(wave_id) = wave_id {
        release_account_usage_lease_locked(ui, &result.account_id, wave_id, result.generation);
    }
    let current_credential_fingerprint = ui
        .accounts
        .iter()
        .find(|account| account.id == result.account_id)
        .map(account_credential_fingerprint);
    if current_credential_fingerprint.is_some_and(|current| {
        current != source_credential_fingerprint && current != credential_fingerprint
    }) {
        // The account id can remain stable while a login replaces its bearer.
        // Do not let a response captured before that replacement overwrite the
        // new account's meter.
        if ui.usage_generations.get(&result.account_id).copied() == Some(result.generation) {
            cancel_account_usage_locked(ui, &result.account_id);
            mark_usage_account_failed(
                ui,
                &result.account_id,
                "Account credentials changed; press u to retry",
            );
            if wave_id.is_none_or(|wave_id| {
                ui.usage_message_wave == Some(wave_id)
                    && ui.message.as_deref().is_some_and(is_usage_fetching_message)
            }) {
                ui.message = Some("Account credentials changed; press u to retry".to_string());
            }
        }
        return;
    }
    apply_account_usage_result_for_wave(ui, result, wave_id);
}

fn apply_account_usage_event_locked(ui: &mut AccountUiState, event: AccountUsageEvent) {
    match event {
        AccountUsageEvent::Result {
            result,
            source_credential_fingerprint,
            credential_fingerprint,
            wave_id,
        } => {
            // A retired wave cannot update UI state, but its result still
            // completes the exact detached request lease.
            if ui.usage_waves.contains_key(&wave_id) {
                apply_account_usage_event_result_for_wave(
                    ui,
                    *result,
                    source_credential_fingerprint,
                    credential_fingerprint,
                    Some(wave_id),
                );
            } else {
                release_account_usage_lease_locked(
                    ui,
                    &result.account_id,
                    wave_id,
                    result.generation,
                );
            }
        }
        AccountUsageEvent::WaveComplete { wave_id } => complete_account_usage_wave(ui, wave_id),
    }
}

fn poll_account_usage_results() {
    let mut ui = account_ui_lock();
    let mut events = Vec::new();
    let mut channel_disconnected = false;
    if let Some(rx) = ui.usage_rx.as_ref() {
        loop {
            match rx.try_recv() {
                Ok(event) => events.push(event),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    channel_disconnected = true;
                    break;
                }
            }
        }
    }
    for event in events {
        apply_account_usage_event_locked(&mut ui, event);
    }
    if channel_disconnected {
        // This remains a fallback for an externally dropped receiver. Normal
        // worker panic/exit is handled by the per-worker completion guard.
        ui.usage_rx = None;
        ui.usage_tx = None;
        let stranded = ui.usage_loading.iter().cloned().collect::<Vec<_>>();
        ui.usage_waves.clear();
        for account_id in &stranded {
            cancel_account_usage_locked(&mut ui, account_id);
            mark_usage_account_failed(&mut ui, account_id, "usage worker stopped");
        }
        if !stranded.is_empty() {
            ui.message = Some("Account usage worker stopped; press u to retry".to_string());
        }
    }
    expire_account_usage_waves(&mut ui);
    ui.usage_pending = ui.usage_loading.len();
    if ui.usage_pending == 0 {
        ui.usage_scope = None;
        if ui.message.as_deref().is_some_and(is_usage_fetching_message) {
            ui.message = Some("Usage updated".to_string());
        }
    }
    drop(ui);
    // A force refresh requested while another worker was active is launched
    // only after the result/guard released that account's lease. This keeps
    // the UI responsive while guaranteeing one dashboard request per account.
    drain_pending_account_usage();
}

#[cfg(test)]
fn apply_account_usage_result(ui: &mut AccountUiState, result: AccountUsageResult) {
    apply_account_usage_result_for_wave(ui, result, None);
}

/// Apply a result only while its account generation is current. `wave_id`
/// scopes status-message ownership so a late failure from an older fan-out
/// cannot replace the message posted by a newer wave.
fn apply_account_usage_result_for_wave(
    ui: &mut AccountUiState,
    result: AccountUsageResult,
    wave_id: Option<u64>,
) {
    let current_generation = ui
        .usage_generations
        .get(&result.account_id)
        .copied()
        .unwrap_or_default();
    if result.generation != current_generation {
        return;
    }
    ui.usage_loading.remove(&result.account_id);
    ui.usage_cancels.remove(&result.account_id);
    match result.state {
        crate::monitor::AccountUsageState::Failed(error) => {
            ui.usage_errors
                .insert(result.account_id.clone(), error.clone());
            let display_name = ui
                .accounts
                .iter()
                .find(|account| account.id == result.account_id)
                .map(|account| account.display_name().to_string())
                .unwrap_or_else(|| short_account_id(&result.account_id));
            let message = format!(
                "Usage refresh failed for {}: {}",
                display_name,
                ellipsize(&error, 120)
            );
            // A wave owns the global usage message only until another wave
            // starts. Legacy/unit-test calls without a wave retain the
            // historical behavior and always publish the failure.
            if wave_id.is_none_or(|wave_id| ui.usage_message_wave == Some(wave_id)) {
                ui.message = Some(message);
            }
            if !matches!(
                ui.usage.get(&result.account_id),
                Some(crate::monitor::AccountUsageState::Ready(_))
            ) {
                ui.usage.insert(
                    result.account_id,
                    crate::monitor::AccountUsageState::Failed(error),
                );
            }
        }
        state => {
            ui.usage_errors.remove(&result.account_id);
            let should_apply = match (ui.usage.get(&result.account_id), &state) {
                (
                    Some(crate::monitor::AccountUsageState::Ready(current)),
                    crate::monitor::AccountUsageState::Ready(next),
                ) => next.fetched_at >= current.fetched_at,
                _ => true,
            };
            if should_apply {
                if matches!(state, crate::monitor::AccountUsageState::Ready(_)) {
                    // A successful network response supersedes the snapshot
                    // that was hydrated from disk.
                    ui.usage_cached.remove(&result.account_id);
                }
                ui.usage.insert(result.account_id, state);
            }
        }
    }
}

fn sand_model_choices(registry: &Registry) -> Vec<String> {
    let mut models = registry.supported_models_for("cursor");
    models.extend(crate::providers::cursor::model::cursor_supported_models());
    models.sort_unstable();
    models.dedup();
    models
}

impl Drop for MonitorApp {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>, anyhow::Error> {
    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;
    Ok(terminal)
}

fn render(frame: &mut ratatui::Frame<'_>, app: &mut MonitorApp, state: &MonitorState) {
    let area = frame.area();
    frame.render_widget(Block::default().style(Style::default().bg(BG)), area);

    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Percentage(30),
            Constraint::Percentage(20),
            Constraint::Percentage(25),
            Constraint::Percentage(25),
            Constraint::Length(1),
        ])
        .split(area);

    render_header(frame, root[0], app, state);
    if matches!(
        app.detail,
        Some(DetailView::Accounts | DetailView::AccountRoutes)
    ) {
        let accounts_area = Rect {
            x: root[1].x,
            y: root[1].y,
            width: root[1].width,
            height: root[4].y + root[4].height - root[1].y,
        };
        if app.detail == Some(DetailView::AccountRoutes) {
            render_account_routes_detail(frame, accounts_area);
        } else {
            render_accounts_detail(frame, accounts_area);
        }
    } else if matches!(app.detail, Some(DetailView::Usage)) {
        // Usage is a full-height detail view so period, cost, and event rows
        // remain visible on ordinary 24-row terminals.
        let usage_area = Rect {
            x: root[1].x,
            y: root[1].y,
            width: root[1].width,
            height: root[4].y + root[4].height - root[1].y,
        };
        render_usage_detail(frame, usage_area, state);
    } else {
        match app.detail {
            Some(DetailView::Session) => render_session_detail(frame, root[1], state, app.selected),
            Some(DetailView::Request) => {
                render_request_detail(frame, root[1], state, app.recent_selected)
            }
            Some(DetailView::Usage) => unreachable!("usage detail handled above"),
            Some(DetailView::Accounts) => unreachable!("accounts detail handled above"),
            Some(DetailView::AccountRoutes) => unreachable!("account routes handled above"),
            None => render_sessions(
                frame,
                root[1],
                &state.sessions,
                app.selected,
                app.focus == FocusPane::Sessions,
            ),
        }
        render_active(frame, root[2], &state.active, app.tick);
        render_recent(
            frame,
            root[3],
            &state.recent,
            app.recent_selected,
            app.focus == FocusPane::Recent,
        );
        render_events(frame, root[4], &state.recent);
    }
    render_footer(frame, root[5], app);

    if app.show_setup {
        render_setup_overlay(frame, area, &app.setup_text);
    }
    if app.show_sand_settings {
        render_sand_settings_overlay(frame, area, app);
    }
    if app.show_help {
        render_help_overlay(frame, area);
    }
    if matches!(app.detail, Some(DetailView::Accounts)) {
        render_account_delete_confirmation(frame, area);
    }
    match app.phase {
        MonitorPhase::Running => {}
        MonitorPhase::ConfirmingShutdown => render_shutdown_confirmation(frame, area),
        MonitorPhase::ShuttingDown => render_shutdown_overlay(frame, area, app.tick),
    }
}

fn render_header(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    app: &MonitorApp,
    state: &MonitorState,
) {
    let uptime = state
        .started_at
        .elapsed()
        .unwrap_or_else(|_| Duration::from_secs(0));
    let top = Line::from(vec![
        Span::styled(
            " claude-cursor-proxy",
            Style::default()
                .fg(BG)
                .bg(TEAL)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  ", Style::default().fg(BG).bg(TEAL)),
        Span::styled(&app.listen_url, Style::default().fg(BG).bg(TEAL)),
        Span::styled("  uptime ", Style::default().fg(BG).bg(TEAL)),
        Span::styled(
            format_duration(uptime),
            Style::default()
                .fg(BG)
                .bg(TEAL)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  sessions ", Style::default().fg(BG).bg(TEAL)),
        Span::styled(
            state.sessions.len().to_string(),
            Style::default()
                .fg(BG)
                .bg(TEAL)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  active ", Style::default().fg(BG).bg(TEAL)),
        Span::styled(
            state.active.len().to_string(),
            Style::default()
                .fg(BG)
                .bg(TEAL)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    let usage_line = state.account_usage.header_line();
    let usage = Line::from(Span::styled(
        format!(" {usage_line}"),
        Style::default()
            .fg(usage_header_color(&state.account_usage))
            .bg(PANEL_BG),
    ));
    frame.render_widget(
        Paragraph::new(vec![top, usage]).style(Style::default().bg(PANEL_BG)),
        area,
    );
}

fn usage_header_color(usage: &crate::monitor::AccountUsageState) -> Color {
    match usage {
        crate::monitor::AccountUsageState::Ready(snapshot) => {
            let hottest = [
                snapshot.total_percent,
                snapshot.auto_percent,
                snapshot.api_percent,
                snapshot.grok_bot_percent,
                usage_ratio_percent(snapshot.plan_used_usd, snapshot.plan_limit_usd),
                usage_ratio_percent(snapshot.on_demand_used_usd, snapshot.on_demand_limit_usd),
            ]
            .into_iter()
            .flatten()
            .filter(|value| value.is_finite())
            .fold(0.0_f64, f64::max);
            // Usage is a quota indicator, not an error state. Keep the header
            // on the warning palette even when a meter is fully consumed;
            // reserve red for an actual failed usage fetch below.
            if hottest >= 70.0 { YELLOW } else { DIM_WHITE }
        }
        crate::monitor::AccountUsageState::Failed(_) => RED,
        crate::monitor::AccountUsageState::MissingAuth => YELLOW,
        crate::monitor::AccountUsageState::Unknown => DIM_WHITE,
    }
}

fn usage_ratio_percent(used: Option<f64>, limit: Option<f64>) -> Option<f64> {
    let (Some(used), Some(limit)) = (used, limit) else {
        return None;
    };
    if !used.is_finite() || !limit.is_finite() || limit <= 0.0 {
        return None;
    }
    Some((used / limit * 100.0).clamp(0.0, 100.0))
}

fn panel(title: &'static str, focused: bool) -> Block<'static> {
    let color = if focused { TEAL } else { SEPARATOR };
    Block::default()
        .title(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(if focused { TEAL } else { DIM_WHITE })
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(color))
        .style(Style::default().bg(PANEL_BG))
}

fn table_header_aligned(
    cells: impl IntoIterator<Item = (&'static str, Alignment)>,
) -> Row<'static> {
    Row::new(
        cells
            .into_iter()
            .map(|(cell, alignment)| {
                Cell::from(
                    Line::from(Span::styled(cell, Style::default().fg(TEAL))).alignment(alignment),
                )
            })
            .collect::<Vec<_>>(),
    )
    .style(Style::default().add_modifier(Modifier::BOLD))
}

fn render_empty_table_state(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    title: &'static str,
    focused: bool,
    message: &str,
) {
    frame.render_widget(panel(title, focused), area);
    let content = Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    };
    if content.width == 0 || content.height == 0 {
        return;
    }

    let line = Rect {
        y: content.y + content.height.saturating_sub(1) / 2,
        height: 1,
        ..content
    };
    frame.render_widget(
        Paragraph::new(ellipsize(message, line.width.into()))
            .alignment(Alignment::Center)
            .style(Style::default().fg(DIM).bg(PANEL_BG)),
        line,
    );
}

fn muted_cell(value: impl Into<String>) -> Cell<'static> {
    Cell::from(Span::styled(value.into(), Style::default().fg(DIM)))
}

fn text_cell(value: impl Into<String>) -> Cell<'static> {
    Cell::from(Span::styled(value.into(), Style::default().fg(DIM_WHITE)))
}

fn model_cell_with_client_type(
    provider: Option<&str>,
    value: Option<&str>,
    width: usize,
    client_type_override: Option<&str>,
) -> Cell<'static> {
    let model = value.unwrap_or("-");
    if provider != Some("cursor") || model == "-" {
        return text_cell(ellipsize(model, width));
    }

    let client_type = client_type_override
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| config::cursor_client_type_for_model(model));
    let marker = format!(" [{}]", client_type);
    let marker_width = marker.chars().count();
    if width <= marker_width {
        return text_cell(ellipsize(&marker, width));
    }

    let model_width = width.saturating_sub(marker_width);
    let model_text = ellipsize(model, model_width);
    let marker_color = if client_type.eq_ignore_ascii_case("sand") {
        TEAL
    } else {
        DIM
    };
    Cell::from(Line::from(vec![
        Span::styled(model_text, Style::default().fg(DIM_WHITE)),
        Span::styled(marker, Style::default().fg(marker_color)),
    ]))
}

fn table_column_width(area: Rect, widths: &[Constraint], column: usize) -> usize {
    let table_width = area.width.saturating_sub(2);
    Layout::horizontal(widths.to_vec())
        .spacing(1)
        .split(Rect::new(0, 0, table_width, 1))
        .get(column)
        .map_or(0, |rect| usize::from(rect.width))
}

fn ellipsize(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_string();
    }
    if width == 0 {
        return String::new();
    }

    value
        .chars()
        .take(width.saturating_sub(1))
        .chain(std::iter::once('…'))
        .collect()
}

fn display_session_id(session_id: Option<&str>) -> &str {
    let Some(session_id) = session_id.filter(|value| !value.is_empty()) else {
        return "no-session";
    };
    if uuid::Uuid::parse_str(session_id).is_ok() {
        // UUIDv7 puts its timestamp in the first eight characters, so sibling
        // agents created together share that prefix. The final eight hex
        // characters come from the random portion and still fit ID_WIDTH.
        return session_id
            .get(session_id.len().saturating_sub(8)..)
            .unwrap_or(session_id);
    }
    session_id
}

fn number_cell(value: impl Into<String>) -> Cell<'static> {
    Cell::from(
        Line::from(Span::styled(value.into(), Style::default().fg(DIM_WHITE)))
            .alignment(Alignment::Right),
    )
}

fn status_cell(value: &str) -> Cell<'static> {
    Cell::from(Span::styled(value.to_string(), status_style(value)))
}

fn status_style(value: &str) -> Style {
    Style::default().fg(status_color(value))
}

fn status_color(value: &str) -> Color {
    match value {
        "completed" => GREEN,
        "streaming" => TEAL,
        "failed" => RED,
        // A downstream cancellation is an expected lifecycle edge, not an
        // upstream/API failure. Keep it visible in request details without
        // making the dashboard read like an error storm.
        "abandoned" => DIM,
        "upstream" => BLUE,
        "selected" | "started" => YELLOW,
        _ => DIM_WHITE,
    }
}

fn http_status_style(status: Option<u16>) -> Style {
    Style::default().fg(http_status_color(status))
}

fn http_status_color(status: Option<u16>) -> Color {
    match status {
        Some(200..=299) => GREEN,
        Some(400..=499) => YELLOW,
        Some(500..=599) => RED,
        Some(_) => DIM_WHITE,
        None => DIM,
    }
}

fn rate_cell(value: String) -> Cell<'static> {
    let color = if value.contains("tok/s") {
        TEAL
    } else if value == "-" {
        DIM
    } else {
        DIM_WHITE
    };
    Cell::from(
        Line::from(Span::styled(value, Style::default().fg(color))).alignment(Alignment::Right),
    )
}

fn provider_cell(value: Option<&str>) -> Cell<'static> {
    let value = value.unwrap_or("-");
    let color = match value {
        "codex" => TEAL,
        "kimi" => Color::Rgb(190, 150, 220),
        "cursor" => Color::Rgb(140, 170, 230),
        "-" => DIM,
        _ => DIM_WHITE,
    };
    Cell::from(Span::styled(value.to_string(), Style::default().fg(color)))
}

fn detail_cell(value: &str) -> Cell<'static> {
    if value.is_empty() || value == "-" {
        Cell::from(Span::styled("", Style::default().fg(DIM)))
    } else {
        Cell::from(Span::styled(value.to_string(), Style::default().fg(YELLOW)))
    }
}

fn error_indicator(request: &CompletedRequest) -> &'static str {
    if request.status == crate::monitor::RequestStatus::Failed
        || request.http_status.is_some_and(|status| status >= 400)
        || (request.status != crate::monitor::RequestStatus::Abandoned
            && request
                .error
                .as_deref()
                .is_some_and(|error| !error.is_empty()))
    {
        "!"
    } else {
        ""
    }
}

fn compact_tokens(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}M", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn token_value(value: Option<u64>) -> String {
    value.map(compact_tokens).unwrap_or_else(|| "-".to_string())
}

fn spinner(tick: usize) -> &'static str {
    const FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    FRAMES[tick % FRAMES.len()]
}

fn sparkline_bucket(timestamp: SystemTime) -> u64 {
    timestamp
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        / SESSION_TOKEN_BUCKET_SECS
}

fn token_sparkline(samples: &[(SystemTime, u64)], width: usize, now: SystemTime) -> String {
    const LEVELS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

    if width == 0 {
        return String::new();
    }

    let mut buckets = HashMap::<u64, u64>::new();
    for (timestamp, tokens) in samples {
        let bucket = sparkline_bucket(*timestamp);
        let total = buckets.entry(bucket).or_default();
        *total = total.saturating_add(*tokens);
    }

    let current_bucket = sparkline_bucket(now);
    let first_bucket = current_bucket.saturating_sub(width.saturating_sub(1) as u64);
    (first_bucket..=current_bucket)
        .map(|bucket| {
            let value = buckets.get(&bucket).copied().unwrap_or(0);
            if value == 0 {
                return ' ';
            }
            let scaled = value.min(SESSION_SPARKLINE_MAX_TOKENS);
            let level = (u128::from(scaled) * LEVELS.len() as u128)
                .div_ceil(u128::from(SESSION_SPARKLINE_MAX_TOKENS))
                .saturating_sub(1) as usize;
            LEVELS[level]
        })
        .collect()
}

fn token_sparkline_line(
    samples: &[(SystemTime, u64)],
    width: usize,
    now: SystemTime,
) -> Line<'static> {
    let mut sparkline = token_sparkline(samples, width, now);
    let current = sparkline
        .pop()
        .map_or_else(String::new, |value| value.to_string());
    Line::from(vec![
        Span::styled(sparkline, Style::default().fg(BLUE)),
        Span::styled(current, Style::default().fg(DIM)),
    ])
}

fn column_constraints<K>(columns: &[ColumnSpec<K>]) -> Vec<Constraint> {
    columns.iter().map(ColumnSpec::constraint).collect()
}

fn column_header<K>(columns: &[ColumnSpec<K>]) -> Row<'static> {
    table_header_aligned(
        columns
            .iter()
            .map(|column| (column.header, column.alignment)),
    )
}

fn target_cell_with_client_type(
    provider: Option<&str>,
    model: Option<&str>,
    width: usize,
    client_type_override: Option<&str>,
) -> Cell<'static> {
    let provider = provider.unwrap_or("-");
    let model = model.unwrap_or("-");
    let target = if provider == "cursor" && model != "-" {
        let client_type = client_type_override
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| config::cursor_client_type_for_model(model));
        format!("{provider}/{model} [{client_type}]")
    } else {
        format!("{provider}/{model}")
    };
    text_cell(ellipsize(&target, width))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionColumn {
    Marker,
    Id,
    Project,
    Active,
    Requests,
    Failures,
    Counts,
    Provider,
    Model,
    Target,
    Effort,
    Input,
    Output,
    Rate,
    Activity,
    Status,
}

fn session_columns(tier: LayoutTier, show_full_sparkline: bool) -> Vec<ColumnSpec<SessionColumn>> {
    use SessionColumn as C;
    match (tier, show_full_sparkline) {
        (LayoutTier::Wide, true) => vec![
            ColumnSpec::fixed(C::Marker, "", Alignment::Left, 1),
            ColumnSpec::fixed(C::Id, "ID", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_WIDE_WIDTH),
            ColumnSpec::fixed(C::Active, "A", Alignment::Right, COUNT_WIDTH),
            ColumnSpec::fixed(C::Requests, "R", Alignment::Right, COUNT_WIDTH),
            ColumnSpec::fixed(C::Failures, "F", Alignment::Right, COUNT_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::fixed(C::Model, "Model", Alignment::Left, MODEL_WIDE_WIDTH),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::fixed(C::Input, "In", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Output, "Out", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::flex(C::Activity, "Tokens/10s · 4k", Alignment::Left, 1),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
        ],
        (LayoutTier::Expanded | LayoutTier::Wide, _) => vec![
            ColumnSpec::fixed(C::Marker, "", Alignment::Left, 1),
            ColumnSpec::fixed(C::Id, "ID", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_MEDIUM_WIDTH),
            ColumnSpec::fixed(C::Counts, "A/R/F", Alignment::Right, 7),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::fixed(C::Model, "Model", Alignment::Left, MODEL_NARROW_WIDTH),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::fixed(C::Input, "In", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Output, "Out", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::flex(C::Activity, "Tokens/10s", Alignment::Left, 1),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
        ],
        (LayoutTier::Medium, _) => vec![
            ColumnSpec::fixed(C::Marker, "", Alignment::Left, 1),
            ColumnSpec::fixed(C::Id, "ID", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_MEDIUM_WIDTH),
            ColumnSpec::fixed(C::Counts, "A/R/F", Alignment::Right, 7),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::flex(C::Model, "Model", Alignment::Left, 1),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Activity, "Tok/10s", Alignment::Left, 8),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
        ],
        (LayoutTier::Narrow, _) => vec![
            ColumnSpec::fixed(C::Marker, "", Alignment::Left, 1),
            ColumnSpec::fixed(C::Id, "ID", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, 10),
            ColumnSpec::fixed(C::Counts, "A/R/F", Alignment::Right, 7),
            ColumnSpec::flex(C::Target, "Target", Alignment::Left, 1),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Activity, "Trend", Alignment::Left, 6),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
        ],
        (LayoutTier::Emergency, _) => vec![
            ColumnSpec::fixed(C::Marker, "", Alignment::Left, 1),
            ColumnSpec::fixed(C::Id, "ID", Alignment::Left, ID_WIDTH),
            ColumnSpec::flex(C::Target, "Target", Alignment::Left, 1),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
        ],
    }
}

fn render_sessions(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    sessions: &[SessionSummary],
    selected: usize,
    focused: bool,
) {
    if sessions.is_empty() {
        render_empty_table_state(frame, area, "Sessions", focused, "No sessions");
        return;
    }

    let tier = LayoutTier::for_outer_width(area.width);
    let show_full_sparkline = tier == LayoutTier::Wide && area.width >= SESSION_SPARKLINE_MIN_WIDTH;
    let columns = session_columns(tier, show_full_sparkline);
    let widths = column_constraints(&columns);
    let now = SystemTime::now();
    let rows = sessions.iter().enumerate().map(|(index, session)| {
        let cells = columns
            .iter()
            .enumerate()
            .map(|(column_index, column)| {
                let width = table_column_width(area, &widths, column_index);
                match column.key {
                    SessionColumn::Marker => {
                        let marker = if focused && index == selected {
                            ">"
                        } else {
                            " "
                        };
                        Cell::from(Span::styled(marker, Style::default().fg(TEAL)))
                    }
                    SessionColumn::Id => {
                        text_cell(display_session_id(session.session_id.as_deref()))
                    }
                    SessionColumn::Project => {
                        text_cell(ellipsize(session.project.as_deref().unwrap_or("-"), width))
                    }
                    SessionColumn::Active => number_cell(session.active_count.to_string()),
                    SessionColumn::Requests => number_cell(session.request_count.to_string()),
                    SessionColumn::Failures => number_cell(session.failure_count.to_string()),
                    SessionColumn::Counts => number_cell(format!(
                        "{}/{}/{}",
                        session.active_count, session.request_count, session.failure_count
                    )),
                    SessionColumn::Provider => provider_cell(session.provider.as_deref()),
                    SessionColumn::Model => model_cell_with_client_type(
                        session.provider.as_deref(),
                        session.model.as_deref(),
                        width,
                        session.client_type.as_deref(),
                    ),
                    SessionColumn::Target => target_cell_with_client_type(
                        session.provider.as_deref(),
                        session.model.as_deref(),
                        width,
                        session.client_type.as_deref(),
                    ),
                    SessionColumn::Effort => text_cell(session.effort.as_deref().unwrap_or("-")),
                    SessionColumn::Input => number_cell(compact_tokens(session.input_tokens)),
                    SessionColumn::Output => number_cell(compact_tokens(session.output_tokens)),
                    SessionColumn::Rate => rate_cell(session.rate().label()),
                    SessionColumn::Activity => Cell::from(token_sparkline_line(
                        &session.output_token_samples,
                        width,
                        now,
                    )),
                    SessionColumn::Status => status_cell(&session.last_status),
                }
            })
            .collect::<Vec<_>>();
        Row::new(cells).style(if index == selected {
            Style::default().bg(SELECTED_BG)
        } else {
            Style::default().bg(PANEL_BG)
        })
    });
    let table = Table::new(rows, widths.clone())
        .header(column_header(&columns))
        .block(panel("Sessions", focused));
    let mut table_state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut table_state);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ActiveColumn {
    Started,
    Status,
    Project,
    Session,
    Provider,
    Model,
    Target,
    Effort,
    Endpoint,
    Input,
    Output,
    Rate,
    Elapsed,
}

fn active_columns(tier: LayoutTier) -> Vec<ColumnSpec<ActiveColumn>> {
    use ActiveColumn as C;
    match tier {
        LayoutTier::Wide => vec![
            ColumnSpec::fixed(C::Started, "Started", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_WIDE_WIDTH),
            ColumnSpec::fixed(C::Session, "Session", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::fixed(C::Model, "Model", Alignment::Left, MODEL_WIDE_WIDTH),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::flex(C::Endpoint, "Endpoint", Alignment::Left, 1),
            ColumnSpec::fixed(C::Input, "In", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Output, "Out", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Elapsed, "Elapsed", Alignment::Right, DURATION_WIDTH),
        ],
        LayoutTier::Expanded => vec![
            ColumnSpec::fixed(C::Started, "Started", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_MEDIUM_WIDTH),
            ColumnSpec::fixed(C::Session, "Session", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::flex(C::Model, "Model", Alignment::Left, 1),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::fixed(C::Endpoint, "Endpoint", Alignment::Left, ENDPOINT_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Elapsed, "Elapsed", Alignment::Right, DURATION_WIDTH),
        ],
        LayoutTier::Medium => vec![
            ColumnSpec::fixed(C::Started, "Started", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::flex(C::Model, "Model", Alignment::Left, 1),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::fixed(C::Endpoint, "Endpoint", Alignment::Left, ENDPOINT_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Elapsed, "Elapsed", Alignment::Right, DURATION_WIDTH),
        ],
        LayoutTier::Narrow => vec![
            ColumnSpec::fixed(C::Started, "Started", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::flex(C::Model, "Model", Alignment::Left, 1),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Elapsed, "Elapsed", Alignment::Right, DURATION_WIDTH),
        ],
        LayoutTier::Emergency => vec![
            ColumnSpec::fixed(C::Started, "Started", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Status, "Status", Alignment::Left, STATUS_WIDTH),
            ColumnSpec::flex(C::Target, "Target", Alignment::Left, 1),
            ColumnSpec::fixed(C::Elapsed, "Elapsed", Alignment::Right, DURATION_WIDTH),
        ],
    }
}

fn render_active(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    active: &[ActiveRequest],
    tick: usize,
) {
    if active.is_empty() {
        render_empty_table_state(frame, area, "Active requests", false, "No active requests");
        return;
    }

    let columns = active_columns(LayoutTier::for_outer_width(area.width));
    let widths = column_constraints(&columns);
    let rows = active.iter().map(|request| {
        let status = format!("{} {}", spinner(tick), request.status.label());
        let cells = columns
            .iter()
            .enumerate()
            .map(|(column_index, column)| {
                let width = table_column_width(area, &widths, column_index);
                match column.key {
                    ActiveColumn::Started => muted_cell(format_system_time(request.started_at)),
                    ActiveColumn::Status => Cell::from(Span::styled(
                        status.clone(),
                        status_style(request.status.label()),
                    )),
                    ActiveColumn::Project => {
                        text_cell(ellipsize(request.project.as_deref().unwrap_or("-"), width))
                    }
                    ActiveColumn::Session => {
                        text_cell(display_session_id(request.session_id.as_deref()))
                    }
                    ActiveColumn::Provider => provider_cell(request.provider.as_deref()),
                    ActiveColumn::Model => model_cell_with_client_type(
                        request.provider.as_deref(),
                        request.model.as_deref(),
                        width,
                        request.client_type.as_deref(),
                    ),
                    ActiveColumn::Target => target_cell_with_client_type(
                        request.provider.as_deref(),
                        request.model.as_deref(),
                        width,
                        request.client_type.as_deref(),
                    ),
                    ActiveColumn::Effort => text_cell(request.effort.as_deref().unwrap_or("-")),
                    ActiveColumn::Endpoint => muted_cell(request.endpoint.label()),
                    ActiveColumn::Input => number_cell(token_value(request.input_tokens)),
                    ActiveColumn::Output => number_cell(token_value(request.output_tokens)),
                    ActiveColumn::Rate => rate_cell(request.rate().label()),
                    ActiveColumn::Elapsed => number_cell(format_duration(request.elapsed())),
                }
            })
            .collect::<Vec<_>>();
        Row::new(cells).style(Style::default().bg(PANEL_BG))
    });
    let table = Table::new(rows, widths.clone())
        .header(column_header(&columns))
        .block(panel("Active requests", false));
    frame.render_widget(table, area);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecentColumn {
    Finished,
    Code,
    Project,
    Session,
    Provider,
    Model,
    Target,
    Effort,
    Endpoint,
    Latency,
    Rate,
    Input,
    Output,
    Details,
    Error,
}

fn recent_columns(tier: LayoutTier) -> Vec<ColumnSpec<RecentColumn>> {
    use RecentColumn as C;
    match tier {
        LayoutTier::Wide => vec![
            ColumnSpec::fixed(C::Finished, "Finished", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_WIDE_WIDTH),
            ColumnSpec::fixed(C::Session, "Session", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::fixed(C::Model, "Model", Alignment::Left, MODEL_WIDE_WIDTH),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::fixed(C::Endpoint, "Endpoint", Alignment::Left, ENDPOINT_WIDTH),
            ColumnSpec::fixed(C::Latency, "Latency", Alignment::Right, DURATION_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Input, "In", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Output, "Out", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::flex(C::Details, "Details", Alignment::Left, 1),
        ],
        LayoutTier::Expanded => vec![
            ColumnSpec::fixed(C::Finished, "Finished", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_MEDIUM_WIDTH),
            ColumnSpec::fixed(C::Session, "Session", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::flex(C::Model, "Model", Alignment::Left, 1),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::fixed(C::Latency, "Latency", Alignment::Right, DURATION_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Input, "In", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Output, "Out", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Error, "!", Alignment::Right, ERROR_WIDTH),
        ],
        LayoutTier::Medium => vec![
            ColumnSpec::fixed(C::Finished, "Finished", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::flex(C::Model, "Model", Alignment::Left, 1),
            ColumnSpec::fixed(C::Effort, "Effort", Alignment::Left, EFFORT_WIDTH),
            ColumnSpec::fixed(C::Latency, "Latency", Alignment::Right, DURATION_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Input, "In", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Output, "Out", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Error, "!", Alignment::Right, ERROR_WIDTH),
        ],
        LayoutTier::Narrow => vec![
            ColumnSpec::fixed(C::Finished, "Finished", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::flex(C::Model, "Model", Alignment::Left, 1),
            ColumnSpec::fixed(C::Latency, "Latency", Alignment::Right, DURATION_WIDTH),
            ColumnSpec::fixed(C::Rate, "Rate", Alignment::Right, RATE_WIDTH),
            ColumnSpec::fixed(C::Input, "In", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Output, "Out", Alignment::Right, TOKEN_WIDTH),
            ColumnSpec::fixed(C::Error, "!", Alignment::Right, ERROR_WIDTH),
        ],
        LayoutTier::Emergency => vec![
            ColumnSpec::fixed(C::Finished, "Finished", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::flex(C::Target, "Target", Alignment::Left, 1),
            ColumnSpec::fixed(C::Latency, "Latency", Alignment::Right, DURATION_WIDTH),
            ColumnSpec::fixed(C::Error, "!", Alignment::Right, ERROR_WIDTH),
        ],
    }
}

fn http_code_cell(status: Option<u16>) -> Cell<'static> {
    Cell::from(
        Line::from(Span::styled(
            status
                .map(|status| status.to_string())
                .unwrap_or_else(|| "-".to_string()),
            http_status_style(status),
        ))
        .alignment(Alignment::Right),
    )
}

fn render_recent(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    recent: &[CompletedRequest],
    selected: usize,
    focused: bool,
) {
    if recent.is_empty() {
        render_empty_table_state(
            frame,
            area,
            "Recent requests",
            focused,
            "No recent requests",
        );
        return;
    }

    let columns = recent_columns(LayoutTier::for_outer_width(area.width));
    let widths = column_constraints(&columns);
    let rows = recent.iter().enumerate().map(|(index, request)| {
        let cells = columns
            .iter()
            .enumerate()
            .map(|(column_index, column)| {
                let width = table_column_width(area, &widths, column_index);
                match column.key {
                    RecentColumn::Finished => muted_cell(format_system_time(request.finished_at)),
                    RecentColumn::Code => http_code_cell(request.http_status),
                    RecentColumn::Project => {
                        text_cell(ellipsize(request.project.as_deref().unwrap_or("-"), width))
                    }
                    RecentColumn::Session => {
                        text_cell(display_session_id(request.session_id.as_deref()))
                    }
                    RecentColumn::Provider => provider_cell(request.provider.as_deref()),
                    RecentColumn::Model => model_cell_with_client_type(
                        request.provider.as_deref(),
                        request.model.as_deref(),
                        width,
                        request.client_type.as_deref(),
                    ),
                    RecentColumn::Target => target_cell_with_client_type(
                        request.provider.as_deref(),
                        request.model.as_deref(),
                        width,
                        request.client_type.as_deref(),
                    ),
                    RecentColumn::Effort => text_cell(request.effort.as_deref().unwrap_or("-")),
                    RecentColumn::Endpoint => muted_cell(request.endpoint.label()),
                    RecentColumn::Latency => number_cell(format_duration(request.latency)),
                    RecentColumn::Rate => rate_cell(request.rate().label()),
                    RecentColumn::Input => number_cell(token_value(request.input_tokens)),
                    RecentColumn::Output => number_cell(token_value(request.output_tokens)),
                    RecentColumn::Details => detail_cell(request.error.as_deref().unwrap_or("")),
                    RecentColumn::Error => detail_cell(error_indicator(request)),
                }
            })
            .collect::<Vec<_>>();
        Row::new(cells).style(if focused && index == selected {
            Style::default().bg(SELECTED_BG)
        } else {
            Style::default().bg(PANEL_BG)
        })
    });
    let table = Table::new(rows, widths.clone())
        .header(column_header(&columns))
        .block(panel("Recent requests", focused));
    let mut table_state = TableState::default().with_selected(Some(selected));
    frame.render_stateful_widget(table, area, &mut table_state);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventColumn {
    Time,
    Code,
    Project,
    Session,
    Provider,
    Model,
    Message,
}

fn event_columns(tier: LayoutTier) -> Vec<ColumnSpec<EventColumn>> {
    use EventColumn as C;
    match tier {
        LayoutTier::Wide => vec![
            ColumnSpec::fixed(C::Time, "Time", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_WIDE_WIDTH),
            ColumnSpec::fixed(C::Session, "Session", Alignment::Left, ID_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::fixed(C::Model, "Model", Alignment::Left, MODEL_WIDE_WIDTH),
            ColumnSpec::flex(C::Message, "Message", Alignment::Left, 1),
        ],
        LayoutTier::Expanded => vec![
            ColumnSpec::fixed(C::Time, "Time", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::fixed(C::Project, "Project", Alignment::Left, PROJECT_MEDIUM_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::fixed(C::Model, "Model", Alignment::Left, MODEL_MEDIUM_WIDTH),
            ColumnSpec::flex(C::Message, "Message", Alignment::Left, 1),
        ],
        LayoutTier::Medium => vec![
            ColumnSpec::fixed(C::Time, "Time", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::fixed(C::Model, "Model", Alignment::Left, MODEL_MEDIUM_WIDTH),
            ColumnSpec::flex(C::Message, "Message", Alignment::Left, 1),
        ],
        LayoutTier::Narrow => vec![
            ColumnSpec::fixed(C::Time, "Time", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::fixed(C::Provider, "Provider", Alignment::Left, PROVIDER_WIDTH),
            ColumnSpec::fixed(C::Model, "Model", Alignment::Left, MODEL_NARROW_WIDTH),
            ColumnSpec::flex(C::Message, "Message", Alignment::Left, 1),
        ],
        LayoutTier::Emergency => vec![
            ColumnSpec::fixed(C::Time, "Time", Alignment::Left, TIME_WIDTH),
            ColumnSpec::fixed(C::Code, "Code", Alignment::Right, CODE_WIDTH),
            ColumnSpec::flex(C::Message, "Message", Alignment::Left, 1),
        ],
    }
}

fn render_events(frame: &mut ratatui::Frame<'_>, area: Rect, recent: &[CompletedRequest]) {
    let events = recent
        .iter()
        .filter(|request| {
            request.status == crate::monitor::RequestStatus::Failed
                || request.http_status.is_some_and(|status| status >= 400)
                || (request.status != crate::monitor::RequestStatus::Abandoned
                    && request.error.is_some())
        })
        .take(12)
        .collect::<Vec<_>>();
    if events.is_empty() {
        render_empty_table_state(frame, area, "Events", false, "No events");
        return;
    }

    let columns = event_columns(LayoutTier::for_outer_width(area.width));
    let widths = column_constraints(&columns);
    let rows = events.iter().map(|request| {
        let message = request
            .error
            .as_deref()
            .filter(|error| !error.is_empty())
            .unwrap_or("-");
        let cells = columns
            .iter()
            .enumerate()
            .map(|(column_index, column)| {
                let width = table_column_width(area, &widths, column_index);
                match column.key {
                    EventColumn::Time => muted_cell(format_system_time(request.finished_at)),
                    EventColumn::Code => http_code_cell(request.http_status),
                    EventColumn::Project => {
                        text_cell(ellipsize(request.project.as_deref().unwrap_or("-"), width))
                    }
                    EventColumn::Session => {
                        text_cell(display_session_id(request.session_id.as_deref()))
                    }
                    EventColumn::Provider => provider_cell(request.provider.as_deref()),
                    EventColumn::Model => model_cell_with_client_type(
                        request.provider.as_deref(),
                        request.model.as_deref(),
                        width,
                        request.client_type.as_deref(),
                    ),
                    EventColumn::Message => detail_cell(message),
                }
            })
            .collect::<Vec<_>>();
        Row::new(cells).style(Style::default().bg(PANEL_BG))
    });
    let table = Table::new(rows, widths.clone())
        .header(column_header(&columns))
        .block(panel("Events", false));
    frame.render_widget(table, area);
}

fn render_session_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    state: &MonitorState,
    selected: usize,
) {
    let lines = if let Some(session) = state.sessions.get(selected) {
        vec![
            detail_line("session", session.label(), WHITE),
            detail_line("project", session.project.as_deref().unwrap_or("-"), TEAL),
            detail_line("active requests", session.active_count.to_string(), YELLOW),
            detail_line(
                "total requests",
                session.request_count.to_string(),
                DIM_WHITE,
            ),
            detail_line("failures", session.failure_count.to_string(), RED),
            detail_line("provider", session.provider.as_deref().unwrap_or("-"), TEAL),
            detail_line("model", session.model.as_deref().unwrap_or("-"), DIM_WHITE),
            detail_line("effort", session.effort.as_deref().unwrap_or("-"), YELLOW),
            detail_line(
                "input tokens",
                compact_tokens(session.input_tokens),
                DIM_WHITE,
            ),
            detail_line(
                "output tokens",
                compact_tokens(session.output_tokens),
                DIM_WHITE,
            ),
            detail_line(
                "total tokens",
                format!(
                    "{}/{}",
                    compact_tokens(session.input_tokens),
                    compact_tokens(session.output_tokens)
                ),
                DIM_WHITE,
            ),
            detail_line("rate", session.rate().label(), TEAL),
            detail_line(
                "last status",
                session.last_status.as_str(),
                status_color(&session.last_status),
            ),
        ]
    } else {
        vec![Line::from(Span::styled(
            "No session selected",
            Style::default().fg(DIM),
        ))]
    };
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(PANEL_BG))
            .block(panel("Session detail", true)),
        area,
    );
}

fn render_request_detail(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    state: &MonitorState,
    selected: usize,
) {
    let lines = if let Some(request) = state.recent.get(selected) {
        let mut lines = vec![
            detail_line("request", request.request_id.clone(), WHITE),
            detail_line(
                "session",
                display_session_id(request.session_id.as_deref()),
                TEAL,
            ),
            detail_line(
                "session seq",
                request
                    .session_seq
                    .map(|seq| seq.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                DIM_WHITE,
            ),
            detail_line("endpoint", request.endpoint.label(), DIM_WHITE),
            detail_line("started", format_system_time(request.started_at), DIM_WHITE),
            detail_line(
                "finished",
                format_system_time(request.finished_at),
                DIM_WHITE,
            ),
            detail_line(
                "status",
                request.status.label(),
                status_color(request.status.label()),
            ),
            detail_line(
                "http status",
                request
                    .http_status
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                http_status_color(request.http_status),
            ),
            detail_line("provider", request.provider.as_deref().unwrap_or("-"), TEAL),
            detail_line("model", request.model.as_deref().unwrap_or("-"), DIM_WHITE),
            detail_line("effort", request.effort.as_deref().unwrap_or("-"), YELLOW),
            detail_line("latency", format_duration(request.latency), DIM_WHITE),
            detail_line("rate", request.rate().label(), TEAL),
            detail_line("input tokens", token_value(request.input_tokens), DIM_WHITE),
            detail_line(
                "output tokens",
                token_value(request.output_tokens),
                DIM_WHITE,
            ),
            detail_line(
                "stream bytes",
                request.streamed_bytes.to_string(),
                DIM_WHITE,
            ),
            detail_line(
                "stream chunks",
                request.stream_chunks.to_string(),
                DIM_WHITE,
            ),
        ];
        if let Some(error) = request.error.as_deref().filter(|error| !error.is_empty()) {
            lines.push(detail_line("detail", error, YELLOW));
        }
        if let Some(path) = &request.traffic_capture_path {
            lines.push(detail_line(
                "capture",
                path.to_string_lossy().into_owned(),
                DIM_WHITE,
            ));
        }
        lines
    } else {
        vec![Line::from(Span::styled(
            "No request selected",
            Style::default().fg(DIM),
        ))]
    };
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(PANEL_BG))
            .block(panel("Request detail", true)),
        area,
    );
}

fn account_route_display_name(
    selector: Option<&str>,
    accounts: &[crate::providers::cursor::auth::CursorAccountProfile],
) -> String {
    let Some(selector) = selector else {
        return "automatic".to_string();
    };
    let mut matches = accounts.iter().filter(|account| {
        config::account_selector_matches(
            selector,
            &account.id,
            account.label.as_deref(),
            account.email(),
        )
    });
    let Some(account) = matches.next() else {
        // Keep an invalid selector visible instead of presenting it as
        // automatic; the request path rejects the same mapping clearly.
        return selector.to_string();
    };
    if matches.next().is_some() {
        // The request path rejects a selector that resolves to multiple
        // accounts. Showing the first match here would falsely imply that the
        // route is valid and deterministic.
        return format!("{selector} (ambiguous)");
    }
    account.display_name().to_string()
}

/// Cursor exposes independent quota lanes for its two request surfaces.  The
/// dashboard's `apiPercentUsed` applies to normal CLI requests, while the Sand
/// endpoint reports its own Grok Bot meter.  Keep this decision beside the
/// model-account picker so a low Bot percentage is never presented as usable
/// capacity for a model that will actually leave through the CLI/API lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AccountQuotaLane {
    CliApi,
    SandBot,
}

impl AccountQuotaLane {
    fn for_model(model: &str) -> Self {
        if config::cursor_model_uses_sand(model) {
            Self::SandBot
        } else {
            Self::CliApi
        }
    }

    fn surface(self) -> &'static str {
        match self {
            Self::CliApi => "CLI",
            Self::SandBot => "Sand",
        }
    }

    fn meter(self) -> &'static str {
        match self {
            Self::CliApi => "CLI/API",
            Self::SandBot => "Sand/Bot",
        }
    }

    fn other_meter(self) -> &'static str {
        match self {
            Self::CliApi => "Sand/Bot",
            Self::SandBot => "CLI/API",
        }
    }

    fn value(self, snapshot: &crate::monitor::AccountUsageSnapshot) -> Option<f64> {
        match self {
            Self::CliApi => snapshot.api_percent,
            Self::SandBot => snapshot.grok_bot_percent,
        }
    }

    fn other_value(self, snapshot: &crate::monitor::AccountUsageSnapshot) -> Option<f64> {
        match self {
            Self::CliApi => snapshot.grok_bot_percent,
            Self::SandBot => snapshot.api_percent,
        }
    }
}

fn quota_meter_status(label: &str, percent: Option<f64>) -> String {
    let Some(percent) = percent.filter(|value| value.is_finite()) else {
        return format!("{label} unavailable");
    };
    let state = if percent >= 100.0 {
        "exhausted"
    } else if percent >= 90.0 {
        "near limit"
    } else {
        "available"
    };
    format!("{label} {} used ({state})", format_usage_percent(percent))
}

fn account_route_quota_hint(
    lane: AccountQuotaLane,
    state: Option<&crate::monitor::AccountUsageState>,
) -> String {
    let snapshot = state.and_then(|state| match state {
        crate::monitor::AccountUsageState::Ready(snapshot) => Some(snapshot),
        _ => None,
    });
    let primary = quota_meter_status(lane.meter(), snapshot.and_then(|s| lane.value(s)));
    let other = quota_meter_status(
        lane.other_meter(),
        snapshot.and_then(|s| lane.other_value(s)),
    );
    format!("lane {} -> {primary}; {other}", lane.surface())
}

fn account_route_selected_account_id(
    selected: usize,
    model: &str,
    policy: &config::CursorAccountRoutingPolicy,
    accounts: &[crate::providers::cursor::auth::CursorAccountProfile],
) -> Option<String> {
    let index = if selected == 0 {
        account_route_account_index(policy, model, accounts)
            .checked_sub(1)
            .or_else(|| accounts.iter().position(|account| account.active))
            .or_else(|| (!accounts.is_empty()).then_some(0))
    } else {
        selected.checked_sub(1)
    }?;
    accounts.get(index).map(|account| account.id.clone())
}

fn render_account_routes_detail(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let (models, policy, selected, account_selected, pane, input, route_message) = {
        let ui = account_route_ui_lock();
        (
            ui.models.clone(),
            ui.policy.clone(),
            ui.selected,
            ui.account_selected,
            ui.pane,
            ui.input.clone(),
            ui.message.clone(),
        )
    };
    let (accounts, usage, usage_cached, metadata, usage_message) = {
        let ui = account_ui_lock();
        (
            ui.accounts.clone(),
            ui.usage.clone(),
            ui.usage_cached.clone(),
            ui.account_metadata.clone(),
            ui.message.clone(),
        )
    };

    // Keep instructions and status outside the panes so the two lists retain
    // a stable height while usage workers update asynchronously.
    let hint_height = if area.height >= 3 { 2 } else { 1 };
    let status_height = if area.height >= 5 { 2 } else { 1 };
    let sections = Layout::vertical([
        Constraint::Length(hint_height),
        Constraint::Min(1),
        Constraint::Length(status_height),
    ])
    .split(area);
    let hint_lines = if let Some(input) = input.as_deref() {
        vec![Line::from(vec![
            Span::styled(
                " Model account routes  ",
                Style::default().fg(WHITE).add_modifier(Modifier::BOLD),
            ),
            Span::styled(" model id: ", Style::default().fg(DIM)),
            Span::styled(format!("{input}_"), Style::default().fg(WHITE)),
            Span::styled("   Enter save  Esc cancel", Style::default().fg(DIM)),
        ])]
    } else {
        vec![
            Line::from(vec![
                Span::styled(
                    " Model account routes  ",
                    Style::default().fg(WHITE).add_modifier(Modifier::BOLD),
                ),
                Span::styled(" Tab/←→", Style::default().fg(TEAL)),
                Span::styled(" pane  ", Style::default().fg(DIM)),
                Span::styled("j/k", Style::default().fg(TEAL)),
                Span::styled(" move  ", Style::default().fg(DIM)),
                Span::styled("Enter/space", Style::default().fg(TEAL)),
                Span::styled(" assign account  ", Style::default().fg(DIM)),
                Span::styled("x", Style::default().fg(TEAL)),
                Span::styled(" automatic  ", Style::default().fg(DIM)),
                Span::styled("r", Style::default().fg(TEAL)),
                Span::styled(" refresh  ", Style::default().fg(DIM)),
                Span::styled("u/U", Style::default().fg(TEAL)),
                Span::styled(" usage", Style::default().fg(DIM)),
            ]),
            Line::from(vec![
                Span::styled(" *", Style::default().fg(TEAL)),
                Span::styled(" active account  ", Style::default().fg(DIM)),
                Span::styled("[bound]", Style::default().fg(TEAL)),
                Span::styled(" model's current route  ", Style::default().fg(DIM)),
                Span::styled("…", Style::default().fg(DIM_WHITE)),
                Span::styled(" usage loading", Style::default().fg(DIM)),
            ]),
        ]
    };
    frame.render_widget(
        Paragraph::new(hint_lines)
            .style(Style::default().bg(BG))
            .wrap(Wrap { trim: false }),
        sections[0],
    );

    let body = sections[1];
    let panes = if body.width >= 100 {
        Layout::horizontal([Constraint::Percentage(42), Constraint::Percentage(58)])
            .spacing(1)
            .split(body)
    } else {
        // A stacked layout keeps identity and quota columns legible on small
        // terminals while retaining the same keyboard focus model.
        Layout::vertical([Constraint::Percentage(44), Constraint::Percentage(56)])
            .spacing(1)
            .split(body)
    };
    render_account_route_models(
        frame,
        panes[0],
        &models,
        &policy,
        &accounts,
        selected,
        pane == AccountRoutePane::Models,
    );
    let model = models.get(selected).map(String::as_str).unwrap_or("-");
    let lane = AccountQuotaLane::for_model(model);
    // When the account pane is on `automatic`, explain the active account's
    // lane as well.  This keeps the diagnostic useful before a user pins a
    // concrete account and avoids comparing the wrong meter by eye.
    let selected_account_id =
        account_route_selected_account_id(account_selected, model, &policy, &accounts);
    let selected_usage = selected_account_id
        .as_deref()
        .and_then(|account_id| usage.get(account_id));
    render_account_route_accounts(
        frame,
        panes[1],
        AccountRouteAccountsView {
            model,
            policy: &policy,
            accounts: &accounts,
            usage: &usage,
            usage_cached: &usage_cached,
            metadata: &metadata,
            selected: account_selected,
            focused: pane == AccountRoutePane::Accounts,
        },
    );

    let quota_hint = account_route_quota_hint(lane, selected_usage);
    let status = [route_message, usage_message]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    // Keep the lane diagnosis on its own row.  Combining it with a worker
    // status can push the useful part past the panel width, especially while
    // all-account usage is refreshing.  The lane text is deliberately muted:
    // a full API meter is a routing diagnosis, not a TUI failure.
    let status_lines = if status.is_empty() {
        vec![
            Line::from(Span::styled(
                " Select a model, then choose an account or automatic.",
                Style::default().fg(DIM),
            )),
            Line::from(Span::styled(
                format!(" {quota_hint}"),
                Style::default().fg(DIM_WHITE),
            )),
        ]
    } else {
        vec![
            Line::from(Span::styled(
                format!(" {}", status.join("  |  ")),
                Style::default().fg(YELLOW),
            )),
            Line::from(Span::styled(
                format!(" {quota_hint}"),
                Style::default().fg(DIM_WHITE),
            )),
        ]
    };
    frame.render_widget(
        Paragraph::new(status_lines)
            .style(Style::default().bg(BG))
            .wrap(Wrap { trim: false }),
        sections[2],
    );
}

fn render_account_route_models(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    models: &[String],
    policy: &config::CursorAccountRoutingPolicy,
    accounts: &[crate::providers::cursor::auth::CursorAccountProfile],
    selected: usize,
    focused: bool,
) {
    let block = panel("Models", focused);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let width = usize::from(inner.width);
    let mut lines = Vec::new();
    if models.is_empty() {
        lines.push(Line::from(Span::styled(
            "No Cursor models are registered",
            Style::default().fg(DIM_WHITE),
        )));
    } else {
        lines.push(Line::from(vec![
            Span::styled("Model", Style::default().fg(TEAL)),
            Span::styled("  route", Style::default().fg(DIM)),
        ]));
        let visible = usize::from(inner.height.saturating_sub(1)).max(1);
        let start = selected.saturating_sub(visible.saturating_sub(1) / 2);
        let end = (start + visible).min(models.len());
        if start > 0 {
            lines.push(Line::from(Span::styled(
                "^ more models",
                Style::default().fg(DIM),
            )));
        }
        for (index, model) in models.iter().enumerate().skip(start).take(end - start) {
            let row_selected = index == selected;
            let style = if row_selected {
                Style::default().fg(WHITE).bg(SELECTED_BG)
            } else {
                Style::default().fg(DIM_WHITE)
            };
            // Show the request surface next to the account assignment.  A
            // model may have a healthy Sand/Bot meter while its CLI/API meter
            // is exhausted (or vice versa); the lane marker makes that split
            // visible before the account is selected.
            let lane = AccountQuotaLane::for_model(model);
            let route = format!(
                "{} · {}",
                account_route_display_name(policy.account_for_model(model), accounts),
                lane.surface()
            );
            let marker = if row_selected { "> " } else { "  " };
            let model_width = width.saturating_sub(3 + 18).max(1);
            let route_width = width.saturating_sub(3 + model_width).max(1);
            lines.push(Line::from(vec![
                Span::styled(marker, style),
                Span::styled(ellipsize(model, model_width), style),
                Span::styled("  ", style),
                Span::styled(ellipsize(&route, route_width), style),
            ]));
        }
        if end < models.len() {
            lines.push(Line::from(Span::styled(
                "v more models",
                Style::default().fg(DIM),
            )));
        }
    }
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(PANEL_BG))
            .wrap(Wrap { trim: false }),
        inner,
    );
}

struct AccountRouteAccountsView<'a> {
    model: &'a str,
    policy: &'a config::CursorAccountRoutingPolicy,
    accounts: &'a [crate::providers::cursor::auth::CursorAccountProfile],
    usage: &'a HashMap<String, crate::monitor::AccountUsageState>,
    usage_cached: &'a HashSet<String>,
    metadata: &'a HashMap<String, crate::providers::cursor::usage::AccountUsageCacheMetadata>,
    selected: usize,
    focused: bool,
}

fn render_account_route_accounts(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    view: AccountRouteAccountsView<'_>,
) {
    let AccountRouteAccountsView {
        model,
        policy,
        accounts,
        usage,
        usage_cached,
        metadata,
        selected,
        focused,
    } = view;
    let block = panel("Accounts", focused);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let width = usize::from(inner.width);
    let current_index = account_route_account_index(policy, model, accounts);
    let mut groups: Vec<Vec<Line<'static>>> = Vec::with_capacity(accounts.len() + 1);
    let automatic_style = if selected == 0 && focused {
        Style::default().fg(WHITE).bg(SELECTED_BG)
    } else {
        Style::default().fg(DIM_WHITE)
    };
    let automatic_marker = if selected == 0 { "> " } else { "  " };
    let automatic_current = if current_index == 0 && policy.account_for_model(model).is_none() {
        "  [current]"
    } else {
        ""
    };
    let lane = AccountQuotaLane::for_model(model);
    let automatic_detail = if accounts.is_empty() {
        "  no saved accounts — add one with cursor auth add".to_string()
    } else {
        format!(
            "  active account + normal failover · {} quota",
            lane.meter()
        )
    };
    groups.push(vec![
        Line::from(vec![
            Span::styled(automatic_marker, automatic_style),
            Span::styled("automatic", automatic_style),
            Span::styled(automatic_current, Style::default().fg(TEAL)),
        ]),
        Line::from(Span::styled(automatic_detail, automatic_style)),
    ]);

    let bound_id = (current_index > 0)
        .then(|| {
            accounts
                .get(current_index - 1)
                .map(|account| account.id.as_str())
        })
        .flatten();
    for (index, account) in accounts.iter().enumerate() {
        let option_index = index + 1;
        let row_selected = option_index == selected;
        let style = if row_selected && focused {
            Style::default().fg(WHITE).bg(SELECTED_BG)
        } else {
            Style::default().fg(DIM_WHITE)
        };
        let usage_state = usage.get(&account.id);
        let snapshot = usage_state.and_then(|state| match state {
            crate::monitor::AccountUsageState::Ready(snapshot) => Some(snapshot),
            _ => None,
        });
        let email = snapshot
            .and_then(|snapshot| snapshot.email.as_deref())
            .filter(|value| !value.trim().is_empty())
            .or_else(|| account.email().filter(|value| !value.trim().is_empty()))
            .or_else(|| {
                metadata
                    .get(&account.id)
                    .and_then(|entry| entry.email.as_deref())
                    .filter(|value| !value.trim().is_empty())
            });
        let name = account_name_for_display_with_label(
            account,
            email,
            metadata
                .get(&account.id)
                .and_then(|entry| entry.label.as_deref()),
        );
        let identity = email
            .map(|email| {
                format!(
                    "{}{}  {} <{}>",
                    if account.active { "* " } else { "  " },
                    if bound_id == Some(account.id.as_str()) {
                        "[bound] "
                    } else {
                        ""
                    },
                    name,
                    email
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "{}{}  {} [id {}]",
                    if account.active { "* " } else { "  " },
                    if bound_id == Some(account.id.as_str()) {
                        "[bound] "
                    } else {
                        ""
                    },
                    name,
                    short_account_id(&account.id),
                )
            });
        let metrics = account_usage_metrics(usage_state, true);
        let plan = snapshot
            .and_then(|snapshot| snapshot.membership.as_deref())
            .filter(|value| !value.trim().is_empty())
            .map(|value| value.replace('_', "-"))
            .unwrap_or_else(|| "-".to_string());
        let plan_spend = snapshot
            .and_then(|snapshot| snapshot.plan_used_usd.zip(snapshot.plan_limit_usd))
            .map(|(used, limit)| format!(" ${used:.0}/${limit:.0}"))
            .unwrap_or_default();
        let bot = if metrics[3] == "-" {
            "-".to_string()
        } else {
            format!("{}/wk", metrics[3])
        };
        let updated = snapshot
            .map(|snapshot| {
                let time = format_system_time_short(snapshot.fetched_at);
                if usage_cached.contains(&account.id) {
                    format!("cached {time}")
                } else {
                    time
                }
            })
            .unwrap_or_else(|| "-".to_string());
        let quota = format!(
            "  updated {updated}  {plan}{plan_spend}  total {}  auto {}  api {}  bot {bot}",
            metrics[0], metrics[1], metrics[2]
        );
        // Keep both freshness and the quota meters visible in the picker on
        // medium-width terminals. Seconds remain available in the account
        // table/detail; the picker falls back to minute precision only when
        // the full row would otherwise ellipsize its last meter.
        let quota = if quota.chars().count() > width {
            let compact_updated = snapshot
                .map(|snapshot| {
                    let time = format_system_time_short(snapshot.fetched_at);
                    if usage_cached.contains(&account.id) {
                        format!("cached {time}")
                    } else {
                        time
                    }
                })
                .unwrap_or_else(|| "-".to_string());
            format!(
                "  updated {compact_updated}  {plan}{plan_spend}  total {}  auto {}  api {}  bot {bot}",
                metrics[0], metrics[1], metrics[2]
            )
        } else {
            quota
        };
        let identity = ellipsize(&identity, width.max(1));
        let quota = ellipsize(&quota, width.max(1));
        groups.push(vec![
            Line::from(Span::styled(
                format!("{}{}", if row_selected { "> " } else { "  " }, identity),
                style,
            )),
            Line::from(Span::styled(quota, style)),
        ]);
    }

    let option_capacity = (usize::from(inner.height) / 2).max(1);
    let selected = selected.min(groups.len().saturating_sub(1));
    let mut start = selected.saturating_sub(option_capacity.saturating_sub(1) / 2);
    start = start.min(groups.len().saturating_sub(option_capacity));
    let end = (start + option_capacity).min(groups.len());
    let mut lines = Vec::new();
    for group in groups.iter().skip(start).take(end - start) {
        lines.extend(group.iter().cloned());
    }
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(
            "No Cursor accounts saved",
            Style::default().fg(DIM_WHITE),
        )));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(PANEL_BG))
            .wrap(Wrap { trim: false }),
        inner,
    );
}

fn short_account_id(id: &str) -> String {
    let chars = id.chars().collect::<Vec<_>>();
    if chars.len() <= 14 {
        return id.to_string();
    }
    chars[..6]
        .iter()
        .chain(['…'].iter())
        .chain(chars[chars.len() - 5..].iter())
        .collect()
}

fn render_accounts_detail(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let ui = account_ui_lock();
    let columns = account_list_columns(area.width);
    let mut lines = vec![Line::from(vec![
        Span::raw("  "),
        Span::styled("Enter", Style::default().fg(TEAL)),
        Span::styled(" switch  ", Style::default().fg(DIM)),
        Span::styled("u", Style::default().fg(TEAL)),
        Span::styled(" selected usage  ", Style::default().fg(DIM)),
        Span::styled("U", Style::default().fg(TEAL)),
        Span::styled(" all usage  ", Style::default().fg(DIM)),
        Span::styled("d", Style::default().fg(TEAL)),
        Span::styled(" delete  ", Style::default().fg(DIM)),
        Span::styled("r", Style::default().fg(TEAL)),
        Span::styled(" refresh  ", Style::default().fg(DIM)),
        Span::styled("m", Style::default().fg(TEAL)),
        Span::styled(" model accounts  ", Style::default().fg(DIM)),
        Span::styled("Esc/a", Style::default().fg(TEAL)),
        Span::styled(" close", Style::default().fg(DIM)),
    ])];
    lines.push(Line::from(""));
    if ui.accounts.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No Cursor accounts saved. Run `cursor auth login` or `cursor auth add`.",
            Style::default().fg(DIM_WHITE),
        )));
    } else {
        // Keep the selected account in view when a user has more accounts
        // than fit in the panel.  The selected row gets a second line for its
        // id, so leave room for that line and the optional range indicators.
        let visible_rows = usize::from(area.height.saturating_sub(9)).max(1);
        let start = ui
            .selected
            .saturating_sub(visible_rows.saturating_sub(1) / 2);
        let end = (start + visible_rows).min(ui.accounts.len());
        if start > 0 {
            lines.push(Line::from(Span::styled(
                "  ^ more accounts",
                Style::default().fg(DIM),
            )));
        }
        lines.push(account_list_header(&columns));
        for (index, account) in ui
            .accounts
            .iter()
            .enumerate()
            .skip(start)
            .take(end.saturating_sub(start))
        {
            let selected = index == ui.selected;
            let selected_style = if selected {
                Style::default().fg(WHITE).bg(SELECTED_BG)
            } else {
                Style::default().fg(DIM_WHITE)
            };
            let marker = if account.active { "*" } else { " " };
            let usage_state = ui.usage.get(&account.id);
            let usage_snapshot = usage_state.and_then(|state| match state {
                crate::monitor::AccountUsageState::Ready(snapshot) => Some(snapshot),
                _ => None,
            });
            // An opaque Cursor token may not contain an email. Once the
            // dashboard response arrives, its `/auth/me` identity is the
            // most useful account identity available to the operator.
            let email = usage_snapshot
                .and_then(|snapshot| snapshot.email.as_deref())
                .filter(|value| !value.trim().is_empty())
                .or_else(|| account.email().filter(|value| !value.trim().is_empty()))
                .or_else(|| {
                    ui.account_metadata
                        .get(&account.id)
                        .and_then(|entry| entry.email.as_deref())
                        .filter(|value| !value.trim().is_empty())
                });
            let name = account_name_for_display_with_label(
                account,
                email,
                ui.account_metadata
                    .get(&account.id)
                    .and_then(|entry| entry.label.as_deref()),
            );
            let metrics = account_usage_metrics(usage_state, columns.compact);
            let mut row = vec![Span::styled(
                format!(" {marker} {} ", if selected { ">" } else { " " }),
                selected_style,
            )];
            push_account_cell(&mut row, &name, columns.name_width, selected_style);
            if let Some(width) = columns.email_width {
                push_account_cell(&mut row, email.unwrap_or("-"), width, selected_style);
            }
            if let Some(width) = columns.updated_width {
                let updated = usage_snapshot
                    .map(|snapshot| {
                        // Include the calendar date so a cached value from a
                        // previous billing day is not mistaken for a fresh
                        // same-clock reading after restart.
                        format_system_time_short_with_date(snapshot.fetched_at)
                    })
                    .or_else(|| {
                        ui.usage_loading
                            .contains(&account.id)
                            .then(|| "...".to_string())
                    })
                    .unwrap_or_else(|| "-".to_string());
                push_account_cell(&mut row, &updated, width, selected_style);
            }
            for (metric, width) in metrics.iter().zip(columns.metric_widths) {
                push_account_cell(&mut row, metric, width, selected_style);
            }
            lines.push(Line::from(row));
            // Keep ids available for accounts without an email, and make it
            // possible to distinguish two accounts sharing a display label.
            if selected && !account.id.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled("     id ", Style::default().fg(DIM)),
                    Span::styled(account.id.clone(), Style::default().fg(DIM)),
                ]));
                if let Some(snapshot) = usage_snapshot {
                    lines.push(Line::from(vec![
                        Span::styled(
                            if ui.usage_cached.contains(&account.id) {
                                "     usage cached at "
                            } else {
                                "     usage updated "
                            },
                            Style::default().fg(DIM),
                        ),
                        Span::styled(
                            format_system_time_with_date(snapshot.fetched_at),
                            Style::default().fg(DIM),
                        ),
                    ]));
                    if ui.usage_loading.contains(&account.id) {
                        lines.push(Line::from(Span::styled(
                            "     usage refreshing...",
                            Style::default().fg(DIM),
                        )));
                    }
                } else if ui.usage_loading.contains(&account.id) {
                    lines.push(Line::from(Span::styled(
                        "     usage refreshing...",
                        Style::default().fg(DIM),
                    )));
                }
                if let Some(error) = ui.usage_errors.get(&account.id) {
                    lines.push(Line::from(Span::styled(
                        format!("     last refresh error {}", ellipsize(error, 96)),
                        Style::default().fg(DIM),
                    )));
                }
            }
        }
        if end < ui.accounts.len() {
            lines.push(Line::from(Span::styled(
                "  v more accounts",
                Style::default().fg(DIM),
            )));
        }
    }
    if let Some(message) = ui.message.as_deref() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            format!("  {message}"),
            Style::default().fg(YELLOW),
        )));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(PANEL_BG))
            .block(panel("Cursor accounts", true))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_account_delete_confirmation(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let target = {
        let ui = account_ui_lock();
        ui.delete_confirm.clone()
    };
    let Some(target) = target else {
        return;
    };

    // Keep the overlay fully inside very small terminals as well as the
    // normal monitor layout. `min` is intentional here: `clamp(1, 72)` would
    // expand a three-row terminal to a seven-row popup.
    let width = 72.min(area.width);
    let height = 7.min(area.height);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    let name_width = usize::from(width.saturating_sub(8)).max(1);
    let name = ellipsize(&target.display_name, name_width);
    let prompt = if target.active {
        format!("Delete active account {name}?")
    } else {
        format!("Delete account {name}?")
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Delete Cursor account",
                Style::default().fg(WHITE).add_modifier(Modifier::BOLD),
            )),
            Line::from(Span::styled(prompt, Style::default().fg(DIM_WHITE))),
            Line::from(Span::styled(
                format!("id {}", ellipsize(&target.id, name_width)),
                Style::default().fg(DIM),
            )),
            Line::from(vec![
                Span::styled("y/Enter", Style::default().fg(TEAL)),
                Span::styled(" confirm   ", Style::default().fg(DIM_WHITE)),
                Span::styled("n/Esc", Style::default().fg(TEAL)),
                Span::styled(" cancel", Style::default().fg(DIM_WHITE)),
            ]),
        ])
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(YELLOW))
                .style(Style::default().bg(BG)),
        )
        .style(Style::default().bg(BG)),
        popup,
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AccountListColumns {
    name_width: usize,
    email_width: Option<usize>,
    updated_width: Option<usize>,
    metric_widths: [usize; 4],
    compact: bool,
}

/// Keep account rows readable at every monitor width. The account name and
/// all four quota meters remain visible; the duplicate email column is the
/// first field dropped on narrow terminals.
fn account_list_columns(area_width: u16) -> AccountListColumns {
    let inner_width = usize::from(area_width.saturating_sub(2));
    let compact = inner_width < 105;
    let metric_widths = if compact {
        [8, 8, 8, 12]
    } else {
        [11, 10, 9, 13]
    };
    // Five marker characters plus one separator after every cell.
    let fixed_metrics = 5 + metric_widths.iter().map(|width| width + 1).sum::<usize>();
    let email_width = if inner_width >= 96 {
        Some(if inner_width < 132 { 24 } else { 32 })
    } else {
        None
    };
    // A short local-time column makes cached snapshots auditable without
    // crowding narrow terminals. The selected-row detail below still shows
    // the full status when this column is omitted.
    let updated_width = (!compact && inner_width >= 115).then_some(12);
    let fixed_email = email_width.map_or(0, |width| width + 1);
    let fixed_updated = updated_width.map_or(0, |width| width + 1);
    let name_width = inner_width
        .saturating_sub(fixed_metrics + fixed_email + fixed_updated)
        .max(1)
        .min(if email_width.is_some() { 24 } else { 32 });
    AccountListColumns {
        name_width,
        email_width,
        updated_width,
        metric_widths,
        compact,
    }
}

fn account_list_header(columns: &AccountListColumns) -> Line<'static> {
    let mut spans = vec![Span::styled("     ", Style::default().fg(DIM))];
    spans.push(Span::styled(
        format!("{:<width$} ", "Name", width = columns.name_width),
        Style::default().fg(TEAL),
    ));
    if let Some(width) = columns.email_width {
        spans.push(Span::styled(
            format!("{:<width$} ", "Email", width = width),
            Style::default().fg(TEAL),
        ));
    }
    if let Some(width) = columns.updated_width {
        spans.push(Span::styled(
            format!("{:<width$} ", "Updated", width = width),
            Style::default().fg(TEAL),
        ));
    }
    let labels = if columns.compact {
        ["Total", "Auto", "API", "Bot"]
    } else {
        ["Total", "Auto", "API", "Bot/wk"]
    };
    for (label, width) in labels.into_iter().zip(columns.metric_widths) {
        spans.push(Span::styled(
            format!("{label:>width$} ", width = width),
            Style::default().fg(TEAL),
        ));
    }
    Line::from(spans)
}

fn push_account_cell<'a>(row: &mut Vec<Span<'a>>, value: &str, width: usize, style: Style) {
    let text = ellipsize(value, width);
    row.push(Span::styled(
        format!("{text:<width$} ", width = width),
        style,
    ));
}

fn account_name_for_display(
    account: &crate::providers::cursor::auth::CursorAccountProfile,
    email: Option<&str>,
) -> String {
    account_name_for_display_with_label(account, email, None)
}

fn account_name_for_display_with_label(
    account: &crate::providers::cursor::auth::CursorAccountProfile,
    email: Option<&str>,
    cached_label: Option<&str>,
) -> String {
    let label = account
        .label
        .as_deref()
        .or(cached_label)
        .map(str::trim)
        .filter(|label| !label.is_empty() && *label != account.id);
    if let Some(label) = label {
        // Login historically defaulted the label to the email. Showing the
        // local part in Name keeps that case useful without hiding the full
        // address in Email.
        if email.is_none_or(|address| !label.eq_ignore_ascii_case(address)) {
            return label.to_string();
        }
    }
    if let Some(email) = email {
        let local = email.split_once('@').map_or(email, |(local, _)| local);
        if !local.trim().is_empty() {
            return local.to_string();
        }
    }
    account.display_name().to_string()
}

fn account_usage_metrics(
    state: Option<&crate::monitor::AccountUsageState>,
    compact: bool,
) -> [String; 4] {
    match state {
        Some(crate::monitor::AccountUsageState::Ready(snapshot)) => [
            snapshot
                .total_percent
                .map(format_usage_percent)
                .unwrap_or_else(|| "-".into()),
            snapshot
                .auto_percent
                .map(format_usage_percent)
                .unwrap_or_else(|| "-".into()),
            snapshot
                .api_percent
                .map(format_usage_percent)
                .unwrap_or_else(|| "-".into()),
            snapshot.grok_bot_percent.map_or_else(
                || "-".into(),
                |value| {
                    let value = format_usage_percent(value);
                    if compact {
                        value
                    } else {
                        format!("{value}/wk")
                    }
                },
            ),
        ],
        Some(crate::monitor::AccountUsageState::Unknown) => {
            ["…".into(), "…".into(), "…".into(), "…".into()]
        }
        Some(crate::monitor::AccountUsageState::Failed(_)) => {
            ["err".into(), "err".into(), "err".into(), "err".into()]
        }
        Some(crate::monitor::AccountUsageState::MissingAuth) | None => {
            ["-".into(), "-".into(), "-".into(), "-".into()]
        }
    }
}

fn render_usage_detail(frame: &mut ratatui::Frame<'_>, area: Rect, state: &MonitorState) {
    let lines = match &state.account_usage {
        crate::monitor::AccountUsageState::Unknown => vec![detail_line(
            "status",
            "fetching official dashboard...",
            DIM_WHITE,
        )],
        crate::monitor::AccountUsageState::MissingAuth => {
            vec![detail_line("status", "not logged in", YELLOW)]
        }
        crate::monitor::AccountUsageState::Failed(error) => {
            vec![detail_line("status", error.clone(), RED)]
        }
        crate::monitor::AccountUsageState::Ready(snapshot) => {
            let mut lines = vec![
                detail_line("account", snapshot.email.as_deref().unwrap_or("-"), WHITE),
                detail_line("plan", snapshot.membership.as_deref().unwrap_or("-"), TEAL),
            ];
            push_usage_percent(&mut lines, "total", snapshot.total_percent);
            push_usage_percent(&mut lines, "auto", snapshot.auto_percent);
            push_usage_percent(&mut lines, "api", snapshot.api_percent);
            push_usage_money(
                &mut lines,
                "plan spend",
                snapshot.plan_used_usd,
                snapshot.plan_limit_usd,
            );
            push_usage_money(
                &mut lines,
                "on-demand",
                snapshot.on_demand_used_usd,
                snapshot.on_demand_limit_usd,
            );
            push_usage_percent(&mut lines, "sand / grok bot", snapshot.grok_bot_percent);
            if let Some(start) = snapshot.grok_bot_period_start.as_deref() {
                lines.push(detail_line("bot period", start, DIM_WHITE));
            }
            if let Some(reset) = snapshot.grok_bot_reset.as_deref() {
                lines.push(detail_line("bot reset", reset, DIM_WHITE));
            }
            if let Some(cost) = snapshot.total_cost_usd {
                lines.push(detail_line(
                    "dashboard cost",
                    format!("${cost:.2}"),
                    DIM_WHITE,
                ));
            }
            if let Some(events) = snapshot.usage_event_count {
                lines.push(detail_line(
                    "dashboard events",
                    events.to_string(),
                    DIM_WHITE,
                ));
            }
            if !snapshot.usage_events.is_empty() {
                lines.push(detail_line("recent events", "", DIM));
                let available_lines = usize::from(area.height.saturating_sub(2));
                let event_limit = available_lines.saturating_sub(lines.len() + 2);
                for event in snapshot.usage_events.iter().take(event_limit) {
                    lines.push(usage_event_line(event));
                }
            }
            lines.push(detail_line(
                "updated",
                format_system_time(snapshot.fetched_at),
                DIM,
            ));
            lines
        }
    };
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(PANEL_BG))
            .block(panel("Account usage", true)),
        area,
    );
}

fn usage_event_line(event: &crate::monitor::AccountUsageEvent) -> Line<'static> {
    let time = event.timestamp.as_deref().unwrap_or("-");
    let model = event.model.as_deref().unwrap_or("-");
    let cost = event
        .charged_usd
        .map(|value| format!("${value:.2}"))
        .unwrap_or_else(|| "-".to_string());
    let kind = event.kind.as_deref().unwrap_or("-");
    let value = format!(
        "{}  {}  {}  {}",
        ellipsize(time, 20),
        ellipsize(model, 22),
        cost,
        ellipsize(kind, 18)
    );
    Line::from(vec![
        Span::raw("  "),
        Span::styled(value, Style::default().fg(DIM_WHITE)),
    ])
}

fn push_usage_percent(lines: &mut Vec<Line<'static>>, label: &'static str, value: Option<f64>) {
    if let Some(value) = value {
        lines.push(detail_line(label, format_usage_percent(value), DIM_WHITE));
    }
}

fn push_usage_money(
    lines: &mut Vec<Line<'static>>,
    label: &'static str,
    used: Option<f64>,
    limit: Option<f64>,
) {
    if let (Some(used), Some(limit)) = (used, limit) {
        lines.push(detail_line(
            label,
            format!("${used:.2} / ${limit:.2}"),
            DIM_WHITE,
        ));
    }
}

fn format_usage_percent(value: f64) -> String {
    format!("{:.1}%", value.clamp(0.0, 100.0))
}

fn detail_line<'a>(label: &'static str, value: impl Into<String>, value_color: Color) -> Line<'a> {
    Line::from(vec![
        Span::styled(format!("  {label:<16}"), Style::default().fg(DIM)),
        Span::styled(value.into(), Style::default().fg(value_color)),
    ])
}

fn render_footer(frame: &mut ratatui::Frame<'_>, area: Rect, _app: &MonitorApp) {
    let spans = vec![
        Span::raw(" "),
        Span::styled("q", Style::default().fg(TEAL)),
        Span::styled(" quit  ", Style::default().fg(DIM)),
        Span::styled("?", Style::default().fg(TEAL)),
        Span::styled(" help  ", Style::default().fg(DIM)),
        Span::styled("b", Style::default().fg(TEAL)),
        Span::styled(" setup  ", Style::default().fg(DIM)),
        Span::styled("u", Style::default().fg(TEAL)),
        Span::styled(" usage  ", Style::default().fg(DIM)),
        Span::styled("a", Style::default().fg(TEAL)),
        Span::styled(" accounts  ", Style::default().fg(DIM)),
        Span::styled("m", Style::default().fg(TEAL)),
        Span::styled(" model accounts  ", Style::default().fg(DIM)),
        Span::styled("s", Style::default().fg(TEAL)),
        Span::styled(" sand models  ", Style::default().fg(DIM)),
        Span::styled("arrows/j/k", Style::default().fg(TEAL)),
        Span::styled(" navigate  ", Style::default().fg(DIM)),
        Span::styled("Tab", Style::default().fg(TEAL)),
        Span::styled(" pane  ", Style::default().fg(DIM)),
        Span::styled("Enter", Style::default().fg(TEAL)),
        Span::styled(" open", Style::default().fg(DIM)),
    ];
    frame.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(BG)),
        area,
    );
}

fn render_shutdown_confirmation(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let width = 44.min(area.width);
    let height = 5.min(area.height);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(Span::styled(
                "Shut down proxy?",
                Style::default().fg(WHITE).add_modifier(Modifier::BOLD),
            )),
            Line::from(vec![
                Span::styled("y", Style::default().fg(TEAL)),
                Span::styled(" confirm   ", Style::default().fg(DIM_WHITE)),
                Span::styled("n/Esc/q", Style::default().fg(TEAL)),
                Span::styled(" cancel", Style::default().fg(DIM_WHITE)),
            ]),
        ])
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(YELLOW))
                .style(Style::default().bg(BG)),
        )
        .style(Style::default().bg(BG)),
        popup,
    );
}

fn render_shutdown_overlay(frame: &mut ratatui::Frame<'_>, area: Rect, tick: usize) {
    let width = 40.min(area.width);
    let height = 5.min(area.height);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(format!("{} ", spinner(tick)), Style::default().fg(TEAL)),
                Span::styled(
                    "Shutting down...",
                    Style::default().fg(WHITE).add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(Span::styled(
                "Press Ctrl-C to force quit",
                Style::default().fg(DIM_WHITE),
            )),
        ])
        .alignment(Alignment::Center)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(TEAL))
                .style(Style::default().bg(BG)),
        )
        .style(Style::default().bg(BG)),
        popup,
    );
}

fn render_help_overlay(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let width = 48.min(area.width.saturating_sub(4)).max(24);
    // Keep every shortcut visible after adding account-management actions.
    // Two rows are reserved for the border; narrow terminals still clamp to
    // the available height.
    let shortcut_rows = 12u16;
    let height = if area.height >= 10 {
        (shortcut_rows + 2).min(area.height.saturating_sub(2))
    } else {
        area.height.max(1)
    };
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .title(Span::styled(" Shortcuts ", Style::default().fg(TEAL)))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(TEAL))
        .style(Style::default().bg(BG));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    let lines = [
        ("q / Ctrl-C", "quit proxy"),
        ("?", "toggle help"),
        ("b", "toggle setup"),
        ("u", "show account usage"),
        ("a", "manage Cursor accounts"),
        ("d", "delete selected Cursor account"),
        ("m", "assign models to accounts"),
        ("s", "configure Sand models"),
        ("arrows", "navigate rows and panes"),
        ("j / k", "previous / next row"),
        ("Tab", "switch pane"),
        ("Enter", "open detail"),
        ("Esc", "close overlay / detail"),
    ];
    let content = lines
        .into_iter()
        .map(|(key, label)| {
            Line::from(vec![
                Span::raw("  "),
                Span::styled(format!("{key:<10}"), Style::default().fg(TEAL)),
                Span::styled(label, Style::default().fg(DIM_WHITE)),
            ])
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(content).style(Style::default().bg(BG)),
        inner,
    );
}

fn render_setup_overlay(frame: &mut ratatui::Frame<'_>, area: Rect, setup_text: &str) {
    let width = 84.min(area.width.saturating_sub(4)).max(36);
    let content_height = setup_text.lines().count() as u16;
    let height = (content_height + 4)
        .min(area.height.saturating_sub(2))
        .max(8);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .title(Span::styled(" Setup ", Style::default().fg(TEAL)))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(TEAL))
        .style(Style::default().bg(BG));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let mut lines = setup_text
        .lines()
        .map(|line| {
            let style = if line.starts_with("export ") {
                Style::default().fg(WHITE)
            } else {
                Style::default().fg(DIM_WHITE)
            };
            Line::from(vec![Span::raw("  "), Span::styled(line.to_string(), style)])
        })
        .collect::<Vec<_>>();
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::raw("  "),
        Span::styled("Esc", Style::default().fg(TEAL)),
        Span::styled(" close  ", Style::default().fg(DIM)),
        Span::styled("b", Style::default().fg(TEAL)),
        Span::styled(" toggle setup", Style::default().fg(DIM)),
    ]));
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(BG))
            .wrap(Wrap { trim: false }),
        inner,
    );
}

fn render_sand_settings_overlay(frame: &mut ratatui::Frame<'_>, area: Rect, app: &MonitorApp) {
    let width = 92.min(area.width.saturating_sub(4)).max(40);
    let visible_rows = area.height.saturating_sub(9).max(3) as usize;
    let height = (visible_rows as u16 + 6)
        .min(area.height.saturating_sub(2))
        .max(8);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    };
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .title(Span::styled(" Sand Models ", Style::default().fg(TEAL)))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(TEAL))
        .style(Style::default().bg(BG));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);

    let mut lines = Vec::new();
    let source = if std::env::var_os("CCP_CURSOR_SAND_MODELS").is_some() {
        "env override active"
    } else {
        "config.json"
    };
    lines.push(Line::from(vec![
        Span::raw("  "),
        Span::styled("source ", Style::default().fg(DIM)),
        Span::styled(source, Style::default().fg(YELLOW)),
        Span::styled("  space/Enter toggle  ", Style::default().fg(DIM)),
        Span::styled("a", Style::default().fg(TEAL)),
        Span::styled(" add", Style::default().fg(DIM)),
    ]));
    if let Some(input) = app.sand_input.as_deref() {
        lines.push(Line::from(vec![
            Span::styled("  model id: ", Style::default().fg(DIM)),
            Span::styled(format!("{input}_"), Style::default().fg(WHITE)),
        ]));
    }

    let start = app
        .sand_selected
        .saturating_sub(visible_rows.saturating_sub(1) / 2);
    for (index, model) in app
        .sand_models
        .iter()
        .enumerate()
        .skip(start)
        .take(visible_rows)
    {
        let selected = index == app.sand_selected;
        let enabled = app.sand_policy.matches_model(model);
        let marker = if enabled { "[sand]" } else { "[cli ]" };
        let style = if selected {
            Style::default().fg(WHITE).bg(SELECTED_BG)
        } else if enabled {
            Style::default().fg(GREEN)
        } else {
            Style::default().fg(DIM_WHITE)
        };
        lines.push(Line::from(vec![
            Span::styled(if selected { "  > " } else { "    " }, style),
            Span::styled(format!("{marker:<7}"), style),
            Span::styled(model.clone(), style),
        ]));
    }
    if app.sand_models.is_empty() {
        lines.push(Line::from(Span::styled(
            "  No Cursor models are registered",
            Style::default().fg(DIM_WHITE),
        )));
    }
    lines.push(Line::from(""));
    if let Some(message) = app.sand_message.as_deref() {
        lines.push(Line::from(Span::styled(
            format!("  {message}"),
            Style::default().fg(YELLOW),
        )));
    }
    if app.sand_input.is_some() {
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled("Enter", Style::default().fg(TEAL)),
            Span::styled(" save  ", Style::default().fg(DIM)),
            Span::styled("Esc", Style::default().fg(TEAL)),
            Span::styled(" cancel", Style::default().fg(DIM)),
        ]));
    } else {
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled("Esc/s", Style::default().fg(TEAL)),
            Span::styled(" close  ", Style::default().fg(DIM)),
            Span::styled("j/k", Style::default().fg(TEAL)),
            Span::styled(" move  ", Style::default().fg(DIM)),
            Span::styled("space", Style::default().fg(TEAL)),
            Span::styled(" toggle  ", Style::default().fg(DIM)),
            Span::styled("a", Style::default().fg(TEAL)),
            Span::styled(" add", Style::default().fg(DIM)),
        ]));
    }
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().bg(BG))
            .wrap(Wrap { trim: false }),
        inner,
    );
}

fn mock_setup_text(port: u16, registry: &Registry) -> String {
    format!(
        "Mock mode uses deterministic simulated monitor traffic.\nNo proxy server is listening.\nRun `claude-cursor-proxy serve` to start the proxy.\n\n{}",
        setup_text(port, registry)
    )
}

pub fn setup_text(port: u16, registry: &Registry) -> String {
    let grouped = registry.grouped_models();
    let model_summary = ["codex", "kimi", "cursor"]
        .into_iter()
        .filter_map(|provider| {
            grouped
                .get(provider)
                .map(|models| format!("{provider}: {} models", models.len()))
        })
        .collect::<Vec<_>>()
        .join("  ");
    let sand_patterns = config::cursor_sand_policy().patterns().join(", ");
    let sand_summary = if sand_patterns.is_empty() {
        "none".to_string()
    } else {
        sand_patterns
    };
    let account_routes = config::cursor_account_routing_policy()
        .routes()
        .iter()
        .map(|rule| format!("{}={}", rule.model, rule.account))
        .collect::<Vec<_>>();
    let account_route_summary = if account_routes.is_empty() {
        "none".to_string()
    } else {
        account_routes.join(", ")
    };
    let mut lines = vec![
        format!("Logs: {}", paths::log_file().display()),
        format!("Config: {}", paths::config_dir().display()),
        format!("Providers: {model_summary}"),
        format!("Sand models: {sand_summary}"),
        format!("Model accounts: {account_route_summary}"),
    ];
    lines.push(format!(
        "export ANTHROPIC_BASE_URL=\"http://localhost:{port}\""
    ));
    lines.push("export ANTHROPIC_AUTH_TOKEN=\"anything\"".to_string());
    lines.push("export ANTHROPIC_MODEL=\"gpt-5.6-sol\"".to_string());
    lines.push("export ANTHROPIC_SMALL_FAST_MODEL=\"gpt-5.6-luna\"".to_string());
    lines.push("export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1".to_string());
    lines.join("\n")
}

fn format_duration(duration: Duration) -> String {
    let total = duration.as_secs();
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let seconds = total % 60;
    if hours > 0 {
        format!("{hours}h{minutes:02}m")
    } else if minutes > 0 {
        format!("{minutes}m{seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn format_system_time_short(time: SystemTime) -> String {
    let Ok(timestamp) = Timestamp::try_from(time) else {
        return "-".to_string();
    };
    Zoned::new(timestamp, TimeZone::system())
        .strftime("%H:%M")
        .to_string()
}

fn format_system_time(time: SystemTime) -> String {
    format_system_time_in_zone(time, TimeZone::system())
}

/// Calendar-aware timestamp used for account usage freshness.  The existing
/// request tables intentionally stay clock-only; account meters can survive a
/// restart, so showing the date here prevents an old cached 03:47 reading
/// from looking current tomorrow.
fn format_system_time_short_with_date(time: SystemTime) -> String {
    let Ok(timestamp) = Timestamp::try_from(time) else {
        return "-".to_string();
    };
    Zoned::new(timestamp, TimeZone::system())
        .strftime("%m-%d %H:%M")
        .to_string()
}

fn format_system_time_with_date(time: SystemTime) -> String {
    let Ok(timestamp) = Timestamp::try_from(time) else {
        return "-".to_string();
    };
    Zoned::new(timestamp, TimeZone::system())
        .strftime("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn format_system_time_in_zone(time: SystemTime, time_zone: TimeZone) -> String {
    let Ok(timestamp) = Timestamp::try_from(time) else {
        return "-".to_string();
    };
    Zoned::new(timestamp, time_zone)
        .strftime("%H:%M:%S")
        .to_string()
}

#[cfg(test)]
mod tests {
    use ratatui::{backend::TestBackend, buffer::Buffer};

    use super::*;
    use crate::monitor::{EndpointKind, mock_state};

    static ACCOUNT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn draw(width: u16, height: u16, render: impl FnOnce(&mut ratatui::Frame<'_>)) -> Buffer {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(render).unwrap();
        terminal.backend().buffer().clone()
    }

    fn buffer_text(buffer: &Buffer) -> String {
        (0..buffer.area.height)
            .map(|y| {
                (0..buffer.area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn placeholder_position(buffer: &Buffer, placeholder: &str) -> Option<(u16, u16)> {
        let symbols = placeholder
            .chars()
            .map(|character| character.to_string())
            .collect::<Vec<_>>();
        (0..buffer.area.height).find_map(|y| {
            (0..buffer.area.width).find_map(|x| {
                symbols
                    .iter()
                    .enumerate()
                    .all(|(offset, symbol)| {
                        x + (offset as u16) < buffer.area.width
                            && buffer[(x + offset as u16, y)].symbol() == symbol
                    })
                    .then_some((x, y))
            })
        })
    }

    fn assert_centered(buffer: &Buffer, placeholder: &str, expected_y: u16) {
        let (x, y) = placeholder_position(buffer, placeholder).unwrap();
        let left_space = x.saturating_sub(1);
        let right_space = buffer
            .area
            .width
            .saturating_sub(x + placeholder.chars().count() as u16 + 1);
        assert_eq!(y, expected_y);
        assert!(left_space.abs_diff(right_space) <= 1);
    }

    fn headers<K>(columns: &[ColumnSpec<K>]) -> Vec<&'static str> {
        columns.iter().map(|column| column.header).collect()
    }

    fn fixed_budget<K>(columns: &[ColumnSpec<K>]) -> u16 {
        let widths = columns
            .iter()
            .map(|column| match column.width {
                layout::ColumnWidth::Fixed(width) => width,
                layout::ColumnWidth::Flex(_) => 0,
            })
            .sum::<u16>();
        widths.saturating_add(columns.len().saturating_sub(1) as u16)
    }

    fn fixed_width<K: Copy + PartialEq>(columns: &[ColumnSpec<K>], key: K) -> Option<u16> {
        columns
            .iter()
            .find(|column| column.key == key)
            .and_then(|column| match column.width {
                layout::ColumnWidth::Fixed(width) => Some(width),
                layout::ColumnWidth::Flex(_) => None,
            })
    }

    fn flex_count<K>(columns: &[ColumnSpec<K>]) -> usize {
        columns
            .iter()
            .filter(|column| matches!(column.width, layout::ColumnWidth::Flex(_)))
            .count()
    }

    fn alignment<K: Copy + PartialEq>(columns: &[ColumnSpec<K>], key: K) -> Option<Alignment> {
        columns
            .iter()
            .find(|column| column.key == key)
            .map(|column| column.alignment)
    }

    #[test]
    fn format_system_time_applies_non_utc_time_zone() {
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(20 * 60 * 60);
        let time_zone = TimeZone::fixed(jiff::tz::offset(5));

        assert_eq!(format_system_time_in_zone(timestamp, time_zone), "01:00:00");
    }

    #[test]
    fn request_tables_share_time_status_provider_model_rhythm() {
        assert_eq!(
            headers(&active_columns(LayoutTier::Medium))[..4],
            ["Started", "Status", "Provider", "Model"]
        );
        assert_eq!(
            headers(&recent_columns(LayoutTier::Medium))[..4],
            ["Finished", "Code", "Provider", "Model"]
        );
        assert_eq!(
            headers(&event_columns(LayoutTier::Medium))[..4],
            ["Time", "Code", "Provider", "Model"]
        );
    }

    #[test]
    fn wide_tables_use_shared_model_and_provider_widths() {
        let sessions = session_columns(LayoutTier::Wide, true);
        let active = active_columns(LayoutTier::Wide);
        let recent = recent_columns(LayoutTier::Wide);
        let events = event_columns(LayoutTier::Wide);

        assert_eq!(
            fixed_width(&sessions, SessionColumn::Model),
            Some(MODEL_WIDE_WIDTH)
        );
        assert_eq!(
            fixed_width(&active, ActiveColumn::Model),
            Some(MODEL_WIDE_WIDTH)
        );
        assert_eq!(
            fixed_width(&recent, RecentColumn::Model),
            Some(MODEL_WIDE_WIDTH)
        );
        assert_eq!(
            fixed_width(&events, EventColumn::Model),
            Some(MODEL_WIDE_WIDTH)
        );
        assert_eq!(
            fixed_width(&active, ActiveColumn::Provider),
            Some(PROVIDER_WIDTH)
        );
        assert_eq!(
            fixed_width(&recent, RecentColumn::Provider),
            Some(PROVIDER_WIDTH)
        );
    }

    #[test]
    fn responsive_schemas_fit_their_minimum_terminal_widths() {
        assert!(fixed_budget(&session_columns(LayoutTier::Emergency, false)) <= 75);
        assert!(fixed_budget(&session_columns(LayoutTier::Narrow, false)) <= 76);
        assert!(fixed_budget(&session_columns(LayoutTier::Medium, false)) <= 88);
        assert!(fixed_budget(&session_columns(LayoutTier::Expanded, false)) <= 118);
        assert!(fixed_budget(&session_columns(LayoutTier::Wide, true)) <= 168);

        assert!(fixed_budget(&active_columns(LayoutTier::Emergency)) <= 75);
        assert!(fixed_budget(&active_columns(LayoutTier::Narrow)) <= 76);
        assert!(fixed_budget(&active_columns(LayoutTier::Medium)) <= 88);
        assert!(fixed_budget(&active_columns(LayoutTier::Expanded)) <= 118);
        assert!(fixed_budget(&active_columns(LayoutTier::Wide)) <= 152);

        assert!(fixed_budget(&recent_columns(LayoutTier::Emergency)) <= 75);
        assert!(fixed_budget(&recent_columns(LayoutTier::Narrow)) <= 76);
        assert!(fixed_budget(&recent_columns(LayoutTier::Medium)) <= 88);
        assert!(fixed_budget(&recent_columns(LayoutTier::Expanded)) <= 118);
        assert!(fixed_budget(&recent_columns(LayoutTier::Wide)) <= 152);

        assert!(fixed_budget(&event_columns(LayoutTier::Emergency)) <= 75);
        assert!(fixed_budget(&event_columns(LayoutTier::Narrow)) <= 76);
        assert!(fixed_budget(&event_columns(LayoutTier::Medium)) <= 88);
        assert!(fixed_budget(&event_columns(LayoutTier::Expanded)) <= 118);
        assert!(fixed_budget(&event_columns(LayoutTier::Wide)) <= 152);
    }

    #[test]
    fn active_table_renders_expected_headers_at_tier_boundaries() {
        let state = mock_state();
        let render_at = |width| {
            let buffer = draw(width, 8, |frame| {
                render_active(frame, frame.area(), &state.active, 0)
            });
            buffer_text(&buffer)
        };

        let emergency = render_at(77);
        assert!(emergency.contains("Started"), "{emergency}");
        assert!(emergency.contains("Target"), "{emergency}");
        assert!(!emergency.contains("Rate"), "{emergency}");

        let narrow = render_at(78);
        assert!(narrow.contains("Provider"), "{narrow}");
        assert!(narrow.contains("Model"), "{narrow}");
        assert!(narrow.contains("Effort"), "{narrow}");
        assert!(!narrow.contains("Project"), "{narrow}");

        let medium = render_at(90);
        assert!(medium.contains("Provider"), "{medium}");
        assert!(medium.contains("Model"), "{medium}");
        assert!(medium.contains("Endpoint"), "{medium}");
        assert!(!medium.contains("Project"), "{medium}");

        let expanded = render_at(120);
        assert!(expanded.contains("Project"), "{expanded}");
        assert!(expanded.contains("Session"), "{expanded}");
        assert!(expanded.contains("Endpoint"), "{expanded}");
        assert!(!expanded.contains("In"), "{expanded}");

        let wide = render_at(154);
        assert!(wide.contains("Project"), "{wide}");
        assert!(wide.contains("Session"), "{wide}");
        assert!(wide.contains("In"), "{wide}");
        assert!(wide.contains("Out"), "{wide}");
    }

    #[test]
    fn each_schema_has_one_meaningful_flexible_column() {
        for tier in [
            LayoutTier::Emergency,
            LayoutTier::Narrow,
            LayoutTier::Medium,
            LayoutTier::Expanded,
            LayoutTier::Wide,
        ] {
            let sessions = session_columns(tier, tier == LayoutTier::Wide);
            let active = active_columns(tier);
            let recent = recent_columns(tier);
            let events = event_columns(tier);

            assert_eq!(flex_count(&sessions), 1);
            assert_eq!(flex_count(&active), 1);
            assert_eq!(flex_count(&recent), 1);
            assert_eq!(flex_count(&events), 1);
            assert_eq!(
                sessions
                    .iter()
                    .filter(|column| column.header.is_empty())
                    .count(),
                1
            );
            assert!(active.iter().all(|column| !column.header.is_empty()));
            assert!(recent.iter().all(|column| !column.header.is_empty()));
            assert!(events.iter().all(|column| !column.header.is_empty()));
        }
    }

    #[test]
    fn metric_columns_are_right_aligned() {
        let sessions = session_columns(LayoutTier::Wide, true);
        for key in [
            SessionColumn::Active,
            SessionColumn::Requests,
            SessionColumn::Failures,
            SessionColumn::Input,
            SessionColumn::Output,
            SessionColumn::Rate,
        ] {
            assert_eq!(alignment(&sessions, key), Some(Alignment::Right));
        }

        let active = active_columns(LayoutTier::Wide);
        for key in [
            ActiveColumn::Input,
            ActiveColumn::Output,
            ActiveColumn::Rate,
            ActiveColumn::Elapsed,
        ] {
            assert_eq!(alignment(&active, key), Some(Alignment::Right));
        }

        let recent = recent_columns(LayoutTier::Wide);
        for key in [
            RecentColumn::Code,
            RecentColumn::Latency,
            RecentColumn::Rate,
            RecentColumn::Input,
            RecentColumn::Output,
        ] {
            assert_eq!(alignment(&recent, key), Some(Alignment::Right));
        }
    }

    #[test]
    fn narrow_schemas_use_available_space_for_context() {
        let sessions = session_columns(LayoutTier::Narrow, false);
        assert!(
            sessions
                .iter()
                .any(|column| column.key == SessionColumn::Project)
        );
        assert!(
            sessions
                .iter()
                .any(|column| column.key == SessionColumn::Target)
        );

        let active = active_columns(LayoutTier::Narrow);
        assert!(
            active
                .iter()
                .any(|column| column.key == ActiveColumn::Provider)
        );
        assert!(
            active
                .iter()
                .any(|column| column.key == ActiveColumn::Model)
        );
        assert!(
            active
                .iter()
                .any(|column| column.key == ActiveColumn::Effort)
        );

        let recent = recent_columns(LayoutTier::Narrow);
        assert!(
            recent
                .iter()
                .any(|column| column.key == RecentColumn::Provider)
        );
        assert!(
            recent
                .iter()
                .any(|column| column.key == RecentColumn::Input)
        );
        assert!(
            recent
                .iter()
                .any(|column| column.key == RecentColumn::Output)
        );

        let events = event_columns(LayoutTier::Emergency);
        assert_eq!(headers(&events), ["Time", "Code", "Message"]);
    }

    #[test]
    fn display_session_id_shortens_uuids() {
        assert_eq!(
            display_session_id(Some("57c7c914-ada4-4f40-9672-985f950fbb66")),
            "950fbb66"
        );
    }

    #[test]
    fn display_session_id_distinguishes_uuidv7_with_shared_timestamp_prefix() {
        let first = "01a01f40-7a01-7000-8000-000000000001";
        let second = "01a01f40-7a01-7000-8000-000000000002";

        assert_ne!(
            display_session_id(Some(first)),
            display_session_id(Some(second)),
            "nearby UUIDv7 subagents must not collide in the eight-character ID column"
        );
    }

    #[test]
    fn display_session_id_handles_atypical_ids() {
        assert_eq!(display_session_id(Some("custom-session")), "custom-session");
        assert_eq!(display_session_id(Some("")), "no-session");
        assert_eq!(display_session_id(None), "no-session");
    }

    #[test]
    fn ellipsize_marks_truncated_values() {
        assert_eq!(ellipsize("claude-sonnet-4-6", 16), "claude-sonnet-4…");
        assert_eq!(ellipsize("gpt-5.6-sol", 16), "gpt-5.6-sol");
        assert_eq!(ellipsize("anything", 0), "");
    }

    #[test]
    fn token_sparkline_uses_fixed_wall_clock_buckets() {
        let samples = [
            (SystemTime::UNIX_EPOCH + Duration::from_secs(78), 2_000),
            (SystemTime::UNIX_EPOCH + Duration::from_secs(85), 3_000),
            (SystemTime::UNIX_EPOCH + Duration::from_secs(100), 4_000),
        ];
        let bucket_start = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let bucket_end = SystemTime::UNIX_EPOCH + Duration::from_secs(109);
        let next_bucket = SystemTime::UNIX_EPOCH + Duration::from_secs(110);

        assert_eq!(token_sparkline(&[], 4, bucket_start), "    ");
        assert_eq!(token_sparkline(&samples, 4, bucket_start), "▄▆ █");
        assert_eq!(token_sparkline(&samples, 4, bucket_end), "▄▆ █");
        assert_eq!(token_sparkline(&samples, 4, next_bucket), "▆ █ ");
        assert_eq!(token_sparkline(&samples, 0, bucket_start), "");
    }

    #[test]
    fn token_sparkline_uses_fixed_shared_scale() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let current = (now, 2_000);
        let offscreen_peak = (SystemTime::UNIX_EPOCH, 4_000);

        assert_eq!(token_sparkline(&[current], 2, now), " ▄");
        assert_eq!(token_sparkline(&[offscreen_peak, current], 2, now), " ▄");
        assert_eq!(token_sparkline(&[(now, 10_000)], 1, now), "█");
    }

    #[test]
    fn token_sparkline_dims_the_current_bucket() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
        let samples = [
            (SystemTime::UNIX_EPOCH + Duration::from_secs(95), 2_000),
            (now, 4_000),
        ];

        let line = token_sparkline_line(&samples, 2, now);

        assert_eq!(line.spans[0].content, "▄");
        assert_eq!(line.spans[0].style.fg, Some(BLUE));
        assert_eq!(line.spans[1].content, "█");
        assert_eq!(line.spans[1].style.fg, Some(DIM));
    }

    #[test]
    fn session_sparkline_appears_at_medium_width_and_expands() {
        let monitor = MonitorHandle::new(10);
        for (index, tokens) in [1_000, 2_000, 4_000].into_iter().enumerate() {
            let request_id = format!("request-{index}");
            monitor.request_started(
                &request_id,
                Some("sess-1".to_string()),
                Some(index as u64 + 1),
                EndpointKind::Messages,
            );
            monitor.provider_selected(&request_id, "codex", "gpt-5.6-sol", None);
            monitor.request_completed(&request_id, 200, Some(100), Some(tokens));
        }
        let state = monitor.snapshot();
        let render_at = |width| {
            let buffer = draw(width, 8, |frame| {
                render_sessions(frame, frame.area(), &state.sessions, 0, true)
            });
            buffer_text(&buffer)
        };
        let spark_chars = |text: &str| {
            text.chars()
                .filter(|ch| matches!(ch, '▁' | '▂' | '▃' | '▄' | '▅' | '▆' | '▇' | '█'))
                .count()
        };

        let emergency = render_at(77);
        assert!(!emergency.contains("Trend"), "{emergency}");
        assert_eq!(spark_chars(&emergency), 0, "{emergency}");

        let narrow = render_at(78);
        assert!(narrow.contains("Trend"), "{narrow}");
        assert!(spark_chars(&narrow) > 0, "{narrow}");

        let medium = render_at(90);
        assert!(medium.contains("Tok/10s"), "{medium}");
        assert!(spark_chars(&medium) > 0, "{medium}");

        let expanded = render_at(120);
        assert!(expanded.contains("Tokens/10s"), "{expanded}");
        assert!(spark_chars(&expanded) > 0, "{expanded}");

        let wide = render_at(SESSION_SPARKLINE_MIN_WIDTH);
        assert!(wide.contains("Tokens/10s · 4k"), "{wide}");
        assert!(spark_chars(&wide) > 0, "{wide}");
    }

    #[test]
    fn empty_tables_hide_columns_and_center_placeholders() {
        let sessions = draw(40, 9, |frame| {
            render_sessions(frame, frame.area(), &[], 0, true)
        });
        let sessions_text = buffer_text(&sessions);
        assert_centered(&sessions, "No sessions", 4);
        assert!(!sessions_text.contains("provider"));
        assert!(sessions_text.contains("No sessions"));

        let active = draw(27, 6, |frame| render_active(frame, frame.area(), &[], 0));
        let active_text = buffer_text(&active);
        assert_centered(&active, "No active requests", 2);
        assert!(!active_text.contains("started"));
        assert!(active_text.contains("No active requests"));

        let recent = draw(40, 9, |frame| {
            render_recent(frame, frame.area(), &[], 0, false)
        });
        let recent_text = buffer_text(&recent);
        assert_centered(&recent, "No recent requests", 4);
        assert!(!recent_text.contains("finished"));
        assert!(recent_text.contains("No recent requests"));

        let events = draw(40, 9, |frame| render_events(frame, frame.area(), &[]));
        let events_text = buffer_text(&events);
        assert_centered(&events, "No events", 4);
        assert!(!events_text.contains("time"));
        assert!(events_text.contains("No events"));
    }

    #[test]
    fn active_status_keeps_full_label_at_narrow_width() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started("request-1", None, None, EndpointKind::Messages);
        monitor.upstream_started("request-1");
        let state = monitor.snapshot();

        let active = draw(88, 6, |frame| {
            render_active(frame, frame.area(), &state.active, 0)
        });

        let active_text = buffer_text(&active);
        assert!(active_text.contains("⠋ upstream"), "{active_text}");
    }

    #[test]
    fn populated_tables_render_rows_without_placeholders() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started(
            "request-1",
            Some("sess-1".to_string()),
            Some(1),
            EndpointKind::Messages,
        );
        monitor.project_resolved("request-1", "example-project");
        monitor.provider_selected(
            "request-1",
            "codex",
            "gpt-5.6-sol",
            Some("high".to_string()),
        );
        let active_state = monitor.snapshot();

        let sessions = draw(170, 8, |frame| {
            render_sessions(frame, frame.area(), &active_state.sessions, 0, true)
        });
        let sessions_text = buffer_text(&sessions);
        assert!(sessions_text.contains("Provider"));
        assert!(sessions_text.contains("Project"));
        assert!(sessions_text.contains("example-project"));
        assert!(sessions_text.contains("sess-1"));
        assert!(!sessions_text.contains("No sessions"));

        let active = draw(120, 8, |frame| {
            render_active(frame, frame.area(), &active_state.active, 0)
        });
        let active_text = buffer_text(&active);
        assert!(active_text.contains("Started"));
        assert!(active_text.contains("gpt-5.6-sol"));
        assert!(!active_text.contains("No active requests"));

        monitor.request_completed("request-1", 200, Some(100), Some(25));
        let completed_state = monitor.snapshot();
        let recent = draw(140, 8, |frame| {
            render_recent(frame, frame.area(), &completed_state.recent, 0, false)
        });
        let recent_text = buffer_text(&recent);
        assert!(recent_text.contains("Finished"));
        assert!(recent_text.contains("200"));
        assert!(!recent_text.contains("No recent requests"));

        let events = draw(100, 8, |frame| {
            render_events(frame, frame.area(), &completed_state.recent)
        });
        assert!(buffer_text(&events).contains("No events"));
    }

    #[test]
    fn selected_rows_scroll_into_table_viewports() {
        let state = mock_state();
        let sessions = (0..12)
            .map(|index| {
                let mut session = state.sessions[0].clone();
                session.session_id = Some(format!("row-{index:04}"));
                session
            })
            .collect::<Vec<_>>();
        let session_buffer = draw(120, 6, |frame| {
            render_sessions(frame, frame.area(), &sessions, 11, true)
        });
        let session_text = buffer_text(&session_buffer);
        assert!(session_text.contains("row-0011"), "{session_text}");
        assert!(!session_text.contains("row-0000"), "{session_text}");

        let recent = (0..12)
            .map(|index| {
                let mut request = state.recent[0].clone();
                request.project = Some(format!("row-{index:04}"));
                request
            })
            .collect::<Vec<_>>();
        let recent_buffer = draw(120, 6, |frame| {
            render_recent(frame, frame.area(), &recent, 11, true)
        });
        let recent_text = buffer_text(&recent_buffer);
        assert!(recent_text.contains("row-0011"), "{recent_text}");
        assert!(!recent_text.contains("row-0000"), "{recent_text}");
    }

    #[test]
    fn mock_state_renders_representative_panes_at_wide_width() {
        let state = mock_state();
        let mut app = MonitorApp {
            listen_url: "mock://tui-demo".to_string(),
            setup_text: String::new(),
            show_setup: false,
            show_sand_settings: false,
            show_help: false,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 0,
            recent_selected: 0,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: None,
            shutdown_complete: None,
            sand_models: Vec::new(),
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: None,
            sand_input: None,
        };

        let buffer = draw(180, 48, |frame| render(frame, &mut app, &state));
        let text = buffer_text(&buffer);

        assert!(text.contains("mock://tui-demo"), "{text}");
        assert!(text.contains("claude-cursor-proxy"), "{text}");
        assert!(text.contains("usage"), "{text}");
        assert!(text.contains("ultra"), "{text}");
        assert!(text.contains("bot"), "{text}");
        assert!(text.contains("cost"), "{text}");
        assert!(text.contains("events 7"), "{text}");
        assert!(text.contains("streaming"), "{text}");
        assert!(text.contains("gpt-5.6-terra"), "{text}");
        assert!(text.contains("upstream connection closed"), "{text}");

        app.detail = Some(DetailView::Usage);
        let usage = draw(80, 24, |frame| render(frame, &mut app, &state));
        let usage_text = buffer_text(&usage);
        assert!(usage_text.contains("bot period"), "{usage_text}");
        assert!(usage_text.contains("dashboard cost"), "{usage_text}");
        assert!(usage_text.contains("claude-fable-5"), "{usage_text}");
    }

    #[test]
    fn usage_detail_renders_dashboard_period_and_events() {
        let state = mock_state();
        let detail = draw(140, 24, |frame| {
            render_usage_detail(frame, frame.area(), &state)
        });
        let text = buffer_text(&detail);
        assert!(text.contains("2026-08-01T00:00:00"), "{text}");
        assert!(text.contains("dashboard cost"), "{text}");
        assert!(text.contains("claude-fable-5"), "{text}");
        assert!(text.contains("INCLUDED"), "{text}");
    }

    #[test]
    fn accounts_view_renders_active_marker_and_usage_per_account() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let first = crate::providers::cursor::auth::CursorAccountProfile {
            id: "account-a".to_string(),
            label: Some("Primary".to_string()),
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: "token-a".to_string(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: Some("user-a".to_string()),
                email: Some("primary@example.com".to_string()),
                source: "test".to_string(),
            },
            active: true,
        };
        let second = crate::providers::cursor::auth::CursorAccountProfile {
            id: "account-b".to_string(),
            label: None,
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: "token-b".to_string(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: Some("user-b".to_string()),
                email: Some("secondary@example.com".to_string()),
                source: "test".to_string(),
            },
            active: false,
        };
        let mut ui = account_ui_lock();
        ui.accounts = vec![first, second];
        ui.selected = 1;
        ui.usage
            .insert("account-a".to_string(), usage_state_with_sand_percent(25.0));
        ui.usage
            .insert("account-b".to_string(), usage_state_with_sand_percent(75.0));
        ui.message = None;
        drop(ui);

        let buffer = draw(140, 14, |frame| render_accounts_detail(frame, frame.area()));
        let text = buffer_text(&buffer);
        assert!(text.contains("Cursor accounts"), "{text}");
        assert!(text.contains("Name"), "{text}");
        assert!(text.contains("Email"), "{text}");
        assert!(text.contains("Total"), "{text}");
        assert!(text.contains("Auto"), "{text}");
        assert!(text.contains("API"), "{text}");
        assert!(text.contains("Updated"), "{text}");
        assert!(text.contains("Bot/wk"), "{text}");
        assert!(text.contains("*   Primary"), "{text}");
        assert!(text.contains("> secondary"), "{text}");
        assert!(text.contains("secondary@example.com"), "{text}");
        assert!(text.contains("25.0%/wk"), "{text}");
        assert!(text.contains("75.0%/wk"), "{text}");
        // API must be rendered as its own meter rather than being lost in a
        // truncated account-wide usage summary.
        assert!(text.matches("5.0%").count() >= 6, "{text}");

        let mut ui = account_ui_lock();
        *ui = AccountUiState::default();
    }

    #[test]
    fn accounts_view_keeps_api_and_bot_visible_at_narrow_width() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let account = crate::providers::cursor::auth::CursorAccountProfile {
            id: "account-narrow".to_string(),
            label: None,
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: "token-narrow".to_string(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: Some("user-narrow".to_string()),
                email: Some("narrow@example.com".to_string()),
                source: "test".to_string(),
            },
            active: true,
        };
        let mut ui = account_ui_lock();
        ui.accounts = vec![account];
        ui.selected = 0;
        ui.usage.insert(
            "account-narrow".to_string(),
            crate::monitor::AccountUsageState::Ready(crate::monitor::AccountUsageSnapshot {
                email: Some("narrow@example.com".to_string()),
                membership: Some("ultra".to_string()),
                auto_percent: Some(12.5),
                api_percent: Some(87.5),
                total_percent: Some(50.0),
                plan_used_usd: None,
                plan_limit_usd: None,
                on_demand_used_usd: None,
                on_demand_limit_usd: None,
                grok_bot_percent: Some(6.25),
                grok_bot_period_start: None,
                grok_bot_reset: None,
                total_cost_usd: None,
                usage_event_count: None,
                usage_events: Vec::new(),
                fetched_at: SystemTime::now(),
            }),
        );
        ui.message = None;
        drop(ui);

        let buffer = draw(88, 14, |frame| render_accounts_detail(frame, frame.area()));
        let text = buffer_text(&buffer);
        assert!(text.contains("Name"), "{text}");
        assert!(text.contains("API"), "{text}");
        assert!(text.contains("Bot"), "{text}");
        assert!(text.contains("87.5%"), "{text}");
        assert!(text.contains("6.2%"), "{text}");
        // The email column is intentionally omitted at this width, while the
        // account name remains identifiable from the email local part.
        assert!(!text.lines().any(|line| line.contains("Email")), "{text}");
        assert!(text.contains("narrow"), "{text}");

        let mut ui = account_ui_lock();
        *ui = AccountUiState::default();
    }

    #[test]
    fn account_routes_view_renders_model_assignment_and_controls() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let account = crate::providers::cursor::auth::CursorAccountProfile {
            id: "account-work".to_string(),
            label: Some("Work".to_string()),
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: "token-work".to_string(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: None,
                email: Some("work@example.com".to_string()),
                source: "test".to_string(),
            },
            active: true,
        };
        {
            let mut accounts = account_ui_lock();
            accounts.accounts = vec![account];
        }
        {
            let mut routes = account_route_ui_lock();
            routes.models = vec![
                "claude-fable-5".to_string(),
                "gemini-3.1-pro".to_string(),
                "grok-*".to_string(),
                "gpt-5.4".to_string(),
            ];
            routes.policy = config::CursorAccountRoutingPolicy::new([
                config::CursorModelAccountRule::new("gemini-3.1-pro", "account-work"),
                config::CursorModelAccountRule::new("grok-*", "account-work"),
            ]);
            routes.selected = 0;
            routes.input = None;
            routes.message = None;
        }

        let buffer = draw(120, 18, |frame| {
            render_account_routes_detail(frame, frame.area())
        });
        let text = buffer_text(&buffer);
        assert!(text.contains("Model account routes"), "{text}");
        assert!(text.contains("gemini-3.1-pro"), "{text}");
        assert!(text.contains("grok-*"), "{text}");
        assert!(text.contains("Work"), "{text}");
        assert!(text.contains("assign account"), "{text}");
        assert!(text.contains("automatic"), "{text}");

        *account_ui_lock() = AccountUiState::default();
        *account_route_ui_lock() = AccountRouteUiState::default();
    }

    #[test]
    fn account_routes_view_marks_ambiguous_selector_instead_of_first_account() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let account = |id: &str, label: &str, email: &str| {
            crate::providers::cursor::auth::CursorAccountProfile {
                id: id.to_string(),
                label: Some(label.to_string()),
                auth: crate::providers::cursor::auth::CursorAuth {
                    access_token: format!("token-{id}"),
                    refresh_token: None,
                    api_key: None,
                    expires: None,
                    user_id: None,
                    email: Some(email.to_string()),
                    source: "test".to_string(),
                },
                active: id == "account-a",
            }
        };
        {
            let mut accounts = account_ui_lock();
            accounts.accounts = vec![
                account("account-a", "Team Alpha", "alpha@example.com"),
                account("account-b", "Team Beta", "beta@example.com"),
            ];
        }
        {
            let mut routes = account_route_ui_lock();
            routes.models = vec!["gemini-3.1-pro".to_string()];
            routes.policy =
                config::CursorAccountRoutingPolicy::new([config::CursorModelAccountRule::new(
                    "gemini-3.1-pro",
                    "team*",
                )]);
            routes.selected = 0;
            routes.input = None;
            routes.message = None;
        }

        let buffer = draw(120, 12, |frame| {
            render_account_routes_detail(frame, frame.area())
        });
        let text = buffer_text(&buffer);
        assert!(text.contains("team* (ambiguous)"), "{text}");
        // The picker intentionally lists both matching accounts so the user
        // can disambiguate the selector before saving a route.
        assert!(text.contains("Team Alpha"), "{text}");
        assert!(text.contains("Team Beta"), "{text}");

        *account_ui_lock() = AccountUiState::default();
        *account_route_ui_lock() = AccountRouteUiState::default();
    }

    #[test]
    fn account_route_picker_shows_identity_plan_quota_and_bound_marker() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let account = crate::providers::cursor::auth::CursorAccountProfile {
            id: "account-primary-long-id".to_string(),
            label: Some("Primary workspace".to_string()),
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: "token-primary".to_string(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: Some("user-primary".to_string()),
                email: Some("primary@example.com".to_string()),
                source: "test".to_string(),
            },
            active: true,
        };
        {
            let mut ui = account_ui_lock();
            ui.accounts = vec![account];
            ui.usage.insert(
                "account-primary-long-id".to_string(),
                crate::monitor::AccountUsageState::Ready(crate::monitor::AccountUsageSnapshot {
                    email: Some("primary@example.com".to_string()),
                    membership: Some("pro".to_string()),
                    auto_percent: Some(10.0),
                    api_percent: Some(7.5),
                    total_percent: Some(42.0),
                    plan_used_usd: Some(12.0),
                    plan_limit_usd: Some(20.0),
                    on_demand_used_usd: None,
                    on_demand_limit_usd: None,
                    grok_bot_percent: Some(3.2),
                    grok_bot_period_start: None,
                    grok_bot_reset: None,
                    total_cost_usd: None,
                    usage_event_count: None,
                    usage_events: Vec::new(),
                    fetched_at: SystemTime::now(),
                }),
            );
            ui.message = None;
        }
        {
            let mut routes = account_route_ui_lock();
            routes.models = vec!["gemini-3.1-pro".to_string()];
            routes.policy =
                config::CursorAccountRoutingPolicy::new([config::CursorModelAccountRule::new(
                    "gemini-3.1-pro",
                    "account-primary-long-id",
                )]);
            routes.selected = 0;
            routes.account_selected = 1;
            routes.pane = AccountRoutePane::Accounts;
            routes.input = None;
            routes.message = None;
        }

        let buffer = draw(140, 20, |frame| {
            render_account_routes_detail(frame, frame.area())
        });
        let text = buffer_text(&buffer);
        assert!(text.contains("Primary workspace"), "{text}");
        assert!(text.contains("primary@example.com"), "{text}");
        assert!(text.contains("pro $12/$20"), "{text}");
        assert!(text.contains("total 42.0%"), "{text}");
        assert!(text.contains("auto 10.0%"), "{text}");
        assert!(text.contains("api 7.5%"), "{text}");
        assert!(text.contains("bot 3.2%/wk"), "{text}");
        assert!(text.contains("updated "), "{text}");
        assert!(text.contains("[bound]"), "{text}");
        assert!(text.contains("automatic"), "{text}");

        *account_ui_lock() = AccountUiState::default();
        *account_route_ui_lock() = AccountRouteUiState::default();
    }

    #[test]
    fn account_route_quota_hint_uses_the_selected_models_surface() {
        let state =
            crate::monitor::AccountUsageState::Ready(crate::monitor::AccountUsageSnapshot {
                email: None,
                membership: None,
                auto_percent: Some(100.0),
                api_percent: Some(100.0),
                total_percent: Some(100.0),
                plan_used_usd: None,
                plan_limit_usd: None,
                on_demand_used_usd: None,
                on_demand_limit_usd: None,
                grok_bot_percent: Some(0.1),
                grok_bot_period_start: None,
                grok_bot_reset: None,
                total_cost_usd: None,
                usage_event_count: None,
                usage_events: Vec::new(),
                fetched_at: SystemTime::now(),
            });

        let cli = account_route_quota_hint(AccountQuotaLane::CliApi, Some(&state));
        assert!(
            cli.starts_with("lane CLI -> CLI/API 100.0% used (exhausted)"),
            "{cli}"
        );
        assert!(cli.contains("Sand/Bot 0.1% used (available)"), "{cli}");

        let sand = account_route_quota_hint(AccountQuotaLane::SandBot, Some(&state));
        assert!(
            sand.starts_with("lane Sand -> Sand/Bot 0.1% used (available)"),
            "{sand}"
        );
        assert!(sand.contains("CLI/API 100.0% used (exhausted)"), "{sand}");
    }

    #[test]
    fn account_route_selected_account_prefers_bound_then_active_for_automatic() {
        let account =
            |id: &str, active: bool| crate::providers::cursor::auth::CursorAccountProfile {
                id: id.to_string(),
                label: Some(id.to_string()),
                auth: crate::providers::cursor::auth::CursorAuth {
                    access_token: format!("token-{id}"),
                    refresh_token: None,
                    api_key: None,
                    expires: None,
                    user_id: None,
                    email: None,
                    source: "test".to_string(),
                },
                active,
            };
        let accounts = vec![account("active", true), account("bound", false)];
        let policy =
            config::CursorAccountRoutingPolicy::new([config::CursorModelAccountRule::new(
                "grok-4.6", "bound",
            )]);

        assert_eq!(
            account_route_selected_account_id(0, "grok-4.6", &policy, &accounts).as_deref(),
            Some("bound")
        );
        assert_eq!(
            account_route_selected_account_id(
                0,
                "gemini-3.1-pro",
                &config::CursorAccountRoutingPolicy::default(),
                &accounts,
            )
            .as_deref(),
            Some("active")
        );
    }

    #[test]
    fn account_route_selection_moves_independently_in_each_pane() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        {
            let mut accounts = account_ui_lock();
            accounts.accounts = vec![
                crate::providers::cursor::auth::CursorAccountProfile {
                    id: "account-a".to_string(),
                    label: Some("A".to_string()),
                    auth: crate::providers::cursor::auth::CursorAuth {
                        access_token: "token-a".to_string(),
                        refresh_token: None,
                        api_key: None,
                        expires: None,
                        user_id: None,
                        email: Some("a@example.com".to_string()),
                        source: "test".to_string(),
                    },
                    active: true,
                },
                crate::providers::cursor::auth::CursorAccountProfile {
                    id: "account-b".to_string(),
                    label: Some("B".to_string()),
                    auth: crate::providers::cursor::auth::CursorAuth {
                        access_token: "token-b".to_string(),
                        refresh_token: None,
                        api_key: None,
                        expires: None,
                        user_id: None,
                        email: Some("b@example.com".to_string()),
                        source: "test".to_string(),
                    },
                    active: false,
                },
            ];
        }
        {
            let mut routes = account_route_ui_lock();
            routes.models = vec!["model-a".to_string(), "model-b".to_string()];
            routes.policy = config::CursorAccountRoutingPolicy::default();
            routes.selected = 0;
            routes.account_selected = 0;
            routes.pane = AccountRoutePane::Models;
        }

        move_account_route_selection(1);
        assert_eq!(account_route_ui_lock().selected, 1);
        account_route_ui_lock().pane = AccountRoutePane::Accounts;
        move_account_route_selection(1);
        assert_eq!(account_route_ui_lock().account_selected, 1);
        move_account_route_selection(1);
        assert_eq!(account_route_ui_lock().account_selected, 2);
        move_account_route_selection(1);
        assert_eq!(account_route_ui_lock().account_selected, 2);
        move_account_route_selection(-1);
        assert_eq!(account_route_ui_lock().account_selected, 1);

        *account_ui_lock() = AccountUiState::default();
        *account_route_ui_lock() = AccountRouteUiState::default();
    }

    #[test]
    fn account_route_assignment_replaces_exact_rule_without_dropping_wildcards() {
        let policy = config::CursorAccountRoutingPolicy::new([
            config::CursorModelAccountRule::new("*", "fallback"),
            config::CursorModelAccountRule::new("gemini-3.1-pro", "old"),
        ]);
        let next =
            account_route_rules_with_assignment(&policy, "gemini-3.1-pro", Some("new-account"));
        assert_eq!(
            next.account_for_model("gemini-3.1-pro"),
            Some("new-account")
        );
        assert_eq!(next.account_for_model("grok-4.6"), Some("fallback"));

        let cleared = account_route_rules_with_assignment(&next, "gemini-3.1-pro", None);
        assert_eq!(
            cleared.account_for_model("gemini-3.1-pro"),
            Some("fallback")
        );
    }

    #[test]
    fn account_route_cycle_clears_fable_alias_and_concrete_rules_together() {
        let policy = config::CursorAccountRoutingPolicy::new([
            config::CursorModelAccountRule::new("*", "fallback"),
            config::CursorModelAccountRule::new("fable", "old-alias"),
            config::CursorModelAccountRule::new("claude-fable-5-thinking-max", "old-concrete"),
            config::CursorModelAccountRule::new("claude-fable-5-preview", "old-preview"),
        ]);

        // This is the sequence used by Enter/space: choose the next account,
        // then eventually cycle to automatic.  Each step must remove stale
        // exact aliases before inserting the newly selected assignment.
        let account_a =
            account_route_rules_with_assignment(&policy, "claude-fable-5", Some("account-a"));
        assert_eq!(
            account_a.account_for_model("claude-fable-5-thinking-max"),
            Some("account-a")
        );
        assert_eq!(
            account_a
                .routes()
                .iter()
                .filter(|rule| !rule.model.contains('*') && !rule.model.contains('?'))
                .count(),
            1,
            "only the newly assigned literal should remain; wildcard fallback is retained"
        );

        let account_b = account_route_rules_with_assignment(
            &account_a,
            "claude-fable-5-thinking-max",
            Some("account-b"),
        );
        assert_eq!(
            account_b.account_for_model("fable"),
            Some("account-b"),
            "rotating from a concrete Fable row must replace the alias assignment too"
        );

        let automatic = account_route_rules_with_assignment(&account_b, "fable-preview", None);
        assert_eq!(
            automatic.account_for_model("claude-fable-5"),
            Some("fallback")
        );
        assert!(automatic.routes().iter().all(|rule| {
            rule.model == "*"
                || rule.model == "gemini-*"
                || (!rule.model.contains("fable") && !rule.model.contains("preview"))
        }));
    }

    #[test]
    fn account_delete_confirmation_targets_selected_id_and_is_rendered() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let account = crate::providers::cursor::auth::CursorAccountProfile {
            id: "account-delete".to_string(),
            label: Some("Delete me".to_string()),
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: "token-delete".to_string(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: Some("user-delete".to_string()),
                email: Some("delete@example.com".to_string()),
                source: "test".to_string(),
            },
            active: true,
        };
        let mut ui = account_ui_lock();
        ui.accounts = vec![account];
        ui.selected = 0;
        ui.message = None;
        ui.delete_confirm = None;
        drop(ui);

        request_account_delete();
        let pending = account_ui_lock()
            .delete_confirm
            .clone()
            .expect("delete confirmation should capture the selected row");
        assert_eq!(pending.id, "account-delete");
        assert_eq!(pending.display_name, "Delete me");
        assert!(pending.active);

        let rendered = draw(80, 12, |frame| {
            render_account_delete_confirmation(frame, frame.area())
        });
        let text = buffer_text(&rendered);
        assert!(text.contains("Delete Cursor account"), "{text}");
        assert!(text.contains("Delete active account Delete me?"), "{text}");
        assert!(text.contains("y/Enter"), "{text}");
        assert!(text.contains("n/Esc"), "{text}");

        cancel_account_delete();
        assert!(account_ui_lock().delete_confirm.is_none());
        let mut ui = account_ui_lock();
        *ui = AccountUiState::default();
    }

    #[test]
    fn account_delete_selection_stays_in_range_after_removal() {
        assert_eq!(selected_index_after_account_delete(4, 0, None, true), 0);
        assert_eq!(selected_index_after_account_delete(4, 3, Some(1), true), 1);
        assert_eq!(selected_index_after_account_delete(4, 3, None, true), 2);
        assert_eq!(selected_index_after_account_delete(4, 3, Some(1), false), 2);
        assert_eq!(selected_index_after_account_delete(0, 2, None, false), 0);
    }

    #[test]
    fn account_name_prefers_custom_label_and_falls_back_to_email_local_part() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut account = crate::providers::cursor::auth::CursorAccountProfile {
            id: "account-name".to_string(),
            label: Some("Work".to_string()),
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: "token-name".to_string(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: None,
                email: Some("person@example.com".to_string()),
                source: "test".to_string(),
            },
            active: false,
        };
        assert_eq!(account_name_for_display(&account, account.email()), "Work");
        account.label = None;
        assert_eq!(
            account_name_for_display(&account, account.email()),
            "person"
        );
        account.auth.email = None;
        assert_eq!(
            account_name_for_display(&account, Some("dashboard@example.net")),
            "dashboard"
        );
    }

    #[test]
    fn account_usage_cancellation_invalidates_pending_workers() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let (_tx, rx) = mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let mut ui = AccountUiState {
            usage_rx: Some(rx),
            usage_pending: 3,
            usage_generation: 41,
            usage_cancel: Some(Arc::clone(&cancel)),
            usage_scope: Some(AccountUsageScope::All),
            ..AccountUiState::default()
        };

        cancel_usage_locked(&mut ui);

        assert!(cancel.load(Ordering::Acquire));
        assert_eq!(ui.usage_generation, 42);
        assert_eq!(ui.usage_pending, 0);
        assert!(ui.usage_rx.is_none());
        assert!(ui.usage_cancel.is_none());
        assert!(ui.usage_scope.is_none());
        assert!(ui.usage_in_flight.is_empty());
    }

    #[test]
    fn account_usage_force_refresh_is_coalesced_behind_live_lease() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut ui = AccountUiState::default();
        ui.usage_loading.insert("account-a".into());
        ui.usage_in_flight.insert(
            "account-a".into(),
            AccountUsageLease {
                wave_id: 4,
                generation: 2,
            },
        );

        assert!(account_usage_busy_locked(&mut ui, "account-a", true));
        assert!(ui.usage_refresh_pending.contains("account-a"));
        // A repeated key is idempotent and must not create a second marker or
        // alter the existing lease while the first socket is still live.
        assert!(account_usage_busy_locked(&mut ui, "account-a", true));
        assert_eq!(
            ui.usage_in_flight.get("account-a"),
            Some(&AccountUsageLease {
                wave_id: 4,
                generation: 2,
            })
        );

        // Once the worker releases its lease, the pending marker is the only
        // signal needed by `drain_pending_account_usage` to launch one
        // follow-up request for the same account.
        release_account_usage_lease_locked(&mut ui, "account-a", 4, 2);
        assert!(!ui.usage_in_flight.contains_key("account-a"));
        assert!(ui.usage_refresh_pending.contains("account-a"));
    }

    #[test]
    fn account_usage_wave_completion_clears_missing_result_and_keeps_ready() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut ui = AccountUiState::default();
        ui.usage
            .insert("account-a".into(), usage_state_with_sand_percent(22.0));
        ui.usage_generations.insert("account-a".into(), 1);
        ui.usage_loading.insert("account-a".into());
        ui.usage_cancels
            .insert("account-a".into(), Arc::new(AtomicBool::new(false)));
        ui.usage_waves.insert(
            7,
            AccountUsageWave {
                expected_workers: 2,
                completed_workers: 0,
                accounts: vec![AccountUsageWaveAccount {
                    account_id: "account-a".into(),
                    generation: 1,
                }],
                deadline: Instant::now() + Duration::from_secs(10),
            },
        );

        complete_account_usage_wave(&mut ui, 7);
        assert!(ui.usage_loading.contains("account-a"));
        complete_account_usage_wave(&mut ui, 7);

        assert!(!ui.usage_loading.contains("account-a"));
        assert!(!ui.usage_waves.contains_key(&7));
        assert!(matches!(
            ui.usage.get("account-a"),
            Some(crate::monitor::AccountUsageState::Ready(_))
        ));
        assert_eq!(
            ui.usage_errors.get("account-a").map(String::as_str),
            Some("Account usage worker stopped")
        );
    }

    #[test]
    fn account_usage_wave_watchdog_keeps_lease_then_drains_queued_refresh() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut ui = AccountUiState::default();
        ui.usage_generations.insert("account-stuck".into(), 3);
        ui.usage_loading.insert("account-stuck".into());
        ui.usage.insert(
            "account-stuck".into(),
            crate::monitor::AccountUsageState::Unknown,
        );
        ui.usage_cancels
            .insert("account-stuck".into(), Arc::new(AtomicBool::new(false)));
        ui.usage_in_flight.insert(
            "account-stuck".into(),
            AccountUsageLease {
                wave_id: 9,
                generation: 3,
            },
        );
        ui.usage_waves.insert(
            9,
            AccountUsageWave {
                expected_workers: 1,
                completed_workers: 0,
                accounts: vec![AccountUsageWaveAccount {
                    account_id: "account-stuck".into(),
                    generation: 3,
                }],
                deadline: Instant::now() - Duration::from_secs(1),
            },
        );

        expire_account_usage_waves(&mut ui);

        assert!(!ui.usage_loading.contains("account-stuck"));
        assert!(!ui.usage_waves.contains_key(&9));
        assert!(matches!(
            ui.usage.get("account-stuck"),
            Some(crate::monitor::AccountUsageState::Failed(message))
                if message == "Account usage refresh timed out"
        ));
        assert_eq!(ui.usage_generations["account-stuck"], 4);
        assert!(
            ui.usage_in_flight.contains_key("account-stuck"),
            "a timed-out socket keeps its lease until the worker truly exits"
        );
        assert!(account_usage_busy_locked(&mut ui, "account-stuck", true));
        assert!(ui.usage_refresh_pending.contains("account-stuck"));

        apply_account_usage_event_locked(
            &mut ui,
            AccountUsageEvent::Result {
                result: Box::new(AccountUsageResult {
                    account_id: "account-stuck".into(),
                    state: usage_state_with_sand_percent(99.0),
                    generation: 3,
                }),
                wave_id: 9,
                source_credential_fingerprint: 1,
                credential_fingerprint: 1,
            },
        );

        assert!(!ui.usage_in_flight.contains_key("account-stuck"));
        assert!(ui.usage_refresh_pending.contains("account-stuck"));
        assert!(matches!(
            ui.usage.get("account-stuck"),
            Some(crate::monitor::AccountUsageState::Failed(message))
                if message == "Account usage refresh timed out"
        ));
    }

    #[test]
    fn account_usage_worker_guard_reports_panic_completion() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let (tx, rx) = mpsc::channel();
        let panic_result = std::panic::catch_unwind(|| {
            let _guard = AccountUsageWorkerGuard {
                tx,
                wave_id: 11,
                account_ids: Vec::new(),
            };
            panic!("simulated usage worker panic");
        });
        assert!(panic_result.is_err());
        assert!(matches!(
            rx.try_recv(),
            Ok(AccountUsageEvent::WaveComplete { wave_id: 11 })
        ));
    }

    #[test]
    fn account_usage_failure_from_old_wave_keeps_new_wave_message() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut ui = AccountUiState {
            usage_message_wave: Some(22),
            message: Some("Fetching account usage...".into()),
            ..AccountUiState::default()
        };
        ui.usage_generations.insert("account-old".into(), 4);
        ui.usage_loading.insert("account-old".into());
        ui.usage_waves.insert(
            21,
            AccountUsageWave {
                expected_workers: 1,
                completed_workers: 0,
                accounts: vec![AccountUsageWaveAccount {
                    account_id: "account-old".into(),
                    generation: 4,
                }],
                deadline: Instant::now() + Duration::from_secs(10),
            },
        );

        apply_account_usage_event_locked(
            &mut ui,
            AccountUsageEvent::Result {
                result: Box::new(AccountUsageResult {
                    account_id: "account-old".into(),
                    state: crate::monitor::AccountUsageState::Failed("old failure".into()),
                    generation: 4,
                }),
                wave_id: 21,
                source_credential_fingerprint: 1,
                credential_fingerprint: 1,
            },
        );

        assert_eq!(ui.message.as_deref(), Some("Fetching account usage..."));
        assert_eq!(
            ui.usage_errors.get("account-old").map(String::as_str),
            Some("old failure")
        );
    }

    #[test]
    fn account_usage_result_accepts_refreshed_credential_fingerprint() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let auth =
            |access_token: &str, refresh_token: &str| crate::providers::cursor::auth::CursorAuth {
                access_token: access_token.into(),
                refresh_token: Some(refresh_token.into()),
                api_key: None,
                expires: None,
                user_id: Some("user".into()),
                email: Some("user@example.com".into()),
                source: "test".into(),
            };
        let source_auth = auth("old-access", "old-refresh");
        let refreshed_auth = auth("new-access", "new-refresh");
        let source_fingerprint = account_auth_credential_fingerprint(&source_auth);
        let refreshed_fingerprint = account_auth_credential_fingerprint(&refreshed_auth);
        assert_ne!(source_fingerprint, refreshed_fingerprint);

        let mut ui = AccountUiState {
            accounts: vec![crate::providers::cursor::auth::CursorAccountProfile {
                id: "stable-id".into(),
                label: Some("Work".into()),
                auth: refreshed_auth,
                active: false,
            }],
            ..AccountUiState::default()
        };
        ui.usage_generations.insert("stable-id".into(), 1);
        ui.usage_loading.insert("stable-id".into());

        apply_account_usage_event_result_for_wave(
            &mut ui,
            AccountUsageResult {
                account_id: "stable-id".into(),
                state: usage_state_with_sand_percent(99.0),
                generation: 1,
            },
            source_fingerprint,
            refreshed_fingerprint,
            None,
        );

        assert_eq!(ui.usage_generations["stable-id"], 1);
        assert!(!ui.usage_loading.contains("stable-id"));
        assert!(!ui.usage_errors.contains_key("stable-id"));
        assert!(matches!(
            ui.usage.get("stable-id"),
            Some(crate::monitor::AccountUsageState::Ready(_))
        ));
    }

    #[test]
    fn account_usage_result_from_replaced_credentials_is_ignored() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let old_auth = crate::providers::cursor::auth::CursorAuth {
            access_token: "old-access".into(),
            refresh_token: Some("old-refresh".into()),
            api_key: None,
            expires: None,
            user_id: Some("user".into()),
            email: Some("user@example.com".into()),
            source: "test".into(),
        };
        let mut ui = AccountUiState {
            accounts: vec![crate::providers::cursor::auth::CursorAccountProfile {
                id: "stable-id".into(),
                label: Some("Work".into()),
                auth: old_auth.clone(),
                active: false,
            }],
            ..AccountUiState::default()
        };
        ui.usage
            .insert("stable-id".into(), usage_state_with_sand_percent(18.0));
        ui.usage_generations.insert("stable-id".into(), 1);
        ui.usage_loading.insert("stable-id".into());
        ui.usage_cancels
            .insert("stable-id".into(), Arc::new(AtomicBool::new(false)));

        let old_profile = crate::providers::cursor::auth::CursorAccountProfile {
            id: "stable-id".into(),
            label: Some("Work".into()),
            auth: old_auth,
            active: false,
        };
        ui.accounts[0].auth.access_token = "new-access".into();
        apply_account_usage_event_result(
            &mut ui,
            AccountUsageResult {
                account_id: "stable-id".into(),
                state: usage_state_with_sand_percent(99.0),
                generation: 1,
            },
            account_credential_fingerprint(&old_profile),
        );

        assert!(!ui.usage_loading.contains("stable-id"));
        assert!(matches!(
            ui.usage.get("stable-id"),
            Some(crate::monitor::AccountUsageState::Ready(_))
        ));
        assert_eq!(ui.usage_generations["stable-id"], 2);
    }

    #[test]
    fn account_usage_refresh_isolated_per_account_and_keeps_ready_on_error() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut ui = AccountUiState::default();
        ui.usage
            .insert("account-a".into(), usage_state_with_sand_percent(20.0));
        ui.usage
            .insert("account-b".into(), usage_state_with_sand_percent(30.0));
        ui.usage_generations.insert("account-a".into(), 1);
        ui.usage_generations.insert("account-b".into(), 1);
        ui.usage_loading
            .extend(["account-a".into(), "account-b".into()]);
        let cancel_a = Arc::new(AtomicBool::new(false));
        let cancel_b = Arc::new(AtomicBool::new(false));
        ui.usage_cancels
            .insert("account-a".into(), Arc::clone(&cancel_a));
        ui.usage_cancels
            .insert("account-b".into(), Arc::clone(&cancel_b));

        cancel_account_usage_locked(&mut ui, "account-a");
        assert!(cancel_a.load(Ordering::Acquire));
        assert!(!cancel_b.load(Ordering::Acquire));
        assert!(!ui.usage_loading.contains("account-a"));
        assert!(ui.usage_loading.contains("account-b"));
        assert_eq!(ui.usage_generations["account-a"], 2);
        assert_eq!(ui.usage_generations["account-b"], 1);

        ui.usage_loading.insert("account-a".into());
        apply_account_usage_result(
            &mut ui,
            AccountUsageResult {
                account_id: "account-a".into(),
                generation: 2,
                state: crate::monitor::AccountUsageState::Failed("temporary outage".into()),
            },
        );
        assert!(matches!(
            ui.usage["account-a"],
            crate::monitor::AccountUsageState::Ready(_)
        ));

        apply_account_usage_result(
            &mut ui,
            AccountUsageResult {
                account_id: "account-b".into(),
                generation: 1,
                state: usage_state_with_sand_percent(40.0),
            },
        );
        assert!(!ui.usage_loading.contains("account-b"));
        assert!(matches!(
            ui.usage["account-b"],
            crate::monitor::AccountUsageState::Ready(_)
        ));
    }

    #[test]
    fn account_usage_result_survives_selection_change() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut ui = AccountUiState {
            accounts: vec![
                crate::providers::cursor::auth::CursorAccountProfile {
                    id: "account-a".into(),
                    label: Some("A".into()),
                    auth: crate::providers::cursor::auth::CursorAuth {
                        access_token: "token-a".into(),
                        refresh_token: None,
                        api_key: None,
                        expires: None,
                        user_id: None,
                        email: Some("a@example.com".into()),
                        source: "test".into(),
                    },
                    active: true,
                },
                crate::providers::cursor::auth::CursorAccountProfile {
                    id: "account-b".into(),
                    label: Some("B".into()),
                    auth: crate::providers::cursor::auth::CursorAuth {
                        access_token: "token-b".into(),
                        refresh_token: None,
                        api_key: None,
                        expires: None,
                        user_id: None,
                        email: Some("b@example.com".into()),
                        source: "test".into(),
                    },
                    active: false,
                },
            ],
            selected: 0,
            ..AccountUiState::default()
        };
        ui.usage_generations.insert("account-a".into(), 1);
        ui.usage_loading.insert("account-a".into());

        // Navigation changes only the cursor. It must not cancel account A's
        // worker or route its result to account B.
        ui.selected = 1;
        apply_account_usage_result(
            &mut ui,
            AccountUsageResult {
                account_id: "account-a".into(),
                generation: 1,
                state: usage_state_with_sand_percent(12.5),
            },
        );
        assert_eq!(ui.selected, 1);
        assert!(!ui.usage_loading.contains("account-a"));
        assert!(matches!(
            ui.usage.get("account-a"),
            Some(crate::monitor::AccountUsageState::Ready(_))
        ));
        assert!(!ui.usage.contains_key("account-b"));
    }

    #[test]
    fn account_list_marks_disk_hydrated_usage_as_cached() {
        let _test_lock = ACCOUNT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let account = crate::providers::cursor::auth::CursorAccountProfile {
            id: "cached-account".into(),
            label: Some("Cached work".into()),
            auth: crate::providers::cursor::auth::CursorAuth {
                access_token: "cached-token".into(),
                refresh_token: None,
                api_key: None,
                expires: None,
                user_id: None,
                email: Some("cached@example.com".into()),
                source: "test".into(),
            },
            active: true,
        };
        let snapshot = usage_state_with_sand_percent(17.0);
        {
            let mut ui = account_ui_lock();
            ui.accounts = vec![account];
            ui.usage.insert("cached-account".into(), snapshot);
            ui.usage_cached.insert("cached-account".into());
            ui.selected = 0;
        }
        let rendered = draw(150, 18, |frame| render_accounts_detail(frame, frame.area()));
        let text = buffer_text(&rendered);
        assert!(text.contains("cached at"), "{text}");
        assert!(text.contains("Cached work"), "{text}");
        *account_ui_lock() = AccountUiState::default();
    }

    #[test]
    fn mock_request_detail_exposes_error_and_capture_fields() {
        let state = mock_state();
        let failed = state
            .recent
            .iter()
            .position(|request| request.request_id == "req-failed-kimi")
            .unwrap();

        let detail = draw(140, 22, |frame| {
            render_request_detail(frame, frame.area(), &state, failed)
        });
        let text = buffer_text(&detail);

        assert!(text.contains("req-failed-kimi"), "{text}");
        assert!(text.contains("upstream connection closed"), "{text}");
        assert!(text.contains("req-failed-kimi.json"), "{text}");
    }

    #[test]
    fn recent_table_uses_error_indicator_at_medium_width() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started("request-1", None, None, EndpointKind::Messages);
        monitor.provider_selected("request-1", "codex", "gpt-5.6-sol", None);
        monitor.request_failed("request-1", Some(502), "upstream unavailable");
        let state = monitor.snapshot();

        let recent = draw(110, 8, |frame| {
            render_recent(frame, frame.area(), &state.recent, 0, true)
        });
        let recent_text = buffer_text(&recent);

        assert!(recent_text.contains("!"), "{recent_text}");
        assert!(!recent_text.contains("Details"), "{recent_text}");
        assert!(
            !recent_text.contains("upstream unavailable"),
            "{recent_text}"
        );
    }

    #[test]
    fn abandoned_request_is_not_rendered_as_an_error_event() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started("request-1", None, None, EndpointKind::Messages);
        monitor.request_abandoned(
            "request-1",
            "Client response stream disconnected before completion",
        );
        let state = monitor.snapshot();

        let recent = draw(110, 8, |frame| {
            render_recent(frame, frame.area(), &state.recent, 0, true)
        });
        let recent_text = buffer_text(&recent);
        assert_eq!(recent_text.matches('!').count(), 1, "{recent_text}");

        let events = draw(100, 8, |frame| {
            render_events(frame, frame.area(), &state.recent)
        });
        let events_text = buffer_text(&events);
        assert!(events_text.contains("No events"), "{events_text}");
    }

    #[test]
    fn recent_table_keeps_detail_text_at_wide_width() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started("request-1", None, None, EndpointKind::Messages);
        monitor.provider_selected("request-1", "codex", "gpt-5.6-sol", None);
        monitor.request_failed("request-1", Some(502), "upstream unavailable");
        let state = monitor.snapshot();

        let recent = draw(180, 8, |frame| {
            render_recent(frame, frame.area(), &state.recent, 0, false)
        });
        let recent_text = buffer_text(&recent);

        assert!(recent_text.contains("Details"), "{recent_text}");
        assert!(
            recent_text.contains("upstream unavailable"),
            "{recent_text}"
        );
    }

    #[test]
    fn request_detail_renders_full_error_text() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started(
            "request-1",
            Some("sess-1".to_string()),
            Some(7),
            EndpointKind::Messages,
        );
        monitor.provider_selected(
            "request-1",
            "codex",
            "gpt-5.6-sol",
            Some("high".to_string()),
        );
        monitor.request_failed("request-1", Some(502), "upstream unavailable");
        let state = monitor.snapshot();

        let detail = draw(120, 20, |frame| {
            render_request_detail(frame, frame.area(), &state, 0)
        });
        let detail_text = buffer_text(&detail);

        assert!(detail_text.contains("Request detail"), "{detail_text}");
        assert!(detail_text.contains("request-1"), "{detail_text}");
        assert!(detail_text.contains("sess-1"), "{detail_text}");
        assert!(
            detail_text.contains("upstream unavailable"),
            "{detail_text}"
        );
    }

    #[test]
    fn events_render_matching_request_rows_without_a_placeholder() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started("request-1", None, None, EndpointKind::Messages);
        monitor.request_failed("request-1", Some(502), "upstream unavailable");
        let state = monitor.snapshot();

        let events = draw(100, 8, |frame| {
            render_events(frame, frame.area(), &state.recent)
        });
        let events_text = buffer_text(&events);
        assert!(events_text.contains("Time"));
        assert!(events_text.contains("502"));
        assert!(events_text.contains("upstream unavailable"));
        assert!(!events_text.contains("No events"));
    }

    #[test]
    fn shutdown_confirmation_can_be_cancelled_before_signalling_server() {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let mut app = MonitorApp {
            listen_url: "http://127.0.0.1:3000".to_string(),
            setup_text: String::new(),
            show_setup: false,
            show_sand_settings: false,
            show_help: false,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 0,
            recent_selected: 0,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: Some(shutdown_tx),
            shutdown_complete: Some(mpsc::channel().1),
            sand_models: Vec::new(),
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: None,
            sand_input: None,
        };

        app.request_shutdown_confirmation();
        assert_eq!(app.phase, MonitorPhase::ConfirmingShutdown);
        assert!(matches!(
            shutdown_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        let state = MonitorHandle::default().snapshot();
        let screen = draw(80, 24, |frame| render(frame, &mut app, &state));
        let text = buffer_text(&screen);
        assert!(text.contains("Shut down proxy?"), "{text}");
        assert!(text.contains("y confirm"), "{text}");
        assert!(text.contains("n/Esc/q cancel"), "{text}");

        app.cancel_shutdown_confirmation();
        assert_eq!(app.phase, MonitorPhase::Running);
        assert!(matches!(
            shutdown_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        app.request_shutdown_confirmation();
        app.begin_shutdown();
        assert_eq!(app.phase, MonitorPhase::ShuttingDown);
        assert_eq!(shutdown_rx.try_recv(), Ok(()));
    }

    #[test]
    fn ctrl_c_starts_shutdown_then_requests_force_quit() {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let (_shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel();
        let mut app = MonitorApp {
            listen_url: "http://127.0.0.1:3000".to_string(),
            setup_text: String::new(),
            show_setup: false,
            show_sand_settings: false,
            show_help: false,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 0,
            recent_selected: 0,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: Some(shutdown_tx),
            shutdown_complete: Some(shutdown_complete_rx),
            sand_models: Vec::new(),
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: None,
            sand_input: None,
        };

        assert!(!app.handle_ctrl_c());
        assert!(app.handle_ctrl_c());

        assert_eq!(app.phase, MonitorPhase::ShuttingDown);
        assert_eq!(shutdown_rx.try_recv(), Ok(()));
        let state = MonitorHandle::default().snapshot();
        let screen = draw(80, 24, |frame| render(frame, &mut app, &state));
        let text = buffer_text(&screen);
        assert!(text.contains("Shutting down..."));
        assert!(text.contains("Press Ctrl-C to force quit"));
    }

    #[test]
    fn shutdown_completion_accepts_notification_and_sender_drop() {
        let (complete_tx, complete_rx) = mpsc::channel();
        let app = MonitorApp {
            listen_url: String::new(),
            setup_text: String::new(),
            show_setup: false,
            show_sand_settings: false,
            show_help: false,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 0,
            recent_selected: 0,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: None,
            shutdown_complete: Some(complete_rx),
            sand_models: Vec::new(),
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: None,
            sand_input: None,
        };

        assert!(!app.shutdown_is_complete());
        complete_tx.send(()).unwrap();
        assert!(app.shutdown_is_complete());

        let (complete_tx, complete_rx) = mpsc::channel();
        let mut app = app;
        app.shutdown_complete = Some(complete_rx);
        drop(complete_tx);
        assert!(app.shutdown_is_complete());
    }

    #[test]
    fn header_renders_configured_listen_url() {
        let app = MonitorApp {
            listen_url: "http://[::]:18765".to_string(),
            setup_text: String::new(),
            show_setup: false,
            show_sand_settings: false,
            show_help: false,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 0,
            recent_selected: 0,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: None,
            shutdown_complete: Some(mpsc::channel().1),
            sand_models: Vec::new(),
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: None,
            sand_input: None,
        };
        let state = MonitorHandle::default().snapshot();

        let header = draw(100, 2, |frame| {
            render_header(frame, frame.area(), &app, &state)
        });
        let text = buffer_text(&header);
        assert!(text.contains("http://[::]:18765"), "{text}");
        assert!(text.contains("usage"), "{text}");
    }

    #[test]
    fn clamp_selection_caps_to_available_sessions() {
        let mut app = MonitorApp {
            listen_url: "http://127.0.0.1:3000".to_string(),
            setup_text: String::new(),
            show_setup: false,
            show_sand_settings: false,
            show_help: false,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 10,
            recent_selected: 10,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: None,
            shutdown_complete: Some(mpsc::channel().1),
            sand_models: Vec::new(),
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: None,
            sand_input: None,
        };

        app.clamp_selection(3, 4);
        assert_eq!(app.selected, 2);
        assert_eq!(app.recent_selected, 3);

        app.clamp_selection(0, 0);
        assert_eq!(app.selected, 0);
        assert_eq!(app.recent_selected, 0);
    }

    #[test]
    fn arrow_navigation_moves_between_focus_panes_at_edges() {
        let mut app = MonitorApp {
            listen_url: "http://127.0.0.1:3000".to_string(),
            setup_text: String::new(),
            show_setup: false,
            show_sand_settings: false,
            show_help: false,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 1,
            recent_selected: 0,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: None,
            shutdown_complete: Some(mpsc::channel().1),
            sand_models: Vec::new(),
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: None,
            sand_input: None,
        };

        app.move_down(2, 3, true);
        assert_eq!(app.focus, FocusPane::Recent);
        assert_eq!(app.recent_selected, 0);

        app.move_up(2, 3, true);
        assert_eq!(app.focus, FocusPane::Sessions);
        assert_eq!(app.selected, 1);
    }

    #[test]
    fn vim_navigation_stays_within_focused_pane() {
        let mut app = MonitorApp {
            listen_url: "http://127.0.0.1:3000".to_string(),
            setup_text: String::new(),
            show_setup: false,
            show_sand_settings: false,
            show_help: false,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 1,
            recent_selected: 0,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: None,
            shutdown_complete: Some(mpsc::channel().1),
            sand_models: Vec::new(),
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: None,
            sand_input: None,
        };

        app.move_down(2, 3, false);
        assert_eq!(app.focus, FocusPane::Sessions);
        assert_eq!(app.selected, 1);

        app.focus = FocusPane::Recent;
        app.move_up(2, 3, false);
        assert_eq!(app.focus, FocusPane::Recent);
        assert_eq!(app.recent_selected, 0);
    }

    #[test]
    fn cursor_model_rows_show_the_selected_client_surface() {
        let monitor = MonitorHandle::new(10);
        monitor.request_started("cursor-request", None, None, EndpointKind::Messages);
        monitor.provider_selected("cursor-request", "cursor", "claude-fable-5", None);
        let state = monitor.snapshot();

        // Wide layout keeps enough room for the model and its route marker.
        let active = draw(180, 8, |frame| {
            render_active(frame, frame.area(), &state.active, 0)
        });
        let text = buffer_text(&active);
        assert!(text.contains("claude-fable-5"), "{text}");
        assert!(
            text.contains("[sand]") || text.contains("[cli]"),
            "Cursor rows must expose the client surface: {text}"
        );
    }

    #[test]
    fn usage_shortcut_remains_available_inside_sand_overlay() {
        let mut app = MonitorApp {
            listen_url: "http://127.0.0.1:18765".to_string(),
            setup_text: String::new(),
            show_setup: true,
            show_sand_settings: true,
            show_help: true,
            detail: None,
            focus: FocusPane::Sessions,
            selected: 0,
            recent_selected: 0,
            tick: 0,
            phase: MonitorPhase::Running,
            shutdown: None,
            shutdown_complete: Some(mpsc::channel().1),
            sand_models: vec!["claude-fable-5".to_string()],
            sand_policy: SandRoutingPolicy::empty(),
            sand_selected: 0,
            sand_message: Some("previous message".to_string()),
            sand_input: None,
        };

        app.handle_sand_key(KeyCode::Char('u'));

        assert!(matches!(app.detail, Some(DetailView::Usage)));
        assert!(!app.show_sand_settings);
        assert!(!app.show_setup);
        assert!(!app.show_help);
        assert!(app.sand_message.is_none());
    }

    fn usage_state_with_sand_percent(percent: f64) -> crate::monitor::AccountUsageState {
        crate::monitor::AccountUsageState::Ready(crate::monitor::AccountUsageSnapshot {
            email: None,
            membership: None,
            auto_percent: Some(5.0),
            api_percent: Some(5.0),
            total_percent: Some(5.0),
            plan_used_usd: None,
            plan_limit_usd: None,
            on_demand_used_usd: None,
            on_demand_limit_usd: None,
            grok_bot_percent: Some(percent),
            grok_bot_period_start: None,
            grok_bot_reset: None,
            total_cost_usd: None,
            usage_event_count: None,
            usage_events: Vec::new(),
            fetched_at: SystemTime::now(),
        })
    }

    #[test]
    fn usage_header_color_includes_the_sand_quota() {
        assert_eq!(
            usage_header_color(&usage_state_with_sand_percent(69.9)),
            DIM_WHITE
        );
        assert_eq!(
            usage_header_color(&usage_state_with_sand_percent(70.0)),
            YELLOW
        );
        assert_eq!(
            usage_header_color(&usage_state_with_sand_percent(90.0)),
            YELLOW
        );
        assert_eq!(
            usage_header_color(&crate::monitor::AccountUsageState::Failed(
                "dashboard unavailable".to_string(),
            )),
            RED
        );
    }
}
