use std::{
    io::{self, Stdout},
    time::{Duration, SystemTime},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Cell, Clear, Paragraph, Row, Table, Wrap},
};
use tokio::sync::oneshot;

use crate::{
    monitor::{ActiveRequest, CompletedRequest, MonitorHandle, MonitorState, SessionSummary},
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
const DIM: Color = Color::Rgb(100, 104, 114);

pub struct MonitorUiConfig<'a> {
    pub port: u16,
    pub registry: &'a Registry,
    pub shutdown: Option<oneshot::Sender<()>>,
}

pub fn run_monitor(
    handle: MonitorHandle,
    config: MonitorUiConfig<'_>,
) -> Result<(), anyhow::Error> {
    let mut terminal = setup_terminal()?;
    let _guard = TerminalGuard;
    let mut app = MonitorApp {
        port: config.port,
        setup_text: setup_text(config.port, config.registry),
        show_setup: true,
        show_help: false,
        detail: false,
        selected: 0,
        shutdown: config.shutdown,
    };

    loop {
        let state = handle.snapshot();
        terminal.draw(|frame| render(frame, &mut app, &state))?;
        if event::poll(Duration::from_millis(250))? {
            match event::read()? {
                Event::Key(key) => match key.code {
                    KeyCode::Char('q') | KeyCode::Char('c')
                        if key.modifiers.contains(KeyModifiers::CONTROL) =>
                    {
                        if let Some(shutdown) = app.shutdown.take() {
                            let _ = shutdown.send(());
                        }
                        break;
                    }
                    KeyCode::Char('?') => app.show_help = !app.show_help,
                    KeyCode::Char('b') => app.show_setup = !app.show_setup,
                    KeyCode::Down | KeyCode::Char('j') => {
                        app.selected = app.selected.saturating_add(1)
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        app.selected = app.selected.saturating_sub(1)
                    }
                    KeyCode::Enter => app.detail = true,
                    KeyCode::Esc => app.detail = false,
                    _ => {}
                },
                Event::Resize(_, _) => {}
                _ => {}
            }
        }
    }
    terminal.show_cursor()?;
    Ok(())
}

struct MonitorApp {
    port: u16,
    setup_text: String,
    show_setup: bool,
    show_help: bool,
    detail: bool,
    selected: usize,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Drop for MonitorApp {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
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

    let outer = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(SEPARATOR))
        .style(Style::default().bg(BG));
    let inner = outer.inner(area);
    frame.render_widget(outer, area);

    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            if app.show_setup {
                Constraint::Length(8)
            } else {
                Constraint::Length(0)
            },
            Constraint::Percentage(40),
            Constraint::Percentage(25),
            Constraint::Percentage(35),
            Constraint::Length(1),
        ])
        .split(inner);

    render_header(frame, root[0], app, state);
    if app.show_setup {
        frame.render_widget(
            Paragraph::new(app.setup_text.as_str())
                .style(Style::default().fg(DIM_WHITE).bg(PANEL_BG))
                .block(panel("Setup"))
                .wrap(Wrap { trim: false }),
            root[1],
        );
    }
    if app.detail {
        render_session_detail(frame, root[2], state, app.selected);
    } else {
        render_sessions(frame, root[2], &state.sessions, app.selected);
    }
    render_active(frame, root[3], &state.active);
    render_recent(frame, root[4], &state.recent);
    render_footer(frame, root[5], app);

    if app.show_help {
        render_help_overlay(frame, area);
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
    let text = Line::from(vec![
        Span::styled(
            " claude-code-proxy",
            Style::default().fg(TEAL).add_modifier(Modifier::BOLD),
        ),
        Span::styled("  ", Style::default().fg(DIM)),
        Span::styled(
            format!("http://127.0.0.1:{}", app.port),
            Style::default().fg(DIM_WHITE),
        ),
        Span::styled("  uptime ", Style::default().fg(DIM)),
        Span::styled(format_duration(uptime), Style::default().fg(WHITE)),
        Span::styled("  sessions ", Style::default().fg(DIM)),
        Span::styled(
            state.sessions.len().to_string(),
            Style::default().fg(if state.sessions.is_empty() {
                DIM
            } else {
                GREEN
            }),
        ),
        Span::styled("  active ", Style::default().fg(DIM)),
        Span::styled(
            state.active.len().to_string(),
            Style::default().fg(if state.active.is_empty() { DIM } else { YELLOW }),
        ),
    ]);
    frame.render_widget(Paragraph::new(text).style(Style::default().bg(BG)), area);
}

fn panel(title: &'static str) -> Block<'static> {
    Block::default()
        .title(Span::styled(
            format!(" {title} "),
            Style::default().fg(DIM_WHITE),
        ))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(SEPARATOR))
        .style(Style::default().bg(PANEL_BG))
}

fn table_header(cells: impl IntoIterator<Item = &'static str>) -> Row<'static> {
    Row::new(
        cells
            .into_iter()
            .map(|cell| Cell::from(Span::styled(cell, Style::default().fg(TEAL))))
            .collect::<Vec<_>>(),
    )
    .style(Style::default().add_modifier(Modifier::BOLD))
}

fn muted_cell(value: impl Into<String>) -> Cell<'static> {
    Cell::from(Span::styled(value.into(), Style::default().fg(DIM)))
}

fn text_cell(value: impl Into<String>) -> Cell<'static> {
    Cell::from(Span::styled(value.into(), Style::default().fg(DIM_WHITE)))
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
        "completed" | "streaming" => GREEN,
        "failed" => RED,
        "upstream" | "selected" | "started" => YELLOW,
        _ => DIM_WHITE,
    }
}

fn http_status_style(status: Option<u16>) -> Style {
    match status {
        Some(200..=299) => Style::default().fg(GREEN),
        Some(400..=499) => Style::default().fg(YELLOW),
        Some(500..=599) => Style::default().fg(RED),
        Some(_) => Style::default().fg(DIM_WHITE),
        None => Style::default().fg(DIM),
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

fn compact_tokens(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}M", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn token_pair(input: Option<u64>, output: Option<u64>) -> String {
    match (input, output) {
        (Some(input), Some(output)) => {
            format!("{}/{}", compact_tokens(input), compact_tokens(output))
        }
        (Some(input), None) => compact_tokens(input),
        (None, Some(output)) => compact_tokens(output),
        (None, None) => "-".to_string(),
    }
}

fn render_sessions(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    sessions: &[SessionSummary],
    selected: usize,
) {
    let rows = sessions.iter().enumerate().map(|(index, session)| {
        let marker = if index == selected { ">" } else { " " };
        Row::new(vec![
            Cell::from(Span::styled(marker, Style::default().fg(TEAL))),
            text_cell(session.label()),
            number_cell(session.active_count.to_string()),
            number_cell(session.request_count.to_string()),
            number_cell(session.failure_count.to_string()),
            provider_cell(session.provider.as_deref()),
            text_cell(session.model.as_deref().unwrap_or("-")),
            number_cell(format!(
                "{}/{}",
                compact_tokens(session.input_tokens),
                compact_tokens(session.output_tokens)
            )),
            rate_cell(session.rate().label()),
            status_cell(&session.last_status),
        ])
        .style(if index == selected {
            Style::default().bg(SELECTED_BG)
        } else {
            Style::default().bg(PANEL_BG)
        })
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(1),
            Constraint::Length(36),
            Constraint::Length(6),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(10),
            Constraint::Percentage(20),
            Constraint::Length(13),
            Constraint::Length(12),
            Constraint::Length(10),
        ],
    )
    .header(table_header([
        "", "session", "active", "reqs", "fail", "provider", "model", "tokens", "rate", "status",
    ]))
    .block(panel("Sessions"));
    frame.render_widget(table, area);
}

fn render_active(frame: &mut ratatui::Frame<'_>, area: Rect, active: &[ActiveRequest]) {
    let rows = active.iter().map(|request| {
        Row::new(vec![
            muted_cell(format_system_time(request.started_at)),
            provider_cell(request.provider.as_deref()),
            text_cell(request.model.as_deref().unwrap_or("-")),
            muted_cell(request.endpoint.label()),
            status_cell(request.status.label()),
            rate_cell(request.rate().label()),
            number_cell(format_duration(request.elapsed())),
        ])
        .style(Style::default().bg(PANEL_BG))
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Percentage(24),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(9),
        ],
    )
    .header(table_header([
        "started", "provider", "model", "endpoint", "status", "rate", "elapsed",
    ]))
    .block(panel("Active requests"));
    frame.render_widget(table, area);
}

fn render_recent(frame: &mut ratatui::Frame<'_>, area: Rect, recent: &[CompletedRequest]) {
    let rows = recent.iter().map(|request| {
        Row::new(vec![
            muted_cell(format_system_time(request.finished_at)),
            Cell::from(Span::styled(
                request
                    .http_status
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                http_status_style(request.http_status),
            )),
            provider_cell(request.provider.as_deref()),
            text_cell(request.model.as_deref().unwrap_or("-")),
            number_cell(format_duration(request.latency)),
            rate_cell(request.rate().label()),
            number_cell(token_pair(request.input_tokens, request.output_tokens)),
            muted_cell(request.error.as_deref().unwrap_or("")),
        ])
        .style(Style::default().bg(PANEL_BG))
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(10),
            Constraint::Percentage(20),
            Constraint::Length(9),
            Constraint::Length(12),
            Constraint::Length(11),
            Constraint::Percentage(24),
        ],
    )
    .header(table_header([
        "finished", "status", "provider", "model", "latency", "rate", "tokens", "error",
    ]))
    .block(panel("Recent requests"));
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
            detail_line("active requests", session.active_count.to_string(), YELLOW),
            detail_line(
                "total requests",
                session.request_count.to_string(),
                DIM_WHITE,
            ),
            detail_line("failures", session.failure_count.to_string(), RED),
            detail_line("provider", session.provider.as_deref().unwrap_or("-"), TEAL),
            detail_line("model", session.model.as_deref().unwrap_or("-"), DIM_WHITE),
            detail_line(
                "tokens",
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
            .block(panel("Session detail")),
        area,
    );
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
        Span::styled("j/k", Style::default().fg(TEAL)),
        Span::styled(" select  ", Style::default().fg(DIM)),
        Span::styled("Enter", Style::default().fg(TEAL)),
        Span::styled(" session", Style::default().fg(DIM)),
    ];
    frame.render_widget(
        Paragraph::new(Line::from(spans)).style(Style::default().bg(BG)),
        area,
    );
}

fn render_help_overlay(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let width = 48.min(area.width.saturating_sub(4)).max(24);
    let height = 12.min(area.height.saturating_sub(2)).max(8);
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
        ("b", "toggle setup panel"),
        ("j / Down", "next session"),
        ("k / Up", "previous session"),
        ("Enter", "session detail"),
        ("Esc", "leave detail"),
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

pub fn setup_text(port: u16, registry: &Registry) -> String {
    let mut lines = vec![
        format!("Logs: {}", paths::log_file().display()),
        format!("Config: {}", paths::config_dir().display()),
    ];
    for provider in ["codex", "kimi", "cursor"] {
        if let Some(models) = registry.grouped_models().get(provider) {
            lines.push(format!("{provider}: {}", models.join(", ")));
        }
    }
    lines.push(format!(
        "export ANTHROPIC_BASE_URL=\"http://localhost:{port}\""
    ));
    lines.push("export ANTHROPIC_AUTH_TOKEN=\"anything\"".to_string());
    lines.push("export ANTHROPIC_MODEL=\"gpt-5.5\"".to_string());
    lines.push("export ANTHROPIC_SMALL_FAST_MODEL=\"gpt-5.4-mini\"".to_string());
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

fn format_system_time(time: SystemTime) -> String {
    let Ok(duration) = time.duration_since(SystemTime::UNIX_EPOCH) else {
        return "-".to_string();
    };
    let seconds = duration.as_secs() % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds % 3600) / 60,
        seconds % 60
    )
}
