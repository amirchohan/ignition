use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
};

use super::{
    commands::{RequestIdSource, parse_command},
    config::{ControlClientConfig, ControlClientConfigError},
    heartbeat::HeartbeatRuntime,
    io::{ControlClientIoError, ControlEndpointClient},
    protocol::{ClientControlEvent, ClientHeartbeatStatus, ClientSnapshotBegin, ClientSnapshotEnd, ClientSnapshotOrder},
};

pub fn run_control_client(config: ControlClientConfig) -> Result<(), ControlClientError> {
    let mut control = ControlEndpointClient::connect(&config.control_addr)?;
    let mut terminal = TerminalGuard::enter()?;
    let mut app = ControlClientApp::new(config);
    let mut events = Vec::with_capacity(128);
    let tick = Duration::from_millis(25);
    let mut last_draw = Instant::now();

    loop {
        let now = Instant::now();
        if let Some(frame) = app.heartbeat.poll_due(now) {
            match control.send(frame) {
                Ok(()) => app.push_status("heartbeat sent".to_owned()),
                Err(error) => app.push_status(format!("heartbeat send failed: {error}")),
            }
        }

        events.clear();
        match control.poll(&mut events) {
            Ok(_) => {}
            Err(error) => app.push_status(format!("control: {error}")),
        }
        for event in events.drain(..) {
            app.apply_event(event);
        }

        while event::poll(Duration::from_millis(1)).map_err(ControlClientError::Terminal)? {
            let Event::Key(key) = event::read().map_err(ControlClientError::Terminal)? else {
                continue;
            };
            if app.on_key(key, &mut control)? {
                return Ok(());
            }
        }

        if last_draw.elapsed() >= tick {
            terminal.draw(|frame| draw(frame, &app)).map_err(ControlClientError::Terminal)?;
            last_draw = Instant::now();
        }
    }
}

#[derive(Debug)]
struct ControlClientApp {
    config: ControlClientConfig,
    input: String,
    heartbeat: HeartbeatRuntime,
    request_ids: RequestIdSource,
    statuses: VecDeque<String>,
    responses: VecDeque<String>,
    snapshot_begin: Option<ClientSnapshotBegin>,
    snapshot_end: Option<ClientSnapshotEnd>,
    snapshot_rows: VecDeque<ClientSnapshotOrder>,
}

impl ControlClientApp {
    fn new(config: ControlClientConfig) -> Self {
        Self {
            heartbeat: HeartbeatRuntime::new(Duration::from_millis(config.heartbeat_interval_ms), Instant::now()),
            config,
            input: String::new(),
            request_ids: RequestIdSource::new(1),
            statuses: VecDeque::with_capacity(64),
            responses: VecDeque::with_capacity(128),
            snapshot_begin: None,
            snapshot_end: None,
            snapshot_rows: VecDeque::with_capacity(256),
        }
    }

    fn on_key(&mut self, key: KeyEvent, control: &mut ControlEndpointClient) -> Result<bool, ControlClientError> {
        match key.code {
            KeyCode::Esc => return Ok(true),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
            KeyCode::Char(ch) => self.input.push(ch),
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Enter => {
                let command = self.input.trim().to_owned();
                self.input.clear();
                if command.is_empty() {
                    return Ok(false);
                }
                match parse_command(&command, &mut self.request_ids) {
                    Ok(Some(frame)) => {
                        control.send(frame)?;
                        self.push_status(format!("sent {command}"));
                    }
                    Ok(None) => {}
                    Err(error) => self.push_status(error.to_string()),
                }
            }
            _ => {}
        }
        Ok(false)
    }

    fn apply_event(&mut self, event: ClientControlEvent) {
        match event {
            ClientControlEvent::ControlResponse(response) => {
                push_bounded(
                    &mut self.responses,
                    format!(
                        "seq={} request={} result={} reason={}",
                        response.engine_seq, response.request_id, response.result, response.reason
                    ),
                    100,
                );
            }
            ClientControlEvent::HeartbeatStatus(status) => self.apply_heartbeat_status(status),
            ClientControlEvent::SnapshotBegin(begin) => {
                let request_id = begin.request_id;
                self.snapshot_begin = Some(begin);
                self.snapshot_end = None;
                self.snapshot_rows.clear();
                self.push_status(format!("snapshot {request_id} begin"));
            }
            ClientControlEvent::SnapshotOrder(row) => {
                push_bounded(&mut self.snapshot_rows, row, self.config.max_snapshot_rows);
            }
            ClientControlEvent::SnapshotEnd(end) => {
                let request_id = end.request_id;
                let row_count = end.row_count;
                let result = end.result;
                self.snapshot_end = Some(end);
                self.push_status(format!("snapshot {request_id} end rows={row_count} result={result}"));
            }
            ClientControlEvent::ProtocolError(error) => {
                self.push_status(format!(
                    "protocol error seq={} template={} code={}",
                    error.engine_seq, error.template_id, error.error_code
                ));
            }
        }
    }

    fn apply_heartbeat_status(&mut self, status: ClientHeartbeatStatus) {
        self.heartbeat.on_ack(status.heartbeat_seq, Instant::now());
        push_bounded(
            &mut self.responses,
            format!(
                "heartbeat seq={} engine_seq={} result={}",
                status.heartbeat_seq, status.engine_seq, status.result
            ),
            100,
        );
    }

    fn push_status(&mut self, status: String) {
        push_bounded(&mut self.statuses, status, 30);
    }
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &ControlClientApp) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(8),
            Constraint::Min(12),
            Constraint::Length(3),
        ])
        .split(frame.area());
    draw_header(frame, root[0], app);
    draw_top_row(frame, root[1], app);
    draw_snapshot(frame, root[2], app);
    draw_input(frame, root[3], app);
}

fn draw_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &ControlClientApp) {
    let ack = app
        .heartbeat
        .last_ack()
        .map(|(seq, _, rtt)| format!("last ack seq={seq} rtt={}ms", rtt.as_millis()))
        .unwrap_or_else(|| "last ack -".to_owned());
    let line = Line::from(vec![
        Span::styled(
            "Ignition control client",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::raw(format!("endpoint {}", app.config.control_addr)),
        Span::raw("  "),
        Span::raw(ack),
        Span::raw("  Esc/Ctrl-C exits"),
    ]);
    frame.render_widget(Paragraph::new(line).block(Block::default().borders(Borders::ALL)), area);
}

fn draw_top_row(frame: &mut ratatui::Frame<'_>, area: Rect, app: &ControlClientApp) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(25), Constraint::Percentage(40)])
        .split(area);
    draw_heartbeat(frame, chunks[0], app);
    draw_commands(frame, chunks[1]);
    draw_responses(frame, chunks[2], app);
}

fn draw_heartbeat(frame: &mut ratatui::Frame<'_>, area: Rect, app: &ControlClientApp) {
    let last_sent = app
        .heartbeat
        .last_sent()
        .map(|(seq, at)| format!("seq={} {}ms ago", seq, at.elapsed().as_millis()))
        .unwrap_or_else(|| "-".to_owned());
    let last_ack = app
        .heartbeat
        .last_ack()
        .map(|(seq, at, rtt)| format!("seq={} {}ms ago rtt={}ms", seq, at.elapsed().as_millis(), rtt.as_millis()))
        .unwrap_or_else(|| "-".to_owned());
    let text = format!(
        "interval: {}ms\nnext seq: {}\npending: {}\nlast sent: {}\nlast ack: {}",
        app.config.heartbeat_interval_ms,
        app.heartbeat.next_seq(),
        app.heartbeat.pending_count(),
        last_sent,
        last_ack
    );
    frame.render_widget(
        Paragraph::new(text).block(Block::default().title("Heartbeat").borders(Borders::ALL)),
        area,
    );
}

fn draw_commands(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let text = "halt\nresume\nshutdown\nsnapshot";
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .block(Block::default().title("Commands").borders(Borders::ALL)),
        area,
    );
}

fn draw_responses(frame: &mut ratatui::Frame<'_>, area: Rect, app: &ControlClientApp) {
    let text = app
        .responses
        .iter()
        .rev()
        .take(area.height.saturating_sub(2) as usize)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .block(Block::default().title("Responses").borders(Borders::ALL)),
        area,
    );
}

fn draw_snapshot(frame: &mut ratatui::Frame<'_>, area: Rect, app: &ControlClientApp) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(72), Constraint::Percentage(28)])
        .split(area);
    draw_snapshot_rows(frame, chunks[0], app);
    draw_snapshot_status(frame, chunks[1], app);
}

fn draw_snapshot_rows(frame: &mut ratatui::Frame<'_>, area: Rect, app: &ControlClientApp) {
    let rows = app.snapshot_rows.iter().take(area.height.saturating_sub(3) as usize).map(|row| {
        Row::new(vec![
            Cell::from(row.row_index.to_string()),
            Cell::from(row.side.to_string()),
            Cell::from(row.client_order_id.to_string()),
            Cell::from(row.account_id.to_string()),
            Cell::from(row.price.to_string()),
            Cell::from(row.remaining_qty.to_string()),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(14),
            Constraint::Length(9),
            Constraint::Length(14),
            Constraint::Length(14),
        ],
    )
    .header(Row::new(["row", "side", "order", "account", "price", "qty"]).style(Style::default().add_modifier(Modifier::BOLD)))
    .block(Block::default().title("Snapshot Orders").borders(Borders::ALL));
    frame.render_widget(table, area);
}

fn draw_snapshot_status(frame: &mut ratatui::Frame<'_>, area: Rect, app: &ControlClientApp) {
    let summary = app
        .snapshot_begin
        .as_ref()
        .map(|begin| {
            format!(
                "request: {}\nresting: {}\nbid: {} x {}\nask: {} x {}\nlast md seq: {}\nnext trade id: {}\nrisk ref: {}",
                begin.request_id,
                begin.resting_order_count,
                display_optional(begin.bid_price),
                display_qty(begin.bid_qty),
                display_optional(begin.ask_price),
                display_qty(begin.ask_qty),
                begin.last_md_seq,
                begin.next_trade_id,
                begin.risk_reference_price
            )
        })
        .unwrap_or_else(|| "no snapshot".to_owned());
    let end = app
        .snapshot_end
        .as_ref()
        .map(|end| format!("\n\nend rows={} result={} reason={}", end.row_count, end.result, end.reason))
        .unwrap_or_default();
    let status_text = app
        .statuses
        .iter()
        .rev()
        .take(area.height.saturating_sub(12) as usize)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    let text = format!("{summary}{end}\n\n{status_text}");
    frame.render_widget(
        Paragraph::new(text)
            .wrap(Wrap { trim: true })
            .block(Block::default().title("Status").borders(Borders::ALL)),
        area,
    );
}

fn draw_input(frame: &mut ratatui::Frame<'_>, area: Rect, app: &ControlClientApp) {
    let text = format!("> {}", app.input);
    frame.render_widget(
        Paragraph::new(text).block(Block::default().title("Command").borders(Borders::ALL)),
        area,
    );
}

fn display_optional(raw: u64) -> String {
    if raw == 0 { "-".to_owned() } else { raw.to_string() }
}

fn display_qty(raw: u64) -> String {
    if raw == 0 { "-".to_owned() } else { raw.to_string() }
}

fn push_bounded<T>(values: &mut VecDeque<T>, value: T, max: usize) {
    if values.len() == max {
        values.pop_front();
    }
    values.push_back(value);
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
}

impl TerminalGuard {
    fn enter() -> Result<Self, ControlClientError> {
        enable_raw_mode().map_err(ControlClientError::Terminal)?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen).map_err(ControlClientError::Terminal)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).map_err(ControlClientError::Terminal)?;
        Ok(Self { terminal })
    }

    fn draw<F>(&mut self, f: F) -> Result<(), std::io::Error>
    where
        F: FnOnce(&mut ratatui::Frame<'_>),
    {
        self.terminal.draw(f).map(|_| ())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Debug)]
pub enum ControlClientError {
    Config(ControlClientConfigError),
    Io(ControlClientIoError),
    Terminal(std::io::Error),
}

impl std::fmt::Display for ControlClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{error}"),
            Self::Io(error) => write!(f, "{error}"),
            Self::Terminal(error) => write!(f, "terminal error: {error}"),
        }
    }
}

impl std::error::Error for ControlClientError {}

impl From<ControlClientConfigError> for ControlClientError {
    fn from(error: ControlClientConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<ControlClientIoError> for ControlClientError {
    fn from(error: ControlClientIoError) -> Self {
        Self::Io(error)
    }
}
