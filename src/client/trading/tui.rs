use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::mpsc::{self, Receiver},
    thread,
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

use crate::{domain::rd::InstrumentSpec, protocol::generated::codecs::book_side};

use super::{
    commands::{ClientIdSource, CommandContext, format_scaled, parse_command},
    config::{SyntheticTrafficConfig, TradingClientConfig, TradingClientConfigError},
    io::{GatewayClient, MarketDataClient, TradingClientIoError},
    protocol::{ClientExecutionReport, ClientInboundEvent, ClientOrderFrame, ClientTradePrint},
    synthetic_traffic::SyntheticTraffic,
};

pub fn run_trading_client(config: TradingClientConfig) -> Result<(), TradingClientError> {
    let instrument = config.instrument().map_err(TradingClientError::ServerConfig)?;
    let mut gateway = GatewayClient::connect(&config.gateway_addr)?;
    let mut market_data = MarketDataClient::bind(&config.market_data.bind_addr, &config.market_data.group_addr)?;
    let synthetic_traffic_rx = spawn_synthetic_traffic(&config, instrument);

    let mut terminal = TerminalGuard::enter()?;
    let mut app = TradingClientApp::new(config, instrument);
    let mut events = Vec::with_capacity(256);
    let mut pending_synthetic_traffic = Vec::with_capacity(64);
    let tick = Duration::from_millis(25);
    let mut last_draw = Instant::now();

    loop {
        drain_synthetic_traffic(&synthetic_traffic_rx, &mut pending_synthetic_traffic, &mut app);
        for frame in pending_synthetic_traffic.drain(..) {
            if let Err(error) = gateway.send(frame) {
                app.push_status(format!("synthetic traffic send failed: {error}"));
            }
        }

        events.clear();
        match gateway.poll(&mut events) {
            Ok(_) => {}
            Err(error) => app.push_status(format!("gateway: {error}")),
        }
        match market_data.poll(&mut events) {
            Ok(_) => {}
            Err(error) => app.push_status(format!("market data: {error}")),
        }
        for event in events.drain(..) {
            app.apply_event(event);
        }

        while event::poll(Duration::from_millis(1)).map_err(TradingClientError::Terminal)? {
            let Event::Key(key) = event::read().map_err(TradingClientError::Terminal)? else {
                continue;
            };
            if app.on_key(key, &mut gateway)? {
                return Ok(());
            }
        }

        if last_draw.elapsed() >= tick {
            terminal.draw(|frame| draw(frame, &app)).map_err(TradingClientError::Terminal)?;
            last_draw = Instant::now();
        }
    }
}

#[derive(Debug)]
struct TradingClientApp {
    instrument: InstrumentSpec,
    input: String,
    statuses: VecDeque<String>,
    all_reports: VecDeque<ClientExecutionReport>,
    manual_reports: VecDeque<ClientExecutionReport>,
    manual_report_order_ids: BTreeSet<u64>,
    report_filter: ReportFilter,
    trades: VecDeque<ClientTradePrint>,
    bids: BTreeMap<u64, u64>,
    asks: BTreeMap<u64, u64>,
    l1: Option<(u64, u64, u64, u64)>,
    ids: ClientIdSource,
    synthetic_traffic_status: String,
}

impl TradingClientApp {
    fn new(config: TradingClientConfig, instrument: InstrumentSpec) -> Self {
        let synthetic_traffic_status = match config.synthetic_traffic {
            SyntheticTrafficConfig::Noop => "synthetic traffic noop".to_owned(),
            SyntheticTrafficConfig::Binance { .. } => "binance starting".to_owned(),
        };
        Self {
            instrument,
            input: String::new(),
            statuses: VecDeque::with_capacity(64),
            all_reports: VecDeque::with_capacity(128),
            manual_reports: VecDeque::with_capacity(128),
            manual_report_order_ids: BTreeSet::new(),
            report_filter: ReportFilter::All,
            trades: VecDeque::with_capacity(128),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            l1: None,
            ids: ClientIdSource::new(1),
            synthetic_traffic_status,
        }
    }

    fn on_key(&mut self, key: KeyEvent, gateway: &mut GatewayClient) -> Result<bool, TradingClientError> {
        match key.code {
            KeyCode::Esc => return Ok(true),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
            KeyCode::F(2) => {
                self.report_filter = self.report_filter.toggle();
                self.push_status(format!("execution reports: {}", self.report_filter.label()));
            }
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
                let context = CommandContext {
                    instrument: self.instrument,
                };
                match parse_command(&command, context, &mut self.ids) {
                    Ok(Some(frame)) => {
                        self.track_manual_frame(frame);
                        gateway.send(frame)?;
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

    fn apply_event(&mut self, event: ClientInboundEvent) {
        match event {
            ClientInboundEvent::ExecutionReport(report) => {
                if self.manual_report_order_ids.contains(&report.client_order_id) {
                    push_bounded(&mut self.manual_reports, report.clone(), 100);
                }
                push_bounded(&mut self.all_reports, report, 100);
            }
            ClientInboundEvent::TradePrint(trade) => push_bounded(&mut self.trades, trade, 100),
            ClientInboundEvent::L1Update(update) => {
                self.l1 = Some((update.bid_price, update.bid_qty, update.ask_price, update.ask_qty));
            }
            ClientInboundEvent::L2Update(update) => {
                let book = match update.side {
                    book_side::BookSide::Bid => &mut self.bids,
                    book_side::BookSide::Ask => &mut self.asks,
                    book_side::BookSide::NullVal => return,
                };
                if update.qty == 0 {
                    book.remove(&update.price);
                } else {
                    book.insert(update.price, update.qty);
                }
            }
            ClientInboundEvent::ProtocolError(error) => {
                self.push_status(format!(
                    "protocol error seq={} template={} code={}",
                    error.engine_seq, error.template_id, error.error_code
                ));
            }
        }
    }

    fn push_status(&mut self, status: String) {
        push_bounded(&mut self.statuses, status, 20);
    }

    fn track_manual_frame(&mut self, frame: ClientOrderFrame) {
        match frame {
            ClientOrderFrame::New(order) => {
                self.manual_report_order_ids.insert(order.client_order_id);
            }
            ClientOrderFrame::Cancel(cancel) => {
                self.manual_report_order_ids.insert(cancel.client_order_id);
                self.manual_report_order_ids.insert(cancel.orig_client_order_id);
            }
            ClientOrderFrame::Replace(replace) => {
                self.manual_report_order_ids.insert(replace.client_order_id);
                self.manual_report_order_ids.insert(replace.orig_client_order_id);
            }
            ClientOrderFrame::MassCancel(cancel) => {
                self.manual_report_order_ids.insert(cancel.client_order_id);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReportFilter {
    All,
    Manual,
}

impl ReportFilter {
    const fn toggle(self) -> Self {
        match self {
            Self::All => Self::Manual,
            Self::Manual => Self::All,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Manual => "manual",
        }
    }
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &TradingClientApp) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(12),
            Constraint::Length(7),
            Constraint::Length(3),
        ])
        .split(frame.area());
    draw_status(frame, root[0], app);
    draw_market_row(frame, root[1], app);
    draw_reports(frame, root[2], app);
    draw_input(frame, root[3], app);
}

fn draw_status(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TradingClientApp) {
    let l1 = app
        .l1
        .map(|(bp, bq, ap, aq)| {
            format!(
                "bid {} x {} | ask {} x {}",
                display_price(app, bp),
                display_qty(app, bq),
                display_price(app, ap),
                display_qty(app, aq)
            )
        })
        .unwrap_or_else(|| "top of book empty".to_owned());
    let line = Line::from(vec![
        Span::styled(
            "Ignition trading client",
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::raw(app.synthetic_traffic_status.as_str()),
        Span::raw("  "),
        Span::raw(l1),
        Span::raw("  "),
        Span::raw(format!("reports {}", app.report_filter.label())),
        Span::raw("  F2 filter  Esc/Ctrl-C exits"),
    ]);
    frame.render_widget(Paragraph::new(line).block(Block::default().borders(Borders::ALL)), area);
}

fn draw_market_row(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TradingClientApp) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(42), Constraint::Percentage(30), Constraint::Percentage(28)])
        .split(area);
    draw_book(frame, chunks[0], app);
    draw_trades(frame, chunks[1], app);
    draw_right_rail(frame, chunks[2], app);
}

fn draw_right_rail(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TradingClientApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);
    draw_commands(frame, chunks[0]);
    draw_status_log(frame, chunks[1], app);
}

fn draw_book(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TradingClientApp) {
    let depth = area.height.saturating_sub(3) as usize;
    let asks = app.asks.iter().take(depth / 2).rev().map(|(price, qty)| {
        Row::new(vec![
            Cell::from("ask").style(Style::default().fg(Color::Red)),
            Cell::from(display_price(app, *price)),
            Cell::from(display_qty(app, *qty)),
        ])
    });
    let bids = app.bids.iter().rev().take(depth / 2).map(|(price, qty)| {
        Row::new(vec![
            Cell::from("bid").style(Style::default().fg(Color::Green)),
            Cell::from(display_price(app, *price)),
            Cell::from(display_qty(app, *qty)),
        ])
    });
    let rows = asks.chain(bids);
    let table = Table::new(
        rows,
        [Constraint::Length(5), Constraint::Percentage(45), Constraint::Percentage(45)],
    )
    .header(Row::new(["side", "price", "qty"]).style(Style::default().add_modifier(Modifier::BOLD)))
    .block(Block::default().title("Order Book").borders(Borders::ALL));
    frame.render_widget(table, area);
}

fn draw_trades(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TradingClientApp) {
    let rows = app.trades.iter().rev().take(area.height.saturating_sub(3) as usize).map(|trade| {
        Row::new(vec![
            Cell::from(trade.trade_id.to_string()),
            Cell::from(display_price(app, trade.price)),
            Cell::from(display_qty(app, trade.qty)),
        ])
    });
    let table = Table::new(
        rows,
        [Constraint::Length(10), Constraint::Percentage(45), Constraint::Percentage(45)],
    )
    .header(Row::new(["trade", "price", "qty"]).style(Style::default().add_modifier(Modifier::BOLD)))
    .block(Block::default().title("Recent Trades").borders(Borders::ALL));
    frame.render_widget(table, area);
}

fn draw_reports(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TradingClientApp) {
    let reports = match app.report_filter {
        ReportFilter::All => &app.all_reports,
        ReportFilter::Manual => &app.manual_reports,
    };
    let rows = reports.iter().rev().take(area.height.saturating_sub(3) as usize).map(|report| {
        Row::new(vec![
            Cell::from(report.engine_seq.to_string()),
            Cell::from(report.client_order_id.to_string()),
            Cell::from(report.account_id.to_string()),
            Cell::from(report.exec_type.to_string()),
            Cell::from(report.ord_status.to_string()),
            Cell::from(report.reject_reason.to_string()),
            Cell::from(display_price(app, report.fill_price)),
            Cell::from(display_qty(app, report.fill_qty)),
            Cell::from(display_qty(app, report.remaining_qty)),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(7),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Length(18),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(12),
        ],
    )
    .header(
        Row::new(["seq", "order", "account", "exec", "status", "reason", "price", "qty", "leaves"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title(format!("Execution Reports ({}) | F2 toggles all/manual", app.report_filter.label()))
            .borders(Borders::ALL),
    );
    frame.render_widget(table, area);
}

fn draw_status_log(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TradingClientApp) {
    let status_text = app
        .statuses
        .iter()
        .rev()
        .take(area.height.saturating_sub(2) as usize)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    frame.render_widget(
        Paragraph::new(status_text)
            .wrap(Wrap { trim: true })
            .block(Block::default().title("Status").borders(Borders::ALL)),
        area,
    );
}

fn draw_commands(frame: &mut ratatui::Frame<'_>, area: Rect) {
    let commands = [
        "buy <price> <qty> [gtc|ioc|post] <account>",
        "sell <price> <qty> [gtc|ioc|post] <account>",
        "market buy|sell <qty> <account>",
        "cancel <orig_order_id> <account>",
        "replace <orig_order_id> <price> <qty> <account>",
        "mass-cancel <account>",
    ]
    .join("\n");
    frame.render_widget(
        Paragraph::new(commands)
            .wrap(Wrap { trim: true })
            .block(Block::default().title("Commands").borders(Borders::ALL)),
        area,
    );
}

fn draw_input(frame: &mut ratatui::Frame<'_>, area: Rect, app: &TradingClientApp) {
    let text = format!("> {}", app.input);
    frame.render_widget(
        Paragraph::new(text).block(Block::default().title("Command").borders(Borders::ALL)),
        area,
    );
}

fn display_price(app: &TradingClientApp, raw: u64) -> String {
    if raw == 0 {
        "-".to_owned()
    } else {
        format_scaled(raw, app.instrument.price_decimals)
    }
}

fn display_qty(app: &TradingClientApp, raw: u64) -> String {
    if raw == 0 {
        "-".to_owned()
    } else {
        format_scaled(raw, app.instrument.qty_decimals)
    }
}

fn spawn_synthetic_traffic(config: &TradingClientConfig, instrument: InstrumentSpec) -> Option<Receiver<SyntheticTrafficMessage>> {
    if matches!(config.synthetic_traffic, SyntheticTrafficConfig::Noop) {
        return None;
    }

    let synthetic_traffic = SyntheticTraffic::new(config.synthetic_traffic.clone(), instrument);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut synthetic_traffic = synthetic_traffic;
        let mut frames = Vec::with_capacity(64);
        loop {
            frames.clear();
            synthetic_traffic.poll(&mut frames);
            if tx
                .send(SyntheticTrafficMessage::Status(synthetic_traffic.status().to_owned()))
                .is_err()
            {
                return;
            }
            if !frames.is_empty() && tx.send(SyntheticTrafficMessage::Frames(frames.clone())).is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    });
    Some(rx)
}

fn drain_synthetic_traffic(rx: &Option<Receiver<SyntheticTrafficMessage>>, out: &mut Vec<ClientOrderFrame>, app: &mut TradingClientApp) {
    let Some(rx) = rx else {
        return;
    };
    while let Ok(message) = rx.try_recv() {
        match message {
            SyntheticTrafficMessage::Status(status) => app.synthetic_traffic_status = status,
            SyntheticTrafficMessage::Frames(mut frames) => out.append(&mut frames),
        }
    }
}

#[derive(Debug, Clone)]
enum SyntheticTrafficMessage {
    Status(String),
    Frames(Vec<ClientOrderFrame>),
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
    fn enter() -> Result<Self, TradingClientError> {
        enable_raw_mode().map_err(TradingClientError::Terminal)?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen).map_err(TradingClientError::Terminal)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).map_err(TradingClientError::Terminal)?;
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
pub enum TradingClientError {
    Config(TradingClientConfigError),
    ServerConfig(crate::config::ConfigError),
    Io(TradingClientIoError),
    Terminal(std::io::Error),
}

impl std::fmt::Display for TradingClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(f, "{error}"),
            Self::ServerConfig(error) => write!(f, "server config error: {error:?}"),
            Self::Io(error) => write!(f, "{error}"),
            Self::Terminal(error) => write!(f, "terminal error: {error}"),
        }
    }
}

impl std::error::Error for TradingClientError {}

impl From<TradingClientConfigError> for TradingClientError {
    fn from(error: TradingClientConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<TradingClientIoError> for TradingClientError {
    fn from(error: TradingClientIoError) -> Self {
        Self::Io(error)
    }
}
