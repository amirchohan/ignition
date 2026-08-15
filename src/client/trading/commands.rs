use crate::domain::{
    rd::InstrumentSpec,
    trading::order::{OrderType, Side, TimeInForce},
};

use super::protocol::{ClientCancelOrder, ClientCancelReplace, ClientMassCancel, ClientNewOrder, ClientOrderFrame};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIdSource {
    next: u64,
}

impl ClientIdSource {
    pub const fn new(first: u64) -> Self {
        Self { next: first }
    }

    pub fn next(&mut self) -> u64 {
        let id = self.next;
        self.next = self.next.saturating_add(1);
        id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandContext {
    pub instrument: InstrumentSpec,
}

pub fn parse_command(
    input: &str,
    context: CommandContext,
    ids: &mut ClientIdSource,
) -> Result<Option<ClientOrderFrame>, CommandParseError> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return Ok(None);
    }

    match parts[0].to_ascii_lowercase().as_str() {
        "buy" => parse_limit(parts.as_slice(), Side::Buy, context, ids),
        "sell" => parse_limit(parts.as_slice(), Side::Sell, context, ids),
        "market" => parse_market(parts.as_slice(), context, ids),
        "cancel" => parse_cancel(parts.as_slice(), context, ids),
        "replace" => parse_replace(parts.as_slice(), context, ids),
        "mass-cancel" => parse_mass_cancel(parts.as_slice(), context, ids),
        _ => Err(CommandParseError::UnknownCommand),
    }
    .map(Some)
}

pub fn scale_decimal_floor(input: &str, decimals: u8, lot: u64) -> Result<u64, CommandParseError> {
    let (raw, _) = scale_decimal(input, decimals)?;
    if lot == 0 {
        return Err(CommandParseError::InvalidScalar);
    }
    Ok(raw / lot * lot)
}

pub fn scale_decimal_ceil(input: &str, decimals: u8, tick: u64) -> Result<u64, CommandParseError> {
    let (mut raw, truncated_nonzero) = scale_decimal(input, decimals)?;
    if tick == 0 {
        return Err(CommandParseError::InvalidScalar);
    }
    if truncated_nonzero {
        raw = raw.checked_add(1).ok_or(CommandParseError::InvalidScalar)?;
    }
    raw.div_ceil(tick).checked_mul(tick).ok_or(CommandParseError::InvalidScalar)
}

pub fn format_scaled(raw: u64, decimals: u8) -> String {
    if decimals == 0 {
        return raw.to_string();
    }
    let scale = 10_u64.saturating_pow(u32::from(decimals));
    let whole = raw / scale;
    let frac = raw % scale;
    format!("{whole}.{frac:0width$}", width = usize::from(decimals))
}

fn parse_limit(
    parts: &[&str],
    side: Side,
    context: CommandContext,
    ids: &mut ClientIdSource,
) -> Result<ClientOrderFrame, CommandParseError> {
    if !(4..=5).contains(&parts.len()) {
        return Err(CommandParseError::Usage("buy|sell <price> <qty> [gtc|ioc|post] <account>"));
    }
    let price = match side {
        Side::Buy => scale_decimal_floor(parts[1], context.instrument.price_decimals, context.instrument.price_tick)?,
        Side::Sell => scale_decimal_ceil(parts[1], context.instrument.price_decimals, context.instrument.price_tick)?,
    };
    let qty = scale_decimal_floor(parts[2], context.instrument.qty_decimals, context.instrument.qty_lot)?;
    if price == 0 || qty == 0 {
        return Err(CommandParseError::InvalidScalar);
    }
    let (tif, account_id) = match parts.len() {
        4 => (TimeInForce::Gtc, parse_u32(parts[3])?),
        5 => (parse_tif(parts[3])?, parse_u32(parts[4])?),
        _ => return Err(CommandParseError::Usage("buy|sell <price> <qty> [gtc|ioc|post] <account>")),
    };
    Ok(ClientOrderFrame::New(ClientNewOrder {
        client_order_id: ids.next(),
        account_id,
        client_ts: ids.next(),
        side,
        order_type: OrderType::Limit,
        time_in_force: tif,
        price,
        qty,
    }))
}

fn parse_market(parts: &[&str], context: CommandContext, ids: &mut ClientIdSource) -> Result<ClientOrderFrame, CommandParseError> {
    if parts.len() != 4 {
        return Err(CommandParseError::Usage("market buy|sell <qty> <account>"));
    }
    let side = parse_side(parts[1])?;
    let qty = scale_decimal_floor(parts[2], context.instrument.qty_decimals, context.instrument.qty_lot)?;
    if qty == 0 {
        return Err(CommandParseError::InvalidScalar);
    }
    let account_id = parse_u32(parts[3])?;
    Ok(ClientOrderFrame::New(ClientNewOrder {
        client_order_id: ids.next(),
        account_id,
        client_ts: ids.next(),
        side,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::Ioc,
        price: 0,
        qty,
    }))
}

fn parse_cancel(parts: &[&str], _context: CommandContext, ids: &mut ClientIdSource) -> Result<ClientOrderFrame, CommandParseError> {
    if parts.len() != 3 {
        return Err(CommandParseError::Usage("cancel <orig_order_id> <account>"));
    }
    let orig_client_order_id = parse_u64(parts[1])?;
    let account_id = parse_u32(parts[2])?;
    Ok(ClientOrderFrame::Cancel(ClientCancelOrder {
        client_order_id: ids.next(),
        account_id,
        orig_client_order_id,
        client_ts: ids.next(),
    }))
}

fn parse_replace(parts: &[&str], context: CommandContext, ids: &mut ClientIdSource) -> Result<ClientOrderFrame, CommandParseError> {
    if parts.len() != 5 {
        return Err(CommandParseError::Usage("replace <orig_order_id> <price> <qty> <account>"));
    }
    let orig_client_order_id = parse_u64(parts[1])?;
    let price = scale_decimal_floor(parts[2], context.instrument.price_decimals, context.instrument.price_tick)?;
    let qty = scale_decimal_floor(parts[3], context.instrument.qty_decimals, context.instrument.qty_lot)?;
    if price == 0 || qty == 0 {
        return Err(CommandParseError::InvalidScalar);
    }
    let account_id = parse_u32(parts[4])?;
    Ok(ClientOrderFrame::Replace(ClientCancelReplace {
        client_order_id: ids.next(),
        account_id,
        orig_client_order_id,
        client_ts: ids.next(),
        price,
        qty,
    }))
}

fn parse_mass_cancel(parts: &[&str], _context: CommandContext, ids: &mut ClientIdSource) -> Result<ClientOrderFrame, CommandParseError> {
    if parts.len() != 2 {
        return Err(CommandParseError::Usage("mass-cancel <account>"));
    }
    Ok(ClientOrderFrame::MassCancel(ClientMassCancel {
        client_order_id: ids.next(),
        account_id: parse_u32(parts[1])?,
        client_ts: ids.next(),
    }))
}

fn parse_tif(input: &str) -> Result<TimeInForce, CommandParseError> {
    match input.to_ascii_lowercase().as_str() {
        "gtc" => Ok(TimeInForce::Gtc),
        "ioc" => Ok(TimeInForce::Ioc),
        "post" | "post-only" => Ok(TimeInForce::PostOnly),
        _ => Err(CommandParseError::InvalidTif),
    }
}

fn parse_side(input: &str) -> Result<Side, CommandParseError> {
    match input.to_ascii_lowercase().as_str() {
        "buy" => Ok(Side::Buy),
        "sell" => Ok(Side::Sell),
        _ => Err(CommandParseError::InvalidSide),
    }
}

fn parse_u64(input: &str) -> Result<u64, CommandParseError> {
    input.parse::<u64>().map_err(|_| CommandParseError::InvalidScalar)
}

fn parse_u32(input: &str) -> Result<u32, CommandParseError> {
    input.parse::<u32>().map_err(|_| CommandParseError::InvalidScalar)
}

fn scale_decimal(input: &str, decimals: u8) -> Result<(u64, bool), CommandParseError> {
    if input.is_empty() || input.starts_with('-') {
        return Err(CommandParseError::InvalidScalar);
    }
    let (whole, frac) = input.split_once('.').unwrap_or((input, ""));
    if whole.is_empty() && frac.is_empty() {
        return Err(CommandParseError::InvalidScalar);
    }
    if !whole.chars().all(|ch| ch.is_ascii_digit()) || !frac.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(CommandParseError::InvalidScalar);
    }
    let scale = 10_u64.checked_pow(u32::from(decimals)).ok_or(CommandParseError::InvalidScalar)?;
    let whole = if whole.is_empty() {
        0
    } else {
        whole.parse::<u64>().map_err(|_| CommandParseError::InvalidScalar)?
    };
    let mut frac_text = frac.to_owned();
    let truncated_nonzero = frac_text
        .get(usize::from(decimals)..)
        .is_some_and(|truncated| truncated.chars().any(|ch| ch != '0'));
    if frac_text.len() > usize::from(decimals) {
        frac_text.truncate(usize::from(decimals));
    }
    while frac_text.len() < usize::from(decimals) {
        frac_text.push('0');
    }
    let frac = if frac_text.is_empty() {
        0
    } else {
        frac_text.parse::<u64>().map_err(|_| CommandParseError::InvalidScalar)?
    };
    whole
        .checked_mul(scale)
        .and_then(|whole| whole.checked_add(frac))
        .map(|raw| (raw, truncated_nonzero))
        .ok_or(CommandParseError::InvalidScalar)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rd::InstrumentSpec;

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    fn context() -> CommandContext {
        CommandContext { instrument: instrument() }
    }

    #[test]
    fn decimal_scaling_floors_and_ceils_without_floats() {
        assert_eq!(scale_decimal_floor("100.009", 2, 1), Ok(10_000));
        assert_eq!(scale_decimal_ceil("100.001", 2, 1), Ok(10_001));
        assert_eq!(scale_decimal_ceil("100.000", 2, 1), Ok(10_000));
    }

    #[test]
    fn parses_limit_command_with_explicit_account() {
        let mut ids = ClientIdSource::new(1);
        let frame = parse_command("buy 100000.00 0.25 post 1", context(), &mut ids)
            .expect("parse")
            .expect("frame");

        let ClientOrderFrame::New(order) = frame else {
            panic!("expected new order");
        };
        assert_eq!(order.account_id, 1);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.time_in_force, TimeInForce::PostOnly);
        assert_eq!(order.price, 10_000_000);
        assert_eq!(order.qty, 25_000_000);
    }

    #[test]
    fn parses_market_and_cancel_commands() {
        let mut ids = ClientIdSource::new(10);
        let market = parse_command("market sell 0.10 9", context(), &mut ids)
            .expect("parse")
            .expect("frame");
        let ClientOrderFrame::New(order) = market else {
            panic!("expected market order");
        };
        assert_eq!(order.account_id, 9);
        assert_eq!(order.side, Side::Sell);
        assert_eq!(order.order_type, OrderType::Market);
        assert_eq!(order.price, 0);

        let cancel = parse_command("cancel 123 9", context(), &mut ids).expect("parse").expect("frame");
        let ClientOrderFrame::Cancel(cancel) = cancel else {
            panic!("expected cancel");
        };
        assert_eq!(cancel.orig_client_order_id, 123);
        assert_eq!(cancel.account_id, 9);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandParseError {
    Usage(&'static str),
    UnknownCommand,
    InvalidSide,
    InvalidTif,
    InvalidScalar,
}

impl std::fmt::Display for CommandParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(usage) => write!(f, "usage: {usage}"),
            Self::UnknownCommand => write!(f, "unknown command"),
            Self::InvalidSide => write!(f, "invalid side"),
            Self::InvalidTif => write!(f, "invalid time-in-force"),
            Self::InvalidScalar => write!(f, "invalid number"),
        }
    }
}
