use crate::domain::rd::InstrumentSpec;

use super::{binance::BinanceDepthSource, config::SyntheticTrafficConfig, protocol::ClientOrderFrame};

#[derive(Debug)]
pub enum SyntheticTraffic {
    Noop,
    Binance(Box<BinanceDepthSource>),
}

impl SyntheticTraffic {
    pub fn new(config: SyntheticTrafficConfig, instrument: InstrumentSpec) -> Self {
        match config {
            SyntheticTrafficConfig::Noop => Self::Noop,
            SyntheticTrafficConfig::Binance {
                symbol,
                levels,
                speed_ms,
                reconnect_backoff_ms,
                buys_account_id,
                sells_account_id,
            } => Self::Binance(Box::new(BinanceDepthSource::new(
                symbol,
                levels,
                speed_ms,
                reconnect_backoff_ms,
                instrument,
                buys_account_id,
                sells_account_id,
            ))),
        }
    }

    pub fn poll(&mut self, out: &mut Vec<ClientOrderFrame>) {
        match self {
            Self::Noop => {}
            Self::Binance(source) => source.poll(out),
        }
    }

    pub fn status(&self) -> &str {
        match self {
            Self::Noop => "source noop",
            Self::Binance(source) => source.status(),
        }
    }
}
