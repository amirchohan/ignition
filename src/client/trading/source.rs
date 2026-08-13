use crate::domain::rd::InstrumentSpec;

use super::{
    binance::BinanceDepthSource,
    config::{BinanceConfig, TrafficSourceKind},
    protocol::ClientOrderFrame,
};

#[derive(Debug)]
pub enum TrafficSource {
    Noop,
    Binance(Box<BinanceDepthSource>),
}

impl TrafficSource {
    pub fn new(kind: TrafficSourceKind, binance: BinanceConfig, instrument: InstrumentSpec, buy_account: u32, sell_account: u32) -> Self {
        match kind {
            TrafficSourceKind::Noop => Self::Noop,
            TrafficSourceKind::Binance => Self::Binance(Box::new(BinanceDepthSource::new(binance, instrument, buy_account, sell_account))),
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
