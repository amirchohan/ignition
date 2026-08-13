//! Multicast market-data feed runtime boundary.
//!
//! The feed owns only market-data transport egress. Matching and market-data
//! sequencing emit typed domain events; this runtime encodes those events into
//! SBE frames and queues them for multicast UDP send.

use crate::{
    domain::{
        DomainError, DomainResult, Price, Qty,
        rd::InstrumentSpec,
        trading::{
            md::{L1Update, L2Update, MarketDataEvent, MarketDataSequencer, TradePrint},
            order::{OrderId, Side},
        },
    },
    io::{IoError, UdpFramePublisher, UdpMulticastConfig},
    protocol::{ProtocolError, wire::trading::encode_market_data_event},
};

/// Market-data multicast feed configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataFeedConfig {
    /// Local UDP bind address.
    pub bind_addr: String,
    /// IPv4 multicast destination address.
    pub group_addr: String,
    /// IPv4 multicast TTL.
    pub multicast_ttl: u32,
    /// Whether multicast loopback is enabled.
    pub multicast_loop_v4: bool,
    /// Maximum encoded SBE datagram size.
    pub max_datagram_len: usize,
    /// Maximum queued encoded datagram bytes before reporting backpressure.
    pub max_pending_datagram_bytes: usize,
}

impl Default for MarketDataFeedConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:0".to_owned(),
            group_addr: "239.255.0.1:9002".to_owned(),
            multicast_ttl: 1,
            multicast_loop_v4: true,
            max_datagram_len: 4096,
            max_pending_datagram_bytes: 1 << 20,
        }
    }
}

/// Live multicast market-data feed runtime.
#[derive(Debug)]
pub struct MarketDataRuntime {
    io: UdpFramePublisher,
    encode_buf: Vec<u8>,
    sequencer: MarketDataSequencer,
    last_l1: Option<L1Update>,
    pending_reference_price: Option<Price>,
    first_error: Option<MarketDataError>,
}

impl MarketDataRuntime {
    /// Open the configured multicast publisher and preallocate encode buffers.
    pub fn new(config: MarketDataFeedConfig) -> Result<Self, MarketDataError> {
        let encode_len = config.max_datagram_len;
        let io = UdpFramePublisher::bind_multicast(UdpMulticastConfig {
            bind_addr: config.bind_addr,
            group_addr: config.group_addr,
            multicast_ttl: config.multicast_ttl,
            multicast_loop_v4: config.multicast_loop_v4,
            max_datagram_len: config.max_datagram_len,
            max_pending_datagram_bytes: config.max_pending_datagram_bytes,
        })?;
        Ok(Self {
            io,
            encode_buf: vec![0_u8; encode_len],
            sequencer: MarketDataSequencer::default(),
            last_l1: None,
            pending_reference_price: None,
            first_error: None,
        })
    }

    /// Emit one trade print with a fresh market-data sequence and trade ID.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_trade(
        &mut self,
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        price: Price,
        fill_qty: u64,
        instrument: &InstrumentSpec,
    ) -> DomainResult<()> {
        let qty = Qty::new(fill_qty, instrument).map_err(|_| DomainError::InvalidQuantity)?;
        let md_seq = self.sequencer.next_md_seq()?;
        let event = MarketDataEvent::TradePrint(TradePrint {
            md_seq,
            trade_id: self.sequencer.next_trade_id()?,
            maker_order_id,
            taker_order_id,
            price,
            qty,
        });
        self.pending_reference_price = Some(price);
        self.emit_market_data(event);
        Ok(())
    }

    /// Emit one absolute L2 aggregate level update.
    pub fn publish_l2_update(&mut self, side: Side, price: Price, qty: u64) -> DomainResult<()> {
        let prev_md_seq = self.sequencer.previous_md_seq();
        let md_seq = self.sequencer.next_md_seq()?;
        self.emit_market_data(MarketDataEvent::L2Update(L2Update {
            md_seq,
            prev_md_seq,
            side,
            price,
            qty,
        }));
        Ok(())
    }

    /// Emit one L1 top-of-book update only when the top state changed.
    pub fn publish_l1_update_if_changed(&mut self, best_bid: Option<(Price, u64)>, best_ask: Option<(Price, u64)>) -> DomainResult<()> {
        let bid_price = best_bid.map(|(price, _)| price);
        let bid_qty = best_bid.map_or(0, |(_, qty)| qty);
        let ask_price = best_ask.map(|(price, _)| price);
        let ask_qty = best_ask.map_or(0, |(_, qty)| qty);

        if self.last_l1.is_some_and(|last| {
            last.bid_price == bid_price && last.bid_qty == bid_qty && last.ask_price == ask_price && last.ask_qty == ask_qty
        }) {
            return Ok(());
        }

        let prev_md_seq = self.sequencer.previous_md_seq();
        let md_seq = self.sequencer.next_md_seq()?;
        let event = L1Update {
            md_seq,
            prev_md_seq,
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
        };
        self.emit_market_data(MarketDataEvent::L1Update(event));
        self.last_l1 = Some(event);
        Ok(())
    }

    /// Return deterministic outbound sequence state for control snapshots.
    pub fn snapshot(&self) -> MarketDataSnapshot {
        MarketDataSnapshot {
            last_md_seq: self.sequencer.previous_md_seq().raw(),
            next_trade_id: self.sequencer.next_trade_id_value().raw(),
        }
    }

    /// Take the latest trade price emitted since the previous risk observation.
    pub fn take_reference_price_update(&mut self) -> Option<Price> {
        self.pending_reference_price.take()
    }

    /// Queue one market-data event and defer the first egress failure until checked.
    fn emit_market_data(&mut self, event: MarketDataEvent) {
        if self.first_error.is_some() {
            return;
        }
        let written = match encode_market_data_event(event, &mut self.encode_buf) {
            Ok(written) => written,
            Err(error) => {
                self.first_error = Some(MarketDataError::Encode(error));
                return;
            }
        };

        if let Err(error) = self.io.queue_frame(&self.encode_buf[..written]) {
            self.first_error = Some(MarketDataError::Io(error));
        }
    }

    /// Flush queued multicast datagrams.
    pub fn flush_pending(&mut self) -> Result<bool, MarketDataError> {
        self.io.flush_pending().map_err(MarketDataError::Io)
    }

    /// Poll market-data egress.
    pub fn poll(&mut self) -> Result<bool, MarketDataError> {
        self.flush_pending()
    }

    /// Return the first egress failure deferred by sink-style emission.
    pub fn check_deferred_error(&mut self) -> Result<(), MarketDataError> {
        match self.first_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Read-only market-data sequencer state included in control snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketDataSnapshot {
    pub last_md_seq: u64,
    pub next_trade_id: u64,
}

/// Market-data feed failure.
#[derive(Debug)]
pub enum MarketDataError {
    Io(IoError),
    Encode(ProtocolError),
}

impl From<IoError> for MarketDataError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for MarketDataError {
    fn from(error: ProtocolError) -> Self {
        Self::Encode(error)
    }
}
