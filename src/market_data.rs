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
    io: Option<UdpFramePublisher>,
    encode_buf: Vec<u8>,
    sequencer: MarketDataSequencer,
    last_l1: Option<L1Update>,
    pending_reference_price: Option<Price>,
    first_error: Option<MarketDataFeedError>,
    #[cfg(test)]
    queued_frames: Vec<u8>,
}

impl MarketDataRuntime {
    /// Open the configured multicast publisher and preallocate encode buffers.
    pub fn new(config: MarketDataFeedConfig) -> Result<Self, MarketDataFeedError> {
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
            io: Some(io),
            encode_buf: vec![0_u8; encode_len],
            sequencer: MarketDataSequencer::default(),
            last_l1: None,
            pending_reference_price: None,
            first_error: None,
            #[cfg(test)]
            queued_frames: Vec::new(),
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
    pub fn publish_l1_update_if_changed(
        &mut self,
        best_bid: Option<(Price, u64)>,
        best_ask: Option<(Price, u64)>,
    ) -> DomainResult<()> {
        let bid_price = best_bid.map(|(price, _)| price);
        let bid_qty = best_bid.map_or(0, |(_, qty)| qty);
        let ask_price = best_ask.map(|(price, _)| price);
        let ask_qty = best_ask.map_or(0, |(_, qty)| qty);

        if self.last_l1.is_some_and(|last| {
            last.bid_price == bid_price
                && last.bid_qty == bid_qty
                && last.ask_price == ask_price
                && last.ask_qty == ask_qty
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

    /// Queue one market-data event and defer the first egress failure until `finish`.
    fn emit_market_data(&mut self, event: MarketDataEvent) {
        if self.first_error.is_some() {
            return;
        }
        let written = match encode_market_data_event(event, &mut self.encode_buf) {
            Ok(written) => written,
            Err(error) => {
                self.first_error = Some(MarketDataFeedError::Encode(error));
                return;
            }
        };

        if let Some(io) = self.io.as_mut() {
            if let Err(error) = io.queue_frame(&self.encode_buf[..written]) {
                self.first_error = Some(MarketDataFeedError::Io(error));
            }
        } else {
            #[cfg(test)]
            {
                self.queued_frames
                    .extend_from_slice(&(written as u16).to_le_bytes());
                self.queued_frames
                    .extend_from_slice(&self.encode_buf[..written]);
            }
        }
    }

    /// Flush queued multicast datagrams.
    pub fn flush_pending(&mut self) -> Result<bool, MarketDataFeedError> {
        match self.io.as_mut() {
            Some(io) => io.flush_pending().map_err(MarketDataFeedError::Io),
            None => Ok(false),
        }
    }

    /// Return the first deferred egress failure recorded by sink-style emission.
    pub fn finish(&mut self) -> Result<(), MarketDataFeedError> {
        match self.first_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Return encoded market-data frames queued by tests before UDP flush.
    #[cfg(test)]
    pub fn queued_frames(&self) -> EncodedMarketDataFrames<'_> {
        EncodedMarketDataFrames {
            bytes: &self.queued_frames,
            cursor: 0,
        }
    }

    /// Clear encoded market-data frames queued by tests.
    #[cfg(test)]
    pub fn clear_queued_frames(&mut self) {
        self.queued_frames.clear();
    }
}

#[cfg(test)]
impl Default for MarketDataRuntime {
    fn default() -> Self {
        let config = MarketDataFeedConfig::default();
        Self {
            io: None,
            encode_buf: vec![0_u8; config.max_datagram_len],
            sequencer: MarketDataSequencer::default(),
            last_l1: None,
            pending_reference_price: None,
            first_error: None,
            queued_frames: Vec::new(),
        }
    }
}

#[cfg(test)]
pub struct EncodedMarketDataFrames<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

#[cfg(test)]
impl<'a> Iterator for EncodedMarketDataFrames<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.bytes.len().saturating_sub(self.cursor) < 2 {
            return None;
        }
        let len =
            u16::from_le_bytes([self.bytes[self.cursor], self.bytes[self.cursor + 1]]) as usize;
        let frame_start = self.cursor + 2;
        let frame_end = frame_start + len;
        if self.bytes.len() < frame_end {
            return None;
        }
        self.cursor = frame_end;
        Some(&self.bytes[frame_start..frame_end])
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
pub enum MarketDataFeedError {
    Io(IoError),
    Encode(ProtocolError),
}

impl From<IoError> for MarketDataFeedError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for MarketDataFeedError {
    fn from(error: ProtocolError) -> Self {
        Self::Encode(error)
    }
}
