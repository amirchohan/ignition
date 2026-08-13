use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream, UdpSocket},
    time::Duration,
};

use crate::protocol::SBE_HEADER_LEN;

use super::protocol::{ClientInboundEvent, ClientOrderFrame, decode_inbound_event, encode_order_frame};

#[derive(Debug)]
pub struct GatewayClient {
    stream: TcpStream,
    read_buf: Vec<u8>,
    read_start: usize,
    encode_buf: Vec<u8>,
}

impl GatewayClient {
    pub fn connect(addr: &str) -> Result<Self, TradingClientIoError> {
        let stream = TcpStream::connect(addr).map_err(TradingClientIoError::ConnectGateway)?;
        stream.set_nonblocking(true).map_err(TradingClientIoError::ConfigureGateway)?;
        stream.set_nodelay(true).map_err(TradingClientIoError::ConfigureGateway)?;
        Ok(Self {
            stream,
            read_buf: Vec::with_capacity(4096),
            read_start: 0,
            encode_buf: vec![0_u8; 4096],
        })
    }

    pub fn send(&mut self, frame: ClientOrderFrame) -> Result<(), TradingClientIoError> {
        let written = encode_order_frame(frame, &mut self.encode_buf).map_err(TradingClientIoError::Protocol)?;
        self.stream
            .write_all(&self.encode_buf[..written])
            .map_err(TradingClientIoError::WriteGateway)
    }

    pub fn poll(&mut self, events: &mut Vec<ClientInboundEvent>) -> Result<bool, TradingClientIoError> {
        let mut made_progress = false;
        let mut buf = [0_u8; 4096];
        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => return Err(TradingClientIoError::GatewayClosed),
                Ok(read) => {
                    made_progress = true;
                    self.read_buf.extend_from_slice(&buf[..read]);
                    self.drain_frames(events)?;
                    self.compact_read_buf();
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    self.compact_read_buf();
                    return Ok(made_progress);
                }
                Err(error) => return Err(TradingClientIoError::ReadGateway(error)),
            }
        }
    }

    fn drain_frames(&mut self, events: &mut Vec<ClientInboundEvent>) -> Result<(), TradingClientIoError> {
        loop {
            if self.read_buf.len().saturating_sub(self.read_start) < SBE_HEADER_LEN {
                return Ok(());
            }
            let block_len = u16::from_le_bytes([self.read_buf[self.read_start], self.read_buf[self.read_start + 1]]) as usize;
            let frame_len = SBE_HEADER_LEN + block_len;
            if self.read_buf.len().saturating_sub(self.read_start) < frame_len {
                return Ok(());
            }
            let frame_start = self.read_start;
            let frame_end = frame_start + frame_len;
            let event = decode_inbound_event(&self.read_buf[frame_start..frame_end]).map_err(TradingClientIoError::Protocol)?;
            events.push(event);
            self.read_start = frame_end;
        }
    }

    fn compact_read_buf(&mut self) {
        if self.read_start == 0 {
            return;
        }
        if self.read_start == self.read_buf.len() {
            self.read_buf.clear();
            self.read_start = 0;
            return;
        }
        let remaining = self.read_buf.len() - self.read_start;
        self.read_buf.copy_within(self.read_start.., 0);
        self.read_buf.truncate(remaining);
        self.read_start = 0;
    }
}

#[derive(Debug)]
pub struct MarketDataClient {
    socket: UdpSocket,
}

impl MarketDataClient {
    pub fn bind(bind_addr: &str, group_addr: &str) -> Result<Self, TradingClientIoError> {
        let group = parse_v4_addr(group_addr)?;
        let socket = UdpSocket::bind(bind_addr).map_err(TradingClientIoError::BindMarketData)?;
        socket
            .set_read_timeout(Some(Duration::from_millis(1)))
            .map_err(TradingClientIoError::ConfigureMarketData)?;
        socket
            .join_multicast_v4(group.ip(), &Ipv4Addr::UNSPECIFIED)
            .map_err(TradingClientIoError::ConfigureMarketData)?;
        socket.set_nonblocking(true).map_err(TradingClientIoError::ConfigureMarketData)?;
        Ok(Self { socket })
    }

    pub fn poll(&mut self, events: &mut Vec<ClientInboundEvent>) -> Result<bool, TradingClientIoError> {
        let mut made_progress = false;
        let mut buf = [0_u8; 4096];
        loop {
            match self.socket.recv(&mut buf) {
                Ok(read) => {
                    made_progress = true;
                    let event = decode_inbound_event(&buf[..read]).map_err(TradingClientIoError::Protocol)?;
                    events.push(event);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return Ok(made_progress),
                Err(error) => return Err(TradingClientIoError::ReadMarketData(error)),
            }
        }
    }
}

#[derive(Debug)]
pub enum TradingClientIoError {
    ConnectGateway(std::io::Error),
    ConfigureGateway(std::io::Error),
    WriteGateway(std::io::Error),
    ReadGateway(std::io::Error),
    GatewayClosed,
    BindMarketData(std::io::Error),
    ConfigureMarketData(std::io::Error),
    ReadMarketData(std::io::Error),
    InvalidMarketDataGroup,
    Protocol(crate::protocol::ProtocolError),
}

impl std::fmt::Display for TradingClientIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectGateway(error) => write!(f, "failed to connect gateway: {error}"),
            Self::ConfigureGateway(error) => write!(f, "failed to configure gateway socket: {error}"),
            Self::WriteGateway(error) => write!(f, "failed to write gateway frame: {error}"),
            Self::ReadGateway(error) => write!(f, "failed to read gateway frame: {error}"),
            Self::GatewayClosed => write!(f, "gateway connection closed"),
            Self::BindMarketData(error) => write!(f, "failed to bind market data socket: {error}"),
            Self::ConfigureMarketData(error) => write!(f, "failed to configure market data socket: {error}"),
            Self::ReadMarketData(error) => write!(f, "failed to read market data: {error}"),
            Self::InvalidMarketDataGroup => write!(f, "market data group must be an ipv4 socket address"),
            Self::Protocol(error) => write!(f, "protocol error: {error:?}"),
        }
    }
}

impl std::error::Error for TradingClientIoError {}

fn parse_v4_addr(input: &str) -> Result<SocketAddrV4, TradingClientIoError> {
    match input.parse::<SocketAddr>() {
        Ok(SocketAddr::V4(addr)) => Ok(addr),
        Ok(SocketAddr::V6(_)) | Err(_) => Err(TradingClientIoError::InvalidMarketDataGroup),
    }
}
