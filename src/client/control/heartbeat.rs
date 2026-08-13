use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use super::protocol::{ClientControlFrame, ClientHeartbeat};

#[derive(Debug)]
pub struct HeartbeatRuntime {
    interval: Duration,
    next_seq: u32,
    last_due: Instant,
    pending: BTreeMap<u32, Instant>,
    last_sent: Option<(u32, Instant)>,
    last_ack: Option<(u32, Instant, Duration)>,
}

impl HeartbeatRuntime {
    pub fn new(interval: Duration, now: Instant) -> Self {
        Self {
            interval,
            next_seq: 1,
            last_due: now.checked_sub(interval).unwrap_or(now),
            pending: BTreeMap::new(),
            last_sent: None,
            last_ack: None,
        }
    }

    pub fn poll_due(&mut self, now: Instant) -> Option<ClientControlFrame> {
        if now.duration_since(self.last_due) < self.interval {
            return None;
        }
        self.last_due = now;
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        self.pending.insert(seq, now);
        self.last_sent = Some((seq, now));
        Some(ClientControlFrame::Heartbeat(ClientHeartbeat { heartbeat_seq: seq }))
    }

    pub fn on_ack(&mut self, seq: u32, now: Instant) {
        let Some(sent_at) = self.pending.remove(&seq) else {
            return;
        };
        self.last_ack = Some((seq, now, now.duration_since(sent_at)));
    }

    pub const fn next_seq(&self) -> u32 {
        self.next_seq
    }

    pub const fn last_sent(&self) -> Option<(u32, Instant)> {
        self.last_sent
    }

    pub const fn last_ack(&self) -> Option<(u32, Instant, Duration)> {
        self.last_ack
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_tracks_due_and_ack_rtt() {
        let start = Instant::now();
        let mut heartbeat = HeartbeatRuntime::new(Duration::from_millis(100), start);

        let Some(ClientControlFrame::Heartbeat(frame)) = heartbeat.poll_due(start) else {
            panic!("first heartbeat should be due");
        };
        assert_eq!(frame.heartbeat_seq, 1);
        assert!(heartbeat.poll_due(start + Duration::from_millis(10)).is_none());

        heartbeat.on_ack(1, start + Duration::from_millis(25));
        assert_eq!(heartbeat.last_ack().expect("ack").0, 1);
        assert_eq!(heartbeat.last_ack().expect("ack").2, Duration::from_millis(25));
    }
}
