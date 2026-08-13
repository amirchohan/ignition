use super::protocol::{ClientControlFrame, ClientKillSwitch, ClientSnapshot};
use crate::protocol::generated::codecs::kill_switch_action;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestIdSource {
    next: u32,
}

impl RequestIdSource {
    pub const fn new(first: u32) -> Self {
        Self { next: first }
    }

    pub fn next(&mut self) -> u32 {
        let id = self.next;
        self.next = self.next.saturating_add(1);
        id
    }
}

pub fn parse_command(input: &str, ids: &mut RequestIdSource) -> Result<Option<ClientControlFrame>, CommandParseError> {
    let parts = input.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return Ok(None);
    }
    if parts.len() != 1 {
        return Err(CommandParseError::Usage);
    }

    match parts[0].to_ascii_lowercase().as_str() {
        "halt" => Ok(Some(kill_switch(ids.next(), kill_switch_action::KillSwitchAction::HaltTrading))),
        "resume" => Ok(Some(kill_switch(ids.next(), kill_switch_action::KillSwitchAction::ResumeTrading))),
        "shutdown" => Ok(Some(kill_switch(ids.next(), kill_switch_action::KillSwitchAction::Shutdown))),
        "snapshot" => Ok(Some(ClientControlFrame::Snapshot(ClientSnapshot { request_id: ids.next() }))),
        _ => Err(CommandParseError::UnknownCommand),
    }
}

fn kill_switch(request_id: u32, action: kill_switch_action::KillSwitchAction) -> ClientControlFrame {
    ClientControlFrame::KillSwitch(ClientKillSwitch { request_id, action })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandParseError {
    Usage,
    UnknownCommand,
}

impl std::fmt::Display for CommandParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage => write!(f, "usage: halt | resume | shutdown | snapshot"),
            Self::UnknownCommand => write!(f, "unknown command"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_control_commands() {
        let mut ids = RequestIdSource::new(1);
        assert!(matches!(
            parse_command("halt", &mut ids).expect("parse"),
            Some(ClientControlFrame::KillSwitch(ClientKillSwitch {
                action: kill_switch_action::KillSwitchAction::HaltTrading,
                ..
            }))
        ));
        assert!(matches!(
            parse_command("resume", &mut ids).expect("parse"),
            Some(ClientControlFrame::KillSwitch(ClientKillSwitch {
                action: kill_switch_action::KillSwitchAction::ResumeTrading,
                ..
            }))
        ));
        assert!(matches!(
            parse_command("shutdown", &mut ids).expect("parse"),
            Some(ClientControlFrame::KillSwitch(ClientKillSwitch {
                action: kill_switch_action::KillSwitchAction::Shutdown,
                ..
            }))
        ));
        assert!(matches!(
            parse_command("snapshot", &mut ids).expect("parse"),
            Some(ClientControlFrame::Snapshot(_))
        ));
    }

    #[test]
    fn rejects_unknown_or_extra_args() {
        let mut ids = RequestIdSource::new(1);
        assert_eq!(parse_command("halt now", &mut ids), Err(CommandParseError::Usage));
        assert_eq!(parse_command("pause", &mut ids), Err(CommandParseError::UnknownCommand));
    }
}
