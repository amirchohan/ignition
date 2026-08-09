#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum KillSwitchAction {
    HaltTrading = 0x1_u8,
    Shutdown = 0x2_u8,
    ResumeTrading = 0x3_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for KillSwitchAction {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::HaltTrading,
            0x2_u8 => Self::Shutdown,
            0x3_u8 => Self::ResumeTrading,
            _ => Self::NullVal,
        }
    }
}
impl From<KillSwitchAction> for u8 {
    #[inline]
    fn from(v: KillSwitchAction) -> Self {
        match v {
            KillSwitchAction::HaltTrading => 0x1_u8,
            KillSwitchAction::Shutdown => 0x2_u8,
            KillSwitchAction::ResumeTrading => 0x3_u8,
            KillSwitchAction::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for KillSwitchAction {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "HaltTrading" => Ok(Self::HaltTrading),
            "Shutdown" => Ok(Self::Shutdown),
            "ResumeTrading" => Ok(Self::ResumeTrading),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for KillSwitchAction {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::HaltTrading => write!(f, "HaltTrading"),
            Self::Shutdown => write!(f, "Shutdown"),
            Self::ResumeTrading => write!(f, "ResumeTrading"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
