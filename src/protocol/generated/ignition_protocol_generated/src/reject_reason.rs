#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum RejectReason {
    None = 0x0_u8,
    Malformed = 0x1_u8,
    UnknownOrder = 0x2_u8,
    InvalidPrice = 0x3_u8,
    InvalidQuantity = 0x4_u8,
    RiskOpenOrderLimit = 0x5_u8,
    RiskNotionalLimit = 0x6_u8,
    PriceBand = 0x7_u8,
    SelfTradePrevention = 0x8_u8,
    KillSwitchActive = 0x9_u8,
    HeartbeatLost = 0xa_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for RejectReason {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::None,
            0x1_u8 => Self::Malformed,
            0x2_u8 => Self::UnknownOrder,
            0x3_u8 => Self::InvalidPrice,
            0x4_u8 => Self::InvalidQuantity,
            0x5_u8 => Self::RiskOpenOrderLimit,
            0x6_u8 => Self::RiskNotionalLimit,
            0x7_u8 => Self::PriceBand,
            0x8_u8 => Self::SelfTradePrevention,
            0x9_u8 => Self::KillSwitchActive,
            0xa_u8 => Self::HeartbeatLost,
            _ => Self::NullVal,
        }
    }
}
impl From<RejectReason> for u8 {
    #[inline]
    fn from(v: RejectReason) -> Self {
        match v {
            RejectReason::None => 0x0_u8,
            RejectReason::Malformed => 0x1_u8,
            RejectReason::UnknownOrder => 0x2_u8,
            RejectReason::InvalidPrice => 0x3_u8,
            RejectReason::InvalidQuantity => 0x4_u8,
            RejectReason::RiskOpenOrderLimit => 0x5_u8,
            RejectReason::RiskNotionalLimit => 0x6_u8,
            RejectReason::PriceBand => 0x7_u8,
            RejectReason::SelfTradePrevention => 0x8_u8,
            RejectReason::KillSwitchActive => 0x9_u8,
            RejectReason::HeartbeatLost => 0xa_u8,
            RejectReason::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for RejectReason {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "None" => Ok(Self::None),
            "Malformed" => Ok(Self::Malformed),
            "UnknownOrder" => Ok(Self::UnknownOrder),
            "InvalidPrice" => Ok(Self::InvalidPrice),
            "InvalidQuantity" => Ok(Self::InvalidQuantity),
            "RiskOpenOrderLimit" => Ok(Self::RiskOpenOrderLimit),
            "RiskNotionalLimit" => Ok(Self::RiskNotionalLimit),
            "PriceBand" => Ok(Self::PriceBand),
            "SelfTradePrevention" => Ok(Self::SelfTradePrevention),
            "KillSwitchActive" => Ok(Self::KillSwitchActive),
            "HeartbeatLost" => Ok(Self::HeartbeatLost),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for RejectReason {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Malformed => write!(f, "Malformed"),
            Self::UnknownOrder => write!(f, "UnknownOrder"),
            Self::InvalidPrice => write!(f, "InvalidPrice"),
            Self::InvalidQuantity => write!(f, "InvalidQuantity"),
            Self::RiskOpenOrderLimit => write!(f, "RiskOpenOrderLimit"),
            Self::RiskNotionalLimit => write!(f, "RiskNotionalLimit"),
            Self::PriceBand => write!(f, "PriceBand"),
            Self::SelfTradePrevention => write!(f, "SelfTradePrevention"),
            Self::KillSwitchActive => write!(f, "KillSwitchActive"),
            Self::HeartbeatLost => write!(f, "HeartbeatLost"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
