#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum OrdStatus {
    New = 0x1_u8,
    PartiallyFilled = 0x2_u8,
    Filled = 0x3_u8,
    Canceled = 0x4_u8,
    Replaced = 0x5_u8,
    Rejected = 0x6_u8,
    Expired = 0x7_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for OrdStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::New,
            0x2_u8 => Self::PartiallyFilled,
            0x3_u8 => Self::Filled,
            0x4_u8 => Self::Canceled,
            0x5_u8 => Self::Replaced,
            0x6_u8 => Self::Rejected,
            0x7_u8 => Self::Expired,
            _ => Self::NullVal,
        }
    }
}
impl From<OrdStatus> for u8 {
    #[inline]
    fn from(v: OrdStatus) -> Self {
        match v {
            OrdStatus::New => 0x1_u8,
            OrdStatus::PartiallyFilled => 0x2_u8,
            OrdStatus::Filled => 0x3_u8,
            OrdStatus::Canceled => 0x4_u8,
            OrdStatus::Replaced => 0x5_u8,
            OrdStatus::Rejected => 0x6_u8,
            OrdStatus::Expired => 0x7_u8,
            OrdStatus::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for OrdStatus {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "New" => Ok(Self::New),
            "PartiallyFilled" => Ok(Self::PartiallyFilled),
            "Filled" => Ok(Self::Filled),
            "Canceled" => Ok(Self::Canceled),
            "Replaced" => Ok(Self::Replaced),
            "Rejected" => Ok(Self::Rejected),
            "Expired" => Ok(Self::Expired),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for OrdStatus {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::New => write!(f, "New"),
            Self::PartiallyFilled => write!(f, "PartiallyFilled"),
            Self::Filled => write!(f, "Filled"),
            Self::Canceled => write!(f, "Canceled"),
            Self::Replaced => write!(f, "Replaced"),
            Self::Rejected => write!(f, "Rejected"),
            Self::Expired => write!(f, "Expired"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
