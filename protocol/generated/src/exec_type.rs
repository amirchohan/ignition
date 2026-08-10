#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ExecType {
    New = 0x1_u8,
    Trade = 0x2_u8,
    Canceled = 0x3_u8,
    Replaced = 0x4_u8,
    Rejected = 0x5_u8,
    Expired = 0x6_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for ExecType {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::New,
            0x2_u8 => Self::Trade,
            0x3_u8 => Self::Canceled,
            0x4_u8 => Self::Replaced,
            0x5_u8 => Self::Rejected,
            0x6_u8 => Self::Expired,
            _ => Self::NullVal,
        }
    }
}
impl From<ExecType> for u8 {
    #[inline]
    fn from(v: ExecType) -> Self {
        match v {
            ExecType::New => 0x1_u8,
            ExecType::Trade => 0x2_u8,
            ExecType::Canceled => 0x3_u8,
            ExecType::Replaced => 0x4_u8,
            ExecType::Rejected => 0x5_u8,
            ExecType::Expired => 0x6_u8,
            ExecType::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for ExecType {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "New" => Ok(Self::New),
            "Trade" => Ok(Self::Trade),
            "Canceled" => Ok(Self::Canceled),
            "Replaced" => Ok(Self::Replaced),
            "Rejected" => Ok(Self::Rejected),
            "Expired" => Ok(Self::Expired),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ExecType {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::New => write!(f, "New"),
            Self::Trade => write!(f, "Trade"),
            Self::Canceled => write!(f, "Canceled"),
            Self::Replaced => write!(f, "Replaced"),
            Self::Rejected => write!(f, "Rejected"),
            Self::Expired => write!(f, "Expired"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
