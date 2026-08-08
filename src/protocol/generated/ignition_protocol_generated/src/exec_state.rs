#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ExecState {
    Accepted = 0x1_u8,
    Rejected = 0x2_u8,
    PartiallyFilled = 0x3_u8,
    Filled = 0x4_u8,
    Cancelled = 0x5_u8,
    Replaced = 0x6_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for ExecState {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::Accepted,
            0x2_u8 => Self::Rejected,
            0x3_u8 => Self::PartiallyFilled,
            0x4_u8 => Self::Filled,
            0x5_u8 => Self::Cancelled,
            0x6_u8 => Self::Replaced,
            _ => Self::NullVal,
        }
    }
}
impl From<ExecState> for u8 {
    #[inline]
    fn from(v: ExecState) -> Self {
        match v {
            ExecState::Accepted => 0x1_u8,
            ExecState::Rejected => 0x2_u8,
            ExecState::PartiallyFilled => 0x3_u8,
            ExecState::Filled => 0x4_u8,
            ExecState::Cancelled => 0x5_u8,
            ExecState::Replaced => 0x6_u8,
            ExecState::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for ExecState {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Accepted" => Ok(Self::Accepted),
            "Rejected" => Ok(Self::Rejected),
            "PartiallyFilled" => Ok(Self::PartiallyFilled),
            "Filled" => Ok(Self::Filled),
            "Cancelled" => Ok(Self::Cancelled),
            "Replaced" => Ok(Self::Replaced),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ExecState {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Accepted => write!(f, "Accepted"),
            Self::Rejected => write!(f, "Rejected"),
            Self::PartiallyFilled => write!(f, "PartiallyFilled"),
            Self::Filled => write!(f, "Filled"),
            Self::Cancelled => write!(f, "Cancelled"),
            Self::Replaced => write!(f, "Replaced"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
