#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum BookSide {
    Bid = 0x1_u8,
    Ask = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for BookSide {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::Bid,
            0x2_u8 => Self::Ask,
            _ => Self::NullVal,
        }
    }
}
impl From<BookSide> for u8 {
    #[inline]
    fn from(v: BookSide) -> Self {
        match v {
            BookSide::Bid => 0x1_u8,
            BookSide::Ask => 0x2_u8,
            BookSide::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for BookSide {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Bid" => Ok(Self::Bid),
            "Ask" => Ok(Self::Ask),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for BookSide {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Bid => write!(f, "Bid"),
            Self::Ask => write!(f, "Ask"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
