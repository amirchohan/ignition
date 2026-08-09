#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ControlResult {
    Ok = 0x1_u8,
    Rejected = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for ControlResult {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::Ok,
            0x2_u8 => Self::Rejected,
            _ => Self::NullVal,
        }
    }
}
impl From<ControlResult> for u8 {
    #[inline]
    fn from(v: ControlResult) -> Self {
        match v {
            ControlResult::Ok => 0x1_u8,
            ControlResult::Rejected => 0x2_u8,
            ControlResult::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for ControlResult {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Ok" => Ok(Self::Ok),
            "Rejected" => Ok(Self::Rejected),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ControlResult {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Ok => write!(f, "Ok"),
            Self::Rejected => write!(f, "Rejected"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
