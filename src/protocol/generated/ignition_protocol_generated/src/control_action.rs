#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ControlAction {
    Activate = 0x1_u8,
    Deactivate = 0x2_u8,
    Query = 0x3_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for ControlAction {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::Activate,
            0x2_u8 => Self::Deactivate,
            0x3_u8 => Self::Query,
            _ => Self::NullVal,
        }
    }
}
impl From<ControlAction> for u8 {
    #[inline]
    fn from(v: ControlAction) -> Self {
        match v {
            ControlAction::Activate => 0x1_u8,
            ControlAction::Deactivate => 0x2_u8,
            ControlAction::Query => 0x3_u8,
            ControlAction::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for ControlAction {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Activate" => Ok(Self::Activate),
            "Deactivate" => Ok(Self::Deactivate),
            "Query" => Ok(Self::Query),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ControlAction {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Activate => write!(f, "Activate"),
            Self::Deactivate => write!(f, "Deactivate"),
            Self::Query => write!(f, "Query"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
