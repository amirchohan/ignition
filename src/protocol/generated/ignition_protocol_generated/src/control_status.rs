#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ControlStatus {
    Ok = 0x1_u8,
    Rejected = 0x2_u8,
    Active = 0x3_u8,
    Inactive = 0x4_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for ControlStatus {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::Ok,
            0x2_u8 => Self::Rejected,
            0x3_u8 => Self::Active,
            0x4_u8 => Self::Inactive,
            _ => Self::NullVal,
        }
    }
}
impl From<ControlStatus> for u8 {
    #[inline]
    fn from(v: ControlStatus) -> Self {
        match v {
            ControlStatus::Ok => 0x1_u8,
            ControlStatus::Rejected => 0x2_u8,
            ControlStatus::Active => 0x3_u8,
            ControlStatus::Inactive => 0x4_u8,
            ControlStatus::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for ControlStatus {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Ok" => Ok(Self::Ok),
            "Rejected" => Ok(Self::Rejected),
            "Active" => Ok(Self::Active),
            "Inactive" => Ok(Self::Inactive),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ControlStatus {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Ok => write!(f, "Ok"),
            Self::Rejected => write!(f, "Rejected"),
            Self::Active => write!(f, "Active"),
            Self::Inactive => write!(f, "Inactive"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
