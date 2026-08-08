#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ErrorCode {
    DecodeError = 0x1_u8,
    UnsupportedTemplate = 0x2_u8,
    UnsupportedVersion = 0x3_u8,
    InternalError = 0x4_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for ErrorCode {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x1_u8 => Self::DecodeError,
            0x2_u8 => Self::UnsupportedTemplate,
            0x3_u8 => Self::UnsupportedVersion,
            0x4_u8 => Self::InternalError,
            _ => Self::NullVal,
        }
    }
}
impl From<ErrorCode> for u8 {
    #[inline]
    fn from(v: ErrorCode) -> Self {
        match v {
            ErrorCode::DecodeError => 0x1_u8,
            ErrorCode::UnsupportedTemplate => 0x2_u8,
            ErrorCode::UnsupportedVersion => 0x3_u8,
            ErrorCode::InternalError => 0x4_u8,
            ErrorCode::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for ErrorCode {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "DecodeError" => Ok(Self::DecodeError),
            "UnsupportedTemplate" => Ok(Self::UnsupportedTemplate),
            "UnsupportedVersion" => Ok(Self::UnsupportedVersion),
            "InternalError" => Ok(Self::InternalError),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for ErrorCode {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::DecodeError => write!(f, "DecodeError"),
            Self::UnsupportedTemplate => write!(f, "UnsupportedTemplate"),
            Self::UnsupportedVersion => write!(f, "UnsupportedVersion"),
            Self::InternalError => write!(f, "InternalError"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
