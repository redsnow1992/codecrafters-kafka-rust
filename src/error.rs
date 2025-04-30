use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(IntoPrimitive, TryFromPrimitive, Debug, PartialEq, Clone)]
#[repr(i16)]
pub enum ErrorCode {
    NONE = 0,
    UnsupportedVersion = 35,
}