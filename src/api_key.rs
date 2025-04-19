use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(IntoPrimitive, TryFromPrimitive, Debug, PartialEq)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    ApiVersions = 18,
}

