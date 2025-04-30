use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(IntoPrimitive, TryFromPrimitive, Debug, PartialEq, Clone)]
#[repr(i16)]
pub enum ApiKey {
    // Produce = 0,
    // Fetch = 1,
    // ListOffsets = 2,
    ApiVersions = 18,
}

#[derive(Debug)]
pub struct ApiKeyInfo {
    pub api_key: ApiKey,
    pub min_version: i16,
    pub max_version: i16,
}
