use api_key::ApiKeyInfo;
use bytes::{BufMut, BytesMut};
use error::ErrorCode;

pub mod api_key;
pub mod error;

const TAG_BUFFER: &[u8] = &[0];

pub trait ResponseBody {
    fn to_bytes(&self) -> BytesMut;
}

#[derive(Debug)]
pub struct ApiVersionsBody {
    pub error_code: ErrorCode,
    pub api_keys: Vec<ApiKeyInfo>,
}

impl ResponseBody for ApiVersionsBody {
    fn to_bytes(&self) -> BytesMut {
        let apk_keys_len = self.api_keys.len();
        let mut buf = BytesMut::with_capacity(2 + 1 + apk_keys_len * 7 + 4 + 1);
        buf.put_i16(self.error_code.clone().into()); // Serialize error_code

        buf.extend_from_slice(&(self.api_keys.len() as u8 + 1).to_be_bytes()); // Serialize the length of api_keys

        self.api_keys.iter().for_each(|api_key_info| {
            buf.put_i16(api_key_info.api_key.clone().into());
            buf.put_i16(api_key_info.min_version);
            buf.put_i16(api_key_info.max_version);
            buf.extend_from_slice(TAG_BUFFER);
        });
        buf.extend_from_slice(&[0u8; 4]);
        buf.extend_from_slice(TAG_BUFFER);

        buf
    }
}
