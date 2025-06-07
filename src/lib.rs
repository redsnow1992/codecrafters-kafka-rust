use bytes::{Buf, BytesMut};

pub mod record;

pub fn decode_unsigned_int (buf: &mut BytesMut) -> u32 {
    let mut value = 0;
    for i in 0..5 {
        let b = buf.try_get_u8().unwrap() as u32;
        value |= (b & 0x7F) << (i * 7);
        if b < 0x80 {
            break;
        }
    }
    value.into()
}

pub fn decode_int(buf: &mut BytesMut) -> i32 {
    let zigzag: u32 = decode_unsigned_int(buf);
    (((zigzag >> 1) as i32) ^ (-((zigzag & 1) as i32))).into()
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use crate::decode_int;

    #[test]
    fn test_decode_int() {
        let mut buf = BytesMut::new();
        buf.put_u8(0x3c);
        println!("{}", decode_int(&mut buf));
    }
}