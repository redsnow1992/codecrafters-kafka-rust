use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::api_versions_request::ApiVersionsRequest;
use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use kafka_protocol::messages::{ApiKey, RequestHeader, RequestKind};
use kafka_protocol::error::ResponseError;

use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, Encodable};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();
    loop {
        match listener.accept().await {
            Ok((mut socket, _addr)) => {
                // println!("Accepted new connection");
                tokio::spawn(async move {
                    let mut buffer = [0; 1024];
                    match socket.read(&mut buffer).await {
                        Ok(n) if n > 0 => {
                            let mut buf = BytesMut::from(&buffer[4..n]);
                            let response = handle(&mut buf).await;
                            let response_bytes: BytesMut = response.into();
                            if let Err(e) = socket.write_all(&response_bytes).await {
                                println!("Failed to write to socket: {}", e);
                            }
                        }
                        Ok(_) => println!("Connection closed by client"),
                        Err(e) => println!("Failed to read from socket: {}", e),
                    }
                });
            }
            Err(e) => {
                println!("Error accepting connection: {}", e);
            }
        }
    }
}

fn check_version(api_key: ApiKey, api_version: i16) -> bool {
    let api_version_range = api_key.valid_versions();
    api_version_range.min <= api_version && api_version_range.max >= api_version
}

async fn handle(buf: &mut BytesMut) -> BytesMut {
    let api_key = buf.peek_bytes(0..2).get_i16();
    let api_version = buf.peek_bytes(2..4).get_i16();
    let header_version = ApiKey::try_from(api_key).unwrap().request_header_version(api_version);
    
    let header = RequestHeader::decode(buf, header_version).unwrap();
    let api_key = ApiKey::try_from(api_key).unwrap();

    if !check_version(api_key, api_version) {
        let mut res_buf = BytesMut::with_capacity(10);
            res_buf.put_i32(4);
            res_buf.put_i32(header.correlation_id);
            res_buf.put_i16(ResponseError::UnsupportedVersion.code());
        return res_buf;
    }

    let req = match api_key {
        ApiKey::ApiVersions => RequestKind::ApiVersions(ApiVersionsRequest::decode(buf, header.request_api_version).unwrap()),
        _ => panic!("Unsupported API key: {:?}", api_key),
    };
    let mut body_buf = BytesMut::new();
    match req {
        RequestKind::ApiVersions(_req) => {
            let response = ApiVersionsResponse::default()
                .with_api_keys(vec![
                    ApiVersion::default()
                        .with_api_key(ApiKey::ApiVersions as i16)
                        .with_min_version(0)
                        .with_max_version(4),
                    ApiVersion::default()
                        .with_api_key(ApiKey::DescribeTopicPartitions as i16)
                        .with_min_version(0)
                        .with_max_version(4),
                ]);
            response.encode(&mut body_buf, api_version).unwrap();
            
            let mut res_buf = BytesMut::with_capacity(8);
            res_buf.put_i32(response.compute_size(api_version).unwrap() as i32 + 4);
            res_buf.put_i32(header.correlation_id);
            
            res_buf.extend_from_slice(&body_buf[..]);
            res_buf
        }
        _ => panic!()
    }
}