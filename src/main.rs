use core::convert::Into;

use bytes::{BufMut, BytesMut};
use codecrafters_kafka::api_key::{ApiKey, ApiKeyInfo};
use codecrafters_kafka::error::ErrorCode;
use codecrafters_kafka::{ApiVersionsBody, ResponseBody};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct ResponseHeader {
    correlation_id: i32,
}


impl From<ResponseHeader> for BytesMut {
    fn from(header: ResponseHeader) -> Self {
        let mut buf = BytesMut::with_capacity(4); // Allocate enough space for the struct
        buf.put_i32(header.correlation_id);       // Serialize correlation_id
        buf
    }
}

struct Response {
    header: ResponseHeader,
    body: Box<dyn ResponseBody>,
}

impl From<Response> for BytesMut {
    fn from(response: Response) -> Self {
        let header_bytes: BytesMut = response.header.into();
        let body_bytes: BytesMut = response.body.to_bytes();
        let message_size = header_bytes.len() + body_bytes.len();
        // println!("Message size: {}", message_size);
        let mut buf = BytesMut::with_capacity(message_size + 4); // Allocate enough space for the struct
        buf.put_i32(message_size as i32); // Placeholder for message_size
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(&body_bytes);

        buf
    }
}

struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
}

struct Request {
    message_size: i32,
    header: RequestHeader,
}

impl Request {
    fn from_bytes(bytes: &[u8]) -> Self {
        let buf = bytes;
        let message_size = i32::from_be_bytes(buf[0..4].try_into().unwrap());
        let request_api_key = i16::from_be_bytes(buf[4..6].try_into().unwrap());
        let request_api_version = i16::from_be_bytes(buf[6..8].try_into().unwrap());
        let correlation_id = i32::from_be_bytes(buf[8..12].try_into().unwrap());
        let client_id = None; // Placeholder for client_id parsing

        Request {
            message_size,
            header: RequestHeader {
                request_api_key,
                request_api_version,
                correlation_id,
                client_id,
            },
        }
    }
    
}

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
                            // println!("Received: {:?}", &buffer[..n]);
                            let request = Request::from_bytes(&buffer[..n]);
                            let response = handle(request).await;
                            let response_bytes: BytesMut = response.into();
                            println!("Response: {:?}", &response_bytes);
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

async fn handle(request: Request) -> Response {
    let body: Box<dyn ResponseBody> = match ApiKey::try_from(request.header.request_api_key).unwrap() {
        ApiKey::ApiVersions => {
            if request.header.request_api_version < 0 || request.header.request_api_version > 4 {
                Box::new(ApiVersionsBody {
                    error_code: ErrorCode::UnsupportedVersion, // Error code for invalid version
                    api_keys: vec![],
                })
            } else {
                Box::new(ApiVersionsBody {
                    error_code: ErrorCode::NONE, // No error
                    api_keys: vec![
                        ApiKeyInfo {
                            api_key: ApiKey::ApiVersions,
                            min_version: 0,
                            max_version: 4,
                        },
                    ],
                })
            }
        },
    };
    let header = ResponseHeader {
        correlation_id: request.header.correlation_id,
    };
    Response {
        // message_size: header.size() + response_body.size(),
        header,
        body,
    }
}