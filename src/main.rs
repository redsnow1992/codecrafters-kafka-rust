use core::convert::Into;

use bytes::{BufMut, BytesMut};
use codecrafters_kafka::api_key::ApiKey;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct ResponseHeader {
    correlation_id: i32,
}

struct ResponseBody {
    error_code: i16,
    api_key: ApiKey,
    min_version: i16,
    max_version: i16,
}

impl ResponseBody {
    fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(2); // Allocate enough space for the struct
        buf.put_i16(self.error_code);             // Serialize error_code
        buf.put_i16(self.api_key.clone().into());       // Serialize api_key
        buf.put_i16(self.min_version);
        buf.put_i16(self.max_version);
        buf
    }

    fn size(&self) -> i32 {
        2 + 2 + 2 + 2 // Size of error_code + api_key + min_version + max_version
    }
}

impl ResponseHeader {
    fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(4); // Allocate enough space for the struct
        buf.put_i32(self.correlation_id);          // Serialize correlation_id
        buf
    }
    
    fn size(&self) -> i32 {
        4 // Size of correlation_id
    }
}

struct Response {
    message_size: i32,
    header: ResponseHeader,
    body: ResponseBody
}

impl Response {
    fn to_bytes(&mut self) -> BytesMut {
        let mut buf = BytesMut::with_capacity((self.message_size + 4).try_into().unwrap()); // Allocate enough space for the struct
        buf.put_i32(self.message_size); // Placeholder for message_size
        buf.put_slice(&self.header.to_bytes()[..]); // Serialize header
        buf.put_slice(&self.body.to_bytes()[..]); // Serialize body
        // self.message_size = (buf.len() - 4) as i32; // Update message_size
        // buf[0..4].copy_from_slice(&self.message_size.to_be_bytes()); // Update message_size in the buffer
        // buf.put_i32(self.message_size); // Serialize message_size
        
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
                            let mut response = handle(request).await;
                            if let Err(e) = socket.write_all(&response.to_bytes()).await {
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
    let request_body = match ApiKey::try_from(request.header.request_api_key).unwrap() {
        ApiKey::Produce => {
            ResponseBody {
                error_code: 0, // No error
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 0,
            }
        },
        ApiKey::Fetch => {
            ResponseBody {
                error_code: 0, // No error
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 0,
            }
        },
        ApiKey::ListOffsets => {
            ResponseBody {
                error_code: 0, // No error
                api_key: ApiKey::ListOffsets,
                min_version: 0,
                max_version: 0,
            }
        },
        ApiKey::ApiVersions => {
            ResponseBody {
                error_code: 35, // No error
                api_key: ApiKey::ApiVersions,
                min_version: 0,
                max_version: 4,
            }
        },
    };
    let header = ResponseHeader {
        correlation_id: request.header.correlation_id,
    };

    Response {
        message_size: header.size() + request_body.size(),
        header: header,
        body: request_body,
    }
}