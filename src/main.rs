use bytes::{BufMut, BytesMut};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct ResponseHeader {
    correlation_id: i32,
}

struct Response {
    message_size: i32,
    header: ResponseHeader,
}

impl Response {
    fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(8); // Allocate enough space for the struct
        buf.put_i32(self.message_size);          // Serialize message_size
        buf.put_i32(self.header.correlation_id); // Serialize correlation_id
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
                            let response = Response {
                                message_size: 0 as i32,
                                header: ResponseHeader { correlation_id: request.header.correlation_id },
                            };
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
