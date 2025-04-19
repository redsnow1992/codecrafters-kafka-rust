// #![allow(unused_imports)]
// use std::net::TcpListener;


struct Header {
    correlation_id: i32
}

struct Response {
    message_size: i32,
    header: Header,
}

impl Response {
    fn to_bytes(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(8); // Allocate enough space for the struct
        buf.put_i32(self.message_size);          // Serialize message_size
        buf.put_i32(self.header.correlation_id); // Serialize correlation_id
        buf
    }
}

// fn main() {
//     let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
//     for stream in listener.incoming() {
//         match stream {
//             Ok(_stream) => {
//                 println!("accepted new connection");
                
//             }
//             Err(e) => {
//                 println!("error: {}", e);
//             }
//         }
//     }
// }

use bytes::{BufMut, BytesMut};
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
                            // println!("Received: {:?}", &buffer[..n]);
                            let response = Response {
                                message_size: 0 as i32,
                                header: Header { correlation_id: 7 },
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
