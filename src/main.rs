
use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};
use codecrafters_kafka::record::record_set_to_topic;
use futures::future::join_all;
use kafka_protocol::messages::api_versions_request::ApiVersionsRequest;
use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use kafka_protocol::messages::describe_topic_partitions_response::{DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{ApiKey, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, FetchRequest, FetchResponse, RequestHeader, RequestKind, ResponseHeader, ResponseKind};
use kafka_protocol::error::ResponseError;
use kafka_protocol::protocol::buf::ByteBuf;
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
use kafka_protocol::records::{RecordBatchDecoder, RecordSet};
use tokio::fs;

use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;


#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut socket, _addr)) => {
                println!("Accepted new connection");
                tokio::spawn(async move {
                    let (mut rd, mut wr) = socket.split(); // Split for concurrent I/O
                    let mut buffer = [0; 1024];
                    loop {
                        match rd.read(&mut buffer).await {
                            Ok(n) if n > 0 => {
                                let mut buf = BytesMut::from(&buffer[4..n]);
                                let response = handle(&mut buf).await;
                                let response_bytes: BytesMut = response.into();
                                if let Err(e) = wr.write_all(&response_bytes).await {
                                    println!("Failed to write to socket: {}", e);
                                }
                            }
                            Ok(_) => println!("Connection closed by client"),
                            Err(e) => println!("Failed to read from socket: {}", e),
                        }
                    }
                    let _ = socket.shutdown().await;
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

fn default_response_header(correlation_id: i32) -> ResponseHeader {
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
}

fn build_response(header: ResponseHeader,
    header_version: i16,
    body: ResponseKind,
    body_version: i16) -> BytesMut {
    let mut res_buf = BytesMut::new();
    let mut header_buf = BytesMut::new();
    let mut body_buf = BytesMut::new();

    header.encode(&mut header_buf, header_version).unwrap();
    body.encode(&mut body_buf, body_version).unwrap();

    let message_size = header_buf.len() + body_buf.len();
    res_buf.put_i32(message_size as i32);

    res_buf.extend_from_slice(&header_buf);
    res_buf.extend_from_slice(&body_buf);
    res_buf
}

fn response_with_error(correlation_id: i32, error: ResponseError) -> BytesMut {
    let mut res_buf = BytesMut::with_capacity(10);
    res_buf.put_i32(4);
    res_buf.put_i32(correlation_id);
    res_buf.put_i16(error.code());
    return res_buf;
}

async fn handle(buf: &mut BytesMut) -> BytesMut {
    let api_key = buf.peek_bytes(0..2).get_i16();
    let api_version = buf.peek_bytes(2..4).get_i16();
    let request_header_version = ApiKey::try_from(api_key).unwrap().request_header_version(api_version);
    
    let request_header = RequestHeader::decode(buf, request_header_version).unwrap();
    let api_key = ApiKey::try_from(api_key).unwrap();

    if !check_version(api_key, api_version) {
        return response_with_error(request_header.correlation_id, ResponseError::UnsupportedVersion);
    }
    let req = match api_key {
        ApiKey::ApiVersions => RequestKind::ApiVersions(ApiVersionsRequest::decode(buf, request_header.request_api_version).unwrap()),
        ApiKey::DescribeTopicPartitions => RequestKind::DescribeTopicPartitions(DescribeTopicPartitionsRequest::decode(buf, request_header.request_api_version).unwrap()),
        ApiKey::Fetch => RequestKind::Fetch(FetchRequest::decode(buf, request_header.request_api_version).unwrap()),
        _ => panic!("Unsupported API key: {:?}", api_key),
    };
    let (response, header_version) = match req {
        RequestKind::ApiVersions(_req) => {
            let resp = ApiVersionsResponse::default()
                .with_api_keys(vec![
                    ApiVersion::default()
                        .with_api_key(ApiKey::ApiVersions as i16)
                        .with_min_version(0)
                        .with_max_version(4),
                    ApiVersion::default()
                        .with_api_key(ApiKey::DescribeTopicPartitions as i16)
                        .with_min_version(0)
                        .with_max_version(4),
                    ApiVersion::default()
                        .with_api_key(ApiKey::Fetch as i16)
                        .with_min_version(0)
                        .with_max_version(16),
                ]);
            (ResponseKind::ApiVersions(resp), ApiVersionsResponse::header_version(api_version))
        }
        RequestKind::DescribeTopicPartitions(_req) => {
            let record_sets = parse_cluster_metadata().await;
            let topic_to_partition_ids = record_set_to_topic(&record_sets);

            let topics = _req.topics
                .iter()
                .map(|tr| {
                    let topic_name = tr.name.clone();
                    let name = topic_name.0.as_str();
                    if let Some(tuple) = topic_to_partition_ids.get(name) {
                        let partitions = tuple.1.iter()
                            .map(|partition_id| DescribeTopicPartitionsResponsePartition::default()
                                .with_partition_index(*partition_id))
                            .collect();
                        DescribeTopicPartitionsResponseTopic::default()
                            .with_name(Some(topic_name))
                            .with_topic_id(tuple.0)
                            .with_partitions(partitions)
                    } else {
                        DescribeTopicPartitionsResponseTopic::default()
                            .with_name(Some(topic_name))
                            .with_error_code(ResponseError::UnknownTopicOrPartition.code())
                            .with_topic_id(Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap())
                    }
                })
                .collect();


            let resp = DescribeTopicPartitionsResponse::default()
                .with_topics(topics);
            
            (ResponseKind::DescribeTopicPartitions(resp), DescribeTopicPartitionsResponse::header_version(api_version))
        }
        RequestKind::Fetch(_req) => {
            let record_sets = parse_cluster_metadata().await;
            let topic_to_partition_ids = record_set_to_topic(&record_sets);
            let topic_id_to_partition_ids: HashMap<Uuid, (&String, Vec<i32>)> = topic_to_partition_ids.iter().map(|kv| (kv.1.0, (kv.0, kv.1.1.clone()))).collect();
            
            let resp = if _req.topics.is_empty() {
                FetchResponse::default()
            } else {
                let mut resps = Vec::new();
                for fetch_topic in _req.topics {
                    let topic_id = fetch_topic.topic_id;
                    let paritions_data = if topic_id_to_partition_ids.contains_key(&topic_id) {
                        let topic_name = topic_id_to_partition_ids[&topic_id].0;
                        let partition_data_future = fetch_topic.partitions.iter().map(async |fp| {
                            let topic_data = read_topic_data(topic_name, fp.partition).await;
                            let topic_data= topic_data.freeze();
                            let partition_data = PartitionData::default()
                                .with_partition_index(fp.partition)
                                .with_records(Some(topic_data));
                            partition_data
                        });
                        join_all(partition_data_future).await
                    } else {
                        vec![PartitionData::default()
                            .with_error_code(ResponseError::UnknownTopicId.code())]
                    };
                    let resp = FetchableTopicResponse::default()
                        .with_topic_id(topic_id)
                        .with_partitions(paritions_data);
                    resps.push(resp);
                }
                FetchResponse::default()
                    .with_responses(resps)
            };

            (ResponseKind::Fetch(resp), FetchResponse::header_version(api_version))
        }
        _ => panic!()
    };
    build_response(default_response_header(request_header.correlation_id), header_version, response, api_version)
}

async fn read_metadata() -> BytesMut {
    let filename = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    read_file(filename).await
}

async fn read_topic_data(topic_name: &str, partition_idx: i32) -> BytesMut {
    let filename = format!("/tmp/kraft-combined-logs/{}-{}/00000000000000000000.log", topic_name, partition_idx);
    read_file(filename.as_str()).await
}

async fn read_file(filename: &str) -> BytesMut {
    let bytes = fs::read(filename).await.unwrap();
    println!("{:02x?}", bytes);
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&bytes);
    buf
}

async fn parse_cluster_metadata() -> Vec<RecordSet> {
    let mut buf = read_metadata().await;
    RecordBatchDecoder::decode_all(&mut buf).unwrap()
}