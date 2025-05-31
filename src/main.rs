
use bytes::{Buf, BufMut, BytesMut};
use codecrafters_kafka::record::{extract_record_value, RecordValue};
use kafka_protocol::messages::api_versions_request::ApiVersionsRequest;
use kafka_protocol::messages::api_versions_response::{ApiVersion, ApiVersionsResponse};
use kafka_protocol::messages::describe_topic_partitions_response::{DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic};
use kafka_protocol::messages::{ApiKey, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, RequestHeader, RequestKind, ResponseHeader, ResponseKind};
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
                ]);
            (ResponseKind::ApiVersions(resp), ApiVersionsResponse::header_version(api_version))
        }
        RequestKind::DescribeTopicPartitions(_req) => {
            let topic_request = &_req.topics[0];
            let record_sets = parse_cluster_metadata().await;
            
            let topic_response = DescribeTopicPartitionsResponseTopic::default()
                .with_name(Some(topic_request.name.clone()))
                .with_error_code(ResponseError::UnknownTopicOrPartition.code())
                .with_topic_id(Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap());
            let mut resp = DescribeTopicPartitionsResponse::default()
                .with_topics(vec![topic_response]);

            for record_set in record_sets.iter() {
                if record_set.records.len() > 1 {
                    let record_value = extract_record_value(&record_set.records[0]);
                    let topic_id = match record_value {
                        RecordValue::TopicRecord(tr) => {
                            if tr.name != topic_request.name.0.as_str() {
                                continue;
                            } else {
                                tr.topic_id
                            }
                        }
                        _ => continue
                    };
                    let partitions = record_set.records[1..]
                        .iter()
                        .flat_map(|r| {
                            let record_value = extract_record_value(r);
                            match record_value {
                                RecordValue::PartitionRecord(pr) => {
                                    let partition = DescribeTopicPartitionsResponsePartition::default()
                                        .with_partition_index(pr.partition_id);
                                    Some(partition)
                                }
                                _ => None
                            }
                        })
                        .collect();

                    let topic_response = DescribeTopicPartitionsResponseTopic::default()
                        .with_name(Some(topic_request.name.clone()))
                        .with_topic_id(topic_id)
                        .with_partitions(partitions);
                        // .with_error_code(ResponseError::UnknownTopicOrPartition.code())
                        // .with_topic_id(Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap());
                    resp = DescribeTopicPartitionsResponse::default()
                        .with_topics(vec![topic_response]);
                }
            }
            (ResponseKind::DescribeTopicPartitions(resp), DescribeTopicPartitionsResponse::header_version(api_version))

            
            // let record = &record_sets[0].records[0];
            // let mut record_value = record.value.clone().unwrap();
            // let topic_record = TopicRecord::decode(&mut record_value);
            // let partition = DescribeTopicPartitionsResponsePartition::default()
            //     .with_partition_index(record.sequence);

            // let topic_response = DescribeTopicPartitionsResponseTopic::default()
            //     .with_name(Some(topic_request.name.clone()))
            //     .with_topic_id(topic_record.topic_id)
            //     .with_partitions(partitions);
            //     // .with_error_code(ResponseError::UnknownTopicOrPartition.code())
            //     // .with_topic_id(Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap());
            // let resp = DescribeTopicPartitionsResponse::default()
            //     .with_topics(vec![topic_response]);
            // (ResponseKind::DescribeTopicPartitions(resp), DescribeTopicPartitionsResponse::header_version(api_version))
        }
        _ => panic!()
    };
    build_response(default_response_header(request_header.correlation_id), header_version, response, api_version)
}

async fn parse_cluster_metadata() -> Vec<RecordSet> {
    let filename = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    let bytes = fs::read(filename).await.unwrap();
    println!("file data: {:02x?}", bytes);
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&bytes);
    RecordBatchDecoder::decode_all(&mut buf).unwrap()
}