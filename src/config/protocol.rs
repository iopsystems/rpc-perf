use super::*;

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Blabber,
    GrpcPing,
    Http1,
    Http2,
    Http2Ping,
    Http3Ping,
    Memcache,
    Momento,
    MomentoHttp,
    Ping,
    Resp,
    Kafka,
    S3,
}
