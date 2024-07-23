use super::*;

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Blabber,
    GrpcPing,
    Http1,
    Http2,
    Http2Ping,
    Memcache,
    Momento,
    Ping,
    Resp,
    Kafka,
}
