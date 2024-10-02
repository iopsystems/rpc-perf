use super::*;

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Blabber,
    Memcache,
    Momento,
    MomentoHttp,
    Ping,
    PingGrpc,
    PingHttp2,
    PingHttp3,
    Resp,
    Kafka,
    S3,
}
