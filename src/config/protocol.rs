use super::*;

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Blabber,
    Memcache,
    Momento,
    MomentoHttp,
    Mysql,
    Ping,
    PingGrpc,
    PingGrpcH2,
    PingGrpcH3,
    Resp,
    Kafka,
    S3,
}
