use super::*;

#[derive(Clone, Copy, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Blabber,
    Memcache,
    Momento,
    MomentoHttp,
    MomentoObjectstore,
    MomentoProtosocket,
    Mysql,
    Ping,
    PingGrpc,
    PingGrpcH2,
    PingGrpcH3,
    Resp,
    S3,
}
