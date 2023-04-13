use super::*;

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Http1,
    Http2,
    Memcache,
    Momento,
    Ping,
    Resp,
}
