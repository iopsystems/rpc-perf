use super::*;

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Memcache,
    Momento,
    Ping,
    Resp,
}
