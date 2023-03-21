// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Http1,
    Memcache,
    Momento,
    Ping,
    Resp,
}
