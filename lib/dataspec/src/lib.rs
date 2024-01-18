//! Format of JSON output from rpc-perf. These structures can be used
//! by any consumer of the produced data to parse the files.
use histogram::SparseHistogram;
use serde::{Deserialize, Serialize};

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Connections))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Connections {
    /// number of current connections (gauge)
    pub current: i64,
    /// number of total connect attempts
    pub total: u64,
    /// number of connections established
    pub opened: u64,
    /// number of connect attempts that failed
    pub error: u64,
    /// number of connect attempts that hit timeout
    pub timeout: u64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Requests))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Requests {
    pub total: u64,
    pub ok: u64,
    pub reconnect: u64,
    pub unsupported: u64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Responses))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Responses {
    /// total number of responses
    pub total: u64,
    /// number of responses that were successful
    pub ok: u64,
    /// number of responses that were unsuccessful
    pub error: u64,
    /// number of responses that were missed due to timeout
    pub timeout: u64,
    /// number of read requests with a hit response
    pub hit: u64,
    /// number of read requests with a miss response
    pub miss: u64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::ClientStats))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ClientStats {
    pub connections: Connections,
    pub requests: Requests,
    pub responses: Responses,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_latency: Option<SparseHistogram>,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::PubsubStats))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PubsubStats {
    pub publishers: Publishers,
    pub subscribers: Subscribers,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_latency: Option<SparseHistogram>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_latency: Option<SparseHistogram>,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Publishers))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Publishers {
    // current number of publishers
    pub current: i64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::Subscribers))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Subscribers {
    // current number of subscribers
    pub current: i64,
}

#[derive(Default, Deserialize, Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "utoipa", schema(as = rpcperf_dataspec::JsonSnapshot))]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct JsonSnapshot {
    pub window: u64,
    pub elapsed: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_qps: Option<f64>,
    pub client: ClientStats,
    pub pubsub: PubsubStats,
}
