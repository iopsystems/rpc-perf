/// # Ping Clients
///
/// Ping clients are a class of simple PING/PONG clients over various forms of
/// transport. They allow characterization of network performance for small
/// payload sizes and can be used to compare various types of transport and the
/// effect they have on performance.
///
/// RPC-Perf ping clients help measure the throughput and latency of fairly
/// minimal network services.
pub mod ascii;
pub mod grpc;

