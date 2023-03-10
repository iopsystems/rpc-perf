// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;

#[derive(Clone, Deserialize)]
pub struct Connection {
    /// The total number of connections this process will have to each endpoint.
    poolsize: usize,
    /// Connection timeout.
    timeout: u64,
    /// Connection ratelimit. Useful when there's a large number of connections
    /// and they need to be established slowly.
    #[serde(default)]
    ratelimit: u64,
    /// Specifies the rate at which connections should randomly reconnect. This
    /// is useful to model steady-state connect pressure on a backend.
    #[serde(default)]
    reconnect_rate: u64,
}

impl Connection {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout)
    }

    pub fn poolsize(&self) -> usize {
        self.poolsize
    }

    pub fn reconnect_rate(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.reconnect_rate)
    }
}
