// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;

#[derive(Clone, Deserialize)]
pub struct Client {
    /// The number of connections this process will have to each endpoint.
    poolsize: usize,
    /// The number of concurrent sessions per connection.
    #[serde(default)]
    concurrency: usize,
    /// Connection timeout.
    connect_timeout: u64,
    /// Request timeout
    request_timeout: u64,
    // number of threads for client tasks
    threads: usize,
    /// Specifies the rate at which connections should randomly reconnect. This
    /// is useful to model steady-state connect pressure on a backend.
    #[serde(default)]
    reconnect_rate: u64,
}

impl Client {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout)
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout)
    }

    pub fn poolsize(&self) -> usize {
        std::cmp::max(1, self.poolsize)
    }

    pub fn concurrency(&self) -> usize {
        std::cmp::max(1, self.concurrency)
    }

    pub fn reconnect_rate(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.reconnect_rate)
    }
}
