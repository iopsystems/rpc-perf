// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;

#[derive(Clone, Deserialize)]
pub struct Pubsub {
    /// Connection timeout.
    connect_timeout: u64,
    /// Publish timeout
    publish_timeout: u64,
    // number of threads for publisher tasks
    publisher_threads: usize,
    // number of threads for subscriber tasks
    subscriber_threads: usize,

    publisher_poolsize: usize,
    publisher_concurrency: usize,
}

impl Pubsub {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout)
    }

    pub fn publish_timeout(&self) -> Duration {
        Duration::from_millis(self.publish_timeout)
    }

    pub fn publisher_threads(&self) -> usize {
        self.publisher_threads
    }

    pub fn subscriber_threads(&self) -> usize {
        self.subscriber_threads
    }

    pub fn publisher_poolsize(&self) -> usize {
        self.publisher_poolsize
    }

    pub fn publisher_concurrency(&self) -> usize {
        self.publisher_concurrency
    }
}
