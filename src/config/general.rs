// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;

#[derive(Clone, Deserialize)]
pub struct General {
    /// The protocol to be used for the test.
    protocol: Protocol,
    /// The reporting interval in seconds.
    interval: u64,
    /// The test duration in seconds.
    duration: u64,
}

impl General {
    pub fn protocol(&self) -> Protocol {
        self.protocol
    }

    pub fn interval(&self) -> Duration {
        Duration::from_secs(self.interval)
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.duration)
    }
}
