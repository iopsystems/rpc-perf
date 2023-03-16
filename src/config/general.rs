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
    /// The output type: log or json
    #[serde(default)]
    output_format: OutputFormat,
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

    pub fn output_format(&self) -> OutputFormat {
        self.output_format
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum OutputFormat {
    Log,
    Json,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Log
    }
}
