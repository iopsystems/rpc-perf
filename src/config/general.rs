use super::*;

#[derive(Clone, Deserialize)]
pub struct General {
    /// The protocol to be used for the test.
    protocol: Protocol,
    /// The reporting interval in seconds.
    interval: u64,
    /// The test duration in seconds.
    duration: u64,
    /// Optional path to output JSON metrics
    #[serde(default)]
    json_output: Option<String>,
    /// The admin listen address
    admin: String,
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

    pub fn json_output(&self) -> Option<String> {
        self.json_output.clone()
    }

    pub fn admin(&self) -> String {
        self.admin.clone()
    }
}

// #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Deserialize)]
// #[serde(rename_all = "snake_case")]
// #[serde(deny_unknown_fields)]
// pub enum OutputFormat {
//     Log,
//     Json,
// }

// impl Default for OutputFormat {
//     fn default() -> Self {
//         Self::Log
//     }
// }
