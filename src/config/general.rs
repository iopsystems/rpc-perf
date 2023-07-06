use sha2::{Digest, Sha512};
use rand_xoshiro::Seed512;
use super::*;

const DEFAULT_INITIAL_SEED: Seed512 = Seed512([
    0x98, 0x0e, 0x01, 0x71, 0x38, 0x0b, 0x54, 0xb4, 0xeb, 0x9a, 0xa2, 0x35, 0xe5, 0xc3, 0x4e, 0xef,
    0xa3, 0x3f, 0xd0, 0x2c, 0xc4, 0x3f, 0xd0, 0x99, 0xab, 0x49, 0xf4, 0x4e, 0x35, 0x63, 0xda, 0x82,
    0x67, 0xcc, 0xcb, 0xf3, 0x96, 0x56, 0x3d, 0x94, 0xd0, 0x74, 0xc1, 0x3c, 0x94, 0xde, 0xc9, 0xff,
    0xa1, 0x5b, 0x53, 0xd2, 0xb8, 0x4b, 0x57, 0xef, 0x7d, 0xe1, 0xc5, 0x3d, 0xbc, 0x46, 0xf9, 0x56,
]);

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
    /// The initial seed for initializing the PRNGs. This can be any string and
    /// we will hash it to determine a corresponding seed.
    initial_seed: Option<String>,
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

    pub fn initial_seed(&self) -> Seed512 {
        if let Some(initial_seed) = &self.initial_seed {
            let mut hasher = Sha512::new();
            hasher.update(initial_seed.as_bytes());
            Seed512(hasher.finalize().into())
        } else {
            DEFAULT_INITIAL_SEED
        }
    }
}
