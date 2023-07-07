use super::*;
use rand::Rng;
use rand_xoshiro::Seed512;
use sha2::{Digest, Sha512};

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
            let mut rng = rand::thread_rng();
            let mut seed = [0_u8; 64];
            rng.fill(&mut seed);
            Seed512(seed)
        }
    }
}
