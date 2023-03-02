pub use super::*;

#[derive(Clone, Deserialize)]
pub struct Request {
    // number of threads to drive requests
    threads: usize,
    // milliseconds
    timeout: u64,
    // zero is treated as unlimited
    #[serde(default)]
    ratelimit: u64,
}

impl Request {
    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout)
    }

    pub fn ratelimit(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.ratelimit)
    }
}
