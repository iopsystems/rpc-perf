use super::*;

#[derive(Clone, Deserialize)]
pub struct Leaderboard {
    /// The number of connections this process will have to each endpoint.
    poolsize: usize,
    /// The number of concurrent sessions per connection.
    #[serde(default)]
    concurrency: usize,
    /// Request timeout
    request_timeout: u64,
    // number of threads for client tasks
    threads: usize,
}

impl Leaderboard {
    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn poolsize(&self) -> usize {
        std::cmp::max(1, self.poolsize)
    }

    pub fn concurrency(&self) -> usize {
        std::cmp::max(1, self.concurrency)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout)
    }
}
