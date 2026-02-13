use super::*;

fn default_concurrency() -> usize {
    1
}

#[derive(Clone, Deserialize)]
pub struct Storage {
    /// The number of connections this process will have to each endpoint.
    poolsize: usize,
    // number of threads for client tasks
    threads: usize,
    /// The number of concurrent sessions per connection (for H2 mux).
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    /// Request timeout in milliseconds.
    #[serde(default)]
    #[allow(dead_code)]
    request_timeout: Option<u64>,
}

impl Storage {
    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn poolsize(&self) -> usize {
        std::cmp::max(1, self.poolsize)
    }

    pub fn concurrency(&self) -> usize {
        std::cmp::max(1, self.concurrency)
    }

    #[allow(dead_code)]
    pub fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout.map(Duration::from_millis)
    }
}
