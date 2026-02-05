use super::*;

#[derive(Clone, Deserialize)]
pub struct Storage {
    /// The number of connections this process will have to each endpoint.
    poolsize: usize,
    // number of threads for client tasks
    threads: usize,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
}

fn default_concurrency() -> usize {
    1
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
}
