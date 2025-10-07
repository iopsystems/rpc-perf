use super::*;

#[derive(Clone, Deserialize)]
pub struct Storage {
    /// The number of connections this process will have to each endpoint.
    poolsize: usize,
    // number of threads for client tasks
    threads: usize,
}

impl Storage {
    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn poolsize(&self) -> usize {
        std::cmp::max(1, self.poolsize)
    }
}
