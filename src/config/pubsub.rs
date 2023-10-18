use super::*;

#[derive(Clone, Deserialize)]
pub struct Pubsub {
    /// Publish timeout
    publish_timeout: u64,
    // number of threads for publisher tasks
    publisher_threads: usize,
    // number of threads for subscriber tasks
    subscriber_threads: usize,
    publisher_poolsize: usize,
    publisher_concurrency: usize,
}

impl Pubsub {
    pub fn publish_timeout(&self) -> Duration {
        Duration::from_millis(self.publish_timeout)
    }

    pub fn publisher_threads(&self) -> usize {
        self.publisher_threads
    }

    pub fn subscriber_threads(&self) -> usize {
        self.subscriber_threads
    }

    pub fn publisher_poolsize(&self) -> usize {
        self.publisher_poolsize
    }

    pub fn publisher_concurrency(&self) -> usize {
        self.publisher_concurrency
    }
}
