use super::*;

#[derive(Clone, Deserialize)]
pub struct Pubsub {
    // connection timeout in ms
    connect_timeout: u64,
    // publish timeout in ms
    publish_timeout: u64,
    // number of threads for publisher tasks
    publisher_threads: usize,
    // number of threads for subscriber tasks
    subscriber_threads: usize,

    publisher_poolsize: usize,
    publisher_concurrency: usize,

    /// Specify the default sizes for the read and write buffers (in bytes).
    /// It is useful to increase the sizes if you expect to send and/or receive
    /// large requests/responses as part of the workload.
    ///
    /// The default is a 16KB for each the read and write buffers.
    ///
    /// Not all client implementations allow setting these values, so this is a
    /// best effort basis.
    #[serde(default = "default_buffer_size")]
    read_buffer_size: usize,
    #[serde(default = "default_buffer_size")]
    write_buffer_size: usize,
}

impl Pubsub {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout)
    }

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

    pub fn read_buffer_size(&self) -> usize {
        // rounds the read buffer size up to the next nearest multiple of the
        // pagesize
        std::cmp::max(1, self.read_buffer_size).div_ceil(PAGESIZE) * PAGESIZE
    }

    #[allow(dead_code)]
    pub fn write_buffer_size(&self) -> usize {
        // rounds the write buffer size up to the next nearest multiple of the
        // pagesize
        std::cmp::max(1, self.write_buffer_size).div_ceil(PAGESIZE) * PAGESIZE
    }
}
