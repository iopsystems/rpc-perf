use super::*;

#[derive(Clone, Deserialize)]
pub struct Client {
    /// The number of connections this process will have to each endpoint.
    poolsize: usize,
    /// The number of concurrent sessions per connection.
    #[serde(default)]
    concurrency: usize,
    /// Connection timeout.
    connect_timeout: u64,
    /// Request timeout
    request_timeout: u64,
    // number of threads for client tasks
    threads: usize,
    /// Specifies the rate at which connections should randomly reconnect. This
    /// is useful to model steady-state connect pressure on a backend.
    #[serde(default)]
    reconnect_rate: u64,

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

impl Client {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout)
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout)
    }

    pub fn poolsize(&self) -> usize {
        std::cmp::max(1, self.poolsize)
    }

    pub fn concurrency(&self) -> usize {
        std::cmp::max(1, self.concurrency)
    }

    pub fn reconnect_rate(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.reconnect_rate)
    }

    pub fn read_buffer_size(&self) -> usize {
        // rounds the read buffer size up to the next nearest multiple of the
        // pagesize
        ((std::cmp::max(1, self.read_buffer_size) + PAGESIZE - 1) / PAGESIZE) * PAGESIZE
    }

    pub fn write_buffer_size(&self) -> usize {
        // rounds the write buffer size up to the next nearest multiple of the
        // pagesize
        ((std::cmp::max(1, self.write_buffer_size) + PAGESIZE - 1) / PAGESIZE) * PAGESIZE
    }
}
