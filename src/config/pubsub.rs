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

    // kafka specific configs
    kafka_acks: Option<String>,
    kafka_linger_ms: Option<String>,
    kafka_batch_size: Option<String>,
    kafka_batch_num_messages: Option<String>,
    kafka_fetch_message_max_bytes: Option<String>,
    kafka_request_timeout_ms: Option<String>,
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

    pub fn kafka_acks(&self) -> &Option<String> {
        &self.kafka_acks
    }

    pub fn kafka_linger_ms(&self) -> &Option<String> {
        &self.kafka_linger_ms
    }

    pub fn kafka_batch_size(&self) -> &Option<String> {
        &self.kafka_batch_size
    }

    pub fn kafka_batch_num_messages(&self) -> &Option<String> {
        &self.kafka_batch_num_messages
    }

    pub fn kafka_fetch_message_max_bytes(&self) -> &Option<String> {
        &self.kafka_fetch_message_max_bytes
    }

    pub fn kafka_request_timeout_ms(&self) -> &Option<String> {
        &self.kafka_request_timeout_ms
    }
}
