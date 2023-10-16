use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum PublisherWorkItem {
    Publish {
        topic: Arc<String>,
        message: Vec<u8>,
    },
    KafkaMessage {
        topic: Arc<String>,
        partition: usize,
        key: Vec<u8>,
        message: Vec<u8>,
    },
}
