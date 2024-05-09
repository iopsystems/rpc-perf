use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum PublisherWorkItem {
    Publish {
        topic: Arc<String>,
        key: Vec<u8>,
        message: Vec<u8>,
    },
}
