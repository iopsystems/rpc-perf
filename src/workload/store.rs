use bytes::Bytes;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub struct Get {
    pub key: Arc<String>,
}

#[derive(Debug, PartialEq)]
pub struct Delete {
    pub key: Arc<String>,
}

#[derive(Debug, PartialEq)]
pub struct Put {
    /// For a PUT request to a store, keys will always be
    /// a `String` type.
    pub key: Arc<String>,
    pub value: Bytes,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum StoreClientRequest {
    // Key-Value
    Get(Get),
    Delete(Delete),
    Put(Put),

    Reconnect,
}
