use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub struct Ping {}

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
    /// For a Momento PUT request to a store, keys will always be
    /// a `String` type.
    pub key: Arc<String>,
    pub value: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum StoreClientRequest {
    // Ping
    Ping(Ping),

    // Key-Value
    Get(Get),
    Delete(Delete),
    Put(Put),

    Reconnect,
}
