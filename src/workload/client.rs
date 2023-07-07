use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, PartialEq)]
pub enum ClientWorkItem {
    Reconnect,
    Request {
        request: ClientRequest,
        sequence: u64,
    },
}

#[derive(Debug, PartialEq)]
pub struct Ping {}

#[derive(Debug, PartialEq)]
pub struct Add {
    pub key: Arc<[u8]>,
    pub value: Vec<u8>,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct Get {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct MultiGet {
    pub keys: Vec<Arc<[u8]>>,
}

#[derive(Debug, PartialEq)]
pub struct Delete {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct Replace {
    pub key: Arc<[u8]>,
    pub value: Vec<u8>,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct Set {
    pub key: Arc<[u8]>,
    pub value: Vec<u8>,
    pub ttl: Option<Duration>,
}

// Hash

#[derive(Debug, PartialEq)]
pub struct HashExists {
    pub key: Arc<[u8]>,
    pub field: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct HashDelete {
    pub key: Arc<[u8]>,
    pub fields: Vec<Arc<[u8]>>,
}

#[derive(Debug, PartialEq)]
pub struct HashGet {
    pub key: Arc<[u8]>,
    pub fields: Vec<Arc<[u8]>>,
}

#[derive(Debug, PartialEq)]
pub struct HashGetAll {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct HashIncrement {
    pub key: Arc<[u8]>,
    pub field: Arc<[u8]>,
    pub amount: i64,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct HashSet {
    pub key: Arc<[u8]>,
    pub data: HashMap<Arc<[u8]>, Vec<u8>>,
    pub ttl: Option<Duration>,
}

// List

#[derive(Debug, PartialEq)]
pub struct ListFetch {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct ListLength {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct ListPopBack {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct ListPopFront {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct ListPushFront {
    pub key: Arc<[u8]>,
    pub elements: Vec<Arc<[u8]>>,
    pub truncate: Option<u32>,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct ListPushBack {
    pub key: Arc<[u8]>,
    pub elements: Vec<Arc<[u8]>>,
    pub truncate: Option<u32>,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct ListRange {
    pub key: Arc<[u8]>,
    pub start: i64,
    pub stop: i64,
}

#[derive(Debug, PartialEq)]
pub struct ListRemove {
    pub key: Arc<[u8]>,
    pub element: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct ListStore {
    pub key: Arc<[u8]>,
    pub elements: Vec<Arc<[u8]>>,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct SetAdd {
    pub key: Arc<[u8]>,
    pub members: Vec<Arc<[u8]>>,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct SetMembers {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct SetRemove {
    pub key: Arc<[u8]>,
    pub members: Vec<Arc<[u8]>>,
}

#[derive(Debug, PartialEq)]
pub struct SortedSetAdd {
    pub key: Arc<[u8]>,
    pub members: Vec<(Arc<[u8]>, f64)>,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct SortedSetRange {
    pub key: Arc<[u8]>,
    pub start: Option<i32>,
    pub end: Option<i32>,
}

#[derive(Debug, PartialEq)]
pub struct SortedSetIncrement {
    pub key: Arc<[u8]>,
    pub member: Arc<[u8]>,
    pub amount: f64,
    pub ttl: Option<Duration>,
}

#[derive(Debug, PartialEq)]
pub struct SortedSetRank {
    pub key: Arc<[u8]>,
    pub member: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct SortedSetRemove {
    pub key: Arc<[u8]>,
    pub members: Vec<Arc<[u8]>>,
}

#[derive(Debug, PartialEq)]
pub struct SortedSetScore {
    pub key: Arc<[u8]>,
    pub members: Vec<Arc<[u8]>>,
}

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum ClientRequest {
    // Ping
    Ping(Ping),

    // Key-Value
    Add(Add),
    Get(Get),
    Delete(Delete),
    MultiGet(MultiGet),
    Replace(Replace),
    Set(Set),

    // Hash Commands
    HashExists(HashExists),
    HashDelete(HashDelete),
    /// Retrieve one or more fields from a hash.
    HashGet(HashGet),
    HashGetAll(HashGetAll),
    HashIncrement(HashIncrement),
    HashSet(HashSet),

    // List Commands
    /// Fetch all elements in a list. Equivalent to:
    /// `ListRange { key, start: 0, stop: -1 }`
    ListFetch(ListFetch),
    /// Return the length of a list.
    ListLength(ListLength),
    /// Remove and return the element at the back of a list.
    ListPopBack(ListPopBack),
    /// Remove and return the element at the front of a list.
    ListPopFront(ListPopFront),
    /// Push one or more elements to the back of a list.
    ListPushBack(ListPushBack),
    /// Push one or more elements to the front of a list.
    ListPushFront(ListPushFront),
    /// Return the elements of a list between the given indices.
    ListRange(ListRange),
    /// Remove all instances of an element from a list.
    ListRemove(ListRemove),
    /// Create or replace a list with a new list.
    ListStore(ListStore),

    Reconnect,

    SetAdd(SetAdd),
    SetMembers(SetMembers),
    SetRemove(SetRemove),

    // Sorted Set
    SortedSetAdd(SortedSetAdd),
    SortedSetIncrement(SortedSetIncrement),
    SortedSetRange(SortedSetRange),
    SortedSetRank(SortedSetRank),
    SortedSetRemove(SortedSetRemove),
    SortedSetScore(SortedSetScore),
}
