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
pub struct Add {
    pub key: Arc<[u8]>,
    pub value: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct Get {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct Delete {
    pub key: Arc<[u8]>,
}

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
pub struct Set {
    pub key: Arc<[u8]>,
    pub value: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub struct HashGet  {
    pub key: Arc<[u8]>,
    pub fields: Vec<Arc<[u8]>>,
}

#[derive(Debug, PartialEq)]
pub struct HashGetAll  {
    pub key: Arc<[u8]>,
}

#[derive(Debug, PartialEq)]
pub struct HashIncrement {
    pub key: Arc<[u8]>,
    pub field: Arc<[u8]>,
    pub amount: i64,
}

#[derive(Debug, PartialEq)]
pub struct HashSet {
    pub key: Arc<[u8]>,
    pub data: HashMap<Arc<[u8]>, Vec<u8>>,
}

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

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum ClientRequest {
    Add(Add),
    Get(Get),
    Delete(Delete),

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
    ListPushBack {
        key: Arc<[u8]>,
        elements: Vec<Arc<[u8]>>,
        truncate: Option<u32>,
    },
    /// Push one or more elements to the front of a list.
    ListPushFront {
        key: Arc<[u8]>,
        elements: Vec<Arc<[u8]>>,
        truncate: Option<u32>,
    },
    /// Return the elements of a list between the given indices.
    ListRange {
        key: Arc<[u8]>,
        start: i64,
        stop: i64,
    },
    /// Remove all instances of an element from a list.
    ListRemove {
        key: Arc<[u8]>,
        element: Arc<[u8]>,
    },
    /// Create or replace a list with a new list.
    ListStore {
        key: Arc<[u8]>,
        elements: Vec<Arc<[u8]>>,
    },


    MultiGet {
        keys: Vec<Arc<[u8]>>,
    },
    Reconnect,
    Replace {
        key: Arc<[u8]>,
        value: Arc<[u8]>,
    },
    Set(Set),
    SetAdd {
        key: Arc<[u8]>,
        members: Vec<Arc<[u8]>>,
    },
    SetMembers {
        key: Arc<[u8]>,
    },
    SetRemove {
        key: Arc<[u8]>,
        members: Vec<Arc<[u8]>>,
    },
    SortedSetAdd {
        key: Arc<[u8]>,
        members: Vec<(Arc<[u8]>, f64)>,
    },
    SortedSetMembers {
        key: Arc<[u8]>,
    },
    SortedSetIncrement {
        key: Arc<[u8]>,
        member: Arc<[u8]>,
        amount: f64,
    },
    SortedSetRank {
        key: Arc<[u8]>,
        member: Arc<[u8]>,
    },
    SortedSetRemove {
        key: Arc<[u8]>,
        members: Vec<Arc<[u8]>>,
    },
    SortedSetScore {
        key: Arc<[u8]>,
        members: Vec<Arc<[u8]>>,
    },
    Ping,
    Publish {
        topic: Arc<String>,
        message: Vec<u8>,
    },
}
