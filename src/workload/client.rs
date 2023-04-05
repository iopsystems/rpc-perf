// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub enum ClientWorkItem {
    Add {
        key: Arc<[u8]>,
        value: Arc<[u8]>,
    },
    Get {
        key: Arc<[u8]>,
    },
    Delete {
        key: Arc<[u8]>,
    },
    HashExists {
        key: Arc<[u8]>,
        field: Arc<[u8]>,
    },
    HashDelete {
        key: Arc<[u8]>,
        fields: Vec<Arc<[u8]>>,
    },
    /// Retrieve one or more fields from a hash.
    HashGet {
        key: Arc<[u8]>,
        fields: Vec<Arc<[u8]>>,
    },
    HashGetAll {
        key: Arc<[u8]>,
    },
    HashIncrement {
        key: Arc<[u8]>,
        field: Arc<[u8]>,
        amount: i64,
    },
    HashSet {
        key: Arc<[u8]>,
        data: HashMap<Arc<[u8]>, Vec<u8>>,
    },
    /// Fetch all elements in a list. Equivalent to:
    /// `ListRange { key, start: 0, stop: -1 }`
    ListFetch {
        key: Arc<[u8]>,
    },
    /// Return the length of a list.
    ListLength {
        key: Arc<[u8]>,
    },
    /// Remove and return the element at the back of a list.
    ListPopBack {
        key: Arc<[u8]>,
    },
    /// Remove and return the element at the front of a list.
    ListPopFront {
        key: Arc<[u8]>,
    },
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
    Set {
        key: Arc<[u8]>,
        value: Vec<u8>,
    },
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
    }
}
