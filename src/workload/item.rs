// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::collections::HashMap;
use std::sync::Arc;

#[allow(dead_code)]
pub enum WorkItem {
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
    HashGet {
        key: Arc<[u8]>,
        field: Arc<[u8]>,
    },
    HashIncrement {
        key: Arc<[u8]>,
        field: Arc<[u8]>,
        amount: i64,
    },
    HashMultiGet {
        key: Arc<[u8]>,
        fields: Vec<Arc<[u8]>>,
    },
    HashSet {
        key: Arc<[u8]>,
        data: HashMap<Arc<[u8]>, Arc<[u8]>>,
    },
    Reconnect,
    Replace {
        key: Arc<[u8]>,
        value: Arc<[u8]>,
    },
    Set {
        key: Arc<[u8]>,
        value: Arc<[u8]>,
    },
    SortedSetAdd {
        key: Arc<[u8]>,
        data: Vec<(Arc<[u8]>, f64)>,
    },
    SortedSetIncrement {
        key: Arc<[u8]>,
        member: Arc<[u8]>,
        amount: f64,
    },
    SortedSetRemove {
        key: Arc<[u8]>,
        data: Vec<Arc<[u8]>>,
    },
    Ping,
}
