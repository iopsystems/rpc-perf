// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

#[allow(dead_code)]
pub enum WorkItem {
    Get {
        key: Arc<String>,
    },
    HashExists {
        key: Arc<String>,
        field: Arc<String>,
    },
    HashDelete {
        key: Arc<String>,
        fields: Vec<Arc<String>>,
    },
    HashGet {
        key: Arc<String>,
        field: Arc<String>,
    },
    HashMultiGet {
        key: Arc<String>,
        fields: Vec<Arc<String>>,
    },
    HashSet {
        key: Arc<String>,
        field: Arc<String>,
        value: String,
    },
    Set {
        key: Arc<String>,
        value: String,
    },
    Ping,
}
