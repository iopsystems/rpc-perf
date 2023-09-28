use serde::{Deserialize, Serialize};

use histogram::Snapshot;

/// This histogram is a sparse, columnar representation of the regular
/// Histogram. It is significantly smaller than a regular Histogram
/// when a large number of buckets are zero, which is a frequent
/// occurence; consequently it is used as the serialization format
/// of the Histogram. It stores an individual vector for each field
/// of non-zero buckets. Assuming index[0] = n, (index[0], count[0])
/// corresponds to the nth bucket.
#[derive(Default, Serialize, Deserialize)]
pub struct Histogram {
    /// parameters representing the resolution and the range of
    /// the histogram tracking request latencies
    pub grouping_power: u8,
    pub max_value_power: u8,
    /// indices for the non-zero buckets in the histogram
    pub index: Vec<usize>,
    /// histogram bucket counts corresponding to the indices
    pub count: Vec<u64>,
}

impl From<Option<Snapshot>> for Histogram {
    fn from(snapshot: Option<Snapshot>) -> Self {
        if snapshot.is_none() {
            return Self {
                grouping_power: 7,
                max_value_power: 64,
                index: Vec::new(),
                count: Vec::new(),
            };
        }

        let snapshot = snapshot.unwrap();

        let mut index = Vec::new();
        let mut count = Vec::new();

        for (i, bucket) in snapshot
            .into_iter()
            .enumerate()
            .filter(|(_i, bucket)| bucket.count() != 0)
        {
            index.push(i);
            count.push(bucket.count());
        }

        let config = snapshot.config();
        Self {
            grouping_power: config.grouping_power(),
            max_value_power: config.max_value_power(),
            index,
            count,
        }
    }
}
