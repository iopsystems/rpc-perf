// for now, we use some of the metrics defined in the protocol crates

pub use protocol_memcache::*;

use ahash::HashMap;
use ahash::HashMapExt;
use metriken::Lazy;
use paste::paste;
use std::concat;
use std::time::SystemTime;

pub static PERCENTILES: &[(&str, f64)] = &[
    ("p25", 25.0),
    ("p50", 50.0),
    ("p75", 75.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p999", 99.9),
    ("p9999", 99.99),
];

pub struct MetricsSnapshot {
    pub current: SystemTime,
    pub previous: SystemTime,
    pub counters: CountersSnapshot,
    pub histograms: HistogramsSnapshot,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsSnapshot {
    pub fn new() -> Self {
        let now = SystemTime::now();

        Self {
            current: now,
            previous: now,
            counters: Default::default(),
            histograms: Default::default(),
        }
    }

    pub fn update(&mut self) {
        self.previous = self.current;
        self.current = SystemTime::now();

        self.counters.update();
        self.histograms.update();
    }

    pub fn percentiles(&self, name: &str) -> Vec<(String, f64, u64)> {
        self.histograms.percentiles(name)
    }

    pub fn histogram_delta(&self, name: &str) -> Option<&metriken::histogram::Snapshot> {
        self.histograms.deltas.get(name)
    }

    pub fn counter_rate(&self, name: &str) -> f64 {
        self.counter_delta(name) as f64
            / (self.current.duration_since(self.previous).unwrap()).as_secs_f64()
    }

    pub fn counter_delta(&self, name: &str) -> u64 {
        let current = self.counters.current.get(name);

        if current.is_none() {
            return 0;
        }

        let previous = self.counters.previous.get(name).unwrap_or(&0);

        current.unwrap() - previous
    }
}

pub struct HistogramsSnapshot {
    pub previous: HashMap<String, metriken::histogram::Snapshot>,
    pub deltas: HashMap<String, metriken::histogram::Snapshot>,
}

impl Default for HistogramsSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl HistogramsSnapshot {
    pub fn new() -> Self {
        let mut current = HashMap::new();

        for metric in metriken::metrics().iter() {
            let any = if let Some(any) = metric.as_any() {
                any
            } else {
                continue;
            };

            if let Some(histogram) = any.downcast_ref::<metriken::AtomicHistogram>() {
                if let Some(snapshot) = histogram.snapshot() {
                    current.insert(metric.name().to_string(), snapshot);
                }
            }
        }

        let deltas = current.clone();

        Self {
            previous: current,
            deltas,
        }
    }

    pub fn update(&mut self) {
        for metric in metriken::metrics().iter() {
            let any = if let Some(any) = metric.as_any() {
                any
            } else {
                continue;
            };

            if let Some(histogram) = any.downcast_ref::<metriken::AtomicHistogram>() {
                let metric = metric.name().to_string();

                if let Some(snapshot) = histogram.snapshot() {
                    if let Some(previous) = self.previous.get(&metric) {
                        self.deltas
                            .insert(metric.clone(), snapshot.wrapping_sub(previous).unwrap());
                    }

                    self.previous.insert(metric, snapshot);
                }
            }
        }
    }

    pub fn percentiles(&self, metric: &str) -> Vec<(String, f64, u64)> {
        let mut result = Vec::new();

        let percentiles: Vec<f64> = PERCENTILES
            .iter()
            .map(|(_, percentile)| *percentile)
            .collect();

        if let Some(snapshot) = self.deltas.get(metric) {
            if let Ok(percentiles) = snapshot.percentiles(&percentiles) {
                for ((label, _), (percentile, bucket)) in PERCENTILES.iter().zip(percentiles.iter())
                {
                    result.push((label.to_string(), *percentile, bucket.end()));
                }
            }
        }

        result
    }
}

#[derive(Clone)]
pub struct CountersSnapshot {
    pub current: HashMap<String, u64>,
    pub previous: HashMap<String, u64>,
}

impl Default for CountersSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

impl CountersSnapshot {
    pub fn new() -> Self {
        let mut current = HashMap::new();
        let previous = HashMap::new();

        for metric in metriken::metrics().iter() {
            let any = if let Some(any) = metric.as_any() {
                any
            } else {
                continue;
            };

            let metric = metric.name().to_string();

            if let Some(_counter) = any.downcast_ref::<metriken::Counter>() {
                current.insert(metric.clone(), 0);
            }
        }
        Self { current, previous }
    }

    pub fn update(&mut self) {
        for metric in metriken::metrics().iter() {
            let any = if let Some(any) = metric.as_any() {
                any
            } else {
                continue;
            };

            if let Some(counter) = any.downcast_ref::<metriken::Counter>() {
                if let Some(old_value) = self
                    .current
                    .insert(metric.name().to_string(), counter.value())
                {
                    self.previous.insert(metric.name().to_string(), old_value);
                }
            }
        }
    }
}

#[macro_export]
#[rustfmt::skip]
macro_rules! counter {
    ($ident:ident, $name:tt) => {
        #[metriken::metric(
            name = $name,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Counter> =
            metriken::Lazy::new(|| metriken::Counter::new());
        paste! {
            #[allow(dead_code)]
            pub static [<$ident _COUNTER>]: &'static str = $name;
        }
    };
    ($ident:ident, $name:tt, $description:tt) => {
        #[metriken::metric(
                                            name = $name,
                                            description = $description,
                                            crate = metriken
                                        )]
        pub static $ident: Lazy<metriken::Counter> =
            metriken::Lazy::new(|| metriken::Counter::new());
        paste! {
            #[allow(dead_code)]
            pub static [<$ident _COUNTER>]: &'static str = $name;
        }
    };
}

#[macro_export]
#[rustfmt::skip]
macro_rules! gauge {
    ($ident:ident, $name:tt) => {
        #[metriken::metric(
            name = $name,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Gauge> = metriken::Lazy::new(|| metriken::Gauge::new());
        paste! {
            #[allow(dead_code)]
            pub static [<$ident _GAUGE>]: &'static str = $name;
        }
    };
    ($ident:ident, $name:tt, $description:tt) => {
        #[metriken::metric(
            name = $name,
            description = $description,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Gauge> = metriken::Lazy::new(|| metriken::Gauge::new());
        paste! {
            pub static [<$ident _GAUGE>]: &'static str = $name;
        }
    };
}

#[macro_export]
#[rustfmt::skip]
macro_rules! histogram {
    ($ident:ident, $name:tt) => {
        #[metriken::metric(
            name = $name,
            crate = metriken
        )]
        pub static $ident: metriken::AtomicHistogram = metriken::AtomicHistogram::new(
            7,
            64,
        );
        paste! {
            pub static [<$ident _HISTOGRAM>]: &'static str = $name;
        }
    };
    ($ident:ident, $name:tt, $description:tt) => {
        #[metriken::metric(
            name = $name,
            description = $description,
            crate = metriken
        )]
        pub static $ident: metriken::AtomicHistogram = metriken::AtomicHistogram::new(
            7,
            64,
        );
        paste! {
            #[allow(dead_code)]
            pub static [<$ident _HISTOGRAM>]: &'static str = $name;
        }
    };
}

#[macro_export]
#[rustfmt::skip]
macro_rules! request {
    ($ident:ident, $name:tt) => {
        #[metriken::metric(
            name = concat!($name, "/total"),
            description = concat!("The total number of ", $name, " requests"),
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
            metriken::Counter::new()
        });
        paste! {
            #[allow(dead_code)]
            pub static [<$ident _COUNTER>]: &'static str = concat!($name, "/total");
        }

        paste! {
            #[metriken::metric(
                name = concat!($name, "/exception"),
                description = concat!("The number of ", $name, " requests that resulted in an exception"),
                crate = metriken
            )]
            pub static [<$ident _EX>]: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
                metriken::Counter::new()
            });
            paste! {
                #[allow(dead_code)]
                pub static [<$ident _EX_COUNTER>]: &'static str = concat!($name, "/exception");
            }   
        }

        paste! {
            #[metriken::metric(
                name = concat!($name, "/ok"),
                description = concat!("The number of ", $name, " requests that were successful"),
                crate = metriken
            )]
            pub static [<$ident _OK>]: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
                metriken::Counter::new()
            });
            paste! {
                #[allow(dead_code)]
                pub static [<$ident _OK_COUNTER>]: &'static str = concat!($name, "/ok");
            }
        }

        paste! {
            #[metriken::metric(
                name = concat!($name, "/timeout"),
                description = concat!("The number of ", $name, " requests that resulted in a timeout"),
                crate = metriken
            )]
            pub static [<$ident _TIMEOUT>]: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
                metriken::Counter::new()
            });
            paste! {
                #[allow(dead_code)]
                pub static [<$ident _TIMEOUT_COUNTER>]: &'static str = concat!($name, "/timeout");
            }
        }
    }
}

histogram!(
    RESPONSE_LATENCY,
    "response_latency",
    "distribution of response latencies in nanoseconds."
);

histogram!(
    SESSION_LIFECYCLE_REQUESTS,
    "session_lifecycle_requests",
    "distribution of requests per session lifecycle. incremented at time of session close."
);

histogram!(PUBSUB_LATENCY, "pubsub_latency");

histogram!(PUBSUB_PUBLISH_LATENCY, "pubsub_publish_latency");

gauge!(CONNECT_CURR, "client/connections/current");
counter!(CONNECT_OK, "client/connect/ok");
counter!(CONNECT_TIMEOUT, "client/connect/timeout");

counter!(REQUEST, "client/request/total", "total requests dequeued");
counter!(
    REQUEST_OK,
    "client/request/ok",
    "requests that were successfully generated and sent"
);
counter!(
    REQUEST_RECONNECT,
    "client/connect/reconnect",
    "requests to reconnect"
);
counter!(
    REQUEST_UNSUPPORTED,
    "client/request/unsupported",
    "skipped requests due to protocol incompatibility"
);

// Fused requests are when we need to send multiple commands to achieve some
// expected behavior. For example, a follow-up `EXPIRE` command to set a ttl
// when the command does not allow it. We track these separately, as we do not
// want them to count towards total QPS or success rate metrics. These are used
// as an indicator that additional requests are being issued for a particular
// workload + backend combination and if there are timeouts/errors that might
// result in the data in the cache getting into an unintended state.
counter!(
    FUSED_REQUEST,
    "client/fused_request/total",
    "total number of fused requests sent"
);
counter!(
    FUSED_REQUEST_OK,
    "client/fused_request/ok",
    "fused requests that completed successfully"
);
counter!(
    FUSED_REQUEST_TIMEOUT,
    "client/fused_request/timeout",
    "number of fused requests that failed due to timeout"
);

counter!(
    FUSED_REQUEST_EX,
    "client/fused_request/exception",
    "number of fused requests that failed with some error"
);

counter!(
    RESPONSE_EX,
    "client/response/exception",
    "responses which encountered some exception while processing"
);
counter!(
    RESPONSE_RATELIMITED,
    "client/response/ratelimited",
    "backend indicated that we were ratelimited"
);
counter!(
    RESPONSE_BACKEND_TIMEOUT,
    "client/response/backend_timeout",
    "responses indicating the backend timedout"
);
counter!(
    RESPONSE_OK,
    "client/response/ok",
    "responses which were successful"
);
counter!(
    RESPONSE_TIMEOUT,
    "client/response/timeout",
    "responses not received due to timeout"
);
counter!(
    RESPONSE_INVALID,
    "client/response/invalid",
    "responses that were invalid for the protocol"
);

counter!(RESPONSE_HIT, "client/response/hit");
counter!(RESPONSE_MISS, "client/response/miss");

// augment the add stats
counter!(
    ADD_TIMEOUT,
    "add/timeout",
    "add requests that resulted in timeout"
);

// augment the get stats
counter!(GET_OK, "get/ok", "get requests that were successful");
counter!(
    GET_TIMEOUT,
    "get/timeout",
    "get requests that resulted in timeout"
);

// augment the replace stats
counter!(
    REPLACE_TIMEOUT,
    "replace/timeout",
    "replace requests that resulted in timeout"
);

// augment the set stats
counter!(
    SET_TIMEOUT,
    "set/timeout",
    "set requests that resulted in timeout"
);

// augment the delete stats
counter!(
    DELETE_OK,
    "delete/ok",
    "delete requests that were successful"
);
counter!(
    DELETE_TIMEOUT,
    "delete/timeout",
    "delete requests that resulted in timeout"
);

request!(HASH_GET, "hash_get");
counter!(HASH_GET_FIELD_HIT, "hash_get/field_hit");
counter!(HASH_GET_FIELD_MISS, "hash_get/field_miss");

request!(HASH_GET_ALL, "hash_get_all");
counter!(HASH_GET_ALL_HIT, "hash_get_all/hit");
counter!(HASH_GET_ALL_MISS, "hash_get_all/miss");

counter!(CONNECT, "client/connect/total");
counter!(CONNECT_EX, "client/connect/exception");

counter!(SESSION, "client/session/total");
counter!(SESSION_CLOSED_CLIENT, "client/session/client_closed");
counter!(SESSION_CLOSED_SERVER, "client/session/server_closed");

/*
 * PING
 */
request!(PING, "ping");

/*
 * HASHES (DICTIONARIES)
 */

request!(HASH_DELETE, "hash_delete");

request!(HASH_EXISTS, "hash_exists");
counter!(HASH_EXISTS_HIT, "hash_exists/hit");
counter!(HASH_EXISTS_MISS, "hash_exists/miss");

request!(HASH_INCR, "hash_incr");
counter!(HASH_INCR_HIT, "hash_incr/hit");
counter!(HASH_INCR_MISS, "hash_incr/miss");

request!(HASH_SET, "hash_set");

/*
 * LISTS
 */

request!(LIST_FETCH, "list_fetch");

request!(LIST_LENGTH, "list_length");

request!(LIST_POP_BACK, "list_pop_back");

request!(LIST_POP_FRONT, "list_pop_front");

request!(LIST_PUSH_BACK, "list_push_back");

request!(LIST_PUSH_FRONT, "list_push_front");

/*
 * SETS
 */

request!(SET_ADD, "set_add");

request!(SET_MEMBERS, "set_members");

request!(SET_REMOVE, "set_remove");

/*
 * SORTED SETS
 */

request!(SORTED_SET_ADD, "sorted_set_add");

request!(SORTED_SET_INCR, "sorted_set_incr");

request!(SORTED_SET_RANGE, "sorted_set_range");

request!(SORTED_SET_RANK, "sorted_set_rank");

request!(SORTED_SET_REMOVE, "sorted_set_remove");

request!(SORTED_SET_SCORE, "sorted_set_score");

/*
 * PUBSUB
 */

request!(PUBSUB_PUBLISH, "publisher/publish/total");
counter!(PUBSUB_PUBLISH_RATELIMITED, "publisher/publish/ratelimiter");

request!(PUBSUB_SUBSCRIBE, "subscriber/subscribe");

counter!(PUBSUB_PUBLISHER_CONNECT, "publisher/connect");
gauge!(PUBSUB_PUBLISHER_CURR, "publisher/current");

gauge!(PUBSUB_SUBSCRIBER_CURR, "subscriber/current");

counter!(PUBSUB_RECEIVE, "subscriber/receive/total");
counter!(PUBSUB_RECEIVE_EX, "subscriber/receive/exception");
counter!(PUBSUB_RECEIVE_CLOSED, "subscriber/receive/closed");
counter!(PUBSUB_RECEIVE_CORRUPT, "subscriber/receive/corrupt");
counter!(PUBSUB_RECEIVE_INVALID, "subscriber/receive/invalid");
counter!(PUBSUB_RECEIVE_OK, "subscriber/receive/ok");
