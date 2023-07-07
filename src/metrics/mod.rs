// for now, we use some of the metrics defined in the protocol crates
pub use protocol_memcache::*;

use ahash::HashMap;
use metriken::Lazy;
use paste::paste;
use std::concat;

type Duration = clocksource::Duration<clocksource::Nanoseconds<u64>>;

pub static PERCENTILES: &[(&str, f64)] = &[
    ("p25", 25.0),
    ("p50", 50.0),
    ("p75", 75.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p999", 99.9),
    ("p9999", 99.99),
];

pub struct Snapshot {
    pub prev: HashMap<Metrics, u64>,
}

#[derive(Eq, Hash, PartialEq, Copy, Clone)]
pub enum Metrics {
    Connect,
    ConnectOk,
    ConnectEx,
    ConnectTimeout,
    Request,
    RequestOk,
    RequestReconnect,
    RequestUnsupported,
    ResponseOk,
    ResponseEx,
    ResponseTimeout,
    ResponseHit,
    ResponseMiss,
    PubsubTx,
    PubsubTxEx,
    PubsubTxOk,
    PubsubTxTimeout,
    PubsubRx,
    PubsubRxEx,
    PubsubRxOk,
    PubsubRxCorrupt,
    PubsubRxInvalid,
}

impl Metrics {
    pub fn metric(&self) -> &metriken::Counter {
        match self {
            Self::Connect => &CONNECT,
            Self::ConnectOk => &CONNECT_OK,
            Self::ConnectEx => &CONNECT_EX,
            Self::ConnectTimeout => &CONNECT_TIMEOUT,
            Self::Request => &REQUEST,
            Self::RequestOk => &REQUEST_OK,
            Self::RequestReconnect => &REQUEST_RECONNECT,
            Self::RequestUnsupported => &REQUEST_UNSUPPORTED,
            Self::ResponseEx => &RESPONSE_EX,
            Self::ResponseOk => &RESPONSE_OK,
            Self::ResponseTimeout => &RESPONSE_TIMEOUT,
            Self::ResponseHit => &RESPONSE_HIT,
            Self::ResponseMiss => &RESPONSE_MISS,
            Self::PubsubTx => &PUBSUB_PUBLISH,
            Self::PubsubTxEx => &PUBSUB_PUBLISH_EX,
            Self::PubsubTxOk => &PUBSUB_PUBLISH_OK,
            Self::PubsubTxTimeout => &PUBSUB_PUBLISH_TIMEOUT,
            Self::PubsubRx => &PUBSUB_RECEIVE,
            Self::PubsubRxEx => &PUBSUB_RECEIVE_EX,
            Self::PubsubRxOk => &PUBSUB_RECEIVE_OK,
            Self::PubsubRxCorrupt => &PUBSUB_RECEIVE_CORRUPT,
            Self::PubsubRxInvalid => &PUBSUB_RECEIVE_INVALID,
        }
    }

    pub fn delta(&self, snapshot: &mut Snapshot) -> u64 {
        let curr = self.metric().value();
        let prev = snapshot.prev.insert(*self, curr).unwrap_or(0);
        curr.wrapping_sub(prev)
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
        pub static $ident: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
            metriken::Counter::new()
        });
    };
    ($ident:ident, $name:tt, $description:tt) => {
        #[metriken::metric(
            name = $name,
            description = $description,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
            metriken::Counter::new()
        });
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
        pub static $ident: Lazy<metriken::Gauge> = metriken::Lazy::new(|| {
            metriken::Gauge::new()
        });
    };
    ($ident:ident, $name:tt, $description:tt) => {
        #[metriken::metric(
            name = $name,
            description = $description,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Gauge> = metriken::Lazy::new(|| {
            metriken::Gauge::new()
        });
    };
}

#[macro_export]
#[rustfmt::skip]
macro_rules! heatmap {
    ($ident:ident, $name:tt) => {
        #[metriken::metric(
            name = $name,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Heatmap> = metriken::Lazy::new(|| {
            metriken::Heatmap::new(0, 8, 64, Duration::from_secs(60), Duration::from_secs(1), None, None).unwrap()
        });
    };
    ($ident:ident, $name:tt, $description:tt) => {
        #[metriken::metric(
            name = $name,
            description = $description,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Heatmap> = metriken::Lazy::new(|| {
            metriken::Heatmap::new(0, 8, 64, Duration::from_secs(60), Duration::from_secs(1), None, None).unwrap()
        });
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
            #[metriken::metric(
                name = concat!($name, "/exception"),
                description = concat!("The number of ", $name, " requests that resulted in an exception"),
                crate = metriken
            )]
            pub static [<$ident _EX>]: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
                metriken::Counter::new()
            });
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
        }
    }
}

heatmap!(
    REQUEST_LATENCY,
    "request_latency",
    "distribution of request latencies in nanoseconds. incremented at time of requests disbatch."
);

heatmap!(
    RESPONSE_LATENCY,
    "response_latency",
    "distribution of response latencies in nanoseconds. incremented at time of response receipt."
);

heatmap!(
    SESSION_LIFECYCLE_REQUESTS,
    "session_lifecycle_requests",
    "distribution of requests per session lifecycle. incremented at time of session close."
);

heatmap!(PUBSUB_LATENCY, "pubsub_latency");

heatmap!(PUBSUB_PUBLISH_LATENCY, "pubsub_publish_latency");

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

request!(PUBSUB_PUBLISH, "publisher/publish");
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
