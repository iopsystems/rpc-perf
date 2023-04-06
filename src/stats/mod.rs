// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

// for now, we use some of the stats defined in the protocol crates
pub use protocol_memcache::*;

use metriken::Lazy;
use metriken::counter;
use metriken::gauge;

type Duration = clocksource::Duration<clocksource::Nanoseconds<u64>>;

#[macro_export]
#[rustfmt::skip]
macro_rules! heatmap {
    ($ident:ident, $name:tt) => {
        #[metriken::metric(
            name = $name,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Heatmap> = metriken::Lazy::new(|| {
            metriken::Heatmap::new(0, 8, 64, Duration::from_secs(1), Duration::from_millis(100)).unwrap()
        });
    };
    ($ident:ident, $name:tt, $description:tt) => {
        #[metriken::metric(
            name = $name,
            description = $description,
            crate = metriken
        )]
        pub static $ident: Lazy<metriken::Heatmap> = metriken::Lazy::new(|| {
            metriken::Heatmap::new(0, 8, 64, Duration::from_secs(1), Duration::from_millis(100)).unwrap()
        });
    };
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

// #[macro_export]
// #[rustfmt::skip]
// macro_rules! gauge {
//     ($ident:ident, $name:tt) => {
//         #[metriken::metric(
//             name = $name,
//             crate = metriken
//         )]
//         pub static $ident: Lazy<metriken::Gauge> = metriken::Lazy::new(|| {
//             metriken::Gauge::new()
//         });
//     };
//     ($ident:ident, $name:tt, $description:tt) => {
//         #[metriken::metric(
//             name = $name,
//             crate = metriken
//         )]
//         pub static $ident: Lazy<metriken::Gauge> = metriken::Lazy::new(|| {
//             metriken::Gauge::new()
//         });
//     };
// }

// #[macro_export]
// #[rustfmt::skip]
// macro_rules! counter {
//     ($ident:ident, $name:tt) => {
//         #[metriken::metric(
//             name = $name,
//             crate = metriken
//         )]
//         pub static $ident: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
//             metriken::Counter::new()
//         });
//     };
//     ($ident:ident, $name:tt, $description:tt) => {
//         #[metriken::metric(
//             name = $name,
//             crate = metriken
//         )]
//         pub static $ident: Lazy<metriken::Counter> = metriken::Lazy::new(|| {
//             metriken::Counter::new()
//         });
//     };
// }

gauge!(CONNECT_CURR);
counter!(CONNECT_OK);
counter!(CONNECT_TIMEOUT);

counter!(REQUEST, "total requests dequeued");
counter!(
    REQUEST_OK,
    "requests that were successfully generated and sent"
);
counter!(REQUEST_RECONNECT, "requests to reconnect");
counter!(
    REQUEST_UNSUPPORTED,
    "skipped requests due to protocol incompatibility"
);

counter!(
    RESPONSE_EX,
    "responses which encountered some exception while processing"
);
counter!(
    RESPONSE_RATELIMITED,
    "backend indicated that we were ratelimited"
);
counter!(
    RESPONSE_BACKEND_TIMEOUT,
    "responses indicating the backend timedout"
);
counter!(RESPONSE_OK, "responses which were successful");
counter!(RESPONSE_TIMEOUT, "responses not received due to timeout");
counter!(
    RESPONSE_INVALID,
    "responses that were invalid for the protocol"
);

counter!(RESPONSE_HIT, "read responses which were a hit");
counter!(RESPONSE_MISS, "read responses which were a miss");

// augment the get stats
counter!(GET_OK, "get requests that were successful");
counter!(GET_TIMEOUT, "get requests that resulted in timeout");

// augment the set stats
counter!(SET_TIMEOUT, "set requests that resulted in timeout");

// augment the delete stats
counter!(DELETE_OK, "delete requests that were successful");
counter!(DELETE_TIMEOUT, "delete requests that resulted in timeout");

counter!(HASH_GET);
counter!(HASH_GET_EX);
counter!(HASH_GET_FIELD_HIT);
counter!(HASH_GET_FIELD_MISS);
counter!(HASH_GET_OK);
counter!(HASH_GET_TIMEOUT);

counter!(HASH_GET_ALL, "requests to get all fields from a hash");
counter!(
    HASH_GET_ALL_EX,
    "requests to get all fields from a hash that resulted in an exception"
);
counter!(
    HASH_GET_ALL_HIT,
    "requests to get all fields from a hash where the hash was found"
);
counter!(
    HASH_GET_ALL_MISS,
    "requests to get all fields from a hash where the hash was not found"
);
counter!(
    HASH_GET_ALL_OK,
    "requests to get all fields from a hash that were successful"
);
counter!(
    HASH_GET_ALL_TIMEOUT,
    "requests to get all fields from a hash that timed out"
);

// `PING` counter comes from protocol-ping for now
counter!(PING);
counter!(PING_EX);
counter!(PING_OK);

counter!(CONNECT);
counter!(CONNECT_EX);

counter!(SESSION);
counter!(SESSION_CLOSED_CLIENT);
counter!(SESSION_CLOSED_SERVER);

/*
 * HASHES (DICTIONARIES)
 */
counter!(HASH_SET);
counter!(HASH_SET_EX);
counter!(HASH_SET_OK);
counter!(HASH_SET_TIMEOUT);

counter!(HASH_DELETE);
counter!(HASH_DELETE_EX);
counter!(HASH_DELETE_OK);
counter!(HASH_DELETE_TIMEOUT);

counter!(HASH_INCR);
counter!(HASH_INCR_EX);
counter!(HASH_INCR_HIT);
counter!(HASH_INCR_MISS);
counter!(HASH_INCR_OK);
counter!(HASH_INCR_TIMEOUT);

counter!(HASH_EXISTS);
counter!(HASH_EXISTS_EX);
counter!(HASH_EXISTS_HIT);
counter!(HASH_EXISTS_MISS);

/*
 * LISTS
 */
counter!(LIST_FETCH);
counter!(LIST_FETCH_EX);
counter!(LIST_FETCH_OK);
counter!(LIST_FETCH_TIMEOUT);
counter!(LIST_LENGTH);
counter!(LIST_LENGTH_EX);
counter!(LIST_LENGTH_OK);
counter!(LIST_LENGTH_TIMEOUT);
counter!(LIST_POP_BACK);
counter!(LIST_POP_BACK_EX);
counter!(LIST_POP_BACK_OK);
counter!(LIST_POP_BACK_TIMEOUT);
counter!(LIST_POP_FRONT);
counter!(LIST_POP_FRONT_EX);
counter!(LIST_POP_FRONT_OK);
counter!(LIST_POP_FRONT_TIMEOUT);
counter!(LIST_PUSH_BACK);
counter!(LIST_PUSH_BACK_EX);
counter!(LIST_PUSH_BACK_OK);
counter!(LIST_PUSH_BACK_TIMEOUT);
counter!(LIST_PUSH_FRONT);
counter!(LIST_PUSH_FRONT_EX);
counter!(LIST_PUSH_FRONT_OK);
counter!(LIST_PUSH_FRONT_TIMEOUT);

/*
 * SETS
 */

counter!(SET_ADD);
counter!(SET_ADD_EX);
counter!(SET_ADD_OK);
counter!(SET_ADD_TIMEOUT);
counter!(SET_MEMBERS);
counter!(SET_MEMBERS_EX);
counter!(SET_MEMBERS_OK);
counter!(SET_MEMBERS_TIMEOUT);
counter!(SET_REMOVE);
counter!(SET_REMOVE_EX);
counter!(SET_REMOVE_OK);
counter!(SET_REMOVE_TIMEOUT);

/*
 * SORTED SETS
 */

counter!(SORTED_SET_ADD);
counter!(SORTED_SET_ADD_EX);
counter!(SORTED_SET_ADD_OK);
counter!(SORTED_SET_ADD_TIMEOUT);
counter!(SORTED_SET_INCR);
counter!(SORTED_SET_INCR_EX);
counter!(SORTED_SET_INCR_OK);
counter!(SORTED_SET_INCR_TIMEOUT);
counter!(SORTED_SET_MEMBERS);
counter!(SORTED_SET_MEMBERS_EX);
counter!(SORTED_SET_MEMBERS_OK);
counter!(SORTED_SET_MEMBERS_TIMEOUT);
counter!(SORTED_SET_RANK);
counter!(SORTED_SET_RANK_EX);
counter!(SORTED_SET_RANK_OK);
counter!(SORTED_SET_RANK_TIMEOUT);
counter!(SORTED_SET_REMOVE);
counter!(SORTED_SET_REMOVE_EX);
counter!(SORTED_SET_REMOVE_OK);
counter!(SORTED_SET_REMOVE_TIMEOUT);
counter!(SORTED_SET_SCORE);
counter!(SORTED_SET_SCORE_EX);
counter!(SORTED_SET_SCORE_OK);
counter!(SORTED_SET_SCORE_TIMEOUT);

/*
 * PUBSUB
 */

counter!(PUBSUB_PUBLISH);
counter!(PUBSUB_PUBLISH_EX);
counter!(PUBSUB_PUBLISH_OK);
counter!(PUBSUB_PUBLISH_TIMEOUT);
counter!(PUBSUB_SUBSCRIBE);
counter!(PUBSUB_SUBSCRIBE_EX);
counter!(PUBSUB_SUBSCRIBE_OK);
counter!(PUBSUB_RECEIVE);
counter!(PUBSUB_RECEIVE_EX);
counter!(PUBSUB_RECEIVE_CLOSED);
counter!(PUBSUB_RECEIVE_OK);
