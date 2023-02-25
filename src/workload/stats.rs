// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

// for now, we use some of the stats defined in the memcache protocol crate
pub use protocol_memcache::*;

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

counter!(HASH_SET);
counter!(HASH_SET_EX);
counter!(HASH_SET_STORED);
counter!(HASH_SET_TIMEOUT);

counter!(HASH_DELETE);
counter!(HASH_DELETE_OK);
counter!(HASH_DELETE_EX);
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

counter!(PING);
counter!(PING_EX);
counter!(PING_OK);

counter!(CONNECT);
counter!(CONNECT_EX);

counter!(SORTED_SET_INCR);
counter!(SORTED_SET_INCR_EX);
counter!(SORTED_SET_INCR_OK);
counter!(SORTED_SET_INCR_TIMEOUT);

counter!(SORTED_SET_ADD);
counter!(SORTED_SET_ADD_EX);
counter!(SORTED_SET_ADD_OK);
counter!(SORTED_SET_ADD_TIMEOUT);

counter!(SORTED_SET_RANK);
counter!(SORTED_SET_RANK_EX);
counter!(SORTED_SET_RANK_OK);
counter!(SORTED_SET_RANK_TIMEOUT);

counter!(SORTED_SET_SCORE);
counter!(SORTED_SET_SCORE_EX);
counter!(SORTED_SET_SCORE_OK);
counter!(SORTED_SET_SCORE_TIMEOUT);

counter!(SORTED_SET_REMOVE);
counter!(SORTED_SET_REMOVE_EX);
counter!(SORTED_SET_REMOVE_OK);
counter!(SORTED_SET_REMOVE_TIMEOUT);

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
counter!(RESPONSE_OK, "responses which were successful");
counter!(RESPONSE_TIMEOUT, "responses not received due to timeout");
counter!(
    RESPONSE_INVALID,
    "responses that were invalid for the protocol"
);
