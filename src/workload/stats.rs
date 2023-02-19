// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

// counter!(GET);
// counter!(GET_EX);
// counter!(GET_KEY_HIT);
// counter!(GET_KEY_MISS);

pub use protocol_memcache::*;

counter!(SET);
counter!(SET_EX);
counter!(SET_STORED);

counter!(HASH_GET);
counter!(HASH_GET_EX);
counter!(HASH_GET_FIELD_HIT);
counter!(HASH_GET_FIELD_MISS);

counter!(HASH_SET);
counter!(HASH_SET_EX);
counter!(HASH_SET_STORED);

counter!(PING);
counter!(PING_EX);
counter!(PING_OK);

counter!(CONNECT);
counter!(CONNECT_EX);

// Total requests taken off the work queue
counter!(REQUEST);
// Requests that were successfully generated and sent
counter!(REQUEST_OK);
// Requests which were just reconnects
counter!(REQUEST_RECONNECT);
// Requests which were skipped due to protocol incompatibility
counter!(REQUEST_UNSUPPORTED);

// Responses which encountered some exception while processing
counter!(RESPONSE_EX);
// Responses which were successful
counter!(RESPONSE_OK);
// Responses not received due to timeout
counter!(RESPONSE_TIMEOUT);
// Responses that were unexpected for the protocol
counter!(RESPONSE_INVALID);
