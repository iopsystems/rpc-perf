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

counter!(RESPONSE_EX);
counter!(RESPONSE_OK);
counter!(RESPONSE_TIMEOUT);
counter!(RESPONSE_INVALID);