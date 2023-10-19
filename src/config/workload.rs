use super::*;

fn one() -> usize {
    1
}

#[derive(Clone, Deserialize)]
pub struct Workload {
    #[serde(default)]
    keyspace: Vec<Keyspace>,
    #[serde(default)]
    topics: Vec<Topics>,
    threads: usize,
    #[serde(default)]
    // zero is treated as unlimited
    ratelimit: u64,
}

#[derive(Clone, Deserialize, Copy, Debug, Ord, Eq, PartialOrd, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ValueKind {
    I64,
    Bytes,
}

impl Workload {
    pub fn keyspaces(&self) -> &[Keyspace] {
        &self.keyspace
    }

    pub fn topics(&self) -> &[Topics] {
        &self.topics
    }

    pub fn threads(&self) -> usize {
        self.threads
    }

    pub fn ratelimit(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.ratelimit)
    }
}

#[derive(Clone, Deserialize)]
pub struct Topics {
    topics: usize,
    #[serde(default = "one")]
    partitions: usize,
    topic_len: usize,
    #[serde(default)]
    topic_names: Vec<String>,
    message_len: usize,
    #[serde(default = "one")]
    key_len: usize,
    weight: usize,
    subscriber_poolsize: usize,
    #[serde(default = "one")]
    subscriber_concurrency: usize,
    #[serde(default)]
    topic_distribution: Distribution,
    #[serde(default)]
    partition_distribution: Distribution,
}

impl Topics {
    pub fn weight(&self) -> usize {
        self.weight
    }

    pub fn partitions(&self) -> usize {
        self.partitions
    }

    pub fn topics(&self) -> usize {
        self.topics
    }

    pub fn topic_names(&self) -> &[String] {
        &self.topic_names
    }

    pub fn topic_len(&self) -> usize {
        self.topic_len
    }

    pub fn key_len(&self) -> usize {
        self.key_len
    }

    pub fn message_len(&self) -> usize {
        self.message_len
    }

    pub fn subscriber_poolsize(&self) -> usize {
        self.subscriber_poolsize
    }

    pub fn subscriber_concurrency(&self) -> usize {
        self.subscriber_concurrency
    }

    pub fn topic_distribution(&self) -> Distribution {
        self.topic_distribution
    }

    pub fn partition_distribution(&self) -> Distribution {
        self.partition_distribution
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Distribution {
    Uniform,
    Zipf,
}

impl Default for Distribution {
    fn default() -> Self {
        Self::Uniform
    }
}

#[derive(Clone, Deserialize)]
pub struct Keyspace {
    #[serde(default)]
    nkeys: usize,
    #[serde(default)]
    klen: usize,
    #[serde(default)]
    key_distribution: Distribution,
    #[serde(default = "one")]
    weight: usize,
    #[serde(default)]
    inner_keys_nkeys: Option<usize>,
    #[serde(default)]
    inner_keys_klen: Option<usize>,
    commands: Vec<Command>,
    #[serde(default)]
    vlen: Option<usize>,
    #[serde(default)]
    vkind: Option<ValueKind>,
    #[serde(default)]
    // no ttl is treated as no-expires or max ttl for the protocol
    ttl: Option<String>,
}

impl Keyspace {
    pub fn nkeys(&self) -> usize {
        self.nkeys
    }

    pub fn klen(&self) -> usize {
        self.klen
    }

    pub fn key_distribution(&self) -> Distribution {
        self.key_distribution
    }

    pub fn weight(&self) -> usize {
        self.weight
    }

    pub fn inner_keys_nkeys(&self) -> Option<usize> {
        self.inner_keys_nkeys
    }

    pub fn inner_keys_klen(&self) -> Option<usize> {
        self.inner_keys_klen
    }

    pub fn commands(&self) -> &[Command] {
        &self.commands
    }

    pub fn vlen(&self) -> Option<usize> {
        self.vlen
    }

    pub fn vkind(&self) -> ValueKind {
        self.vkind.unwrap_or(ValueKind::Bytes)
    }

    pub fn ttl(&self) -> Option<Duration> {
        self.ttl
            .as_ref()
            .map(|ttl| ttl.parse::<humantime::Duration>().unwrap().into())
    }
}

#[derive(Clone, Copy, Deserialize)]
pub struct Command {
    verb: Verb,
    #[serde(default = "one")]
    weight: usize,
    #[serde(default = "one")]
    cardinality: usize,
    #[serde(default)]
    truncate: Option<u32>,
    #[serde(default)]
    start: Option<i32>,
    #[serde(default)]
    end: Option<i32>,
    #[serde(default)]
    by_score: bool,
}

impl Command {
    pub fn verb(&self) -> Verb {
        self.verb
    }

    pub fn weight(&self) -> usize {
        self.weight
    }

    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn truncate(&self) -> Option<u32> {
        self.truncate
    }

    pub fn start(&self) -> Option<i32> {
        self.start
    }

    pub fn end(&self) -> Option<i32> {
        self.end
    }

    pub fn by_score(&self) -> bool {
        self.by_score
    }
}

// #[derive(Deserialize, Clone, Copy, Eq, PartialEq)]
// #[serde(rename_all = "snake_case")]
// #[serde(deny_unknown_fields)]
#[derive(Clone, Deserialize, Copy, Debug, Ord, Eq, PartialOrd, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Verb {
    /// Sends a `PING` to the server and expects a `PONG`
    /// * Ping: `PING`
    /// * RESP: `PING`
    Ping,

    /*
     * KEY-VALUE
     */
    /// Set the value for a key if it does not already exist.
    /// * Memcache: `add`
    /// * Momento: unsupported
    /// * RESP: `SET` with `NX` option
    Add,
    /// Read the value for one or more keys.
    /// * Memcache: `get`
    /// * Momento: `get` (NOTE: cardinality > 1 is not supported)
    /// * RESP: `GET` or `MGET`
    Get,
    /// Set the value for a key.
    /// * Memcache: `set`
    /// * Momento: `set`
    /// * RESP: `SET`
    Set,
    /// Remove a key.
    /// * Memcache: `delete`
    /// * Momento: `delete`
    /// * RESP: `DEL`
    #[serde(alias = "del")]
    Delete,
    /// Set the value for a key only if it already exists.
    /// * Memcache: `replace`
    /// * Momento: unsupported
    /// * RESP: `SET` with `XX` option
    Replace,

    /*
     * HASHES (DICTIONARIES)
     */
    /// Delete one or more fields in a hash.
    /// * Momento: `dictionary_delete`
    /// * RESP: `HDEL`
    #[serde(alias = "dictionary_delete")]
    #[serde(alias = "hdel")]
    HashDelete,
    /// Check if a field exists in a hash.
    /// * RESP: `HEXISTS`
    #[serde(alias = "hexists")]
    HashExists,
    /// Reads the value for one or more fields in a hash.
    /// * Momento: `dictionary_get`
    /// * RESP: `HGET` or `HMGET`
    #[serde(alias = "dictionary_get")]
    #[serde(alias = "hget")]
    #[serde(alias = "hmget")]
    HashGet,
    /// Returns all the fields for a hash.
    /// * Momento: `dictionary_fetch`
    /// * RESP: `HGETALL`
    #[serde(alias = "dictionary_fetch")]
    #[serde(alias = "hgetall")]
    HashGetAll,
    /// Increment the value for a field in a hash.
    /// * Momento: `dictionary_increment`
    /// * RESP: `HINCRBY`
    #[serde(alias = "dictionary_increment")]
    #[serde(alias = "hincrby")]
    HashIncrement,
    /// Set the value for one or more fields in a hash.
    /// * Momento: `dictionary_set`
    /// * RESP: `HSET` or `HMSET`
    #[serde(alias = "dictionary_set")]
    #[serde(alias = "hset")]
    #[serde(alias = "hmset")]
    HashSet,

    /*
     * LISTS
     */
    /// Pushes a value to the front of a list.
    /// * Momento: `list_push_front`
    /// * RESP: `LPUSH` (if truncate is set, is fused with `LTRIM`)
    #[serde(alias = "lpush")]
    ListPushFront,
    /// Pushes a value to the back of a list.
    /// * Momento: `list_push_back`
    /// * RESP: `RPUSH` (if truncate is set, is fused with `LTRIM`)
    #[serde(alias = "rpush")]
    ListPushBack,
    /// Retrieves all elements in the list.
    /// * Momento: `list_fetch`
    /// * RESP: `LRANGE 0 -1 [key]`
    ListFetch,
    /// Retrieves the lengths of the list.
    /// * Momento: `list_length`
    /// * RESP: `LLEN`
    #[serde(alias = "llen")]
    ListLength,
    /// Removes and returns the value at the front of the list
    /// * Momento: `list_pop_front`
    /// * RESP: `LPOP`
    #[serde(alias = "lpop")]
    ListPopFront,
    /// Removes and returns the value at the back of the list
    /// * Momento: `list_pop_back`
    /// * RESP: `RPOP`
    #[serde(alias = "rpop")]
    ListPopBack,

    /*
     * SETS
     */
    /// Adds one or more members to a set.
    /// * Momento: `set_add_element`
    /// * RESP: `SADD`
    #[serde(alias = "set_add_element")]
    #[serde(alias = "sadd")]
    SetAdd,
    /// Retrieves the members of a set.
    /// * Momento: `set_fetch`
    /// * RESP: `SMEMBERS`
    #[serde(alias = "set_fetch")]
    #[serde(alias = "smembers")]
    SetMembers,
    /// Retrieves the members of a set.
    /// * Momento: `set_remove_element`
    /// * RESP: `SREM`
    #[serde(alias = "set_remove_element")]
    #[serde(alias = "srem")]
    SetRemove,

    /*
     * SORTED SETS
     */
    /// Adds one or more members to a sorted set.
    /// * Momento: `sorted_set_put`
    /// * RESP: `ZADD`
    #[serde(alias = "sorted_set_put")]
    #[serde(alias = "zadd")]
    SortedSetAdd,
    /// Retrieves a range of members of a sorted set (with scores), sorted by
    /// index.
    /// * Momento: `sorted_set_fetch_by_index`
    /// * RESP: `ZRANGE`
    #[serde(alias = "sorted_set_fetch_by_index")]
    #[serde(alias = "zrange")]
    SortedSetRange,
    /// Increment the score for a member of a sorted set.
    /// * Moemento: `sorted_set_increment`
    /// * RESP: `ZINCRBY`
    #[serde(alias = "sorted_set_increment")]
    #[serde(alias = "zincrby")]
    SortedSetIncrement,
    /// Retrieve the rank for a member of a sorted set.
    /// * Momento: `sorted_set_get_rank`
    /// * RESP: `ZRANK`
    #[serde(alias = "sorted_set_get_rank")]
    #[serde(alias = "zrank")]
    SortedSetRank,
    /// Removes one or more members from a sorted set.
    /// * Momento: `sorted_set_remove`
    /// * RESP: `ZREM`
    #[serde(alias = "sorted_set_remove")]
    #[serde(alias = "zrem")]
    SortedSetRemove,
    /// Retrieve the score for a one or more members of a sorted set.
    /// * Momento: `sorted_set_get_score`
    /// * RESP: `ZSCORE` / `ZMSCORE`
    #[serde(alias = "sorted_set_get_score")]
    #[serde(alias = "zmscore")]
    #[serde(alias = "zscore")]
    SortedSetScore,
}

impl Verb {
    // Returns `true` if the verb supports cardinality > 1
    pub fn supports_cardinality(&self) -> bool {
        matches!(
            self,
            Self::HashDelete
                | Self::HashGet
                | Self::HashSet
                | Self::ListPushBack
                | Self::ListPushFront
                | Self::SetAdd
                | Self::SetRemove
                | Self::SortedSetAdd
                | Self::SortedSetScore
                | Self::SortedSetRemove
        )
    }

    pub fn supports_start(&self) -> bool {
        matches!(self, Self::SortedSetRange)
    }

    pub fn supports_end(&self) -> bool {
        matches!(self, Self::SortedSetRange)
    }

    pub fn supports_by_score(&self) -> bool {
        matches!(self, Self::SortedSetRange)
    }

    pub fn needs_inner_key(&self) -> bool {
        matches!(
            self,
            Self::HashDelete
                | Self::HashExists
                | Self::HashGet
                | Self::HashSet
                | Self::ListPushBack
                | Self::ListPushFront
                | Self::SetAdd
                | Self::SetRemove
                | Self::SortedSetAdd
                | Self::SortedSetIncrement
                | Self::SortedSetRank
                | Self::SortedSetRemove
                | Self::SortedSetScore
        )
    }

    pub fn supports_truncate(&self) -> bool {
        matches!(self, Self::ListPushBack | Self::ListPushFront)
    }
}
