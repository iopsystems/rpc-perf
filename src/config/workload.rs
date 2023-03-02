use super::*;

fn one() -> usize {
    1
}

#[derive(Clone, Deserialize)]
pub struct Workload {
    keyspace: Vec<Keyspace>,
    threads: usize,
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

    pub fn threads(&self) -> usize {
        self.threads
    }
}

#[derive(Clone, Deserialize)]
pub struct Keyspace {
    nkeys: usize,
    klen: usize,
    #[serde(default = "one")]
    weight: usize,
    inner_keys_nkeys: Option<usize>,
    inner_keys_klen: Option<usize>,
    commands: Vec<Command>,
    vlen: Option<usize>,
    vkind: Option<ValueKind>,
}

impl Keyspace {
    pub fn nkeys(&self) -> usize {
        self.nkeys
    }

    pub fn klen(&self) -> usize {
        self.klen
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
}

#[derive(Clone, Copy, Deserialize)]
pub struct Command {
    verb: Verb,
    #[serde(default = "one")]
    weight: usize,
    #[serde(default = "one")]
    cardinality: usize,
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
    /// Adds one or more members to a sorted set.
    /// * Momento: `sorted_set_put`
    /// * RESP: `ZADD`
    #[serde(alias = "sorted_set_put")]
    #[serde(alias = "zadd")]
    SortedSetAdd,
    /// Increment the score for a member of a sorted set.
    /// * Moemento: `sorted_set_increment`
    /// * RESP: `ZINCRBY`
    #[serde(alias = "sorted_set_increment")]
    #[serde(alias = "zincrby")]
    SortedSetIncrement,
    /// Retrieve the score for a one or more members of a sorted set.
    /// * Momento: `sorted_set_get_score`
    /// * RESP: `ZMSCORE`
    #[serde(alias = "sorted_set_get_score")]
    #[serde(alias = "zmscore")]
    #[serde(alias = "zscore")]
    SortedSetScore,
    /// Retrieve members from a sorted set.
    /// * Momento: `sorted_set_fetch`
    /// * RESP: `ZRANGE`
    // SortedSetRange,
    /// Retrieve the rank for a member of a sorted set.
    /// * Momento: `sorted_set_get_rank`
    /// * RESP: `ZRANK`
    SortedSetRank,
    /// Removes one or more members from a sorted set.
    /// * Momento: `sorted_set_remove`
    /// * RESP: `ZREM`
    SortedSetRemove,
    // /// Retrieve the score for a member of a sorted set.
    // /// * Momento: `sorted_set_get_score`
    // /// * RESP: `ZSCORE`
    // SortedSetScore,
    // TODO(bmartin): the commands below were previously supported
    // /// Sends a payload with a CRC to an echo server and checks for corruption.
    // Echo,
    // /// Hash set non-existing, set the value for a field within the hash stored
    // /// at the key only if the field does not exist.
    // Hsetnx,
    // /// Insert all the specified values at the tail of the list stored at a key.
    // /// Creates a new key if the key does not exist. Returns an error if the key
    // /// contains a value which is not a list.
    // Rpush,
    // /// Insert all the specified values at the tail of the list stored at a key,
    // /// returns an error if the key does not exist or contains a value which is
    // /// not a list.
    // Rpushx,
    // /// Count the number of items stored at a key
    // Count,
    // /// Returns the elements of the list stored at the key
    // Lrange,
    // /// Trims the elements of the list sotred at the key
    // Ltrim,
}

impl Verb {
    // Returns `true` if the verb supports cardinality > 1
    pub fn supports_cardinality(&self) -> bool {
        matches!(
            self,
            Self::HashDelete
                | Self::HashGet
                | Self::HashSet
                | Self::SortedSetAdd
                | Self::SortedSetScore
                | Self::SortedSetRemove
        )
    }

    pub fn needs_inner_key(&self) -> bool {
        matches!(
            self,
            Self::HashDelete
                | Self::HashExists
                | Self::HashGet
                | Self::HashSet
                | Self::SortedSetAdd
                | Self::SortedSetIncrement
                | Self::SortedSetRank
                | Self::SortedSetRemove
                | Self::SortedSetScore
        )
    }
}
