use super::*;

#[derive(Clone, Deserialize)]
pub struct Workload {
    keyspace: Vec<Keyspace>,
    threads: usize,
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
    weight: usize,
    inner_keys_nkeys: usize,
    inner_keys_klen: usize,
    commands: Vec<Command>,
    vlen: usize,
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

    pub fn inner_keys_nkeys(&self) -> usize {
        self.inner_keys_nkeys
    }

    pub fn inner_keys_klen(&self) -> usize {
        self.inner_keys_klen
    }

    pub fn commands(&self) -> &[Command] {
        &self.commands
    }

    pub fn vlen(&self) -> usize {
        self.vlen
    }
}

#[derive(Clone, Deserialize)]
pub struct Command {
    verb: Verb,
    weight: usize,
}

impl Command {
    pub fn verb(&self) -> Verb {
        self.verb
    }

    pub fn weight(&self) -> usize {
        self.weight
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
    /// Read the value for a key.
    /// * Memcache: `get`
    /// * Momento: `get`
    /// * RESP: `GET`
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
    Delete,
    /// Delete one or more fields in a hash.
    /// * Momento: `dictionary_delete`
    /// * RESP: `HDEL`
    HashDelete,
    /// Check if a field exists in a hash.
    /// * Momento: `dictionary_get`
    /// * RESP: `HEXISTS`
    HashExists,
    /// Reads the value for one field in a hash.
    /// * Momento: `dictionary_get`
    /// * RESP: `HGET`
    HashGet,
    /// Reads the value for multiple fields in a hash.
    /// * Momento: `dictionary_get`
    /// * RESP: `HMGET`
    // HashMultiGet,
    /// Set the value for a field in a hash.
    /// * Momento: `dictionary_set`
    /// * RESP: `HSET`
    HashSet,
    /// Get multiple keys.
    /// * Memcache: `get`
    /// * RESP: `MGET`
    // MultiGet,
    /// Adds one or more members to a sorted set.
    /// * Momento: `sorted_set_put`
    /// * RESP: `ZADD`
    SortedSetAdd,
    /// Increment the score for a member of a sorted set.
    /// * Moemento: `sorted_set_increment`
    /// * RESP: `ZINCRBY`
    SortedSetIncrement,
    /// Retrieve the score for a one or more members of a sorted set.
    /// * Momento: `sorted_set_get_score`
    /// * RESP: `ZMSCORE`
    SortedSetMultiScore,
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
    /// Retrieve the score for a member of a sorted set.
    /// * Momento: `sorted_set_get_score`
    /// * RESP: `ZSCORE`
    SortedSetScore,
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
