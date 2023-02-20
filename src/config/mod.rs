

use core::num::NonZeroU64;
use std::io::Read;
use std::time::Duration;

use serde::Deserialize;

mod debug;
pub use debug::Debug;

#[derive(Clone, Deserialize)]
pub struct Config {
	general: General,
	connection: Connection,
	debug: Debug,
	#[serde(default)]
	keyspaces: Vec<Keyspace>,
	endpoints: Vec<String>,
	request: Request,
}

impl Config {
	pub fn new(filename: &str) -> Self {
        let mut file = match std::fs::File::open(filename) {
            Ok(c) => c,
            Err(error) => {
                eprintln!("error loading config file: {filename}\n{error}");
                std::process::exit(1);
            }
        };
        let mut content = String::new();
        match file.read_to_string(&mut content) {
            Ok(_) => {}
            Err(error) => {
                eprintln!("error reading config file: {filename}\n{error}");
                std::process::exit(1);
            }
        }
        let toml = toml::from_str(&content);
        match toml {
            Ok(toml) => toml,
            Err(error) => {
                eprintln!("Failed to parse TOML config: {filename}\n{error}");
                std::process::exit(1);
            }
        }
    }

	pub fn general(&self) -> &General {
		&self.general
	} 

	pub fn connection(&self) -> &Connection {
		&self.connection
	}

	pub fn endpoints(&self) -> &[String] {
		&self.endpoints
	}

	pub fn request(&self) -> &Request {
		&self.request
	}

	pub fn keyspaces(&self) -> &[Keyspace] {
		&self.keyspaces
	}

	pub fn debug(&self) -> &Debug {
		&self.debug
	}
}

#[derive(Clone, Deserialize)]
pub struct General {
	threads: usize,
	protocol: Protocol,
	interval: u64,
	duration: u64,
}

impl General {
	pub fn threads(&self) -> usize {
		self.threads
	}

	pub fn protocol(&self) -> Protocol {
		self.protocol
	}

	pub fn interval(&self) -> Duration {
		Duration::from_secs(self.interval)
	}

	pub fn duration(&self) -> Duration {
		Duration::from_secs(self.duration)
	}
}

#[derive(Clone, Copy, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
	Memcache,
	Momento,
	Ping,
	Resp,
}

#[derive(Clone, Deserialize)]
pub struct Connection {
	poolsize: usize,
	timeout: u64,
	ratelimit: u64,
	reconnect_rate: u64,
}

impl Connection {
	pub fn timeout(&self) -> Duration {
		Duration::from_millis(self.timeout)
	}

	pub fn poolsize(&self) -> usize {
		self.poolsize
	}

	pub fn reconnect_rate(&self) -> Option<NonZeroU64> {
		NonZeroU64::new(self.reconnect_rate)
	}
}

#[derive(Clone, Deserialize)]
pub struct Endpoints {
	endpoints: Vec<String>,
}

#[derive(Clone, Deserialize)]
pub struct Request {
	// milliseconds
	timeout: u64,
	// zero is treated as unlimited
	ratelimit: u64,
}

impl Request {
	pub fn timeout(&self) -> Duration {
		Duration::from_millis(self.timeout)
	}

	pub fn ratelimit(&self) -> Option<NonZeroU64> {
		NonZeroU64::new(self.ratelimit)
	}
}

#[derive(Clone, Deserialize)]
pub struct Keyspace {
	nkeys: usize,
	length: usize,
	commands: Vec<Command>,
}

#[derive(Clone, Deserialize)]
pub struct Command {
	verb: Verb,
}


// #[derive(Deserialize, Clone, Copy, Eq, PartialEq)]
// #[serde(rename_all = "snake_case")]
// #[serde(deny_unknown_fields)]
#[derive(Clone, Deserialize)]
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
    HashMultiGet,
    /// Set the value for a field in a hash.
    /// * Momento: `dictionary_set`
    /// * RESP: `HSET`
    HashSet,
    /// Get multiple keys.
    /// * Memcache: `get`
    /// * RESP: `MGET`
    MultiGet,
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
    SortedSetRange,
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
