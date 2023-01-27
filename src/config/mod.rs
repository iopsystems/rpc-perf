use std::collections::HashMap;
use std::io::Read;
use serde_derive::*;

// The default interval, in seconds, between reports on stdout
fn default_interval() -> usize {
    60
}

// The total test runtime in seconds
fn default_duration() -> usize {
    300
}

// The number of per-backend connections / channels to drive the load
fn default_poolsize() -> usize {
	1
}

// The number of worker threads used to drive the load.
// NOTE: this doesn't mean exactly what we want yet, as we are just
// setting the number of tokio worker threads to this value. The
// workload generation tasks share this pool too.
fn default_threads() -> usize {
	1
}

fn empty_map() -> HashMap<String, String> {
    HashMap::new()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct File {
	general: General,
	keyspace: Vec<Keyspace>,
}

impl File {
	pub fn general(&self) -> &General {
		&self.general
	}

	pub fn load(filename: &str) -> Self {
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

	pub fn keyspaces(&self) -> &[Keyspace] {
		&self.keyspace
	}
}

#[derive(Deserialize)]
pub struct General {
	protocol: Protocol,
	#[serde(default = "default_interval")]
	interval: usize,
	#[serde(default = "default_duration")]
	duration: usize,
	#[serde(default = "default_poolsize")]
	poolsize: usize,
	#[serde(default = "default_threads")]
	threads: usize,
}

impl General {
	pub fn protocol(&self) -> Protocol {
		self.protocol
	}

	pub fn threads(&self) -> usize {
		self.threads
	}

	pub fn duration(&self) -> usize {
		self.duration
	}

	pub fn interval(&self) -> usize {
		self.interval
	}

	pub fn poolsize(&self) -> usize {
		self.poolsize
	}
}

// keyspace
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Keyspace {
	key_length: usize,
	key_count: usize,
	distribution: Distribution,
	commands: CommandConfig,
}

impl Keyspace {
	pub fn key_length(&self) -> usize {
		self.key_length
	}

	pub fn key_count(&self) -> usize {
		self.key_count
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Distribution {
	model: DistributionModel,
	#[serde(serialize_with = "toml::ser::tables_last")]
    #[serde(default = "empty_map")]
	parameters: HashMap<String, String>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum Protocol {
    Memcache,
    Momento,
    Ping,
    Resp,
}

#[derive(Debug, Deserialize, Clone, Copy, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum DistributionModel {
	Uniform,
	Zipf,
}

#[derive(Deserialize, Clone, Copy, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum Type {
	Bytes,
	Ascii,
	U32,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandConfig {
	command: Command,
	weight: usize,
	// rate: usize,
	// keyspace: Keyspace,
}

impl CommandConfig {
	pub fn command(&self) -> Command {
		self.command
	}

	pub fn weight(&self) -> usize {
		self.weight
	}
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum Command {
	Add,
	Get,
	#[serde(alias = "hexists")]
	HashExists,
	#[serde(alias = "hget")]
	HashGet,
	#[serde(alias = "hmget")]
	HashMultiGet,
	#[serde(alias = "hset")]
	HashSet,
	Replace,
	Set,
}