use core::num::NonZeroU64;
use serde::Deserialize;
use std::io::Read;
use std::time::Duration;

mod client;
mod debug;
mod general;
mod leaderboard;
mod metrics;
mod oltp;
mod protocol;
mod pubsub;
mod storage;
mod target;
mod tls;
mod workload;

pub use client::Client;
pub use debug::Debug;
pub use general::General;
pub use leaderboard::Leaderboard as LeaderboardConfig;
pub use metrics::{Format as MetricsFormat, Metrics};
pub use oltp::Oltp as OltpConfig;
pub use protocol::Protocol;
pub use pubsub::Pubsub;
pub use storage::Storage;
pub use target::Target;
pub use tls::Tls;
pub use workload::{
    Command, Distribution, Keyspace, Leaderboard, LeaderboardCommand, LeaderboardVerb, Oltp,
    RampCompletionAction, RampType, Store, StoreCommand, StoreVerb, Topics, ValueKind, Verb,
    Workload,
};

pub const PAGESIZE: usize = 4096;
pub const DEFAULT_BUFFER_SIZE: usize = 4 * PAGESIZE;

pub fn default_buffer_size() -> usize {
    DEFAULT_BUFFER_SIZE
}

#[derive(Clone, Deserialize)]
pub struct Config {
    general: General,
    client: Option<Client>,
    debug: Debug,
    oltp: Option<OltpConfig>,
    pubsub: Option<Pubsub>,
    target: Target,
    tls: Option<Tls>,
    workload: Workload,
    metrics: Option<Metrics>,
    storage: Option<Storage>,
    leaderboard: Option<LeaderboardConfig>,
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
        let mut config: Config = toml::from_str(&content)
            .map_err(|e| {
                eprintln!("Failed to parse TOML config: {filename}\n{e}");
                std::process::exit(1);
            })
            .unwrap();

        config.workload.ratelimit().validate();
        if config.metrics().is_none() {
            config.metrics = Metrics::from_general(&config.general);
        }
        if let Some(x) = config.metrics.as_ref() {
            x.validate(&config.general);
        }
        config
    }

    pub fn general(&self) -> &General {
        &self.general
    }

    pub fn client(&self) -> Option<&Client> {
        self.client.as_ref()
    }

    pub fn oltp(&self) -> Option<&OltpConfig> {
        self.oltp.as_ref()
    }

    pub fn pubsub(&self) -> Option<&Pubsub> {
        self.pubsub.as_ref()
    }

    pub fn target(&self) -> &Target {
        &self.target
    }

    pub fn tls(&self) -> Option<&Tls> {
        self.tls.as_ref()
    }

    pub fn workload(&self) -> &Workload {
        &self.workload
    }

    pub fn debug(&self) -> &Debug {
        &self.debug
    }

    pub fn metrics(&self) -> Option<&Metrics> {
        self.metrics.as_ref()
    }

    pub fn storage(&self) -> Option<&Storage> {
        self.storage.as_ref()
    }

    pub fn leaderboard(&self) -> Option<&LeaderboardConfig> {
        self.leaderboard.as_ref()
    }
}
