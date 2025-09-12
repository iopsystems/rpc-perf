use serde::Deserialize;

use crate::config::{workload::Ratelimit, Config, Protocol};

#[derive(Clone, Deserialize)]
pub struct Replay {
    #[serde(default)]
    command_log: String,

    #[serde(default)]
    speed: Option<f64>,

    #[serde(default)]
    ratelimit: Option<Ratelimit>,
}

impl Replay {
    pub fn validate(&self, config: &Config) {
        if config.general().protocol() == Protocol::Momento
            || config.general().protocol() == Protocol::MomentoProtosocket
        {
            if config.target().cache_name().is_none() {
                eprintln!("cache name is required for replay against momento");
                std::process::exit(1);
            } else if let Some(cache_name) = config.target().cache_name() {
                if cache_name.is_empty() {
                    eprintln!("cache name is required for replay against momento");
                    std::process::exit(1);
                }
            }
        }

        if self.command_log.is_empty() {
            eprintln!("command log file to replay from is required");
            std::process::exit(1);
        }

        if let Err(error) = std::fs::File::open(&self.command_log) {
            eprintln!("error loading command log file: {error}");
            std::process::exit(1);
        }

        if self.speed.is_some() && self.ratelimit.is_some() {
            eprintln!("speed and ratelimit cannot be specified at the same time");
            std::process::exit(1);
        }

        if let Some(ratelimit) = self.ratelimit() {
            ratelimit.validate();
        }
    }

    pub fn command_log(&self) -> &String {
        &self.command_log
    }

    pub fn speed(&self) -> Option<f64> {
        self.speed
    }

    pub fn ratelimit(&self) -> Option<&Ratelimit> {
        self.ratelimit.as_ref()
    }
}
