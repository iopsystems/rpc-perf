// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use core::num::NonZeroU64;
use serde::Deserialize;
use std::io::Read;
use std::time::Duration;

mod connection;
mod debug;
mod general;
mod protocol;
mod request;
mod target;
mod workload;

pub use connection::Connection;
pub use debug::Debug;
pub use general::General;
pub use protocol::Protocol;
pub use request::Request;
pub use target::Target;
pub use workload::{Command, Keyspace, ValueKind, Verb, Workload};

#[derive(Clone, Deserialize)]
pub struct Config {
    general: General,
    connection: Connection,
    debug: Debug,
    target: Target,
    request: Request,
    workload: Workload,
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

    pub fn target(&self) -> &Target {
        &self.target
    }

    pub fn request(&self) -> &Request {
        &self.request
    }

    pub fn workload(&self) -> &Workload {
        &self.workload
    }

    pub fn debug(&self) -> &Debug {
        &self.debug
    }
}
