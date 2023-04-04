// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

pub mod http1;
pub mod http2;
pub mod memcache;
pub mod momento;
pub mod ping;
pub mod resp;

use crate::workload::WorkItem;
use crate::*;
use ::momento::MomentoError;
use async_channel::Receiver;
use std::io::{Error, ErrorKind, Result};
use tokio::io::*;
use tokio::runtime::Runtime;
use tokio::time::{timeout, Duration};

pub fn launch_clients(config: &Config, work_receiver: Receiver<WorkItem>) -> Runtime {
    debug!("Launching clients...");

    // spawn the request drivers on their own runtime
    let mut client_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.client().threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Http1 => {
            clients::http1::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::Http2 => {
            clients::http2::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::Memcache => {
            clients::memcache::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::Momento => {
            clients::momento::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::Ping => {
            clients::ping::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::Resp => {
            clients::resp::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
    }

    client_rt
}

pub enum ResponseError {
    /// Some exception while reading the response
    Exception,
    /// A timeout while awaiting the response
    Timeout,
    /// Some backends may have rate limits
    Ratelimited,
    /// Some backends may have their own timeout
    BackendTimeout,
}

impl From<MomentoError> for ResponseError {
    fn from(other: MomentoError) -> Self {
        match other {
            MomentoError::LimitExceeded { .. } => ResponseError::Ratelimited,
            MomentoError::Timeout { .. } => ResponseError::BackendTimeout,
            _ => ResponseError::Exception,
        }
    }
}
