use crate::workload::ClientRequest;
use crate::workload::ClientWorkItem as WorkItem;
use crate::*;

use ::momento::{MomentoError, MomentoErrorCode};
use async_channel::Receiver;
use tokio::io::*;
use tokio::runtime::Runtime;
use tokio::time::{timeout, Duration};

use std::io::{Error, ErrorKind, Result};
use std::time::Instant;

mod http1;
mod http2;
mod memcache;
mod momento;
mod ping;
mod redis;

pub fn launch_clients(config: &Config, work_receiver: Receiver<WorkItem>) -> Option<Runtime> {
    debug!("Launching clients...");

    config.client()?;

    // spawn the request drivers on their own runtime
    let mut client_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.client().unwrap().threads())
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
            clients::redis::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::Kafka => {
            error!("keyspace is not supported for the kafka protocol");
            std::process::exit(1);
        }
        Protocol::Blabber => {
            error!("keyspace is not supported for the blabber protocol");
            std::process::exit(1);
        }
    }

    Some(client_rt)
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
        match other.error_code {
            MomentoErrorCode::LimitExceededError { .. } => ResponseError::Ratelimited,
            MomentoErrorCode::TimeoutError { .. } => ResponseError::BackendTimeout,
            _ => ResponseError::Exception,
        }
    }
}
