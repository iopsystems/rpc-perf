use crate::*;

use ::momento::{MomentoError, MomentoErrorCode};
use async_channel::Receiver;
use std::io::{Error, ErrorKind, Result};
use std::time::Instant;
use tokio::runtime::Runtime;
use workload::{ClientWorkItemKind, StoreClientRequest};

mod momento;

pub fn launch_store_clients(
    config: &Config,
    work_receiver: Receiver<ClientWorkItemKind<StoreClientRequest>>,
) -> Option<Runtime> {
    if config.storage().is_none() {
        debug!("No store configuration specified");
        return None;
    }
    debug!("Launching clients...");

    config.storage()?;

    // spawn the request drivers on their own runtime
    let mut client_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.storage().unwrap().threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Momento => momento::launch_tasks(&mut client_rt, config.clone(), work_receiver),
        _ => {
            error!("momento protocol is the only supported store protocol");
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
