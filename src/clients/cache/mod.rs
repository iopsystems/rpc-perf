use crate::workload::ClientRequest;
use crate::*;

use async_channel::Receiver;
use tokio::io::*;
use tokio::runtime::Runtime;
use tokio::time::{timeout, Duration};
use workload::ClientWorkItemKind;

use std::io::{Error, ErrorKind, Result};
use std::time::Instant;

mod memcache;
mod momento;
mod redis;

pub fn launch(
    config: &Config,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
) -> Option<Runtime> {
    debug!("Launching clients...");

    config.client()?;

    // spawn the request drivers on their own runtime
    let mut client_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.client().unwrap().threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Memcache => memcache::launch_tasks(&mut client_rt, config.clone(), work_receiver),
        Protocol::Momento => momento::launch_tasks(&mut client_rt, config.clone(), work_receiver),
        Protocol::Ping => {
            crate::clients::ping::ascii::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::Resp => redis::launch_tasks(&mut client_rt, config.clone(), work_receiver),
        protocol => {
            error!("keyspace is not supported for the {:?} protocol", protocol);
            std::process::exit(1);
        }
    }

    Some(client_rt)
}
