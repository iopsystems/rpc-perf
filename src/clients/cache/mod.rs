/// # Cache clients
///
/// Cache clients support key-value storage where data that is written is not
/// guaranteed to be persistent. Often writes specify some Time-To-Live (TTL) or
/// expiration. Some cache backends may support more advanced datastructures
/// such as hashes, lists, sets, etc. These operations are also included here
/// when applicable.
///
/// The RPC-Perf cache clients track hit rate, throughput, and latency to help
/// measure cache effectiveness and performance.
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
    if config.client().is_none() {
        debug!("No client configuration specified");
        return None;
    }

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
        Protocol::MomentoProtosocket => {
            momento::protosocket::launch_tasks(&mut client_rt, config.clone(), work_receiver, false)
        }
        Protocol::MomentoProtosocketPrivate => {
            momento::protosocket::launch_tasks(&mut client_rt, config.clone(), work_receiver, true)
        }
        Protocol::MomentoHttp => {
            momento::http::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::Ping => {
            crate::clients::ping::ascii::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        Protocol::PingGrpc => crate::clients::ping::grpc::tonic::launch_tasks(
            &mut client_rt,
            config.clone(),
            work_receiver,
        ),
        Protocol::PingGrpcH2 => crate::clients::ping::grpc::h2::launch_tasks(
            &mut client_rt,
            config.clone(),
            work_receiver,
        ),
        Protocol::PingGrpcH3 => crate::clients::ping::grpc::h3::launch_tasks(
            &mut client_rt,
            config.clone(),
            work_receiver,
        ),
        Protocol::Resp => redis::launch_tasks(&mut client_rt, config.clone(), work_receiver),
        protocol => {
            eprintln!("keyspace is not supported for the {:?} protocol", protocol);
            std::process::exit(1);
        }
    }

    Some(client_rt)
}
