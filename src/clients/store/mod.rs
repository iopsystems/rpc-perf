/// # Store Clients
///
/// Store clients are focused on object storage. This is distinct from caching
/// in that data is not evicted from an object store. Expiration may still be
/// possible with some implementations.
///
/// RPC-Perf store clients are used to evaluate the performance of object
/// storage services in terms of throughput and latency.

use crate::*;

use async_channel::Receiver;
use tokio::runtime::Runtime;
use workload::{ClientWorkItemKind, StoreClientRequest};

mod momento;

pub fn launch(
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
        protocol => {
            error!(
                "store commands are not supported for the {:?} protocol",
                protocol
            );
            std::process::exit(1);
        }
    }

    Some(client_rt)
}
