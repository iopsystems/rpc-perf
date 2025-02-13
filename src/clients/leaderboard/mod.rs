/// # Leaderboard Clients
///
/// Leaderboards provide at-scale competitor ranking and lookup.
///
/// RPC-Perf store clients are used to evaluate the performance of object
/// leaderboard services in terms of throughput and latency.
use crate::*;

use async_channel::Receiver;
use tokio::runtime::Runtime;
use workload::{ClientWorkItemKind, LeaderboardClientRequest};

mod momento;

pub fn launch(
    config: &Config,
    work_receiver: Receiver<ClientWorkItemKind<LeaderboardClientRequest>>,
) -> Option<Runtime> {
    if config.storage().is_none() {
        debug!("No store configuration specified");
        return None;
    }
    debug!("Launching clients...");

    config.leaderboard()?;

    // spawn the request drivers on their own runtime
    let mut client_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.leaderboard().unwrap().threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Momento => momento::launch_tasks(&mut client_rt, config.clone(), work_receiver),
        protocol => {
            eprintln!(
                "store commands are not supported for the {:?} protocol",
                protocol
            );
        }
    }

    Some(client_rt)
}
