use tokio::runtime::Runtime;
use crate::workload::WorkItem;
use async_channel::Receiver;
use super::*;

mod publishers {

}

pub fn launch_publishers(config: &Config, work_receiver: Receiver<WorkItem>) -> Option<Runtime> {
    if config.pubsub().is_none() {
        debug!("No pubsub configuration specified");
        return None;
    }

    debug!("Launching clients...");

    // spawn the request drivers on their own runtime
    let mut publisher_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.pubsub().unwrap().publisher_threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Momento => {
            todo!("unimplemeted");
            // publishers::momento::launch_tasks(&mut client_rt, config.clone(), work_receiver)
        }
        _ => todo!(),
    }

    Some(publisher_rt)
}