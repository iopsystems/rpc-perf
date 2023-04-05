use crate::clients::*;
use crate::workload::Component;
use crate::workload::PublisherWorkItem as WorkItem;
use crate::*;
use async_channel::Receiver;
use std::io::{Error, ErrorKind, Result};
// use tokio::io::*;
use tokio::runtime::Runtime;
// use tokio::time::{timeout, Duration};

mod momento;

pub fn launch_publishers(config: &Config, work_receiver: Receiver<WorkItem>) -> Option<Runtime> {
    if config.pubsub().is_none() {
        debug!("No pubsub configuration specified");
        return None;
    }

    debug!("Launching publishers...");

    // spawn the request drivers on their own runtime
    let mut publisher_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.pubsub().unwrap().publisher_threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Momento => {
            momento::launch_publishers(&mut publisher_rt, config.clone(), work_receiver);
        }
        _ => {
            error!("pubsub is not supported for the selected protocol");
            std::process::exit(1);
        }
    }

    Some(publisher_rt)
}

pub fn launch_subscribers(config: &Config, workload_components: Vec<Component>) -> Option<Runtime> {
    if config.pubsub().is_none() {
        debug!("No pubsub configuration specified");
        return None;
    }

    debug!("Launching subscribers...");

    // spawn the request drivers on their own runtime
    let mut subscriber_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.pubsub().unwrap().publisher_threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Momento => {
            momento::launch_subscribers(&mut subscriber_rt, config.clone(), workload_components);
        }
        _ => {
            error!("pubsub is not supported for the selected protocol");
            std::process::exit(1);
        }
    }

    Some(subscriber_rt)
}
