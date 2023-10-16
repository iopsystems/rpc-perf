use crate::clients::*;
use crate::workload::Component;
use crate::workload::PublisherWorkItem as WorkItem;
use crate::*;
use async_channel::Receiver;
use std::io::{Error, ErrorKind, Result};
use tokio::runtime::Runtime;

mod momento;
mod kafka;

pub struct PubsubRuntimes {
    publisher_rt: Option<Runtime>,
    subscriber_rt: Option<Runtime>,
}

impl PubsubRuntimes {
    pub fn shutdown_timeout(&mut self, duration: Duration) {
        if let Some(rt) = self.publisher_rt.take() {
            rt.shutdown_timeout(duration);
        }
        if let Some(rt) = self.subscriber_rt.take() {
            rt.shutdown_timeout(duration);
        }
    }
}

pub fn launch_pubsub(
    config: &Config,
    work_receiver: Receiver<WorkItem>,
    workload_components: Vec<Component>,
) -> PubsubRuntimes {
    // create topics here?    
    if config.pubsub().is_some() {
        match config.general().protocol() {
            Protocol::Kafka => {
                let mut topic_rt = Builder::new_multi_thread()
                .enable_all()
                .worker_threads(1)
                .build()
                .expect("failed to initialize tokio runtime");
                kafka::create_topics(&mut topic_rt, config.clone(), & workload_components)
            }
            _ => {}
        }
    }

    PubsubRuntimes {
        publisher_rt: launch_publishers(config, work_receiver, &workload_components),
        subscriber_rt: launch_subscribers(config, &workload_components),
    }
}

fn launch_publishers(config: &Config, work_receiver: Receiver<WorkItem>, workload_components: &Vec<Component>) -> Option<Runtime> {
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
        Protocol::Kafka => {
            kafka::launch_publishers(&mut publisher_rt, config.clone(), work_receiver, &workload_components);
        }
        _ => {
            error!("pubsub is not supported for the selected protocol");
            std::process::exit(1);
        }
    }

    Some(publisher_rt)
}

fn launch_subscribers(config: &Config, workload_components: &Vec<Component>) -> Option<Runtime> {
    if config.pubsub().is_none() {
        debug!("No pubsub configuration specified");
        return None;
    }

    debug!("Launching subscribers...");

    // spawn the request drivers on their own runtime
    let mut subscriber_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.pubsub().unwrap().subscriber_threads())
        .build()
        .expect("failed to initialize tokio runtime");

    match config.general().protocol() {
        Protocol::Momento => {
            momento::launch_subscribers(&mut subscriber_rt, config.clone(), &workload_components);
        }
        Protocol::Kafka => {
            kafka::launch_subscribers(&mut subscriber_rt, config.clone(), &workload_components);
        }
        _ => {
            error!("pubsub is not supported for the selected protocol");
            std::process::exit(1);
        }
    }

    Some(subscriber_rt)
}
