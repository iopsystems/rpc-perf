use crate::clients::*;
use crate::workload::Component;
use crate::workload::PublisherWorkItem as WorkItem;
use crate::*;

use ahash::RandomState;
use async_channel::Receiver;
use tokio::runtime::Runtime;

use std::io::{Error, ErrorKind, Result};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

mod blabber;
mod kafka;
mod momento;

pub fn hasher() -> RandomState {
    RandomState::with_seeds(
        0xd5b96f9126d61cee,
        0x50af85c9d1b6de70,
        0xbd7bdf2fee6d15b2,
        0x3dbe88bb183ac6f4,
    )
}

struct MessageValidator {
    hash_builder: RandomState,
}

pub enum ValidationError {
    Unexpected,
    Corrupted,
}

impl MessageValidator {
    /// Deterministic seeds are used so that multiple validators can stamp and
    /// validate messages produced by other instances.
    pub fn new() -> Self {
        MessageValidator {
            hash_builder: hasher(),
        }
    }

    /// Sets the checksum and timestamp in the message. Returns the timestamp.
    pub fn stamp(&self, message: &mut [u8]) -> u64 {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let ts = timestamp.to_be_bytes();

        // write the current unix time into the message
        message[16..24].copy_from_slice(&ts[0..8]);

        // todo, write a sequence number into the message

        // checksum the message and put the checksum into the message
        let checksum = self.hash_builder.hash_one(&message).to_be_bytes();
        message[8..16].copy_from_slice(&checksum);

        timestamp
    }

    /// Validate the message checksum and returns a validation result.
    pub fn validate(&self, v: &mut [u8]) -> std::result::Result<u64, ValidationError> {
        let now_unix = SystemTime::now();

        // check if the magic bytes match
        if v[0..8] != [0x54, 0x45, 0x53, 0x54, 0x49, 0x4E, 0x47, 0x21] {
            RESPONSE_EX.increment();
            PUBSUB_RECEIVE_INVALID.increment();
            return Err(ValidationError::Unexpected);
        }

        // validate the checksum
        let csum = v[8..16].to_owned();
        v[8..16].copy_from_slice(&[0; 8]);
        let calculated_csum = self.hash_builder.hash_one(&v).to_be_bytes();
        if csum != calculated_csum {
            PUBSUB_RECEIVE.increment();
            PUBSUB_RECEIVE_CORRUPT.increment();
            return Err(ValidationError::Corrupted);
        }

        // calculate and return the end to end latency
        let ts = u64::from_be_bytes(v[16..24].try_into().unwrap());
        let latency = now_unix.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 - ts;

        let _ = PUBSUB_LATENCY.increment(latency);
        PUBSUB_RECEIVE.increment();
        PUBSUB_RECEIVE_OK.increment();

        Ok(latency)
    }
}

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
    workload_components: &[Component],
) -> PubsubRuntimes {
    PubsubRuntimes {
        publisher_rt: launch_publishers(config, work_receiver, workload_components),
        subscriber_rt: launch_subscribers(config, workload_components),
    }
}

fn launch_publishers(
    config: &Config,
    work_receiver: Receiver<WorkItem>,
    workload_components: &[Component],
) -> Option<Runtime> {
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
        Protocol::Blabber => {
            blabber::launch_publishers(&mut publisher_rt, config.clone(), work_receiver);
        }
        Protocol::Momento => {
            momento::launch_publishers(&mut publisher_rt, config.clone(), work_receiver);
        }
        Protocol::Kafka => {
            kafka::create_topics(&mut publisher_rt, config.clone(), workload_components);
            kafka::launch_publishers(&mut publisher_rt, config.clone(), work_receiver);
        }
        _ => {
            error!("pubsub is not supported for the selected protocol");
            std::process::exit(1);
        }
    }

    Some(publisher_rt)
}

fn launch_subscribers(config: &Config, workload_components: &[Component]) -> Option<Runtime> {
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
        Protocol::Blabber => {
            blabber::launch_subscribers(&mut subscriber_rt, config.clone(), workload_components);
        }
        Protocol::Momento => {
            momento::launch_subscribers(&mut subscriber_rt, config.clone(), workload_components);
        }
        Protocol::Kafka => {
            kafka::launch_subscribers(&mut subscriber_rt, config.clone(), workload_components);
        }
        _ => {
            error!("pubsub is not supported for the selected protocol");
            std::process::exit(1);
        }
    }

    Some(subscriber_rt)
}
