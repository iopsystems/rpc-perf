use crate::clients::*;
use crate::workload::Component;
use crate::workload::PublisherWorkItem as WorkItem;
use crate::*;
use ahash::RandomState;
use async_channel::Receiver;
use clocksource::Nanoseconds;
use std::io::{Error, ErrorKind, Result};
use tokio::runtime::Runtime;

mod kafka;
mod momento;

struct MessageStamp {
    hash_builder: RandomState,
}
pub enum MessageValidator {
    ValidatedMessage(clocksource::Duration<Nanoseconds<u64>>, Instant),
    UnexpectedMessage,
    CorruptedMessage,
}
impl MessageStamp {
    // Deterministic seeds are used so that multiple MessageStamp can stamp and validate messages
    pub fn new() -> Self {
        MessageStamp {
            hash_builder: RandomState::with_seeds(
                0xd5b96f9126d61cee,
                0x50af85c9d1b6de70,
                0xbd7bdf2fee6d15b2,
                0x3dbe88bb183ac6f4,
            ),
        }
    }
    pub fn stamp_msg(&self, message: &mut Vec<u8>) -> u64 {
        let timestamp = (UnixInstant::now() - UnixInstant::from_nanos(0)).as_nanos();
        let ts = timestamp.to_be_bytes();
        // write the current unix time into the message
        [
            message[16],
            message[17],
            message[18],
            message[19],
            message[20],
            message[21],
            message[22],
            message[23],
        ] = ts;

        // todo, write a sequence number into the message

        // checksum the message and put the checksum into the message
        [
            message[8],
            message[9],
            message[10],
            message[11],
            message[12],
            message[13],
            message[14],
            message[15],
        ] = self.hash_builder.hash_one(&message).to_be_bytes();
        timestamp
    }
    pub fn validate_msg(&self, v: &mut Vec<u8>) -> MessageValidator {
        let now = Instant::now();
        let now_unix = UnixInstant::now();
        if [v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]]
            != [0x54, 0x45, 0x53, 0x54, 0x49, 0x4E, 0x47, 0x21]
        {
            return MessageValidator::UnexpectedMessage;
        }
        let csum = [v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]];
        [v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]] = [0; 8];
        if csum != self.hash_builder.hash_one(&v).to_be_bytes() {
            return MessageValidator::CorruptedMessage;
        }
        let ts = u64::from_be_bytes([v[16], v[17], v[18], v[19], v[20], v[21], v[22], v[23]]);

        let latency = now_unix - UnixInstant::from_nanos(ts);
        let then = now - latency;
        MessageValidator::ValidatedMessage(latency, then)
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
    workload_components: Vec<Component>,
) -> PubsubRuntimes {
    if config.pubsub().is_some() {
        match config.general().protocol() {
            Protocol::Kafka => {
                let mut topic_rt = Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(1)
                    .build()
                    .expect("failed to initialize tokio runtime");
                kafka::create_topics(&mut topic_rt, config.clone(), &workload_components)
            }
            _ => {}
        }
    }

    PubsubRuntimes {
        publisher_rt: launch_publishers(config, work_receiver),
        subscriber_rt: launch_subscribers(config, &workload_components),
    }
}

fn launch_publishers(config: &Config, work_receiver: Receiver<WorkItem>) -> Option<Runtime> {
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
            kafka::launch_publishers(&mut publisher_rt, config.clone(), work_receiver);
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
