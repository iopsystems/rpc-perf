// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;
use ::momento::preview::topics::{SubscriptionItem, TopicClient, ValueKind};
use ahash::RandomState;
use std::sync::Arc;
use tokio::time::timeout;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_subscribers(
    runtime: &mut Runtime,
    config: Config,
    workload_components: Vec<Component>,
) {
    debug!("launching momento subscriber tasks");

    let cache_name = config
        .target()
        .cache_name()
        .unwrap_or_else(|| {
            eprintln!("cache name is not specified in the `target` section");
            std::process::exit(1);
        })
        .to_string();

    for component in workload_components {
        if let Component::Topics(topics) = component {
            let poolsize = topics.subscriber_poolsize();
            let concurrency = topics.subscriber_concurrency();

            for _ in 0..poolsize {
                let client = {
                    let _guard = runtime.enter();

                    // initialize the Momento cache client
                    if std::env::var("MOMENTO_AUTHENTICATION").is_err() {
                        eprintln!("environment variable `MOMENTO_AUTHENTICATION` is not set");
                        std::process::exit(1);
                    }
                    let auth_token = std::env::var("MOMENTO_AUTHENTICATION")
                        .expect("MOMENTO_AUTHENTICATION must be set");
                    match TopicClient::connect(auth_token, None, None) {
                        Ok(c) => Arc::new(c),
                        Err(e) => {
                            eprintln!("could not create cache client: {}", e);
                            std::process::exit(1);
                        }
                    }
                };

                for _ in 0..concurrency {
                    for topic in topics.topics() {
                        runtime.spawn(subscriber_task(
                            client.clone(),
                            cache_name.clone(),
                            topic.to_string(),
                        ));
                    }
                }
            }
        }
    }
}

async fn subscriber_task(client: Arc<TopicClient>, cache_name: String, topic: String) {
    PUBSUB_SUBSCRIBE.increment();
    if let Ok(mut subscription) = client
        .subscribe(cache_name.clone(), topic.to_string(), None)
        .await
    {
        PUBSUB_SUBSCRIBER_CURR.add(1);
        PUBSUB_SUBSCRIBE_OK.increment();

        // Create a new hasher state to validate the integrity of received
        // messages. Deterministic seeds are used so that multiple processes can
        // verify the messages.
        let hash_builder = RandomState::with_seeds(
            0xd5b96f9126d61cee,
            0x50af85c9d1b6de70,
            0xbd7bdf2fee6d15b2,
            0x3dbe88bb183ac6f4,
        );

        while RUNNING.load(Ordering::Relaxed) {
            match subscription.item().await {
                Ok(Some(SubscriptionItem::Value(v))) => {
                    if let ValueKind::Binary(mut v) = v.kind {
                        let now = Instant::now();
                        let now_unix = UnixInstant::now();

                        if [v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]]
                            != [0x54, 0x45, 0x53, 0x54, 0x49, 0x4E, 0x47, 0x21]
                        {
                            // unexpected message
                            error!("pubsub: invalid message received");
                            RESPONSE_EX.increment();
                            PUBSUB_RECEIVE_INVALID.increment();
                            continue;
                        }

                        // grab the checksum and zero it in the message
                        let csum = [v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]];
                        [v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]] = [0; 8];

                        if csum != hash_builder.hash_one(&v).to_be_bytes() {
                            // corrupted message
                            error!("pubsub: corrupt message received");
                            PUBSUB_RECEIVE.increment();
                            PUBSUB_RECEIVE_CORRUPT.increment();
                            continue;
                        }

                        let ts = u64::from_be_bytes([
                            v[16], v[17], v[18], v[19], v[20], v[21], v[22], v[23],
                        ]);

                        let latency = now_unix - UnixInstant::from_nanos(ts);
                        let then = now - latency;

                        PUBSUB_LATENCY.increment(then, latency.as_nanos(), 1);

                        PUBSUB_RECEIVE.increment();
                        PUBSUB_RECEIVE_OK.increment();
                    } else {
                        error!("there was a string in the topic");
                        // unexpected message
                        PUBSUB_RECEIVE.increment();
                        PUBSUB_RECEIVE_EX.increment();
                    }
                }
                Ok(Some(SubscriptionItem::Discontinuity(_))) => {
                    // todo: do something about discontinuities?
                }
                Ok(None) => {
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_CLOSED.increment();
                    PUBSUB_SUBSCRIBER_CURR.sub(1);
                    break;
                }
                Err(_) => {
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_EX.increment();
                    PUBSUB_SUBSCRIBER_CURR.sub(1);
                    break;
                }
            }
        }
    } else {
        PUBSUB_SUBSCRIBE_EX.increment();
    }
}

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_publishers(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching momento protocol tasks");

    for _ in 0..config.pubsub().unwrap().publisher_poolsize() {
        let client = {
            let _guard = runtime.enter();

            // initialize the Momento cache client
            if std::env::var("MOMENTO_AUTHENTICATION").is_err() {
                eprintln!("environment variable `MOMENTO_AUTHENTICATION` is not set");
                std::process::exit(1);
            }
            let auth_token = std::env::var("MOMENTO_AUTHENTICATION")
                .expect("MOMENTO_AUTHENTICATION must be set");
            match TopicClient::connect(auth_token, None, None) {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    eprintln!("could not create cache client: {}", e);
                    std::process::exit(1);
                }
            }
        };

        PUBSUB_PUBLISHER_CONNECT.increment();
        PUBSUB_PUBLISHER_CURR.add(1);

        // create one task per channel
        for _ in 0..config.pubsub().unwrap().publisher_concurrency() {
            runtime.spawn(publisher_task(
                config.clone(),
                client.clone(),
                work_receiver.clone(),
            ));
        }
    }
}

async fn publisher_task(
    config: Config,
    // cache_name: String,
    client: Arc<TopicClient>,
    work_receiver: Receiver<WorkItem>,
) -> Result<()> {
    let cache_name = config
        .target()
        .cache_name()
        .unwrap_or_else(|| {
            eprintln!("cache name is not specified in the `target` section");
            std::process::exit(1);
        })
        .to_string();

    // Create a new hasher state to validate the integrity of received
    // messages. Deterministic seeds are used so that multiple processes can
    // verify the messages.
    let hash_builder = RandomState::with_seeds(
        0xd5b96f9126d61cee,
        0x50af85c9d1b6de70,
        0xbd7bdf2fee6d15b2,
        0x3dbe88bb183ac6f4,
    );

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        let now_unix = UnixInstant::now();
        let result = match work_item {
            WorkItem::Publish { topic, mut message } => {
                let ts = (now_unix - UnixInstant::from_nanos(0))
                    .as_nanos()
                    .to_be_bytes();

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
                ] = hash_builder.hash_one(&message).to_be_bytes();

                PUBSUB_PUBLISH.increment();

                match timeout(
                    config.pubsub().unwrap().publish_timeout(),
                    client.publish(cache_name.clone(), topic.to_string(), message),
                )
                .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(e)) => {
                        PUBSUB_PUBLISH_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
        };

        let stop = Instant::now();

        match result {
            Ok(_) => {
                let latency = stop.duration_since(start).as_nanos();

                PUBSUB_PUBLISH_OK.increment();
                PUBSUB_PUBLISH_LATENCY.increment(start, latency, 1);
            }
            Err(ResponseError::Exception) => {
                PUBSUB_PUBLISH_EX.increment();
            }
            Err(ResponseError::Timeout) | Err(ResponseError::BackendTimeout) => {
                PUBSUB_PUBLISH_TIMEOUT.increment();
            }
            Err(ResponseError::Ratelimited) => {
                PUBSUB_PUBLISH_RATELIMITED.increment();
            }
        }
    }

    Ok(())
}
