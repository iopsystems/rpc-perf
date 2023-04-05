// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;
use ::momento::preview::topics::TopicClient;
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
        PUBSUB_SUBSCRIBE_OK.increment();
        while RUNNING.load(Ordering::Relaxed) {
            match subscription.item().await {
                Ok(Some(_)) => {
                    RESPONSE_OK.increment();
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_OK.increment();
                    // got some item
                }
                Ok(None) => {
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_CLOSED.increment();
                    break;
                }
                Err(_) => {
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_EX.increment();
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

        CONNECT.increment();
        CONNECT_CURR.add(1);

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

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        let result = match work_item {
            WorkItem::Publish { topic, message } => {
                // todo!();
                PUBSUB_PUBLISH.increment();
                match timeout(
                    config.pubsub().unwrap().publish_timeout(),
                    client.publish(cache_name.clone(), topic.to_string(), message),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        PUBSUB_PUBLISH_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        PUBSUB_PUBLISH_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        PUBSUB_PUBLISH_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            } // _ => {
              //     REQUEST_UNSUPPORTED.increment();
              //     continue;
              // }
        };

        REQUEST_OK.increment();

        let stop = Instant::now();

        match result {
            Ok(_) => {
                RESPONSE_OK.increment();

                let latency = stop.duration_since(start).as_nanos();

                REQUEST_LATENCY.increment(start, latency, 1);
                RESPONSE_LATENCY.increment(stop, latency, 1);
            }
            Err(ResponseError::Exception) => {
                RESPONSE_EX.increment();
            }
            Err(ResponseError::Timeout) => {
                RESPONSE_TIMEOUT.increment();
            }
            Err(ResponseError::Ratelimited) => {
                RESPONSE_RATELIMITED.increment();
            }
            Err(ResponseError::BackendTimeout) => {
                RESPONSE_BACKEND_TIMEOUT.increment();
            }
        }
    }

    Ok(())
}
