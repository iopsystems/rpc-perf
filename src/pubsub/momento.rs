use super::*;

use ::momento::topics::configurations::LowLatency;
use ::momento::topics::{TopicClient, ValueKind};
use ::momento::CredentialProvider;
use futures::stream::StreamExt;
use tokio::time::timeout;

use std::sync::Arc;
use std::time::Instant;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_subscribers(
    runtime: &mut Runtime,
    config: Config,
    workload_components: &[Component],
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

                    // initialize the Momento topic client
                    if std::env::var("MOMENTO_API_KEY").is_err() {
                        eprintln!("environment variable `MOMENTO_API_KEY` is not set");
                        std::process::exit(1);
                    }

                    let credential_provider =
                        match CredentialProvider::from_env_var("MOMENTO_API_KEY".to_string()) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("MOMENTO_API_KEY key should be valid: {e}");
                                std::process::exit(1);
                            }
                        };

                    match TopicClient::builder()
                        .configuration(LowLatency::v1())
                        .credential_provider(credential_provider)
                        .build()
                    {
                        Ok(c) => Arc::new(c),
                        Err(e) => {
                            eprintln!("could not create topic client: {}", e);
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
        .subscribe(cache_name.clone(), topic.to_string())
        .await
    {
        PUBSUB_SUBSCRIBER_CURR.add(1);
        PUBSUB_SUBSCRIBE_OK.increment();

        let validator = MessageValidator::new();

        while RUNNING.load(Ordering::Relaxed) {
            match subscription.next().await {
                Some(v) => {
                    if let ValueKind::Binary(mut v) = v.kind {
                        let _ = validator.validate(&mut v);
                    } else {
                        error!("there was a string in the topic");
                        // unexpected message
                        PUBSUB_RECEIVE.increment();
                        PUBSUB_RECEIVE_EX.increment();
                    }
                }
                None => {
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_CLOSED.increment();
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

            // initialize the Momento topic client
            if std::env::var("MOMENTO_API_KEY").is_err() {
                eprintln!("environment variable `MOMENTO_API_KEY` is not set");
                std::process::exit(1);
            }

            let credential_provider =
                match CredentialProvider::from_env_var("MOMENTO_API_KEY".to_string()) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("MOMENTO_API_KEY key should be valid: {e}");
                        std::process::exit(1);
                    }
                };

            match TopicClient::builder()
                .configuration(LowLatency::v1())
                .credential_provider(credential_provider)
                .build()
            {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    eprintln!("could not create topic client: {}", e);
                    std::process::exit(1);
                }
            }
        };

        PUBSUB_PUBLISHER_CONNECT.increment();

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
    PUBSUB_PUBLISHER_CURR.add(1);

    let cache_name = config
        .target()
        .cache_name()
        .unwrap_or_else(|| {
            eprintln!("cache name is not specified in the `target` section");
            std::process::exit(1);
        })
        .to_string();

    let validator = MessageValidator::new();

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        let result = match work_item {
            WorkItem::Publish {
                topic,
                mut message,
                key: _,
            } => {
                validator.stamp(&mut message);

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
                let latency = stop.duration_since(start).as_nanos() as u64;

                PUBSUB_PUBLISH_OK.increment();
                let _ = PUBSUB_PUBLISH_LATENCY.increment(latency);
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

    PUBSUB_PUBLISHER_CURR.sub(1);

    Ok(())
}
