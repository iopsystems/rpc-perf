use super::*;
use ::momento::preview::topics::{SubscriptionItem, TopicClient, ValueKind};
use ::momento::CredentialProviderBuilder;
use futures::stream::StreamExt;
use std::sync::Arc;
use tokio::time::timeout;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_subscribers(
    runtime: &mut Runtime,
    config: Config,
    workload_components: &Vec<Component>,
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
                    if std::env::var("MOMENTO_AUTHENTICATION").is_err() {
                        eprintln!("environment variable `MOMENTO_AUTHENTICATION` is not set");
                        std::process::exit(1);
                    }
                    let auth_token = std::env::var("MOMENTO_AUTHENTICATION")
                        .expect("MOMENTO_AUTHENTICATION must be set");
                    let credential_provider = CredentialProviderBuilder::from_string(auth_token)
                        .build()
                        .unwrap_or_else(|e| {
                            eprintln!("failed to initialize credential provider. error: {e}");
                            std::process::exit(1);
                        });
                    match TopicClient::connect(credential_provider, None) {
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

        let msg_stamp = MessageStamp::new();

        while RUNNING.load(Ordering::Relaxed) {
            match subscription.next().await {
                Some(SubscriptionItem::Value(v)) => {
                    if let ValueKind::Binary(mut v) = v.kind {
                        match msg_stamp.validate_msg(&mut v) {
                            MessageValidationResult::Unexpected => {
                                error!("pubsub: invalid message received");
                                RESPONSE_EX.increment();
                                PUBSUB_RECEIVE_INVALID.increment();
                                continue;
                            }
                            MessageValidationResult::Corrupted => {
                                error!("pubsub: corrupt message received");
                                PUBSUB_RECEIVE.increment();
                                PUBSUB_RECEIVE_CORRUPT.increment();
                                continue;
                            }
                            MessageValidationResult::Validated(latency) => {
                                let _ = PUBSUB_LATENCY.increment(latency);
                                PUBSUB_RECEIVE.increment();
                                PUBSUB_RECEIVE_OK.increment();
                            }
                        }
                    } else {
                        error!("there was a string in the topic");
                        // unexpected message
                        PUBSUB_RECEIVE.increment();
                        PUBSUB_RECEIVE_EX.increment();
                    }
                }
                Some(SubscriptionItem::Discontinuity(_)) => {
                    // todo: do something about discontinuities?
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
            if std::env::var("MOMENTO_AUTHENTICATION").is_err() {
                eprintln!("environment variable `MOMENTO_AUTHENTICATION` is not set");
                std::process::exit(1);
            }
            let auth_token = std::env::var("MOMENTO_AUTHENTICATION")
                .expect("MOMENTO_AUTHENTICATION must be set");
            let credential_provider = CredentialProviderBuilder::from_string(auth_token)
                .build()
                .unwrap_or_else(|e| {
                    eprintln!("failed to initialize credential provider. error: {e}");
                    std::process::exit(1);
                });
            match TopicClient::connect(credential_provider, None) {
                Ok(c) => Arc::new(c),
                Err(e) => {
                    eprintln!("could not create cache client: {}", e);
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

    let msg_stamp = MessageStamp::new();

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
                partition: _,
                key: _,
            } => {
                msg_stamp.stamp_msg(&mut message);
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
