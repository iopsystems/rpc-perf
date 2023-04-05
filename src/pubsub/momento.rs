// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use std::sync::Arc;
use ::momento::preview::topics::TopicClient;
use tokio::time::timeout;
use super::*;
// use std::borrow::Borrow;

// use ::momento::response::*;
// use ::momento::*;

// use std::collections::HashMap;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_subscribers(runtime: &mut Runtime, config: Config) {
    todo!();
    // debug!("launching momento protocol tasks");

    // for _ in 0..config.client().poolsize() {
    //     let client = {
    //         let _guard = runtime.enter();

    //         // initialize the Momento cache client
    //         if std::env::var("MOMENTO_AUTHENTICATION").is_err() {
    //             eprintln!("environment variable `MOMENTO_AUTHENTICATION` is not set");
    //             std::process::exit(1);
    //         }
    //         let auth_token =
    //             std::env::var("MOMENTO_AUTHENTICATION").expect("MOMENTO_AUTHENTICATION must be set");
    //         match TopicClient::connect(auth_token, None, None) {
    //             Ok(c) => Arc::new(c),
    //             Err(e) => {
    //                 eprintln!("could not create cache client: {}", e);
    //                 std::process::exit(1);
    //             }
    //         }
    //     };

    //     CONNECT.increment();
    //     CONNECT_CURR.add(1);

    //     // create one task per channel
    //     for _ in 0..config.client().concurrency() {
    //         runtime.spawn(publisher_task(config.clone(), client.clone(), work_receiver.clone()));
    //     }
    // }
}

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_publishers(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching momento protocol tasks");

    for _ in 0..config.client().poolsize() {
        let client = {
            let _guard = runtime.enter();

            // initialize the Momento cache client
            if std::env::var("MOMENTO_AUTHENTICATION").is_err() {
                eprintln!("environment variable `MOMENTO_AUTHENTICATION` is not set");
                std::process::exit(1);
            }
            let auth_token =
                std::env::var("MOMENTO_AUTHENTICATION").expect("MOMENTO_AUTHENTICATION must be set");
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
        for _ in 0..config.client().concurrency() {
            runtime.spawn(publisher_task(config.clone(), client.clone(), work_receiver.clone()));
        }
    }
}

async fn publisher_task(
    config: Config,
    // cache_name: String,
    client: Arc<TopicClient>,
    work_receiver: Receiver<WorkItem>,
) -> Result<()> {
    let cache_name = config.target().cache_name().unwrap_or_else(|| {
        eprintln!("cache name is not specified in the `target` section");
        std::process::exit(1);
    }).to_string();

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
                // GET.increment();
                match timeout(config.client().request_timeout(), client.publish(cache_name.clone(), topic.to_string(), message)).await {
                    Ok(Ok(_)) => {
                        // PUBLISH_OK.increment();
                        Ok(())
                    },
                    Ok(Err(e)) => {
                        // PUBLISH_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        // PUBLISH_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            _ => {
                REQUEST_UNSUPPORTED.increment();
                continue;
            }
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
