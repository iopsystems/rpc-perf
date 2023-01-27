// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

use ::momento::simple_cache_client::Fields;
use ::momento::simple_cache_client::SimpleCacheClient;
use ::momento::simple_cache_client::SimpleCacheClientBuilder;

use std::collections::HashMap;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, poolsize: usize, work_receiver: Receiver<WorkItem>) {
    let client = {
        let _guard = runtime.enter();

        // initialize the Momento cache client
        if std::env::var("MOMENTO_AUTHENTICATION").is_err() {
            eprintln!("environment variable `MOMENTO_AUTHENTICATION` is not set");
            std::process::exit(1);
        }
        let auth_token =
            std::env::var("MOMENTO_AUTHENTICATION").expect("MOMENTO_AUTHENTICATION must be set");
        let client =
            match SimpleCacheClientBuilder::new(auth_token, NonZeroU64::new(600).unwrap()) {
                Ok(c) => c.build(),
                Err(e) => {
                    eprintln!("could not create cache client: {}", e);
                    std::process::exit(1);
                }
            };

        client
    };

    // create one task per channel
    for _ in 0..poolsize {
        runtime.spawn(task(
            client.clone(),
            work_receiver.clone(),
        ));
    }
}

async fn task(
    mut client: SimpleCacheClient,
    work_receiver: Receiver<WorkItem>,
) -> Result<()> {
    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        let start = Instant::now();
        let result = match work_item {
            WorkItem::Get { key } => {
                GET.increment();
                timeout(
                    Duration::from_millis(200),
                    client.get("preview-cache", (*key).to_owned()),
                )
                .await
                .map(|r| r.is_ok())
            },
            WorkItem::Set { key, value } => timeout(
                Duration::from_millis(200),
                client.set("preview-cache", (*key).to_owned(), (*value).to_owned(), None),
            )
            .await
            .map(|r| r.is_ok()),
            WorkItem::HashDelete { key, fields } => timeout(
                Duration::from_millis(200),
                client.dictionary_delete("preview-cache", key.as_str(), Fields::Some(fields.iter().map(|f| f.as_str()).collect())),
            )
            .await
            .map(|r| r.is_ok()),
            WorkItem::HashGet { key, field } => timeout(
                Duration::from_millis(200),
                client.dictionary_get("preview-cache", key.as_str(), vec![field.as_str()]),
            )
            .await
            .map(|r| r.is_ok()),
            WorkItem::HashMultiGet { key, fields } => timeout(
                Duration::from_millis(200),
                client.dictionary_get("preview-cache", key.as_str(), fields.iter().map(|f| f.as_str()).collect()),
            )
            .await
            .map(|r| r.is_ok()),
            WorkItem::HashSet { key, field, value } => timeout(
                Duration::from_millis(200),
                client.dictionary_set(
                    "preview-cache",
                    key.as_str(),
                    HashMap::from([(field.as_str(), value)]),
                    None,
                    false,
                ),
            )
            .await
            .map(|r| r.is_ok()),
            _ => {
                continue;
            }
        };

        let stop = Instant::now();

        if let Ok(ok) = result {
            if ok {
                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            } else {
                RESPONSE_EX.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
        } else {
            RESPONSE_TIMEOUT.increment();
        }
    }

    Ok(())
}