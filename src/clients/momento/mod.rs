use super::*;
use ::momento::response::*;
use ::momento::*;
use std::borrow::Borrow;
use std::collections::HashMap;

mod commands;

use commands::*;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching momento protocol tasks");

    for _ in 0..config.client().unwrap().poolsize() {
        let client = {
            let _guard = runtime.enter();

            // initialize the Momento cache client
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
            match SimpleCacheClientBuilder::new(
                credential_provider,
                std::time::Duration::from_secs(900),
            ) {
                Ok(c) => c.build(),
                Err(e) => {
                    eprintln!("could not create cache client: {}", e);
                    std::process::exit(1);
                }
            }
        };

        CONNECT.increment();
        CONNECT_CURR.increment();

        // create one task per channel
        for _ in 0..config.client().unwrap().concurrency() {
            runtime.spawn(task(config.clone(), client.clone(), work_receiver.clone()));
        }
    }
}

async fn task(
    config: Config,
    // cache_name: String,
    mut client: SimpleCacheClient,
    work_receiver: Receiver<WorkItem>,
) -> Result<()> {
    let cache_name = config.target().cache_name().unwrap_or_else(|| {
        eprintln!("cache name is not specified in the `target` section");
        std::process::exit(1);
    });

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        let result = match work_item {
            WorkItem::Request { request, .. } => match request {
                /*
                 * KEY-VALUE
                 */
                ClientRequest::Get(r) => get(&mut client, &config, cache_name, r).await,
                ClientRequest::Set(r) => set(&mut client, &config, cache_name, r).await,
                ClientRequest::Delete(r) => delete(&mut client, &config, cache_name, r).await,

                /*
                 * HASHES (DICTIONARIES)
                 */
                ClientRequest::HashDelete(r) => {
                    hash_delete(&mut client, &config, cache_name, r).await
                }
                ClientRequest::HashGet(r) => hash_get(&mut client, &config, cache_name, r).await,
                ClientRequest::HashGetAll(r) => {
                    hash_get_all(&mut client, &config, cache_name, r).await
                }
                ClientRequest::HashIncrement(r) => {
                    hash_increment(&mut client, &config, cache_name, r).await
                }
                ClientRequest::HashSet(r) => hash_set(&mut client, &config, cache_name, r).await,

                /*
                 * SETS
                 */
                ClientRequest::SetAdd(r) => set_add(&mut client, &config, cache_name, r).await,
                ClientRequest::SetMembers(r) => {
                    set_members(&mut client, &config, cache_name, r).await
                }
                ClientRequest::SetRemove(r) => {
                    set_remove(&mut client, &config, cache_name, r).await
                }

                /*
                 * LISTS
                 */
                ClientRequest::ListPushFront(r) => {
                    list_push_front(&mut client, &config, cache_name, r).await
                }
                ClientRequest::ListPushBack(r) => {
                    list_push_back(&mut client, &config, cache_name, r).await
                }
                ClientRequest::ListFetch(r) => {
                    list_fetch(&mut client, &config, cache_name, r).await
                }
                ClientRequest::ListLength(r) => {
                    list_length(&mut client, &config, cache_name, r).await
                }
                ClientRequest::ListPopFront(r) => {
                    list_pop_front(&mut client, &config, cache_name, r).await
                }
                ClientRequest::ListPopBack(r) => {
                    list_pop_back(&mut client, &config, cache_name, r).await
                }
                ClientRequest::ListRemove(r) => {
                    list_remove(&mut client, &config, cache_name, r).await
                }

                /*
                 * SORTED SETS
                 */
                ClientRequest::SortedSetAdd(r) => {
                    sorted_set_add(&mut client, &config, cache_name, r).await
                }
                ClientRequest::SortedSetRange(r) => {
                    sorted_set_range(&mut client, &config, cache_name, r).await
                }
                ClientRequest::SortedSetIncrement(r) => {
                    sorted_set_increment(&mut client, &config, cache_name, r).await
                }
                ClientRequest::SortedSetRank(r) => {
                    sorted_set_rank(&mut client, &config, cache_name, r).await
                }
                ClientRequest::SortedSetRemove(r) => {
                    sorted_set_remove(&mut client, &config, cache_name, r).await
                }
                ClientRequest::SortedSetScore(r) => {
                    sorted_set_score(&mut client, &config, cache_name, r).await
                }

                /*
                 * UNSUPPORTED
                 */
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                    continue;
                }
            },
            WorkItem::Reconnect => {
                continue;
            }
        };

        REQUEST_OK.increment();

        let stop = Instant::now();

        match result {
            Ok(_) => {
                RESPONSE_OK.increment();

                let latency = stop.duration_since(start).as_nanos() as u64;

                let _ = RESPONSE_LATENCY.increment(latency);
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
