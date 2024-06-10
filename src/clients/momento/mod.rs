use super::*;

use ::momento::cache::configurations::LowLatency;
use ::momento::*;

mod commands;

use commands::*;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching momento protocol tasks");

    for _ in 0..config.client().unwrap().poolsize() {
        let client = {
            let _guard = runtime.enter();

            // initialize the Momento cache client
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

            match CacheClient::builder()
                .default_ttl(Duration::from_secs(900))
                .configuration(LowLatency::v1())
                .credential_provider(credential_provider)
                .build()
            {
                Ok(c) => c,
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
    mut client: CacheClient,
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
