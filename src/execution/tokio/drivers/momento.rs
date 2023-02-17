// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use std::borrow::Borrow;
use super::*;

use ::momento::response::*;
use ::momento::*;
// use ::momento::simple_cache_client::SimpleCacheClient;
// use ::momento::simple_cache_client::SimpleCacheClientBuilder;

use std::collections::HashMap;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching momento protocol tasks");
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
            match SimpleCacheClientBuilder::new(auth_token, std::time::Duration::from_secs(600)) {
                Ok(c) => c.build(),
                Err(e) => {
                    eprintln!("could not create cache client: {}", e);
                    std::process::exit(1);
                }
            };

        client
    };

    CONNECT.increment();
    CONNECT_CURR.add(1);

    // create one task per channel
    for _ in 0..config.connection().poolsize() {
        runtime.spawn(task(client.clone(), work_receiver.clone()));
    }
}

async fn task(mut client: SimpleCacheClient, work_receiver: Receiver<WorkItem>) -> Result<()> {
    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        let start = Instant::now();
        let result = match work_item {
            WorkItem::Get { key } => {
                GET.increment();
                match timeout(
                    Duration::from_millis(200),
                    client.get("preview-cache", &*key),
                )
                .await
                {
                    Ok(Ok(r)) => match r.result {
                        MomentoGetStatus::HIT => {
                            GET_KEY_HIT.increment();
                            Ok(())
                        }
                        MomentoGetStatus::MISS => {
                            GET_KEY_MISS.increment();
                            Ok(())
                        }
                        MomentoGetStatus::ERROR => {
                            GET_EX.increment();
                            Err(ResponseError::Exception)
                        }
                    },
                    Ok(Err(_)) => {
                        GET_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::Set { key, value } => {
                SET.increment();
                match timeout(
                    Duration::from_millis(200),
                    client.set(
                        "preview-cache",
                        (*key).to_owned(),
                        (*value).to_owned(),
                        None,
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SET_STORED.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        SET_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashDelete { key, fields } => {
                // HDEL.increment();
                match timeout(
                    Duration::from_millis(200),
                    client.dictionary_delete(
                        "preview-cache",
                        &*key,
                        Fields::Some(fields.iter().map(|f| &**f).collect()),
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        // HDEL_DELETED.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        // HDEL_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashGet { key, field } => {
                // HGET.increment();
                match timeout(
                    Duration::from_millis(200),
                    client.dictionary_get(
                        "preview-cache",
                        &*key,
                        vec![&*field],
                    ),
                )
                .await
                {
                    Ok(Ok(r)) => match r.result {
                        MomentoDictionaryGetStatus::FOUND => {
                            // HGET_FIELD_HIT.increment();
                            Ok(())
                        }
                        MomentoDictionaryGetStatus::MISSING => {
                            // HGET_FIELD_MISS.increment();
                            Ok(())
                        }
                        MomentoDictionaryGetStatus::ERROR => {
                            // HGET_EX.increment();
                            Err(ResponseError::Exception)
                        }
                    },
                    Ok(Err(_)) => {
                        // HGET_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashIncrement { key, field, amount } => {
                // HGET.increment();
                match timeout(
                    Duration::from_millis(200),
                    client.dictionary_increment(
                        "preview-cache",
                        &*key,
                        &*field,
                        amount,
                        CollectionTtl::new(None, false),
                    ),
                )
                .await
                {
                    Ok(Ok(r)) => {
                        #[allow(clippy::if_same_then_else)]
                        if r.value == amount {
                            // miss
                            // HINCRBY_MISS.increment();
                        } else {
                            // hit
                            // HINCRBY_HIT.increment();
                        }
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        // HINCRBY_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashMultiGet { key, fields } => {
                HASH_GET.increment();
                match timeout(
                    Duration::from_millis(200),
                    client.dictionary_get(
                        "preview-cache",
                        &*key,
                        fields.iter().map(|f| &**f).collect(),
                    ),
                )
                .await
                {
                    Ok(Ok(r)) => match r.dictionary {
                        Some(dict) => {
                            let mut hit = 0;
                            let mut miss = 0;
                            for field in fields {
                                if dict.contains_key(&*field) {
                                    hit += 1;
                                } else {
                                    miss += 1;
                                }
                            }
                            HASH_GET_FIELD_HIT.add(hit);
                            HASH_GET_FIELD_MISS.add(miss);
                            Ok(())
                        }
                        None => {
                            HASH_GET_FIELD_MISS.add(fields.len() as _);
                            Ok(())
                        }
                    },
                    Ok(Err(_)) => {
                        HASH_GET_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashSet { key, data } => {
                let fields = data.len();
                HASH_SET.increment();
                let data: HashMap<Vec<u8>, Vec<u8>> = data
                    .iter()
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .collect();
                match timeout(
                    Duration::from_millis(200),
                    client.dictionary_set(
                        "preview-cache",
                        Into::<Vec<u8>>::into(&*key),
                        data,
                        CollectionTtl::new(None, false),
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        HASH_SET_STORED.add(fields as _);
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        HASH_GET_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::SortedSetAdd { key, data } => {
                let data: Vec<sorted_set::SortedSetElement> = data
                    .iter()
                    .map(|(name, score)| sorted_set::SortedSetElement { name: name.to_vec(), score: *score })
                    .collect();
                match timeout(
                    Duration::from_millis(200),
                    client.sorted_set_put(
                        "preview-cache",
                        Into::<Vec<u8>>::into(&*key),
                        data,
                        CollectionTtl::new(None, false),
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        // SORTED_SET_SET_STORED.add(fields as _);
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        // SORTED_SET_SET_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
                // Ok(())
            }
            WorkItem::SortedSetIncrement { key, member, amount } => {
                match timeout(
                    Duration::from_millis(200),
                    client.sorted_set_increment(
                        "preview-cache",
                        &*key,
                        &*member,
                        amount,
                        CollectionTtl::new(None, false),
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        // SORTED_SET_SET_STORED.add(fields as _);
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        // SORTED_SET_SET_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
                // Ok(())
            }
            WorkItem::SortedSetRemove { key, data } => {
                let names: Vec<&[u8]> = data.iter().map(|v| v.borrow()).collect();
                match timeout(
                    Duration::from_millis(200),
                    client.sorted_set_remove(
                        "preview-cache",
                        &*key,
                        names,
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        // SORTED_SET_SET_STORED.add(fields as _);
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        // SORTED_SET_SET_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
                // Ok(())
            }
            _ => {
                continue;
            }
        };

        let stop = Instant::now();

        match result {
            Ok(_) => {
                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Err(ResponseError::Exception) => {
                RESPONSE_EX.increment();
            }
            Err(ResponseError::Timeout) => {
                RESPONSE_TIMEOUT.increment();
            }
        }
    }

    Ok(())
}
