// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;
use std::borrow::Borrow;

use ::momento::response::*;
use ::momento::*;

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
    for _ in 0..(config.connection().poolsize() * config.general().threads()) {
        runtime.spawn(task(config.clone(), client.clone(), work_receiver.clone()));
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
            WorkItem::Get { key } => {
                GET.increment();
                match timeout(config.request().timeout(), client.get(cache_name, &*key)).await {
                    Ok(Ok(r)) => match r.result {
                        MomentoGetStatus::HIT => {
                            GET_OK.increment();
                            GET_KEY_HIT.increment();
                            Ok(())
                        }
                        MomentoGetStatus::MISS => {
                            GET_OK.increment();
                            GET_KEY_MISS.increment();
                            Ok(())
                        }
                        MomentoGetStatus::ERROR => {
                            GET_EX.increment();
                            Err(ResponseError::Exception)
                        }
                    },
                    Ok(Err(e)) => {
                        GET_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        GET_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::Set { key, value } => {
                SET.increment();
                match timeout(
                    config.request().timeout(),
                    client.set(cache_name, (*key).to_owned(), (*value).to_owned(), None),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SET_STORED.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SET_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SET_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::Delete { key } => {
                DELETE.increment();
                match timeout(
                    config.request().timeout(),
                    client.delete(cache_name, (*key).to_owned()),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        DELETE_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        DELETE_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        DELETE_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }

            /*
             * HASHES (DICTIONARIES)
             */
            WorkItem::HashDelete { key, fields } => {
                HASH_DELETE.increment();
                match timeout(
                    config.request().timeout(),
                    client.dictionary_delete(
                        cache_name,
                        &*key,
                        Fields::Some(fields.iter().map(|f| &**f).collect()),
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        HASH_DELETE_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        HASH_DELETE_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        HASH_DELETE_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::HashGet { key, fields } => {
                HASH_GET.increment();
                match timeout(
                    config.request().timeout(),
                    client.dictionary_get(cache_name, &*key, fields.iter().map(|f| &**f).collect()),
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
                    Ok(Err(e)) => {
                        HASH_GET_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        HASH_GET_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::HashGetAll { key } => {
                HASH_GET_ALL.increment();
                match timeout(
                    config.request().timeout(),
                    client.dictionary_fetch(cache_name, &*key),
                )
                .await
                {
                    Ok(Ok(r)) => match r.dictionary {
                        Some(_) => {
                            HASH_GET_ALL_HIT.increment();
                            Ok(())
                        }
                        None => {
                            HASH_GET_ALL_MISS.increment();
                            Ok(())
                        }
                    },
                    Ok(Err(e)) => {
                        HASH_GET_ALL_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        HASH_GET_ALL_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::HashIncrement { key, field, amount } => {
                HASH_INCR.increment();
                match timeout(
                    config.request().timeout(),
                    client.dictionary_increment(
                        cache_name,
                        &*key,
                        &*field,
                        amount,
                        CollectionTtl::new(None, false),
                    ),
                )
                .await
                {
                    Ok(Ok(r)) => {
                        HASH_INCR_OK.increment();
                        #[allow(clippy::if_same_then_else)]
                        if r.value == amount {
                            HASH_INCR_MISS.increment();
                        } else {
                            HASH_INCR_HIT.increment();
                        }
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        HASH_INCR_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        HASH_INCR_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::HashSet { key, data } => {
                HASH_SET.increment();
                let data: HashMap<Vec<u8>, Vec<u8>> =
                    data.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
                match timeout(
                    config.request().timeout(),
                    client.dictionary_set(cache_name, &*key, data, CollectionTtl::new(None, false)),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        HASH_SET_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        HASH_SET_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        HASH_SET_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }

            /*
             * SETS
             */
            WorkItem::SetAdd { key, members } => {
                SET_ADD.increment();
                let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                match timeout(
                    config.request().timeout(),
                    client.set_add_elements(
                        cache_name,
                        &*key,
                        members,
                        CollectionTtl::new(None, false),
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SET_ADD_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SET_ADD_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SET_ADD_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::SetMembers { key } => {
                SET_MEMBERS.increment();
                match timeout(
                    config.request().timeout(),
                    client.set_fetch(cache_name, &*key),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SET_MEMBERS_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SET_MEMBERS_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SET_MEMBERS_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::SetRemove { key, members } => {
                SET_REMOVE.increment();
                let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                match timeout(
                    config.request().timeout(),
                    client.set_remove_elements(cache_name, &*key, members),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SET_REMOVE_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SET_REMOVE_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SET_REMOVE_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }

            /*
             * LISTS
             */
            WorkItem::ListPushFront {
                key,
                elements,
                truncate,
            } => {
                LIST_PUSH_FRONT.increment();
                let result = if elements.len() == 1 {
                    match timeout(
                        config.request().timeout(),
                        client.list_push_front(
                            cache_name,
                            &*key,
                            &*elements[0],
                            truncate,
                            CollectionTtl::new(None, false),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e.into()),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                } else {
                    // note: we need to reverse because the semantics of list
                    // concat do not match the redis push semantics
                    let elements: Vec<&[u8]> = elements.iter().map(|v| v.borrow()).rev().collect();
                    match timeout(
                        config.request().timeout(),
                        client.list_concat_front(
                            cache_name,
                            &*key,
                            elements,
                            truncate,
                            CollectionTtl::new(None, false),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e.into()),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                };
                match result {
                    Ok(_) => {
                        LIST_PUSH_FRONT_OK.increment();
                    }
                    Err(ResponseError::Timeout) => {
                        LIST_PUSH_FRONT_TIMEOUT.increment();
                    }
                    Err(_) => {
                        LIST_PUSH_FRONT_EX.increment();
                    }
                }

                result
            }
            WorkItem::ListPushBack {
                key,
                elements,
                truncate,
            } => {
                LIST_PUSH_BACK.increment();
                let result = if elements.len() == 1 {
                    match timeout(
                        config.request().timeout(),
                        client.list_push_back(
                            cache_name,
                            &*key,
                            &*elements[0],
                            truncate,
                            CollectionTtl::new(None, false),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e.into()),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                } else {
                    // note: we need to reverse because the semantics of list
                    // concat do not match the redis push semantics
                    let elements: Vec<&[u8]> = elements.iter().map(|v| v.borrow()).rev().collect();
                    match timeout(
                        config.request().timeout(),
                        client.list_concat_back(
                            cache_name,
                            &*key,
                            elements,
                            truncate,
                            CollectionTtl::new(None, false),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e.into()),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                };
                match result {
                    Ok(_) => {
                        LIST_PUSH_BACK_OK.increment();
                    }
                    Err(ResponseError::Timeout) => {
                        LIST_PUSH_BACK_TIMEOUT.increment();
                    }
                    Err(_) => {
                        LIST_PUSH_BACK_EX.increment();
                    }
                }

                result
            }
            WorkItem::ListFetch { key } => {
                LIST_FETCH.increment();
                match timeout(
                    config.request().timeout(),
                    client.list_fetch(cache_name, &*key),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        LIST_FETCH_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        LIST_FETCH_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        LIST_FETCH_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::ListLength { key } => {
                LIST_LENGTH.increment();
                match timeout(
                    config.request().timeout(),
                    client.list_fetch(cache_name, &*key),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        LIST_LENGTH_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        LIST_LENGTH_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        LIST_LENGTH_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::ListPopFront { key } => {
                LIST_POP_FRONT.increment();
                match timeout(
                    config.request().timeout(),
                    client.list_pop_front(cache_name, &*key),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        LIST_POP_FRONT_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        LIST_POP_FRONT_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        LIST_POP_FRONT_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::ListPopBack { key } => {
                LIST_POP_BACK.increment();
                match timeout(
                    config.request().timeout(),
                    client.list_pop_back(cache_name, &*key),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        LIST_POP_BACK_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        LIST_POP_BACK_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        LIST_POP_BACK_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }

            /*
             * SORTED SETS
             */
            WorkItem::SortedSetAdd { key, members } => {
                SORTED_SET_ADD.increment();
                let members: Vec<sorted_set::SortedSetElement> = members
                    .iter()
                    .map(|(value, score)| sorted_set::SortedSetElement {
                        value: value.to_vec(),
                        score: *score,
                    })
                    .collect();
                match timeout(
                    config.request().timeout(),
                    client.sorted_set_put(
                        cache_name,
                        &*key,
                        members,
                        CollectionTtl::new(None, false),
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SORTED_SET_ADD_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SORTED_SET_ADD_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SORTED_SET_ADD_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::SortedSetMembers { key } => {
                SORTED_SET_MEMBERS.increment();
                match timeout(
                    config.request().timeout(),
                    client.sorted_set_fetch(
                        cache_name,
                        &*key,
                        momento::sorted_set::Order::Ascending,
                        None,
                        None,
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SORTED_SET_MEMBERS_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SORTED_SET_MEMBERS_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SORTED_SET_MEMBERS_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::SortedSetIncrement {
                key,
                member,
                amount,
            } => {
                SORTED_SET_INCR.increment();
                match timeout(
                    config.request().timeout(),
                    client.sorted_set_increment(
                        cache_name,
                        &*key,
                        &*member,
                        amount,
                        CollectionTtl::new(None, false),
                    ),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SORTED_SET_INCR_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SORTED_SET_INCR_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SORTED_SET_INCR_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::SortedSetRank { key, member } => {
                SORTED_SET_RANK.increment();
                match timeout(
                    config.request().timeout(),
                    client.sorted_set_get_rank(cache_name, &*key, &*member),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SORTED_SET_RANK_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SORTED_SET_RANK_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SORTED_SET_RANK_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::SortedSetRemove { key, members } => {
                SORTED_SET_REMOVE.increment();
                let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                match timeout(
                    config.request().timeout(),
                    client.sorted_set_remove(cache_name, &*key, members),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SORTED_SET_REMOVE_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SORTED_SET_REMOVE_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SORTED_SET_REMOVE_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::SortedSetScore { key, members } => {
                SORTED_SET_SCORE.increment();
                let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                match timeout(
                    config.request().timeout(),
                    client.sorted_set_get_score(cache_name, &*key, members),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        SORTED_SET_SCORE_OK.increment();
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        SORTED_SET_SCORE_EX.increment();
                        Err(e.into())
                    }
                    Err(_) => {
                        SORTED_SET_SCORE_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }

            WorkItem::Reconnect => {
                continue;
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
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
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
