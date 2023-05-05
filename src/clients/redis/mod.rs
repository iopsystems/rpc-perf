use super::*;
use crate::net::Connector;
use crate::Instant;
use ::redis::{AsyncCommands, RedisConnectionInfo};
use ::redis::aio::Connection;
use std::borrow::Borrow;

mod commands;

use commands::*;

/// Launch tasks with one conncetion per task as RESP protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching resp protocol tasks");

    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..config.client().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            runtime.spawn(task(
                work_receiver.clone(),
                endpoint.clone(),
                config.clone(),
            ));
        }
    }
}

#[allow(dead_code)]
#[allow(clippy::slow_vector_initialization)]
async fn task(work_receiver: Receiver<WorkItem>, endpoint: String, config: Config) -> Result<()> {
    trace!("launching resp task for endpoint: {endpoint}");
    let connector = Connector::new(&config)?;

    let redis_connection_info = RedisConnectionInfo {
        db: 0,
        username: None,
        password: None,
    };

    let mut connection = None;

    while RUNNING.load(Ordering::Relaxed) {
        if connection.is_none() {
            CONNECT.increment();
            connection = match timeout(
                config.client().unwrap().connect_timeout(),
                connector.connect(&endpoint),
            )
            .await
            {
                Ok(Ok(c)) => {
                    CONNECT_OK.increment();
                    CONNECT_CURR.add(1);
                    if let Ok(c) = ::redis::aio::Connection::new(&redis_connection_info, c).await {
                        Some(c)
                    } else {
                        CONNECT_EX.increment();
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                Ok(Err(e)) => {
                    trace!("error connecting: {e}");
                    CONNECT_EX.increment();
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(_) => {
                    trace!("connect timeout");
                    CONNECT_TIMEOUT.increment();
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }

        let mut con = connection.take().unwrap();
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        let result = match work_item {
            WorkItem::Request { request, .. } => match request {
                ClientRequest::Get(r) => get(&mut con, &config, r).await,
                ClientRequest::Set(r) => set(&mut con, &config, r).await,
                ClientRequest::Delete(r) => delete(&mut con, &config, r).await,

                /*
                 * HASHES (DICTIONARIES)
                 */
                ClientRequest::HashDelete(r) => hash_delete(&mut con, &config, r).await,
                ClientRequest::HashExists(r) => hash_exists(&mut con, &config, r).await,
                ClientRequest::HashIncrement(r) => hash_increment(&mut con, &config, r).await,
                // transparently issues either a `hget` or `hmget`
                ClientRequest::HashGet(r) => hash_get(&mut con, &config, r).await,
                ClientRequest::HashGetAll(r) => hash_get_all(&mut con, &config, r).await,
                ClientRequest::HashSet(r) => hash_set(&mut con, &config, r).await,

                /*
                 * LISTS
                 */
                // To truncate, we must fuse an LTRIM at the end of the LPUSH
                ClientRequest::ListPushFront(r) => list_push_front(&mut con, &config, r).await,
                // To truncate, we must fuse an RTRIM at the end of the RPUSH
                ClientRequest::ListPushBack(r) => list_push_back(&mut con, &config, r).await,
                ClientRequest::ListFetch(r) => list_fetch(&mut con, &config, r).await,
                ClientRequest::ListLength(r) => list_length(&mut con, &config, r).await,
                ClientRequest::ListPopFront(r) => list_pop_front(&mut con, &config, r).await,
                ClientRequest::ListPopBack(r) => list_pop_back(&mut con, &config, r).await,

                ClientRequest::Ping { .. } => {
                    PING.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        ::redis::cmd("PING").query_async(&mut con),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            PING_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            PING_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => Err(ResponseError::Timeout),
                    }
                }

                /*
                 * SETS
                 */
                ClientRequest::SetAdd(r) => set_add(&mut con, &config, r).await,
                ClientRequest::SetMembers(r) => set_members(&mut con, &config, r).await,
                ClientRequest::SetRemove(r) => set_remove(&mut con, &config, r).await,

                /*
                 * SORTED SETS
                 */
                ClientRequest::SortedSetAdd { key, members } => {
                    SORTED_SET_ADD.increment();
                    if members.is_empty() {
                        connection = Some(con);
                        continue;
                    } else if members.len() == 1 {
                        let (member, score) = members.first().unwrap();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.zadd::<&[u8], f64, &[u8], f64>(
                                key.as_ref(),
                                member.as_ref(),
                                *score,
                            ),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                SORTED_SET_ADD_OK.increment();
                                Ok(())
                            }
                            Ok(Err(_)) => {
                                SORTED_SET_ADD_EX.increment();
                                Err(ResponseError::Exception)
                            }
                            Err(_) => {
                                SORTED_SET_ADD_TIMEOUT.increment();
                                Err(ResponseError::Timeout)
                            }
                        }
                    } else {
                        let d: Vec<(f64, &[u8])> =
                            members.iter().map(|(m, s)| (*s, m.as_ref())).collect();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.zadd_multiple::<&[u8], f64, &[u8], f64>(&key, &d),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                SORTED_SET_ADD_OK.increment();
                                Ok(())
                            }
                            Ok(Err(_)) => {
                                SORTED_SET_ADD_EX.increment();
                                Err(ResponseError::Exception)
                            }
                            Err(_) => {
                                SORTED_SET_ADD_TIMEOUT.increment();
                                Err(ResponseError::Timeout)
                            }
                        }
                    }
                }
                ClientRequest::SortedSetMembers { key } => {
                    SORTED_SET_INCR.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        ::redis::cmd("ZUNION")
                            .arg(1)
                            .arg(&*key)
                            .arg("WITHSCORES")
                            .query_async::<_, Vec<(Vec<u8>, f64)>>(&mut con),
                    )
                    .await
                    {
                        Ok(Ok(set)) => {
                            if set.is_empty() {
                                RESPONSE_MISS.increment();
                            } else {
                                RESPONSE_HIT.increment();
                            }

                            SORTED_SET_INCR_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SORTED_SET_INCR_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SORTED_SET_INCR_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::SortedSetIncrement {
                    key,
                    member,
                    amount,
                } => {
                    SORTED_SET_INCR.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.zincr::<&[u8], &[u8], f64, String>(&key, &member, amount),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            SORTED_SET_INCR_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SORTED_SET_INCR_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SORTED_SET_INCR_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::SortedSetRemove { key, members } => {
                    SORTED_SET_REMOVE.increment();
                    let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.zrem::<&[u8], Vec<&[u8]>, usize>(&key, members),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            SORTED_SET_REMOVE_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SORTED_SET_REMOVE_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SORTED_SET_REMOVE_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::SortedSetScore { key, members } => {
                    SORTED_SET_SCORE.increment();

                    let result = if members.len() == 1 {
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.zscore::<&[u8], &[u8], Option<f64>>(
                                key.as_ref(),
                                members[0].as_ref(),
                            ),
                        )
                        .await
                        {
                            Ok(Ok(score)) => {
                                if score.is_some() {
                                    RESPONSE_HIT.increment();
                                } else {
                                    RESPONSE_MISS.increment();
                                }
                                Ok(())
                            }
                            Ok(Err(_)) => Err(ResponseError::Exception),
                            Err(_) => Err(ResponseError::Timeout),
                        }
                    } else {
                        let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.zscore_multiple::<&[u8], &[u8], Vec<Option<f64>>>(
                                key.as_ref(),
                                &members,
                            ),
                        )
                        .await
                        {
                            Ok(Ok(scores)) => {
                                for score in scores {
                                    if score.is_some() {
                                        RESPONSE_HIT.increment();
                                    } else {
                                        RESPONSE_MISS.increment();
                                    }
                                }
                                Ok(())
                            }
                            Ok(Err(_)) => Err(ResponseError::Exception),
                            Err(_) => Err(ResponseError::Timeout),
                        }
                    };

                    match result {
                        Ok(_) => {
                            SORTED_SET_SCORE_OK.increment();
                        }
                        Err(ResponseError::Exception) => {
                            SORTED_SET_SCORE_EX.increment();
                        }
                        Err(ResponseError::Timeout) => {
                            SORTED_SET_SCORE_TIMEOUT.increment();
                        }
                        _ => {}
                    }

                    result
                }
                ClientRequest::SortedSetRank { key, member } => {
                    SORTED_SET_RANK.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.zrank::<&[u8], &[u8], Option<u64>>(key.as_ref(), member.as_ref()),
                    )
                    .await
                    {
                        Ok(Ok(rank)) => {
                            if rank.is_some() {
                                RESPONSE_HIT.increment();
                            } else {
                                RESPONSE_MISS.increment();
                            }

                            SORTED_SET_RANK_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SORTED_SET_RANK_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SORTED_SET_RANK_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                    connection = Some(con);
                    continue;
                }
            },
            WorkItem::Reconnect => {
                CONNECT_CURR.sub(1);
                continue;
            }
        };

        REQUEST_OK.increment();

        let stop = Instant::now();

        let latency_ns = stop.duration_since(start).as_nanos();

        match result {
            Ok(_) => {
                connection = Some(con);
                RESPONSE_OK.increment();

                REQUEST_LATENCY.increment(start, latency_ns, 1);
                RESPONSE_LATENCY.increment(stop, latency_ns, 1);
            }
            Err(ResponseError::Exception) => {
                CONNECT_CURR.sub(1);
                RESPONSE_EX.increment();
            }
            Err(ResponseError::Timeout) => {
                CONNECT_CURR.sub(1);
                RESPONSE_TIMEOUT.increment();
            }
            Err(ResponseError::Ratelimited) => {
                RESPONSE_RATELIMITED.increment();
                connection = Some(con);
            }
            Err(ResponseError::BackendTimeout) => {
                RESPONSE_BACKEND_TIMEOUT.increment();
                connection = Some(con);
            }
        }
    }

    Ok(())
}
