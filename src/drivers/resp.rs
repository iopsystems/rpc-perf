// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::Instant;
use redis::AsyncCommands;
use std::borrow::Borrow;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;

/// Launch tasks with one conncetion per task as RESP protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching resp protocol tasks");

    let endpoints: Vec<SocketAddr> = config
        .target()
        .endpoints()
        .iter()
        .map(|e| {
            e.to_socket_addrs()
                .expect("bad endpoint")
                .next()
                .expect("lookup failed")
        })
        .collect();

    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..(config.connection().poolsize() * config.general().threads()) {
        for endpoint in &endpoints {
            runtime.spawn(task(work_receiver.clone(), *endpoint, config.clone()));
        }
    }
}

#[allow(dead_code)]
#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<WorkItem>,
    endpoint: SocketAddr,
    config: Config,
) -> Result<()> {
    trace!("launching resp task for endpoint: {endpoint}");

    let client = redis::Client::open(format!("redis://{}", endpoint)).map_err(|e| {
        warn!("failed to create redis client: {e}");
        Error::new(ErrorKind::Other, "failed to create redis client")
    })?;
    let mut connection = None;

    while RUNNING.load(Ordering::Relaxed) {
        if connection.is_none() {
            CONNECT.increment();
            connection =
                match timeout(config.connection().timeout(), client.get_async_connection()).await {
                    Ok(Ok(c)) => {
                        CONNECT_OK.increment();
                        CONNECT_CURR.add(1);
                        Some(c)
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
            WorkItem::Get { key } => {
                GET.increment();
                match timeout(
                    config.request().timeout(),
                    con.get::<&[u8], Option<Vec<u8>>>(&key),
                )
                .await
                {
                    Ok(Ok(None)) => {
                        GET_KEY_MISS.increment();
                        Ok(())
                    }
                    Ok(Ok(Some(_))) => {
                        GET_KEY_HIT.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        GET_EX.increment();
                        Err(ResponseError::Exception)
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
                    con.set::<&[u8], &[u8], ()>(&key, &value),
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
                    Err(_) => {
                        SET_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::Delete { key } => {
                DELETE.increment();
                match timeout(config.request().timeout(), con.del::<&[u8], ()>(&key)).await {
                    Ok(Ok(_)) => {
                        DELETE_OK.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        DELETE_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => {
                        DELETE_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::HashDelete { key, fields } => {
                HASH_DELETE.increment();
                let fields: Vec<&[u8]> = fields.iter().map(|v| v.borrow()).collect();
                match timeout(
                    config.request().timeout(),
                    con.hdel::<&[u8], Vec<&[u8]>, Vec<u8>>(&key, fields),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        HASH_DELETE_OK.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        HASH_DELETE_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => {
                        HASH_DELETE_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::HashExists { key, field } => {
                match timeout(
                    config.request().timeout(),
                    con.hexists::<&[u8], &[u8], bool>(&key, &field),
                )
                .await
                {
                    Ok(Ok(true)) => {
                        HASH_EXISTS_HIT.increment();
                        Ok(())
                    }
                    Ok(Ok(false)) => {
                        HASH_EXISTS_MISS.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => Err(ResponseError::Exception),
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashIncrement { key, field, amount } => {
                match timeout(
                    config.request().timeout(),
                    con.hincr::<&[u8], &[u8], i64, i64>(&key, &field, amount),
                )
                .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(_)) => Err(ResponseError::Exception),
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            // transparently issues either a `hget` or `hmget`
            WorkItem::HashGet { key, fields } => {
                HASH_GET.increment();

                let result = if fields.len() == 1 {
                    match timeout(
                        config.request().timeout(),
                        con.hget::<&[u8], &[u8], Option<Vec<u8>>>(&key, &fields[0]),
                    )
                    .await
                    {
                        Ok(Ok(Some(_))) => {
                            HASH_GET_FIELD_HIT.increment();
                            Ok(())
                        }
                        Ok(Ok(None)) => {
                            HASH_GET_FIELD_MISS.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                } else {
                    let fields: Vec<&[u8]> = fields.iter().map(|f| &**f).collect();
                    match timeout(
                        config.request().timeout(),
                        con.hget::<&[u8], &[&[u8]], Option<Vec<Option<Vec<u8>>>>>(&key, &fields),
                    )
                    .await
                    {
                        Ok(Ok(Some(values))) => {
                            let mut hits = 0;
                            let mut misses = 0;
                            for value in values {
                                if value.is_some() {
                                    hits += 1;
                                } else {
                                    misses += 1;
                                }
                            }
                            HASH_GET_FIELD_HIT.add(hits);
                            HASH_GET_FIELD_MISS.add(misses);
                            Ok(())
                        }
                        Ok(Ok(None)) => {
                            HASH_GET_FIELD_MISS.add(fields.len() as _);
                            Ok(())
                        }
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                };

                match result {
                    Ok(()) => {
                        HASH_GET_OK.increment();
                    }
                    Err(ResponseError::Exception) => {
                        HASH_GET_EX.increment();
                    }
                    Err(ResponseError::Timeout) => {
                        HASH_GET_TIMEOUT.increment();
                    }
                    _ => {}
                }

                result
            }
            WorkItem::HashGetAll { key } => {
                HASH_GET_ALL.increment();
                match timeout(
                    config.request().timeout(),
                    con.hgetall::<&[u8], Option<HashMap<Vec<u8>, Vec<u8>>>>(key.as_ref()),
                )
                .await
                {
                    Ok(Ok(Some(_))) => {
                        HASH_GET_ALL_OK.increment();
                        HASH_GET_ALL_HIT.increment();
                        Ok(())
                    }
                    Ok(Ok(None)) => {
                        HASH_GET_ALL_OK.increment();
                        HASH_GET_ALL_MISS.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        HASH_GET_ALL_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => {
                        HASH_GET_ALL_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::HashSet { key, data } => {
                if data.is_empty() {
                    panic!("empty data for hash set");
                }

                HASH_SET.increment();
                let result = if data.len() == 1 {
                    let (field, value) = data.iter().next().unwrap();
                    match timeout(
                        config.request().timeout(),
                        con.hset::<&[u8], &[u8], &[u8], Vec<u8>>(
                            key.as_ref(),
                            field.as_ref(),
                            value.as_ref(),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                } else {
                    let d: Vec<(&[u8], &[u8])> =
                        data.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect();
                    match timeout(
                        config.request().timeout(),
                        con.hset_multiple::<&[u8], &[u8], &[u8], Vec<u8>>(&key, &d),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                };

                match result {
                    Ok(_) => {
                        HASH_SET_OK.increment();
                    }
                    Err(ResponseError::Timeout) => {
                        HASH_SET_TIMEOUT.increment();
                    }
                    Err(_) => {
                        HASH_SET_EX.increment();
                    }
                }

                result
            }
            WorkItem::ListFetch { key } => {
                LIST_FETCH.increment();
                match timeout(
                    config.request().timeout(),
                    con.lrange::<&[u8], Vec<Vec<u8>>>(key.as_ref(), 0, -1),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        LIST_FETCH_OK.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        LIST_FETCH_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => {
                        LIST_FETCH_TIMEOUT.increment();
                        Err(ResponseError::Timeout)
                    }
                }
            }
            WorkItem::Ping { .. } => {
                PING.increment();
                match timeout(
                    config.request().timeout(),
                    redis::cmd("PING").query_async(&mut con),
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
            WorkItem::SortedSetAdd { key, members } => {
                SORTED_SET_ADD.increment();
                if members.is_empty() {
                    connection = Some(con);
                    continue;
                } else if members.len() == 1 {
                    let (member, score) = members.first().unwrap();
                    match timeout(
                        config.request().timeout(),
                        con.zadd::<&[u8], f64, &[u8], f64>(key.as_ref(), member.as_ref(), *score),
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
                        config.request().timeout(),
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
            WorkItem::SortedSetIncrement {
                key,
                member,
                amount,
            } => {
                SORTED_SET_INCR.increment();
                match timeout(
                    config.request().timeout(),
                    con.zincr::<&[u8], &[u8], f64, f64>(&key, &member, amount),
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
            WorkItem::SortedSetRemove { key, members } => {
                SORTED_SET_REMOVE.increment();
                let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                match timeout(
                    config.request().timeout(),
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
            WorkItem::SortedSetScore { key, members } => {
                SORTED_SET_SCORE.increment();

                let result = if members.len() == 1 {
                    match timeout(
                        config.request().timeout(),
                        con.zscore::<&[u8], &[u8], Option<f64>>(key.as_ref(), members[0].as_ref()),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                } else {
                    let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                    match timeout(
                        config.request().timeout(),
                        con.zscore_multiple::<&[u8], &[u8], Vec<Option<f64>>>(
                            key.as_ref(),
                            &members,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
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
            WorkItem::SortedSetRank { key, member } => {
                SORTED_SET_RANK.increment();
                match timeout(
                    config.request().timeout(),
                    con.zscore::<&[u8], &[u8], Option<u64>>(key.as_ref(), member.as_ref()),
                )
                .await
                {
                    Ok(Ok(_)) => {
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
            WorkItem::Reconnect => {
                CONNECT_CURR.sub(1);
                continue;
            }
            _ => {
                REQUEST_UNSUPPORTED.increment();
                connection = Some(con);
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
                RESPONSE_LATENCY.increment(stop, latency_ns, 1);
            }
            Err(ResponseError::Exception) => {
                CONNECT_CURR.sub(1);
                RESPONSE_EX.increment();
                RESPONSE_LATENCY.increment(stop, latency_ns, 1);
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
