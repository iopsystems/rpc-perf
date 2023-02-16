// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::Instant;
use redis::AsyncCommands;
use std::borrow::Borrow;
use std::net::SocketAddr;

/// Launch tasks with one conncetion per task as RESP protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..config.connection().poolsize() {
        for endpoint in config.endpoints() {
            runtime.spawn(task(work_receiver.clone(), endpoint, config.clone()));
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
    let client = redis::Client::open(format!("redis://{}", endpoint))
        .map_err(|_| Error::new(ErrorKind::Other, "failed to create redis client"))?;
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
                    Ok(Err(_)) => {
                        CONNECT_EX.increment();
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    Err(_) => {
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

        let start = Instant::now();
        let result = match work_item {
            WorkItem::Get { key } => {
                GET.increment();
                match timeout(
                    Duration::from_millis(200),
                    con.get::<&[u8], Option<Vec<u8>>>(key.as_ref()),
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
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::Set { key, value } => {
                SET.increment();
                match timeout(
                    Duration::from_millis(200),
                    con.set::<&[u8], &[u8], ()>(key.as_ref(), value.as_ref()),
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
                let fields: Vec<&[u8]> = fields.iter().map(|v| v.borrow()).collect();
                match timeout(
                    Duration::from_millis(200),
                    con.hdel::<&[u8], Vec<&[u8]>, Vec<u8>>(key.as_ref(), fields),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        // DELETE_DELETED.increment();
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        // DELETE_EX.increment();
                        Err(ResponseError::Exception)
                    }
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashExists { key, field } => {
                match timeout(
                    Duration::from_millis(200),
                    con.hexists::<&[u8], &[u8], bool>(key.as_ref(), field.as_ref()),
                )
                .await
                {
                    Ok(Ok(true)) => {
                        // HASH_EXISTS_HIT.increment();
                        Ok(())
                    }
                    Ok(Ok(false)) => Ok(()),
                    Ok(Err(_)) => Err(ResponseError::Exception),
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashGet { key, field } => {
                match timeout(
                    Duration::from_millis(200),
                    con.hget::<&[u8], &[u8], Vec<u8>>(key.as_ref(), field.as_ref()),
                )
                .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(_)) => Err(ResponseError::Exception),
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashIncrement { key, field, amount } => {
                match timeout(
                    Duration::from_millis(200),
                    con.hincr::<&[u8], &[u8], i64, i64>(key.as_ref(), field.as_ref(), amount),
                )
                .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(_)) => Err(ResponseError::Exception),
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashMultiGet { key, fields } => {
                let fields: Vec<&[u8]> = fields.iter().map(|v| v.borrow()).collect();
                match timeout(
                    Duration::from_millis(200),
                    con.hget::<&[u8], Vec<&[u8]>, Vec<u8>>(key.as_ref(), fields),
                )
                .await
                {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(_)) => Err(ResponseError::Exception),
                    Err(_) => Err(ResponseError::Timeout),
                }
            }
            WorkItem::HashSet { key, data } => {
                if data.is_empty() {
                    connection = Some(con);
                    continue;
                } else if data.len() == 1 {
                    let (field, value) = data.iter().next().unwrap();
                    match timeout(
                        Duration::from_millis(200),
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
                        Duration::from_millis(200),
                        con.hset_multiple::<&[u8], &[u8], &[u8], Vec<u8>>(key.as_ref(), &d),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                }
            }
            WorkItem::Ping { .. } => {
                PING.increment();
                match timeout(
                    Duration::from_millis(200),
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
            _ => {
                connection = Some(con);
                continue;
            }
        };

        let stop = Instant::now();

        match result {
            Ok(_) => {
                connection = Some(con);
                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Err(ResponseError::Exception) => {
                CONNECT_CURR.sub(1);
                RESPONSE_EX.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Err(ResponseError::Timeout) => {
                CONNECT_CURR.sub(1);
                RESPONSE_TIMEOUT.increment();
            }
        }
    }

    Ok(())
}
