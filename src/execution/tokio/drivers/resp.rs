// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use redis::AsyncCommands;
use crate::Instant;
use std::borrow::Borrow;
use super::*;

/// Launch tasks with one conncetion per task as RESP protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, poolsize: usize, work_receiver: Receiver<WorkItem>) {
    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..poolsize {
        runtime.spawn(task(work_receiver.clone()));
    }
}

#[allow(dead_code)]
#[allow(clippy::slow_vector_initialization)]
async fn task(work_receiver: Receiver<WorkItem>) -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1/").map_err(|_| Error::new(ErrorKind::Other, "failed to create redis client"))?;
    let mut connection = client.get_async_connection().await.map(Some).unwrap_or(None);

    while RUNNING.load(Ordering::Relaxed) {
        if connection.is_none() {
            connection = client.get_async_connection().await.map(Some).unwrap_or(None);
            continue;
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
                match timeout(Duration::from_millis(200), con.get::<&[u8], Option<Vec<u8>>>(key.as_ref())).await {
                    Ok(Ok(None)) => {
                        GET_KEY_MISS.increment();
                        Ok(Ok(()))
                    }
                    Ok(Ok(Some(_))) => {
                        GET_KEY_HIT.increment();
                        Ok(Ok(()))
                    }
                    Ok(Err(e)) => {
                        GET_EX.increment();
                        Ok(Err(e))
                    }
                    Err(e) => {
                        Err(e)
                    }
                }
            }
            WorkItem::Set { key, value } => {
                SET.increment();
                match timeout(Duration::from_millis(200), con.set::<&[u8], &[u8], ()>(key.as_ref(), value.as_ref())).await {
                    Ok(Ok(_)) => {
                        SET_STORED.increment();
                        Ok(Ok(()))
                    }
                    Ok(Err(e)) => {
                        SET_EX.increment();
                        Ok(Err(e))
                    }
                    Err(e) => {
                        Err(e)
                    }
                }
            }
            WorkItem::HashDelete { key, fields } => {
                let fields: Vec<&String> = fields.iter().map(|v| v.borrow()).collect();
                timeout(Duration::from_millis(200), con.hdel::<&String, Vec<&String>, Vec<u8>>(key.as_ref(), fields)).await.map(|r| r.map(|_| ()))
            }
            WorkItem::HashExists { key, field } => {
                timeout(Duration::from_millis(200), con.hexists::<&[u8], &[u8], bool>(key.as_ref(), field.as_ref())).await.map(|r| r.map(|_| ()))
            }
            WorkItem::HashGet { key, field } => {
                timeout(Duration::from_millis(200), con.hget::<&String, &String, Vec<u8>>(key.as_ref(), field.as_ref())).await.map(|r| r.map(|_| ()))
            }
            WorkItem::HashMultiGet { key, fields } => {
                let fields: Vec<&String> = fields.iter().map(|v| v.borrow()).collect();
                timeout(Duration::from_millis(200), con.hget::<&String, Vec<&String>, Vec<u8>>(key.as_ref(), fields)).await.map(|r| r.map(|_| ()))
            }
            WorkItem::HashSet { key, field, value } => {
                timeout(Duration::from_millis(200), con.hset::<&String, &String, Vec<u8>, Vec<u8>>(key.as_ref(), field.as_ref(), value.into())).await.map(|r| r.map(|_| ()))
            }
            WorkItem::Ping { .. } => {
                timeout(Duration::from_millis(200), redis::cmd("PING").query_async(&mut con)).await
            }
            _ => {
                continue;
            }
        };

        let stop = Instant::now();

        match result {
            Ok(Ok(_)) => {
                connection = Some(con);

                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Ok(Err(_)) => {
                RESPONSE_EX.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Err(_) => {
                RESPONSE_TIMEOUT.increment();
            }
        }
    }

    Ok(())
}