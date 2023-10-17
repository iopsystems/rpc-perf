use super::*;
use crate::net::Connector;
use ::redis::aio::Connection;
use ::redis::{AsyncCommands, RedisConnectionInfo};
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
                    CONNECT_CURR.increment();
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
                /*
                 * PING
                 */
                ClientRequest::Ping(r) => ping(&mut con, &config, r).await,

                /*
                 * KEY-VALUE
                 */
                ClientRequest::Add(r) => add(&mut con, &config, r).await,
                ClientRequest::Delete(r) => delete(&mut con, &config, r).await,
                ClientRequest::Get(r) => get(&mut con, &config, r).await,
                ClientRequest::Replace(r) => replace(&mut con, &config, r).await,
                ClientRequest::Set(r) => set(&mut con, &config, r).await,

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

                /*
                 * SETS
                 */
                ClientRequest::SetAdd(r) => set_add(&mut con, &config, r).await,
                ClientRequest::SetMembers(r) => set_members(&mut con, &config, r).await,
                ClientRequest::SetRemove(r) => set_remove(&mut con, &config, r).await,

                /*
                 * SORTED SETS
                 */
                ClientRequest::SortedSetAdd(r) => sorted_set_add(&mut con, &config, r).await,
                ClientRequest::SortedSetRange(r) => sorted_set_range(&mut con, &config, r).await,
                ClientRequest::SortedSetIncrement(r) => {
                    sorted_set_increment(&mut con, &config, r).await
                }
                ClientRequest::SortedSetRemove(r) => sorted_set_remove(&mut con, &config, r).await,
                ClientRequest::SortedSetScore(r) => sorted_set_score(&mut con, &config, r).await,
                ClientRequest::SortedSetRank(r) => sorted_set_rank(&mut con, &config, r).await,

                /*
                 * UNSUPPORTED
                 */
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

        let latency_ns = stop.duration_since(start).as_nanos() as u64;

        match result {
            Ok(_) => {
                connection = Some(con);
                RESPONSE_OK.increment();

                let _ = RESPONSE_LATENCY.increment(latency_ns);
            }
            Err(ResponseError::Exception) => {
                CONNECT_CURR.decrement();
                RESPONSE_EX.increment();
            }
            Err(ResponseError::Timeout) => {
                CONNECT_CURR.decrement();
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
