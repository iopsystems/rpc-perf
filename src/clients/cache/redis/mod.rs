use crate::clients::cache::*;
use crate::clients::common::Queue;
use crate::clients::*;

use ::redis::aio::MultiplexedConnection;
use ::redis::AsyncCommands;

use std::borrow::Borrow;
use std::net::SocketAddr;

mod commands;

use commands::*;

/// Launch tasks with one conncetion per task as RESP protocol is not mux-enabled.
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
) {
    debug!("launching resp protocol tasks");

    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..config.client().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            // for each endpoint there are poolsize # of pool managers, each
            // managed a single connection

            let queue = Queue::new(1);
            runtime.spawn(pool_manager(
                endpoint.clone(),
                config.clone(),
                queue.clone(),
            ));

            // one task for each concurrent session on the connection

            for _ in 0..config.client().unwrap().concurrency() {
                runtime.spawn(task(
                    work_receiver.clone(),
                    endpoint.clone(),
                    config.clone(),
                    queue.clone(),
                ));
            }
        }
    }
}

pub async fn pool_manager(endpoint: String, _config: Config, queue: Queue<MultiplexedConnection>) {
    let mut client = None;

    let endpoint = if endpoint.parse::<SocketAddr>().is_ok() {
        format!("redis://{endpoint}")
    } else {
        endpoint
    };

    while RUNNING.load(Ordering::Relaxed) {
        if client.is_none() {
            CONNECT.increment();

            if let Ok(c) = ::redis::Client::open(endpoint.clone()) {
                CONNECT_OK.increment();
                CONNECT_CURR.increment();

                client = Some(c);
            } else {
                CONNECT_EX.increment();

                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            continue;
        }

        if let Ok(connection) = client
            .as_ref()
            .unwrap()
            .get_multiplexed_async_connection()
            .await
        {
            let _ = queue.send(connection).await;
        } else {
            client = None;
        }
    }
}

#[allow(dead_code)]
#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
    endpoint: String,
    config: Config,
    queue: Queue<MultiplexedConnection>,
) -> Result<()> {
    trace!("launching resp task for endpoint: {endpoint}");

    let mut connection = None;

    while RUNNING.load(Ordering::Relaxed) {
        if connection.is_none() {
            if let Ok(c) = queue.recv().await {
                connection = Some(c);
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }

            continue;
        }

        let mut con = connection.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        let result = match work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
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
            ClientWorkItemKind::Reconnect => {
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
                RESPONSE_EX.increment();
            }
            Err(ResponseError::Timeout) => {
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
