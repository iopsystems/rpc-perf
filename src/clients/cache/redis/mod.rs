use crate::clients::cache::*;
use crate::clients::common::Queue;
use crate::clients::*;

use ::redis::aio::MultiplexedConnection;
use ::redis::AsyncCommands;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use std::borrow::Borrow;
use std::net::SocketAddr;

mod commands;

use commands::*;

/// Launch tasks with poolsize connections per endpoint. Each connection
/// pipelines up to `concurrency` requests using MultiplexedConnection.
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
) {
    debug!("launching resp protocol tasks");

    for _ in 0..config.client().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            let queue = Queue::new(1);
            runtime.spawn(pool_manager(
                endpoint.clone(),
                config.clone(),
                queue.clone(),
            ));

            // One task per connection, using concurrency for pipeline depth
            runtime.spawn(task(
                work_receiver.clone(),
                endpoint.clone(),
                config.clone(),
                queue.clone(),
            ));
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

/// Result of executing a single request, used for FuturesUnordered
enum RequestResult {
    Ok {
        latency_ns: u64,
        latency_histograms: Vec<&'static metriken::AtomicHistogram>,
    },
    Exception,
    Timeout,
    Ratelimited {
        latency_ns: u64,
        latency_histograms: Vec<&'static metriken::AtomicHistogram>,
    },
    BackendTimeout {
        latency_ns: u64,
        latency_histograms: Vec<&'static metriken::AtomicHistogram>,
    },
}

#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
    _endpoint: String,
    config: Config,
    queue: Queue<MultiplexedConnection>,
) {
    let concurrency_limit = config.client().unwrap().concurrency();

    while RUNNING.load(Ordering::Relaxed) {
        // Get a connection from the pool manager
        let connection = match queue.recv().await {
            Ok(c) => c,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
        };

        // Run pipelined requests until connection fails
        run_pipelined(&work_receiver, connection, &config, concurrency_limit).await;
    }
}

/// Run pipelined requests on a single connection using FuturesUnordered.
/// Returns when the connection fails or the work channel closes.
async fn run_pipelined(
    work_receiver: &Receiver<ClientWorkItemKind<ClientRequest>>,
    connection: MultiplexedConnection,
    config: &Config,
    concurrency_limit: usize,
) {
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

    while RUNNING.load(Ordering::Relaxed) {
        let in_flight_count = in_flight.len();

        tokio::select! {
            biased;

            // Drive in-flight requests to completion
            result = async {
                if in_flight.is_empty() {
                    futures::future::pending().await
                } else {
                    in_flight.next().await
                }
            } => {
                match result {
                    Some(RequestResult::Ok { latency_ns, latency_histograms }) => {
                        RESPONSE_OK.increment();
                        for hist in latency_histograms {
                            let _ = hist.increment(latency_ns);
                        }
                    }
                    Some(RequestResult::Exception) => {
                        RESPONSE_EX.increment();
                    }
                    Some(RequestResult::Timeout) => {
                        RESPONSE_TIMEOUT.increment();
                    }
                    Some(RequestResult::Ratelimited { latency_ns, latency_histograms }) => {
                        RESPONSE_RATELIMITED.increment();
                        for hist in latency_histograms {
                            let _ = hist.increment(latency_ns);
                        }
                    }
                    Some(RequestResult::BackendTimeout { latency_ns, latency_histograms }) => {
                        RESPONSE_BACKEND_TIMEOUT.increment();
                        for hist in latency_histograms {
                            let _ = hist.increment(latency_ns);
                        }
                    }
                    None => return,
                }
            }

            // Accept new work if under concurrency limit
            work_item = async {
                if in_flight_count < concurrency_limit {
                    work_receiver.recv().await
                } else {
                    futures::future::pending().await
                }
            } => {
                match work_item {
                    Ok(item) => {
                        in_flight.push(execute_request(item, connection.clone(), config.clone()));
                    }
                    Err(_) => {
                        // Channel closed, drain in-flight and exit
                        while in_flight.next().await.is_some() {}
                        return;
                    }
                }
            }
        }
    }

    // Drain remaining in-flight on shutdown
    while in_flight.next().await.is_some() {}
}

/// Execute a single request on a cloned connection.
#[allow(clippy::slow_vector_initialization)]
async fn execute_request(
    work_item: ClientWorkItemKind<ClientRequest>,
    mut con: MultiplexedConnection,
    config: Config,
) -> RequestResult {
    REQUEST.increment();
    let start = Instant::now();
    let mut latency_histograms: Vec<&'static metriken::AtomicHistogram> = vec![&RESPONSE_LATENCY];

    let result = match work_item {
        ClientWorkItemKind::Request { request, .. } => match request {
            ClientRequest::Ping(r) => ping(&mut con, &config, r).await,
            ClientRequest::Add(r) => add(&mut con, &config, r).await,
            ClientRequest::Delete(r) => delete(&mut con, &config, r).await,
            ClientRequest::Get(r) => {
                latency_histograms.push(&KVGET_RESPONSE_LATENCY);
                get(&mut con, &config, r).await
            }
            ClientRequest::Replace(r) => replace(&mut con, &config, r).await,
            ClientRequest::Set(r) => {
                latency_histograms.push(&KVSET_RESPONSE_LATENCY);
                set(&mut con, &config, r).await
            }
            ClientRequest::HashDelete(r) => hash_delete(&mut con, &config, r).await,
            ClientRequest::HashExists(r) => hash_exists(&mut con, &config, r).await,
            ClientRequest::HashIncrement(r) => hash_increment(&mut con, &config, r).await,
            ClientRequest::HashGet(r) => hash_get(&mut con, &config, r).await,
            ClientRequest::HashGetAll(r) => hash_get_all(&mut con, &config, r).await,
            ClientRequest::HashSet(r) => hash_set(&mut con, &config, r).await,
            ClientRequest::ListPushFront(r) => list_push_front(&mut con, &config, r).await,
            ClientRequest::ListPushBack(r) => list_push_back(&mut con, &config, r).await,
            ClientRequest::ListFetch(r) => list_fetch(&mut con, &config, r).await,
            ClientRequest::ListLength(r) => list_length(&mut con, &config, r).await,
            ClientRequest::ListPopFront(r) => list_pop_front(&mut con, &config, r).await,
            ClientRequest::ListPopBack(r) => list_pop_back(&mut con, &config, r).await,
            ClientRequest::SetAdd(r) => set_add(&mut con, &config, r).await,
            ClientRequest::SetMembers(r) => set_members(&mut con, &config, r).await,
            ClientRequest::SetRemove(r) => set_remove(&mut con, &config, r).await,
            ClientRequest::SortedSetAdd(r) => sorted_set_add(&mut con, &config, r).await,
            ClientRequest::SortedSetRange(r) => sorted_set_range(&mut con, &config, r).await,
            ClientRequest::SortedSetIncrement(r) => sorted_set_increment(&mut con, &config, r).await,
            ClientRequest::SortedSetRemove(r) => sorted_set_remove(&mut con, &config, r).await,
            ClientRequest::SortedSetScore(r) => sorted_set_score(&mut con, &config, r).await,
            ClientRequest::SortedSetRank(r) => sorted_set_rank(&mut con, &config, r).await,
            _ => {
                REQUEST_UNSUPPORTED.increment();
                return RequestResult::Ok { latency_ns: 0, latency_histograms: vec![] };
            }
        },
        ClientWorkItemKind::Reconnect => {
            return RequestResult::Ok { latency_ns: 0, latency_histograms: vec![] };
        }
    };

    REQUEST_OK.increment();
    let latency_ns = start.elapsed().as_nanos() as u64;

    match result {
        Ok(_) => RequestResult::Ok { latency_ns, latency_histograms },
        Err(ResponseError::Exception) => RequestResult::Exception,
        Err(ResponseError::Timeout) => RequestResult::Timeout,
        Err(ResponseError::Ratelimited) => RequestResult::Ratelimited { latency_ns, latency_histograms },
        Err(ResponseError::BackendTimeout) => RequestResult::BackendTimeout { latency_ns, latency_histograms },
    }
}
