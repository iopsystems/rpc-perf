use crate::clients::common::*;
use crate::clients::h2_pool::{h2_pool_manager, MomentoHttpRequestBuilder};
use crate::workload::{ClientWorkItemKind, StoreClientRequest};
use crate::*;

use async_channel::Receiver;
use bytes::Bytes;
use h2::client::SendRequest;
use http::Method;
use tokio::runtime::Runtime;

use std::time::Instant;

/// Launch pool managers and worker tasks for Momento ObjectStore.
///
/// Uses the `[storage]` config section for poolsize and concurrency.
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<StoreClientRequest>>,
) {
    debug!("launching momento objectstore protocol tasks");

    let storage = config.storage().unwrap();

    for _ in 0..storage.poolsize() {
        for endpoint in config.target().endpoints() {
            let queue = Queue::new(1);
            runtime.spawn(h2_pool_manager(endpoint.clone(), queue.clone()));

            for _ in 0..storage.concurrency() {
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

#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<ClientWorkItemKind<StoreClientRequest>>,
    endpoint: String,
    config: Config,
    queue: Queue<SendRequest<Bytes>>,
) -> Result<(), std::io::Error> {
    let token = std::env::var("MOMENTO_API_KEY").unwrap_or_else(|_| {
        eprintln!("environment variable `MOMENTO_API_KEY` is not set");
        std::process::exit(1);
    });

    let store = config.target().object_store_name().unwrap_or_else(|| {
        eprintln!("object_store_name is not specified in the `target` section");
        std::process::exit(1);
    });

    let uri = endpoint.parse::<http::Uri>().unwrap_or_else(|e| {
        eprintln!("target endpoint could not be parsed as a uri: {endpoint}\n{e}");
        std::process::exit(1);
    });

    let endpoint = uri
        .authority()
        .unwrap_or_else(|| {
            eprintln!("endpoint uri is missing an authority: {endpoint}");
            std::process::exit(1);
        })
        .as_str()
        .to_owned();

    while RUNNING.load(Ordering::Relaxed) {
        let sender = queue.recv().await;

        if sender.is_err() {
            continue;
        }

        let mut sender = sender.unwrap();

        let work_item = match work_receiver.recv().await {
            Ok(w) => w,
            Err(_) => {
                return Err(std::io::Error::other("channel closed"));
            }
        };

        STORE_REQUEST.increment();

        match &work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
                StoreClientRequest::Get(r) => {
                    let key = &*r.key;
                    let uri = format!("/objectstore/{store}/{key}");
                    let request =
                        MomentoHttpRequestBuilder::new(&endpoint, Method::GET, &uri).build(&token);

                    let start = Instant::now();

                    match sender.send_request(request, true) {
                        Ok((response, _)) => {
                            let response = response.await;

                            let mut response = match response {
                                Ok(r) => r,
                                Err(_e) => {
                                    STORE_GET_EX.increment();
                                    STORE_RESPONSE_EX.increment();
                                    continue;
                                }
                            };

                            let ttfb = start.elapsed();
                            let status = response.status().as_u16();

                            // drain the response body
                            let body = response.body_mut();

                            if !body.is_end_stream() {
                                let mut flow_control = body.flow_control().clone();

                                let used = flow_control.used_capacity();
                                if flow_control.release_capacity(used).is_err() {
                                    STORE_GET_EX.increment();
                                    STORE_RESPONSE_EX.increment();
                                    continue;
                                }

                                while let Some(chunk) = body.data().await {
                                    if chunk.is_err() {
                                        STORE_GET_EX.increment();
                                        STORE_RESPONSE_EX.increment();
                                        continue;
                                    }

                                    let _ = flow_control.release_capacity(chunk.unwrap().len());
                                }
                            }

                            let latency = start.elapsed();

                            STORE_REQUEST_OK.increment();

                            match status {
                                200 => {
                                    STORE_GET_OK.increment();
                                    STORE_GET_KEY_FOUND.increment();

                                    STORE_RESPONSE_OK.increment();
                                    STORE_RESPONSE_FOUND.increment();

                                    let _ =
                                        STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = STORE_RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                404 => {
                                    STORE_GET_OK.increment();
                                    STORE_GET_KEY_NOT_FOUND.increment();

                                    STORE_RESPONSE_OK.increment();
                                    STORE_RESPONSE_NOT_FOUND.increment();

                                    let _ =
                                        STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = STORE_RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                429 => {
                                    STORE_GET_EX.increment();
                                    STORE_RESPONSE_RATELIMITED.increment();
                                }
                                _ => {
                                    STORE_GET_EX.increment();
                                    STORE_RESPONSE_EX.increment();
                                }
                            }
                        }
                        Err(_) => {
                            continue;
                        }
                    }
                }
                StoreClientRequest::Put(r) => {
                    let key = &*r.key;
                    let ttl_seconds = r.ttl.map(|d| d.as_secs()).unwrap_or(3600);
                    let uri = format!("/objectstore/{store}/{key}?ttl_seconds={ttl_seconds}");
                    let request =
                        MomentoHttpRequestBuilder::new(&endpoint, Method::PUT, &uri).build(&token);

                    let start = Instant::now();

                    if let Ok((response, mut stream)) = sender.send_request(request, false) {
                        let value = &r.value;

                        let mut idx = 0;

                        while idx < value.len() {
                            stream.reserve_capacity(value.len() - idx);
                            let mut available = stream.capacity();

                            if available == 0 {
                                available = 16384;
                            }

                            let end = idx + available;

                            if end >= value.len() {
                                stream
                                    .send_data(value.slice(idx..value.len()), true)
                                    .unwrap();
                                break;
                            } else {
                                stream.send_data(value.slice(idx..end), false).unwrap();
                                idx = end;
                            }
                        }

                        stream.reserve_capacity(1024);

                        let response = response.await;

                        if response.is_err() {
                            STORE_PUT_EX.increment();
                            STORE_RESPONSE_EX.increment();
                            continue;
                        }

                        let ttfb = start.elapsed();

                        let mut response = response.unwrap();
                        let body = response.body_mut();

                        if !body.is_end_stream() {
                            let mut flow_control = body.flow_control().clone();

                            while let Some(chunk) = body.data().await {
                                if chunk.is_err() {
                                    STORE_PUT_EX.increment();
                                    STORE_RESPONSE_EX.increment();
                                    continue;
                                }

                                let _ = flow_control.release_capacity(chunk.unwrap().len());
                            }
                        };

                        let status = response.status().as_u16();
                        let latency = start.elapsed();

                        STORE_REQUEST_OK.increment();

                        match status {
                            204 => {
                                STORE_PUT_OK.increment();
                                STORE_PUT_STORED.increment();

                                STORE_RESPONSE_OK.increment();

                                let _ = STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                let _ = STORE_RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                            }
                            429 => {
                                STORE_PUT_EX.increment();
                                STORE_RESPONSE_RATELIMITED.increment();
                            }
                            _ => {
                                STORE_PUT_EX.increment();
                                STORE_RESPONSE_EX.increment();
                            }
                        }
                    }
                }
                StoreClientRequest::Delete(r) => {
                    let key = &*r.key;
                    let uri = format!("/objectstore/{store}/{key}");
                    let request = MomentoHttpRequestBuilder::new(&endpoint, Method::DELETE, &uri)
                        .build(&token);

                    let start = Instant::now();

                    match sender.send_request(request, true) {
                        Ok((response, _)) => {
                            let response = response.await;

                            let mut response = match response {
                                Ok(r) => r,
                                Err(_e) => {
                                    STORE_DELETE_EX.increment();
                                    STORE_RESPONSE_EX.increment();
                                    continue;
                                }
                            };

                            let ttfb = start.elapsed();
                            let status = response.status().as_u16();

                            // drain the response body
                            let body = response.body_mut();

                            if !body.is_end_stream() {
                                let mut flow_control = body.flow_control().clone();

                                let used = flow_control.used_capacity();
                                if flow_control.release_capacity(used).is_err() {
                                    STORE_DELETE_EX.increment();
                                    STORE_RESPONSE_EX.increment();
                                    continue;
                                }

                                while let Some(chunk) = body.data().await {
                                    if chunk.is_err() {
                                        STORE_DELETE_EX.increment();
                                        STORE_RESPONSE_EX.increment();
                                        continue;
                                    }

                                    let _ = flow_control.release_capacity(chunk.unwrap().len());
                                }
                            }

                            let latency = start.elapsed();

                            STORE_REQUEST_OK.increment();

                            match status {
                                204 => {
                                    STORE_DELETE_OK.increment();

                                    STORE_RESPONSE_OK.increment();

                                    let _ =
                                        STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = STORE_RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                429 => {
                                    STORE_DELETE_EX.increment();
                                    STORE_RESPONSE_RATELIMITED.increment();
                                }
                                _ => {
                                    STORE_DELETE_EX.increment();
                                    STORE_RESPONSE_EX.increment();
                                }
                            }
                        }
                        Err(_) => {
                            continue;
                        }
                    }
                }
                _ => {
                    STORE_REQUEST_UNSUPPORTED.increment();
                    continue;
                }
            },
            ClientWorkItemKind::Reconnect => {
                continue;
            }
        };
    }

    Ok(())
}
