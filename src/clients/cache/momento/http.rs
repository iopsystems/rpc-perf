use crate::clients::common::*;
use crate::clients::h2_pool::{h2_pool_manager, MomentoHttpRequestBuilder};
use crate::workload::{ClientRequest, ClientWorkItemKind};
use crate::*;

use async_channel::Receiver;
use bytes::{Bytes, BytesMut};
use h2::client::SendRequest;
use http::Method;
use tokio::runtime::Runtime;

use std::time::Instant;

// launch a pool manager and worker tasks since HTTP/2.0 is mux'ed we prepare
// senders in the pool manager and pass them over a queue to our worker tasks
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
) {
    debug!("launching momento http protocol tasks");

    for _ in 0..config.client().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            // for each endpoint have poolsize # of pool_managers, each managing
            // a single TCP stream

            let queue = Queue::new(1);
            runtime.spawn(h2_pool_manager(endpoint.clone(), queue.clone()));

            // since HTTP/2.0 allows muxing several sessions onto a single TCP
            // stream, we launch one task for each session on this TCP stream
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

// a task for http/2.0
#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
    endpoint: String,
    config: Config,
    queue: Queue<SendRequest<Bytes>>,
) -> Result<(), std::io::Error> {
    let token = std::env::var("MOMENTO_API_KEY").unwrap_or_else(|_| {
        eprintln!("environment variable `MOMENTO_API_KEY` is not set");
        std::process::exit(1);
    });

    let cache = config.target().cache_name().unwrap_or_else(|| {
        eprintln!("cache name is not specified in the `target` section");
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

    let mut buffer = BytesMut::new();

    while RUNNING.load(Ordering::Relaxed) {
        let sender = queue.recv().await;

        if sender.is_err() {
            continue;
        }

        let mut sender = sender.unwrap();

        let work_item = match work_receiver.recv().await {
            Ok(w) => w,
            Err(_) => {
                return Err(std::io::Error::other(
                    "channel closed",
                ));
            }
        };

        REQUEST.increment();

        match &work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
                ClientRequest::Get(r) => {
                    let key = std::str::from_utf8(&r.key).unwrap();
                    let uri = format!("/cache/{cache}?key={key}");
                    let request =
                        MomentoHttpRequestBuilder::new(&endpoint, Method::GET, &uri).build(&token);

                    let start = Instant::now();

                    match sender.send_request(request, true) {
                        Ok((response, _)) => {
                            let response = response.await;

                            let mut response = match response {
                                Ok(r) => r,
                                Err(_e) => {
                                    GET_EX.increment();

                                    RESPONSE_EX.increment();

                                    continue;
                                }
                            };

                            // ttfb based on the headers being received

                            let ttfb = start.elapsed();

                            let status = response.status().as_u16();

                            // read the response body to completion

                            buffer.truncate(0);

                            let body = response.body_mut();

                            if !body.is_end_stream() {
                                // get the flow control handle
                                let mut flow_control = body.flow_control().clone();

                                // release all capacity that we can release
                                let used = flow_control.used_capacity();
                                if flow_control.release_capacity(used).is_err() {
                                    GET_EX.increment();

                                    RESPONSE_EX.increment();

                                    continue;
                                }

                                // loop to read all the data
                                while let Some(chunk) = body.data().await {
                                    if chunk.is_err() {
                                        GET_EX.increment();

                                        RESPONSE_EX.increment();

                                        continue;
                                    }

                                    // Let the server send more data.
                                    let _ = flow_control.release_capacity(chunk.unwrap().len());
                                }
                            }

                            let latency = start.elapsed();

                            REQUEST_OK.increment();

                            match status {
                                200 => {
                                    GET_OK.increment();
                                    GET_KEY_HIT.increment();

                                    RESPONSE_OK.increment();
                                    RESPONSE_HIT.increment();

                                    let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                404 => {
                                    GET_OK.increment();
                                    GET_KEY_MISS.increment();

                                    RESPONSE_OK.increment();
                                    RESPONSE_MISS.increment();

                                    let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                429 => {
                                    GET_EX.increment();

                                    RESPONSE_RATELIMITED.increment();
                                }
                                _ => {
                                    GET_EX.increment();

                                    RESPONSE_EX.increment();
                                }
                            }
                        }
                        Err(_) => {
                            continue;
                        }
                    }
                }
                ClientRequest::Set(r) => {
                    let key = std::str::from_utf8(&r.key).unwrap();
                    let ttl = r.ttl.unwrap_or_else(|| Duration::from_secs(900));
                    let uri = format!("/cache/{cache}?key={key}&ttl_seconds={}", ttl.as_secs());
                    let request =
                        MomentoHttpRequestBuilder::new(&endpoint, Method::PUT, &uri).build(&token);

                    let start = Instant::now();

                    if let Ok((response, mut stream)) = sender.send_request(request, false) {
                        let value = &r.value;

                        let mut idx = 0;

                        while idx < value.len() {
                            stream.reserve_capacity(value.len() - idx);
                            let mut available = stream.capacity();

                            // default minimum of a 16KB frame...
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

                        // reduce the stream capacity
                        stream.reserve_capacity(1024);

                        let response = response.await;

                        if response.is_err() {
                            SET_EX.increment();

                            RESPONSE_EX.increment();

                            continue;
                        }

                        let ttfb = start.elapsed();

                        let mut response = response.unwrap();
                        let body = response.body_mut();

                        if !body.is_end_stream() {
                            // get the flow control handle
                            let mut flow_control = body.flow_control().clone();

                            // loop to read all the response
                            while let Some(chunk) = body.data().await {
                                if chunk.is_err() {
                                    SET_EX.increment();

                                    RESPONSE_EX.increment();

                                    continue;
                                }

                                // Let the server send more data.
                                let _ = flow_control.release_capacity(chunk.unwrap().len());
                            }
                        };

                        let status = response.status().as_u16();

                        let latency = start.elapsed();

                        REQUEST_OK.increment();

                        match status {
                            204 => {
                                SET_STORED.increment();

                                RESPONSE_OK.increment();

                                let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                let _ = RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                            }
                            429 => {
                                SET_EX.increment();

                                RESPONSE_RATELIMITED.increment();
                            }
                            _ => {
                                SET_EX.increment();

                                RESPONSE_EX.increment();
                            }
                        }
                    }
                }
                ClientRequest::Delete(r) => {
                    let key = std::str::from_utf8(&r.key).unwrap();
                    let uri = format!("/cache/{cache}?key={key}");
                    let request = MomentoHttpRequestBuilder::new(&endpoint, Method::DELETE, &uri)
                        .build(&token);

                    let start = Instant::now();

                    match sender.send_request(request, false) {
                        Ok((response, _)) => {
                            let response = response.await;

                            let mut response = match response {
                                Ok(r) => r,
                                Err(_e) => {
                                    DELETE_EX.increment();

                                    RESPONSE_EX.increment();

                                    continue;
                                }
                            };

                            // ttfb based on the headers being received

                            let ttfb = start.elapsed();

                            let status = response.status().as_u16();

                            // read the response body to completion

                            buffer.truncate(0);

                            let body = response.body_mut();

                            if !body.is_end_stream() {
                                // get the flow control handle
                                let mut flow_control = body.flow_control().clone();

                                // release all capacity that we can release
                                let used = flow_control.used_capacity();
                                if flow_control.release_capacity(used).is_err() {
                                    DELETE_EX.increment();

                                    RESPONSE_EX.increment();

                                    continue;
                                }

                                // loop to read all the data
                                while let Some(chunk) = body.data().await {
                                    if chunk.is_err() {
                                        DELETE_EX.increment();

                                        RESPONSE_EX.increment();

                                        continue;
                                    }

                                    // Let the server send more data.
                                    let _ = flow_control.release_capacity(chunk.unwrap().len());
                                }
                            }

                            let latency = start.elapsed();

                            REQUEST_OK.increment();

                            match status {
                                204 => {
                                    DELETE_DELETED.increment();

                                    RESPONSE_OK.increment();

                                    let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                404 => {
                                    DELETE_NOT_FOUND.increment();

                                    RESPONSE_OK.increment();

                                    let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                429 => {
                                    DELETE_EX.increment();

                                    RESPONSE_RATELIMITED.increment();
                                }
                                _ => {
                                    DELETE_EX.increment();

                                    RESPONSE_EX.increment();
                                }
                            }
                        }
                        Err(_) => {
                            continue;
                        }
                    }
                }
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                    continue;
                }
            },
            ClientWorkItemKind::Reconnect => {
                REQUEST_UNSUPPORTED.increment();
                continue;
            }
        };
    }

    Ok(())
}
