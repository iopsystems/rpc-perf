// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;
use crate::net::Connector;
use bytes::Bytes;
use http_body_util::Empty;
use hyper::header::{HeaderName, HeaderValue};
use hyper::{Request, Uri};

/// Launch tasks with one conncetion per task as http/1.1 is not mux'd
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching http1 protocol tasks");

    if config.client().unwrap().concurrency() > 1 {
        error!("HTTP/1.1 does not support multiplexing sessions onto single streams. Ignoring the concurrency parameter.");
    }

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

// a task for http/1.1
#[allow(clippy::slow_vector_initialization)]
async fn task(work_receiver: Receiver<WorkItem>, endpoint: String, config: Config) -> Result<()> {
    let connector = Connector::new(&config)?;
    let mut sender = None;
    let mut session_requests = 0;
    let mut session_start = Instant::now();

    while RUNNING.load(Ordering::Relaxed) {
        if sender.is_none() {
            if session_requests != 0 {
                let stop = Instant::now();
                let lifecycle_ns = (stop - session_start).as_nanos();
                SESSION_LIFECYCLE_REQUESTS.increment(stop, lifecycle_ns, 1);
            }
            CONNECT.increment();
            let stream = match timeout(
                config.client().unwrap().connect_timeout(),
                connector.connect(&endpoint),
            )
            .await
            {
                Ok(Ok(s)) => s,
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
            };

            let (s, conn) = match hyper::client::conn::http1::handshake(stream).await {
                Ok((s, c)) => {
                    CONNECT_OK.increment();
                    (s, c)
                }
                Err(_e) => {
                    CONNECT_EX.increment();
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            session_start = Instant::now();
            session_requests = 0;
            CONNECT_CURR.add(1);
            SESSION.increment();

            sender = Some(s);

            tokio::task::spawn(async move {
                if let Err(err) = conn.await {
                    println!("Connection failed: {:?}", err);
                }
            });
        }

        let mut s = sender.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();

        // compose request into buffer
        let request = match &work_item {
            WorkItem::Request { request, sequence } => match request {
                ClientRequest::Get { key } => {
                    let key = unsafe { std::str::from_utf8_unchecked(key) };
                    let url: Uri = if config.tls().is_none() {
                        format!("http://{endpoint}/{key}").parse().unwrap()
                    } else {
                        format!("https://{endpoint}/{key}").parse().unwrap()
                    };
                    let authority = url.authority().unwrap().clone();

                    Request::builder()
                        .uri(url)
                        .header(hyper::header::HOST, authority.as_str())
                        .header(
                            hyper::header::USER_AGENT,
                            &format!("rpc-perf/5.0.0-alpha (request; seq:{sequence})"),
                        )
                        .body(Empty::<Bytes>::new())
                        .expect("failed to build request")
                }
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                    sender = Some(s);
                    continue;
                }
            },
            WorkItem::Reconnect => {
                SESSION_CLOSED_CLIENT.increment();
                REQUEST_RECONNECT.increment();
                CONNECT_CURR.sub(1);
                continue;
            }
        };

        REQUEST_OK.increment();

        // send request
        let start = Instant::now();
        let response = timeout(
            config.client().unwrap().request_timeout(),
            s.send_request(request),
        )
        .await;
        let stop = Instant::now();

        match response {
            Ok(Ok(response)) => {
                // validate response
                match &work_item {
                    WorkItem::Request { request, .. } => match request {
                        ClientRequest::Get { .. } => {
                            GET_OK.increment();
                        }
                        _ => {
                            error!("unexpected request");
                            unimplemented!();
                        }
                    },
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }

                RESPONSE_OK.increment();

                let latency = stop.duration_since(start).as_nanos();

                REQUEST_LATENCY.increment(start, latency, 1);
                RESPONSE_LATENCY.increment(stop, latency, 1);

                if let Some(header) = response
                    .headers()
                    .get(HeaderName::from_bytes(b"Connection").unwrap())
                {
                    if header == HeaderValue::from_static("close") {
                        SESSION_CLOSED_SERVER.increment();
                    }
                }
            }
            Ok(Err(_e)) => {
                RESPONSE_EX.increment();

                // record execption
                match work_item {
                    WorkItem::Request { request, .. } => match request {
                        ClientRequest::Get { .. } => {
                            GET_EX.increment();
                        }
                        _ => {
                            error!("unexpected request");
                            unimplemented!();
                        }
                    },
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }
                SESSION_CLOSED_CLIENT.increment();
                CONNECT_CURR.sub(1);
                continue;
            }
            Err(_) => {
                RESPONSE_TIMEOUT.increment();
                SESSION_CLOSED_CLIENT.increment();
                CONNECT_CURR.sub(1);
                continue;
            }
        }

        if let Err(_e) = s.ready().await {
            CONNECT_CURR.sub(1);
            continue;
        }

        session_requests += 1;

        sender = Some(s);
    }

    Ok(())
}
