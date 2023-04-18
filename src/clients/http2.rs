// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;
use crate::net::Connector;
use bytes::Bytes;
use http_body_util::Empty;
use hyper::client::conn::http2::SendRequest;
use hyper::header::{HeaderName, HeaderValue};
use hyper::rt::Executor;
use hyper::{Request, Uri};
use std::future::Future;

#[derive(Clone)]
struct Queue<T> {
    tx: async_channel::Sender<T>,
    rx: async_channel::Receiver<T>,
}

impl<T> Queue<T> {
    pub fn new(size: usize) -> Self {
        let (tx, rx) = async_channel::bounded::<T>(size);

        Self { tx, rx }
    }

    pub async fn send(&self, item: T) -> std::result::Result<(), async_channel::SendError<T>> {
        self.tx.send(item).await
    }

    pub async fn recv(&self) -> std::result::Result<T, async_channel::RecvError> {
        self.rx.recv().await
    }
}

// launch a pool manager and worker tasks since HTTP/2.0 is mux'ed we prepare
// senders in the pool manager and pass them over a queue to our worker tasks
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching http2 protocol tasks");

    for _ in 0..config.client().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            // for each endpoint have poolsize # of pool_managers, each managing
            // a single TCP stream

            let queue = Queue::new(1);
            runtime.spawn(pool_manager(
                endpoint.clone(),
                config.clone(),
                queue.clone(),
            ));

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

struct TokioExecutor;

impl<F> Executor<F> for TokioExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        tokio::spawn(future);
    }
}

async fn pool_manager(endpoint: String, config: Config, queue: Queue<SendRequest<Empty<Bytes>>>) {
    let connector = Connector::new(&config).expect("failed to init connector");
    let mut sender = None;

    while RUNNING.load(Ordering::Relaxed) {
        if sender.is_none() {
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

            let (s, conn) =
                match hyper::client::conn::http2::handshake(TokioExecutor {}, stream).await {
                    Ok((s, c)) => (s, c),
                    Err(_e) => {
                        CONNECT_EX.increment();
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };

            SESSION.increment();

            sender = Some(s);

            tokio::task::spawn(async move {
                if let Err(err) = conn.await {
                    println!("Connection failed: {:?}", err);
                }
            });
        }

        let mut s = sender.take().unwrap();

        if let Err(_e) = s.ready().await {
            continue;
        }

        if queue.send(s.clone()).await.is_err() {
            return;
        }

        sender = Some(s);
    }
}

// a task for http/2.0
#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<WorkItem>,
    endpoint: String,
    config: Config,
    queue: Queue<SendRequest<Empty<Bytes>>>,
) -> Result<()> {
    // let connector = Connector::new(&config)?;
    let mut sender = None;

    while RUNNING.load(Ordering::Relaxed) {
        if sender.is_none() {
            let s = queue
                .recv()
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            sender = Some(s);
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
                match work_item {
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
                    .get(HeaderName::from_bytes(b"connection").unwrap())
                {
                    if header == HeaderValue::from_static("close") {
                        SESSION_CLOSED_SERVER.increment();
                    }
                }
            }
            Ok(Err(_e)) => {
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
                continue;
            }
            Err(_) => {
                RESPONSE_TIMEOUT.increment();
                SESSION_CLOSED_CLIENT.increment();
                continue;
            }
        }

        if let Err(_e) = s.ready().await {
            continue;
        }

        sender = Some(s);
    }

    Ok(())
}
