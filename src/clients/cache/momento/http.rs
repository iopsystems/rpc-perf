use rustls::KeyLogFile;
use crate::clients::common::*;
use crate::workload::{ClientRequest, ClientWorkItemKind};
use crate::*;

use async_channel::Receiver;
use bytes::{Bytes, BytesMut};
use h2::client::SendRequest;
use http::{Method, Version};
use rustls::pki_types::ServerName;
use rustls::RootCertStore;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_rustls::TlsConnector;

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

pub async fn pool_manager(endpoint: String, _config: Config, queue: Queue<SendRequest<Bytes>>) {
    let mut client = None;

    let root_store = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.into(),
    };

    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    config.key_log = Arc::new(KeyLogFile::new());

    let config = Arc::new(config);

    while RUNNING.load(Ordering::Relaxed) {
        if client.is_none() {
            CONNECT.increment();

            if let Ok((addr, auth)) = resolve(&endpoint).await {
                if let Ok(tcp) = TcpStream::connect(addr).await {
                    if tcp.set_nodelay(true).is_err() {
                        continue;
                    }

                    let connector: TlsConnector = TlsConnector::from(config.clone());

                    let stream = connector
                        .connect(ServerName::try_from(auth.to_string()).unwrap(), tcp)
                        .await
                        .unwrap();

                    let client_builder = ::h2::client::Builder::new()
                        // .initial_window_size(32 * 1024 * 1024)
                        // .initial_connection_window_size(32 * 1024 * 1024)
                        // .max_frame_size(8 * 1024 * 1024)
                        .handshake(stream);

                    if let Ok((h2, connection)) = client_builder.await {
                        tokio::spawn(async move {
                            let _ = connection.await;
                        });

                        if let Ok(h2) = h2.ready().await {
                            CONNECT_OK.increment();
                            CONNECT_CURR.increment();

                            client = Some(h2);

                            continue;
                        }
                    }
                }
            }

            // Successfully negotiated connections result in early continue back
            // to the top of the loop. If we hit this, that means there was some
            // exception during connection establishment / negotiation.
            CONNECT_EX.increment();
        } else if let Ok(s) = client.clone().unwrap().ready().await {
            let _ = queue.send(s).await;
        } else {
            client = None;
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

    while RUNNING.load(Ordering::Relaxed) {
        let sender = queue.recv().await;

        if sender.is_err() {
            continue;
        }

        let mut sender = sender.unwrap();

        let work_item = match work_receiver.recv().await {
            Ok(w) => w,
            Err(e) => {
                error!("error while attempting to receive work item: {e}");
                continue;
            }
        };

        REQUEST.increment();

        match &work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
                ClientRequest::Get(r) => {
                    let request = MomentoRequestBuilder::get(
                        &endpoint,
                        cache,
                        std::str::from_utf8(&r.key).unwrap(),
                    )
                    .build(&token);

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

                            let status = response.status().as_u16();

                            // read the response body to completion

                            let mut buffer = BytesMut::new();

                            while let Some(chunk) = response.body_mut().data().await {
                                if let Ok(b) = chunk {
                                    // info!("chunk for get: ({}) {}", std::str::from_utf8(&r.key).unwrap(), b.len());
                                    buffer.extend_from_slice(&b);
                                    if response.body_mut().flow_control().release_capacity(b.len()).is_err() {
                                        // info!("error releasing capacity");
                                        GET_EX.increment();

                                        RESPONSE_EX.increment();

                                        continue;
                                    }

                                    // if body.is_end_stream() {
                                    //     info!("end of stream");
                                    // }
                                } else {
                                    GET_EX.increment();

                                    RESPONSE_EX.increment();

                                    continue;
                                }
                            }

                            // info!("get complete");

                            let latency = start.elapsed();

                            REQUEST_OK.increment();

                            match status {
                                200 => {
                                    GET_OK.increment();
                                    GET_KEY_HIT.increment();

                                    RESPONSE_OK.increment();
                                    RESPONSE_HIT.increment();

                                    let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                }
                                404 => {
                                    GET_OK.increment();
                                    GET_KEY_MISS.increment();

                                    RESPONSE_OK.increment();
                                    RESPONSE_MISS.increment();

                                    let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
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
                    let request = MomentoRequestBuilder::set(
                        &endpoint,
                        cache,
                        std::str::from_utf8(&r.key).unwrap(),
                        r.ttl,
                    )
                    .build(&token);

                    let start = Instant::now();

                    if let Ok((response, mut stream)) = sender.send_request(request, false) {
                        if stream
                            .send_data(Bytes::from(r.value.clone()), true)
                            .is_err()
                        {
                            continue;
                        }

                        let response = response.await;

                        let mut response = match response {
                            Ok(r) => r,
                            Err(_e) => {
                                SET_EX.increment();

                                RESPONSE_EX.increment();

                                continue;
                            }
                        };

                        let status = response.status().as_u16();

                        // info!("set status: {status}");

                        // read the response body to completion

                        let mut buffer = BytesMut::new();

                        while let Some(chunk) = response.body_mut().data().await {
                            if let Ok(b) = chunk {
                                // info!("chunk for set: {}", b.len());
                                buffer.extend_from_slice(&b);
                                if response.body_mut().flow_control().release_capacity(b.len()).is_err() {
                                    // info!("error releasing capacity");
                                    SET_EX.increment();

                                    RESPONSE_EX.increment();

                                    continue;
                                }

                            } else {
                                SET_EX.increment();

                                RESPONSE_EX.increment();

                                continue;
                            }
                        }

                        // info!("set complete");

                        let latency = start.elapsed();

                        REQUEST_OK.increment();

                        match status {
                            204 => {
                                SET_STORED.increment();

                                RESPONSE_OK.increment();
                                RESPONSE_HIT.increment();

                                let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
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
                    let request = MomentoRequestBuilder::delete(
                        &endpoint,
                        cache,
                        std::str::from_utf8(&r.key).unwrap(),
                    )
                    .build(&token);

                    let start = Instant::now();

                    match sender.send_request(request, false) {
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

                            let status = response.status().as_u16();

                            // info!("delete status: {status}");

                            // read the response body to completion

                            let mut buffer = BytesMut::new();

                            while let Some(chunk) = response.body_mut().data().await {
                                if let Ok(b) = chunk {
                                    // info!("chunk for delete: {}", b.len());
                                    buffer.extend_from_slice(&b);
                                    if response.body_mut().flow_control().release_capacity(b.len()).is_err() {
                                        // info!("error releasing capacity");
                                        DELETE_EX.increment();

                                        RESPONSE_EX.increment();

                                        continue;
                                    }
                                } else {
                                    DELETE_EX.increment();

                                    RESPONSE_EX.increment();

                                    continue;
                                }
                            }

                            // info!("delete complete");

                            let latency = start.elapsed();

                            REQUEST_OK.increment();

                            match status {
                                204 => {
                                    DELETE_DELETED.increment();

                                    RESPONSE_OK.increment();

                                    let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                }
                                404 => {
                                    DELETE_NOT_FOUND.increment();

                                    RESPONSE_OK.increment();
                                    let _ = RESPONSE_LATENCY.increment(latency.as_nanos() as _);
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

pub struct MomentoRequestBuilder {
    inner: http::request::Builder,
}

impl MomentoRequestBuilder {
    fn new(endpoint: &str, method: Method, relative_uri: &str) -> Self {
        let inner = http::request::Builder::new()
            .version(Version::HTTP_2)
            .method(method)
            .uri(&format!("https://{endpoint}{relative_uri}"));

        Self { inner }
    }

    pub fn build(self, token: &str) -> http::Request<()> {
        self.inner.header("authorization", token).body(()).unwrap()
    }

    pub fn delete(endpoint: &str, cache: &str, key: &str) -> Self {
        let uri = format!("/cache/{cache}?key={key}");

        Self::new(endpoint, Method::DELETE, &uri)
    }

    pub fn get(endpoint: &str, cache: &str, key: &str) -> Self {
        let uri = format!("/cache/{cache}?key={key}");

        Self::new(endpoint, Method::GET, &uri)
    }

    pub fn set(endpoint: &str, cache: &str, key: &str, ttl: Option<Duration>) -> Self {
        let uri = if let Some(ttl) = ttl {
            format!("/cache/{cache}?key={key}&ttl_seconds={}", ttl.as_secs())
        } else {
            format!("/cache/{cache}?key={key}")
        };

        Self::new(endpoint, Method::PUT, &uri)
    }
}
