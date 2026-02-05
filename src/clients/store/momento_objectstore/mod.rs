use crate::clients::common::*;
use crate::workload::{ClientWorkItemKind, StoreClientRequest};
use crate::*;
use rustls::KeyLogFile;

use async_channel::Receiver;
use bytes::{Bytes, BytesMut};
use h2::client::SendRequest;
use http::{Method, Version};
use rustls::pki_types::ServerName;
use rustls::RootCertStore;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio_rustls::TlsConnector;

use std::time::{Duration, Instant};

const MB: u32 = 1024 * 1024;

pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<StoreClientRequest>>,
) {
    debug!("launching momento objectstore protocol tasks");

    for _ in 0..config.storage().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            let queue = Queue::new(1);
            runtime.spawn(pool_manager(
                endpoint.clone(),
                config.clone(),
                queue.clone(),
            ));

            for _ in 0..config.storage().unwrap().concurrency() {
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
                    let stream = match connector
                        .connect(ServerName::try_from(auth.host().to_string()).unwrap(), tcp)
                        .await
                    {
                        Ok(s) => s,
                        Err(_) => {
                            CONNECT_EX.increment();
                            continue;
                        }
                    };

                    let client_builder = ::h2::client::Builder::new()
                        .initial_window_size(8 * MB)
                        .initial_connection_window_size(8 * MB)
                        .max_frame_size(2 * MB)
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

            CONNECT_EX.increment();
        } else if let Ok(s) = client.clone().unwrap().ready().await {
            let _ = queue.send(s).await;
        } else {
            client = None;
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

    let store_name = config.target().object_store_name().unwrap_or_else(|| {
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
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "channel closed",
                ));
            }
        };

        REQUEST.increment();

        match &work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
                StoreClientRequest::Get(r) => {
                    let request = ObjectStoreRequestBuilder::get(
                        &endpoint,
                        store_name,
                        &r.key,
                    )
                    .build(&token);

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

                            buffer.truncate(0);

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

                                    let _ = STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = STORE_RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                404 => {
                                    STORE_GET_OK.increment();
                                    STORE_GET_KEY_NOT_FOUND.increment();
                                    STORE_RESPONSE_OK.increment();
                                    STORE_RESPONSE_NOT_FOUND.increment();

                                    let _ = STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
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
                    let ttl = r.ttl.unwrap_or_else(|| Duration::from_secs(3600));

                    let request = ObjectStoreRequestBuilder::put(
                        &endpoint,
                        store_name,
                        &r.key,
                        ttl,
                    )
                    .build(&token);

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

                        if value.is_empty() {
                            stream.send_data(Bytes::new(), true).unwrap();
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
                    let request = ObjectStoreRequestBuilder::delete(
                        &endpoint,
                        store_name,
                        &r.key,
                    )
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

                            buffer.truncate(0);

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

                                    let _ = STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                    let _ = STORE_RESPONSE_TTFB.increment(ttfb.as_nanos() as _);
                                }
                                404 => {
                                    STORE_DELETE_OK.increment();
                                    STORE_RESPONSE_OK.increment();

                                    let _ = STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
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

pub struct ObjectStoreRequestBuilder {
    inner: http::request::Builder,
}

impl ObjectStoreRequestBuilder {
    fn new(endpoint: &str, method: Method, relative_uri: &str) -> Self {
        let inner = http::request::Builder::new()
            .version(Version::HTTP_2)
            .method(method)
            .uri(format!("https://{endpoint}{relative_uri}"));

        Self { inner }
    }

    pub fn build(self, token: &str) -> http::Request<()> {
        self.inner.header("authorization", token).body(()).unwrap()
    }

    pub fn delete(endpoint: &str, store: &str, key: &str) -> Self {
        let uri = format!("/objectstore/{store}/{key}");
        Self::new(endpoint, Method::DELETE, &uri)
    }

    pub fn get(endpoint: &str, store: &str, key: &str) -> Self {
        let uri = format!("/objectstore/{store}/{key}");
        Self::new(endpoint, Method::GET, &uri)
    }

    pub fn put(endpoint: &str, store: &str, key: &str, ttl: Duration) -> Self {
        let uri = format!("/objectstore/{store}/{key}?ttl_seconds={}", ttl.as_secs());
        Self::new(endpoint, Method::PUT, &uri)
    }
}
