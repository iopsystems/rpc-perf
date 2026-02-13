use crate::clients::common::*;
use crate::*;
use rustls::KeyLogFile;

use bytes::Bytes;
use h2::client::SendRequest;
use http::{Method, Version};
use rustls::pki_types::ServerName;
use rustls::RootCertStore;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

const MB: u32 = 1024 * 1024;

/// Shared HTTP/2 connection pool manager for Momento services.
///
/// Maintains a TLS+H2 connection and hands out ready `SendRequest` handles
/// via the provided queue. Both the cache (momento_http) and store
/// (momento_objectstore) clients reuse this.
pub async fn h2_pool_manager(endpoint: String, queue: Queue<SendRequest<Bytes>>) {
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

/// Generic HTTP/2 request builder for Momento services.
///
/// Both cache and objectstore clients construct their own relative URIs
/// and then use this builder to produce the final `http::Request`.
pub struct MomentoHttpRequestBuilder {
    inner: http::request::Builder,
}

impl MomentoHttpRequestBuilder {
    pub fn new(endpoint: &str, method: Method, relative_uri: &str) -> Self {
        let inner = http::request::Builder::new()
            .version(Version::HTTP_2)
            .method(method)
            .uri(format!("https://{endpoint}{relative_uri}"));

        Self { inner }
    }

    pub fn build(self, token: &str) -> http::Request<()> {
        self.inner.header("authorization", token).body(()).unwrap()
    }
}
