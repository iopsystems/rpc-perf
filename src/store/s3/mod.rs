use crate::workload::ClientWorkItemKind;
use crate::workload::StoreClientRequest;
use crate::*;

use async_channel::Receiver;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::{HeaderMap, Method, Version};
use http_body_util::{BodyExt, Full};
use hyper_rustls::ConfigBuilderExt;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::runtime::Runtime;

use std::io::{Error, ErrorKind};
use std::time::Instant;

mod aws_helpers;

use aws_helpers::*;

// launch a pool manager and worker tasks since HTTP/2.0 is mux'ed we prepare
// senders in the pool manager and pass them over a queue to our worker tasks
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<StoreClientRequest>>,
) {
    debug!("launching s3 protocol tasks");

    if config.storage().unwrap().concurrency() > 1 {
        error!("S3 uses HTTP/1.1 which does not support multiplexing sessions onto single streams. Ignoring the concurrency parameter.");
    }

    for _ in 0..config.storage().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            runtime.spawn(task(
                work_receiver.clone(),
                endpoint.clone(),
                config.clone(),
            ));
        }
    }
}

#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<ClientWorkItemKind<StoreClientRequest>>,
    endpoint: String,
    _config: Config,
) -> Result<(), std::io::Error> {
    let access_key = std::env::var("AWS_ACCESS_KEY").unwrap_or_else(|_| {
        eprintln!("environment variable `AUTH_TOKEN` is not set");
        std::process::exit(1);
    });

    let secret_key = std::env::var("AWS_SECRET_KEY").unwrap_or_else(|_| {
        eprintln!("environment variable `AUTH_TOKEN` is not set");
        std::process::exit(1);
    });

    let uri = endpoint
        .parse::<http::Uri>()
        .map_err(|_| Error::new(ErrorKind::Other, "failed to parse uri"))?;

    let auth = uri
        .authority()
        .ok_or(Error::new(ErrorKind::Other, "uri has no authority"))?
        .clone();

    let parts: Vec<&str> = auth.host().split('.').collect();

    if parts.len() != 5 {
        eprintln!("expected endpoint to be in the form: bucket.region.amazonaws.com");
        std::process::exit(1);
    };

    let bucket = parts[0].to_string();
    let region = parts[2].to_string();

    let port = auth.port_u16().unwrap_or(443);

    let _connect_addr = format!("{auth}:{port}");

    let rustls_config = rustls::ClientConfig::builder()
        .with_native_roots()?
        .with_no_client_auth();

    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(rustls_config)
        .https_or_http()
        .enable_http1()
        .build();

    let mut client = None;
    let mut session_requests = 0;
    let mut session_start = Instant::now();

    while RUNNING.load(Ordering::Relaxed) {
        if client.is_none() {
            if session_requests != 0 {
                let stop = Instant::now();
                let lifecycle_ns = (stop - session_start).as_nanos() as u64;
                let _ = SESSION_LIFECYCLE_REQUESTS.increment(lifecycle_ns);
            }
            CONNECT.increment();

            let c: Client<_, Full<Bytes>> =
                Client::builder(TokioExecutor::new()).build(connector.clone());

            client = Some(c);

            session_start = Instant::now();
            session_requests = 0;
            CONNECT_CURR.increment();
            SESSION.increment();

            continue;
        }

        let c = client.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();

        match &work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
                StoreClientRequest::Get(r) => {
                    let key = &*r.key;

                    let request = S3RequestBuilder::get_object(
                        region.clone(),
                        bucket.clone(),
                        key.to_string(),
                    )
                    .build(&access_key, &secret_key);

                    let start = Instant::now();

                    match c.request(request).await {
                        Ok(response) => {
                            let status = response.status().as_u16();

                            // wait until we have a complete response body
                            let body = response.into_body().collect().await.unwrap().to_bytes();

                            let latency = start.elapsed();

                            STORE_REQUEST_OK.increment();

                            match status {
                                200 => {
                                    STORE_RESPONSE_OK.increment();
                                    STORE_GET_OK.increment();
                                    STORE_RESPONSE_FOUND.increment();
                                    STORE_GET_KEY_FOUND.increment();

                                    let _ =
                                        STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                }
                                404 => {
                                    STORE_RESPONSE_OK.increment();
                                    STORE_GET_OK.increment();
                                    STORE_RESPONSE_NOT_FOUND.increment();
                                    STORE_GET_KEY_NOT_FOUND.increment();

                                    let _ =
                                        STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                }
                                _ => {
                                    STORE_RESPONSE_EX.increment();
                                    STORE_GET_EX.increment();

                                    debug!("Error Body:\n{}", String::from_utf8_lossy(&body));
                                }
                            }
                        }
                        Err(e) => {
                            debug!("error: {e}");
                            continue;
                        }
                    }
                }
                StoreClientRequest::Put(r) => {
                    let key = &*r.key;
                    let value = r.value.clone();

                    let request = S3RequestBuilder::put_object(
                        region.clone(),
                        bucket.clone(),
                        key.to_string(),
                        value,
                    )
                    .build(&access_key, &secret_key);

                    let start = Instant::now();

                    match c.request(request).await {
                        Ok(response) => {
                            let status = response.status().as_u16();

                            // wait until we have a complete response body
                            let body = response.into_body().collect().await.unwrap().to_bytes();

                            let latency = start.elapsed();

                            STORE_REQUEST_OK.increment();

                            match status {
                                200 => {
                                    STORE_RESPONSE_OK.increment();
                                    STORE_PUT_OK.increment();
                                    STORE_RESPONSE_FOUND.increment();
                                    STORE_PUT_STORED.increment();

                                    let _ =
                                        STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                }
                                _ => {
                                    STORE_RESPONSE_EX.increment();
                                    STORE_PUT_EX.increment();

                                    error!("Error Body:\n{}", String::from_utf8_lossy(&body));
                                }
                            }
                        }
                        Err(e) => {
                            error!("error: {e}");
                            continue;
                        }
                    }
                }
                StoreClientRequest::Delete(r) => {
                    let key = &*r.key;

                    let request = S3RequestBuilder::delete_object(
                        region.clone(),
                        bucket.clone(),
                        key.to_string(),
                    )
                    .build(&access_key, &secret_key);

                    let start = Instant::now();

                    match c.request(request).await {
                        Ok(response) => {
                            let status = response.status().as_u16();

                            // wait until we have a complete response body
                            let body = response.into_body().collect().await.unwrap().to_bytes();

                            let latency = start.elapsed();

                            STORE_REQUEST_OK.increment();

                            match status {
                                204 => {
                                    STORE_RESPONSE_OK.increment();
                                    STORE_DELETE_OK.increment();

                                    let _ =
                                        STORE_RESPONSE_LATENCY.increment(latency.as_nanos() as _);
                                }
                                _ => {
                                    STORE_RESPONSE_EX.increment();
                                    STORE_DELETE_EX.increment();

                                    error!("Error Body:\n{}", String::from_utf8_lossy(&body));
                                }
                            }
                        }
                        Err(e) => {
                            error!("error: {e}");
                            CONNECT_CURR.decrement();
                            continue;
                        }
                    }
                }
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                }
            },
            ClientWorkItemKind::Reconnect => {
                CONNECT_CURR.decrement();
                continue;
            }
        };

        client = Some(c);
    }

    Ok(())
}

pub struct S3RequestBuilder {
    inner: http::request::Builder,
    region: String,
    relative_uri: String,
    content: Vec<u8>,
    content_sha256: String,
    timestamp: DateTime<Utc>,
}

impl S3RequestBuilder {
    fn new(
        region: String,
        bucket: String,
        method: Method,
        relative_uri: String,
        content: Vec<u8>,
    ) -> Self {
        let now = Utc::now();
        // let date = format!("{}", now.format("%Y%m%d"));
        let datetime = format!("{}", now.format("%Y%m%dT%H%M%SZ"));
        // let rfc2822 = now.to_rfc2822().to_string();

        let content_sha256 = sha256_sum(&content);

        let mut headers = HeaderMap::new();

        headers.insert(
            "host",
            format!("{bucket}.s3.amazonaws.com").parse().unwrap(),
        );
        headers.insert("x-amz-content-sha256", content_sha256.parse().unwrap());
        headers.insert("x-amz-date", datetime.parse().unwrap());

        let inner = http::Request::builder()
            .version(Version::HTTP_11)
            .method(method)
            .uri(&format!("https://{bucket}.s3.amazonaws.com{relative_uri}"))
            .header("host", &format!("{bucket}.s3.amazonaws.com"))
            .header("x-amz-content-sha256", &content_sha256)
            .header("x-amz-date", datetime);

        Self {
            inner,
            region,
            relative_uri,
            content,
            content_sha256,
            timestamp: now,
        }
    }

    pub fn build(self, access_key: &str, secret_key: &str) -> http::Request<Full<Bytes>> {
        // form and hash the canonical request
        let mut canonical_request = vec![
            self.inner.method_ref().unwrap().as_str().to_string(),
            self.relative_uri,
            String::new(),
        ];

        let mut signed_hdr_names = Vec::new();
        let mut signed_headers = Vec::new();

        for (key, value) in self.inner.headers_ref().unwrap().iter() {
            signed_headers.push(format!("{key}:{}", value.to_str().unwrap()));
            signed_hdr_names.push(format!("{key}"));
        }

        signed_hdr_names.sort();
        signed_headers.sort();

        canonical_request.extend_from_slice(&signed_headers);

        let signed_hdr_names = signed_hdr_names.join(";");

        canonical_request.push(String::new());
        canonical_request.push(signed_hdr_names.clone());
        canonical_request.push(self.content_sha256);

        let canonical_request = canonical_request.join("\n");

        let date = format!("{}", self.timestamp.format("%Y%m%d"));
        let datetime = format!("{}", self.timestamp.format("%Y%m%dT%H%M%SZ"));

        trace!("canonical request:\n{canonical_request}");

        let region = self.region;

        let request_hash = sha256_sum(canonical_request);
        let scope = format!("{date}/{region}/s3/aws4_request");
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{request_hash}");

        trace!("string to sign:\n{string_to_sign}");

        let signing_key = generate_signing_key(secret_key, &date, &region, "s3");
        let signature = calculate_signature(signing_key, string_to_sign.as_bytes());

        // and finally our authorization header
        let authorization = format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders={signed_hdr_names},Signature={signature}");

        self.inner
            .header("authorization", authorization)
            .body(Full::<Bytes>::new(self.content.into()))
            .unwrap()
    }

    pub fn delete_object(region: String, bucket: String, key: String) -> Self {
        Self::new(
            region,
            bucket,
            Method::DELETE,
            format!("/{key}"),
            Vec::new(),
        )
    }

    pub fn get_object(region: String, bucket: String, key: String) -> Self {
        Self::new(region, bucket, Method::GET, format!("/{key}"), Vec::new())
    }

    pub fn put_object(region: String, bucket: String, key: String, value: Vec<u8>) -> Self {
        let mut s = Self::new(region, bucket, Method::PUT, format!("/{key}"), value);

        s.inner = s
            .inner
            .header("date", s.timestamp.to_rfc2822())
            .header("x-amz-storage-class", "STANDARD");

        s
    }
}
