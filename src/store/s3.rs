use crate::workload::ClientWorkItemKind;
use crate::workload::StoreClientRequest;
use crate::*;
use async_channel::Receiver;
use bytes::Bytes;
use chrono::Utc;
use futures::Future;
use hmac::{Hmac, Mac};
use http::Method;
use http::Version;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::rt::Executor;
use hyper_rustls::ConfigBuilderExt;
use hyper_util::client::legacy::Client;
use sha2::digest::FixedOutput;
use sha2::Digest;
use sha2::Sha256;
use std::io::Error;
use std::io::ErrorKind;
use std::time::Instant;
use tokio::runtime::Runtime;

static EMPTY_BODY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

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

#[derive(Clone)]
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

    let bucket = parts[0];
    let region = parts[2];

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
                Client::builder(TokioExecutor {}).build(connector.clone());

            client = Some(c);

            session_start = Instant::now();
            session_requests = 0;
            CONNECT_CURR.increment();
            SESSION.increment();

            continue;

            // session = Some(s);

            // tokio::task::spawn(async move {
            //     if let Err(err) = conn.await {
            //         println!("Connection failed: {:?}", err);
            //     }
            // });
        }

        let c = client.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();

        let now = Utc::now();

        // date with format 20240920
        let date = format!("{}", now.format("%Y%m%d"));

        // datetime with format: 20240920T084700Z
        let datetime = format!("{}", now.format("%Y%m%dT%H%M%SZ"));

        // datetime in rfc2822 format: "Fri, 28 Nov 2014 12:00:09 +0000"
        let rfc2822 = now.to_rfc2822().to_string();

        match &work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
                StoreClientRequest::Get(r) => {
                    let key = &*r.key;
                    let content_sha256 = EMPTY_BODY_SHA256;

                    // form and hash the canonical request
                    let canonical_request = vec![
                        "GET".to_string(),
                        format!("/{key}"),
                        "".to_string(),
                        format!("host:{bucket}.s3.amazonaws.com"),
                        format!("x-amz-content-sha256:{content_sha256}"),
                        format!("x-amz-date:{datetime}"),
                        "".to_string(),
                        "host;x-amz-content-sha256;x-amz-date".to_string(),
                        content_sha256.to_string(),
                    ]
                    .join("\n");

                    trace!("canonical request:\n{canonical_request}");

                    let request_hash = sha256_sum(canonical_request);

                    let scope = format!("{date}/{region}/s3/aws4_request");

                    let string_to_sign =
                        format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{request_hash}");

                    trace!("string to sign:\n{string_to_sign}");

                    let signing_key = generate_signing_key(&secret_key, &date, region, "s3");
                    let signature = calculate_signature(signing_key, string_to_sign.as_bytes());

                    // and finally our authorization header
                    let authorization = format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={signature}");

                    trace!("authorization: {authorization}");

                    // now we can make our request
                    let request = http::request::Builder::new()
                        .version(Version::HTTP_11)
                        .method(Method::GET)
                        .uri(&format!("https://{bucket}.s3.amazonaws.com/{key}"))
                        .header("host", &format!("{bucket}.s3.amazonaws.com"))
                        .header("authorization", authorization)
                        .header("x-amz-content-sha256", content_sha256)
                        .header("x-amz-date", datetime)
                        .body(Full::<Bytes>::new(vec![].into()))
                        .unwrap();

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
                    let content_sha256 = sha256_sum(&r.value);

                    // form and hash the canonical request
                    let canonical_request = vec![
                        "PUT".to_string(),
                        format!("/{key}"),
                        "".to_string(),
                        format!("date:{rfc2822}"),
                        format!("host:{bucket}.s3.amazonaws.com"),
                        format!("x-amz-content-sha256:{content_sha256}"),
                        format!("x-amz-date:{datetime}"),
                        "x-amz-storage-class:STANDARD".to_string(),
                        "".to_string(),
                        "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class".to_string(),
                        content_sha256.to_string(),
                    ]
                    .join("\n");

                    trace!("canonical request:\n{canonical_request}");

                    let request_hash = sha256_sum(canonical_request);

                    let scope = format!("{date}/{region}/s3/aws4_request");

                    let string_to_sign =
                        format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{request_hash}");

                    trace!("string to sign:\n{string_to_sign}");

                    let signing_key = generate_signing_key(&secret_key, &date, region, "s3");
                    let signature = calculate_signature(signing_key, string_to_sign.as_bytes());

                    // and finally our authorization header
                    let authorization = format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,Signature={signature}");

                    trace!("authorization: {authorization}");

                    // now we can make our request
                    let request = http::request::Builder::new()
                        .version(Version::HTTP_11)
                        .method(Method::PUT)
                        .uri(&format!("https://{bucket}.s3.amazonaws.com/{key}"))
                        .header("date", rfc2822)
                        .header("host", &format!("{bucket}.s3.amazonaws.com"))
                        .header("authorization", authorization)
                        .header("x-amz-content-sha256", content_sha256)
                        .header("x-amz-date", datetime)
                        .header("x-amz-storage-class", "STANDARD")
                        .body(Full::<Bytes>::new(r.value.clone().into()))
                        .unwrap();

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
                    let content_sha256 = EMPTY_BODY_SHA256;

                    // form and hash the canonical request
                    let canonical_request = vec![
                        "DELETE".to_string(),
                        format!("/{key}"),
                        "".to_string(),
                        format!("host:{bucket}.s3.amazonaws.com"),
                        format!("x-amz-content-sha256:{content_sha256}"),
                        format!("x-amz-date:{datetime}"),
                        "".to_string(),
                        "host;x-amz-content-sha256;x-amz-date".to_string(),
                        content_sha256.to_string(),
                    ]
                    .join("\n");

                    trace!("canonical request:\n{canonical_request}");

                    let request_hash = sha256_sum(canonical_request);

                    let scope = format!("{date}/{region}/s3/aws4_request");

                    let string_to_sign =
                        format!("AWS4-HMAC-SHA256\n{datetime}\n{scope}\n{request_hash}");

                    trace!("string to sign:\n{string_to_sign}");

                    let signing_key = generate_signing_key(&secret_key, &date, region, "s3");
                    let signature = calculate_signature(signing_key, string_to_sign.as_bytes());

                    // and finally our authorization header
                    let authorization = format!("AWS4-HMAC-SHA256 Credential={access_key}/{scope},SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature={signature}");

                    trace!("authorization: {authorization}");

                    // now we can make our request
                    let request = http::request::Builder::new()
                        .version(Version::HTTP_11)
                        .method(Method::DELETE)
                        .uri(&format!("https://{bucket}.s3.amazonaws.com/{key}"))
                        .header("host", &format!("{bucket}.s3.amazonaws.com"))
                        .header("authorization", authorization)
                        .header("x-amz-content-sha256", content_sha256)
                        .header("x-amz-date", datetime)
                        .body(Full::<Bytes>::new(vec![].into()))
                        .unwrap();

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

        client = Some(c);
    }

    Ok(())
}

/* the code below was taken from AWS Rust SDK */

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/// Generates a signing key for Sigv4
pub fn generate_signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> impl AsRef<[u8]> {
    // kSecret = your secret access key
    // kDate = HMAC("AWS4" + kSecret, Date)
    // kRegion = HMAC(kDate, Region)
    // kService = HMAC(kRegion, Service)
    // kSigning = HMAC(kService, "aws4_request")

    let secret = format!("AWS4{}", secret);
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_ref()).expect("HMAC can take key of any size");
    mac.update(date.as_bytes());
    let tag = mac.finalize_fixed();

    // sign region
    let mut mac = Hmac::<Sha256>::new_from_slice(&tag).expect("HMAC can take key of any size");
    mac.update(region.as_bytes());
    let tag = mac.finalize_fixed();

    // sign service
    let mut mac = Hmac::<Sha256>::new_from_slice(&tag).expect("HMAC can take key of any size");
    mac.update(service.as_bytes());
    let tag = mac.finalize_fixed();

    // sign request
    let mut mac = Hmac::<Sha256>::new_from_slice(&tag).expect("HMAC can take key of any size");
    mac.update("aws4_request".as_bytes());
    mac.finalize_fixed()
}

pub fn calculate_signature(signing_key: impl AsRef<[u8]>, string_to_sign: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key.as_ref())
        .expect("HMAC can take key of any size");
    mac.update(string_to_sign);
    hex::encode(mac.finalize_fixed())
}

pub fn sha256_sum(bytes: impl AsRef<[u8]>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize_fixed())
}
