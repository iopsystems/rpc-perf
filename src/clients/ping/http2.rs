use crate::clients::http2::Queue;
use crate::clients::http2::TokioExecutor;
use crate::clients::timeout;
use crate::clients::WorkItem;
use crate::net::Connector;
use crate::workload::ClientRequest;
use crate::*;
use async_channel::Receiver;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use chrono::DateTime;
use chrono::Utc;
use h2::client::SendRequest;
use http::uri::Authority;
use http::HeaderName;
use http::HeaderValue;
use http::Method;
use http::Uri;
use http::Version;
use session::Buffer;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Write;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

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

async fn resolve(uri: &str) -> Result<(std::net::SocketAddr, Authority), std::io::Error> {
    let uri = uri
        .parse::<http::Uri>()
        .map_err(|_| Error::new(ErrorKind::Other, "failed to parse uri"))?;

    let auth = uri
        .authority()
        .ok_or(Error::new(ErrorKind::Other, "uri has no authority"))?
        .clone();

    let port = auth.port_u16().unwrap_or(443);

    let addr = tokio::net::lookup_host((auth.host(), port))
        .await?
        .next()
        .ok_or(Error::new(ErrorKind::Other, "dns found no addresses"))?;

    Ok((addr, auth))
}

pub async fn pool_manager(endpoint: String, config: Config, queue: Queue<SendRequest<Bytes>>) {
    let mut client = None;

    // let connector = Connector::new(&config).expect("failed to init connector");
    // let mut sender = None;

    while RUNNING.load(Ordering::Relaxed) {
        if client.is_none() {
            CONNECT.increment();

            if let Ok((addr, _auth)) = resolve(&endpoint).await {
                if let Ok(tcp) = TcpStream::connect(addr).await {
                    if tcp.set_nodelay(true).is_err() {
                        continue;
                    }

                    if let Ok((h2, connection)) = ::h2::client::handshake(tcp).await {
                        tokio::spawn(async move {
                            connection.await.unwrap();
                        });

                        if let Ok(h2) = h2.ready().await {
                            client = Some(h2);
                        }
                    }
                }
            }
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
    work_receiver: Receiver<WorkItem>,
    endpoint: String,
    config: Config,
    queue: Queue<SendRequest<Bytes>>,
) -> Result<(), std::io::Error> {
    // let mut buffer = Buffer::new(16384);
    // let parser = protocol_ping::ResponseParser::new();
    // let mut sender = None;

    let uri = endpoint
        .parse::<http::Uri>()
        .map_err(|_| Error::new(ErrorKind::Other, "failed to parse uri"))?;

    let auth = uri
        .authority()
        .ok_or(Error::new(ErrorKind::Other, "uri has no authority"))?
        .clone();

    let port = auth.port_u16().unwrap_or(443);

    while RUNNING.load(Ordering::Relaxed) {
        let sender = queue.recv().await;

        if sender.is_err() {
            continue;
        }

        let mut sender = sender.unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();

        // compose request into buffer
        match &work_item {
            WorkItem::Request { request, .. } => match request {
                ClientRequest::Ping(_) => {}
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                    continue;
                }
            },
            WorkItem::Reconnect => {
                REQUEST_UNSUPPORTED.increment();
                continue;
            }
        };

        let now: DateTime<Utc> = Utc::now();

        let request = http::request::Builder::new()
            .version(Version::HTTP_2)
            .method(Method::POST)
            .uri(&format!("http://192.168.1.205:12321/pingpong.Ping/Ping"))
            .header("content-type", "application/grpc")
            .header("date", now.to_rfc2822())
            .header("user-agent", "unknown/0.0.0")
            .header("te", "trailers")
            .body(())
            .unwrap();

        let start = Instant::now();

        if let Ok((response, mut stream)) = sender.send_request(request, false) {
            if stream
                .send_data(Bytes::from(vec![0, 0, 0, 0, 0]), true)
                .is_err()
            {
                // REQUEST_EX.increment();
                continue;
            } else {
                REQUEST_OK.increment();
            }

            if let Ok(_response) = response.await {
                let stop = Instant::now();

                RESPONSE_OK.increment();
                PING_OK.increment();

                let latency = stop.duration_since(start).as_nanos() as u64;
                let _ = RESPONSE_LATENCY.increment(latency);
            } else {
                RESPONSE_EX.increment();
            }
        }
    }

    Ok(())
}
