// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;
use crate::net::Connector;

use hyper::{Body, Request, StatusCode};
use hyper::client::conn::http1;

/// Launch tasks with one conncetion per task as ping protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching ping protocol tasks");

    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..config.connection().poolsize() {
        for endpoint in config.target().endpoints() {
            runtime.spawn(task(
                work_receiver.clone(),
                endpoint.clone(),
                config.clone(),
            ));
        }
    }
}

// a task for HTTP/1.1
#[allow(clippy::slow_vector_initialization)]
async fn task(work_receiver: Receiver<WorkItem>, endpoint: String, config: Config) -> Result<()> {
    let connector = Connector::new(&config)?;


    let mut sender = None;
    // let mut connection = None;

    while RUNNING.load(Ordering::Relaxed) {
        if sender.is_none() {
            info!("connecting");
            CONNECT.increment();
            match timeout(config.connection().timeout(), connector.connect(&endpoint)).await {
                Ok(Ok(s)) => {
                    if let Ok((s, c)) = http1::handshake(s).await {
                        CONNECT_OK.increment();
                        CONNECT_CURR.add(1);
                        sender = Some(s);
                        // connection = Some(c);
                        tokio::spawn(async move {
                            if let Err(e) = c.await {
                                eprintln!("Error in connection: {}", e);
                            }
                        });
                    } else {
                        CONNECT_EX.increment();
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    
                    // stream = Some(s);
                }
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
            }
        }

        let mut s = sender.take().unwrap();
        

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        let start = Instant::now();

        info!("build request");

        // compose request into buffer
        let request = match work_item {
            WorkItem::Get { .. } => {
                Request::builder()
                    .header("Host", "example.com")
                    .method("GET")
                    .body(Body::from("")).unwrap()
            }
            // WorkItem::Ping => {
            //     Request::Ping.compose(&mut write_buffer);
            // }
            WorkItem::Reconnect => {
                REQUEST_RECONNECT.increment();
                continue;
            }
            _ => {
                REQUEST_UNSUPPORTED.increment();
                sender = Some(s);
                continue;
            }
        };

        REQUEST_OK.increment();

        info!("send request");

        // send request
        let response = s.send_request(request).await;

        let stop = Instant::now();

        info!("process response");

        match response {
            Ok(response) => {
                // validate response
                match work_item {
                    WorkItem::Get { .. } => match response.status() {
                        StatusCode::OK => {
                            GET_OK.increment();
                        }
                        _ => {
                            GET_EX.increment();
                        }
                    },
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }

                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);

                if s.ready().await.is_err() {
                    continue;
                }

                info!("ready");

                sender = Some(s);
            }
            Err(_e) => {
                // record execption
                match work_item {
                    WorkItem::Get { .. } => {
                        GET_EX.increment();
                    }
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }
            }
            // Err(_) => {
            //     error!("timeout");
            //     RESPONSE_TIMEOUT.increment();
            // }
        }


    }

    Ok(())
}
