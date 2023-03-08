// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use protocol_ping::Compose;
use protocol_ping::{Parse, Request, Response};
use session::Buf;
use session::BufMut;
use session::Buffer;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;

/// Launch tasks with one conncetion per task as ping protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching ping protocol tasks");

    let endpoints: Vec<SocketAddr> = config
        .target()
        .endpoints()
        .iter()
        .map(|e| {
            e.to_socket_addrs()
                .expect("bad endpoint")
                .next()
                .expect("lookup failed")
        })
        .collect();
    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..(config.connection().poolsize() * config.general().threads()) {
        for endpoint in &endpoints {
            runtime.spawn(task(work_receiver.clone(), *endpoint, config.clone()));
        }
    }
}

// a task for ping servers (eg: Pelikan Pingserver)
#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<WorkItem>,
    endpoint: SocketAddr,
    config: Config,
) -> Result<()> {
    let mut stream = None;
    let parser = protocol_ping::ResponseParser::new();
    let mut read_buffer = Buffer::new(4096);
    let mut write_buffer = Buffer::new(4096);

    while RUNNING.load(Ordering::Relaxed) {
        if stream.is_none() {
            CONNECT.increment();
            stream =
                match timeout(config.connection().timeout(), TcpStream::connect(endpoint)).await {
                    Ok(Ok(s)) => Some(s),
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

        let mut s = stream.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        let start = Instant::now();

        // compose request into buffer
        match work_item {
            WorkItem::Ping => {
                Request::Ping.compose(&mut write_buffer);
            }
            WorkItem::Reconnect => {
                REQUEST_RECONNECT.increment();
                continue;
            }
            _ => {
                REQUEST_UNSUPPORTED.increment();
                stream = Some(s);
                continue;
            }
        }

        REQUEST_OK.increment();

        // println!("wrote: {} bytes to buffer", write_buffer.remaining());

        // send request
        s.write_all(write_buffer.borrow()).await?;
        write_buffer.clear();

        // println!("request sent");

        // read until response or timeout
        let mut remaining_time = config.request().timeout().as_nanos() as u64;
        let response = loop {
            match timeout(
                Duration::from_millis(remaining_time / 1000000),
                s.read(read_buffer.borrow_mut()),
            )
            .await
            {
                Ok(Ok(n)) => {
                    unsafe {
                        read_buffer.advance_mut(n);
                    }
                    match parser.parse(read_buffer.borrow()) {
                        Ok(resp) => {
                            let consumed = resp.consumed();
                            let resp = resp.into_inner();

                            read_buffer.advance(consumed);

                            break Ok(resp);
                        }
                        Err(e) => match e.kind() {
                            ErrorKind::WouldBlock => {
                                let elapsed = start.elapsed().as_nanos();
                                remaining_time = remaining_time.saturating_sub(elapsed);
                                if remaining_time == 0 {
                                    break Err(ResponseError::Timeout);
                                }
                            }
                            _ => {
                                break Err(ResponseError::Exception);
                            }
                        },
                    }
                }
                Ok(Err(_)) => {
                    break Err(ResponseError::Exception);
                }
                Err(_) => {
                    break Err(ResponseError::Timeout);
                }
            }
        };

        let stop = Instant::now();

        match response {
            Ok(response) => {
                // validate response
                match work_item {
                    WorkItem::Ping => match response {
                        Response::Pong => {
                            PING_OK.increment();
                        }
                    },
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }

                stream = Some(s);

                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Err(ResponseError::Exception) => {
                // record execption
                match work_item {
                    WorkItem::Ping => {
                        error!("ping exception");
                        PING_EX.increment();
                    }
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }
            }
            Err(ResponseError::Timeout) => {
                error!("timeout");
                RESPONSE_TIMEOUT.increment();
            }
            Err(ResponseError::Ratelimited) => {
                RESPONSE_RATELIMITED.increment();
                stream = Some(s);
            }
            Err(ResponseError::BackendTimeout) => {
                RESPONSE_BACKEND_TIMEOUT.increment();
                stream = Some(s);
            }
        }

        // info!("next");
    }

    Ok(())
}