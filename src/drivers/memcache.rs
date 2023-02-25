// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

// use tokio::runtime::Runtime;
// use super::Error;
use super::*;
use protocol_memcache::Ttl;

use protocol_memcache::{Compose, Parse, Request, Response};
use session::{Buf, BufMut, Buffer};
use std::borrow::{Borrow, BorrowMut};
use std::net::{SocketAddr, ToSocketAddrs};

/// Launch tasks with one conncetion per task as memcache protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching memcache protocol tasks");

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

    // create one task per connection
    for _ in 0..(config.connection().poolsize() * config.general().threads()) {
        for endpoint in &endpoints {
            runtime.spawn(task(work_receiver.clone(), *endpoint, config.clone()));
        }
    }
}

#[allow(clippy::slow_vector_initialization)]
async fn task(
    work_receiver: Receiver<WorkItem>,
    endpoint: SocketAddr,
    config: Config,
) -> Result<()> {
    let mut stream = None;
    let parser = protocol_memcache::ResponseParser {};
    let mut read_buffer = Buffer::new(4096);
    let mut write_buffer = Buffer::new(4096);

    while RUNNING.load(Ordering::Relaxed) {
        if stream.is_none() {
            CONNECT.increment();
            stream =
                match timeout(config.connection().timeout(), TcpStream::connect(endpoint)).await {
                    Ok(Ok(s)) => {
                        CONNECT_OK.increment();
                        CONNECT_CURR.add(1);
                        Some(s)
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

        let mut s = stream.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();


        // compose request into buffer
        let request = match &work_item {
            WorkItem::Add { key, value } => {
                ADD.increment();
                Request::add(
                    (**key).to_owned().into_boxed_slice(),
                    (**value).to_owned().into_boxed_slice(),
                    0,
                    Ttl::none(),
                    false,
                )
            }
            WorkItem::Get { key } => {
                GET.increment();
                Request::get(vec![(**key).to_owned().into_boxed_slice()].into_boxed_slice())
            }
            WorkItem::Delete { key } => {
                DELETE.increment();
                Request::delete((**key).to_owned().into_boxed_slice(), false)
            }
            WorkItem::Replace { key, value } => {
                REPLACE.increment();
                Request::replace(
                    (**key).to_owned().into_boxed_slice(),
                    (**value).to_owned().into_boxed_slice(),
                    0,
                    Ttl::none(),
                    false,
                )
            }
            WorkItem::Set { key, value } => {
                SET.increment();
                Request::set(
                    (**key).to_owned().into_boxed_slice(),
                    (**value).to_owned().into_boxed_slice(),
                    0,
                    Ttl::none(),
                    false,
                )
            }
            WorkItem::Reconnect => {
                CONNECT_CURR.sub(1);
                continue;
            }
            _ => {
                stream = Some(s);
                continue;
            }
        };
        
        REQUEST_OK.increment();
        request.compose(&mut write_buffer);

        let mut start: Instant;

        // send request
        loop {
            s.writable().await?;
            start = Instant::now();

            match s.try_write(write_buffer.borrow()) {
                Ok(n) => {
                    write_buffer.advance(n);
                    if write_buffer.remaining() == 0 {
                        break;
                    } else {
                        continue;
                    }
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        // s.write_all(write_buffer.borrow()).await?;
        write_buffer.clear();
        read_buffer.clear();

        // read until response or timeout
        let response = loop {
            let remaining_time = 200_u64.saturating_sub(start.elapsed().as_millis());
            if remaining_time == 0 {
                break Err(ResponseError::Timeout);
            }

            match timeout(
                Duration::from_millis(remaining_time),
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
                            ErrorKind::WouldBlock => { }
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
                match &work_item {
                    WorkItem::Get { .. } => match response {
                        Response::Values(values) => {
                            if values.values().is_empty() {
                                GET_KEY_MISS.increment();
                            } else {
                                GET_KEY_HIT.increment();
                            }
                        }
                        _ => {
                            GET_EX.increment();
                            RESPONSE_EX.increment();
                            continue;
                        }
                    },
                    WorkItem::Replace { .. } => match response {
                        Response::Stored(_) => {
                            REPLACE_STORED.increment();
                        }
                        Response::NotStored(_) => {
                            REPLACE_NOT_STORED.increment();
                        }
                        _ => {
                            REPLACE_EX.increment();
                            RESPONSE_EX.increment();
                            continue;
                        }
                    },
                    WorkItem::Set { .. } => match response {
                        Response::Stored(_) => {
                            SET_STORED.increment();
                        }
                        _ => {
                            SET_EX.increment();
                            RESPONSE_EX.increment();
                            continue;
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
                match &work_item {
                    WorkItem::Ping => {
                        error!("ping exception");
                        PING_EX.increment();
                    }
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }
                CONNECT_CURR.sub(1);
                RESPONSE_EX.increment();
            }
            Err(ResponseError::Timeout) => {
                // println!("timeout for: {:?}", work_item);
                // error!("timeout");
                RESPONSE_TIMEOUT.increment();
                CONNECT_CURR.sub(1);
            }
        }
    }

    Ok(())
}
