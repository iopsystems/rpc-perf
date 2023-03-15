// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;

use protocol_memcache::{Compose, Parse, Request, Response, Ttl};
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
    for _ in 0..config.connection().poolsize() {
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

        // check if we should reconnect
        if work_item == WorkItem::Reconnect {
            CONNECT_CURR.sub(1);
            continue;
        }

        let request = Request::try_from(&work_item);

        // skip unsupported work items
        if request.is_err() {
            stream = Some(s);
            continue;
        }

        // compose request
        REQUEST_OK.increment();
        request.unwrap().compose(&mut write_buffer);

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

        // clear the buffers
        write_buffer.clear();
        read_buffer.clear();

        // read until response or timeout
        let response = loop {
            let remaining_time = config
                .request()
                .timeout()
                .as_millis()
                .saturating_sub(start.elapsed().as_millis().into());
            if remaining_time == 0 {
                break Err(ResponseError::Timeout);
            }

            match timeout(
                Duration::from_millis(remaining_time as _),
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
                            ErrorKind::WouldBlock => {}
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
                let latency_ns = stop.duration_since(start).as_nanos();

                if validate_response(&work_item, &response).is_err() {
                    RESPONSE_EX.increment();

                    continue;
                }

                stream = Some(s);

                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, latency_ns, 1);
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
                RESPONSE_TIMEOUT.increment();
                CONNECT_CURR.sub(1);
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
    }

    Ok(())
}

impl TryFrom<&WorkItem> for Request {
    type Error = ();
    fn try_from(other: &WorkItem) -> std::result::Result<protocol_memcache::Request, ()> {
        match other {
            WorkItem::Add { key, value } => {
                ADD.increment();
                Ok(Request::add(
                    (**key).to_owned().into_boxed_slice(),
                    (**value).to_owned().into_boxed_slice(),
                    0,
                    Ttl::none(),
                    false,
                ))
            }
            WorkItem::Get { key } => {
                GET.increment();
                Ok(Request::get(
                    vec![(**key).to_owned().into_boxed_slice()].into_boxed_slice(),
                ))
            }
            WorkItem::Delete { key } => {
                DELETE.increment();
                Ok(Request::delete(
                    (**key).to_owned().into_boxed_slice(),
                    false,
                ))
            }
            WorkItem::Replace { key, value } => {
                REPLACE.increment();
                Ok(Request::replace(
                    (**key).to_owned().into_boxed_slice(),
                    (**value).to_owned().into_boxed_slice(),
                    0,
                    Ttl::none(),
                    false,
                ))
            }
            WorkItem::Set { key, value } => {
                SET.increment();
                Ok(Request::set(
                    (**key).to_owned().into_boxed_slice(),
                    (**value).to_owned().into_boxed_slice(),
                    0,
                    Ttl::none(),
                    false,
                ))
            }
            _ => Err(()),
        }
    }
}

fn validate_response(work_item: &WorkItem, response: &Response) -> std::result::Result<(), ()> {
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

                return Err(());
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
                return Err(());
            }
        },
        WorkItem::Set { .. } => match response {
            Response::Stored(_) => {
                SET_STORED.increment();
            }
            _ => {
                SET_EX.increment();
                RESPONSE_EX.increment();
                return Err(());
            }
        },
        _ => {
            error!("unexpected work item");
            unimplemented!();
        }
    }

    Ok(())
}
