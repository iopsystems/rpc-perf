// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;
use crate::net::Connector;

use protocol_memcache::{Compose, Parse, Request, Response, Ttl};
use session::{Buf, BufMut, Buffer};
use std::borrow::{Borrow, BorrowMut};

/// Launch tasks with one conncetion per task as memcache protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching memcache protocol tasks");

    // create one task per connection
    for _ in 0..config.client().unwrap().poolsize() {
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
async fn task(work_receiver: Receiver<WorkItem>, endpoint: String, config: Config) -> Result<()> {
    let connector = Connector::new(&config)?;

    let mut stream = None;
    let parser = protocol_memcache::ResponseParser {};
    let mut read_buffer = Buffer::new(4096);
    let mut write_buffer = Buffer::new(4096);

    while RUNNING.load(Ordering::Relaxed) {
        if stream.is_none() {
            CONNECT.increment();
            stream = match timeout(
                config.client().unwrap().connect_timeout(),
                connector.connect(&endpoint),
            )
            .await
            {
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

        // send request
        let start = Instant::now();
        s.write_all(write_buffer.borrow()).await?;

        // clear the buffers
        write_buffer.clear();
        read_buffer.clear();

        // read until response or timeout
        let response = loop {
            let remaining_time = config
                .client()
                .unwrap()
                .request_timeout()
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
                    CONNECT_CURR.sub(1);

                    continue;
                }

                stream = Some(s);

                RESPONSE_OK.increment();

                REQUEST_LATENCY.increment(start, latency_ns, 1);
                RESPONSE_LATENCY.increment(stop, latency_ns, 1);
            }
            Err(ResponseError::Exception) => {
                // use validate response to record the exception
                let _ = validate_response(&work_item, &Response::error());

                RESPONSE_EX.increment();
                CONNECT_CURR.sub(1);
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
            WorkItem::Request { request, .. } => match request {
                ClientRequest::Add { key, value } => {
                    ADD.increment();
                    Ok(Request::add(
                        (**key).to_owned().into_boxed_slice(),
                        (**value).to_owned().into_boxed_slice(),
                        0,
                        Ttl::none(),
                        false,
                    ))
                }
                ClientRequest::Get { key } => {
                    GET.increment();
                    Ok(Request::get(
                        vec![(**key).to_owned().into_boxed_slice()].into_boxed_slice(),
                    ))
                }
                ClientRequest::Delete { key } => {
                    DELETE.increment();
                    Ok(Request::delete(
                        (**key).to_owned().into_boxed_slice(),
                        false,
                    ))
                }
                ClientRequest::Replace { key, value } => {
                    REPLACE.increment();
                    Ok(Request::replace(
                        (**key).to_owned().into_boxed_slice(),
                        (**value).to_owned().into_boxed_slice(),
                        0,
                        Ttl::none(),
                        false,
                    ))
                }
                ClientRequest::Set { key, value } => {
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
            },
            _ => Err(()),
        }
    }
}

fn validate_response(work_item: &WorkItem, response: &Response) -> std::result::Result<(), ()> {
    // validate response
    match &work_item {
        WorkItem::Request { request, .. } => match request {
            ClientRequest::Get { .. } => match response {
                Response::Values(values) => {
                    if values.values().is_empty() {
                        RESPONSE_MISS.increment();
                        GET_KEY_MISS.increment();
                    } else {
                        RESPONSE_HIT.increment();
                        GET_KEY_HIT.increment();
                    }
                }
                _ => {
                    GET_EX.increment();
                    return Err(());
                }
            },
            ClientRequest::Replace { .. } => match response {
                Response::Stored(_) => {
                    REPLACE_STORED.increment();
                }
                Response::NotStored(_) => {
                    REPLACE_NOT_STORED.increment();
                }
                _ => {
                    REPLACE_EX.increment();
                    return Err(());
                }
            },
            ClientRequest::Set { .. } => match response {
                Response::Stored(_) => {
                    SET_STORED.increment();
                }
                Response::NotStored(_) => {
                    SET_NOT_STORED.increment();
                }
                _ => {
                    SET_EX.increment();
                    return Err(());
                }
            },
            _ => {
                error!("unexpected request");
                unimplemented!();
            }
        },
        _ => {
            error!("unexpected work item");
            unimplemented!();
        }
    }

    Ok(())
}
