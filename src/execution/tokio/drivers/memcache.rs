// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use protocol_memcache::*;
use session::BufMut;
use session::Buf;
use std::borrow::BorrowMut;
use std::borrow::Borrow;
use session::Buffer;
use protocol_memcache::{Compose, Parse, Request, Response};
use super::*;
use super::Error;

/// Launch tasks with one conncetion per task as memcache protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, poolsize: usize, work_receiver: Receiver<WorkItem>) {
    // create one task per connection
    for _ in 0..poolsize {
        runtime.spawn(task(work_receiver.clone()));
    }
}

#[allow(clippy::slow_vector_initialization)]
async fn task(work_receiver: Receiver<WorkItem>) -> Result<()> {
    let mut stream = None;
    let parser = protocol_memcache::ResponseParser {};
    let mut read_buffer = Buffer::new(4096);
    let mut write_buffer = Buffer::new(4096);

    while RUNNING.load(Ordering::Relaxed) {
        if stream.is_none() {
            CONNECT.increment();
            stream = Some(TcpStream::connect("127.0.0.1:12321").await?);
        }

        // println!("have connection, getting work");

        let mut s = stream.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        // println!("got work, composing request");

        let start = Instant::now();
        
        // compose request into buffer
        let request = match &work_item {
            WorkItem::Get { key } => {
                GET.increment();
                Request::get(vec![(**key).to_owned().into_boxed_slice()].into_boxed_slice())
            }
            WorkItem::Replace { key, value } => {
                Request::replace(key.as_bytes().to_owned().into_boxed_slice(), value.as_bytes().to_owned().into_boxed_slice(), 0, Ttl::none(), false)
            }
            WorkItem::Set { key, value } => {
                Request::set((**key).to_owned().into_boxed_slice(), (**value).to_owned().into_boxed_slice(), 0, Ttl::none(), false)
            }
            _ => {
                continue;
            }
        };
        request.compose(&mut write_buffer);

        // println!("wrote: {} bytes to buffer", write_buffer.remaining());

        // send request
        s.write_all(write_buffer.borrow()).await?;
        write_buffer.clear();

        // println!("request sent");

        // read until response or timeout
        let mut remaining_time = 200_000_000;
        let response = loop {
            match timeout(Duration::from_millis(remaining_time / 1000000), s.read(read_buffer.borrow_mut())).await {
                Ok(Ok(n)) => {
                    unsafe { read_buffer.advance_mut(n); }
                    match parser.parse(read_buffer.borrow()) {
                        Ok(resp) => {
                            let consumed = resp.consumed();
                            let resp = resp.into_inner();

                            read_buffer.advance(consumed);

                            break Ok(Ok(resp));
                        }
                        Err(e) => match e.kind() {
                            ErrorKind::WouldBlock => {
                                let elapsed = start.elapsed().as_nanos();
                                remaining_time = remaining_time.saturating_sub(elapsed);
                                if remaining_time == 0 {
                                    break Err(());
                                }
                            },
                            _ => {
                                break Ok(Err(()));
                            }
                        }
                    }
                }
                Ok(Err(_)) => {
                    break Ok(Err(()));
                }
                Err(_) => {
                    break Err(());
                }
            }
        };

        let stop = Instant::now();

        match response {
            Ok(Ok(response)) => {
                // validate response
                match &work_item {
                    WorkItem::Get { .. }=> {
                        match response {
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
                        }
                    }
                    WorkItem::Replace { .. }=> {
                        match response {
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
                        }
                    }
                    WorkItem::Set { .. }=> {
                        match response {
                            Response::Stored(_) => {
                                SET_STORED.increment();
                            }
                            _ => {
                                SET_EX.increment();
                                RESPONSE_EX.increment();
                                continue;
                            }
                        }
                    }
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }

                stream = Some(s);

                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Ok(Err(_)) => {
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
            }
            Err(_) => {
                error!("timeout");
                RESPONSE_TIMEOUT.increment();
            }
        }

        // info!("next");
    }

    Ok(())
}