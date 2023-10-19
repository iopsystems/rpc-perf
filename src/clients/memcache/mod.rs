use super::*;
use crate::net::Connector;
use protocol_memcache::{Compose, Parse, Request, Response, Ttl};
use session::{Buf, BufMut, Buffer};
use std::borrow::{Borrow, BorrowMut};

mod commands;

struct RequestWithValidator {
    request: Request,
    validator: Box<dyn Fn(Response) -> std::result::Result<(), ()> + Send>,
}

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

    // we would not be creating a memcache client task if we didn't have a
    // client config, so this unwrap will succeed.
    let client_config = config.client().unwrap();

    let mut stream = None;
    let parser = protocol_memcache::ResponseParser {};
    let mut read_buffer = Buffer::new(client_config.read_buffer_size());
    let mut write_buffer = Buffer::new(client_config.write_buffer_size());

    while RUNNING.load(Ordering::Relaxed) {
        if stream.is_none() {
            CONNECT.increment();
            stream = match timeout(
                client_config.connect_timeout(),
                connector.connect(&endpoint),
            )
            .await
            {
                Ok(Ok(s)) => {
                    CONNECT_OK.increment();
                    CONNECT_CURR.increment();
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
            CONNECT_CURR.decrement();
            continue;
        }

        let request = RequestWithValidator::try_from(&work_item);

        // skip unsupported work items
        if request.is_err() {
            stream = Some(s);
            continue;
        }

        let request = request.unwrap();

        // compose request
        REQUEST_OK.increment();
        request.request.compose(&mut write_buffer);

        // send request
        let start = Instant::now();
        s.write_all(write_buffer.borrow()).await?;

        // clear the buffers
        write_buffer.clear();
        read_buffer.clear();

        // read until response or timeout
        let response = loop {
            let remaining_time = client_config
                .request_timeout()
                .as_millis()
                .saturating_sub(start.elapsed().as_millis());
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
                let latency_ns = stop.duration_since(start).as_nanos() as u64;

                // check if the response is valid
                if (request.validator)(response).is_err() {
                    // increment error stats, connection will be dropped
                    RESPONSE_EX.increment();
                    CONNECT_CURR.increment();
                } else {
                    // increment success stats and latency
                    RESPONSE_OK.increment();

                    let _ = RESPONSE_LATENCY.increment(latency_ns);

                    // preserve the connection for the next request
                    stream = Some(s);
                }
            }
            Err(ResponseError::Exception) => {
                // use validate response to record the exception
                let _ = (request.validator)(Response::error());

                // increment error stats and allow connection to be dropped
                RESPONSE_EX.increment();
                CONNECT_CURR.decrement();
            }
            Err(ResponseError::Timeout) => {
                // increment error stats and allow connection to be dropped
                RESPONSE_TIMEOUT.increment();
                CONNECT_CURR.decrement();
            }
            Err(ResponseError::Ratelimited) | Err(ResponseError::BackendTimeout) => {
                unimplemented!();
            }
        }
    }

    Ok(())
}

impl From<&workload::client::Delete> for Request {
    fn from(other: &workload::client::Delete) -> Self {
        DELETE.increment();
        Request::delete((*other.key).to_owned().into_boxed_slice(), false)
    }
}

impl TryFrom<&WorkItem> for RequestWithValidator {
    type Error = ();
    fn try_from(other: &WorkItem) -> std::result::Result<RequestWithValidator, ()> {
        match other {
            WorkItem::Request { request, .. } => match request {
                ClientRequest::Add(r) => Ok(Self::from(r)),
                ClientRequest::Get(r) => Ok(Self::from(r)),
                ClientRequest::Delete(r) => Ok(Self::from(r)),
                ClientRequest::Replace(r) => Ok(Self::from(r)),
                ClientRequest::Set(r) => Ok(Self::from(r)),
                _ => Err(()),
            },
            _ => Err(()),
        }
    }
}
