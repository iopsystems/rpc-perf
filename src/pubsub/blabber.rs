use super::*;
use crate::net::Connector;
use bytes::Buf;
use bytes::BufMut;
use session::Buffer;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use tokio::io::AsyncReadExt;

use tokio::time::timeout;

// blabber has a header before the standard pubsub message
//
//  ___________________________________
// | 0 ..   | 4 ..    | 8 .. length    |
// |        |         |                |
// | length | padding | pubsub message |
// |________|_________|________________|

const HEADER_LEN: u32 = 8;

/// Launch tasks with one conncetion per task as ping protocol is not mux-enabled.
pub fn launch_subscribers(
    runtime: &mut Runtime,
    config: Config,
    workload_components: &[Component],
) {
    debug!("launching blabber subscriber tasks");

    for component in workload_components {
        if let Component::Topics(topics) = component {
            let connections = topics.subscriber_poolsize() * topics.subscriber_concurrency();

            // create one task per "connection"
            // note: these may be channels instead of connections for multiplexed protocols
            for _ in 0..connections {
                for endpoint in config.target().endpoints() {
                    runtime.spawn(subscriber_task(endpoint.clone(), config.clone()));
                }
            }
        }
    }
}

// a task for blabber servers (eg: Pelikan Blabber)
#[allow(clippy::slow_vector_initialization)]
async fn subscriber_task(endpoint: String, config: Config) -> Result<()> {
    let validator = MessageValidator::new();

    let connector = Connector::new(&config)?;

    // this unwrap will succeed because we wouldn't be creating these tasks if
    // there wasn't a client config.
    let pubsub_config = config.pubsub().unwrap();

    let mut stream = None;
    let mut read_buffer = Buffer::new(pubsub_config.read_buffer_size());

    while RUNNING.load(Ordering::Relaxed) {
        if stream.is_none() {
            CONNECT.increment();
            PUBSUB_SUBSCRIBE.increment();

            stream = match timeout(
                pubsub_config.connect_timeout(),
                connector.connect(&endpoint),
            )
            .await
            {
                Ok(Ok(s)) => {
                    CONNECT_OK.increment();
                    CONNECT_CURR.increment();
                    PUBSUB_SUBSCRIBER_CURR.add(1);
                    PUBSUB_SUBSCRIBE_OK.increment();

                    Some(s)
                }
                Ok(Err(_)) => {
                    CONNECT_EX.increment();
                    PUBSUB_SUBSCRIBE_EX.increment();

                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(_) => {
                    CONNECT_TIMEOUT.increment();
                    PUBSUB_SUBSCRIBE_EX.increment();

                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }

        let mut s = stream.take().unwrap();

        // read until response or timeout
        loop {
            match s.read(read_buffer.borrow_mut()).await {
                Ok(n) => {
                    unsafe {
                        read_buffer.advance_mut(n);
                    }
                    {
                        loop {
                            let consumed = {
                                let rbuf: &[u8] = read_buffer.borrow();

                                if rbuf.len() >= HEADER_LEN as usize {
                                    let len =
                                        u32::from_be_bytes(rbuf[0..4].try_into().unwrap()) as usize;

                                    // check if we have only a partial message
                                    if rbuf.len() < len {
                                        break;
                                    }

                                    let mut mbuf = rbuf[8..len].to_owned();

                                    let _ = validator.validate(&mut mbuf);

                                    // return the number of bytes consumed
                                    len
                                } else {
                                    break;
                                }
                            };

                            read_buffer.advance(consumed);
                        }
                    }
                }
                Err(_) => {
                    PUBSUB_RECEIVE.increment();
                    PUBSUB_RECEIVE_EX.increment();
                    PUBSUB_SUBSCRIBER_CURR.sub(1);
                }
            }
        }
    }

    Ok(())
}

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_publishers(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching blabber publisher tasks");

    for _ in 0..config.pubsub().unwrap().publisher_poolsize() {
        PUBSUB_PUBLISHER_CONNECT.increment();

        // create one task per channel
        for _ in 0..config.pubsub().unwrap().publisher_concurrency() {
            runtime.spawn(publisher_task(work_receiver.clone()));
        }
    }
}

async fn publisher_task(work_receiver: Receiver<WorkItem>) -> Result<()> {
    PUBSUB_PUBLISHER_CURR.add(1);

    while RUNNING.load(Ordering::Relaxed) {
        let _work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;
    }

    PUBSUB_PUBLISHER_CURR.sub(1);

    Ok(())
}
