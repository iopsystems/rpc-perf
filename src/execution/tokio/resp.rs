use redis::AsyncCommands;
use crate::Instant;
use std::borrow::Borrow;
// use redis::Commands;
use super::*;

pub fn launch_resp_tasks(runtime: &mut Runtime, work_receiver: Receiver<WorkItem>) {
    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..CONNECTIONS {
        runtime.spawn(resp_task(work_receiver.clone()));
    }
}

// a task for RESP compatible servers (eg: Redis)
#[allow(dead_code)]
#[allow(clippy::slow_vector_initialization)]
pub async fn resp_task(work_receiver: Receiver<WorkItem>) -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1/").map_err(|_| Error::new(ErrorKind::Other, "failed to create redis client"))?;
    let mut connection = client.get_async_connection().await.map(Some).unwrap_or(None);

    while RUNNING.load(Ordering::Relaxed) {
        if connection.is_none() {
            connection = client.get_async_connection().await.map(Some).unwrap_or(None);
            continue;
        }

        let mut con = connection.take().unwrap();
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        let start = Instant::now();
        let result = match work_item {
            WorkItem::Get { key } => {
                con.get::<&String, Vec<u8>>(key.as_ref()).await.map(|_| ())
            }
            WorkItem::Set { key, value } => {
                con.set::<&String, Vec<u8>, ()>(key.as_ref(), value.into()).await
            }
            WorkItem::HashGet { key, field } => {
                con.hget::<&String, &String, Vec<u8>>(key.as_ref(), field.as_ref()).await.map(|_| ())
            }
            WorkItem::HashSet { key, field, value } => {
                con.hset::<&String, &String, Vec<u8>, Vec<u8>>(key.as_ref(), field.as_ref(), value.into()).await.map(|_| ())
            }
            WorkItem::HashDelete { key, fields } => {
                let fields: Vec<&String> = fields.iter().map(|v| v.borrow()).collect();
                con.hdel::<&String, Vec<&String>, Vec<u8>>(key.as_ref(), fields).await.map(|_| ())
            }
            WorkItem::HashMultiGet { key, fields } => {
                let fields: Vec<&String> = fields.iter().map(|v| v.borrow()).collect();
                con.hget::<&String, Vec<&String>, Vec<u8>>(key.as_ref(), fields).await.map(|_| ())
            }
            WorkItem::Ping { .. } => {
                redis::cmd("PING").query_async(&mut con).await
            }
            // _ => {
            //     continue;
            // }
        };

        let stop = Instant::now();

        RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);

        if result.is_ok() {
            RESPONSE_OK.increment();
            connection = Some(con);
        } else {
            RESPONSE_EX.increment();
        }
    }

    // let mut stream = None;

    // let mut buf = Vec::with_capacity(4096);
    // buf.resize(4096, 0);

    // while RUNNING.load(Ordering::Relaxed) {
    //     if stream.is_none() {
    //         CONNECT.increment();
    //         // stream = timeout(Duration::from_millis(200), TcpStream::connect("127.0.0.1:6379")).await?.map(Some).unwrap_or(None);
    //         stream = Some(TcpStream::connect("127.0.0.1:6379").await?);
    //     }

    //     let mut s = stream.take().unwrap();

    //     let work_item = work_receiver
    //         .recv()
    //         .await
    //         .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

    //     let result = match work_item {
    //         WorkItem::Get { key } => {
    //             GET.increment();
    //             unsafe {
    //                 buf.set_len(0);
    //             }
    //             buf.extend_from_slice(format!("GET {}\r\n", key).as_bytes());

    //             s.write_all(&buf).await?;

    //             unsafe {
    //                 buf.set_len(0);
    //             }

    //             match timeout(Duration::from_millis(200), s.read(&mut buf)).await {
    //                 Ok(Ok(_)) => {
    //                     stream = Some(s);

    //                     Ok(true)
    //                 }
    //                 Ok(Err(_)) => Ok(false),
    //                 Err(e) => Err(e),
    //             }
    //         }
    //         WorkItem::Set { key, value } => {
    //             SET.increment();
    //             unsafe {
    //                 buf.set_len(0);
    //             }
    //             buf.extend_from_slice(format!("SET {} {}\r\n", key, value).as_bytes());

    //             s.write_all(&buf).await?;

    //             unsafe {
    //                 buf.set_len(0);
    //             }

    //             match timeout(Duration::from_millis(200), s.read(&mut buf)).await {
    //                 Ok(Ok(_)) => {
    //                     stream = Some(s);

    //                     Ok(true)
    //                 }
    //                 Ok(Err(_)) => Ok(false),
    //                 Err(e) => Err(e),
    //             }
    //         }
    //         WorkItem::Ping => {
    //             // TODO: this could be implemented as the version command
    //             continue;
    //         }
    //         _ => {
    //             continue;
    //         }
    //     };

    //     if let Ok(ok) = result {
    //         if ok {
    //             RESPONSE_OK.increment();
    //         } else {
    //             RESPONSE_EX.increment();
    //         }
    //     } else {
    //         RESPONSE_TIMEOUT.increment();
    //     }
    // }

    Ok(())
}