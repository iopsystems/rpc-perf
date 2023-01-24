use async_memcached::Client;
use super::*;

pub fn launch_memcache_tasks(runtime: &mut Runtime, work_receiver: Receiver<WorkItem>) {
    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..CONNECTIONS {
        runtime.spawn(memcache_task(work_receiver.clone()));
    }
}

// a task for memcache compatible servers (eg: Pelikan Segcache, Memcached)
#[allow(clippy::slow_vector_initialization)]
pub async fn memcache_task(work_receiver: Receiver<WorkItem>) -> Result<()> {
    let mut client = None;

    let mut buf = Vec::with_capacity(4096);
    buf.resize(4096, 0);

    while RUNNING.load(Ordering::Relaxed) {
        if client.is_none() {
            CONNECT.increment();
            client = Some(Client::new("127.0.0.1:12321").await.map_err(|_| Error::new(ErrorKind::Other, "failed to create memcache client"))?);
        }

        let mut c = client.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        let result = match work_item {
            WorkItem::Get { key } => {
                GET.increment();
                timeout(Duration::from_millis(200), c.get(key.as_bytes())).await.map(|r| {
                    match r {
                        Ok(None) => {
                            GET_KEY_MISS.increment();
                            Ok(())
                        }
                        Ok(Some(_)) => {
                            GET_KEY_HIT.increment();
                            Ok(())
                        }
                        Err(e) => {
                            // GET_TIMEOUT.increment();
                            GET_EX.increment();
                            Err(e)
                            // e
                        }
                    }
                })
            }
            WorkItem::Set { key, value } => {
                SET.increment();
                timeout(Duration::from_millis(200), c.set(key.as_bytes(), value.as_bytes(), None, None)).await.map(|r| {
                    match r {
                        Ok(_) => {
                            SET_STORED.increment();
                            Ok(())
                        }
                        Err(e) => {
                            SET_EX.increment();
                            Err(e)
                        }
                    }
                })
            }
            WorkItem::Ping { .. } => {
                PING.increment();
                timeout(Duration::from_millis(200), c.version()).await.map(|r| {
                    match r {
                        Ok(_) => {
                            PING_OK.increment();
                            Ok(())
                        }
                        Err(e) => {
                            PING_EX.increment();
                        Err(e)
                        }
                    }
                })
            }
            _ => {
                continue;
            }
        };

        match result {
            Ok(Ok(_)) => {
                RESPONSE_OK.increment();
                client = Some(c);
            }
            Ok(Err(_)) => {
                RESPONSE_EX.increment();
                client = Some(c);
            }
            Err(_) => {
                RESPONSE_TIMEOUT.increment();
            }
        }
    }

    Ok(())
}