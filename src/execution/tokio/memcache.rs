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
    let mut stream = None;

    let mut buf = Vec::with_capacity(4096);
    buf.resize(4096, 0);

    while RUNNING.load(Ordering::Relaxed) {
        if stream.is_none() {
            CONNECT.increment();
            stream = Some(TcpStream::connect("127.0.0.1:12321").await?);
        }

        let mut s = stream.take().unwrap();

        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        let result = match work_item {
            WorkItem::Get { key } => {
                GET.increment();

                unsafe {
                    buf.set_len(0);
                }
                buf.extend_from_slice(format!("GET {}\r\n", key).as_bytes());

                s.write_all(&buf).await?;

                unsafe {
                    buf.set_len(0);
                }

                match timeout(Duration::from_millis(200), s.read(&mut buf)).await {
                    Ok(Ok(_)) => {
                        stream = Some(s);

                        Ok(true)
                    }
                    Ok(Err(_)) => Ok(false),
                    Err(e) => Err(e),
                }
            }
            WorkItem::Set { key, value } => {
                unsafe {
                    buf.set_len(0);
                }
                buf.extend_from_slice(
                    format!("SET {} 0 0 {}\r\n{}\r\n", key, value.len(), value).as_bytes(),
                );

                s.write_all(&buf).await?;

                unsafe {
                    buf.set_len(0);
                }

                match timeout(Duration::from_millis(200), s.read(&mut buf)).await {
                    Ok(Ok(_)) => {
                        stream = Some(s);

                        Ok(true)
                    }
                    Ok(Err(_)) => Ok(false),
                    Err(e) => Err(e),
                }
            }
            // memcache backends don't support hash commands
            WorkItem::HashGet { .. } | WorkItem::HashSet { .. } => {
                continue;
            }
            WorkItem::Ping => {
                // TODO: this could be implemented as the version command
                continue;
            }
        };

        if let Ok(ok) = result {
            if ok {
                RESPONSE_OK.increment();
            } else {
                RESPONSE_EX.increment();
            }
        } else {
            RESPONSE_TIMEOUT.increment();
        }
    }

    Ok(())
}