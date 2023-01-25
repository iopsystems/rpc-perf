// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

pub fn launch_tasks(runtime: &mut Runtime, work_receiver: Receiver<WorkItem>) {
    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..CONNECTIONS {
        runtime.spawn(task(work_receiver.clone()));
    }
}

// a task for ping servers (eg: Pelikan Pingserver)
#[allow(clippy::slow_vector_initialization)]
async fn task(work_receiver: Receiver<WorkItem>) -> Result<()> {
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

        let start = Instant::now();
        let result = match work_item {
            WorkItem::Ping => {
                unsafe {
                    buf.set_len(0);
                }
                buf.extend_from_slice(b"PING\r\n");

                s.write_all(&buf).await?;

                unsafe {
                    buf.set_len(0);
                }

                timeout(Duration::from_millis(200), s.read(&mut buf)).await.map(|r| {
                    match r {
                        Ok(n) => {
                            unsafe {
                                buf.set_len(n);
                            }
                            if buf == b"PONG\r\n" {
                                RESPONSE_OK.increment();
                                Ok(())
                            } else {
                                RESPONSE_EX.increment();
                                Err(Error::new(ErrorKind::InvalidData, "unexpected data"))
                            }
                        }
                        Err(e) => {
                            Err(e)
                        }
                    }
                })
            }
            _ => {
                continue;
            }
        };

        let stop = Instant::now();

        match result {
            Ok(Ok(_)) => {
                stream = Some(s);

                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Ok(Err(_)) => {
                RESPONSE_EX.increment();
            }
            Err(_) => {
                RESPONSE_TIMEOUT.increment();
            }
        }
    }

    Ok(())
}