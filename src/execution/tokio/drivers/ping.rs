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
        if let WorkItem::Ping = work_item {
            unsafe {
                buf.set_len(0);
            }
            buf.extend_from_slice(b"PING\r\n");

            s.write_all(&buf).await?;

            unsafe {
                buf.set_len(0);
            }

            if let Ok(result) = timeout(Duration::from_millis(200), s.read(&mut buf)).await {
                match result {
                    Ok(n) => {
                        unsafe {
                            buf.set_len(n);
                        }
                        if buf == b"PONG\r\n" {
                            RESPONSE_OK.increment();
                            stream = Some(s);
                        } else {
                            RESPONSE_EX.increment();
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            } else {
                RESPONSE_TIMEOUT.increment();
            }
        }
    }

    Ok(())
}