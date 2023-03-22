// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use super::*;

use reqwest::Client;

/// Launch tasks with one conncetion per task as http/1.1 is not mux'd
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching http1 protocol tasks");

    for _ in 0..config.connection().poolsize() {
        for endpoint in config.target().endpoints() {
            runtime.spawn(task(
                work_receiver.clone(),
                endpoint.clone(),
                config.clone(),
            ));
        }
    }
}

// a task for http/1.1
#[allow(clippy::slow_vector_initialization)]
async fn task(work_receiver: Receiver<WorkItem>, endpoint: String, config: Config) -> Result<()> {
    // technically, we might not have an open connection until a request is sent
    // but this is the only mechanism we have right now to make these stats look
    // sensible in the output
    CONNECT.increment();
    CONNECT_CURR.add(1);

    let client = Client::builder()
        .http1_only()
        .user_agent("rpc-perf/1.0")
        .timeout(config.request().timeout())
        .connect_timeout(config.connection().timeout())
        .pool_idle_timeout(None)
        .connection_verbose(true)
        .build()
        .expect("failed to create client");

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();

        // compose request into buffer
        let request = match work_item {
            WorkItem::Get { .. } => {
                client
                    .get(format!("http://{endpoint}/"))
                    .build()
                    .expect("failed to create request")

                // Request::Ping.compose(&mut write_buffer);
            }
            WorkItem::Reconnect => {
                REQUEST_RECONNECT.increment();
                continue;
            }
            _ => {
                REQUEST_UNSUPPORTED.increment();
                // stream = Some(s);
                continue;
            }
        };

        REQUEST_OK.increment();

        // send request
        let start = Instant::now();
        let response = client.execute(request).await;
        let stop = Instant::now();

        match response {
            Ok(_response) => {
                // validate response
                match work_item {
                    WorkItem::Get { .. } => {
                        GET_OK.increment();
                    }
                    _ => {
                        error!("unexpected work item");
                        unimplemented!();
                    }
                }

                RESPONSE_OK.increment();
                RESPONSE_LATENCY.increment(stop, stop.duration_since(start).as_nanos(), 1);
            }
            Err(e) => {
                if e.is_timeout() {
                    RESPONSE_TIMEOUT.increment();
                } else {
                    // record execption
                    match work_item {
                        WorkItem::Get { .. } => {
                            GET_EX.increment();
                        }
                        _ => {
                            error!("unexpected work item");
                            unimplemented!();
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
