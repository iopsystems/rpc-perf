use std::time::Instant;
use crate::workload::ClientWorkItemKind;
use crate::workload::ClientRequest;
use std::io::Error;
use tokio::runtime::Runtime;
use async_channel::Receiver;
use crate::*;
use tonic::transport::Channel;

use pingpong::ping_client::PingClient;
use pingpong::PingRequest;

pub mod pingpong {
    tonic::include_proto!("pingpong");
}

// launch a pool manager and worker tasks since HTTP/2.0 is mux'ed we prepare
// senders in the pool manager and pass them over a queue to our worker tasks
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
) {
    debug!("launching ping grpc protocol tasks");

    for endpoint in config.target().endpoints() {
        for _ in 0..config.client().unwrap().poolsize() {
            while RUNNING.load(Ordering::Relaxed) {
                let endpoint = endpoint.clone();

                CONNECT.increment();

                let _ = runtime.enter();

                if let Ok(client) = runtime.block_on(async { PingClient::connect(endpoint).await })
                {
                    CONNECT_OK.increment();
                    CONNECT_CURR.increment();

                    // create one task per channel
                    for _ in 0..config.client().unwrap().concurrency() {
                        runtime.spawn(task(config.clone(), client.clone(), work_receiver.clone()));
                    }
                } else {
                    CONNECT_EX.increment();
                }
            }
        }
    }
}

async fn task(
    _config: Config,
    mut client: PingClient<Channel>,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
) -> Result<(), std::io::Error> {
    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::other("channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        #[allow(clippy::single_match)]
        let result = match work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
                ClientRequest::Ping(_) => client
                    .ping(tonic::Request::new(PingRequest {}))
                    .await
                    .map(|_| ()),
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                    continue;
                }
            },
            _ => {
                continue;
            }
        };

        REQUEST_OK.increment();

        let stop = Instant::now();

        match result {
            Ok(_) => {
                RESPONSE_OK.increment();

                let latency = stop.duration_since(start).as_nanos() as u64;

                let _ = RESPONSE_LATENCY.increment(latency);
            }
            Err(_) => {
                RESPONSE_EX.increment();
            }
        }
    }

    Ok(())
}
