use super::*;
use tonic::transport::Channel;

use pingpong::ping_client::PingClient;
use pingpong::PingRequest;

pub mod pingpong {
    tonic::include_proto!("pingpong");
}

// launch a pool manager and worker tasks since HTTP/2.0 is mux'ed we prepare
// senders in the pool manager and pass them over a queue to our worker tasks
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching http2 protocol tasks");

    for endpoint in config.target().endpoints() {
        for _ in 0..config.client().unwrap().poolsize() {
            let _ = runtime.enter();

            let endpoint = endpoint.clone();

            let client = runtime
                .block_on(async { PingClient::connect(endpoint).await })
                .unwrap();

            // create one task per channel
            for _ in 0..config.client().unwrap().concurrency() {
                runtime.spawn(task(config.clone(), client.clone(), work_receiver.clone()));
            }
        }
    }
}

async fn task(
    _config: Config,
    mut client: PingClient<Channel>,
    work_receiver: Receiver<WorkItem>,
) -> Result<()> {
    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        #[allow(clippy::single_match)]
        let result = match work_item {
            WorkItem::Request { request, .. } => match request {
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
                todo!()
            } // Err(ResponseError::Exception) => {
              //     RESPONSE_EX.increment();
              // }
              // Err(ResponseError::Timeout) => {
              //     RESPONSE_TIMEOUT.increment();
              // }
              // Err(ResponseError::Ratelimited) => {
              //     RESPONSE_RATELIMITED.increment();
              // }
              // Err(ResponseError::BackendTimeout) => {
              //     RESPONSE_BACKEND_TIMEOUT.increment();
              // }
        }
    }

    Ok(())
}
