use crate::clients::cache::momento::protosocket_commands;
use crate::clients::ResponseError;
use crate::workload::{ClientRequest, ClientWorkItemKind};
use crate::*;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use momento::protosocket::cache::Configuration;
use momento::{CredentialProvider, ProtosocketCacheClient};

use async_channel::Receiver;
use tokio::runtime::Runtime;

use std::time::Instant;

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
    use_private_endpoints: bool,
) {
    debug!("launching momento-protosocket protocol tasks");

    let credential_provider = match CredentialProvider::from_env_var("MOMENTO_API_KEY") {
        Ok(v) => {
            if use_private_endpoints {
                v.with_private_endpoints()
            } else {
                v
            }
        }
        Err(e) => {
            eprintln!("MOMENTO_API_KEY environment error: {e:?}");
            std::process::exit(1);
        }
    };

    for _connection_number in 0..config.client().unwrap().poolsize() {
        runtime.spawn(launch_protosocket_task(
            config.clone(),
            credential_provider.clone(),
            work_receiver.clone(),
        ));
    }
}

async fn launch_protosocket_task(
    config: Config,
    credential_provider: CredentialProvider,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
) {
    let client = match ProtosocketCacheClient::builder()
        .default_ttl(Duration::from_secs(900))
        .configuration(
            Configuration::builder()
                .timeout(Duration::from_secs(10))
                .connection_count(1)
                .az_id(None)
                .build(),
        )
        .credential_provider(credential_provider)
        .runtime(tokio::runtime::Handle::current())
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("could not create protosocket cache client: {}", e);
            std::process::exit(1);
        }
    };

    CONNECT.increment();
    CONNECT_CURR.increment();

    let result = protosocket_task(
        config.clone(),
        client.clone(),
        work_receiver.clone(),
        config.client().unwrap().concurrency(),
    )
    .await;
    eprintln!("protosocket driver task exited: {result:?}");
}

async fn protosocket_task(
    config: Config,
    client: ProtosocketCacheClient,
    work_receiver: Receiver<ClientWorkItemKind<ClientRequest>>,
    concurrency_limit: usize,
) -> super::Result<()> {
    eprintln!("started protosocket task");
    let cache_name = config.target().cache_name().unwrap_or_else(|| {
        eprintln!("cache_name must be specified in the `target` section");
        std::process::exit(1);
    });
    let mut in_flight = FuturesUnordered::new();

    while RUNNING.load(Ordering::Relaxed) {
        let in_flight_count = in_flight.len();
        tokio::select! {
            // Always prioritize driving runnable in-flight tasks to completion
            biased;
            resolved = async {
                if in_flight.is_empty() {
                    futures::future::pending().await
                } else {
                    in_flight.next().await
                }
            } => {
                match resolved {
                    Some(()) => (),
                    None => {
                        eprintln!("in-flight protosocket task exited");
                        break;
                    }
                }
            }
            work_item = async {
                if in_flight_count < concurrency_limit {
                    work_receiver.recv().await
                } else {
                    futures::future::pending().await
                }
            } => {
                match work_item {
                    Ok(work_item) => {
                        in_flight.push(run_work_item(work_item, &config, &client, cache_name));
                    }
                    Err(e) => {
                        eprintln!("work channel closed: {e:?}");
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn run_work_item(
    work_item: ClientWorkItemKind<ClientRequest>,
    config: &Config,
    client: &ProtosocketCacheClient,
    cache_name: &str,
) {
    REQUEST.increment();
    let start = Instant::now();
    let result = match work_item {
        ClientWorkItemKind::Request { request, .. } => match request {
            ClientRequest::Get(r) => protosocket_commands::get(client, config, cache_name, r).await,
            ClientRequest::Set(r) => protosocket_commands::set(client, config, cache_name, r).await,
            // ClientRequest::Delete(r) => {
            //     protosocket_commands::delete(&mut client, &config, cache_name, r).await
            // }
            /*
             * UNSUPPORTED
             */
            _ => {
                REQUEST_UNSUPPORTED.increment();
                return;
            }
        },
        ClientWorkItemKind::Reconnect => {
            return;
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
        Err(ResponseError::Exception) => {
            RESPONSE_EX.increment();
        }
        Err(ResponseError::Timeout) => {
            RESPONSE_TIMEOUT.increment();
        }
        Err(ResponseError::Ratelimited) => {
            RESPONSE_RATELIMITED.increment();
        }
        Err(ResponseError::BackendTimeout) => {
            RESPONSE_BACKEND_TIMEOUT.increment();
        }
    }
}
