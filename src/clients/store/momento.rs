use crate::clients::*;
use crate::*;
use crate::workload::*;

use async_channel::Receiver;
use tokio::runtime::Runtime;
use ::momento::storage::configurations::LowLatency;
use ::momento::*;
use paste::paste;
use ::momento::storage::PutRequest;
use storage::GetResponse;
use tokio::time::timeout;

use std::time::Instant;
use std::io::{Error, Result};

/// Launch tasks with one channel per task as gRPC is mux-enabled.
pub fn launch_tasks(
    runtime: &mut Runtime,
    config: Config,
    work_receiver: Receiver<ClientWorkItemKind<StoreClientRequest>>,
) {
    debug!("launching momento protocol tasks");

    for _ in 0..config.storage().unwrap().poolsize() {
        let client = {
            let _guard = runtime.enter();

            // initialize the Momento cache client
            if std::env::var("MOMENTO_API_KEY").is_err() {
                eprintln!("environment variable `MOMENTO_API_KEY` is not set");
                std::process::exit(1);
            }

            let credential_provider =
                match CredentialProvider::from_env_var("MOMENTO_API_KEY".to_string()) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("MOMENTO_API_KEY key should be valid: {e}");
                        std::process::exit(1);
                    }
                };

            match PreviewStorageClient::builder()
                .configuration(LowLatency::latest())
                .credential_provider(credential_provider)
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("could not create storage client: {}", e);
                    std::process::exit(1);
                }
            }
        };

        STORE_CONNECT.increment();
        STORE_CONNECT_CURR.increment();

        // create one task per channel
        for _ in 0..config.storage().unwrap().concurrency() {
            runtime.spawn(task(config.clone(), client.clone(), work_receiver.clone()));
        }
    }
}

async fn task(
    config: Config,
    mut client: PreviewStorageClient,
    work_receiver: Receiver<ClientWorkItemKind<StoreClientRequest>>,
) -> Result<()> {
    let store_config = config.storage().unwrap_or_else(|| {
        eprintln!("store configuration was not specified");
        std::process::exit(1);
    });
    let store_name = store_config.store_name().unwrap_or_else(|| {
        eprintln!("store name is not specified in the `store` section");
        std::process::exit(1);
    });

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::other("channel closed"))?;

        STORE_REQUEST.increment();
        let start = Instant::now();
        let result = match work_item {
            ClientWorkItemKind::Request { request, .. } => match request {
                /*
                 * KEY-VALUE
                 */
                StoreClientRequest::Get(r) => store_get(&mut client, &config, store_name, r).await,
                StoreClientRequest::Put(r) => put(&mut client, &config, store_name, r).await,
                StoreClientRequest::Delete(r) => {
                    store_delete(&mut client, &config, store_name, r).await
                }
                _ => {
                    STORE_REQUEST_UNSUPPORTED.increment();
                    continue;
                }
            },
            ClientWorkItemKind::Reconnect => {
                continue;
            }
        };

        STORE_REQUEST_OK.increment();

        let stop = Instant::now();

        match result {
            Ok(_) => {
                STORE_RESPONSE_OK.increment();

                let latency = stop.duration_since(start).as_nanos() as u64;

                let _ = STORE_RESPONSE_LATENCY.increment(latency);
            }
            Err(ResponseError::Exception) => {
                STORE_RESPONSE_EX.increment();
            }
            Err(ResponseError::Timeout) => {
                STORE_RESPONSE_TIMEOUT.increment();
            }
            Err(ResponseError::Ratelimited) => {
                STORE_RESPONSE_RATELIMITED.increment();
            }
            Err(ResponseError::BackendTimeout) => {
                STORE_RESPONSE_BACKEND_TIMEOUT.increment();
            }
        }
    }

    Ok(())
}

/// Puts a key-value pair in a store.
pub async fn put(
    client: &mut PreviewStorageClient,
    config: &Config,
    store_name: &str,
    request: workload::store::Put,
) -> std::result::Result<(), ResponseError> {
    STORE_PUT.increment();

    let r = PutRequest::new(store_name, &*request.key, &*request.value);
    let result = timeout(
        config.storage().unwrap().request_timeout(),
        client.send_request(r),
    )
    .await;

    record_result!(result, STORE_PUT, STORE_PUT_STORED)
}

/// Retrieve a key-value pair from the store.
pub async fn store_get(
    client: &mut PreviewStorageClient,
    config: &Config,
    store_name: &str,
    request: workload::store::Get,
) -> std::result::Result<(), ResponseError> {
    STORE_GET.increment();

    match timeout(
        config.storage().unwrap().request_timeout(),
        client.get(store_name, &*request.key),
    )
    .await
    {
        Ok(Ok(r)) => match r {
            GetResponse::Found { .. } => {
                STORE_GET_OK.increment();
                STORE_RESPONSE_FOUND.increment();
                STORE_GET_KEY_FOUND.increment();
                Ok(())
            }
            GetResponse::NotFound => {
                STORE_GET_OK.increment();
                STORE_RESPONSE_NOT_FOUND.increment();
                STORE_GET_KEY_NOT_FOUND.increment();
                Ok(())
            }
        },
        Ok(Err(e)) => {
            STORE_GET_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            STORE_GET_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}

/// Remove a key from the store.
pub async fn store_delete(
    client: &mut PreviewStorageClient,
    config: &Config,
    store_name: &str,
    request: workload::store::Delete,
) -> std::result::Result<(), ResponseError> {
    STORE_DELETE.increment();

    let result = timeout(
        config.storage().unwrap().request_timeout(),
        client.delete(store_name, (*request.key).to_owned()),
    )
    .await;

    record_result!(result, STORE_DELETE)
}
