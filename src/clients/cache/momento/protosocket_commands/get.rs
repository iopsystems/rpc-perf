use ::momento::cache::GetResponse;
use momento::ProtosocketCacheClient;
use tokio::time::timeout;

use crate::{
    clients::ResponseError,
    config::Config,
    metrics::{
        GET, GET_EX, GET_KEY_HIT, GET_KEY_MISS, GET_OK, GET_TIMEOUT, RESPONSE_HIT, RESPONSE_MISS,
    },
    workload,
};

/// Retrieve a key-value pair from the cache.
pub async fn get(
    client: &ProtosocketCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::Get,
) -> std::result::Result<(), ResponseError> {
    GET.increment();

    match timeout(
        config.client().unwrap().request_timeout(),
        client.get(cache_name, &*request.key),
    )
    .await
    {
        Ok(Ok(r)) => match r {
            GetResponse::Hit { .. } => {
                GET_OK.increment();
                RESPONSE_HIT.increment();
                GET_KEY_HIT.increment();
                Ok(())
            }
            GetResponse::Miss => {
                GET_OK.increment();
                RESPONSE_MISS.increment();
                GET_KEY_MISS.increment();
                Ok(())
            }
        },
        Ok(Err(e)) => {
            GET_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            GET_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
