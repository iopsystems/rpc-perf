use super::*;

use ::momento::cache::GetResponse;

/// Retrieve a key-value pair from the cache.
pub async fn get(
    client: &mut CacheClient,
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
