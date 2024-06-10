use super::*;

use ::momento::cache::DictionaryFetchResponse;

/// Retrieve all fields for a hash (dictionary).
pub async fn hash_get_all(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashGetAll,
) -> std::result::Result<(), ResponseError> {
    HASH_GET_ALL.increment();

    match timeout(
        config.client().unwrap().request_timeout(),
        client.dictionary_fetch(cache_name, &*request.key),
    )
    .await
    {
        Ok(Ok(r)) => match r {
            DictionaryFetchResponse::Hit { .. } => {
                RESPONSE_HIT.increment();
                HASH_GET_ALL_HIT.increment();
                Ok(())
            }
            DictionaryFetchResponse::Miss => {
                RESPONSE_MISS.increment();
                HASH_GET_ALL_MISS.increment();
                Ok(())
            }
        },
        Ok(Err(e)) => {
            HASH_GET_ALL_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            HASH_GET_ALL_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
