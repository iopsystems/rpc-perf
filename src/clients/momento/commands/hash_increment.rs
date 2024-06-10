use super::*;

use ::momento::cache::{CollectionTtl, DictionaryIncrementRequest};

/// Increment the value for a field in a dictionary.
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn hash_increment(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashIncrement,
) -> std::result::Result<(), ResponseError> {
    HASH_INCR.increment();

    let r =
        DictionaryIncrementRequest::new(cache_name, &*request.key, &*request.field, request.amount)
            .ttl(CollectionTtl::new(request.ttl, false));

    match timeout(
        config.client().unwrap().request_timeout(),
        client.send_request(r),
    )
    .await
    {
        Ok(Ok(r)) => {
            HASH_INCR_OK.increment();
            if r.value == request.amount {
                RESPONSE_MISS.increment();
                HASH_INCR_MISS.increment();
            } else {
                RESPONSE_HIT.increment();
                HASH_INCR_HIT.increment();
            }
            Ok(())
        }
        Ok(Err(e)) => {
            HASH_INCR_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            HASH_INCR_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
