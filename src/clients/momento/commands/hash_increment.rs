use super::*;

pub async fn hash_increment(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashIncrement,
) -> std::result::Result<(), ResponseError> {
    HASH_INCR.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.dictionary_increment(
            cache_name,
            &*request.key,
            &*request.field,
            request.amount,
            CollectionTtl::new(request.ttl, false),
        ),
    )
    .await
    {
        Ok(Ok(r)) => {
            HASH_INCR_OK.increment();
            #[allow(clippy::if_same_then_else)]
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
