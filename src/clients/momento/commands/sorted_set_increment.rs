use super::*;

/// Increment the score for a member in a sorted set.
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn sorted_set_increment(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetIncrement,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_INCR.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_increment(
            cache_name,
            &*request.key,
            &*request.member,
            request.amount,
            CollectionTtl::new(request.ttl, false),
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            SORTED_SET_INCR_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SORTED_SET_INCR_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SORTED_SET_INCR_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
