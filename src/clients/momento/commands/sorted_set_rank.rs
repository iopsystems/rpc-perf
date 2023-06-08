use super::*;

/// Retrieve the rank for a member of a sorted set.
pub async fn sorted_set_rank(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetRank,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_RANK.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_get_rank(cache_name, &*request.key, &*request.member),
    )
    .await
    {
        Ok(Ok(_)) => {
            SORTED_SET_RANK_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SORTED_SET_RANK_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SORTED_SET_RANK_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
