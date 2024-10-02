use super::*;

/// Retrieve the rank for a member of a sorted set.
pub async fn sorted_set_rank(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetRank,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_RANK.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_get_rank(cache_name, &*request.key, &*request.member),
    )
    .await;

    record_result!(result, SORTED_SET_RANK)
}
