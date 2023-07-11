use super::*;

/// Retrieve the score of one or more members of a sorted set.
pub async fn sorted_set_score(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetScore,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_SCORE.increment();
    let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_get_score(cache_name, &*request.key, members),
    )
    .await;
    record_result!(result, SORTED_SET_SCORE)
}
