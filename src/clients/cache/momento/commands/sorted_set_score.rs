use super::*;

/// Retrieve the score of one or more members of a sorted set.
pub async fn sorted_set_score(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetScore,
) -> std::result::Result<(), ResponseError> {
    if request.members.is_empty() {
        return Ok(());
    }

    SORTED_SET_SCORE.increment();

    if request.members.len() == 1 {
        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.sorted_set_get_score(cache_name, &*request.key, &*request.members[0]),
        )
        .await;

        record_result!(result, SORTED_SET_SCORE)
    } else {
        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.sorted_set_get_scores(
                cache_name,
                &*request.key,
                request.members.iter().map(|f| &**f),
            ),
        )
        .await;

        record_result!(result, SORTED_SET_SCORE)
    }
}
