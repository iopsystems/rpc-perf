use super::*;

/// Removes one or more members of a sorted set.
pub async fn sorted_set_remove(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetRemove,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_REMOVE.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_remove_elements(
            cache_name,
            &*request.key,
            request.members.iter().map(|v| v.to_vec()),
        ),
    )
    .await;

    record_result!(result, SORTED_SET_REMOVE)
}
