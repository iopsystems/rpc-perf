use super::*;

/// Removes one or more members of a sorted set.
pub async fn sorted_set_remove(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetRemove,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_REMOVE.increment();
    let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_remove(cache_name, &*request.key, members),
    )
    .await;
    record_result!(result, SORTED_SET_REMOVE)
}
