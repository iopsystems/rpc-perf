use super::*;

/// Removes a member (element) from a set.
pub async fn set_remove(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetRemove,
) -> std::result::Result<(), ResponseError> {
    SET_REMOVE.increment();

    let members: Vec<&[u8]> = request.members.iter().map(|v| &**v).collect();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.set_remove_elements(cache_name, &*request.key, members),
    )
    .await;

    record_result!(result, SET_REMOVE)
}
