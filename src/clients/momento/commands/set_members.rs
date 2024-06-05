use super::*;

/// Retrieve all the members (elements) within a set.
pub async fn set_members(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetMembers,
) -> std::result::Result<(), ResponseError> {
    SET_MEMBERS.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.set_fetch(cache_name, &*request.key),
    )
    .await;

    record_result!(result, SET_MEMBERS)
}
