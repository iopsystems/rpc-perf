use super::*;

/// Removes a member (element) from a list.
pub async fn list_remove(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListRemove,
) -> std::result::Result<(), ResponseError> {
    LIST_REMOVE.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.list_remove_value(cache_name, &*request.key, &*request.element),
    )
    .await;

    record_result!(result, LIST_REMOVE)
}
