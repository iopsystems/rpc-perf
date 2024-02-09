use super::*;

/// Returns the length of a list in the cache.
pub async fn list_length(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListLength,
) -> std::result::Result<(), ResponseError> {
    LIST_LENGTH.increment();
    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.list_length(cache_name, &*request.key),
    )
    .await;
    record_result!(result, LIST_LENGTH)
}
