use super::*;

/// Retrieve all the members of a list in the cache.
pub async fn list_fetch(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListFetch,
) -> std::result::Result<(), ResponseError> {
    LIST_FETCH.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.list_fetch(cache_name, &*request.key),
    )
    .await;

    record_result!(result, LIST_FETCH)
}
