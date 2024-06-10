use super::*;

/// Removes and returns the item from the back of a list.
pub async fn list_pop_back(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListPopBack,
) -> std::result::Result<(), ResponseError> {
    LIST_POP_BACK.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.list_pop_back(cache_name, &*request.key),
    )
    .await;

    record_result!(result, LIST_POP_BACK)
}
