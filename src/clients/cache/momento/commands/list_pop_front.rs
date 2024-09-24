use super::*;

/// Removes and returns the item from the front of a list.
pub async fn list_pop_front(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListPopFront,
) -> std::result::Result<(), ResponseError> {
    LIST_POP_FRONT.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.list_pop_front(cache_name, &*request.key),
    )
    .await;

    record_result!(result, LIST_POP_FRONT)
}
