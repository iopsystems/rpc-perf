use super::*;

/// Remove a key from the cache.
pub async fn delete(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::Delete,
) -> std::result::Result<(), ResponseError> {
    DELETE.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.delete(cache_name, (*request.key).to_owned()),
    )
    .await;

    record_result!(result, DELETE)
}
