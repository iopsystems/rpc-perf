use super::*;

/// Sets a key-value pair in the cache.
pub async fn set(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::Set,
) -> std::result::Result<(), ResponseError> {
    SET.increment();
    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.set(
            cache_name,
            (*request.key).to_owned(),
            (*request.value).to_owned(),
            request.ttl,
        ),
    )
    .await;
    record_result!(result, SET, SET_STORED)
}
