use super::*;

use ::momento::cache::SetRequest;

/// Sets a key-value pair in the cache.
pub async fn set(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::Set,
) -> std::result::Result<(), ResponseError> {
    SET.increment();

    let mut r = SetRequest::new(cache_name, &*request.key, &*request.value);

    if let Some(ttl) = request.ttl {
        r = r.ttl(ttl);
    }

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.send_request(r),
    )
    .await;

    record_result!(result, SET, SET_STORED)
}
