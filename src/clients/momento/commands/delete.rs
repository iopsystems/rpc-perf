use super::*;

/// Remove a key from the cache.
pub async fn delete(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::Delete,
) -> std::result::Result<(), ResponseError> {
    DELETE.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.delete(cache_name, (*request.key).to_owned()),
    )
    .await
    {
        Ok(Ok(_)) => {
            DELETE_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            DELETE_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            DELETE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
