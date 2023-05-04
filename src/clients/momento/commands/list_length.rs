use super::*;

pub async fn list_length(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListLength,
) -> std::result::Result<(), ResponseError> {
    LIST_LENGTH.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.list_fetch(cache_name, &*request.key),
    )
    .await
    {
        Ok(Ok(_)) => {
            LIST_LENGTH_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            LIST_LENGTH_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            LIST_LENGTH_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}