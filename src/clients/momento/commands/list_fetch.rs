use super::*;

pub async fn list_fetch(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListFetch,
) -> std::result::Result<(), ResponseError> {
    LIST_FETCH.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.list_fetch(cache_name, &*request.key),
    )
    .await
    {
        Ok(Ok(_)) => {
            LIST_FETCH_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            LIST_FETCH_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            LIST_FETCH_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
