use super::*;

pub async fn list_pop_back(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListPopBack,
) -> std::result::Result<(), ResponseError> {
    LIST_POP_BACK.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.list_pop_back(cache_name, &*request.key),
    )
    .await
    {
        Ok(Ok(_)) => {
            LIST_POP_BACK_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            LIST_POP_BACK_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            LIST_POP_BACK_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}