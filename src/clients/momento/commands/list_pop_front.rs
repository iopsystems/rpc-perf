use super::*;

/// Removes and returns the item from the front of a list.
pub async fn list_pop_front(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListPopFront,
) -> std::result::Result<(), ResponseError> {
    LIST_POP_FRONT.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.list_pop_front(cache_name, &*request.key),
    )
    .await
    {
        Ok(Ok(_)) => {
            LIST_POP_FRONT_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            LIST_POP_FRONT_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            LIST_POP_FRONT_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
