use super::*;

/// Retrieve all elements from a list in the cache.
pub async fn list_fetch(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::ListFetch,
) -> std::result::Result<(), ResponseError> {
    LIST_FETCH.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.lrange::<&[u8], Option<Vec<Vec<u8>>>>(request.key.as_ref(), 0, -1),
    )
    .await
    {
        Ok(Ok(_)) => {
            LIST_FETCH_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            LIST_FETCH_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            LIST_FETCH_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
