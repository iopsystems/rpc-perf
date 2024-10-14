use super::*;

/// Delete a key from the cache. This will delete an entire Hash/Set/SortedSet
/// if used in the same keyspace.
pub async fn delete(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::Delete,
) -> std::result::Result<(), ResponseError> {
    DELETE.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.del::<&[u8], ()>(&request.key),
    )
    .await
    {
        Ok(Ok(_)) => {
            DELETE_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            DELETE_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            DELETE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
