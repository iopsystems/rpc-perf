use super::*;

/// Retrieve all fields for a hash.
pub async fn hash_get_all(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::HashGetAll,
) -> std::result::Result<(), ResponseError> {
    HASH_GET_ALL.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.hgetall::<&[u8], Option<HashMap<Vec<u8>, Vec<u8>>>>(request.key.as_ref()),
    )
    .await
    {
        Ok(Ok(Some(_))) => {
            RESPONSE_HIT.increment();
            HASH_GET_ALL_OK.increment();
            HASH_GET_ALL_HIT.increment();
            Ok(())
        }
        Ok(Ok(None)) => {
            RESPONSE_MISS.increment();
            HASH_GET_ALL_OK.increment();
            HASH_GET_ALL_MISS.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            HASH_GET_ALL_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            HASH_GET_ALL_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
