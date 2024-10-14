use super::*;

/// Checks if a field exists in a hash.
pub async fn hash_exists(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::HashExists,
) -> std::result::Result<(), ResponseError> {
    HASH_EXISTS.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.hexists::<&[u8], &[u8], bool>(&request.key, &request.field),
    )
    .await
    {
        Ok(Ok(true)) => {
            RESPONSE_HIT.increment();
            HASH_EXISTS_HIT.increment();
            HASH_EXISTS_OK.increment();
            Ok(())
        }
        Ok(Ok(false)) => {
            RESPONSE_MISS.increment();
            HASH_EXISTS_MISS.increment();
            HASH_EXISTS_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            HASH_EXISTS_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            HASH_EXISTS_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
