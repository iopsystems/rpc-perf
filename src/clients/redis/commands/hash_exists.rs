use super::*;

pub async fn hash_exists(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::HashExists,
) -> std::result::Result<(), ResponseError> {
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.hexists::<&[u8], &[u8], bool>(&request.key, &request.field),
    )
    .await
    {
        Ok(Ok(true)) => {
            RESPONSE_HIT.increment();
            HASH_EXISTS_HIT.increment();
            Ok(())
        }
        Ok(Ok(false)) => {
            RESPONSE_MISS.increment();
            HASH_EXISTS_MISS.increment();
            Ok(())
        }
        Ok(Err(_)) => Err(ResponseError::Exception),
        Err(_) => Err(ResponseError::Timeout),
    }
}
