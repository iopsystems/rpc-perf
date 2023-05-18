use super::*;

pub async fn hash_increment(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::HashIncrement,
) -> std::result::Result<(), ResponseError> {
    HASH_INCR.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.hincr::<&[u8], &[u8], i64, i64>(&request.key, &request.field, request.amount),
    )
    .await
    {
        Ok(Ok(_)) => {
            HASH_INCR_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            HASH_INCR_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            HASH_INCR_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
