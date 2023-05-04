use super::*;

pub async fn hash_increment(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::HashIncrement,
) -> std::result::Result<(), ResponseError> {
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.hincr::<&[u8], &[u8], i64, i64>(&request.key, &request.field, request.amount),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(_)) => Err(ResponseError::Exception),
        Err(_) => Err(ResponseError::Timeout),
    }
}
