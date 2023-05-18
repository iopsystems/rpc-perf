use super::*;

pub async fn list_length(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::ListLength,
) -> std::result::Result<(), ResponseError> {
    LIST_LENGTH.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.llen::<&[u8], Option<u64>>(request.key.as_ref()),
    )
    .await
    {
        Ok(Ok(_)) => {
            LIST_LENGTH_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            LIST_LENGTH_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            LIST_LENGTH_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
