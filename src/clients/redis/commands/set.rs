use super::*;

pub async fn set(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::Set
) -> std::result::Result<(), ResponseError> {
    SET.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.set::<&[u8], &[u8], ()>(&key, &value),
    )
    .await
    {
        Ok(Ok(_)) => {
            SET_STORED.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            SET_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            SET_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}