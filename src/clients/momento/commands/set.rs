use super::*;

pub async fn set(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::Set,
) -> std::result::Result<(), ResponseError> {
    SET.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.set(
            cache_name,
            (*request.key).to_owned(),
            (*request.value).to_owned(),
            None,
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            SET_STORED.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SET_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SET_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
