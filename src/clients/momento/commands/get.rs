use super::*;

pub async fn get(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::Get,
) -> std::result::Result<(), ResponseError> {
    GET.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.get(cache_name, &*request.key),
    )
    .await
    {
        Ok(Ok(r)) => match r.result {
            MomentoGetStatus::HIT => {
                GET_OK.increment();
                RESPONSE_HIT.increment();
                GET_KEY_HIT.increment();
                Ok(())
            }
            MomentoGetStatus::MISS => {
                GET_OK.increment();
                RESPONSE_MISS.increment();
                GET_KEY_MISS.increment();
                Ok(())
            }
            MomentoGetStatus::ERROR => {
                GET_EX.increment();
                Err(ResponseError::Exception)
            }
        },
        Ok(Err(e)) => {
            GET_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            GET_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
