use super::*;

pub async fn get(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::Get
) -> std::result::Result<(), ResponseError> {
    GET.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.get::<&[u8], Option<Vec<u8>>>(&request.key),
    )
    .await
    {
        Ok(Ok(None)) => {
            RESPONSE_MISS.increment();
            GET_KEY_MISS.increment();
            Ok(())
        }
        Ok(Ok(Some(_))) => {
            RESPONSE_HIT.increment();
            GET_KEY_HIT.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            GET_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            GET_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}