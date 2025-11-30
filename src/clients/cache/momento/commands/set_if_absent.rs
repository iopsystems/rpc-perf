use super::*;
use ::momento::cache::SetIfAbsentResponse;

pub async fn set_if_absent(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetIfAbsent,
) -> std::result::Result<(), ResponseError> {
    SET_IF_ABSENT.increment();

    match timeout(
        config.client().unwrap().request_timeout(),
        client.set_if_absent(
            cache_name,
            (*request.key).to_owned(),
            (*request.value).to_owned(),
        ),
    )
    .await
    {
        Ok(Ok(r)) => match r {
            SetIfAbsentResponse::Stored { .. } => {
                SET_IF_ABSENT_STORED.increment();
                Ok(())
            }
            SetIfAbsentResponse::NotStored => {
                SET_IF_ABSENT_NOT_STORED.increment();
                Ok(())
            }
        },
        Ok(Err(e)) => {
            SET_IF_ABSENT_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SET_IF_ABSENT_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
