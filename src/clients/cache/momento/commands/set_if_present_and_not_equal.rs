use super::*;
use ::momento::cache::SetIfPresentAndNotEqualResponse;

pub async fn set_if_present_and_not_equal(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetIfPresentAndNotEqual,
) -> std::result::Result<(), ResponseError> {
    SET_IF_PRESENT_AND_NOT_EQUAL.increment();

    match timeout(
        config.client().unwrap().request_timeout(),
        client.set_if_present_and_not_equal(
            cache_name,
            (*request.key).to_owned(),
            (*request.new_value).to_owned(),
            (*request.old_value).to_owned(),
        ),
    )
    .await
    {
        Ok(Ok(r)) => match r {
            SetIfPresentAndNotEqualResponse::Stored { .. } => {
                SET_IF_PRESENT_AND_NOT_EQUAL_STORED.increment();
                Ok(())
            }
            SetIfPresentAndNotEqualResponse::NotStored => {
                SET_IF_PRESENT_AND_NOT_EQUAL_NOT_STORED.increment();
                Ok(())
            }
        },
        Ok(Err(e)) => {
            SET_IF_PRESENT_AND_NOT_EQUAL_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SET_IF_PRESENT_AND_NOT_EQUAL_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
