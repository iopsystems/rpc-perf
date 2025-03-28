use super::*;
use ::momento::cache::ItemGetTypeResponse;

pub async fn item_get_type(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ItemGetType,
) -> std::result::Result<(), ResponseError> {
    ITEM_GET_TYPE.increment();

    match timeout(
        config.client().unwrap().request_timeout(),
        client.item_get_type(cache_name, (*request.key).to_owned()),
    )
    .await
    {
        Ok(Ok(r)) => match r {
            ItemGetTypeResponse::Hit { .. } => {
                ITEM_GET_TYPE_HIT.increment();
                Ok(())
            }
            ItemGetTypeResponse::Miss => {
                ITEM_GET_TYPE_MISS.increment();
                Ok(())
            }
        },
        Ok(Err(e)) => {
            ITEM_GET_TYPE_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            ITEM_GET_TYPE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
