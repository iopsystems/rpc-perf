use super::*;

pub async fn item_get_type(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ItemGetType,
) -> std::result::Result<(), ResponseError> {
    ITEM_GET_TYPE.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.item_get_type(cache_name, (*request.key).to_owned()),
    )
    .await;

    record_result!(result, ITEM_GET_TYPE)
}
