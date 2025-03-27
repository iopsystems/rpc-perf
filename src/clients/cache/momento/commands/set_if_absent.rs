use super::*;

pub async fn set_if_absent(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetIfAbsent,
) -> std::result::Result<(), ResponseError> {
    SET_IF_ABSENT.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.set_if_absent(
            cache_name,
            (*request.key).to_owned(),
            (*request.value).to_owned(),
        ),
    )
    .await;

    record_result!(result, SET_IF_ABSENT)
}
