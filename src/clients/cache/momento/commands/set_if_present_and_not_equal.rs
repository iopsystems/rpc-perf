use super::*;

pub async fn set_if_present_and_not_equal(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetIfPresentAndNotEqual,
) -> std::result::Result<(), ResponseError> {
    SET_IF_PRESENT_AND_NOT_EQUAL.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.set_if_present_and_not_equal(
            cache_name,
            (*request.key).to_owned(),
            (*request.value).to_owned(), // new value
            (*request.value).to_owned(), // old value
        ),
    )
    .await;

    record_result!(result, SET_IF_PRESENT_AND_NOT_EQUAL)
}
