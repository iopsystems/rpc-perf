use super::*;

/// Remove one or more fields from a hash (dictionary).
pub async fn hash_delete(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashDelete,
) -> std::result::Result<(), ResponseError> {
    HASH_DELETE.increment();

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.dictionary_remove_fields(
            cache_name,
            &*request.key,
            request.fields.iter().map(|f| &**f),
        ),
    )
    .await;

    record_result!(result, HASH_DELETE)
}
