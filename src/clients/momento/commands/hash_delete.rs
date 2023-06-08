use super::*;

/// Remove one or more fields from a hash (dictionary).
pub async fn hash_delete(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashDelete,
) -> std::result::Result<(), ResponseError> {
    HASH_DELETE.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.dictionary_delete(
            cache_name,
            &*request.key,
            Fields::Some(request.fields.iter().map(|f| &**f).collect()),
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            HASH_DELETE_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            HASH_DELETE_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            HASH_DELETE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
