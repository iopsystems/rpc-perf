use super::*;

/// Set the value for a field in a hash (dictionary).
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn hash_set(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashSet,
) -> std::result::Result<(), ResponseError> {
    HASH_SET.increment();
    let data: HashMap<Vec<u8>, Vec<u8>> = request
        .data
        .iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.dictionary_set(
            cache_name,
            &*request.key,
            data,
            CollectionTtl::new(request.ttl, false),
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            HASH_SET_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            HASH_SET_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            HASH_SET_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
