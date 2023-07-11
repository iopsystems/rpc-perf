use super::*;

/// Adds an memeber (element) to a set.
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn set_add(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetAdd,
) -> std::result::Result<(), ResponseError> {
    SET_ADD.increment();
    let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.set_add_elements(
            cache_name,
            &*request.key,
            members,
            CollectionTtl::new(request.ttl, false),
        ),
    )
    .await;
    record_result!(result, SET_ADD)
}
