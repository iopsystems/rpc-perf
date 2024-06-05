use super::*;

use ::momento::cache::{CollectionTtl, SetAddElementsRequest};

/// Adds one or more members (elements) to a set.
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn set_add(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetAdd,
) -> std::result::Result<(), ResponseError> {
    SET_ADD.increment();

    let members: Vec<&[u8]> = request.members.iter().map(|v| &**v).collect();

    let r = SetAddElementsRequest::new(cache_name, &*request.key, members)
        .ttl(CollectionTtl::new(request.ttl, false));

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.send_request(r),
    )
    .await;

    record_result!(result, SET_ADD)
}
