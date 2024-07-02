use super::*;

use ::momento::cache::{CollectionTtl, SortedSetIncrementScoreRequest};

/// Increment the score for a member in a sorted set.
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn sorted_set_increment(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetIncrement,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_INCR.increment();

    let r = SortedSetIncrementScoreRequest::new(
        cache_name,
        &*request.key,
        &*request.member,
        request.amount,
    )
    .ttl(CollectionTtl::new(request.ttl, false));

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.send_request(r),
    )
    .await;

    record_result!(result, SORTED_SET_INCR)
}
