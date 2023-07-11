use super::*;
use core::ops::Bound;

/// Performs a range query on a sorted set, returning the specified range of
/// elements. Supports selecting a range of keys by index (rank).
pub async fn sorted_set_range(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetRange,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_RANGE.increment();

    let start = match request.start {
        None => Bound::Unbounded,
        Some(v) => Bound::Included(v),
    };

    let end = match request.end {
        None => Bound::Unbounded,
        Some(v) => Bound::Included(v),
    };

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_fetch_by_index(
            cache_name,
            &*request.key,
            momento::sorted_set::Order::Ascending,
            (start, end),
        ),
    )
    .await;

    record_result!(result, SORTED_SET_RANGE)
}
