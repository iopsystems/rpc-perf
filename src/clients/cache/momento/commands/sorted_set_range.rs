use super::*;

use ::momento::cache::{SortedSetFetchByRankRequest, SortedSetFetchByScoreRequest, SortedSetOrder};

/// Performs a range query on a sorted set, returning the specified range of
/// elements. Supports selecting a range of keys by index (rank).
pub async fn sorted_set_range(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetRange,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_RANGE.increment();

    let result = if !request.by_score {
        let r = SortedSetFetchByRankRequest::new(cache_name, &*request.key)
            .start_rank(request.start)
            .end_rank(request.end)
            .order(SortedSetOrder::Ascending);

        timeout(
            config.client().unwrap().request_timeout(),
            client.send_request(r),
        )
        .await
    } else {
        let r = SortedSetFetchByScoreRequest::new(cache_name, &*request.key)
            .min_score(request.start.map(|v| v.into()))
            .max_score(request.end.map(|v| v.into()))
            .order(SortedSetOrder::Ascending);

        timeout(
            config.client().unwrap().request_timeout(),
            client.send_request(r),
        )
        .await
    };

    record_result!(result, SORTED_SET_RANGE)
}
