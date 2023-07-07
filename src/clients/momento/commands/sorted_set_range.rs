use super::*;

/// Performs a range query on a sorted set, returning the specified range of
/// elements. Currently, this only supports selecting by index (rank) and
/// selects all members of the sorted set.
pub async fn sorted_set_range(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetRange,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_RANGE.increment();

    let result = match (request.start, request.end) {
        (None, None) => {
            timeout(
                config.client().unwrap().request_timeout(),
                client.sorted_set_fetch_by_index(
                    cache_name,
                    &*request.key,
                    momento::sorted_set::Order::Ascending,
                    ..,
                ),
            )
            .await
        }
        (Some(start), None) => {
            timeout(
                config.client().unwrap().request_timeout(),
                client.sorted_set_fetch_by_index(
                    cache_name,
                    &*request.key,
                    momento::sorted_set::Order::Ascending,
                    start..,
                ),
            )
            .await
        }
        (None, Some(end)) => {
            timeout(
                config.client().unwrap().request_timeout(),
                client.sorted_set_fetch_by_index(
                    cache_name,
                    &*request.key,
                    momento::sorted_set::Order::Ascending,
                    ..end,
                ),
            )
            .await
        }
        (Some(start), Some(end)) => {
            timeout(
                config.client().unwrap().request_timeout(),
                client.sorted_set_fetch_by_index(
                    cache_name,
                    &*request.key,
                    momento::sorted_set::Order::Ascending,
                    start..end,
                ),
            )
            .await
        }
    };

    match result {
        Ok(Ok(_)) => {
            SORTED_SET_RANGE_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SORTED_SET_RANGE_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SORTED_SET_RANGE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
