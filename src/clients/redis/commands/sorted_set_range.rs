use super::*;

/// Performs a range query on a sorted set, returning the specified range of
/// elements. Currently, this only supports selecting by index (rank) and
/// selects all members of the sorted set.
pub async fn sorted_set_range(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SortedSetRange,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_RANGE.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.zrange::<&[u8], Vec<(Vec<u8>, f64)>>(
            request.key.as_ref(),
            request.start.unwrap_or(0).try_into().unwrap(),
            request.end.unwrap_or(-1).try_into().unwrap(),
        ),
    )
    .await
    {
        Ok(Ok(set)) => {
            if set.is_empty() {
                RESPONSE_MISS.increment();
            } else {
                RESPONSE_HIT.increment();
            }

            SORTED_SET_RANGE_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            SORTED_SET_RANGE_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            SORTED_SET_RANGE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
