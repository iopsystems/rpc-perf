use super::*;

/// Performs a range query on a sorted set, returning the specified range of
/// elements. Supports selecting a range of keys by index (rank).
pub async fn sorted_set_range(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::SortedSetRange,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_RANGE.increment();

    let result = if !request.by_score {
        timeout(
            config.client().unwrap().request_timeout(),
            connection.zrange::<&[u8], Vec<(Vec<u8>, f64)>>(
                request.key.as_ref(),
                request.start.unwrap_or(0).try_into().unwrap(),
                request.end.unwrap_or(-1).try_into().unwrap(),
            ),
        )
        .await
    } else {
        timeout(
            config.client().unwrap().request_timeout(),
            connection.zrangebyscore::<&[u8], f64, f64, Vec<(Vec<u8>, f64)>>(
                request.key.as_ref(),
                request.start.map(|v| v as f64).unwrap_or(f64::MIN),
                request.end.map(|v| v as f64).unwrap_or(f64::MAX),
            ),
        )
        .await
    };

    match result {
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
