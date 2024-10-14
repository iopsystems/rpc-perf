use super::*;

/// Returns the rank for a member in a sorted set.
pub async fn sorted_set_rank(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::SortedSetRank,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_RANK.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection
            .zrank::<&[u8], &[u8], Option<u64>>(request.key.as_ref(), request.member.as_ref()),
    )
    .await
    {
        Ok(Ok(rank)) => {
            if rank.is_some() {
                RESPONSE_HIT.increment();
            } else {
                RESPONSE_MISS.increment();
            }

            SORTED_SET_RANK_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            SORTED_SET_RANK_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            SORTED_SET_RANK_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
