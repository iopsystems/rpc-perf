use super::*;

/// Removes a member from a sorted set.
pub async fn sorted_set_remove(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::SortedSetRemove,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_REMOVE.increment();
    let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.zrem::<&[u8], Vec<&[u8]>, usize>(&request.key, members),
    )
    .await
    {
        Ok(Ok(_)) => {
            SORTED_SET_REMOVE_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            SORTED_SET_REMOVE_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            SORTED_SET_REMOVE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
