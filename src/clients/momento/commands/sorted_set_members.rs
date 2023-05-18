use super::*;

pub async fn sorted_set_members(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetMembers,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_MEMBERS.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_fetch(
            cache_name,
            &*request.key,
            momento::sorted_set::Order::Ascending,
            None,
            None,
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            SORTED_SET_MEMBERS_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SORTED_SET_MEMBERS_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SORTED_SET_MEMBERS_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
