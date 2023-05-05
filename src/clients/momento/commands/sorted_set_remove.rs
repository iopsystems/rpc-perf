use super::*;

pub async fn sorted_set_remove(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetRemove,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_REMOVE.increment();
    let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_remove(cache_name, &*request.key, members),
    )
    .await
    {
        Ok(Ok(_)) => {
            SORTED_SET_REMOVE_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SORTED_SET_REMOVE_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SORTED_SET_REMOVE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
