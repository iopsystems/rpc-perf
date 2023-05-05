use super::*;

pub async fn set_members(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetMembers
) -> std::result::Result<(), ResponseError> {
    SET_MEMBERS.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.set_fetch(cache_name, &*request.key),
    )
    .await
    {
        Ok(Ok(_)) => {
            SET_MEMBERS_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SET_MEMBERS_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SET_MEMBERS_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}