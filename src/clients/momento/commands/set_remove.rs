use super::*;

/// Removes a member (element) from a set.
pub async fn set_remove(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetRemove,
) -> std::result::Result<(), ResponseError> {
    SET_REMOVE.increment();
    let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.set_remove_elements(cache_name, &*request.key, members),
    )
    .await
    {
        Ok(Ok(_)) => {
            SET_REMOVE_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SET_REMOVE_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SET_REMOVE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
