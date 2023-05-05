use super::*;

pub async fn set_add(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SetAdd
) -> std::result::Result<(), ResponseError> {
    SET_ADD.increment();
    let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.set_add_elements(
            cache_name,
            &*request.key,
            members,
            CollectionTtl::new(None, false),
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            SET_ADD_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SET_ADD_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SET_ADD_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}