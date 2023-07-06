use super::*;

/// Return the members of a set.
pub async fn set_members(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SetMembers,
) -> std::result::Result<(), ResponseError> {
    SET_MEMBERS.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.smembers::<&[u8], Option<Vec<Vec<u8>>>>(request.key.as_ref()),
    )
    .await
    {
        Ok(Ok(set)) => {
            if set.is_some() {
                RESPONSE_HIT.increment();
            } else {
                RESPONSE_MISS.increment();
            }

            SET_MEMBERS_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            SET_MEMBERS_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            SET_MEMBERS_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
