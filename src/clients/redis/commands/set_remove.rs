use super::*;

pub async fn set_remove(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SetRemove,
) -> std::result::Result<(), ResponseError> {
    if request.members.is_empty() {
        return Ok(());
    }

    SET_REMOVE.increment();

    if request.members.len() == 1 {
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.srem::<&[u8], &[u8], u64>(request.key.as_ref(), &*request.members[0]),
        )
        .await
        {
            Ok(Ok(_)) => {
                SET_REMOVE_OK.increment();
                Ok(())
            }
            Ok(Err(_)) => {
                SET_REMOVE_EX.increment();
                Err(ResponseError::Exception)
            }
            Err(_) => {
                SET_REMOVE_TIMEOUT.increment();
                Err(ResponseError::Timeout)
            }
        }
    } else {
        let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.srem::<&[u8], &Vec<&[u8]>, u64>(&request.key, &members),
        )
        .await
        {
            Ok(Ok(_)) => {
                SET_REMOVE_OK.increment();
                Ok(())
            }
            Ok(Err(_)) => {
                SET_REMOVE_EX.increment();
                Err(ResponseError::Exception)
            }
            Err(_) => {
                SET_REMOVE_TIMEOUT.increment();
                Err(ResponseError::Timeout)
            }
        }
    }
}