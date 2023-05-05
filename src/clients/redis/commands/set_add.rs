use super::*;

pub async fn set_add(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SetAdd,
) -> std::result::Result<(), ResponseError> {
    if request.members.is_empty() {
        return Ok(());
    }

    SET_ADD.increment();

    if request.members.len() == 1 {
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.sadd::<&[u8], &[u8], u64>(request.key.as_ref(), &*request.members[0]),
        )
        .await
        {
            Ok(Ok(_)) => {
                SET_ADD_OK.increment();
                Ok(())
            }
            Ok(Err(_)) => {
                SET_ADD_EX.increment();
                Err(ResponseError::Exception)
            }
            Err(_) => {
                SET_ADD_TIMEOUT.increment();
                Err(ResponseError::Timeout)
            }
        }
    } else {
        let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.sadd::<&[u8], &Vec<&[u8]>, u64>(&request.key, &members),
        )
        .await
        {
            Ok(Ok(_)) => {
                SET_ADD_OK.increment();
                Ok(())
            }
            Ok(Err(_)) => {
                SET_ADD_EX.increment();
                Err(ResponseError::Exception)
            }
            Err(_) => {
                SET_ADD_TIMEOUT.increment();
                Err(ResponseError::Timeout)
            }
        }
    }
}