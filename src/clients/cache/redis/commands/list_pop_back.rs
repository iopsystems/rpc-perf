use super::*;

/// Removes and returns the element from the back of a list.
pub async fn list_pop_back(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::ListPopBack,
) -> std::result::Result<(), ResponseError> {
    LIST_POP_BACK.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.rpop::<&[u8], Option<Vec<u8>>>(request.key.as_ref(), None),
    )
    .await
    {
        Ok(Ok(_)) => {
            LIST_POP_BACK_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            LIST_POP_BACK_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            LIST_POP_BACK_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
