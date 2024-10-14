use super::*;

/// Removes and returns an element from the front of a list.
pub async fn list_pop_front(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::ListPopFront,
) -> std::result::Result<(), ResponseError> {
    LIST_POP_FRONT.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.lpop::<&[u8], Option<Vec<u8>>>(request.key.as_ref(), None),
    )
    .await
    {
        Ok(Ok(_)) => {
            LIST_POP_FRONT_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            LIST_POP_FRONT_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            LIST_POP_FRONT_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
