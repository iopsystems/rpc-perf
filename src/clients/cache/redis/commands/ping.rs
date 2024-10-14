use super::*;

/// Sends a `PING` and expects a `PONG` response from the server.
pub async fn ping(
    connection: &mut MultiplexedConnection,
    config: &Config,
    _request: workload::client::Ping,
) -> std::result::Result<(), ResponseError> {
    PING.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        ::redis::cmd("PING").query_async(connection),
    )
    .await
    {
        Ok(Ok(())) => {
            PING_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            PING_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            PING_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
