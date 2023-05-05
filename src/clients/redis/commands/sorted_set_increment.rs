use super::*;

pub async fn sorted_set_increment(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SortedSetIncrement,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_INCR.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.zincr::<&[u8], &[u8], f64, String>(
            &request.key,
            &request.member,
            request.amount,
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            SORTED_SET_INCR_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            SORTED_SET_INCR_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            SORTED_SET_INCR_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
