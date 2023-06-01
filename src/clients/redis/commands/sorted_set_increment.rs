use ::redis::RedisError;
use tokio::time::error::Elapsed;
use std::result::Result;
use super::*;

pub async fn sorted_set_increment(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SortedSetIncrement,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_INCR.increment();
    let result = match timeout(
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
    };

    // If successful, we may need to set an expiration. This is best-effort only
    if result.is_ok() && request.ttl.is_some() {
        let ttl = request.ttl.unwrap();

        let (mut base_command, ttl) = if ttl.subsec_nanos() == 0 {
            (::redis::cmd("EXPIRE"), ttl.as_secs())
        } else {
            (::redis::cmd("PEXPIRE"), ttl.as_nanos() as u64)
        };

        let _: Result<Result<u64, RedisError>, Elapsed> = timeout(
            config.client().unwrap().request_timeout(),
            base_command.arg(&*request.key).arg(ttl).arg("NX").query_async(connection)
        ).await;
    }

    result
}
