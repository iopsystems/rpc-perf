use super::*;
use ::redis::RedisError;
use std::result::Result;
use tokio::time::error::Elapsed;

/// Increment the score for a member of a sorted set.
///
/// NOTE: if a TTL is specified for the keyspace, a second command is issued to
/// set the ttl for the key if a TTL is not already set. The operation to set
/// the expiration may fail and will not be retried. Both the `ZINCR` and
/// `EXPIRE`/`PEXPIRE` commands will count towards the request latency. The
/// success/failure of the command to set the expiration does not count towards
/// the request metrics (such as the number of requests, success rate, etc).
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

        let fused_result: Result<Result<u64, RedisError>, Elapsed> = timeout(
            config.client().unwrap().request_timeout(),
            base_command
                .arg(&*request.key)
                .arg(ttl)
                .arg("NX")
                .query_async(connection),
        )
        .await;

        FUSED_REQUEST.increment();

        match fused_result {
            Ok(Ok(_)) => {
                FUSED_REQUEST_OK.increment();
            }
            Ok(Err(_)) => {
                FUSED_REQUEST_TIMEOUT.increment();
            }
            Err(_) => {
                FUSED_REQUEST_EX.increment();
            }
        }
    }

    result
}
