use super::*;
use ::redis::RedisError;
use std::result::Result;
use tokio::time::error::Elapsed;

/// Adds one or more members to a set.
///
/// NOTE: if a TTL is specified for the keyspace, a second command is issued to
/// set the ttl for the key if a TTL is not already set. The operation to set
/// the expiration may fail and will not be retried. Both the `SADD` and
/// `EXPIRE`/`PEXPIRE` commands will count towards the request latency. The
/// success/failure of the command to set the expiration does not count towards
/// the request metrics (such as the number of requests, success rate, etc).
pub async fn set_add(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SetAdd,
) -> std::result::Result<(), ResponseError> {
    if request.members.is_empty() {
        return Ok(());
    }

    SET_ADD.increment();

    let result = if request.members.len() == 1 {
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
