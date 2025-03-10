use super::*;
use ::redis::RedisError;
use std::result::Result;
use tokio::time::error::Elapsed;

/// Pushes an element to the back of a list.
///
/// NOTE: if a TTL is specified for the keyspace, a second command is issued to
/// set the ttl for the key if a TTL is not already set. The operation to set
/// the expiration may fail and will not be retried. Both the `RPUSH` and
/// `EXPIRE`/`PEXPIRE` commands will count towards the request latency. The
/// success/failure of the command to set the expiration does not count towards
/// the request metrics (such as the number of requests, success rate, etc).
pub async fn list_push_back(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::ListPushBack,
) -> std::result::Result<(), ResponseError> {
    LIST_PUSH_BACK.increment();
    let elements: Vec<&[u8]> = request.elements.iter().map(|v| v.borrow()).collect();
    let mut result = match timeout(
        config.client().unwrap().request_timeout(),
        connection.rpush::<&[u8], &Vec<&[u8]>, u64>(request.key.as_ref(), &elements),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(_)) => Err(ResponseError::Exception),
        Err(_) => Err(ResponseError::Timeout),
    };

    if result.is_ok() {
        if let Some(len) = request.truncate {
            match timeout(
                config.client().unwrap().request_timeout(),
                connection.ltrim::<&[u8], ()>(request.key.as_ref(), -(len as isize + 1), -1),
            )
            .await
            {
                Ok(Ok(_)) => {
                    result = Ok(());
                }
                Ok(Err(_)) => {
                    result = Err(ResponseError::Exception);
                }
                Err(_) => {
                    result = Err(ResponseError::Timeout);
                }
            }
        }
    }

    match result {
        Ok(_) => {
            LIST_PUSH_BACK_OK.increment();
        }
        Err(ResponseError::Timeout) => {
            LIST_PUSH_BACK_TIMEOUT.increment();
        }
        Err(_) => {
            LIST_PUSH_BACK_EX.increment();
        }
    }

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
