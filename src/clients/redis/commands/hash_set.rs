use ::redis::RedisError;
use tokio::time::error::Elapsed;
use super::*;

use std::result::Result;

/// Sets the value for a field within a hash.
///
/// NOTE: if a TTL is specified for the keyspace, a second command is issued to
/// set the ttl for the key if a TTL is not already set. The operation to set
/// the expiration may fail and will not be retried. Both the `HSET` and
/// `EXPIRE`/`PEXPIRE` commands will count towards the request latency. The
/// success/failure of the command to set the expiration does not count towards
/// the request metrics (such as the number of requests, success rate, etc).
pub async fn hash_set(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::HashSet,
) -> Result<(), ResponseError> {
    if request.data.is_empty() {
        panic!("empty data for hash set");
    }

    HASH_SET.increment();
    let result = if request.data.len() == 1 {
        let (field, value) = request.data.iter().next().unwrap();
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.hset::<&[u8], &[u8], &[u8], ()>(
                request.key.as_ref(),
                field.as_ref(),
                value.as_ref(),
            ),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => Err(ResponseError::Exception),
            Err(_) => Err(ResponseError::Timeout),
        }
    } else {
        let d: Vec<(&[u8], &[u8])> = request
            .data
            .iter()
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
            .collect();
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.hset_multiple::<&[u8], &[u8], &[u8], ()>(&request.key, &d),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(_)) => Err(ResponseError::Exception),
            Err(_) => Err(ResponseError::Timeout),
        }
    };

    match result {
        Ok(_) => {
            HASH_SET_OK.increment();
        }
        Err(ResponseError::Timeout) => {
            HASH_SET_TIMEOUT.increment();
        }
        Err(_) => {
            HASH_SET_EX.increment();
        }
    }

    // If set was successful, we may need to set an expiration. This is
    // best-effort and could fail if the connection is unreliable or a timeout
    // occurs.
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
