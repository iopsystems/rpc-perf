use super::*;

/// Sets a key-value pair in the cache if the key already exists.
pub async fn replace(
    connection: &mut MultiplexedConnection,
    config: &Config,
    request: workload::client::Replace,
) -> std::result::Result<(), ResponseError> {
    REPLACE.increment();

    let key = &*request.key;
    let value = &*request.value;

    let mut base_command = ::redis::cmd("SET");

    let mut command = base_command.arg(key).arg("XX").arg(value);

    if let Some(ttl) = request.ttl {
        if ttl.subsec_nanos() == 0 {
            command = base_command
                .arg(key)
                .arg("XX")
                .arg("EX")
                .arg(ttl.as_secs())
                .arg(value);
        } else {
            command = base_command
                .arg(key)
                .arg("XX")
                .arg("PX")
                .arg(ttl.as_millis() as u64)
                .arg(value);
        }
    }

    match timeout(
        config.client().unwrap().request_timeout(),
        command.query_async::<MultiplexedConnection, Option<String>>(connection),
    )
    .await
    {
        Ok(Ok(s)) => {
            if s.is_none() {
                REPLACE_NOT_STORED.increment();
            } else {
                REPLACE_STORED.increment();
            }
            Ok(())
        }
        Ok(Err(_)) => {
            REPLACE_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            REPLACE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
