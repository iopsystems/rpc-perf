use super::*;

pub async fn add(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::Add,
) -> std::result::Result<(), ResponseError> {
    ADD.increment();

    let key = &*request.key;
    let value = &*request.value;

    let mut base_command = ::redis::cmd("SET");

    let mut command = base_command.arg(key).arg("NX").arg(value);

    if let Some(ttl) = request.ttl {
        if ttl.subsec_nanos() == 0 {
            command = base_command.arg(key).arg("NX").arg("EX").arg(ttl.as_secs()).arg(value);
        } else {
            command = base_command.arg(key).arg("NX").arg("PX").arg(ttl.as_millis() as u64).arg(value);
        }
    }

    match timeout(
        config.client().unwrap().request_timeout(),
        command.query_async::<::redis::aio::Connection<net::Stream>, Option<String>>(connection),
    )
    .await
    {
        Ok(Ok(s)) => {
            if s.is_none() {
                ADD_NOT_STORED.increment();
            } else {
                ADD_STORED.increment();
            }
            Ok(())
        }
        Ok(Err(_)) => {
            ADD_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            ADD_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
