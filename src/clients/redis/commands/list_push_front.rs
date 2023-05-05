use super::*;

pub async fn list_push_front(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::ListPushFront,
) -> std::result::Result<(), ResponseError> {
    LIST_PUSH_FRONT.increment();
    let elements: Vec<&[u8]> = request.elements.iter().map(|v| v.borrow()).collect();
    let mut result = match timeout(
        config.client().unwrap().request_timeout(),
        connection.lpush::<&[u8], &Vec<&[u8]>, u64>(request.key.as_ref(), &elements),
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
                connection.ltrim::<&[u8], ()>(request.key.as_ref(), 0, len as _),
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
            LIST_PUSH_FRONT_OK.increment();
        }
        Err(ResponseError::Timeout) => {
            LIST_PUSH_FRONT_TIMEOUT.increment();
        }
        Err(_) => {
            LIST_PUSH_FRONT_EX.increment();
        }
    }

    result
}