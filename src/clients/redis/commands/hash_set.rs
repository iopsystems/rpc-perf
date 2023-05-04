use super::*;

pub async fn hash_set(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::HashSet,
) -> std::result::Result<(), ResponseError> {
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
        let d: Vec<(&[u8], &[u8])> =
            request.data.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect();
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

    result
}
