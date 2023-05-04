use super::*;

pub async fn hash_get(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::HashGet,
) -> std::result::Result<(), ResponseError> {
    HASH_GET.increment();

    let result = if request.fields.len() == 1 {
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.hget::<&[u8], &[u8], Option<Vec<u8>>>(&request.key, &request.fields[0]),
        )
        .await
        {
            Ok(Ok(Some(_))) => {
                RESPONSE_HIT.increment();
                HASH_GET_FIELD_HIT.increment();
                Ok(())
            }
            Ok(Ok(None)) => {
                RESPONSE_MISS.increment();
                HASH_GET_FIELD_MISS.increment();
                Ok(())
            }
            Ok(Err(_)) => Err(ResponseError::Exception),
            Err(_) => Err(ResponseError::Timeout),
        }
    } else {
        let fields: Vec<&[u8]> = request.fields.iter().map(|f| &**f).collect();
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.hget::<&[u8], &[&[u8]], Option<Vec<Option<Vec<u8>>>>>(
                &request.key, &fields,
            ),
        )
        .await
        {
            Ok(Ok(Some(values))) => {
                let mut hits = 0;
                let mut misses = 0;
                for value in values {
                    if value.is_some() {
                        hits += 1;
                    } else {
                        misses += 1;
                    }
                }
                RESPONSE_HIT.add(hits);
                RESPONSE_MISS.add(misses);
                HASH_GET_FIELD_HIT.add(hits);
                HASH_GET_FIELD_MISS.add(misses);
                Ok(())
            }
            Ok(Ok(None)) => {
                RESPONSE_MISS.add(fields.len() as _);
                HASH_GET_FIELD_MISS.add(fields.len() as _);
                Ok(())
            }
            Ok(Err(_)) => Err(ResponseError::Exception),
            Err(_) => Err(ResponseError::Timeout),
        }
    };

    match result {
        Ok(()) => {
            HASH_GET_OK.increment();
        }
        Err(ResponseError::Exception) => {
            HASH_GET_EX.increment();
        }
        Err(ResponseError::Timeout) => {
            HASH_GET_TIMEOUT.increment();
        }
        _ => {}
    }

    result
}
