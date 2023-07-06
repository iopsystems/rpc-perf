use super::*;

/// Retrieve the value for one or more fields in a hash (dictionary).
pub async fn hash_get(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashGet,
) -> std::result::Result<(), ResponseError> {
    HASH_GET.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.dictionary_get(
            cache_name,
            &*request.key,
            request.fields.iter().map(|f| &**f).collect(),
        ),
    )
    .await
    {
        Ok(Ok(r)) => match r.dictionary {
            Some(dict) => {
                let mut hit = 0;
                let mut miss = 0;
                for field in request.fields {
                    if dict.contains_key(&*field) {
                        hit += 1;
                    } else {
                        miss += 1;
                    }
                }
                RESPONSE_HIT.add(hit);
                RESPONSE_MISS.add(miss);
                HASH_GET_FIELD_HIT.add(hit);
                HASH_GET_FIELD_MISS.add(miss);
                Ok(())
            }
            None => {
                RESPONSE_MISS.add(request.fields.len() as _);
                HASH_GET_FIELD_MISS.add(request.fields.len() as _);
                Ok(())
            }
        },
        Ok(Err(e)) => {
            HASH_GET_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            HASH_GET_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
