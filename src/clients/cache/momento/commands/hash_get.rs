use super::*;

use ::momento::cache::DictionaryGetFieldsResponse;

/// Retrieve the value for one or more fields in a hash (dictionary).
pub async fn hash_get(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashGet,
) -> std::result::Result<(), ResponseError> {
    HASH_GET.increment();

    match timeout(
        config.client().unwrap().request_timeout(),
        client.dictionary_get_fields(
            cache_name,
            &*request.key,
            request.fields.iter().map(|f| &**f),
        ),
    )
    .await
    {
        Ok(Ok(r)) => match r {
            DictionaryGetFieldsResponse::Hit { .. } => {
                let mut hit = 0;
                let mut miss = 0;
                let dict: HashMap<Vec<u8>, Vec<u8>> = match r.try_into() {
                    Ok(d) => d,
                    Err(e) => {
                        HASH_GET_EX.increment();
                        return Err(e.into());
                    }
                };
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
            DictionaryGetFieldsResponse::Miss => {
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
