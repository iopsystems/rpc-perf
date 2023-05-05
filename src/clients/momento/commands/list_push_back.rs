use super::*;

pub async fn list_push_back(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListPushBack,
) -> std::result::Result<(), ResponseError> {
    LIST_PUSH_BACK.increment();
    let result = if request.elements.len() == 1 {
        match timeout(
            config.client().unwrap().request_timeout(),
            client.list_push_back(
                cache_name,
                &*request.key,
                &*request.elements[0],
                request.truncate,
                CollectionTtl::new(None, false),
            ),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(ResponseError::Timeout),
        }
    } else {
        // note: we need to reverse because the semantics of list
        // concat do not match the redis push semantics
        let elements: Vec<&[u8]> =
            request.elements.iter().map(|v| v.borrow()).rev().collect();
        match timeout(
            config.client().unwrap().request_timeout(),
            client.list_concat_back(
                cache_name,
                &*request.key,
                elements,
                request.truncate,
                CollectionTtl::new(None, false),
            ),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(ResponseError::Timeout),
        }
    };
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

    result
}