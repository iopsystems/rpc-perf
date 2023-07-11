use super::*;

/// Pushes an item onto the back of a list.
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn list_push_back(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListPushBack,
) -> std::result::Result<(), ResponseError> {
    LIST_PUSH_BACK.increment();
    if request.elements.len() == 1 {
        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.list_push_back(
                cache_name,
                &*request.key,
                &*request.elements[0],
                request.truncate,
                CollectionTtl::new(request.ttl, false),
            ),
        )
        .await;
        record_result!(result, LIST_PUSH_BACK)
    } else {
        // note: we need to reverse because the semantics of list
        // concat do not match the redis push semantics
        let elements: Vec<&[u8]> = request.elements.iter().map(|v| v.borrow()).rev().collect();
        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.list_concat_back(
                cache_name,
                &*request.key,
                elements,
                request.truncate,
                CollectionTtl::new(None, false),
            ),
        )
        .await;
        record_result!(result, LIST_PUSH_BACK)
    }
}
