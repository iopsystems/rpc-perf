use super::*;

use ::momento::cache::{CollectionTtl, ListConcatenateFrontRequest, ListPushFrontRequest};

/// Pushes an item onto the front of a list.
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn list_push_front(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::ListPushFront,
) -> std::result::Result<(), ResponseError> {
    LIST_PUSH_FRONT.increment();

    if request.elements.len() == 1 {
        let r = ListPushFrontRequest::new(cache_name, &*request.key, &*request.elements[0])
            .truncate_back_to_size(request.truncate)
            .ttl(CollectionTtl::new(request.ttl, false));

        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.send_request(r),
        )
        .await;

        record_result!(result, LIST_PUSH_FRONT)
    } else {
        // note: we need to reverse because the semantics of list
        // concat do not match the redis push semantics
        let elements: Vec<&[u8]> = request.elements.iter().map(|v| &**v).rev().collect();

        let r = ListConcatenateFrontRequest::new(cache_name, &*request.key, elements)
            .truncate_back_to_size(request.truncate)
            .ttl(CollectionTtl::new(None, false));

        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.send_request(r),
        )
        .await;

        record_result!(result, LIST_PUSH_FRONT)
    }
}
