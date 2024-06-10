use super::*;

use ::momento::cache::{CollectionTtl, SortedSetPutElementRequest, SortedSetPutElementsRequest};

pub async fn sorted_set_add(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetAdd,
) -> std::result::Result<(), ResponseError> {
    if request.members.is_empty() {
        return Ok(());
    }

    SORTED_SET_ADD.increment();

    if request.members.len() == 1 {
        let (member, score) = request.members.first().unwrap();

        let r = SortedSetPutElementRequest::new(cache_name, &*request.key, &**member, *score)
            .ttl(CollectionTtl::new(request.ttl, false));

        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.send_request(r),
        )
        .await;

        record_result!(result, SORTED_SET_ADD)
    } else {
        let d: Vec<(Vec<u8>, f64)> = request
            .members
            .into_iter()
            .map(|(m, s)| (m.to_vec(), s))
            .collect();

        let r = SortedSetPutElementsRequest::new(cache_name, &*request.key, d)
            .ttl(CollectionTtl::new(request.ttl, false));

        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.send_request(r),
        )
        .await;

        record_result!(result, SORTED_SET_ADD)
    }
}
