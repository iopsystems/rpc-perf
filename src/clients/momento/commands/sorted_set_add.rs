use super::*;

pub async fn sorted_set_add(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetAdd,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_ADD.increment();
    let members: Vec<sorted_set::SortedSetElement> = request
        .members
        .iter()
        .map(|(value, score)| sorted_set::SortedSetElement {
            value: value.to_vec(),
            score: *score,
        })
        .collect();
    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_put(
            cache_name,
            &*request.key,
            members,
            CollectionTtl::new(request.ttl, false),
        ),
    )
    .await;
    record_result!(result, SORTED_SET_ADD)
}
