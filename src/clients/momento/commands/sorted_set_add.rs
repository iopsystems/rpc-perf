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
    match timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_put(
            cache_name,
            &*request.key,
            members,
            CollectionTtl::new(request.ttl, false),
        ),
    )
    .await
    {
        Ok(Ok(_)) => {
            SORTED_SET_ADD_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SORTED_SET_ADD_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SORTED_SET_ADD_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
