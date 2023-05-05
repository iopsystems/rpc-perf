use super::*;

pub async fn sorted_set_score(
    client: &mut SimpleCacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::SortedSetScore,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_SCORE.increment();
    let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
    match timeout(
        config.client().unwrap().request_timeout(),
        client.sorted_set_get_score(cache_name, &*request.key, members),
    )
    .await
    {
        Ok(Ok(_)) => {
            SORTED_SET_SCORE_OK.increment();
            Ok(())
        }
        Ok(Err(e)) => {
            SORTED_SET_SCORE_EX.increment();
            Err(e.into())
        }
        Err(_) => {
            SORTED_SET_SCORE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
