use super::*;

pub async fn sorted_set_members(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SortedSetMembers,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_MEMBERS.increment();
    match timeout(
        config.client().unwrap().request_timeout(),
        ::redis::cmd("ZUNION")
            .arg(1)
            .arg(&*request.key)
            .arg("WITHSCORES")
            .query_async::<_, Vec<(Vec<u8>, f64)>>(connection),
    )
    .await
    {
        Ok(Ok(set)) => {
            if set.is_empty() {
                RESPONSE_MISS.increment();
            } else {
                RESPONSE_HIT.increment();
            }

            SORTED_SET_MEMBERS_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            SORTED_SET_MEMBERS_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            SORTED_SET_MEMBERS_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
