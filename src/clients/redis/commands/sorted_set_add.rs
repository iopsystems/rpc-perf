use super::*;

pub async fn sorted_set_add(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SortedSetAdd,
) -> std::result::Result<(), ResponseError> {
    if request.members.is_empty() {
        return Ok(());
    }

    SORTED_SET_ADD.increment();

    if request.members.len() == 1 {
        let (member, score) = request.members.first().unwrap();
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.zadd::<&[u8], f64, &[u8], f64>(
                request.key.as_ref(),
                member.as_ref(),
                *score,
            ),
        )
        .await
        {
            Ok(Ok(_)) => {
                SORTED_SET_ADD_OK.increment();
                Ok(())
            }
            Ok(Err(_)) => {
                SORTED_SET_ADD_EX.increment();
                Err(ResponseError::Exception)
            }
            Err(_) => {
                SORTED_SET_ADD_TIMEOUT.increment();
                Err(ResponseError::Timeout)
            }
        }
    } else {
        let d: Vec<(f64, &[u8])> = request
            .members
            .iter()
            .map(|(m, s)| (*s, m.as_ref()))
            .collect();
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.zadd_multiple::<&[u8], f64, &[u8], f64>(&request.key, &d),
        )
        .await
        {
            Ok(Ok(_)) => {
                SORTED_SET_ADD_OK.increment();
                Ok(())
            }
            Ok(Err(_)) => {
                SORTED_SET_ADD_EX.increment();
                Err(ResponseError::Exception)
            }
            Err(_) => {
                SORTED_SET_ADD_TIMEOUT.increment();
                Err(ResponseError::Timeout)
            }
        }
    }
}
