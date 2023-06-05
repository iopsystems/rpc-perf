use super::*;

/// Returns the score of one or more members in a sorted set.
pub async fn sorted_set_score(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::SortedSetScore,
) -> std::result::Result<(), ResponseError> {
    SORTED_SET_SCORE.increment();

    let result = if request.members.len() == 1 {
        match timeout(
            config.client().unwrap().request_timeout(),
            connection.zscore::<&[u8], &[u8], Option<f64>>(
                request.key.as_ref(),
                request.members[0].as_ref(),
            ),
        )
        .await
        {
            Ok(Ok(score)) => {
                if score.is_some() {
                    RESPONSE_HIT.increment();
                } else {
                    RESPONSE_MISS.increment();
                }
                Ok(())
            }
            Ok(Err(_)) => Err(ResponseError::Exception),
            Err(_) => Err(ResponseError::Timeout),
        }
    } else {
        let members: Vec<&[u8]> = request.members.iter().map(|v| v.borrow()).collect();
        match timeout(
            config.client().unwrap().request_timeout(),
            connection
                .zscore_multiple::<&[u8], &[u8], Vec<Option<f64>>>(request.key.as_ref(), &members),
        )
        .await
        {
            Ok(Ok(scores)) => {
                for score in scores {
                    if score.is_some() {
                        RESPONSE_HIT.increment();
                    } else {
                        RESPONSE_MISS.increment();
                    }
                }
                Ok(())
            }
            Ok(Err(_)) => Err(ResponseError::Exception),
            Err(_) => Err(ResponseError::Timeout),
        }
    };

    match result {
        Ok(_) => {
            SORTED_SET_SCORE_OK.increment();
        }
        Err(ResponseError::Exception) => {
            SORTED_SET_SCORE_EX.increment();
        }
        Err(ResponseError::Timeout) => {
            SORTED_SET_SCORE_TIMEOUT.increment();
        }
        _ => {}
    }

    result
}
