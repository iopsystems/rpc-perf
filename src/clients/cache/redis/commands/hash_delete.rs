use super::*;

/// Delete a field from a hash stored in the cache.
pub async fn hash_delete(
    connection: &mut Connection<net::Stream>,
    config: &Config,
    request: workload::client::HashDelete,
) -> std::result::Result<(), ResponseError> {
    HASH_DELETE.increment();
    let fields: Vec<&[u8]> = request.fields.iter().map(|v| v.borrow()).collect();
    match timeout(
        config.client().unwrap().request_timeout(),
        connection.hdel::<&[u8], Vec<&[u8]>, usize>(&request.key, fields),
    )
    .await
    {
        Ok(Ok(_)) => {
            HASH_DELETE_OK.increment();
            Ok(())
        }
        Ok(Err(_)) => {
            HASH_DELETE_EX.increment();
            Err(ResponseError::Exception)
        }
        Err(_) => {
            HASH_DELETE_TIMEOUT.increment();
            Err(ResponseError::Timeout)
        }
    }
}
