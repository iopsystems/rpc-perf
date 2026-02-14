use super::*;

use ::momento::cache::GetBatchRequest;

pub async fn get_batch(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::GetBatch,
) -> std::result::Result<(), ResponseError> {
    if request.keys.is_empty() {
        return Ok(());
    }

    GETS.increment();

    // Record batch size in metrics
    let num_keys = request.keys.len() as u64;
    GETS_KEY.add(num_keys);
    let _ = GETS_CARDINALITY.increment(num_keys);
    let _ = KVGETBATCH_BATCH_SIZE.increment(num_keys);

    let keys: Vec<&[u8]> = request.keys.iter().map(|k| k.as_ref()).collect();
    let r = GetBatchRequest::new(cache_name, keys);

    let result = timeout(
        config.client().unwrap().request_timeout(),
        client.send_request(r),
    )
    .await;

    record_result!(result, GET)
}
