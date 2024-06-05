use super::*;

use ::momento::cache::{CollectionTtl, DictionarySetFieldRequest, DictionarySetFieldsRequest};

/// Set the value for a field in a hash (dictionary).
///
/// NOTE: if a TTL is specified, this command will not refresh the TTL for the
/// collection.
pub async fn hash_set(
    client: &mut CacheClient,
    config: &Config,
    cache_name: &str,
    request: workload::client::HashSet,
) -> std::result::Result<(), ResponseError> {
    if request.data.is_empty() {
        panic!("empty data for hash set");
    }

    HASH_SET.increment();

    if request.data.len() == 1 {
        let (field, value) = request.data.into_iter().next().unwrap();

        let r = DictionarySetFieldRequest::new(
            cache_name,
            &*request.key,
            &*field,
            value,
        )
        .ttl(CollectionTtl::new(request.ttl, false));

        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.send_request(r),
        )
        .await;

        record_result!(result, HASH_SET)
    } else {
        let d: Vec<(Vec<u8>, Vec<u8>)> = request
            .data
            .into_iter()
            .map(|(k, v)| (k.to_vec(), v))
            .collect();

        let r = DictionarySetFieldsRequest::new(cache_name, &*request.key, d)
            .ttl(CollectionTtl::new(request.ttl, false));

        let result = timeout(
            config.client().unwrap().request_timeout(),
            client.send_request(r),
        )
        .await;

        record_result!(result, HASH_SET)
    }
}
