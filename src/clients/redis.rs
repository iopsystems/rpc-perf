use super::*;
use crate::net::Connector;
use crate::Instant;
use ::redis::{AsyncCommands, RedisConnectionInfo};
use std::borrow::Borrow;

/// Launch tasks with one conncetion per task as RESP protocol is not mux-enabled.
pub fn launch_tasks(runtime: &mut Runtime, config: Config, work_receiver: Receiver<WorkItem>) {
    debug!("launching resp protocol tasks");

    // create one task per "connection"
    // note: these may be channels instead of connections for multiplexed protocols
    for _ in 0..config.client().unwrap().poolsize() {
        for endpoint in config.target().endpoints() {
            runtime.spawn(task(
                work_receiver.clone(),
                endpoint.clone(),
                config.clone(),
            ));
        }
    }
}

#[allow(dead_code)]
#[allow(clippy::slow_vector_initialization)]
async fn task(work_receiver: Receiver<WorkItem>, endpoint: String, config: Config) -> Result<()> {
    trace!("launching resp task for endpoint: {endpoint}");
    let connector = Connector::new(&config)?;

    let redis_connection_info = RedisConnectionInfo {
        db: 0,
        username: None,
        password: None,
    };

    let mut connection = None;

    while RUNNING.load(Ordering::Relaxed) {
        if connection.is_none() {
            CONNECT.increment();
            connection = match timeout(
                config.client().unwrap().connect_timeout(),
                connector.connect(&endpoint),
            )
            .await
            {
                Ok(Ok(c)) => {
                    CONNECT_OK.increment();
                    CONNECT_CURR.add(1);
                    if let Ok(c) = ::redis::aio::Connection::new(&redis_connection_info, c).await {
                        Some(c)
                    } else {
                        CONNECT_EX.increment();
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                Ok(Err(e)) => {
                    trace!("error connecting: {e}");
                    CONNECT_EX.increment();
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(_) => {
                    trace!("connect timeout");
                    CONNECT_TIMEOUT.increment();
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            }
        }

        let mut con = connection.take().unwrap();
        let work_item = work_receiver
            .recv()
            .await
            .map_err(|_| Error::new(ErrorKind::Other, "channel closed"))?;

        REQUEST.increment();
        let start = Instant::now();
        let result = match work_item {
            WorkItem::Request { request, .. } => match request {
                ClientRequest::Get { key } => {
                    GET.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.get::<&[u8], Option<Vec<u8>>>(&key),
                    )
                    .await
                    {
                        Ok(Ok(None)) => {
                            RESPONSE_MISS.increment();
                            GET_KEY_MISS.increment();
                            Ok(())
                        }
                        Ok(Ok(Some(_))) => {
                            RESPONSE_HIT.increment();
                            GET_KEY_HIT.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            GET_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            GET_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::Set { key, value } => {
                    SET.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.set::<&[u8], &[u8], ()>(&key, &value),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            SET_STORED.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SET_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SET_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::Delete { key } => {
                    DELETE.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.del::<&[u8], ()>(&key),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            DELETE_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            DELETE_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            DELETE_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }

                /*
                 * HASHES (DICTIONARIES)
                 */
                ClientRequest::HashDelete { key, fields } => {
                    HASH_DELETE.increment();
                    let fields: Vec<&[u8]> = fields.iter().map(|v| v.borrow()).collect();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.hdel::<&[u8], Vec<&[u8]>, usize>(&key, fields),
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
                ClientRequest::HashExists { key, field } => {
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.hexists::<&[u8], &[u8], bool>(&key, &field),
                    )
                    .await
                    {
                        Ok(Ok(true)) => {
                            RESPONSE_HIT.increment();
                            HASH_EXISTS_HIT.increment();
                            Ok(())
                        }
                        Ok(Ok(false)) => {
                            RESPONSE_MISS.increment();
                            HASH_EXISTS_MISS.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                }
                ClientRequest::HashIncrement { key, field, amount } => {
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.hincr::<&[u8], &[u8], i64, i64>(&key, &field, amount),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    }
                }
                // transparently issues either a `hget` or `hmget`
                ClientRequest::HashGet { key, fields } => {
                    HASH_GET.increment();

                    let result = if fields.len() == 1 {
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.hget::<&[u8], &[u8], Option<Vec<u8>>>(&key, &fields[0]),
                        )
                        .await
                        {
                            Ok(Ok(Some(_))) => {
                                RESPONSE_HIT.increment();
                                HASH_GET_FIELD_HIT.increment();
                                Ok(())
                            }
                            Ok(Ok(None)) => {
                                RESPONSE_MISS.increment();
                                HASH_GET_FIELD_MISS.increment();
                                Ok(())
                            }
                            Ok(Err(_)) => Err(ResponseError::Exception),
                            Err(_) => Err(ResponseError::Timeout),
                        }
                    } else {
                        let fields: Vec<&[u8]> = fields.iter().map(|f| &**f).collect();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.hget::<&[u8], &[&[u8]], Option<Vec<Option<Vec<u8>>>>>(
                                &key, &fields,
                            ),
                        )
                        .await
                        {
                            Ok(Ok(Some(values))) => {
                                let mut hits = 0;
                                let mut misses = 0;
                                for value in values {
                                    if value.is_some() {
                                        hits += 1;
                                    } else {
                                        misses += 1;
                                    }
                                }
                                RESPONSE_HIT.add(hits);
                                RESPONSE_MISS.add(misses);
                                HASH_GET_FIELD_HIT.add(hits);
                                HASH_GET_FIELD_MISS.add(misses);
                                Ok(())
                            }
                            Ok(Ok(None)) => {
                                RESPONSE_MISS.add(fields.len() as _);
                                HASH_GET_FIELD_MISS.add(fields.len() as _);
                                Ok(())
                            }
                            Ok(Err(_)) => Err(ResponseError::Exception),
                            Err(_) => Err(ResponseError::Timeout),
                        }
                    };

                    match result {
                        Ok(()) => {
                            HASH_GET_OK.increment();
                        }
                        Err(ResponseError::Exception) => {
                            HASH_GET_EX.increment();
                        }
                        Err(ResponseError::Timeout) => {
                            HASH_GET_TIMEOUT.increment();
                        }
                        _ => {}
                    }

                    result
                }
                ClientRequest::HashGetAll { key } => {
                    HASH_GET_ALL.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.hgetall::<&[u8], Option<HashMap<Vec<u8>, Vec<u8>>>>(key.as_ref()),
                    )
                    .await
                    {
                        Ok(Ok(Some(_))) => {
                            RESPONSE_HIT.increment();
                            HASH_GET_ALL_OK.increment();
                            HASH_GET_ALL_HIT.increment();
                            Ok(())
                        }
                        Ok(Ok(None)) => {
                            RESPONSE_MISS.increment();
                            HASH_GET_ALL_OK.increment();
                            HASH_GET_ALL_MISS.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            HASH_GET_ALL_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            HASH_GET_ALL_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::HashSet { key, data } => {
                    if data.is_empty() {
                        panic!("empty data for hash set");
                    }

                    HASH_SET.increment();
                    let result = if data.len() == 1 {
                        let (field, value) = data.iter().next().unwrap();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.hset::<&[u8], &[u8], &[u8], ()>(
                                key.as_ref(),
                                field.as_ref(),
                                value.as_ref(),
                            ),
                        )
                        .await
                        {
                            Ok(Ok(_)) => Ok(()),
                            Ok(Err(_)) => Err(ResponseError::Exception),
                            Err(_) => Err(ResponseError::Timeout),
                        }
                    } else {
                        let d: Vec<(&[u8], &[u8])> =
                            data.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.hset_multiple::<&[u8], &[u8], &[u8], ()>(&key, &d),
                        )
                        .await
                        {
                            Ok(Ok(_)) => Ok(()),
                            Ok(Err(_)) => Err(ResponseError::Exception),
                            Err(_) => Err(ResponseError::Timeout),
                        }
                    };

                    match result {
                        Ok(_) => {
                            HASH_SET_OK.increment();
                        }
                        Err(ResponseError::Timeout) => {
                            HASH_SET_TIMEOUT.increment();
                        }
                        Err(_) => {
                            HASH_SET_EX.increment();
                        }
                    }

                    result
                }

                /*
                 * LISTS
                 */
                // To truncate, we must fuse an LTRIM at the end of the LPUSH
                ClientRequest::ListPushFront {
                    key,
                    elements,
                    truncate,
                } => {
                    LIST_PUSH_FRONT.increment();
                    let elements: Vec<&[u8]> = elements.iter().map(|v| v.borrow()).collect();
                    let mut result = match timeout(
                        config.client().unwrap().request_timeout(),
                        con.lpush::<&[u8], &Vec<&[u8]>, u64>(key.as_ref(), &elements),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    };

                    if result.is_ok() {
                        if let Some(len) = truncate {
                            match timeout(
                                config.client().unwrap().request_timeout(),
                                con.ltrim::<&[u8], ()>(key.as_ref(), 0, len as _),
                            )
                            .await
                            {
                                Ok(Ok(_)) => {
                                    result = Ok(());
                                }
                                Ok(Err(_)) => {
                                    result = Err(ResponseError::Exception);
                                }
                                Err(_) => {
                                    result = Err(ResponseError::Timeout);
                                }
                            }
                        }
                    }

                    match result {
                        Ok(_) => {
                            LIST_PUSH_FRONT_OK.increment();
                        }
                        Err(ResponseError::Timeout) => {
                            LIST_PUSH_FRONT_TIMEOUT.increment();
                        }
                        Err(_) => {
                            LIST_PUSH_FRONT_EX.increment();
                        }
                    }

                    result
                }
                // To truncate, we must fuse an LTRIM at the end of the LPUSH
                ClientRequest::ListPushBack {
                    key,
                    elements,
                    truncate,
                } => {
                    LIST_PUSH_BACK.increment();
                    let elements: Vec<&[u8]> = elements.iter().map(|v| v.borrow()).collect();
                    let mut result = match timeout(
                        config.client().unwrap().request_timeout(),
                        con.rpush::<&[u8], &Vec<&[u8]>, u64>(key.as_ref(), &elements),
                    )
                    .await
                    {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(_)) => Err(ResponseError::Exception),
                        Err(_) => Err(ResponseError::Timeout),
                    };

                    if result.is_ok() {
                        if let Some(len) = truncate {
                            match timeout(
                                config.client().unwrap().request_timeout(),
                                con.ltrim::<&[u8], ()>(key.as_ref(), -(len as isize + 1), -1),
                            )
                            .await
                            {
                                Ok(Ok(_)) => {
                                    result = Ok(());
                                }
                                Ok(Err(_)) => {
                                    result = Err(ResponseError::Exception);
                                }
                                Err(_) => {
                                    result = Err(ResponseError::Timeout);
                                }
                            }
                        }
                    }

                    match result {
                        Ok(_) => {
                            LIST_PUSH_BACK_OK.increment();
                        }
                        Err(ResponseError::Timeout) => {
                            LIST_PUSH_BACK_TIMEOUT.increment();
                        }
                        Err(_) => {
                            LIST_PUSH_BACK_EX.increment();
                        }
                    }

                    result
                }
                ClientRequest::ListFetch { key } => {
                    LIST_FETCH.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.lrange::<&[u8], Option<Vec<Vec<u8>>>>(key.as_ref(), 0, -1),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            LIST_FETCH_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            LIST_FETCH_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            LIST_FETCH_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::ListLength { key } => {
                    LIST_LENGTH.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.llen::<&[u8], Option<u64>>(key.as_ref()),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            LIST_LENGTH_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            LIST_LENGTH_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            LIST_LENGTH_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::ListPopFront { key } => {
                    LIST_POP_FRONT.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.lpop::<&[u8], Option<Vec<u8>>>(key.as_ref(), None),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            LIST_POP_FRONT_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            LIST_POP_FRONT_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            LIST_POP_FRONT_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::ListPopBack { key } => {
                    LIST_POP_BACK.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.rpop::<&[u8], Option<Vec<u8>>>(key.as_ref(), None),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            LIST_POP_BACK_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            LIST_POP_BACK_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            LIST_POP_BACK_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }

                ClientRequest::Ping { .. } => {
                    PING.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        ::redis::cmd("PING").query_async(&mut con),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            PING_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            PING_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => Err(ResponseError::Timeout),
                    }
                }

                /*
                 * SETS
                 */
                ClientRequest::SetAdd { key, members } => {
                    SET_ADD.increment();
                    if members.is_empty() {
                        connection = Some(con);
                        continue;
                    } else if members.len() == 1 {
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.sadd::<&[u8], &[u8], u64>(key.as_ref(), &*members[0]),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                SET_ADD_OK.increment();
                                Ok(())
                            }
                            Ok(Err(_)) => {
                                SET_ADD_EX.increment();
                                Err(ResponseError::Exception)
                            }
                            Err(_) => {
                                SET_ADD_TIMEOUT.increment();
                                Err(ResponseError::Timeout)
                            }
                        }
                    } else {
                        let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.sadd::<&[u8], &Vec<&[u8]>, u64>(&key, &members),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                SET_ADD_OK.increment();
                                Ok(())
                            }
                            Ok(Err(_)) => {
                                SET_ADD_EX.increment();
                                Err(ResponseError::Exception)
                            }
                            Err(_) => {
                                SET_ADD_TIMEOUT.increment();
                                Err(ResponseError::Timeout)
                            }
                        }
                    }
                }
                ClientRequest::SetMembers { key } => {
                    SET_MEMBERS.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.smembers::<&[u8], Option<Vec<Vec<u8>>>>(key.as_ref()),
                    )
                    .await
                    {
                        Ok(Ok(set)) => {
                            if set.is_some() {
                                RESPONSE_HIT.increment();
                            } else {
                                RESPONSE_MISS.increment();
                            }

                            SET_MEMBERS_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SET_MEMBERS_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SET_MEMBERS_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::SetRemove { key, members } => {
                    SET_REMOVE.increment();
                    if members.is_empty() {
                        connection = Some(con);
                        continue;
                    } else if members.len() == 1 {
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.srem::<&[u8], &[u8], u64>(key.as_ref(), &*members[0]),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                SET_REMOVE_OK.increment();
                                Ok(())
                            }
                            Ok(Err(_)) => {
                                SET_REMOVE_EX.increment();
                                Err(ResponseError::Exception)
                            }
                            Err(_) => {
                                SET_REMOVE_TIMEOUT.increment();
                                Err(ResponseError::Timeout)
                            }
                        }
                    } else {
                        let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.srem::<&[u8], &Vec<&[u8]>, u64>(&key, &members),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                SET_REMOVE_OK.increment();
                                Ok(())
                            }
                            Ok(Err(_)) => {
                                SET_REMOVE_EX.increment();
                                Err(ResponseError::Exception)
                            }
                            Err(_) => {
                                SET_REMOVE_TIMEOUT.increment();
                                Err(ResponseError::Timeout)
                            }
                        }
                    }
                }

                /*
                 * SORTED SETS
                 */
                ClientRequest::SortedSetAdd { key, members } => {
                    SORTED_SET_ADD.increment();
                    if members.is_empty() {
                        connection = Some(con);
                        continue;
                    } else if members.len() == 1 {
                        let (member, score) = members.first().unwrap();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.zadd::<&[u8], f64, &[u8], f64>(
                                key.as_ref(),
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
                        let d: Vec<(f64, &[u8])> =
                            members.iter().map(|(m, s)| (*s, m.as_ref())).collect();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.zadd_multiple::<&[u8], f64, &[u8], f64>(&key, &d),
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
                ClientRequest::SortedSetMembers { key } => {
                    SORTED_SET_INCR.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        ::redis::cmd("ZUNION")
                            .arg(1)
                            .arg(&*key)
                            .arg("WITHSCORES")
                            .query_async::<_, Vec<(Vec<u8>, f64)>>(&mut con),
                    )
                    .await
                    {
                        Ok(Ok(set)) => {
                            if set.is_empty() {
                                RESPONSE_MISS.increment();
                            } else {
                                RESPONSE_HIT.increment();
                            }

                            SORTED_SET_INCR_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SORTED_SET_INCR_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SORTED_SET_INCR_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::SortedSetIncrement {
                    key,
                    member,
                    amount,
                } => {
                    SORTED_SET_INCR.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.zincr::<&[u8], &[u8], f64, String>(&key, &member, amount),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            SORTED_SET_INCR_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SORTED_SET_INCR_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SORTED_SET_INCR_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::SortedSetRemove { key, members } => {
                    SORTED_SET_REMOVE.increment();
                    let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.zrem::<&[u8], Vec<&[u8]>, usize>(&key, members),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            SORTED_SET_REMOVE_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SORTED_SET_REMOVE_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SORTED_SET_REMOVE_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                ClientRequest::SortedSetScore { key, members } => {
                    SORTED_SET_SCORE.increment();

                    let result = if members.len() == 1 {
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.zscore::<&[u8], &[u8], Option<f64>>(
                                key.as_ref(),
                                members[0].as_ref(),
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
                        let members: Vec<&[u8]> = members.iter().map(|v| v.borrow()).collect();
                        match timeout(
                            config.client().unwrap().request_timeout(),
                            con.zscore_multiple::<&[u8], &[u8], Vec<Option<f64>>>(
                                key.as_ref(),
                                &members,
                            ),
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
                ClientRequest::SortedSetRank { key, member } => {
                    SORTED_SET_RANK.increment();
                    match timeout(
                        config.client().unwrap().request_timeout(),
                        con.zrank::<&[u8], &[u8], Option<u64>>(key.as_ref(), member.as_ref()),
                    )
                    .await
                    {
                        Ok(Ok(rank)) => {
                            if rank.is_some() {
                                RESPONSE_HIT.increment();
                            } else {
                                RESPONSE_MISS.increment();
                            }

                            SORTED_SET_RANK_OK.increment();
                            Ok(())
                        }
                        Ok(Err(_)) => {
                            SORTED_SET_RANK_EX.increment();
                            Err(ResponseError::Exception)
                        }
                        Err(_) => {
                            SORTED_SET_RANK_TIMEOUT.increment();
                            Err(ResponseError::Timeout)
                        }
                    }
                }
                _ => {
                    REQUEST_UNSUPPORTED.increment();
                    connection = Some(con);
                    continue;
                }
            },
            WorkItem::Reconnect => {
                CONNECT_CURR.sub(1);
                continue;
            }
        };

        REQUEST_OK.increment();

        let stop = Instant::now();

        let latency_ns = stop.duration_since(start).as_nanos();

        match result {
            Ok(_) => {
                connection = Some(con);
                RESPONSE_OK.increment();

                REQUEST_LATENCY.increment(start, latency_ns, 1);
                RESPONSE_LATENCY.increment(stop, latency_ns, 1);
            }
            Err(ResponseError::Exception) => {
                CONNECT_CURR.sub(1);
                RESPONSE_EX.increment();
            }
            Err(ResponseError::Timeout) => {
                CONNECT_CURR.sub(1);
                RESPONSE_TIMEOUT.increment();
            }
            Err(ResponseError::Ratelimited) => {
                RESPONSE_RATELIMITED.increment();
                connection = Some(con);
            }
            Err(ResponseError::BackendTimeout) => {
                RESPONSE_BACKEND_TIMEOUT.increment();
                connection = Some(con);
            }
        }
    }

    Ok(())
}
