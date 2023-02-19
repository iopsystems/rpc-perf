// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use rand::Rng;

use super::*;
use std::collections::HashMap;
use tokio::time::Interval;
use workload::WorkItem;

pub async fn requests(work_sender: Sender<WorkItem>, config: Config) -> Result<()> {
    let mut rng = Xoshiro256Plus::seed_from_u64(0);

    let rate = NonZeroU64::new(config.request().ratelimit().unwrap_or(0) as u64);

    info!("request ratelimit: {:?}", rate);

    let mut ratelimit_params = if rate.is_some() {
        Some(convert_ratelimit(rate.unwrap()))
    } else {
        None
    };

    while RUNNING.load(Ordering::Relaxed) {
        let quanta = if let Some((quanta, ref mut interval)) = ratelimit_params {
            interval.tick().await;
            quanta
        } else {
            1
        };

        for _ in 0..quanta {
            let keyspace = config.choose_keyspace(&mut rng);
            let command = keyspace.choose_command(&mut rng);
            let work_item = match command.verb() {
                Verb::Get => WorkItem::Get {
                    key: keyspace.generate_key(&mut rng).into(),
                },
                Verb::Set => WorkItem::Set {
                    key: keyspace.generate_key(&mut rng).into(),
                    value: keyspace.generate_value(&mut rng).into(),
                },
                Verb::Delete => WorkItem::Delete {
                    key: keyspace.generate_key(&mut rng).into(),
                },
                Verb::HashGet => WorkItem::HashGet {
                    key: keyspace.generate_key(&mut rng).into(),
                    field: keyspace.generate_inner_key(&mut rng).unwrap().into(),
                },
                Verb::HashDelete => WorkItem::HashDelete {
                    key: keyspace.generate_key(&mut rng).into(),
                    fields: vec![keyspace.generate_inner_key(&mut rng).unwrap().into()],
                },
                Verb::HashExists => WorkItem::HashExists {
                    key: keyspace.generate_key(&mut rng).into(),
                    field: keyspace.generate_inner_key(&mut rng).unwrap().into(),
                },
                Verb::HashMultiGet => WorkItem::HashDelete {
                    key: keyspace.generate_key(&mut rng).into(),
                    fields: vec![keyspace.generate_inner_key(&mut rng).unwrap().into()],
                },
                Verb::HashSet => {
                    let mut data = HashMap::new();
                    data.insert(
                        keyspace.generate_inner_key(&mut rng).unwrap().into(),
                        keyspace.generate_value(&mut rng).into(),
                    );
                    WorkItem::HashSet {
                        key: keyspace.generate_key(&mut rng).into(),
                        data,
                    }
                }
                Verb::MultiGet => WorkItem::MultiGet {
                    keys: vec![keyspace.generate_key(&mut rng).into()],
                },
                Verb::Ping => WorkItem::Ping {},
                Verb::SortedSetAdd => WorkItem::SortedSetAdd {
                    key: keyspace.generate_key(&mut rng).into(),
                    members: vec![(
                        keyspace.generate_inner_key(&mut rng).unwrap().into(),
                        rng.gen(),
                    )],
                },
                Verb::SortedSetRemove => WorkItem::SortedSetRemove {
                    key: keyspace.generate_key(&mut rng).into(),
                    members: vec![keyspace.generate_inner_key(&mut rng).unwrap().into()],
                },
                Verb::SortedSetIncrement => WorkItem::SortedSetIncrement {
                    key: keyspace.generate_key(&mut rng).into(),
                    member: keyspace.generate_inner_key(&mut rng).unwrap().into(),
                    amount: rng.gen(),
                },
                Verb::SortedSetScore => WorkItem::SortedSetScore {
                    key: keyspace.generate_key(&mut rng).into(),
                    member: keyspace.generate_inner_key(&mut rng).unwrap().into(),
                },
                Verb::SortedSetMultiScore => WorkItem::SortedSetMultiScore {
                    key: keyspace.generate_key(&mut rng).into(),
                    members: vec![keyspace.generate_inner_key(&mut rng).unwrap().into()],
                },
                Verb::SortedSetRank => WorkItem::SortedSetRank {
                    key: keyspace.generate_key(&mut rng).into(),
                    member: keyspace.generate_inner_key(&mut rng).unwrap().into(),
                },
                Verb::SortedSetRange => {
                    todo!()
                    // WorkItem::SortedSetRange {
                    //     key: keyspace.generate_key(&mut rng).into(),
                    //     start: 0,
                    //     stop: -1,
                    // }
                }
            };

            let _ = work_sender.send(work_item).await;
        }
    }

    Ok(())
}

pub async fn reconnect(work_sender: Sender<WorkItem>, config: Config) -> Result<()> {
    let rate = NonZeroU64::new(config.connection().reconnect().unwrap_or(0) as u64);

    let mut ratelimit_params = if rate.is_some() {
        Some(convert_ratelimit(rate.unwrap()))
    } else {
        None
    };

    while RUNNING.load(Ordering::Relaxed) {
        let quanta = if let Some((quanta, ref mut interval)) = ratelimit_params {
            interval.tick().await;
            quanta
        } else {
            1
        };

        for _ in 0..quanta {
            let _ = work_sender.send(WorkItem::Reconnect).await;
        }
    }

    Ok(())
}

pub fn convert_ratelimit(rate: NonZeroU64) -> (u64, Interval) {
    let rate = u64::from(rate);

    // TODO: this gives approximate rates
    //
    // timer granularity should be millisecond level on most platforms
    // for higher rates, we can insert multiple work items every interval
    let (quanta, interval) = if rate <= 1000 {
        (1, 1000 / rate)
    } else {
        (rate / 1000, 1)
    };

    (
        quanta,
        ::tokio::time::interval(Duration::from_millis(interval)),
    )
}
