// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use ratelimit::Ratelimiter;
use rand_distr::WeightedAliasIndex;
use rand_distr::Distribution;
use std::sync::Arc;
use std::collections::HashSet;
use rand::distributions::Alphanumeric;
use rand::distributions::Uniform;
use rand_xoshiro::Xoshiro512PlusPlus;
use rand::Rng;

use super::*;
// use std::collections::HashMap;
use tokio::time::Interval;
use workload::WorkItem;

use config::Verb;

struct Keyspace {
    keys: Vec<Arc<[u8]>>,
    distribution: KeyDistribution,
    rng: Xoshiro512PlusPlus,
    verb: Vec<Verb>,
    verb_dist: WeightedAliasIndex<usize>,
}

pub struct KeyDistribution {
    inner: rand::distributions::Uniform<usize>,
}

impl Keyspace {
    pub fn new(nkeys: usize, length: usize) -> Self {
        let mut rng = Xoshiro512PlusPlus::seed_from_u64(0);
        let mut keys = HashSet::with_capacity(nkeys);
        while keys.len() < nkeys {
            let key = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(length)
                .collect::<Vec<u8>>();
            let _ = keys.insert(key);
        }
        let keys = keys.drain().map(|k| k.into()).collect();
        let distribution = KeyDistribution { inner: Uniform::new(0, nkeys) };
        Self {
            keys,
            distribution,
            rng: Xoshiro512PlusPlus::seed_from_u64(0),
            verb: vec![Verb::Get, Verb::Set],
            verb_dist: WeightedAliasIndex::new(vec![80, 20]).unwrap(),
        }
    }

    pub fn sample(&mut self) -> Arc<[u8]> {
        let index = self.distribution.inner.sample(&mut self.rng);
        self.keys[index].clone()
    }
}

pub fn requests(work_sender: Sender<WorkItem>, config: Config) -> Result<()> {
    let mut rng = Xoshiro512PlusPlus::seed_from_u64(0);

    let mut keyspace = Keyspace::new(10_000_000, 64);

    let ratelimiter = config.request().ratelimit().map(|r| Ratelimiter::new(1_000_000, 1, r.into()));

    while RUNNING.load(Ordering::Relaxed) {
        if let Some(ratelimiter) = ratelimiter {
            ratelimiter.wait();
        }

        // let quanta = if let Some((quanta, ref mut interval)) = ratelimit_params {
        //     interval.tick().await;
        //     quanta
        // } else {
        //     1
        // };


        // for _ in 0..quanta {
        let verb = &keyspace.verb[keyspace.verb_dist.sample(&mut rng)];

        let work_item = match verb {
            Verb::Get => WorkItem::Get {
                key: keyspace.sample(),
            },
            Verb::Set => WorkItem::Set {
                key: keyspace.sample(),
                value: (&mut rng)
                    .sample_iter(&Alphanumeric)
                    .take(128)
                    .collect::<Vec<u8>>().into(),
            },
            _ => {
                continue;
            }
        };
        // let work_item = if distr.sample(&mut rng) < 80 {
        //     WorkItem::Get {
        //         key: keyspace.sample(),
        //     }
        // } else {
        //     WorkItem::Set {
        //         key: keyspace.sample(),
        //         value: (&mut rng)
        //             .sample_iter(&Alphanumeric)
        //             .take(128)
        //             .collect::<Vec<u8>>().into(),
        //     }
        // };
        let _ = work_sender.send_blocking(work_item);
        // }

        // for _ in 0..quanta {
        //     let keyspace = config.choose_keyspace(&mut rng);
        //     let command = keyspace.choose_command(&mut rng);
        //     let work_item = match command.verb() {
        //         Verb::Get => WorkItem::Get {
        //             key: keyspace.generate_key(&mut rng).into(),
        //         },
        //         Verb::Set => WorkItem::Set {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             value: keyspace.generate_value(&mut rng).into(),
        //         },
        //         Verb::Delete => WorkItem::Delete {
        //             key: keyspace.generate_key(&mut rng).into(),
        //         },
        //         Verb::HashGet => WorkItem::HashGet {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             field: keyspace.generate_inner_key(&mut rng).unwrap().into(),
        //         },
        //         Verb::HashDelete => WorkItem::HashDelete {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             fields: vec![keyspace.generate_inner_key(&mut rng).unwrap().into()],
        //         },
        //         Verb::HashExists => WorkItem::HashExists {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             field: keyspace.generate_inner_key(&mut rng).unwrap().into(),
        //         },
        //         Verb::HashMultiGet => WorkItem::HashDelete {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             fields: vec![keyspace.generate_inner_key(&mut rng).unwrap().into()],
        //         },
        //         Verb::HashSet => {
        //             let mut data = HashMap::new();
        //             data.insert(
        //                 keyspace.generate_inner_key(&mut rng).unwrap().into(),
        //                 keyspace.generate_value(&mut rng).into(),
        //             );
        //             WorkItem::HashSet {
        //                 key: keyspace.generate_key(&mut rng).into(),
        //                 data,
        //             }
        //         }
        //         Verb::MultiGet => WorkItem::MultiGet {
        //             keys: vec![keyspace.generate_key(&mut rng).into()],
        //         },
        //         Verb::Ping => WorkItem::Ping {},
        //         Verb::SortedSetAdd => WorkItem::SortedSetAdd {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             members: vec![(
        //                 keyspace.generate_inner_key(&mut rng).unwrap().into(),
        //                 rng.gen(),
        //             )],
        //         },
        //         Verb::SortedSetRemove => WorkItem::SortedSetRemove {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             members: vec![keyspace.generate_inner_key(&mut rng).unwrap().into()],
        //         },
        //         Verb::SortedSetIncrement => WorkItem::SortedSetIncrement {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             member: keyspace.generate_inner_key(&mut rng).unwrap().into(),
        //             amount: rng.gen(),
        //         },
        //         Verb::SortedSetScore => WorkItem::SortedSetScore {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             member: keyspace.generate_inner_key(&mut rng).unwrap().into(),
        //         },
        //         Verb::SortedSetMultiScore => WorkItem::SortedSetMultiScore {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             members: vec![keyspace.generate_inner_key(&mut rng).unwrap().into()],
        //         },
        //         Verb::SortedSetRank => WorkItem::SortedSetRank {
        //             key: keyspace.generate_key(&mut rng).into(),
        //             member: keyspace.generate_inner_key(&mut rng).unwrap().into(),
        //         },
        //         Verb::SortedSetRange => {
        //             todo!()
        //             // WorkItem::SortedSetRange {
        //             //     key: keyspace.generate_key(&mut rng).into(),
        //             //     start: 0,
        //             //     stop: -1,
        //             // }
        //         }
        //     };

        //     let _ = work_sender.send(work_item).await;
        // }
    }

    Ok(())
}

pub async fn reconnect(work_sender: Sender<WorkItem>, config: Config) -> Result<()> {
    let rate = config.connection().reconnect_rate();

    let mut ratelimit_params = if rate.is_some() {
        Some(convert_ratelimit(rate.unwrap()))
    } else {
        // NOTE: we treat reconnect differently and don't generate any reconnects
        // if there is no ratelimit specified.
        return Ok(());
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
