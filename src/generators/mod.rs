// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use config::Verb;
use core::num::NonZeroU64;
use rand::distributions::Alphanumeric;
use rand::distributions::Uniform;
use rand::Rng;
use rand::RngCore;
use rand::SeedableRng;
use rand_distr::Distribution;
use rand_distr::WeightedAliasIndex;
use rand_xoshiro::Xoshiro512PlusPlus;
use ratelimit::Ratelimiter;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Result;
use std::sync::Arc;
use tokio::time::Interval;
use workload::WorkItem;

#[derive(Clone)]
pub struct TrafficGenerator {
    ratelimiter: Option<Arc<Ratelimiter>>,
    keyspaces: Vec<Keyspace>,
    keyspace_dist: WeightedAliasIndex<usize>,
}

impl TrafficGenerator {
    pub fn new(config: &Config) -> Self {
        let ratelimiter = config
            .request()
            .ratelimit()
            .map(|r| Arc::new(Ratelimiter::new(1000, 1, r.into())));

        let mut keyspaces = Vec::new();
        let mut keyspace_weights = Vec::new();

        for keyspace in config.workload().keyspaces() {
            keyspaces.push(Keyspace::new(keyspace));
            keyspace_weights.push(keyspace.weight());
        }

        Self {
            ratelimiter,
            keyspaces,
            keyspace_dist: WeightedAliasIndex::new(keyspace_weights).unwrap(),
        }
    }

    pub fn generate(&self, rng: &mut dyn RngCore) -> WorkItem {
        if let Some(ref ratelimiter) = self.ratelimiter {
            ratelimiter.wait();
        }

        let keyspace = &self.keyspaces[self.keyspace_dist.sample(rng)];
        let verb = &keyspace.verbs[keyspace.verb_dist.sample(rng)];

        match verb {
            Verb::Get => WorkItem::Get {
                key: keyspace.sample(rng),
            },
            Verb::Set => WorkItem::Set {
                key: keyspace.sample(rng),
                value: rng
                    .sample_iter(&Alphanumeric)
                    .take(keyspace.vlen())
                    .collect::<Vec<u8>>()
                    .into(),
            },
            Verb::Delete => WorkItem::Delete {
                key: keyspace.sample(rng),
            },
            Verb::HashGet => WorkItem::HashGet {
                key: keyspace.sample(rng),
                field: keyspace.sample_inner(rng),
            },
            Verb::HashDelete => WorkItem::HashDelete {
                key: keyspace.sample(rng),
                fields: vec![keyspace.sample_inner(rng)],
            },
            Verb::HashExists => WorkItem::HashExists {
                key: keyspace.sample(rng),
                field: keyspace.sample_inner(rng),
            },
            Verb::HashSet => {
                let mut data = HashMap::new();
                data.insert(
                    keyspace.sample_inner(rng),
                    rng.sample_iter(&Alphanumeric)
                        .take(keyspace.vlen())
                        .collect::<Vec<u8>>()
                        .into(),
                );
                WorkItem::HashSet {
                    key: keyspace.sample(rng),
                    data,
                }
            }
            Verb::Ping => WorkItem::Ping {},
            Verb::SortedSetAdd => WorkItem::SortedSetAdd {
                key: keyspace.sample(rng),
                members: vec![(keyspace.sample_inner(rng), rng.gen())],
            },
            Verb::SortedSetRemove => WorkItem::SortedSetRemove {
                key: keyspace.sample(rng),
                members: vec![keyspace.sample_inner(rng)],
            },
            Verb::SortedSetIncrement => WorkItem::SortedSetIncrement {
                key: keyspace.sample(rng),
                member: keyspace.sample_inner(rng),
                amount: rng.gen(),
            },
            Verb::SortedSetScore => WorkItem::SortedSetScore {
                key: keyspace.sample(rng),
                member: keyspace.sample_inner(rng),
            },
            Verb::SortedSetMultiScore => WorkItem::SortedSetMultiScore {
                key: keyspace.sample(rng),
                members: vec![keyspace.sample_inner(rng)],
            },
            Verb::SortedSetRank => WorkItem::SortedSetRank {
                key: keyspace.sample(rng),
                member: keyspace.sample_inner(rng),
            },
        }
    }
}

#[derive(Clone)]
struct Keyspace {
    keys: Vec<Arc<[u8]>>,
    key_dist: KeyDistribution,
    verbs: Vec<Verb>,
    verb_dist: WeightedAliasIndex<usize>,
    inner_keys: Vec<Arc<[u8]>>,
    inner_key_dist: KeyDistribution,
    vlen: usize,
}

#[derive(Clone)]
pub struct KeyDistribution {
    inner: rand::distributions::Uniform<usize>,
}

impl Keyspace {
    pub fn new(keyspace: &config::Keyspace) -> Self {
        let nkeys = keyspace.nkeys();
        let klen = keyspace.klen();

        // we use a predictable seed to generate the keys in the keyspace
        let mut rng = Xoshiro512PlusPlus::seed_from_u64(0);
        let mut keys = HashSet::with_capacity(nkeys);
        while keys.len() < nkeys {
            let key = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(klen)
                .collect::<Vec<u8>>();
            let _ = keys.insert(key);
        }
        let keys = keys.drain().map(|k| k.into()).collect();
        let key_dist = KeyDistribution {
            inner: Uniform::new(0, nkeys),
        };

        let nkeys = keyspace.inner_keys_nkeys();
        let klen = keyspace.inner_keys_klen();

        // we use a predictable seed to generate the keys in the keyspace
        let mut rng = Xoshiro512PlusPlus::seed_from_u64(0);
        let mut inner_keys = HashSet::with_capacity(nkeys);
        while inner_keys.len() < nkeys {
            let key = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(klen)
                .collect::<Vec<u8>>();
            let _ = inner_keys.insert(key);
        }
        let inner_keys = inner_keys.drain().map(|k| k.into()).collect();
        let inner_key_dist = KeyDistribution {
            inner: Uniform::new(0, nkeys),
        };

        let mut verbs = Vec::new();
        let mut verb_weights = Vec::new();

        for command in keyspace.commands() {
            info!("verb: {:?} weight: {}", command.verb(), command.weight());
            verbs.push(command.verb());
            verb_weights.push(command.weight());
        }

        let verb_dist = WeightedAliasIndex::new(verb_weights).unwrap();

        Self {
            keys,
            key_dist,
            verbs,
            verb_dist,
            inner_keys,
            inner_key_dist,
            vlen: keyspace.vlen(),
        }
    }

    pub fn sample(&self, rng: &mut dyn RngCore) -> Arc<[u8]> {
        let index = self.key_dist.inner.sample(rng);
        self.keys[index].clone()
    }

    pub fn sample_inner(&self, rng: &mut dyn RngCore) -> Arc<[u8]> {
        let index = self.inner_key_dist.inner.sample(rng);
        self.inner_keys[index].clone()
    }

    pub fn vlen(&self) -> usize {
        self.vlen
    }
}

pub fn requests(work_sender: Sender<WorkItem>, generator: TrafficGenerator) -> Result<()> {
    // use a prng seeded from the entropy pool so that request generation is
    // unpredictable within the space and each individual request generation
    // task will generate a unique sequence
    let mut rng = Xoshiro512PlusPlus::from_entropy();

    while RUNNING.load(Ordering::Relaxed) {
        let work_item = generator.generate(&mut rng);
        let _ = work_sender.send_blocking(work_item);
    }

    Ok(())
}

pub async fn reconnect(work_sender: Sender<WorkItem>, config: Config) -> Result<()> {
    let rate = config.connection().reconnect_rate();

    let mut ratelimit_params = if rate.is_some() {
        Some(convert_ratelimit(rate.unwrap()))
    } else {
        // NOTE: we treat reconnect differently and don't generate any
        // reconnects if there is no ratelimit specified. (In contrast to
        // request generation where no ratelimit means unlimited).
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
