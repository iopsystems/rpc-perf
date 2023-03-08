// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use config::{Command, ValueKind, Verb};
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

    pub fn ratelimiter(&self) -> Option<Arc<Ratelimiter>> {
        self.ratelimiter.clone()
    }

    pub fn generate(&self, rng: &mut dyn RngCore) -> WorkItem {
        if let Some(ref ratelimiter) = self.ratelimiter {
            ratelimiter.wait();
        }

        let keyspace = &self.keyspaces[self.keyspace_dist.sample(rng)];
        let command = &keyspace.commands[keyspace.command_dist.sample(rng)];

        match command.verb() {
            Verb::Get => WorkItem::Get {
                key: keyspace.sample(rng),
            },
            Verb::Set => WorkItem::Set {
                key: keyspace.sample(rng),
                value: keyspace.gen_value(rng),
            },
            Verb::Delete => WorkItem::Delete {
                key: keyspace.sample(rng),
            },
            Verb::HashGet => {
                let cardinality = command.cardinality();
                let mut fields = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    fields.push(keyspace.sample_inner(rng));
                }

                WorkItem::HashGet {
                    key: keyspace.sample(rng),
                    fields,
                }
            }
            Verb::HashGetAll => WorkItem::HashGetAll {
                key: keyspace.sample(rng),
            },
            Verb::HashDelete => {
                let cardinality = command.cardinality();
                let mut fields = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    fields.push(keyspace.sample_inner(rng));
                }

                WorkItem::HashDelete {
                    key: keyspace.sample(rng),
                    fields,
                }
            }
            Verb::HashExists => WorkItem::HashExists {
                key: keyspace.sample(rng),
                field: keyspace.sample_inner(rng),
            },
            Verb::HashIncrement => WorkItem::HashIncrement {
                key: keyspace.sample(rng),
                field: keyspace.sample_inner(rng),
                amount: rng.gen(),
            },
            Verb::HashSet => {
                let mut data = HashMap::new();
                while data.len() < command.cardinality() {
                    data.insert(keyspace.sample_inner(rng), keyspace.gen_value(rng));
                }
                WorkItem::HashSet {
                    key: keyspace.sample(rng),
                    data,
                }
            }
            Verb::ListPushFront => WorkItem::ListPushFront {
                key: keyspace.sample(rng),
                element: keyspace.sample_inner(rng),
                truncate: command.truncate(),
            },
            Verb::ListPushBack => WorkItem::ListPushBack {
                key: keyspace.sample(rng),
                element: keyspace.sample_inner(rng),
                truncate: command.truncate(),
            },
            Verb::ListFetch => WorkItem::ListFetch {
                key: keyspace.sample(rng),
            },
            Verb::Ping => WorkItem::Ping {},
            Verb::SetAdd => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                WorkItem::SetAdd {
                    key: keyspace.sample(rng),
                    members,
                }
            }
            Verb::SetMembers => WorkItem::SetMembers {
                key: keyspace.sample(rng),
            },
            Verb::SetRemove => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                WorkItem::SetRemove {
                    key: keyspace.sample(rng),
                    members,
                }
            }
            Verb::SortedSetAdd => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().map(|m| (m, rng.gen())).collect();
                WorkItem::SortedSetAdd {
                    key: keyspace.sample(rng),
                    members,
                }
            }
            Verb::SortedSetMembers => WorkItem::SortedSetMembers {
                key: keyspace.sample(rng),
            },
            Verb::SortedSetRemove => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                WorkItem::SortedSetRemove {
                    key: keyspace.sample(rng),
                    members,
                }
            }
            Verb::SortedSetIncrement => WorkItem::SortedSetIncrement {
                key: keyspace.sample(rng),
                member: keyspace.sample_inner(rng),
                amount: rng.gen(),
            },
            Verb::SortedSetScore => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                WorkItem::SortedSetScore {
                    key: keyspace.sample(rng),
                    members,
                }
            }
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
    commands: Vec<Command>,
    command_dist: WeightedAliasIndex<usize>,
    inner_keys: Vec<Arc<[u8]>>,
    inner_key_dist: KeyDistribution,
    vlen: usize,
    vkind: ValueKind,
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

        let nkeys = keyspace.inner_keys_nkeys().unwrap_or(1);
        let klen = keyspace.inner_keys_klen().unwrap_or(1);

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
        let inner_keys: Vec<Arc<[u8]>> = inner_keys.drain().map(|k| k.into()).collect();
        let inner_key_dist = KeyDistribution {
            inner: Uniform::new(0, nkeys),
        };

        let mut commands = Vec::new();
        let mut command_weights = Vec::new();

        for command in keyspace.commands() {
            commands.push(*command);
            command_weights.push(command.weight());

            // validate that the keyspace is adaquately specified for the given
            // verb

            // commands that set generated values need a `vlen`
            if keyspace.vlen().is_none()
                && keyspace.vkind() == ValueKind::Bytes
                && matches!(command.verb(), Verb::Set | Verb::HashSet)
            {
                eprintln!(
                    "verb: {:?} requires that the keyspace has a `vlen` set when `vkind` is `bytes`",
                    command.verb()
                );
                std::process::exit(2);
            }

            // cardinality must always be > 0
            if command.cardinality() == 0 {
                eprintln!("cardinality must not be zero",);
                std::process::exit(2);
            }

            // not all commands support cardinality > 1
            if command.cardinality() > 1 && !command.verb().supports_cardinality() {
                eprintln!(
                    "verb: {:?} requires that `cardinality` is set to `1`",
                    command.verb()
                );
                std::process::exit(2);
            }

            if command.truncate().is_some() {
                // truncate must be >= 1
                if command.truncate().unwrap() == 0 {
                    eprintln!("truncate must be >= 1",);
                    std::process::exit(2);
                }

                // not all commands support truncate
                if !command.verb().supports_truncate() {
                    eprintln!("verb: {:?} does not support truncate", command.verb());
                    std::process::exit(2);
                }
            }

            if command.verb().needs_inner_key()
                && (keyspace.inner_keys_nkeys().is_none() || keyspace.inner_keys_klen().is_none())
            {
                eprintln!(
                    "verb: {:?} requires that `inner_key_klen` and `inner_key_nkeys` are set",
                    command.verb()
                );
                std::process::exit(2);
            }
        }

        let command_dist = WeightedAliasIndex::new(command_weights).unwrap();

        Self {
            keys,
            key_dist,
            commands,
            command_dist,
            inner_keys,
            inner_key_dist,
            vlen: keyspace.vlen().unwrap_or(0),
            vkind: keyspace.vkind(),
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

    pub fn gen_value(&self, rng: &mut dyn RngCore) -> Vec<u8> {
        match self.vkind {
            ValueKind::I64 => format!("{}", rng.gen::<i64>()).into_bytes(),
            ValueKind::Bytes => {
                let mut buf = vec![0_u8; self.vlen];
                rng.fill(&mut buf[0..self.vlen]);
                buf
            }
        }
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
    // timer granulcardinality should be millisecond level on most platforms
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