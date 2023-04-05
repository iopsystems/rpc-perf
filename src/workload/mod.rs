// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

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
use tokio::runtime::Runtime;
use tokio::time::Interval;

mod client;
mod publisher;

pub use client::ClientWorkItem;
pub use publisher::PublisherWorkItem;

pub fn launch_workload(
    generator: Generator,
    config: &Config,
    client_sender: Sender<ClientWorkItem>,
    pubsub_sender: Sender<PublisherWorkItem>,
) -> Runtime {
    debug!("Launching workload...");

    // spawn the request drivers on their own runtime
    let workload_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .expect("failed to initialize tokio runtime");

    // spawn the request generators on a blocking threads
    for _ in 0..config.workload().threads() {
        let client_sender = client_sender.clone();
        let pubsub_sender = pubsub_sender.clone();
        let generator = generator.clone();
        workload_rt.spawn_blocking(move || {
            // use a prng seeded from the entropy pool so that request generation is
            // unpredictable within the space and each individual request generation
            // task will generate a unique sequence
            let mut rng = Xoshiro512PlusPlus::from_entropy();

            while RUNNING.load(Ordering::Relaxed) {
                generator.generate(&client_sender, &pubsub_sender, &mut rng);
            }
        });
    }

    let c = config.clone();
    workload_rt.spawn_blocking(move || reconnect(client_sender, c));

    workload_rt
}

#[derive(Clone)]
pub struct Generator {
    ratelimiter: Option<Arc<Ratelimiter>>,
    components: Vec<Component>,
    component_dist: WeightedAliasIndex<usize>,
}

impl Generator {
    pub fn new(config: &Config) -> Self {
        let ratelimiter = config
            .workload()
            .ratelimit()
            .map(|r| Arc::new(Ratelimiter::new(1000, 1, r.into())));

        let mut components = Vec::new();
        let mut component_weights = Vec::new();

        for keyspace in config.workload().keyspaces() {
            components.push(Component::Keyspace(Keyspace::new(keyspace)));
            component_weights.push(keyspace.weight());
        }

        for topics in config.workload().topics() {
            components.push(Component::Topics(Topics::new(topics)));
            component_weights.push(topics.weight());
        }

        if components.is_empty() {
            eprintln!("no workload components were specified in the config");
            std::process::exit(1);
        }

        Self {
            ratelimiter,
            components,
            component_dist: WeightedAliasIndex::new(component_weights).unwrap(),
        }
    }

    pub fn ratelimiter(&self) -> Option<Arc<Ratelimiter>> {
        self.ratelimiter.clone()
    }

    pub fn generate(
        &self,
        client_sender: &Sender<ClientWorkItem>,
        pubsub_sender: &Sender<PublisherWorkItem>,
        rng: &mut dyn RngCore,
    ) {
        if let Some(ref ratelimiter) = self.ratelimiter {
            loop {
                if ratelimiter.try_wait().is_ok() {
                    break;
                }

                std::thread::sleep(std::time::Duration::from_micros(100));
            }
        }

        match &self.components[self.component_dist.sample(rng)] {
            Component::Keyspace(keyspace) => {
                let _ = client_sender.send_blocking(self.generate_request(keyspace, rng));
            }
            Component::Topics(topics) => {
                let _ = pubsub_sender.send_blocking(self.generate_pubsub(topics, rng));
            }
        }
    }

    fn generate_pubsub(&self, topics: &Topics, rng: &mut dyn RngCore) -> PublisherWorkItem {
        let index = topics.topic_dist.inner.sample(rng);
        let topic = topics.topics[index].clone();

        let mut message = vec![0_u8; topics.message_len];
        rng.fill(&mut message[32..topics.message_len]);

        PublisherWorkItem::Publish { topic, message }
    }

    fn generate_request(&self, keyspace: &Keyspace, rng: &mut dyn RngCore) -> ClientWorkItem {
        let command = &keyspace.commands[keyspace.command_dist.sample(rng)];

        match command.verb() {
            Verb::Get => ClientWorkItem::Get {
                key: keyspace.sample(rng),
            },
            Verb::Set => ClientWorkItem::Set {
                key: keyspace.sample(rng),
                value: keyspace.gen_value(rng),
            },
            Verb::Delete => ClientWorkItem::Delete {
                key: keyspace.sample(rng),
            },
            Verb::HashGet => {
                let cardinality = command.cardinality();
                let mut fields = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    fields.push(keyspace.sample_inner(rng));
                }

                ClientWorkItem::HashGet {
                    key: keyspace.sample(rng),
                    fields,
                }
            }
            Verb::HashGetAll => ClientWorkItem::HashGetAll {
                key: keyspace.sample(rng),
            },
            Verb::HashDelete => {
                let cardinality = command.cardinality();
                let mut fields = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    fields.push(keyspace.sample_inner(rng));
                }

                ClientWorkItem::HashDelete {
                    key: keyspace.sample(rng),
                    fields,
                }
            }
            Verb::HashExists => ClientWorkItem::HashExists {
                key: keyspace.sample(rng),
                field: keyspace.sample_inner(rng),
            },
            Verb::HashIncrement => ClientWorkItem::HashIncrement {
                key: keyspace.sample(rng),
                field: keyspace.sample_inner(rng),
                amount: rng.gen(),
            },
            Verb::HashSet => {
                let mut data = HashMap::new();
                while data.len() < command.cardinality() {
                    data.insert(keyspace.sample_inner(rng), keyspace.gen_value(rng));
                }
                ClientWorkItem::HashSet {
                    key: keyspace.sample(rng),
                    data,
                }
            }
            Verb::ListPushFront => {
                let cardinality = command.cardinality();
                let mut elements = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    elements.push(keyspace.sample_inner(rng));
                }
                ClientWorkItem::ListPushFront {
                    key: keyspace.sample(rng),
                    elements,
                    truncate: command.truncate(),
                }
            }
            Verb::ListPushBack => {
                let cardinality = command.cardinality();
                let mut elements = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    elements.push(keyspace.sample_inner(rng));
                }
                ClientWorkItem::ListPushBack {
                    key: keyspace.sample(rng),
                    elements,
                    truncate: command.truncate(),
                }
            }
            Verb::ListFetch => ClientWorkItem::ListFetch {
                key: keyspace.sample(rng),
            },
            Verb::ListLength => ClientWorkItem::ListLength {
                key: keyspace.sample(rng),
            },
            Verb::ListPopFront => ClientWorkItem::ListPopFront {
                key: keyspace.sample(rng),
            },
            Verb::ListPopBack => ClientWorkItem::ListPopBack {
                key: keyspace.sample(rng),
            },
            Verb::Ping => ClientWorkItem::Ping {},
            Verb::SetAdd => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                ClientWorkItem::SetAdd {
                    key: keyspace.sample(rng),
                    members,
                }
            }
            Verb::SetMembers => ClientWorkItem::SetMembers {
                key: keyspace.sample(rng),
            },
            Verb::SetRemove => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                ClientWorkItem::SetRemove {
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
                ClientWorkItem::SortedSetAdd {
                    key: keyspace.sample(rng),
                    members,
                }
            }
            Verb::SortedSetMembers => ClientWorkItem::SortedSetMembers {
                key: keyspace.sample(rng),
            },
            Verb::SortedSetRemove => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                ClientWorkItem::SortedSetRemove {
                    key: keyspace.sample(rng),
                    members,
                }
            }
            Verb::SortedSetIncrement => ClientWorkItem::SortedSetIncrement {
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
                ClientWorkItem::SortedSetScore {
                    key: keyspace.sample(rng),
                    members,
                }
            }
            Verb::SortedSetRank => ClientWorkItem::SortedSetRank {
                key: keyspace.sample(rng),
                member: keyspace.sample_inner(rng),
            },
        }
    }

    pub fn components(&self) -> &[Component] {
        &self.components
    }
}

#[derive(Clone)]
pub enum Component {
    Keyspace(Keyspace),
    Topics(Topics),
}

#[derive(Clone)]
pub struct Topics {
    topics: Vec<Arc<String>>,
    topic_dist: KeyDistribution,
    message_len: usize,
    subscriber_poolsize: usize,
    subscriber_concurrency: usize,
}

impl Topics {
    pub fn new(topics: &config::Topics) -> Self {
        // ntopics must be >= 1
        let ntopics = std::cmp::max(1, topics.topics());
        let topiclen = topics.topic_len();
        let message_len = topics.message_len();
        let subscriber_poolsize = topics.subscriber_poolsize();
        let subscriber_concurrency = topics.subscriber_concurrency();

        // we use a predictable seed to generate the topic names
        let mut rng = Xoshiro512PlusPlus::seed_from_u64(0);
        let mut topics = HashSet::with_capacity(ntopics);
        while topics.len() < ntopics {
            let topic = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(topiclen)
                .collect::<Vec<u8>>();
            let _ = topics.insert(unsafe { std::str::from_utf8_unchecked(&topic) }.to_string());
        }
        let topics = topics.drain().map(|k| k.into()).collect();
        let topic_dist = KeyDistribution {
            inner: Uniform::new(0, ntopics),
        };

        Self {
            topics,
            topic_dist,
            message_len,
            subscriber_poolsize,
            subscriber_concurrency,
        }
    }

    pub fn topics(&self) -> &[Arc<String>] {
        &self.topics
    }

    pub fn subscriber_poolsize(&self) -> usize {
        self.subscriber_poolsize
    }

    pub fn subscriber_concurrency(&self) -> usize {
        self.subscriber_concurrency
    }
}

#[derive(Clone)]
pub struct Keyspace {
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
        // nkeys must be >= 1
        let nkeys = std::cmp::max(1, keyspace.nkeys());
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

pub async fn reconnect(work_sender: Sender<ClientWorkItem>, config: Config) -> Result<()> {
    if config.client().is_none() {
        return Ok(());
    }

    let rate = config.client().unwrap().reconnect_rate();

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
            let _ = work_sender.send(ClientWorkItem::Reconnect).await;
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
