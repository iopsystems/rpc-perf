use super::*;
use bytes::Bytes;
use bytes::BytesMut;
use config::{Command, RampCompletionAction, RampType, ValueKind, Verb};
use flate2::write::GzEncoder;
use flate2::Compression;
use rand::seq::SliceRandom;
use rand::{rng, Rng, RngCore, SeedableRng};
use rand_distr::weighted::WeightedAliasIndex;
use rand_distr::Distribution as RandomDistribution;
use rand_distr::{Alphanumeric, Uniform, Zipf};
use rand_xoshiro::{Seed512, Xoshiro512PlusPlus};
use ratelimit::Ratelimiter;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::io::{Result, Write};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub mod client;
mod oltp;
mod publisher;

pub use client::ClientRequest;
pub use oltp::OltpRequest;
pub use publisher::PublisherWorkItem;

pub mod store;
pub use store::StoreClientRequest;

pub mod leaderboard;
pub use leaderboard::LeaderboardClientRequest;

static SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);

// a multiplier for the ratelimiter token bucket capacity
pub(crate) static BUCKET_CAPACITY: u64 = 64;

pub fn launch_workload(
    generator: Generator,
    config: &Config,
    client_sender: Sender<ClientWorkItemKind<ClientRequest>>,
    pubsub_sender: Sender<PublisherWorkItem>,
    store_sender: Sender<ClientWorkItemKind<StoreClientRequest>>,
    leaderboard_sender: Sender<ClientWorkItemKind<LeaderboardClientRequest>>,
    oltp_sender: Sender<ClientWorkItemKind<OltpRequest>>,
) -> Runtime {
    debug!("Launching workload...");

    // spawn the request drivers on their own runtime
    let workload_rt = Builder::new_multi_thread()
        .enable_all()
        .thread_name("rpc-perf-gen")
        .worker_threads(config.workload().threads())
        .build()
        .expect("failed to initialize tokio runtime");

    // initialize a PRNG with the default initial seed. We will then use this to
    // generate unique seeds for each workload thread.
    let mut rng = Xoshiro512PlusPlus::from_seed(config.general().initial_seed());

    // spawn the request generators on a blocking threads
    for _ in 0..config.workload().threads() {
        let client_sender = client_sender.clone();
        let pubsub_sender = pubsub_sender.clone();
        let store_sender = store_sender.clone();
        let leaderboard_sender = leaderboard_sender.clone();
        let oltp_sender = oltp_sender.clone();
        let generator = generator.clone();

        // generate the seed for this workload thread
        let mut seed = [0; 64];
        rng.fill_bytes(&mut seed);

        workload_rt.spawn(async move {
            // since this seed is unique, each workload thread should produce
            // requests in a different sequence
            let mut rng = Xoshiro512PlusPlus::from_seed(Seed512(seed));

            while RUNNING.load(Ordering::Relaxed) {
                generator
                    .generate(
                        &client_sender,
                        &pubsub_sender,
                        &store_sender,
                        &leaderboard_sender,
                        &oltp_sender,
                        &mut rng,
                    )
                    .await;
            }
        });
    }

    let c = config.clone();
    let store_c = config.clone();
    workload_rt.spawn_blocking(move || reconnect(client_sender, c));
    workload_rt.spawn_blocking(move || reconnect(store_sender, store_c));

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
        let ratelimiter = config.workload().ratelimit().start().map(|rate| {
            let rate = rate.get();
            let amount = (rate as f64 / 1_000_000.0).ceil() as u64;
            RATELIMIT_CURR.set(rate as i64);

            // even though we might not have nanosecond level clock resolution,
            // by using a nanosecond level duration, we achieve more accurate
            // ratelimits.
            let interval = Duration::from_nanos(1_000_000_000 / (rate / amount));

            Arc::new(
                Ratelimiter::builder(amount, interval)
                    .max_tokens(amount * BUCKET_CAPACITY)
                    .build()
                    .expect("failed to initialize ratelimiter"),
            )
        });

        let mut components = Vec::new();
        let mut component_weights = Vec::new();

        for keyspace in config.workload().keyspaces() {
            components.push(Component::Keyspace(Keyspace::new(config, keyspace)));
            component_weights.push(keyspace.weight());
        }

        for topics in config.workload().topics() {
            components.push(Component::Topics(Topics::new(config, topics)));
            component_weights.push(topics.weight());
        }

        for store in config.workload().stores() {
            components.push(Component::Store(Store::new(config, store)));
            component_weights.push(store.weight());
        }

        for leaderboard in config.workload().leaderboards() {
            components.push(Component::Leaderboard(Leaderboard::new(
                config,
                leaderboard,
            )));
            component_weights.push(leaderboard.weight());
        }

        if let Some(oltp) = config.workload().oltp() {
            components.push(Component::Oltp(Oltp::new(config, oltp)));
            component_weights.push(oltp.weight());
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

    pub async fn generate(
        &self,
        client_sender: &Sender<ClientWorkItemKind<ClientRequest>>,
        pubsub_sender: &Sender<PublisherWorkItem>,
        store_sender: &Sender<ClientWorkItemKind<StoreClientRequest>>,
        leaderboard_sender: &Sender<ClientWorkItemKind<LeaderboardClientRequest>>,
        oltp_sender: &Sender<ClientWorkItemKind<OltpRequest>>,
        rng: &mut Xoshiro512PlusPlus,
    ) {
        if let Some(ref ratelimiter) = self.ratelimiter {
            loop {
                RATELIMIT_DROPPED.set(ratelimiter.dropped());

                if ratelimiter.try_wait().is_ok() {
                    break;
                }

                std::thread::sleep(std::time::Duration::from_micros(100));
            }
        }

        match &self.components[self.component_dist.sample(rng)] {
            Component::Keyspace(keyspace) => {
                if client_sender
                    .send(self.generate_request(keyspace, rng))
                    .await
                    .is_err()
                {
                    REQUEST_DROPPED.increment();
                }
            }
            Component::Topics(topics) => {
                if pubsub_sender
                    .send(self.generate_pubsub(topics, rng))
                    .await
                    .is_err()
                {
                    REQUEST_DROPPED.increment();
                }
            }
            Component::Store(store) => {
                if store_sender
                    .send(self.generate_store_request(store, rng))
                    .await
                    .is_err()
                {
                    REQUEST_DROPPED.increment();
                }
            }
            Component::Leaderboard(leaderboard) => {
                if leaderboard_sender
                    .send(self.generate_leaderboard_request(leaderboard, rng))
                    .await
                    .is_err()
                {
                    REQUEST_DROPPED.increment();
                }
            }
            Component::Oltp(oltp) => {
                if oltp_sender
                    .send(self.generate_oltp_request(oltp, rng))
                    .await
                    .is_err()
                {
                    REQUEST_DROPPED.increment();
                }
            }
        }
    }

    fn generate_pubsub(&self, topics: &Topics, rng: &mut dyn RngCore) -> PublisherWorkItem {
        let topic_index = topics.topic_dist.sample(rng);
        let topic = topics.topics[topic_index].clone();

        let mut m = vec![0_u8; topics.message_len];

        // add a header
        [m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7]] =
            [0x54, 0x45, 0x53, 0x54, 0x49, 0x4E, 0x47, 0x21];

        // determine the range to fill with random bytes and fill that range
        let limit = std::cmp::min(topics.message_random_bytes + 32, m.len());
        rng.fill(&mut m[32..limit]);

        // generate the key
        if topics.key_len == 0 {
            PublisherWorkItem::Publish {
                topic,
                key: None,
                message: m,
            }
        } else {
            let mut k = vec![0_u8; topics.key_len];
            rng.fill(&mut k[0..topics.key_len]);
            PublisherWorkItem::Publish {
                topic,
                key: Some(k),
                message: m,
            }
        }
    }

    fn generate_store_request(
        &self,
        store: &Store,
        rng: &mut dyn RngCore,
    ) -> ClientWorkItemKind<StoreClientRequest> {
        let command = &store.commands[store.command_dist.sample(rng)];
        let sequence = SEQUENCE_NUMBER.fetch_add(1, Ordering::Relaxed);

        let request = match command.verb() {
            StoreVerb::Put => StoreClientRequest::Put(store::Put {
                key: store.sample_string(rng),
                value: store.gen_value(sequence as _, rng),
                ttl: store.ttl(),
            }),
            StoreVerb::Get => StoreClientRequest::Get(store::Get {
                key: store.sample_string(rng),
            }),
            StoreVerb::Delete => StoreClientRequest::Delete(store::Delete {
                key: store.sample_string(rng),
            }),
        };
        ClientWorkItemKind::Request { request, sequence }
    }

    fn generate_leaderboard_request(
        &self,
        leaderboard_workload: &Leaderboard,
        rng: &mut dyn RngCore,
    ) -> ClientWorkItemKind<LeaderboardClientRequest> {
        let sequence = SEQUENCE_NUMBER.fetch_add(1, Ordering::Relaxed);

        let command = leaderboard_workload.sample_command(rng);
        let leaderboard_name = leaderboard_workload.sample_leaderboard(rng);
        let request = match command.verb() {
            LeaderboardVerb::GetCompetitionRank => {
                LeaderboardClientRequest::GetCompetitionRank(leaderboard::GetCompetitionRank {
                    leaderboard: leaderboard_name,
                    ids: leaderboard_workload.sample_ids(command.cardinality(), rng),
                })
            }
            LeaderboardVerb::Upsert => LeaderboardClientRequest::Upsert(leaderboard::Upsert {
                leaderboard: leaderboard_name,
                elements: leaderboard_workload.generate_elements(command.cardinality(), rng),
            }),
        };

        ClientWorkItemKind::Request { request, sequence }
    }

    fn generate_oltp_request(
        &self,
        oltp: &Oltp,
        rng: &mut dyn RngCore,
    ) -> ClientWorkItemKind<OltpRequest> {
        let id = rng.random_range(1..=oltp.keys);
        let table = rng.random_range(1..=oltp.tables) as i32;
        let sequence = SEQUENCE_NUMBER.fetch_add(1, Ordering::Relaxed);

        let request = OltpRequest::PointSelect(oltp::PointSelect {
            id,
            table: format!("sbtest{table}"),
        });

        ClientWorkItemKind::Request { request, sequence }
    }

    fn generate_request(
        &self,
        keyspace: &Keyspace,
        rng: &mut dyn RngCore,
    ) -> ClientWorkItemKind<ClientRequest> {
        let command = &keyspace.commands[keyspace.command_dist.sample(rng)];

        let sequence = SEQUENCE_NUMBER.fetch_add(1, Ordering::Relaxed);

        let request = match command.verb() {
            Verb::Add => ClientRequest::Add(client::Add {
                key: keyspace.sample(rng),
                value: keyspace.gen_value(sequence as _, rng),
                ttl: keyspace.ttl(),
            }),
            Verb::Get => ClientRequest::Get(client::Get {
                key: keyspace.sample(rng),
            }),
            Verb::Set => ClientRequest::Set(client::Set {
                key: keyspace.sample(rng),
                value: keyspace.gen_value(sequence as _, rng),
                ttl: keyspace.ttl(),
            }),
            Verb::Delete => ClientRequest::Delete(client::Delete {
                key: keyspace.sample(rng),
            }),
            Verb::Replace => ClientRequest::Replace(client::Replace {
                key: keyspace.sample(rng),
                value: keyspace.gen_value(sequence as _, rng),
                ttl: keyspace.ttl(),
            }),
            Verb::HashGet => {
                let cardinality = command.cardinality();
                let mut fields = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    fields.push(keyspace.sample_inner(rng));
                }

                ClientRequest::HashGet(client::HashGet {
                    key: keyspace.sample(rng),
                    fields,
                })
            }
            Verb::HashGetAll => ClientRequest::HashGetAll(client::HashGetAll {
                key: keyspace.sample(rng),
            }),
            Verb::HashDelete => {
                let cardinality = command.cardinality();
                let mut fields = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    fields.push(keyspace.sample_inner(rng));
                }

                ClientRequest::HashDelete(client::HashDelete {
                    key: keyspace.sample(rng),
                    fields,
                })
            }
            Verb::HashExists => ClientRequest::HashExists(client::HashExists {
                key: keyspace.sample(rng),
                field: keyspace.sample_inner(rng),
            }),
            Verb::HashIncrement => ClientRequest::HashIncrement(client::HashIncrement {
                key: keyspace.sample(rng),
                field: keyspace.sample_inner(rng),
                amount: rng.random(),
                ttl: keyspace.ttl(),
            }),
            Verb::HashSet => {
                let mut data = HashMap::new();
                while data.len() < command.cardinality() {
                    data.insert(
                        keyspace.sample_inner(rng),
                        keyspace.gen_value(sequence as usize + data.len(), rng),
                    );
                }
                ClientRequest::HashSet(client::HashSet {
                    key: keyspace.sample(rng),
                    data,
                    ttl: keyspace.ttl(),
                })
            }
            Verb::ListPushFront => {
                let cardinality = command.cardinality();
                let mut elements = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    elements.push(keyspace.sample_inner(rng));
                }
                ClientRequest::ListPushFront(client::ListPushFront {
                    key: keyspace.sample(rng),
                    elements,
                    truncate: command.truncate(),
                    ttl: keyspace.ttl(),
                })
            }
            Verb::ListPushBack => {
                let cardinality = command.cardinality();
                let mut elements = Vec::with_capacity(cardinality);
                for _ in 0..cardinality {
                    elements.push(keyspace.sample_inner(rng));
                }
                ClientRequest::ListPushBack(client::ListPushBack {
                    key: keyspace.sample(rng),
                    elements,
                    truncate: command.truncate(),
                    ttl: keyspace.ttl(),
                })
            }
            Verb::ListFetch => ClientRequest::ListFetch(client::ListFetch {
                key: keyspace.sample(rng),
            }),
            Verb::ListLength => ClientRequest::ListLength(client::ListLength {
                key: keyspace.sample(rng),
            }),
            Verb::ListPopFront => ClientRequest::ListPopFront(client::ListPopFront {
                key: keyspace.sample(rng),
            }),
            Verb::ListPopBack => ClientRequest::ListPopBack(client::ListPopBack {
                key: keyspace.sample(rng),
            }),
            Verb::ListRemove => ClientRequest::ListRemove(client::ListRemove {
                key: keyspace.sample(rng),
                element: keyspace.sample_inner(rng),
            }),
            Verb::Ping => ClientRequest::Ping(client::Ping {}),
            Verb::SetAdd => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                ClientRequest::SetAdd(client::SetAdd {
                    key: keyspace.sample(rng),
                    members,
                    ttl: keyspace.ttl(),
                })
            }
            Verb::SetMembers => ClientRequest::SetMembers(client::SetMembers {
                key: keyspace.sample(rng),
            }),
            Verb::SetRemove => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                ClientRequest::SetRemove(client::SetRemove {
                    key: keyspace.sample(rng),
                    members,
                })
            }
            Verb::SortedSetAdd => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().map(|m| (m, rng.random())).collect();
                ClientRequest::SortedSetAdd(client::SortedSetAdd {
                    key: keyspace.sample(rng),
                    members,
                    ttl: keyspace.ttl(),
                })
            }
            Verb::SortedSetRange => ClientRequest::SortedSetRange(client::SortedSetRange {
                key: keyspace.sample(rng),
                start: command.start(),
                end: command.end(),
                by_score: command.by_score(),
            }),
            Verb::SortedSetRemove => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                ClientRequest::SortedSetRemove(client::SortedSetRemove {
                    key: keyspace.sample(rng),
                    members,
                })
            }
            Verb::SortedSetIncrement => {
                ClientRequest::SortedSetIncrement(client::SortedSetIncrement {
                    key: keyspace.sample(rng),
                    member: keyspace.sample_inner(rng),
                    amount: rng.random(),
                    ttl: keyspace.ttl(),
                })
            }
            Verb::SortedSetScore => {
                let mut members = HashSet::new();
                while members.len() < command.cardinality() {
                    members.insert(keyspace.sample_inner(rng));
                }
                let members = members.drain().collect();
                ClientRequest::SortedSetScore(client::SortedSetScore {
                    key: keyspace.sample(rng),
                    members,
                })
            }
            Verb::SortedSetRank => ClientRequest::SortedSetRank(client::SortedSetRank {
                key: keyspace.sample(rng),
                member: keyspace.sample_inner(rng),
            }),
        };

        ClientWorkItemKind::Request { request, sequence }
    }

    pub fn components(&self) -> &[Component] {
        &self.components
    }
}

#[derive(Clone)]
pub enum Component {
    Keyspace(Keyspace),
    Topics(Topics),
    Store(Store),
    Leaderboard(Leaderboard),
    Oltp(Oltp),
}

#[derive(Clone)]
pub struct Topics {
    topics: Vec<Arc<String>>,
    partitions: usize,
    replications: usize,
    topic_dist: Distribution,
    key_len: usize,
    message_len: usize,
    message_random_bytes: usize,
    subscriber_poolsize: usize,
    subscriber_concurrency: usize,
    momento_subscribers_per_topic: Option<usize>,
}

impl Topics {
    pub fn new(config: &Config, topics: &config::Topics) -> Self {
        let message_random_bytes =
            estimate_random_bytes_needed(topics.message_len(), topics.compression_ratio());
        // ntopics, partitions, and replications must be >= 1
        let ntopics = std::cmp::max(1, topics.topics());
        let partitions = std::cmp::max(1, topics.partitions());
        let replications = std::cmp::max(1, topics.replications());
        let topiclen = topics.topic_len();
        let message_len = topics.message_len();
        let key_len = topics.key_len();
        let subscriber_poolsize = topics.subscriber_poolsize();
        let subscriber_concurrency = topics.subscriber_concurrency();
        let topic_dist = match topics.topic_distribution() {
            config::Distribution::Uniform => {
                Distribution::Uniform(Uniform::new(0, ntopics).unwrap())
            }
            config::Distribution::Zipf => {
                Distribution::Zipf(Zipf::new(ntopics as f64, 1.0).unwrap())
            }
        };
        let topic_names: Vec<Arc<String>>;
        // if the given topic_names has the matched format, we use topic names there
        if topics
            .topic_names()
            .iter()
            .map(|n| n.len() == topiclen)
            .fold(topics.topic_names().len() == ntopics, |acc, c| acc && c)
        {
            topic_names = topics
                .topic_names()
                .iter()
                .map(|k| Arc::new((*k).clone()))
                .collect();
            debug!("Use given topic names:{:?}", topic_names);
        } else {
            // initialize topic name PRNG and generate a set of unique topics
            let mut rng = Xoshiro512PlusPlus::from_seed(config.general().initial_seed());
            let mut raw_seed = [0_u8; 64];
            rng.fill_bytes(&mut raw_seed);
            let topic_name_seed = Seed512(raw_seed);
            let mut rng = Xoshiro512PlusPlus::from_seed(topic_name_seed);
            let mut topics = HashSet::with_capacity(ntopics);
            while topics.len() < ntopics {
                let topic = (&mut rng)
                    .sample_iter(&Alphanumeric)
                    .take(topiclen)
                    .collect::<Vec<u8>>();
                let _ = topics.insert(unsafe { std::str::from_utf8_unchecked(&topic) }.to_string());
            }
            topic_names = topics.drain().map(|k| k.into()).collect();
        }

        Self {
            topics: topic_names,
            partitions,
            replications,
            topic_dist,
            key_len,
            message_len,
            message_random_bytes,
            subscriber_poolsize,
            subscriber_concurrency,
            momento_subscribers_per_topic: topics.momento_subscribers_per_topic(),
        }
    }

    pub fn topics(&self) -> &[Arc<String>] {
        &self.topics
    }

    pub fn partitions(&self) -> usize {
        self.partitions
    }

    pub fn replications(&self) -> usize {
        self.replications
    }

    pub fn subscriber_poolsize(&self) -> usize {
        self.subscriber_poolsize
    }

    pub fn subscriber_concurrency(&self) -> usize {
        self.subscriber_concurrency
    }

    pub fn momento_subscribers_per_topic(&self) -> Option<usize> {
        self.momento_subscribers_per_topic
    }
}

#[derive(Clone)]
pub struct Keyspace {
    keys: Vec<Arc<[u8]>>,
    key_dist: Distribution,
    commands: Vec<Command>,
    command_dist: WeightedAliasIndex<usize>,
    inner_keys: Vec<Arc<[u8]>>,
    inner_key_dist: Distribution,
    vlen: usize,
    vkind: ValueKind,
    ttl: Option<Duration>,
    vbuf: Bytes,
}

#[derive(Clone)]
pub enum Distribution {
    Uniform(Uniform<usize>),
    Zipf(Zipf<f64>),
}

impl Distribution {
    pub fn sample(&self, rng: &mut dyn RngCore) -> usize {
        match self {
            Self::Uniform(dist) => dist.sample(rng),
            Self::Zipf(dist) => dist.sample(rng) as usize,
        }
    }
}

impl Keyspace {
    pub fn new(config: &Config, keyspace: &config::Keyspace) -> Self {
        let value_random_bytes = estimate_random_bytes_needed(
            keyspace.vlen().unwrap_or(0),
            keyspace.compression_ratio(),
        );

        // nkeys must be >= 1
        let nkeys = std::cmp::max(1, keyspace.nkeys());
        let klen = keyspace.klen();

        // initialize a PRNG with the default initial seed
        let mut rng = Xoshiro512PlusPlus::from_seed(config.general().initial_seed());

        // generate the seed for key PRNG
        let mut raw_seed = [0_u8; 64];
        rng.fill_bytes(&mut raw_seed);
        let key_seed = Seed512(raw_seed);

        // generate the seed for inner key PRNG
        let mut raw_seed = [0_u8; 64];
        rng.fill_bytes(&mut raw_seed);
        let inner_key_seed = Seed512(raw_seed);

        // we use a predictable seed to generate the keys in the keyspace
        let mut rng = Xoshiro512PlusPlus::from_seed(key_seed);
        let mut keys = HashSet::with_capacity(nkeys);
        while keys.len() < nkeys {
            let key = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(klen)
                .collect::<Vec<u8>>();
            let _ = keys.insert(key);
        }
        let keys = keys.drain().map(|k| k.into()).collect();
        let key_dist = match keyspace.key_distribution() {
            config::Distribution::Uniform => Distribution::Uniform(Uniform::new(0, nkeys).unwrap()),
            config::Distribution::Zipf => Distribution::Zipf(Zipf::new(nkeys as f64, 1.0).unwrap()),
        };

        let nkeys = keyspace.inner_keys_nkeys().unwrap_or(1);
        let klen = keyspace.inner_keys_klen().unwrap_or(1);

        // we use a predictable seed to generate the keys in the keyspace
        let mut rng = Xoshiro512PlusPlus::from_seed(inner_key_seed);
        let mut inner_keys = HashSet::with_capacity(nkeys);
        while inner_keys.len() < nkeys {
            let key = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(klen)
                .collect::<Vec<u8>>();
            let _ = inner_keys.insert(key);
        }
        let inner_keys: Vec<Arc<[u8]>> = inner_keys.drain().map(|k| k.into()).collect();
        let inner_key_dist = Distribution::Uniform(Uniform::new(0, nkeys).unwrap());

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

            if command.start().is_some() && !command.verb().supports_start() {
                eprintln!(
                    "verb: {:?} does not support the `start` argument",
                    command.verb()
                );
            }

            if command.end().is_some() && !command.verb().supports_end() {
                eprintln!(
                    "verb: {:?} does not support the `end` argument",
                    command.verb()
                );
            }

            if command.by_score() && !command.verb().supports_by_score() {
                eprintln!(
                    "verb: {:?} does not support the `by_score` option",
                    command.verb()
                );
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

        // prepare 100MB of random value
        let len = 100 * 1024 * 1024;
        let mut vbuf = BytesMut::zeroed(len);
        rng.fill_bytes(&mut vbuf[0..value_random_bytes]);

        // if we have compressible values, we need to distribute the remaining
        // zeros evenly across the value buffer
        if vbuf.len() > value_random_bytes {
            vbuf.shuffle(&mut rng);
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
            ttl: keyspace.ttl(),
            vbuf: vbuf.into(),
        }
    }

    pub fn sample(&self, rng: &mut dyn RngCore) -> Arc<[u8]> {
        let index = self.key_dist.sample(rng);
        self.keys[index].clone()
    }

    pub fn sample_inner(&self, rng: &mut dyn RngCore) -> Arc<[u8]> {
        let index = self.inner_key_dist.sample(rng);
        self.inner_keys[index].clone()
    }

    pub fn gen_value(&self, sequence: usize, rng: &mut dyn RngCore) -> Bytes {
        match self.vkind {
            ValueKind::I64 => format!("{}", rng.random::<i64>()).into_bytes().into(),
            ValueKind::Bytes => {
                let start = sequence % (self.vbuf.len() - self.vlen);
                let end = start + self.vlen;

                self.vbuf.slice(start..end)
            }
        }
    }

    pub fn ttl(&self) -> Option<Duration> {
        self.ttl
    }
}

#[derive(Clone)]
pub struct Store {
    keys: Vec<Arc<[u8]>>,
    key_dist: Distribution,
    commands: Vec<StoreCommand>,
    command_dist: WeightedAliasIndex<usize>,
    vlen: usize,
    vkind: ValueKind,
    vbuf: Bytes,
    ttl: Option<Duration>,
}

impl Store {
    pub fn new(config: &Config, store: &config::Store) -> Self {
        let value_random_bytes =
            estimate_random_bytes_needed(store.vlen().unwrap_or(0), store.compression_ratio());

        // nkeys must be >= 1
        let nkeys = std::cmp::max(1, store.nkeys());
        let klen = store.klen();

        // initialize a PRNG with the default initial seed
        let mut rng = Xoshiro512PlusPlus::from_seed(config.general().initial_seed());

        // generate the seed for key PRNG
        let mut raw_seed = [0_u8; 64];
        rng.fill_bytes(&mut raw_seed);
        let key_seed = Seed512(raw_seed);

        // generate the seed for inner key PRNG
        let mut raw_seed = [0_u8; 64];
        rng.fill_bytes(&mut raw_seed);

        // we use a predictable seed to generate the keys in the store
        let mut rng = Xoshiro512PlusPlus::from_seed(key_seed);
        let mut keys = HashSet::with_capacity(nkeys);
        while keys.len() < nkeys {
            let key = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(klen)
                .collect::<Vec<u8>>();
            let _ = keys.insert(key);
        }
        let keys = keys.drain().map(|k| k.into()).collect();
        let key_dist = match store.key_distribution() {
            config::Distribution::Uniform => Distribution::Uniform(Uniform::new(0, nkeys).unwrap()),
            config::Distribution::Zipf => Distribution::Zipf(Zipf::new(nkeys as f64, 1.0).unwrap()),
        };

        let mut commands = Vec::new();
        let mut command_weights = Vec::new();

        for command in store.commands() {
            commands.push(*command);
            command_weights.push(command.weight());

            // validate that the store is adaquately specified for the given
            // verb

            // commands that set generated values need a `vlen`
            if store.vlen().is_none()
                && store.vkind() == ValueKind::Bytes
                && matches!(command.verb(), StoreVerb::Put)
            {
                eprintln!(
                    "verb: {:?} requires that the keyspace has a `vlen` set when `vkind` is `bytes`",
                    command.verb()
                );
                std::process::exit(2);
            }
        }

        // prepare 100MB of random value
        let len = 100 * 1024 * 1024;
        let mut vbuf = BytesMut::zeroed(len);
        rng.fill_bytes(&mut vbuf[0..value_random_bytes]);
        vbuf.shuffle(&mut rng);

        let command_dist = WeightedAliasIndex::new(command_weights).unwrap();
        Self {
            keys,
            key_dist,
            commands,
            command_dist,
            vlen: store.vlen().unwrap_or(0),
            vkind: store.vkind(),
            vbuf: vbuf.into(),
            ttl: store.ttl(),
        }
    }

    pub fn sample(&self, rng: &mut dyn RngCore) -> Arc<[u8]> {
        let index = self.key_dist.sample(rng);
        self.keys[index].clone()
    }

    pub fn sample_string(&self, rng: &mut dyn RngCore) -> Arc<String> {
        let keys = self.sample(rng);
        Arc::new(String::from_utf8_lossy(&keys).into_owned())
    }

    pub fn gen_value(&self, sequence: usize, rng: &mut dyn RngCore) -> Bytes {
        match self.vkind {
            ValueKind::I64 => format!("{}", rng.random::<i64>()).into_bytes().into(),
            ValueKind::Bytes => {
                let start = sequence % (self.vbuf.len() - self.vlen);
                let end = start + self.vlen;

                self.vbuf.slice(start..end)
            }
        }
    }

    pub fn ttl(&self) -> Option<Duration> {
        self.ttl
    }
}

#[derive(Clone)]
pub struct Leaderboard {
    leaderboards: Vec<Arc<[u8]>>,
    leaderboard_dist: Distribution,
    ids: Vec<Arc<u32>>,
    id_dist: Distribution,
    commands: Vec<LeaderboardCommand>,
    command_dist: WeightedAliasIndex<usize>,
}

// Data generation is as follows:
// 1. Leaderboard names
// - We define the set of leaderboards: L = {L_1, L_2, ..., L_{nleaderboards}}
// - Each leaderboard name L_i is a randomly generated alphanumeric string of length `leaderboard_len`:
//      L_i ~ Uniform(Alphanumeric, leaderboard_len)
//
// 2 .IDs
// - We define the set of IDs: I = {I_1, I_2, ..., I_{nids}}
// - Each ID I_i sampled without replacement: I_i ~ Uniform({0, 1, ..., 2^32 -1}, nids)
//
// 3. Commands
// For each command we first sample a leaderboard name uniformly at random fro L.
//
// i. Upsert
//  - Produce `cardinality` id-score pairs to upsert.
//  - Sample `cardinality` ids uniformly at random from I without replacement.
//  - For each id, sample a f64 score uniformly at random from the unit interval.
//
// ii. GetCompetitionRank
// - Sample `cardinality` ids uniformly at random from I without replacement.
impl Leaderboard {
    pub fn new(config: &Config, leaderboard: &config::Leaderboard) -> Self {
        let nleaderboards = leaderboard.nleaderboards();
        let leadboard_len = leaderboard.leaderboard_len();
        let nids = leaderboard.nids();

        // initialize a PRNG with the default initial seed
        let mut rng = Xoshiro512PlusPlus::from_seed(config.general().initial_seed());

        // generate the seed for leaderboard PRNG
        let mut raw_seed = [0_u8; 64];
        rng.fill_bytes(&mut raw_seed);
        let leaderboard_seed = Seed512(raw_seed);

        // generate the seed for id PRNG
        let mut raw_seed = [0_u8; 64];
        rng.fill_bytes(&mut raw_seed);
        let id_seed = Seed512(raw_seed);

        // generate leaderboards
        let mut rng = Xoshiro512PlusPlus::from_seed(leaderboard_seed);
        let leaderboards = sample_unique(nleaderboards, &mut rng, |rng| {
            rng.sample_iter(&Alphanumeric)
                .take(leadboard_len)
                .collect::<Vec<u8>>()
        })
        .into_iter()
        .map(|l| l.into())
        .collect();
        let leaderboard_dist = Distribution::Uniform(Uniform::new(0, nleaderboards).unwrap());

        // generate ids
        let mut rng = Xoshiro512PlusPlus::from_seed(id_seed);
        let ids = sample_unique(nids, &mut rng, |rng| Arc::new(rng.random()));
        let id_dist = Distribution::Uniform(Uniform::new(0, nids).unwrap());

        let mut commands = Vec::new();
        let mut command_weights = Vec::new();

        for command in leaderboard.commands() {
            Leaderboard::validate_command(command);

            commands.push(*command);
            command_weights.push(command.weight());
        }

        let command_dist = WeightedAliasIndex::new(command_weights).unwrap();

        Self {
            leaderboards,
            leaderboard_dist,
            ids,
            id_dist,
            commands,
            command_dist,
        }
    }

    fn validate_command(command: &LeaderboardCommand) {
        if command.cardinality() == 0 {
            eprintln!("cardinality must not be zero");
            std::process::exit(2);
        } else if !command.verb().supports_cardinality() && command.cardinality() > 1 {
            eprintln!(
                "verb: {:?} requires that `cardinality` is set to `1`",
                command.verb()
            );
            std::process::exit(2);
        }
    }

    pub fn sample_command(&self, rng: &mut dyn RngCore) -> &LeaderboardCommand {
        let index = self.command_dist.sample(rng);
        &self.commands[index]
    }

    pub fn sample_leaderboard(&self, rng: &mut dyn RngCore) -> Arc<String> {
        let index = self.leaderboard_dist.sample(rng);
        Arc::new(String::from_utf8_lossy(&self.leaderboards[index]).into_owned())
    }

    pub fn sample_ids(&self, num_ids: usize, rng: &mut dyn RngCore) -> Arc<[u32]> {
        let sample: Vec<u32> = sample_unique(num_ids, rng, |rng| {
            let index = self.id_dist.sample(rng);
            *self.ids[index].as_ref()
        });
        Arc::from(sample.into_boxed_slice())
    }

    /// Generate a score in the unit interval [0, 1)
    pub fn generate_score(&self, rng: &mut dyn RngCore) -> f64 {
        rng.random()
    }

    /// Generate a set of id-score pairs to upsert into a leaderboard.
    /// Samples the ids uniformly at random from the set of ids and generates
    /// a score in the unit interval [0, 1) for each id.
    pub fn generate_elements(&self, num_elements: usize, rng: &mut dyn RngCore) -> Vec<(u32, f64)> {
        let ids = self.sample_ids(num_elements, rng);

        ids.into_iter()
            .map(|id| (*id, self.generate_score(rng)))
            .collect()
    }
}

#[derive(Clone)]
pub struct Oltp {
    tables: u8,
    keys: i32,
}

impl Oltp {
    pub fn new(_config: &Config, oltp: &config::Oltp) -> Self {
        {
            Self {
                tables: oltp.tables(),
                keys: oltp.keys(),
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum ClientWorkItemKind<T> {
    Reconnect,
    Request { request: T, sequence: u64 },
}

pub async fn reconnect<TRequestKind>(
    work_sender: Sender<ClientWorkItemKind<TRequestKind>>,
    config: Config,
) -> Result<()> {
    if config.client().is_none() {
        return Ok(());
    }

    let ratelimiter = config.client().unwrap().reconnect_rate().map(|rate| {
        let rate = rate.get();
        let amount = (rate as f64 / 1_000_000.0).ceil() as u64;
        RATELIMIT_CURR.set(rate as i64);

        // even though we might not have nanosecond level clock resolution,
        // by using a nanosecond level duration, we achieve more accurate
        // ratelimits.
        let interval = Duration::from_nanos(1_000_000_000 / (rate / amount));

        Arc::new(
            Ratelimiter::builder(amount, interval)
                .max_tokens(amount * BUCKET_CAPACITY)
                .build()
                .expect("failed to initialize ratelimiter"),
        )
    });

    if ratelimiter.is_none() {
        return Ok(());
    }

    let ratelimiter = ratelimiter.unwrap();

    while RUNNING.load(Ordering::Relaxed) {
        match ratelimiter.try_wait() {
            Ok(_) => {
                let _ = work_sender.send(ClientWorkItemKind::Reconnect).await;
            }
            Err(d) => {
                std::thread::sleep(d);
            }
        }
    }

    Ok(())
}

#[derive(Clone)]
pub struct Ratelimit {
    limits: Vec<u64>,
    interval: Duration,
    ramp_completion_action: RampCompletionAction,
    current_idx: usize,
}

impl Ratelimit {
    pub fn new(config: &Config) -> Option<Self> {
        let ratelimit_config = config.workload().ratelimit();

        if !ratelimit_config.is_dynamic() {
            return None;
        }

        // Unwrapping values is safe since the structure has already been
        // validated for dynamic ratelimit parameters
        let start: u64 = ratelimit_config.start().unwrap().into();
        let end = ratelimit_config.end().unwrap();
        let step = ratelimit_config.step().unwrap() as usize;
        let interval = ratelimit_config.interval().unwrap();
        let ramp_type = ratelimit_config.ramp_type();
        let ramp_completion_action = ratelimit_config.ramp_completion_action();

        // Store all the ratelimits to test in a vector
        let nsteps = ((end - start) as usize / step) + 1;
        let mut limits: Vec<u64> = Vec::with_capacity(nsteps);
        for i in (start..end + 1).step_by(step) {
            limits.push(i);
        }

        // Shuffle the order of ratelimits if specified
        if ramp_type == RampType::Shuffled {
            limits.shuffle(&mut rng());
        }

        // If the test is to be mirrored, store the ratelimits in reverse
        // order in the vector as well
        if ramp_completion_action == RampCompletionAction::Mirror {
            for i in (0..limits.len()).rev() {
                limits.push(limits[i]);
            }
        }

        Some(Ratelimit {
            limits,
            interval,
            ramp_completion_action,
            current_idx: 0,
        })
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn next_ratelimit(&mut self) -> u64 {
        let limit = self.limits[self.current_idx];
        self.current_idx += 1;

        if self.current_idx == self.limits.len() {
            // If the test is to be looped or mirrored reset the pointer to the
            // beginning of the vector and start again, else move back to the
            // previous (final stable) value
            if self.ramp_completion_action == RampCompletionAction::Loop
                || self.ramp_completion_action == RampCompletionAction::Mirror
            {
                self.current_idx = 0;
            } else {
                self.current_idx -= 1;
            }
        }

        limit
    }
}

fn estimate_random_bytes_needed(length: usize, compression_ratio: f64) -> usize {
    // if compression ratio is low, all bytes should be random
    if compression_ratio <= 1.0 {
        return length;
    }

    // we need to approximate the number of random bytes to send, we do
    // this iteratively assuming gzip compression.

    // doesn't matter what seed we use here
    let mut rng = Xoshiro512PlusPlus::seed_from_u64(0);

    // message buffer
    let mut m = vec![0; length];

    let mut best = 0;

    for idx in 0..m.len() {
        // zero all bytes
        for b in &mut m {
            *b = 0
        }

        // fill first N bytes with pseudorandom data
        rng.fill_bytes(&mut m[0..idx]);

        // shuffle the value
        m.shuffle(&mut rng);

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        let _ = encoder.write_all(&m);
        let compressed = encoder.finish().unwrap();

        let ratio = m.len() as f64 / compressed.len() as f64;

        if ratio < compression_ratio {
            break;
        }

        best = idx;
    }

    best
}

/// Samples `n` distinct items from a distribution using the given random number generator
/// and generator function.
///
/// Be careful when using this that the domain of values generated by the generator function
/// is large enough to support the number of unique items requested. This could take a long
/// time to complete if the domain is close enough to the number of unique items requested.
fn sample_unique<T, F, R>(n: usize, mut rng: R, mut generator_fn: F) -> Vec<T>
where
    T: Eq + Hash,
    F: FnMut(&mut R) -> T,
    R: RngCore,
{
    let mut unique_items = HashSet::with_capacity(n);
    while unique_items.len() < n {
        let item = generator_fn(&mut rng);
        unique_items.insert(item);
    }
    unique_items.into_iter().collect()
}
