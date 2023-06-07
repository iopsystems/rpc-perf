use super::*;
use config::{Command, ValueKind, Verb};
use rand::distributions::{Alphanumeric, Uniform};
use rand::{Rng, RngCore, SeedableRng};
use rand_distr::Distribution as RandomDistribution;
use rand_distr::WeightedAliasIndex;
use rand_xoshiro::{Seed512, Xoshiro512PlusPlus};
use ratelimit::Ratelimiter;
use std::collections::{HashMap, HashSet};
use std::io::Result;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::runtime::Runtime;
use zipf::ZipfDistribution;

pub mod client;
mod publisher;

pub use client::{ClientRequest, ClientWorkItem};
pub use publisher::PublisherWorkItem;

static SEQUENCE_NUMBER: AtomicU64 = AtomicU64::new(0);

const KEY_GENERATOR_SEED: Seed512 = Seed512([
    0xab, 0xa0, 0x43, 0xe6, 0x2f, 0x1e, 0xa9, 0x18, 0xdc, 0x01, 0x3c, 0x57, 0x7f, 0xe5, 0x0c, 0x92,
    0x9e, 0x66, 0x6e, 0x60, 0xbe, 0x80, 0x11, 0x06, 0x7a, 0xdb, 0x22, 0x7d, 0xea, 0xdd, 0xd7, 0xc4,
    0x98, 0x0e, 0x01, 0x71, 0x38, 0x0b, 0x54, 0xb4, 0xeb, 0x9a, 0xa2, 0x35, 0xe5, 0xc3, 0x4e, 0xef,
    0x67, 0xcc, 0xcb, 0xf3, 0x96, 0x56, 0x3d, 0x94, 0xd0, 0x74, 0xc1, 0x3c, 0x94, 0xde, 0xc9, 0xff,
]);

const WORKLOAD_SEED: Seed512 = Seed512([
    0xa3, 0x3f, 0xd0, 0x2c, 0xc4, 0x3f, 0xd0, 0x99, 0xab, 0x49, 0xf4, 0x4e, 0x35, 0x63, 0xda, 0x82,
    0xa1, 0x5b, 0x53, 0xd2, 0xb8, 0x4b, 0x57, 0xef, 0x7d, 0xe1, 0xc5, 0x3d, 0xbc, 0x46, 0xf9, 0x56,
    0x60, 0x8b, 0xd0, 0xdc, 0x71, 0x47, 0x01, 0x41, 0x26, 0xbb, 0x99, 0x72, 0x1b, 0x0d, 0x50, 0x55,
    0x50, 0xc5, 0xd4, 0xb9, 0x35, 0xcf, 0xb1, 0x2e, 0x9d, 0xf1, 0x87, 0x17, 0x5e, 0x4b, 0x9d, 0x68,
]);

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

    // create a PRNG to generate seeds for each workload generating thread by
    // using a PRNG, individual runs (when there is just one workload thread)
    // should produce the exact same sequence of requests. When there is more
    // than one workload thread, the behavior is non-deterministic due to
    // unpredictable interleaving order between each thread adding requests into
    // the work queue.
    let mut rng = Xoshiro512PlusPlus::from_seed(WORKLOAD_SEED);

    // spawn the request generators on a blocking threads
    for _ in 0..config.workload().threads() {
        let client_sender = client_sender.clone();
        let pubsub_sender = pubsub_sender.clone();
        let generator = generator.clone();

        // generate the seed for this workload thread
        let mut seed = [0; 64];
        rng.fill_bytes(&mut seed);

        workload_rt.spawn_blocking(move || {
            // since this seed is unique, each workload thread should produce
            // requests in a different sequence
            let mut rng = Xoshiro512PlusPlus::from_seed(Seed512(seed));

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
        let ratelimiter = config.workload().ratelimit().map(|rate| {
            let amount = (rate.get() as f64 / 1_000_000.0).ceil() as u64;

            // even though we might not have nanosecond level clock resolution,
            // by using a nanosecond level duration, we achieve more accurate
            // ratelimits.
            let interval = Duration::from_nanos(1_000_000_000 / (rate.get() / amount));

            let capacity = std::cmp::max(100, amount);

            Arc::new(
                Ratelimiter::builder(amount, interval)
                    .max_tokens(capacity)
                    .build()
                    .expect("failed to initialize ratelimiter"),
            )
        });

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
        let index = topics.topic_dist.sample(rng);
        let topic = topics.topics[index].clone();

        let mut m = vec![0_u8; topics.message_len];
        // add a header
        [m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7]] =
            [0x54, 0x45, 0x53, 0x54, 0x49, 0x4E, 0x47, 0x21];
        rng.fill(&mut m[32..topics.message_len]);

        PublisherWorkItem::Publish { topic, message: m }
    }

    fn generate_request(&self, keyspace: &Keyspace, rng: &mut dyn RngCore) -> ClientWorkItem {
        let command = &keyspace.commands[keyspace.command_dist.sample(rng)];

        let request = match command.verb() {
            Verb::Get => ClientRequest::Get(client::Get {
                key: keyspace.sample(rng),
            }),
            Verb::Set => ClientRequest::Set(client::Set {
                key: keyspace.sample(rng),
                value: keyspace.gen_value(rng),
            }),
            Verb::Delete => ClientRequest::Delete(client::Delete {
                key: keyspace.sample(rng),
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
                amount: rng.gen(),
            }),
            Verb::HashSet => {
                let mut data = HashMap::new();
                while data.len() < command.cardinality() {
                    data.insert(keyspace.sample_inner(rng), keyspace.gen_value(rng));
                }
                ClientRequest::HashSet(client::HashSet {
                    key: keyspace.sample(rng),
                    data,
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
                let members = members.drain().map(|m| (m, rng.gen())).collect();
                ClientRequest::SortedSetAdd(client::SortedSetAdd {
                    key: keyspace.sample(rng),
                    members,
                })
            }
            Verb::SortedSetMembers => ClientRequest::SortedSetMembers(client::SortedSetMembers {
                key: keyspace.sample(rng),
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
                    amount: rng.gen(),
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

        ClientWorkItem::Request {
            request,
            sequence: SEQUENCE_NUMBER.fetch_add(1, Ordering::Relaxed),
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
    topic_dist: Distribution,
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
        let topic_dist = match topics.topic_distribution() {
            config::Distribution::Uniform => Distribution::Uniform(Uniform::new(0, ntopics)),
            config::Distribution::Zipf => {
                Distribution::Zipf(ZipfDistribution::new(ntopics, 1.0).unwrap())
            }
        };

        // we use a predictable seed to generate the topic names
        let mut rng = Xoshiro512PlusPlus::from_seed(KEY_GENERATOR_SEED);
        let mut topics = HashSet::with_capacity(ntopics);
        while topics.len() < ntopics {
            let topic = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(topiclen)
                .collect::<Vec<u8>>();
            let _ = topics.insert(unsafe { std::str::from_utf8_unchecked(&topic) }.to_string());
        }
        let topics = topics.drain().map(|k| k.into()).collect();

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
    key_dist: Distribution,
    commands: Vec<Command>,
    command_dist: WeightedAliasIndex<usize>,
    inner_keys: Vec<Arc<[u8]>>,
    inner_key_dist: Distribution,
    vlen: usize,
    vkind: ValueKind,
}

#[derive(Clone)]
pub enum Distribution {
    Uniform(rand::distributions::Uniform<usize>),
    Zipf(zipf::ZipfDistribution),
}

impl Distribution {
    pub fn sample(&self, rng: &mut dyn RngCore) -> usize {
        match self {
            Self::Uniform(dist) => dist.sample(rng),
            Self::Zipf(dist) => dist.sample(rng),
        }
    }
}

impl Keyspace {
    pub fn new(keyspace: &config::Keyspace) -> Self {
        // nkeys must be >= 1
        let nkeys = std::cmp::max(1, keyspace.nkeys());
        let klen = keyspace.klen();

        // we use a predictable seed to generate the keys in the keyspace
        let mut rng = Xoshiro512PlusPlus::from_seed(KEY_GENERATOR_SEED);
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
            config::Distribution::Uniform => Distribution::Uniform(Uniform::new(0, nkeys)),
            config::Distribution::Zipf => {
                Distribution::Zipf(ZipfDistribution::new(nkeys, 1.0).unwrap())
            }
        };

        let nkeys = keyspace.inner_keys_nkeys().unwrap_or(1);
        let klen = keyspace.inner_keys_klen().unwrap_or(1);

        // we use a predictable seed to generate the keys in the keyspace
        let mut rng = Xoshiro512PlusPlus::from_seed(KEY_GENERATOR_SEED);
        let mut inner_keys = HashSet::with_capacity(nkeys);
        while inner_keys.len() < nkeys {
            let key = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(klen)
                .collect::<Vec<u8>>();
            let _ = inner_keys.insert(key);
        }
        let inner_keys: Vec<Arc<[u8]>> = inner_keys.drain().map(|k| k.into()).collect();
        let inner_key_dist = Distribution::Uniform(Uniform::new(0, nkeys));

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
        let index = self.key_dist.sample(rng);
        self.keys[index].clone()
    }

    pub fn sample_inner(&self, rng: &mut dyn RngCore) -> Arc<[u8]> {
        let index = self.inner_key_dist.sample(rng);
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

    let ratelimiter = config.client().unwrap().reconnect_rate().map(|rate| {
        let amount = (rate.get() as f64 / 1_000_000.0).ceil() as u64;

        // even though we might not have nanosecond level clock resolution,
        // by using a nanosecond level duration, we achieve more accurate
        // ratelimits.
        let interval = Duration::from_nanos(1_000_000_000 / (rate.get() / amount));

        let capacity = std::cmp::max(100, amount);

        Arc::new(
            Ratelimiter::builder(amount, interval)
                .max_tokens(capacity)
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
                let _ = work_sender.send(ClientWorkItem::Reconnect).await;
            }
            Err(d) => {
                std::thread::sleep(d);
            }
        }
    }

    Ok(())
}
