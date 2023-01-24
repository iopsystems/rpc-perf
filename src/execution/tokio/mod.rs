use ringlog::Drain;
use async_channel::{bounded, Receiver, Sender};
use core::num::NonZeroU64;
use core::sync::atomic::{AtomicBool, Ordering};

use rand::distributions::Alphanumeric;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use std::collections::HashMap;

use rand::Rng;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256Plus;
use std::sync::Arc;
use ::tokio::io::*;
use ::tokio::net::TcpStream;
use ::tokio::runtime::Builder;
use ::tokio::runtime::Runtime;
use ::tokio::time::*;

use crate::*;

mod generators;
use self::generators::*;

mod memcache;
mod momento;
mod ping;
mod resp;

use self::memcache::*;
use self::momento::*;
use self::ping::*;
use self::resp::*;

counter!(GET);
counter!(GET_EX);
counter!(GET_KEY_HIT);
counter!(GET_KEY_MISS);

counter!(SET);
counter!(SET_EX);
counter!(SET_STORED);

counter!(HASH_GET);
counter!(HASH_GET_EX);
counter!(HASH_GET_FIELD_HIT);
counter!(HASH_GET_FIELD_MISS);

counter!(HASH_SET);
counter!(HASH_SET_EX);
counter!(HASH_SET_STORED);

counter!(PING);
counter!(PING_EX);
counter!(PING_OK);

counter!(CONNECT);
counter!(CONNECT_EX);

counter!(RESPONSE_EX);
counter!(RESPONSE_OK);
counter!(RESPONSE_TIMEOUT);
counter!(RESPONSE_INVALID);


static RUNNING: AtomicBool = AtomicBool::new(true);

const WINDOW: u64 = 60;
const WORKERS: usize = 8;
const CONNECTIONS: usize = 16;

const PROTOCOL: Protocol = Protocol::Memcache;

#[allow(dead_code)]
enum Protocol {
    Memcache,
    Momento,
    Ping,
    Resp,
}

// this should take some sort of configuration
pub fn run(log: Box<dyn Drain>) -> Result<()> {
    let mut log = log;

    // Create the runtime
    let mut rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(WORKERS)
        .build()?;

    // TODO: figure out what a reasonable size is here
    let (work_sender, work_receiver) = bounded(CONNECTIONS * 8);

    match PROTOCOL {
        Protocol::Memcache => launch_memcache_tasks(&mut rt, work_receiver),
        Protocol::Momento => launch_momento_tasks(&mut rt, work_receiver),
        Protocol::Ping => launch_ping_tasks(&mut rt, work_receiver),
        Protocol::Resp => launch_resp_tasks(&mut rt, work_receiver),
    }

    // initialize keyspace
    let klen = 64;
    let nkeys = 10_000;
    let distribution = Uniform::from(0..nkeys);
    let keyspace = Keyspace::new(klen, nkeys, Box::new(distribution), None);

    // initialize inner keyspace (for hash operations)
    let klen = 16;
    let nkeys = 1000;
    let distribution = Uniform::from(0..nkeys);
    let inner_keyspace = InnerKeyspace::new(klen, nkeys, Box::new(distribution), Some(Box::new(Uniform::from(1..16))));

    // launch the workload generator(s)
    rt.spawn(get_requests(
        work_sender.clone(),
        keyspace.clone(),
        NonZeroU64::new(0),
    ));
    rt.spawn(hash_delete_requests(
        work_sender.clone(),
        keyspace.clone(),
        inner_keyspace.clone(),
        NonZeroU64::new(5),
    ));
    rt.spawn(hash_get_requests(
        work_sender.clone(),
        keyspace.clone(),
        inner_keyspace.clone(),
        NonZeroU64::new(5),
    ));
    rt.spawn(hash_set_requests(
        work_sender.clone(),
        keyspace.clone(),
        inner_keyspace.clone(),
        64,
        NonZeroU64::new(5),
    ));
    rt.spawn(hash_multi_get_requests(
        work_sender.clone(),
        keyspace.clone(),
        inner_keyspace,
        NonZeroU64::new(5),
    ));
    rt.spawn(set_requests(work_sender, keyspace, 64, NonZeroU64::new(5)));

    rt.spawn(async move {
        while RUNNING.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(50)).await;
            let _ = log.flush();
        }
        let _ = log.flush();
    });

    for _ in 0..5 {
        rt.block_on(async {
            sleep(Duration::from_secs(WINDOW)).await;
        });

        info!(
            "connect attempt/s: {}",
            CONNECT.reset() / WINDOW
        );
        info!(
            "response error/s: {}",
            RESPONSE_EX.reset() / WINDOW
        );
        info!(
            "response ok/s: {}",
            RESPONSE_OK.reset() / WINDOW
        );
        info!(
            "response timeout/s: {}",
            RESPONSE_TIMEOUT.reset() / WINDOW
        );
        info!(
            "response latency p999: {}",
            RESPONSE_LATENCY.percentile(99.9).map(|b| format!("{}", b.high())).unwrap_or_else(|_| "ERR".to_string())
        )
    }

    RUNNING.store(false, Ordering::Relaxed);

    Ok(())
}

#[derive(Clone)]
pub struct Keyspace<T>
where
    T: Distribution<usize>,
{
    keys: Arc<Box<[Arc<String>]>>,
    distribution: Box<T>,
    cardinality: Option<Box<T>>,
    rng: Xoshiro256Plus,
}

#[derive(Clone)]
pub struct InnerKeyspace<T>
where
    T: Distribution<usize>,
{
    inner: Keyspace<T>,
}

impl<T> InnerKeyspace<T>
where
    T: Distribution<usize>,
{
    pub fn new(klen: usize, count: usize, distribution: Box<T>, cardinality: Option<Box<T>>) -> Self {
        Self {
            inner: Keyspace::new(klen, count, distribution, cardinality),
        }
    }

    pub fn sample(&mut self) -> Arc<String> {
        self.inner.sample()
    }

    pub fn multi_sample(&mut self) -> Vec<Arc<String>> {
        self.inner.multi_sample()
    }
}

impl<T> Keyspace<T>
where
    T: Distribution<usize>,
{
    pub fn new(klen: usize, count: usize, distribution: Box<T>, cardinality: Option<Box<T>>) -> Self {
        let mut rng = Xoshiro256Plus::seed_from_u64(0);

        let mut keys = Vec::with_capacity(count);

        for _ in 0..count {
            let key: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(klen)
                .map(char::from)
                .collect();
            keys.push(Arc::new(key));
        }
        Keyspace {
            keys: Arc::new(keys.into_boxed_slice()),
            distribution,
            cardinality,
            rng,
        }
    }

    pub fn sample(&mut self) -> Arc<String> {
        let idx = self.distribution.sample(&mut self.rng);
        self.keys[idx].clone()
    }

    pub fn multi_sample(&mut self) -> Vec<Arc<String>> {
        let cardinality = if let Some(c) = &self.cardinality {
            c.sample(&mut self.rng)
        } else {
            1
        };

        let mut samples = Vec::with_capacity(cardinality);
        for _ in 0..cardinality {
            let idx = self.distribution.sample(&mut self.rng);
            samples.push(self.keys[idx].clone())
        }
        samples
    }
}

#[allow(dead_code)]
pub enum WorkItem {
    Get {
        key: Arc<String>,
    },
    HashDelete {
        key: Arc<String>,
        fields: Vec<Arc<String>>,
    },
    HashGet {
        key: Arc<String>,
        field: Arc<String>,
    },
    HashMultiGet {
        key: Arc<String>,
        fields: Vec<Arc<String>>,
    },
    HashSet {
        key: Arc<String>,
        field: Arc<String>,
        value: String,
    },
    Set {
        key: Arc<String>,
        value: String,
    },
    Ping,
}








