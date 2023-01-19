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
use tokio::io::*;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;
use tokio::time::*;

mod get;
mod hash_get;

use self::get::*;
use self::hash_get::*;

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
pub fn run() -> Result<()> {
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
    let keyspace = Keyspace::new(klen, nkeys, Box::new(distribution));

    // initialize inner keyspace (for hash operations)
    let klen = 16;
    let nkeys = 1000;
    let distribution = Uniform::from(0..nkeys);
    let inner_keyspace = InnerKeyspace::new(klen, nkeys, Box::new(distribution));

    // launch the workload generator(s)
    rt.spawn(get_requests(
        work_sender.clone(),
        keyspace.clone(),
        NonZeroU64::new(10),
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
        inner_keyspace,
        64,
        NonZeroU64::new(5),
    ));
    rt.spawn(set_requests(work_sender, keyspace, 64, NonZeroU64::new(5)));

    for _ in 0..5 {
        rt.block_on(async {
            sleep(Duration::from_secs(WINDOW)).await;
        });

        println!(
            "connect attempt/s: {}",
            CONNECT.reset() / WINDOW
        );
        println!(
            "response error/s: {}",
            RESPONSE_EX.reset() / WINDOW
        );
        println!(
            "response ok/s: {}",
            RESPONSE_OK.reset() / WINDOW
        );
        println!(
            "response timeout/s: {}",
            RESPONSE_TIMEOUT.reset() / WINDOW
        );
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
    pub fn new(klen: usize, count: usize, distribution: Box<T>) -> Self {
        Self {
            inner: Keyspace::new(klen, count, distribution),
        }
    }

    pub fn sample(&mut self) -> Arc<String> {
        self.inner.sample()
    }
}

impl<T> Keyspace<T>
where
    T: Distribution<usize>,
{
    pub fn new(klen: usize, count: usize, distribution: Box<T>) -> Self {
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
            rng,
        }
    }

    pub fn sample(&mut self) -> Arc<String> {
        let idx = self.distribution.sample(&mut self.rng);
        self.keys[idx].clone()
    }
}

#[allow(dead_code)]
pub enum WorkItem {
    Get {
        key: Arc<String>,
    },
    HashGet {
        key: Arc<String>,
        field: Arc<String>,
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

async fn set_requests<T: Distribution<usize>>(
    work_sender: Sender<WorkItem>,
    mut keyspace: Keyspace<T>,
    vlen: usize,
    rate: Option<NonZeroU64>,
) -> Result<()> {
    // initialize rng to generate values
    let mut rng = Xoshiro256Plus::seed_from_u64(0);

    // if the rate is none, we treat as non-ratelimited and add items to
    // the work queue as quickly as possible
    if rate.is_none() {
        while RUNNING.load(Ordering::Relaxed) {
            let key = keyspace.sample();

            let value: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(vlen)
                .map(char::from)
                .collect();

            let _ = work_sender.send(WorkItem::Set { key, value }).await;
        }

        return Ok(());
    }

    let rate = u64::from(rate.unwrap());

    // TODO: this gives approximate rates
    //
    // timer granularity should be millisecond level on most platforms
    // for higher rates, we can insert multiple work items every interval
    let (quanta, interval) = if rate <= 1000 {
        (1, 1000 / rate)
    } else {
        (rate / 1000, 1)
    };

    let mut interval = tokio::time::interval(Duration::from_millis(interval));

    while RUNNING.load(Ordering::Relaxed) {
        interval.tick().await;
        for _ in 0..quanta {
            let key = keyspace.sample();

            let value: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(vlen)
                .map(char::from)
                .collect();

            let _ = work_sender.send(WorkItem::Set { key, value }).await;
        }
    }

    Ok(())
}

async fn hash_set_requests<T: Distribution<usize>>(
    work_sender: Sender<WorkItem>,
    mut keyspace: Keyspace<T>,
    mut inner_keyspace: InnerKeyspace<T>,
    vlen: usize,
    rate: Option<NonZeroU64>,
) -> Result<()> {
    // initialize rng to generate values
    let mut rng = Xoshiro256Plus::seed_from_u64(0);

    // if the rate is none, we treat as non-ratelimited and add items to
    // the work queue as quickly as possible
    if rate.is_none() {
        while RUNNING.load(Ordering::Relaxed) {
            let key = keyspace.sample();
            let field = inner_keyspace.sample();
            let value: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(vlen)
                .map(char::from)
                .collect();

            let _ = work_sender
                .send(WorkItem::HashSet { key, field, value })
                .await;
        }

        return Ok(());
    }

    let rate = u64::from(rate.unwrap());

    // TODO: this gives approximate rates
    //
    // timer granularity should be millisecond level on most platforms
    // for higher rates, we can insert multiple work items every interval
    let (quanta, interval) = if rate <= 1000 {
        (1, 1000 / rate)
    } else {
        (rate / 1000, 1)
    };

    let mut interval = tokio::time::interval(Duration::from_millis(interval));

    while RUNNING.load(Ordering::Relaxed) {
        interval.tick().await;
        for _ in 0..quanta {
            let key = keyspace.sample();
            let field = inner_keyspace.sample();
            let value: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(vlen)
                .map(char::from)
                .collect();

            let _ = work_sender
                .send(WorkItem::HashSet { key, field, value })
                .await;
        }
    }

    Ok(())
}






