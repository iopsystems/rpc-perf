// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use crate::workload::*;

use async_channel::{bounded, Sender};
use rand::distributions::{Alphanumeric, Distribution, Uniform};
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256Plus;
use ringlog::Drain;
use ::tokio::io::*;
use ::tokio::runtime::{Builder};
use ::tokio::time::{Duration, sleep};

use core::num::NonZeroU64;
use core::sync::atomic::{AtomicBool, Ordering};

mod generators;
mod drivers;

use self::generators::*;

static RUNNING: AtomicBool = AtomicBool::new(true);

const WINDOW: u64 = 60;
const WORKERS: usize = 8;
const CONNECTIONS: usize = 16;

const PROTOCOL: Protocol = Protocol::Resp;

#[allow(dead_code)]
enum Protocol {
    // Memcache,
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
        // Protocol::Memcache => launch_memcache_tasks(&mut rt, work_receiver),
        Protocol::Momento => drivers::momento::launch_tasks(&mut rt, work_receiver),
        Protocol::Ping => drivers::ping::launch_tasks(&mut rt, work_receiver),
        Protocol::Resp => drivers::resp::launch_tasks(&mut rt, work_receiver),
    }

    // initialize keyspace - used for kv requests
    let klen = 64;
    let nkeys = 10_000;
    let distribution = Uniform::from(0..nkeys);
    let keyspace = Keyspace::new(klen, nkeys, Box::new(distribution), None);

    // initialize outer keyspace - used for hash requests
    let klen = 32;
    let nkeys = 10_000;
    let distribution = Uniform::from(0..nkeys);
    let outer_keyspace = Keyspace::new(klen, nkeys, Box::new(distribution), None);

    // initialize inner keyspace - used for hash requests
    let klen = 16;
    let nkeys = 1000;
    let distribution = Uniform::from(0..nkeys);
    let inner_keyspace = InnerKeyspace::new(klen, nkeys, Box::new(distribution), Some(Box::new(Uniform::from(1..16))));

    // launch the workload generator(s)
    rt.spawn(get_requests(
        work_sender.clone(),
        keyspace.clone(),
        NonZeroU64::new(10000),
    ));
    rt.spawn(hash_delete_requests(
        work_sender.clone(),
        outer_keyspace.clone(),
        inner_keyspace.clone(),
        NonZeroU64::new(5),
    ));
    rt.spawn(hash_exists_requests(
        work_sender.clone(),
        outer_keyspace.clone(),
        inner_keyspace.clone(),
        NonZeroU64::new(5),
    ));
    rt.spawn(hash_get_requests(
        work_sender.clone(),
        outer_keyspace.clone(),
        inner_keyspace.clone(),
        NonZeroU64::new(5),
    ));
    rt.spawn(hash_set_requests(
        work_sender.clone(),
        outer_keyspace,
        inner_keyspace.clone(),
        64,
        NonZeroU64::new(5000),
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
