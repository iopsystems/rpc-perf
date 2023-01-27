// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::*;
use crate::config::*;
use crate::workload::*;
use crate::workload::Keyspace;

use async_channel::{bounded, Sender};
use rand::distributions::{Alphanumeric, Uniform};
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256Plus;
use ringlog::Drain;
use ::tokio::io::*;
use ::tokio::runtime::{Builder};
use ::tokio::time::{Duration, sleep};

use core::num::NonZeroU64;

mod generators;
mod drivers;

use self::generators::*;

const CONNECTIONS: usize = 16;

// const PROTOCOL: Protocol = Protocol::Memcache;

// this should take some sort of configuration
pub fn run(config: config::File, log: Box<dyn Drain>) -> Result<()> {
    let mut log = log;

    // Create the runtime
    let mut rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.general().threads())
        .build()?;

    // TODO: figure out what a reasonable size is here
    let (work_sender, work_receiver) = bounded(1_000_000);

    let poolsize = config.general().poolsize();

    match config.general().protocol() {
        Protocol::Memcache => drivers::memcache::launch_tasks(&mut rt, poolsize, work_receiver),
        Protocol::Momento => drivers::momento::launch_tasks(&mut rt, poolsize, work_receiver),
        Protocol::Ping => drivers::ping::launch_tasks(&mut rt, poolsize, work_receiver),
        Protocol::Resp => drivers::resp::launch_tasks(&mut rt, poolsize, work_receiver),
    }

    // rt.spawn

    for keyspace in config.keyspaces() {
        let klen = keyspace.key_length();
        let nkeys = keyspace.key_count();
        // let dist = keyspace.distribution();
        let dist = Uniform::from(0..nkeys);
        let keyspace = Keyspace::new(klen, nkeys, Box::new(dist), None);

        for command in keyspace.commands() {

        }
    }

    for command in config.commands() {
        match command.command() {
            // config::Command::Add {

            // }
            config::Command::Get => {
                let klen = command.keyspace().len();
                let nkeys = command.keyspace().keys();
                let distribution = Uniform::from(0..nkeys);
                let keyspace = Keyspace::new(klen, nkeys, Box::new(distribution), None);
                rt.spawn(get_requests(
                    work_sender.clone(),
                    keyspace.clone(),
                    NonZeroU64::new(80000),
                ));
            }
            config::Command::Set => {
                let klen = command.keyspace().len();
                let nkeys = command.keyspace().keys();
                let distribution = Uniform::from(0..nkeys);
                let keyspace = Keyspace::new(klen, nkeys, Box::new(distribution), None);
                rt.spawn(set_requests(
                    work_sender.clone(),
                    keyspace.clone(),
                    64,
                    NonZeroU64::new(80000),
                ));
            }
            _ => {

            }
        }
    }

    // // initialize keyspace - used for kv requests
    // let klen = 64;
    // let nkeys = 1_000_000;
    // let distribution = Uniform::from(0..nkeys);
    // let keyspace = Keyspace::new(klen, nkeys, Box::new(distribution), None);

    // // initialize outer keyspace - used for hash requests
    // let klen = 32;
    // let nkeys = 10_000;
    // let distribution = Uniform::from(0..nkeys);
    // let outer_keyspace = Keyspace::new(klen, nkeys, Box::new(distribution), None);

    // // initialize inner keyspace - used for hash requests
    // let klen = 16;
    // let nkeys = 1000;
    // let distribution = Uniform::from(0..nkeys);
    // let inner_keyspace = InnerKeyspace::new(klen, nkeys, Box::new(distribution), Some(Box::new(Uniform::from(1..16))));

    // launch the workload generator(s)
    // rt.spawn(get_requests(
    //     work_sender.clone(),
    //     keyspace.clone(),
    //     NonZeroU64::new(80000),
    // ));
    // rt.spawn(hash_delete_requests(
    //     work_sender.clone(),
    //     outer_keyspace.clone(),
    //     inner_keyspace.clone(),
    //     NonZeroU64::new(5),
    // ));
    // rt.spawn(hash_exists_requests(
    //     work_sender.clone(),
    //     outer_keyspace.clone(),
    //     inner_keyspace.clone(),
    //     NonZeroU64::new(5),
    // ));
    // rt.spawn(hash_get_requests(
    //     work_sender.clone(),
    //     outer_keyspace.clone(),
    //     inner_keyspace.clone(),
    //     NonZeroU64::new(5),
    // ));
    // rt.spawn(hash_set_requests(
    //     work_sender.clone(),
    //     outer_keyspace,
    //     inner_keyspace.clone(),
    //     64,
    //     NonZeroU64::new(5),
    // ));
    // rt.spawn(hash_multi_get_requests(
    //     work_sender.clone(),
    //     keyspace.clone(),
    //     inner_keyspace,
    //     NonZeroU64::new(5),
    // ));
    // rt.spawn(ping_requests(
    //     work_sender.clone(),
    //     NonZeroU64::new(5),
    // ));
    // rt.spawn(set_requests(work_sender, keyspace, 64, NonZeroU64::new(20000)));

    rt.spawn(async move {
        while RUNNING.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(50)).await;
            let _ = log.flush();
        }
        let _ = log.flush();
    });

    let window = config.general().interval() as u64;
    let mut interval = config.general().interval();
    let mut duration = config.general().duration();

    while duration > 0 {
        rt.block_on(async {
            sleep(Duration::from_secs(1)).await;
        });

        interval = interval.saturating_sub(1);
        duration = duration.saturating_sub(1);

        if interval == 0 {
            info!(
                "connect rate (/s): attempt: {}",
                CONNECT.reset() / window
            );
            let get_total = GET.reset();
            let get_ex = GET_EX.reset();
            let get_hit = GET_KEY_HIT.reset() as f64;
            let get_miss = GET_KEY_MISS.reset() as f64;
            let get_hr = 100.0 * get_hit / (get_hit + get_miss);
            let get_sr = 100.0 - (get_ex as f64 / get_total as f64) * 100.0;
            info!("command response: get: rate (/s): {} hit rate(%): {:.2} success rate(%): {:.2}",
                get_total / window,
                get_hr,
                get_sr,
            );
            info!("response rate (/s): ok: {} error: {} timeout: {}",
                RESPONSE_OK.reset() / window,
                RESPONSE_EX.reset() / window,
                RESPONSE_TIMEOUT.reset() / window,
            );
            info!(
                "response latency (us): p25: {} p50: {} p75: {} p90: {} p99: {} p999: {} p9999: {}",
                RESPONSE_LATENCY.percentile(25.0).map(|b| format!("{}", b.high() / 1000)).unwrap_or_else(|_| "ERR".to_string()),
                RESPONSE_LATENCY.percentile(50.0).map(|b| format!("{}", b.high() / 1000)).unwrap_or_else(|_| "ERR".to_string()),
                RESPONSE_LATENCY.percentile(75.0).map(|b| format!("{}", b.high() / 1000)).unwrap_or_else(|_| "ERR".to_string()),
                RESPONSE_LATENCY.percentile(90.0).map(|b| format!("{}", b.high() / 1000)).unwrap_or_else(|_| "ERR".to_string()),
                RESPONSE_LATENCY.percentile(99.0).map(|b| format!("{}", b.high() / 1000)).unwrap_or_else(|_| "ERR".to_string()),
                RESPONSE_LATENCY.percentile(99.9).map(|b| format!("{}", b.high() / 1000)).unwrap_or_else(|_| "ERR".to_string()),
                RESPONSE_LATENCY.percentile(99.99).map(|b| format!("{}", b.high() / 1000)).unwrap_or_else(|_| "ERR".to_string()),
            );

            interval = config.general().interval();
        }


    }

    RUNNING.store(false, Ordering::Relaxed);

    Ok(())
}
