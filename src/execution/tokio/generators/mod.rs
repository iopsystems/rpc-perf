// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

// use rand_distr::Distribution as RandDistribution;
use super::*;

use workload::WorkItem;

use tokio::time::Interval;

// mod get;
// mod hash_delete;
// mod hash_exists;
// mod hash_get;
// mod hash_multi_get;
// mod hash_set;
// mod ping;
// mod set;

// pub use get::*;
// pub use hash_delete::*;
// pub use hash_exists::*;
// pub use hash_get::*;
// pub use hash_multi_get::*;
// pub use hash_set::*;
// pub use ping::*;
// pub use set::*;

// use rand_distr::WeightedAliasIndex;

pub async fn requests(
    work_sender: Sender<WorkItem>,
    config: Config,
    rate: Option<NonZeroU64>,
) -> Result<()> {
    let mut rng = Xoshiro256Plus::seed_from_u64(0);
    let keyspace = config.choose_keyspace(&mut rng);
    let command = keyspace.choose_command(&mut rng);

    let work_item = match command.verb() {
        Verb::Get => {
            WorkItem::Get { key: keyspace.generate_key(&mut rng).into() }
        }
        Verb::Set => {
            WorkItem::Set { 
                key: keyspace.generate_key(&mut rng).into(),
                value: keyspace.generate_value(&mut rng).into(),
            }
        }
    };

    // let mut weights = Vec::with_capacity(keyspaces.len());
    // let mut keyspaces: Vec<Keyspace>
    //  = keyspaces.iter().map(|ks| {
    //     let klen = ks.key_length();
    //     let nkeys = ks.key_count();
    //     let dist = Distribution::Uniform(Uniform::from(0..nkeys));
    //     weights.push(1);
    //     Keyspace::new(klen, nkeys, dist)
    // }).collect();

    // let keyspace_distr = WeightedAliasIndex::new(weights).unwrap();

    // let mut rng = Xoshiro256Plus::seed_from_u64(0);

    // // if the rate is none, we treat as non-ratelimited and add items to
    // // the work queue as quickly as possible
    // if rate.is_none() {
    //     while RUNNING.load(Ordering::Relaxed) {
    //         let keyspace = &mut keyspaces[keyspace_distr.sample(&mut rng)];
    //         work_sender.send(keyspace.generate_work_item());
    //         // let _ = work_sender.send(WorkItem::Get { key }).await;
    //     }

    //     return Ok(());
    // }

    // let (quanta, mut interval) = convert_ratelimit(rate.unwrap());

    // while RUNNING.load(Ordering::Relaxed) {
    //     interval.tick().await;
    //     for _ in 0..quanta {
    //         // let key = keyspace.sample();
    //         // let _ = work_sender.send(Item::Get { key }).await;
    //     }
    // }

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

    (quanta, ::tokio::time::interval(Duration::from_millis(interval)))
}
