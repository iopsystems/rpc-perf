// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use super::*;

pub async fn set_requests(
    work_sender: Sender<WorkItem>,
    mut keyspace: Keyspace,
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

    let (quanta, mut interval) = convert_ratelimit(rate.unwrap());

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