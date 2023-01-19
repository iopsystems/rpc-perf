use super::*;

pub async fn get_requests<T: Distribution<usize>>(
    work_sender: Sender<WorkItem>,
    mut keyspace: Keyspace<T>,
    rate: Option<NonZeroU64>,
) -> Result<()> {
    // if the rate is none, we treat as non-ratelimited and add items to
    // the work queue as quickly as possible
    if rate.is_none() {
        while RUNNING.load(Ordering::Relaxed) {
            let key = keyspace.sample();

            let _ = work_sender.send(WorkItem::Get { key }).await;
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
            let _ = work_sender.send(WorkItem::Get { key }).await;
        }
    }

    Ok(())
}