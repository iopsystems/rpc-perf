use crate::*;
use chrono::{Timelike, Utc};
use config::MetricsFormat;
use metriken_exposition::{MsgpackToParquet, ParquetOptions, Snapshot, SnapshotterBuilder};
use std::os::fd::{AsRawFd, FromRawFd};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::time::{timeout, Instant};

#[macro_export]
macro_rules! output {
    () => {
        let now = chrono::Utc::now();
        println!("{}", now.to_rfc3339_opts(chrono::SecondsFormat::Millis, false));
    };
    ($($arg:tt)*) => {{
        let now = chrono::Utc::now();
        println!("{} {}", now.to_rfc3339_opts(chrono::SecondsFormat::Millis, false), format_args!($($arg)*));
    }};
}

pub async fn log(config: Config) {
    WAIT.fetch_add(1, Ordering::Relaxed);

    let mut window_id = 0;

    let mut snapshot = MetricsSnapshot::default();
    tokio::time::sleep(Duration::from_secs(1)).await;
    snapshot.update();

    let client = !config.workload().keyspaces().is_empty();
    let pubsub = !config.workload().topics().is_empty();

    // get an aligned start time
    let start = tokio::time::Instant::now() - Duration::from_nanos(Utc::now().nanosecond() as u64)
        + config.general().interval();

    // get the stop time
    let stop = start + config.general().duration();

    let mut interval = tokio::time::interval_at(start, config.general().interval());

    while RUNNING.load(Ordering::Relaxed) && Instant::now() + config.general().interval() <= stop {
        // use a timeout here so we always check RUNNING at least once a second
        if timeout(Duration::from_secs(1), interval.tick())
            .await
            .is_err()
        {
            continue;
        }

        snapshot.update();

        output!("-----");
        output!("Window: {}", window_id);

        // output the client stats
        if client {
            client_stats(&mut snapshot);
        }

        // output the pubsub stats
        if pubsub {
            pubsub_stats(&mut snapshot);
        }

        window_id += 1;
    }

    RUNNING.store(false, Ordering::Relaxed);
    WAIT.fetch_sub(1, Ordering::Relaxed);
}

/// Outputs client stats
fn client_stats(snapshot: &mut MetricsSnapshot) {
    let connect_ok = snapshot.counter_rate(CONNECT_OK_COUNTER);
    let connect_ex = snapshot.counter_rate(CONNECT_EX_COUNTER);
    let connect_timeout = snapshot.counter_rate(CONNECT_TIMEOUT_COUNTER);
    let connect_total = snapshot.counter_rate(CONNECT_COUNTER);

    let request_reconnect = snapshot.counter_rate(REQUEST_RECONNECT_COUNTER);
    let request_ok = snapshot.counter_rate(REQUEST_OK_COUNTER);
    let request_unsupported = snapshot.counter_rate(REQUEST_UNSUPPORTED_COUNTER);
    let request_total = snapshot.counter_rate(REQUEST_COUNTER);

    let response_ok = snapshot.counter_rate(RESPONSE_OK_COUNTER);
    let response_ex = snapshot.counter_rate(RESPONSE_EX_COUNTER);
    let response_timeout = snapshot.counter_rate(RESPONSE_TIMEOUT_COUNTER);
    let response_hit = snapshot.counter_rate(RESPONSE_HIT_COUNTER);
    let response_miss = snapshot.counter_rate(RESPONSE_MISS_COUNTER);

    let connect_sr = 100.0 * connect_ok / connect_total;

    let response_latency = snapshot.percentiles(RESPONSE_LATENCY_HISTOGRAM);

    output!(
        "Client Connection: Open: {} Success Rate: {:.2} %",
        CONNECT_CURR.value(),
        connect_sr
    );
    output!(
        "Client Connection Rates (/s): Attempt: {:.2} Opened: {:.2} Errors: {:.2} Timeout: {:.2} Closed: {:.2}",
        connect_total,
        connect_ok,
        connect_ex,
        connect_timeout,
        request_reconnect,
    );

    let request_sr = 100.0 * request_ok / request_total;
    let request_ur = 100.0 * request_unsupported / request_total;

    output!(
        "Client Request: Success: {:.2} % Unsupported: {:.2} %",
        request_sr,
        request_ur,
    );
    output!(
        "Client Request Rate (/s): Ok: {:.2} Unsupported: {:.2}",
        request_ok,
        request_unsupported,
    );

    let response_total = response_ok + response_ex + response_timeout;

    let response_sr = 100.0 * response_ok / response_total;
    let response_to = 100.0 * response_timeout / response_total;
    let response_hr = 100.0 * response_hit / (response_hit + response_miss);

    output!(
        "Client Response: Success: {:.2} % Timeout: {:.2} % Hit: {:.2} %",
        response_sr,
        response_to,
        response_hr,
    );
    output!(
        "Client Response Rate (/s): Ok: {:.2} Error: {:.2} Timeout: {:.2}",
        response_ok,
        response_ex,
        response_timeout,
    );

    let mut latencies = "Client Response Latency (us):".to_owned();

    for (label, _percentile, nanoseconds) in response_latency {
        let microseconds = nanoseconds / 1000;
        latencies.push_str(&format!(" {label}: {microseconds}"))
    }

    output!("{latencies}");
}

/// Output pubsub metrics and return the number of successful publish operations
fn pubsub_stats(snapshot: &mut MetricsSnapshot) {
    // publisher stats
    let pubsub_tx_ex = snapshot.counter_rate(PUBSUB_PUBLISH_EX_COUNTER);
    let pubsub_tx_ok = snapshot.counter_rate(PUBSUB_PUBLISH_OK_COUNTER);
    let pubsub_tx_timeout = snapshot.counter_rate(PUBSUB_PUBLISH_TIMEOUT_COUNTER);
    let pubsub_tx_total = snapshot.counter_rate(PUBSUB_PUBLISH_COUNTER);

    let pubsub_publish_latency = snapshot.percentiles(PUBSUB_PUBLISH_LATENCY_HISTOGRAM);

    // subscriber stats
    let pubsub_rx_ok = snapshot.counter_rate(PUBSUB_RECEIVE_OK_COUNTER);
    let pubsub_rx_ex = snapshot.counter_rate(PUBSUB_RECEIVE_EX_COUNTER);
    let pubsub_rx_corrupt = snapshot.counter_rate(PUBSUB_RECEIVE_CORRUPT_COUNTER);
    let pubsub_rx_invalid = snapshot.counter_rate(PUBSUB_RECEIVE_INVALID_COUNTER);
    let pubsub_rx_total = snapshot.counter_rate(PUBSUB_RECEIVE_COUNTER);

    // end-to-end stats
    let pubsub_latency = snapshot.percentiles(PUBSUB_LATENCY_HISTOGRAM);
    output!("Ratelimit: Current: {}", RATELIMIT_CURR.value());
    output!("Publishers: Current: {}", PUBSUB_PUBLISHER_CURR.value());

    let pubsub_tx_sr = 100.0 * pubsub_tx_ok / pubsub_tx_total;
    let pubsub_tx_to = 100.0 * pubsub_tx_timeout / pubsub_tx_total;
    output!(
        "Publisher Publish: Success: {:.2} % Timeout: {:.2} %",
        pubsub_tx_sr,
        pubsub_tx_to
    );

    output!(
        "Publisher Publish Rate (/s): Ok: {:.2} Error: {:.2} Timeout: {:.2}",
        pubsub_tx_ok,
        pubsub_tx_ex,
        pubsub_tx_timeout,
    );

    output!("Subscribers: Current: {}", PUBSUB_SUBSCRIBER_CURR.value(),);

    let pubsub_rx_sr = 100.0 * pubsub_rx_ok / pubsub_rx_total;
    let pubsub_rx_cr = 100.0 * pubsub_rx_corrupt / pubsub_rx_total;
    output!(
        "Subscriber Receive: Success: {:.2} % Corrupted: {:.2} %",
        pubsub_rx_sr,
        pubsub_rx_cr
    );

    output!(
        "Subscriber Receive Rate (/s): Ok: {:.2} Error: {:.2} Corrupt: {:.2} Invalid: {:.2}",
        pubsub_rx_ok,
        pubsub_rx_ex,
        pubsub_rx_corrupt,
        pubsub_rx_invalid,
    );

    let mut latencies = "Pubsub Publish Latency (us):".to_owned();

    for (label, _percentile, nanoseconds) in pubsub_publish_latency {
        let microseconds = nanoseconds / 1000;
        latencies.push_str(&format!(" {label}: {microseconds}"))
    }

    output!("{latencies}");

    let mut latencies = "Pubsub End-to-End Latency (us):".to_owned();

    for (label, _percentile, nanoseconds) in pubsub_latency {
        let microseconds = nanoseconds / 1000;
        latencies.push_str(&format!(" {label}: {microseconds}"))
    }

    output!("{latencies}");
}

pub async fn metrics(config: Config) {
    if config.metrics().is_none() {
        return;
    }
    let metrics_config = config.metrics().unwrap();
    let output = metrics_config.output();

    let file = tempfile::NamedTempFile::new_in("./");
    if file.is_err() {
        return;
    }
    let file = file.unwrap();
    let mut writer = BufWriter::new(unsafe { File::from_raw_fd(file.as_raw_fd()) });

    WAIT.fetch_add(1, Ordering::Relaxed);

    // get an aligned start time
    let start = tokio::time::Instant::now() - Duration::from_nanos(Utc::now().nanosecond() as u64)
        + Duration::from_secs(1);

    // get the stop time
    let stop = start + config.general().duration();

    let mut interval = tokio::time::interval_at(start, metrics_config.interval());

    let snapshotter = SnapshotterBuilder::new()
        .metadata("source".to_string(), env!("CARGO_BIN_NAME").to_string())
        .metadata("version".to_string(), env!("CARGO_PKG_VERSION").to_string())
        .metadata(
            "sampling_interval_ms".to_string(),
            metrics_config.interval().as_millis().to_string(),
        )
        .build();

    while RUNNING.load(Ordering::Relaxed) && Instant::now() + config.general().interval() <= stop {
        // use a timeout here so we always check RUNNING at least once a second
        if timeout(Duration::from_secs(1), interval.tick())
            .await
            .is_err()
        {
            continue;
        }

        let snapshot = snapshotter.snapshot();

        let buf = match metrics_config.format() {
            MetricsFormat::Json => Snapshot::to_json(&snapshot).expect("failed to serialize"),
            MetricsFormat::MsgPack | MetricsFormat::Parquet => {
                Snapshot::to_msgpack(&snapshot).expect("failed to serialize")
            }
        };
        let _ = writer.write_all(&buf).await;
    }

    let _ = writer.flush().await;
    drop(writer);

    // Post-process metrics into a parquet file
    if metrics_config.format() == MetricsFormat::Parquet {
        // If parquet conversion fails, log the error and fall through to the
        // regular path which stores the file as a msgpack artifact.
        //
        // NOTE: although this function is blocking, we always launch the
        // metrics task into a runtime with multiple threads that handles only
        // the control plane tasks. In the future, we may wish to move this
        // conversion out onto a thread in the blocking pool instead.
        let mut opts = ParquetOptions::new().histogram_type(metrics_config.histogram().into());
        if let Some(x) = metrics_config.batch_size() {
            opts = opts.max_batch_size(x);
        }
        if let Err(e) = MsgpackToParquet::with_options(opts).convert_file_path(file.path(), output)
        {
            eprintln!("error converting output to parquet: {}", e);
        } else {
            WAIT.fetch_sub(1, Ordering::Relaxed);
            return;
        }
    }

    // Persist the temp file to the desired output artifact
    if let Err(e) = file.persist(output) {
        eprintln!("error persisting metrics file, no artifact saved: {}", e);
    }

    WAIT.fetch_sub(1, Ordering::Relaxed);
}
