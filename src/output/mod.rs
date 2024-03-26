use crate::*;
use config::MetricsFormat;
use metriken_exposition::{MsgpackToParquet, Snapshot, SnapshotterBuilder};
use std::io::{BufWriter, Write};

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

pub fn log(config: &Config) {
    let mut interval = config.general().interval().as_millis();
    let mut duration = config.general().duration().as_millis();

    let mut window_id = 0;

    let mut snapshot = MetricsSnapshot::default();
    snapshot.update();

    let client = !config.workload().keyspaces().is_empty();
    let pubsub = !config.workload().topics().is_empty();

    while duration > 0 {
        std::thread::sleep(Duration::from_millis(1));

        interval = interval.saturating_sub(1);
        duration = duration.saturating_sub(1);

        if interval == 0 {
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

            interval = config.general().interval().as_millis();
            window_id += 1;
        }
    }
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

    output!("Publishers: Current: {}", PUBSUB_PUBLISHER_CURR.value(),);

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

pub fn metrics(config: Config) {
    if config.general().metrics_output().is_none() {
        return;
    }
    let output = config.general().metrics_output().unwrap();

    let file = tempfile::NamedTempFile::new_in("./");
    if file.is_err() {
        return;
    }
    let file = file.unwrap();
    let mut writer = BufWriter::new(file.as_file());

    let mut now = std::time::Instant::now();
    let mut next = now + Duration::from_secs(1);
    let end = now + config.general().duration();

    while end > now {
        std::thread::sleep(Duration::from_millis(1));

        now = std::time::Instant::now();

        if next <= now {
            let snapshot = SnapshotterBuilder::new().build().snapshot();

            let buf = match config.general().metrics_format() {
                MetricsFormat::Json => Snapshot::to_json(&snapshot).expect("failed to serialize"),
                MetricsFormat::MsgPack | MetricsFormat::Parquet => {
                    Snapshot::to_msgpack(&snapshot).expect("failed to serialize")
                }
            };
            let _ = writer.write_all(&buf);

            next += Duration::from_secs(1);
        }
    }
    let _ = writer.flush();
    drop(writer);

    // Post-process metrics into a parquet file
    if config.general().metrics_format() == MetricsFormat::Parquet {
        // If parquet conversion fails, log the error and fall through to the
        // regular path which stores the file as a msgpack artifact.
        if let Err(e) = MsgpackToParquet::new().convert_file_path(file.path(), &output) {
            eprintln!("error converting output to parquet: {}", e);
        } else {
            return;
        }
    }

    // Persist the temp file to the desired output artifact
    if let Err(e) = file.persist(output) {
        eprintln!("error persisting metrics file, no artifact saved: {}", e);
    }
}
