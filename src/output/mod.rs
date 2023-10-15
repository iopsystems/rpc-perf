use crate::*;
use ratelimit::Ratelimiter;
use rpcperf_dataspec::*;
use std::io::{BufWriter, Write};

#[macro_export]
macro_rules! output {
    () => {
        let now = clocksource::DateTime::now();
        println!("{}", now.to_rfc3339_opts(clocksource::SecondsFormat::Millis, false));
    };
    ($($arg:tt)*) => {{
        let now = clocksource::DateTime::now();
        println!("{} {}", now.to_rfc3339_opts(clocksource::SecondsFormat::Millis, false), format_args!($($arg)*));
    }};
}

pub fn log(config: &Config) {
    let mut interval = config.general().interval().as_millis();
    let mut duration = config.general().duration().as_millis();

    let mut window_id = 0;

    let mut snapshot = MetricsSnapshot::default();

    let mut prev = Instant::now();

    let client = !config.workload().keyspaces().is_empty();
    let pubsub = !config.workload().topics().is_empty();

    while duration > 0 {
        std::thread::sleep(Duration::from_millis(1));

        interval = interval.saturating_sub(1);
        duration = duration.saturating_sub(1);

        if interval == 0 {
            let now = Instant::now();
            let elapsed = now.duration_since(prev).as_secs_f64();
            prev = now;

            snapshot.update();

            output!("-----");
            output!("Window: {}", window_id);

            // output the client stats
            if client {
                client_stats(&mut snapshot, elapsed);
            }

            // output the pubsub stats
            if pubsub {
                pubsub_stats(&mut snapshot, elapsed);
            }

            interval = config.general().interval().as_millis();
            window_id += 1;
        }
    }
}

/// Outputs client stats
fn client_stats(snapshot: &mut MetricsSnapshot, elapsed: f64) {
    let connect_ok = snapshot.counter_rate(CONNECT_OK_COUNTER);
    let connect_ex = snapshot.counter_rate(CONNECT_EX_COUNTER);
    let connect_timeout = snapshot.counter_rate(CONNECT_TIMEOUT_COUNTER);
    let connect_total = snapshot.counter_rate(CONNECT_COUNTER);

    let request_reconnect = snapshot.counter_rate(REQUEST_RECONNECT_COUNTER);
    let request_ok = snapshot.counter_rate(REQUEST_OK_COUNTER);
    let request_unsupported = snapshot.counter_rate(REQUEST_UNSUPPORTED_COUNTER);
    let request_total = snapshot.counter_rate(CONNECT_OK_COUNTER);

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
        request_ok / elapsed,
        request_unsupported / elapsed,
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
        response_ok / elapsed,
        response_ex / elapsed,
        response_timeout / elapsed,
    );

    let mut latencies = "Client Response Latency (us):".to_owned();

    for (label, _percentile, nanoseconds) in response_latency {
        let microseconds = nanoseconds / 1000;
        latencies.push_str(&format!(" {label}: {microseconds}"))
    }

    output!("{latencies}");
}

/// Output pubsub metrics and return the number of successful publish operations
fn pubsub_stats(snapshot: &mut MetricsSnapshot, elapsed: f64) {
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
        pubsub_tx_ok / elapsed,
        pubsub_tx_ex / elapsed,
        pubsub_tx_timeout / elapsed,
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
        pubsub_rx_ok / elapsed,
        pubsub_rx_ex / elapsed,
        pubsub_rx_corrupt / elapsed,
        pubsub_rx_invalid / elapsed,
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

pub fn json(config: Config, ratelimit: Option<&Ratelimiter>) {
    if config.general().json_output().is_none() {
        return;
    }

    let file = std::fs::File::create(config.general().json_output().unwrap());

    if file.is_err() {
        return;
    }

    let mut writer = BufWriter::new(file.unwrap());

    let mut now = std::time::Instant::now();

    let mut prev = now;
    let mut next = now + Duration::from_secs(1);
    let end = now + config.general().duration();

    let mut window_id = 0;

    let mut snapshot = MetricsSnapshot::default();

    while end > now {
        std::thread::sleep(Duration::from_millis(1));

        now = std::time::Instant::now();

        if next <= now {
            snapshot.update();

            let elapsed = now.duration_since(prev).as_secs_f64();
            prev = now;
            next += Duration::from_secs(1);

            let connections = Connections {
                current: CONNECT_CURR.value(),
                total: snapshot.counter_delta(CONNECT_COUNTER),
                opened: snapshot.counter_delta(CONNECT_OK_COUNTER),
                error: snapshot.counter_delta(CONNECT_EX_COUNTER),
                timeout: snapshot.counter_delta(CONNECT_TIMEOUT_COUNTER),
            };

            let requests = Requests {
                total: snapshot.counter_delta(REQUEST_COUNTER),
                ok: snapshot.counter_delta(REQUEST_OK_COUNTER),
                reconnect: snapshot.counter_delta(REQUEST_RECONNECT_COUNTER),
                unsupported: snapshot.counter_delta(REQUEST_UNSUPPORTED_COUNTER),
            };

            let response_ok = snapshot.counter_delta(RESPONSE_OK_COUNTER);
            let response_ex = snapshot.counter_delta(RESPONSE_EX_COUNTER);
            let response_timeout = snapshot.counter_delta(RESPONSE_TIMEOUT_COUNTER);

            let response_total = response_ok + response_ex + response_timeout;

            let responses = Responses {
                total: response_total,
                ok: response_ok,
                error: response_ex,
                timeout: response_timeout,
                hit: snapshot.counter_delta(RESPONSE_HIT_COUNTER),
                miss: snapshot.counter_delta(RESPONSE_MISS_COUNTER),
            };

            let json = JsonSnapshot {
                window: window_id,
                elapsed,
                target_qps: ratelimit.as_ref().map(|ratelimit| ratelimit.rate()),
                client: ClientStats {
                    connections,
                    requests,
                    responses,
                    response_latency: snapshot
                        .histogram_delta(RESPONSE_LATENCY_HISTOGRAM)
                        .map(rpcperf_dataspec::Histogram::from)
                        .unwrap_or_default(),
                },
                pubsub: PubsubStats {
                    publishers: Publishers {
                        current: PUBSUB_PUBLISHER_CURR.value(),
                    },
                    subscribers: Subscribers {
                        current: PUBSUB_SUBSCRIBER_CURR.value(),
                    },
                },
            };

            let _ = writer.write_all(
                serde_json::to_string(&json)
                    .expect("failed to serialize")
                    .as_bytes(),
            );
            let _ = writer.write_all(b"\n");

            window_id += 1;
        }
    }
}
