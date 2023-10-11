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

    let mut snapshot = Snapshot::default();

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

/// Outputs client stats and returns the number of requests successfully sent
fn client_stats(snapshot: &mut Snapshot, elapsed: f64) -> u64 {
    let connect_ok = Counters::ConnectOk.delta(snapshot);
    let connect_ex = Counters::ConnectEx.delta(snapshot);
    let connect_timeout = Counters::ConnectTimeout.delta(snapshot);
    let connect_total = Counters::Connect.delta(snapshot);

    let request_reconnect = Counters::RequestReconnect.delta(snapshot);
    let request_ok = Counters::RequestOk.delta(snapshot);
    let request_unsupported = Counters::RequestUnsupported.delta(snapshot);
    let request_total = Counters::Request.delta(snapshot);

    let response_ok = Counters::ResponseOk.delta(snapshot);
    let response_ex = Counters::ResponseEx.delta(snapshot);
    let response_timeout = Counters::ResponseTimeout.delta(snapshot);
    let response_hit = Counters::ResponseHit.delta(snapshot);
    let response_miss = Counters::ResponseMiss.delta(snapshot);

    let connect_sr = 100.0 * connect_ok as f64 / connect_total as f64;

    let response_latency = Histograms::ResponseLatency.delta(snapshot);

    output!(
        "Client Connection: Open: {} Success Rate: {:.2} %",
        CONNECT_CURR.value(),
        connect_sr
    );
    output!(
        "Client Connection Rates (/s): Attempt: {:.2} Opened: {:.2} Errors: {:.2} Timeout: {:.2} Closed: {:.2}",
        connect_total as f64 / elapsed,
        connect_ok as f64 / elapsed,
        connect_ex as f64 / elapsed,
        connect_timeout as f64 / elapsed,
        request_reconnect as f64 / elapsed,
    );

    let request_sr = 100.0 * request_ok as f64 / request_total as f64;
    let request_ur = 100.0 * request_unsupported as f64 / request_total as f64;

    output!(
        "Client Request: Success: {:.2} % Unsupported: {:.2} %",
        request_sr,
        request_ur,
    );
    output!(
        "Client Request Rate (/s): Ok: {:.2} Unsupported: {:.2}",
        request_ok as f64 / elapsed,
        request_unsupported as f64 / elapsed,
    );

    let response_total = response_ok + response_ex + response_timeout;

    let response_sr = 100.0 * response_ok as f64 / response_total as f64;
    let response_to = 100.0 * response_timeout as f64 / response_total as f64;
    let response_hr = 100.0 * response_hit as f64 / (response_hit + response_miss) as f64;

    output!(
        "Client Response: Success: {:.2} % Timeout: {:.2} % Hit: {:.2} %",
        response_sr,
        response_to,
        response_hr,
    );
    output!(
        "Client Response Rate (/s): Ok: {:.2} Error: {:.2} Timeout: {:.2}",
        response_ok as f64 / elapsed,
        response_ex as f64 / elapsed,
        response_timeout as f64 / elapsed,
    );

    let mut latencies = "Client Response Latency (us):".to_owned();

    if let Some(snapshot) = response_latency {
        let p: Vec<f64> = PERCENTILES.iter().map(|(_, v)| *v).collect();

        if let Ok(result) = snapshot.percentiles(&p) {
            let percentiles: Vec<(&str, u64)> = result
                .iter()
                .zip(PERCENTILES.iter())
                .map(|((_, b), (l, _))| (*l, b.end()))
                .collect();

            for (label, value) in percentiles {
                latencies.push_str(&format!(" {label}: {value}"))
            }
        }
    }

    output!("{latencies}");

    request_ok
}

/// Output pubsub metrics and return the number of successful publish operations
fn pubsub_stats(snapshot: &mut Snapshot, elapsed: f64) -> u64 {
    // publisher stats
    let pubsub_tx_ex = Counters::PubsubTxEx.delta(snapshot);
    let pubsub_tx_ok = Counters::PubsubTxOk.delta(snapshot);
    let pubsub_tx_timeout = Counters::PubsubTxTimeout.delta(snapshot);
    let pubsub_tx_total = Counters::PubsubTx.delta(snapshot);

    let pubsub_publish_latency = Histograms::PubsubPublishLatency.delta(snapshot);

    // subscriber stats
    let pubsub_rx_ok = Counters::PubsubRxOk.delta(snapshot);
    let pubsub_rx_ex = Counters::PubsubRxEx.delta(snapshot);
    let pubsub_rx_corrupt = Counters::PubsubRxCorrupt.delta(snapshot);
    let pubsub_rx_invalid = Counters::PubsubRxInvalid.delta(snapshot);
    let pubsub_rx_total = Counters::PubsubRx.delta(snapshot);

    // end-to-end stats
    let pubsub_latency = Histograms::PubsubLatency.delta(snapshot);

    output!("Publishers: Current: {}", PUBSUB_PUBLISHER_CURR.value(),);

    let pubsub_tx_sr = 100.0 * pubsub_tx_ok as f64 / pubsub_tx_total as f64;
    let pubsub_tx_to = 100.0 * pubsub_tx_timeout as f64 / pubsub_tx_total as f64;
    output!(
        "Publisher Publish: Success: {:.2} % Timeout: {:.2} %",
        pubsub_tx_sr,
        pubsub_tx_to
    );

    output!(
        "Publisher Publish Rate (/s): Ok: {:.2} Error: {:.2} Timeout: {:.2}",
        pubsub_tx_ok as f64 / elapsed,
        pubsub_tx_ex as f64 / elapsed,
        pubsub_tx_timeout as f64 / elapsed,
    );

    output!("Subscribers: Current: {}", PUBSUB_SUBSCRIBER_CURR.value(),);

    let pubsub_rx_sr = 100.0 * pubsub_rx_ok as f64 / pubsub_rx_total as f64;
    let pubsub_rx_cr = 100.0 * pubsub_rx_corrupt as f64 / pubsub_rx_total as f64;
    output!(
        "Subscriber Receive: Success: {:.2} % Corrupted: {:.2} %",
        pubsub_rx_sr,
        pubsub_rx_cr
    );

    output!(
        "Subscriber Receive Rate (/s): Ok: {:.2} Error: {:.2} Corrupt: {:.2} Invalid: {:.2}",
        pubsub_rx_ok as f64 / elapsed,
        pubsub_rx_ex as f64 / elapsed,
        pubsub_rx_corrupt as f64 / elapsed,
        pubsub_rx_invalid as f64 / elapsed,
    );

    let mut latencies = "Pubsub Publish Latency (us):".to_owned();

    if let Some(snapshot) = pubsub_publish_latency {
        let p: Vec<f64> = PERCENTILES.iter().map(|(_, v)| *v).collect();

        if let Ok(result) = snapshot.percentiles(&p) {
            let percentiles: Vec<(&str, u64)> = result
                .iter()
                .zip(PERCENTILES.iter())
                .map(|((_, b), (l, _))| (*l, b.end()))
                .collect();

            for (label, value) in percentiles {
                latencies.push_str(&format!(" {label}: {value}"))
            }
        }
    }

    output!("{latencies}");

    let mut latencies = "Pubsub End-to-End Latency (us):".to_owned();

    if let Some(snapshot) = pubsub_latency {
        let p: Vec<f64> = PERCENTILES.iter().map(|(_, v)| *v).collect();

        if let Ok(result) = snapshot.percentiles(&p) {
            let percentiles: Vec<(&str, u64)> = result
                .iter()
                .zip(PERCENTILES.iter())
                .map(|((_, b), (l, _))| (*l, b.end()))
                .collect();

            for (label, value) in percentiles {
                latencies.push_str(&format!(" {label}: {value}"))
            }
        }
    }

    output!("{latencies}");

    pubsub_tx_ok
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

    let mut snapshot = Snapshot::default();

    while end > now {
        std::thread::sleep(Duration::from_millis(1));

        now = std::time::Instant::now();

        if next <= now {
            // let now = Instant::now();
            let elapsed = now.duration_since(prev).as_secs_f64();
            prev = now;
            next += Duration::from_secs(1);

            let connections = Connections {
                current: CONNECT_CURR.value(),
                total: Counters::Connect.delta(&mut snapshot),
                opened: Counters::ConnectOk.delta(&mut snapshot),
                error: Counters::ConnectEx.delta(&mut snapshot),
                timeout: Counters::ConnectTimeout.delta(&mut snapshot),
            };

            let requests = Requests {
                total: Counters::Request.delta(&mut snapshot),
                ok: Counters::RequestOk.delta(&mut snapshot),
                reconnect: Counters::RequestReconnect.delta(&mut snapshot),
                unsupported: Counters::RequestUnsupported.delta(&mut snapshot),
            };

            let response_ok = Counters::ResponseOk.delta(&mut snapshot);
            let response_ex = Counters::ResponseEx.delta(&mut snapshot);
            let response_timeout = Counters::ResponseTimeout.delta(&mut snapshot);
            let response_total = response_ok + response_ex + response_timeout;
            let response_hit = Counters::ResponseHit.delta(&mut snapshot);
            let response_miss = Counters::ResponseHit.delta(&mut snapshot);

            let responses = Responses {
                total: response_total,
                ok: response_ok,
                error: response_ex,
                timeout: response_timeout,
                hit: response_hit,
                miss: response_miss,
            };

            let json = JsonSnapshot {
                window: window_id,
                elapsed,
                target_qps: ratelimit.as_ref().map(|ratelimit| ratelimit.rate()),
                client: ClientStats {
                    connections,
                    requests,
                    responses,
                    response_latency: RESPONSE_LATENCY
                        .snapshot()
                        .map(|snapshot| rpcperf_dataspec::Histogram::from(&snapshot))
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
