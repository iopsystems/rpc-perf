// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use crate::*;
use ahash::HashMap;
use ahash::HashMapExt;
use ratelimit::Ratelimiter;
use std::io::BufWriter;
use std::io::Write;
use std::sync::Arc;

use serde::Serialize;

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

pub fn log(config: &Config, traffic_ratelimit: Option<Arc<Ratelimiter>>) {
    let mut interval = config.general().interval().as_millis();
    let mut duration = config.general().duration().as_millis();

    let mut window_id = 0;

    let mut snapshot = Snapshot {
        prev: HashMap::new(),
    };

    let mut prev = Instant::now();

    let mut windows_under_target_rate = 0;
    let mut windows_over_p999_slo = 0;

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

            // output the client stats and retrieve the number of client
            // requests sent since last snapshot
            let client_ok = if client {
                client_stats(&mut snapshot, elapsed)
            } else {
                0
            };

            // output the pubsusb stats and retrieve the number of messages
            // published since last snapshot
            let publish_ok = if pubsub {
                pubsub_stats(&mut snapshot, elapsed)
            } else {
                0
            };

            // the total number of requests + publishes
            let total_ok = client_ok + publish_ok;

            // a check to determine if we're approximately hitting our target
            // ratelimit. If not, this will terminate the run.
            if let Some(rate) = traffic_ratelimit.as_ref().map(|v| v.rate()) {
                if total_ok as f64 / elapsed < 0.95 * rate as f64 {
                    windows_under_target_rate += 1;
                } else {
                    windows_under_target_rate = 0;
                }

                if windows_under_target_rate > 5 {
                    break;
                }
            }

            // a check to determine if we're achieving our p999 SLO (if set)
            if config.workload().p999_slo() > 0 {
                if let Ok(p999_latency) =
                    RESPONSE_LATENCY.percentile(0.999).map(|b| b.high() / 1000)
                {
                    if p999_latency > config.workload().p999_slo() {
                        windows_over_p999_slo += 1;
                    } else {
                        windows_over_p999_slo = 0;
                    }

                    if windows_over_p999_slo > 5 {
                        break;
                    }
                }
            }

            interval = config.general().interval().as_millis();
            window_id += 1;
        }
    }
}

/// Outputs client stats and returns the number of requests successfully sent
fn client_stats(snapshot: &mut Snapshot, elapsed: f64) -> u64 {
    let connect_ok = Stat::ConnectOk.delta(snapshot);
    let connect_ex = Stat::ConnectEx.delta(snapshot);
    let connect_timeout = Stat::ConnectTimeout.delta(snapshot);
    let connect_total = Stat::Connect.delta(snapshot);

    let request_reconnect = Stat::RequestReconnect.delta(snapshot);
    let request_ok = Stat::RequestOk.delta(snapshot);
    let request_unsupported = Stat::RequestUnsupported.delta(snapshot);
    let request_total = Stat::Request.delta(snapshot);

    let response_ok = Stat::ResponseOk.delta(snapshot);
    let response_ex = Stat::ResponseEx.delta(snapshot);
    let response_timeout = Stat::ResponseTimeout.delta(snapshot);
    let response_hit = Stat::ResponseHit.delta(snapshot);
    let response_miss = Stat::ResponseMiss.delta(snapshot);

    let connect_sr = 100.0 * connect_ok as f64 / connect_total as f64;

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
    for (label, percentile) in PERCENTILES {
        let value = RESPONSE_LATENCY
            .percentile(*percentile)
            .map(|b| format!("{}", b.high() / 1000))
            .unwrap_or_else(|_| "ERR".to_string());
        latencies.push_str(&format!(" {label}: {value}"))
    }

    output!("{latencies}");

    request_ok
}

/// Output pubsub metrics and return the number of successful publish operations
fn pubsub_stats(snapshot: &mut Snapshot, elapsed: f64) -> u64 {
    // publisher stats
    let pubsub_tx_ex = Stat::PubsubTxEx.delta(snapshot);
    let pubsub_tx_ok = Stat::PubsubTxOk.delta(snapshot);
    let pubsub_tx_timeout = Stat::PubsubTxTimeout.delta(snapshot);
    let pubsub_tx_total = Stat::PubsubTx.delta(snapshot);

    // subscriber stats
    let pubsub_rx_ok = Stat::PubsubRxOk.delta(snapshot);
    let pubsub_rx_ex = Stat::PubsubRxEx.delta(snapshot);
    let pubsub_rx_corrupt = Stat::PubsubRxCorrupt.delta(snapshot);
    let pubsub_rx_invalid = Stat::PubsubRxInvalid.delta(snapshot);
    let pubsub_rx_total = Stat::PubsubRx.delta(snapshot);

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
    for (label, percentile) in PERCENTILES {
        let value = PUBSUB_PUBLISH_LATENCY
            .percentile(*percentile)
            .map(|b| format!("{}", b.high() / 1000))
            .unwrap_or_else(|_| "ERR".to_string());
        latencies.push_str(&format!(" {label}: {value}"))
    }

    output!("{latencies}");

    let mut latencies = "Pubsub End-to-End Latency (us):".to_owned();
    for (label, percentile) in PERCENTILES {
        let value = PUBSUB_LATENCY
            .percentile(*percentile)
            .map(|b| format!("{}", b.high() / 1000))
            .unwrap_or_else(|_| "ERR".to_string());
        latencies.push_str(&format!(" {label}: {value}"))
    }

    output!("{latencies}");

    pubsub_tx_ok
}

#[derive(Serialize)]
struct Bucket {
    index: usize,
    low: u64,
    high: u64,
    count: u32,
}

#[derive(Serialize)]
struct Connections {
    /// number of current connections (gauge)
    current: i64,
    /// number of total connect attempts
    total: u64,
    /// number of connections established
    opened: u64,
    /// number of connect attempts that failed
    error: u64,
    /// number of connect attempts that hit timeout
    timeout: u64,
}

#[derive(Serialize, Copy, Clone)]
struct Requests {
    total: u64,
    ok: u64,
    reconnect: u64,
    unsupported: u64,
}

#[derive(Serialize)]
struct Responses {
    /// total number of responses
    total: u64,
    /// number of responses that were successful
    ok: u64,
    /// number of responses that were unsuccessful
    error: u64,
    /// number of responses that were missed due to timeout
    timeout: u64,
    /// number of read requests with a hit response
    hit: u64,
    /// number of read requests with a miss response
    miss: u64,
}

#[derive(Serialize)]
struct Client {
    connections: Connections,
    requests: Requests,
    responses: Responses,
    request_latency: Vec<Bucket>,
}

#[derive(Serialize)]
struct Pubsub {
    publishers: Publishers,
    subscribers: Subscribers,
}

#[derive(Serialize)]
struct Publishers {
    // current number of publishers
    current: i64,
}

#[derive(Serialize)]
struct Subscribers {
    // current number of subscribers
    current: i64,
}

#[derive(Serialize)]
struct JsonSnapshot {
    window: u64,
    elapsed: f64,
    client: Client,
    pubsub: Pubsub,
}

// gets the non-zero buckets for the most recent window in the heatmap
fn heatmap_to_buckets(heatmap: &Heatmap) -> Vec<Bucket> {
    if let Some(histogram) = heatmap.iter().nth(60).map(|w| w.histogram()) {
        (*histogram)
            .into_iter()
            .enumerate()
            // Only include buckets that actually contain values
            .filter(|(_index, bucket)| bucket.count() != 0)
            .map(|(index, bucket)| Bucket {
                index,
                low: bucket.low(),
                high: bucket.high(),
                count: bucket.count(),
            })
            .collect()
    } else {
        eprintln!("no histogram");
        vec![]
    }
}

pub fn json(config: Config, traffic_ratelimit: Option<Arc<Ratelimiter>>) {
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

    // let mut interval: u64 = 1000;
    // let mut duration = config.general().duration().as_millis();

    let mut window_id = 0;

    let mut snapshot = Snapshot {
        prev: HashMap::new(),
    };

    let mut windows_under_target_rate = 0;

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
                total: Stat::Connect.delta(&mut snapshot),
                opened: Stat::ConnectOk.delta(&mut snapshot),
                error: Stat::ConnectEx.delta(&mut snapshot),
                timeout: Stat::ConnectTimeout.delta(&mut snapshot),
            };

            let requests = Requests {
                total: Stat::Request.delta(&mut snapshot),
                ok: Stat::RequestOk.delta(&mut snapshot),
                reconnect: Stat::RequestReconnect.delta(&mut snapshot),
                unsupported: Stat::RequestUnsupported.delta(&mut snapshot),
            };

            let response_ok = Stat::ResponseOk.delta(&mut snapshot);
            let response_ex = Stat::ResponseEx.delta(&mut snapshot);
            let response_timeout = Stat::ResponseTimeout.delta(&mut snapshot);
            let response_total = response_ok + response_ex + response_timeout;

            let responses = Responses {
                total: response_total,
                ok: response_ok,
                error: response_ex,
                timeout: response_timeout,
                hit: 0,
                miss: 0,
            };

            let json = JsonSnapshot {
                window: window_id,
                elapsed,
                client: Client {
                    connections,
                    requests,
                    responses,
                    request_latency: heatmap_to_buckets(&REQUEST_LATENCY),
                },
                pubsub: Pubsub {
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

            if let Some(rate) = traffic_ratelimit.as_ref().map(|v| v.rate()) {
                if requests.ok as f64 / elapsed < 0.95 * rate as f64 {
                    windows_under_target_rate += 1;
                } else {
                    windows_under_target_rate = 0;
                }

                if windows_under_target_rate > 5 {
                    break;
                }
            }

            window_id += 1;
        }
    }
}
