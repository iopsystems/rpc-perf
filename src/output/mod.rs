use crate::*;
use ahash::{HashMap, HashMapExt};
use ratelimit::Ratelimiter;
use serde::Serialize;
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

    let mut snapshot = Snapshot {
        prev: HashMap::new(),
    };

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
    let connect_ok = Metrics::ConnectOk.delta(snapshot);
    let connect_ex = Metrics::ConnectEx.delta(snapshot);
    let connect_timeout = Metrics::ConnectTimeout.delta(snapshot);
    let connect_total = Metrics::Connect.delta(snapshot);

    let request_reconnect = Metrics::RequestReconnect.delta(snapshot);
    let request_ok = Metrics::RequestOk.delta(snapshot);
    let request_unsupported = Metrics::RequestUnsupported.delta(snapshot);
    let request_total = Metrics::Request.delta(snapshot);

    let response_ok = Metrics::ResponseOk.delta(snapshot);
    let response_ex = Metrics::ResponseEx.delta(snapshot);
    let response_timeout = Metrics::ResponseTimeout.delta(snapshot);
    let response_hit = Metrics::ResponseHit.delta(snapshot);
    let response_miss = Metrics::ResponseMiss.delta(snapshot);

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
        let value = match RESPONSE_LATENCY.percentile(*percentile) {
            Some(Ok(b)) => format!("{}", b.high() / 1000),
            _ => "ERR".to_string(),
        };
        latencies.push_str(&format!(" {label}: {value}"))
    }

    output!("{latencies}");

    request_ok
}

/// Output pubsub metrics and return the number of successful publish operations
fn pubsub_stats(snapshot: &mut Snapshot, elapsed: f64) -> u64 {
    // publisher stats
    let pubsub_tx_ex = Metrics::PubsubTxEx.delta(snapshot);
    let pubsub_tx_ok = Metrics::PubsubTxOk.delta(snapshot);
    let pubsub_tx_timeout = Metrics::PubsubTxTimeout.delta(snapshot);
    let pubsub_tx_total = Metrics::PubsubTx.delta(snapshot);

    // subscriber stats
    let pubsub_rx_ok = Metrics::PubsubRxOk.delta(snapshot);
    let pubsub_rx_ex = Metrics::PubsubRxEx.delta(snapshot);
    let pubsub_rx_corrupt = Metrics::PubsubRxCorrupt.delta(snapshot);
    let pubsub_rx_invalid = Metrics::PubsubRxInvalid.delta(snapshot);
    let pubsub_rx_total = Metrics::PubsubRx.delta(snapshot);

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
        let value = match RESPONSE_LATENCY.percentile(*percentile) {
            Some(Ok(b)) => format!("{}", b.high() / 1000),
            _ => "ERR".to_string(),
        };
        latencies.push_str(&format!(" {label}: {value}"))
    }

    output!("{latencies}");

    let mut latencies = "Pubsub End-to-End Latency (us):".to_owned();
    for (label, percentile) in PERCENTILES {
        let value = match RESPONSE_LATENCY.percentile(*percentile) {
            Some(Ok(b)) => format!("{}", b.high() / 1000),
            _ => "ERR".to_string(),
        };
        latencies.push_str(&format!(" {label}: {value}"))
    }

    output!("{latencies}");

    pubsub_tx_ok
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
/// Sparse histogram in column-order, i.e., storing a single vector
/// per-field of non-zero buckets. Assuming index[0] = n,
/// (index[0], count[0]) corresponds to the nth bucket.
struct RequestLatencies {
    /// parameters representing the resolution and the range of
    /// the histogram tracking request latencies
    m: u32,
    r: u32,
    n: u32,
    /// indices for the non-zero buckets in the histogram
    index: Vec<usize>,
    /// histogram bucket counts corresponding to the indices
    count: Vec<u32>,
}

#[derive(Serialize)]
struct Client {
    connections: Connections,
    requests: Requests,
    responses: Responses,
    request_latency: RequestLatencies,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    target_qps: Option<f64>,
    client: Client,
    pubsub: Pubsub,
}

// gets the non-zero buckets for the most recent window in the heatmap
fn heatmap_to_buckets(heatmap: &Heatmap) -> RequestLatencies {
    // XXX: The heatmap corrects for wraparound and fixes indices once
    // the heatmap is full so this returns the histogram for the last
    // completed epoch, assuming a heatmap with a total of 60 valid
    // histograms. However, this only kicks in after the entire histogram
    // has been populated, so for the first minute, no histograms
    // are returned (the histogram at offset 59 is still invalid).
    if let Some(Some(histogram)) = heatmap.iter().map(|mut i| i.nth(59)) {
        let p = histogram.parameters();
        let mut index = Vec::new();
        let mut count = Vec::new();

        for (i, bucket) in histogram
            .into_iter()
            .enumerate()
            .filter(|(_i, bucket)| bucket.count() != 0)
        {
            index.push(i);
            count.push(bucket.count());
        }

        RequestLatencies {
            m: p.0,
            r: p.1,
            n: p.2,
            index,
            count,
        }
    } else {
        trace!("no histogram");
        RequestLatencies {
            m: 0,
            r: 0,
            n: 0,
            index: vec![],
            count: vec![],
        }
    }
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

    let mut snapshot = Snapshot {
        prev: HashMap::new(),
    };

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
                total: Metrics::Connect.delta(&mut snapshot),
                opened: Metrics::ConnectOk.delta(&mut snapshot),
                error: Metrics::ConnectEx.delta(&mut snapshot),
                timeout: Metrics::ConnectTimeout.delta(&mut snapshot),
            };

            let requests = Requests {
                total: Metrics::Request.delta(&mut snapshot),
                ok: Metrics::RequestOk.delta(&mut snapshot),
                reconnect: Metrics::RequestReconnect.delta(&mut snapshot),
                unsupported: Metrics::RequestUnsupported.delta(&mut snapshot),
            };

            let response_ok = Metrics::ResponseOk.delta(&mut snapshot);
            let response_ex = Metrics::ResponseEx.delta(&mut snapshot);
            let response_timeout = Metrics::ResponseTimeout.delta(&mut snapshot);
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
                target_qps: ratelimit.as_ref().map(|ratelimit| ratelimit.rate()),
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

            window_id += 1;
        }
    }
}
