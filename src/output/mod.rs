// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use crate::*;

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

pub fn log(config: &Config) {
    let mut interval = config.general().interval().as_millis();
    let mut duration = config.general().duration().as_millis();

    let mut window_id = 0;

    let mut snapshot = Snapshot {
        prev: HashMap::new(),
    };

    let mut prev = Instant::now();

    while duration > 0 {
        std::thread::sleep(Duration::from_millis(10));

        interval = interval.saturating_sub(10);
        duration = duration.saturating_sub(10);

        if interval == 0 {
            let now = Instant::now();
            let elapsed = now.duration_since(prev).as_secs_f64();
            prev = now;

            let connect_ok = Stat::ConnectOk.delta(&mut snapshot);
            let connect_ex = Stat::ConnectEx.delta(&mut snapshot);
            let connect_timeout = Stat::ConnectTimeout.delta(&mut snapshot);
            let connect_total = Stat::Connect.delta(&mut snapshot);

            let request_reconnect = Stat::RequestReconnect.delta(&mut snapshot);
            let request_ok = Stat::RequestOk.delta(&mut snapshot);
            let request_unsupported = Stat::RequestUnsupported.delta(&mut snapshot);
            let request_total = Stat::Request.delta(&mut snapshot);

            let response_ok = Stat::ResponseOk.delta(&mut snapshot);
            let response_ex = Stat::ResponseEx.delta(&mut snapshot);
            let response_timeout = Stat::ResponseTimeout.delta(&mut snapshot);

            output!("-----");
            output!("Window: {}", window_id);

            let connect_sr = connect_ok as f64 / connect_total as f64;

            output!(
                "Connection: Open: {} Success Rate: {:.2} %",
                CONNECT_CURR.value(),
                connect_sr
            );
            output!(
                "Connection Rates (/s): Attempt: {:.2} Opened: {:.2} Errors: {:.2} Timeout: {:.2} Closed: {:.2}",
                connect_total as f64 / elapsed,
                connect_ok as f64 / elapsed,
                connect_ex as f64 / elapsed,
                connect_timeout as f64 / elapsed,
                request_reconnect as f64 / elapsed,
            );

            let request_sr = 100.0 * request_ok as f64 / request_total as f64;
            let request_ur = 100.0 * request_unsupported as f64 / request_total as f64;

            output!(
                "Request: Success: {:.2} % Unsupported: {:.2} %",
                request_sr,
                request_ur,
            );
            output!(
                "Request Rate (/s): Ok: {:.2} Unsupported: {:.2}",
                request_ok as f64 / elapsed,
                request_unsupported as f64 / elapsed,
            );

            let response_total = response_ok + response_ex + response_timeout;

            let response_sr = 100.0 * response_ok as f64 / response_total as f64;
            let response_to = 100.0 * response_timeout as f64 / response_total as f64;

            output!(
                "Response: Success: {:.2} % Timeout: {:.2} %",
                response_sr,
                response_to
            );
            output!(
                "Response Rate (/s): Ok: {:.2} Error: {:.2} Timeout: {:.2}",
                response_ok as f64 / elapsed,
                response_ex as f64 / elapsed,
                response_timeout as f64 / elapsed,
            );

            let mut latencies = "response latency (us):".to_owned();
            for (label, percentile) in PERCENTILES {
                let value = RESPONSE_LATENCY
                    .percentile(*percentile)
                    .map(|b| format!("{}", b.high() / 1000))
                    .unwrap_or_else(|_| "ERR".to_string());
                latencies.push_str(&format!(" {label}: {value}"))
            }

            output!("{latencies}");

            interval = config.general().interval().as_millis();
            window_id += 1;
        }
    }
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

#[derive(Serialize)]
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
struct JsonSnapshot {
    window: u64,
    elapsed: f64,
    connections: Connections,
    requests: Requests,
    responses: Responses,

    // connect: Vec<Bucket>,
    response_latency: Vec<Bucket>,
}

// gets the non-zero buckets for the most recent window in the heatmap
fn heatmap_to_buckets(heatmap: &Heatmap, window: usize) -> Vec<Bucket> {
    let idx = if window > 59 {
        1
    } else {
        60 - window
    };

    if let Some(histogram) = heatmap.iter().nth(idx).map(|w| w.histogram()) {
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

pub fn json(config: &Config) {
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

            let json =  JsonSnapshot {
                window: window_id,
                elapsed,
                connections,
                requests,
                responses,
                response_latency: heatmap_to_buckets(&RESPONSE_LATENCY, window_id as usize),
            };

            println!(
                "{}",
                serde_json::to_string(&json).expect("Failed to output to stdout")
            );

            window_id += 1;
        }
    }
}
