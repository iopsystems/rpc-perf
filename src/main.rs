// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use metriken::Counter;
use metriken::Gauge;
use metriken::Heatmap;
use std::collections::HashMap;
use warp::Filter;
#[macro_use]
extern crate ringlog;

use crate::generators::reconnect;
use crate::generators::requests;
use crate::generators::TrafficGenerator;
use async_channel::{bounded, Sender};
use backtrace::Backtrace;
use clap::{Arg, Command};
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use ringlog::{File, Level, LogBuilder, MultiLogBuilder, Output, Stderr};
use tokio::runtime::Builder;
use tokio::time::sleep;

mod config;
mod drivers;
mod generators;
// mod execution;
mod workload;

use config::*;
use workload::stats::*;

pub static PERCENTILES: &[(&str, f64)] = &[
    ("p25", 25.0),
    ("p50", 50.0),
    ("p75", 75.0),
    ("p90", 90.0),
    ("p99", 99.0),
    ("p999", 99.9),
    ("p9999", 99.99),
];

type Instant = clocksource::Instant<clocksource::Nanoseconds<u64>>;

static RUNNING: AtomicBool = AtomicBool::new(true);

heatmap!(
    RESPONSE_LATENCY,
    1_000_000_000,
    "distribution of response latencies in nanoseconds"
);

gauge!(CONNECT_CURR);
counter!(CONNECT_OK);
counter!(CONNECT_TIMEOUT);

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

fn main() {
    // custom panic hook to terminate whole process after unwinding
    std::panic::set_hook(Box::new(|s| {
        eprintln!("{s}");
        eprintln!("{:?}", Backtrace::new());
        std::process::exit(101);
    }));

    // parse command line options
    let matches = Command::new(env!("CARGO_BIN_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .long_about(
            "A benchmarking, load generation, and traffic replay tool for RPC \
            services.",
        )
        .arg(
            Arg::new("CONFIG")
                .help("Server configuration file")
                .action(clap::ArgAction::Set)
                .index(1),
        )
        .get_matches();

    // load config from file
    let config = if let Some(file) = matches.get_one::<String>("CONFIG") {
        Config::new(file)
    } else {
        eprintln!("configuration file not provided");
        std::process::exit(1);
    };

    // configure debug log
    let debug_output: Box<dyn Output> = if let Some(file) = config.debug().log_file() {
        let backup = config
            .debug()
            .log_backup()
            .unwrap_or(format!("{}.old", file));
        Box::new(
            File::new(&file, &backup, config.debug().log_max_size())
                .expect("failed to open debug log file"),
        )
    } else {
        // by default, log to stderr
        Box::new(Stderr::new())
    };

    let level = config.debug().log_level();

    let debug_log = if level <= Level::Info {
        LogBuilder::new().format(ringlog::default_format)
    } else {
        LogBuilder::new()
    }
    .output(debug_output)
    .log_queue_depth(config.debug().log_queue_depth())
    .single_message_size(config.debug().log_single_message_size())
    .build()
    .expect("failed to initialize debug log");

    let mut log = MultiLogBuilder::new()
        .level_filter(config.debug().log_level().to_level_filter())
        .default(debug_log)
        .build()
        .start();

    output!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    // initialize async runtime
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to initialize tokio runtime");

    // spawn logging thread
    rt.spawn(async move {
        while RUNNING.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(50)).await;
            let _ = log.flush();
        }
        let _ = log.flush();
    });

    // spawn the admin thread
    rt.spawn(admin_http());

    // TODO: figure out what a reasonable size is here
    let (work_sender, work_receiver) = bounded(1_000_000);

    output!("Protocol: {:?}", config.general().protocol());

    debug!("Initializing traffic generator");
    let traffic_generator = TrafficGenerator::new(&config);

    debug!("Launching workload generation");
    // spawn the request generators on a blocking threads
    for _ in 0..config.workload().threads() {
        let work_sender = work_sender.clone();
        let traffic_generator = traffic_generator.clone();
        rt.spawn_blocking(move || requests(work_sender, traffic_generator));
    }

    rt.spawn(reconnect(work_sender, config.clone()));

    debug!("Launching workload drivers");
    // spawn the workload drivers on their own runtime
    let mut worker_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.general().threads())
        .build()
        .expect("failed to initialize tokio runtime");
    match config.general().protocol() {
        Protocol::Memcache => {
            drivers::memcache::launch_tasks(&mut worker_rt, config.clone(), work_receiver)
        }
        Protocol::Momento => drivers::momento::launch_tasks(&mut worker_rt, config.clone(), work_receiver),
        Protocol::Ping => drivers::ping::launch_tasks(&mut worker_rt, config.clone(), work_receiver),
        Protocol::Resp => drivers::resp::launch_tasks(&mut worker_rt, config.clone(), work_receiver),
    }

    let mut interval = config.general().interval().as_millis();
    let mut duration = config.general().duration().as_millis();

    let mut window_id = 0;

    let mut snapshot = Snapshot {
        prev: HashMap::new(),
    };

    let mut prev = Instant::now();

    while duration > 0 {
        rt.block_on(async {
            sleep(Duration::from_millis(10)).await;
        });

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

    RUNNING.store(false, Ordering::Relaxed);
}

struct Snapshot {
    prev: HashMap<Stat, u64>,
}

#[derive(Eq, Hash, PartialEq, Copy, Clone)]
enum Stat {
    Connect,
    ConnectOk,
    ConnectEx,
    ConnectTimeout,
    Request,
    RequestOk,
    RequestReconnect,
    RequestUnsupported,
    ResponseOk,
    ResponseEx,
    ResponseTimeout,
}

impl Stat {
    pub fn metric(&self) -> &metriken::Counter {
        match self {
            Self::Connect => &CONNECT,
            Self::ConnectOk => &CONNECT_OK,
            Self::ConnectEx => &CONNECT_EX,
            Self::ConnectTimeout => &CONNECT_TIMEOUT,
            Self::Request => &REQUEST,
            Self::RequestOk => &REQUEST_OK,
            Self::RequestReconnect => &REQUEST_RECONNECT,
            Self::RequestUnsupported => &REQUEST_UNSUPPORTED,
            Self::ResponseEx => &RESPONSE_EX,
            Self::ResponseOk => &RESPONSE_OK,
            Self::ResponseTimeout => &RESPONSE_TIMEOUT,
        }
    }

    pub fn delta(&self, snapshot: &mut Snapshot) -> u64 {
        let curr = self.metric().value();
        let prev = snapshot.prev.insert(*self, curr).unwrap_or(0);
        curr.wrapping_sub(prev)
    }
}

// fn verb_stats(verb: Verb) {
//     match verb {
//         Verb::Get => {
//             let get_total = GET.reset() as f64;
//             let get_ex = GET_EX.reset() as f64;
//             let get_hit = GET_KEY_HIT.reset() as f64;
//             let get_miss = GET_KEY_MISS.reset() as f64;
//             let get_hr = 100.0 * get_hit / (get_hit + get_miss);
//             let get_sr = 100.0 - (100.0 * get_ex / get_total);
//             output!(
//                 "\tGet: rate (/s): {:.2} hit rate(%): {:.2} success rate(%): {:.2}",
//                 get_total / window as f64,
//                 get_hr,
//                 get_sr,
//             );
//         }
//         Verb::Set => {
//             let set_total = SET.reset() as f64;
//             let set_stored = SET_STORED.reset() as f64;
//             let set_sr = (set_stored / set_total) * 100.0;
//             output!(
//                 "\tSet: rate (/s): {:.2} success rate(%): {:.2}",
//                 set_total / window as f64,
//                 set_sr,
//             );
//         }
//         Verb::SortedSetAdd => {
//             let total = SORTED_SET_ADD.reset() as f64;
//             let set_stored = SET_STORED.reset() as f64;
//             let set_sr = (set_stored / set_total) * 100.0;
//             output!(
//                 "\tSet: rate (/s): {:.2} success rate(%): {:.2}",
//                 set_total / window as f64,
//                 set_sr,
//             );
//         }
//     }
// }

async fn admin_http() {
    let root = warp::path::end().map(|| "rpc-perf");

    let vars = warp::path("vars").map(human_stats);

    let metrics = warp::path("metrics").map(prometheus_stats);

    let routes = warp::get().and(root.or(vars).or(metrics));

    warp::serve(routes).run(([0, 0, 0, 0], 9091)).await;
}

fn prometheus_stats() -> String {
    let mut data = Vec::new();

    for metric in &metriken::metrics() {
        let any = match metric.as_any() {
            Some(any) => any,
            None => {
                continue;
            }
        };

        if metric.name().starts_with("log_") {
            continue;
        }

        if let Some(counter) = any.downcast_ref::<Counter>() {
            data.push(format!(
                "# TYPE {} counter\n{} {}",
                metric.name(),
                metric.name(),
                counter.value()
            ));
        } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
            data.push(format!(
                "# TYPE {} gauge\n{} {}",
                metric.name(),
                metric.name(),
                gauge.value()
            ));
        } else if let Some(heatmap) = any.downcast_ref::<Heatmap>() {
            for (_label, percentile) in PERCENTILES {
                let value = heatmap
                    .percentile(*percentile)
                    .map(|b| b.high())
                    .unwrap_or(0);
                data.push(format!(
                    "# TYPE {} gauge\n{}{{percentile=\"{:02}\"}} {}",
                    metric.name(),
                    metric.name(),
                    percentile,
                    value
                ));
            }
        }
    }

    data.sort();
    let mut content = data.join("\n");
    content += "\n";
    let parts: Vec<&str> = content.split('/').collect();
    parts.join("_")
}

fn human_stats() -> String {
    let mut data = Vec::new();

    for metric in &metriken::metrics() {
        let any = match metric.as_any() {
            Some(any) => any,
            None => {
                continue;
            }
        };

        if metric.name().starts_with("log_") {
            continue;
        }

        if let Some(counter) = any.downcast_ref::<Counter>() {
            data.push(format!("{}: {}", metric.name(), counter.value()));
        } else if let Some(gauge) = any.downcast_ref::<Gauge>() {
            data.push(format!("{}: {}", metric.name(), gauge.value()));
        } else if let Some(heatmap) = any.downcast_ref::<Heatmap>() {
            for (label, p) in PERCENTILES {
                let percentile = heatmap.percentile(*p).map(|b| b.high()).unwrap_or(0);
                data.push(format!("{}/{}: {}", metric.name(), label, percentile));
            }
        }
    }

    data.sort();
    let mut content = data.join("\n");
    content += "\n";
    content
}
