// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

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

mod admin;
mod cli;
mod config;
mod drivers;
mod generators;
mod stats;
mod workload;

use config::*;
use stats::*;

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

fn main() {
    console_subscriber::init();

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

    // TODO: figure out what a reasonable size is here
    let (work_sender, work_receiver) = bounded(128);

    output!("Protocol: {:?}", config.general().protocol());

    debug!("Initializing traffic generator");
    let traffic_generator = TrafficGenerator::new(&config);

    // spawn the admin thread
    rt.spawn(admin::http(traffic_generator.ratelimiter()));

    debug!("Launching workload generation");
    // spawn the request generators on a blocking threads
    for _ in 0..config.workload().threads() {
        let work_sender = work_sender.clone();
        let traffic_generator = traffic_generator.clone();
        rt.spawn_blocking(move || requests(work_sender, traffic_generator));
    }

    let c = config.clone();
    rt.spawn_blocking(move || reconnect(work_sender, c));

    debug!("Launching workload drivers");
    // spawn the request drivers on their own runtime
    let mut request_rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(config.request().threads())
        .event_interval(31)
        .global_queue_interval(61)
        .build()
        .expect("failed to initialize tokio runtime");
    match config.general().protocol() {
        Protocol::Memcache => {
            drivers::memcache::launch_tasks(&mut request_rt, config.clone(), work_receiver)
        }
        Protocol::Momento => {
            drivers::momento::launch_tasks(&mut request_rt, config.clone(), work_receiver)
        }
        Protocol::Ping => {
            drivers::ping::launch_tasks(&mut request_rt, config.clone(), work_receiver)
        }
        Protocol::Resp => {
            drivers::resp::launch_tasks(&mut request_rt, config.clone(), work_receiver)
        }
    }

    // provide output on cli and block until run is over
    cli::output(&config);

    RUNNING.store(false, Ordering::Relaxed);

    std::thread::sleep(std::time::Duration::from_millis(100));
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
