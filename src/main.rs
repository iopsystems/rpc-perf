// SPDX-License-Identifier: (Apache-2.0)
// Copyright Authors of rpc-perf

use crate::clients::launch_clients;
use crate::pubsub::launch_publishers;
use crate::pubsub::launch_subscribers;
use crate::workload::launch_workload;
use metriken::Counter;
use metriken::Gauge;
use metriken::Heatmap;
use ringlog::*;
use std::collections::HashMap;
use warp::Filter;

use crate::workload::Generator;
use async_channel::{bounded, Sender};
use backtrace::Backtrace;
use clap::{Arg, Command};
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use ringlog::{File, Level, LogBuilder, MultiLogBuilder, Output, Stderr};
use tokio::runtime::Builder;
use tokio::time::sleep;

mod admin;
mod clients;
mod config;
mod net;
mod output;
mod pubsub;
mod stats;
mod workload;

use config::*;
use stats::*;

type Instant = clocksource::Instant<clocksource::Nanoseconds<u64>>;
type UnixInstant = clocksource::UnixInstant<clocksource::Nanoseconds<u64>>;

static RUNNING: AtomicBool = AtomicBool::new(true);

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
            clocksource::refresh_clock();
            sleep(Duration::from_millis(1)).await;
            let _ = log.flush();
        }
        let _ = log.flush();
    });

    // TODO: figure out what a reasonable size is here
    let (client_sender, client_receiver) = bounded(128);
    let (pubsub_sender, pubsub_receiver) = bounded(128);

    output!("Protocol: {:?}", config.general().protocol());

    debug!("Initializing workload generator");
    let workload_generator = Generator::new(&config);

    let workload_ratelimit = workload_generator.ratelimiter();

    let workload_components = workload_generator.components().to_owned();

    // spawn the admin thread
    rt.spawn(admin::http(config.clone(), workload_ratelimit.clone()));

    let workload_rt = launch_workload(workload_generator, &config, client_sender, pubsub_sender);

    let client_rt = launch_clients(&config, client_receiver);
    let publisher_rt = launch_publishers(&config, pubsub_receiver);
    let subscriber_rt = launch_subscribers(&config, workload_components);

    // launch json log output
    {
        let config = config.clone();
        let workload_ratelimit = workload_ratelimit.clone();
        rt.spawn_blocking(move || output::json(config, workload_ratelimit));
    }

    // provide output on cli and block until run is over
    output::log(&config, workload_ratelimit);

    // signal to other threads to shutdown
    RUNNING.store(false, Ordering::Relaxed);

    if let Some(client_rt) = client_rt {
        client_rt.shutdown_timeout(std::time::Duration::from_millis(100));
    }
    if let Some(publisher_rt) = publisher_rt {
        publisher_rt.shutdown_timeout(std::time::Duration::from_millis(100));
    }
    if let Some(subscriber_rt) = subscriber_rt {
        subscriber_rt.shutdown_timeout(std::time::Duration::from_millis(100));
    }
    workload_rt.shutdown_timeout(std::time::Duration::from_millis(100));

    // delay before exiting
    std::thread::sleep(std::time::Duration::from_millis(100));
}
