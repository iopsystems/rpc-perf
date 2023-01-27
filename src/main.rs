// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate ringlog;

use crate::config::Config;
use core::sync::atomic::{AtomicBool, Ordering};
use ringlog::MultiLogBuilder;
use ringlog::Stdout;
use ringlog::LogBuilder;
use ringlog::LevelFilter;

mod config;
mod execution;
mod workload;

use workload::stats::*;

type Instant = clocksource::Instant<clocksource::Nanoseconds<u64>>;

static RUNNING: AtomicBool = AtomicBool::new(true);

heatmap!(
    RESPONSE_LATENCY,
    1_000_000_000,
    "distribution of response latencies in nanoseconds"
);

fn main() {
    let config = Config::new(Some("configs/redis.toml"));

    let debug_log = LogBuilder::new()
        .output(Box::new(Stdout::new()))
        // .log_queue_depth(debug_config.log_queue_depth())
        // .single_message_size(debug_config.log_single_message_size())
        .build()
        .expect("failed to initialize debug log");

    let log = MultiLogBuilder::new()
        .level_filter(LevelFilter::Info)
        .default(debug_log)
        .build()
        .start();

    info!("rpc-perf alpha");



    info!("exection model: async with tokio executor");
    let _ = execution::tokio::run(config, log);
}
