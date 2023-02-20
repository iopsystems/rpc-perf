// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

#[macro_use]
extern crate ringlog;

use crate::config::*;
use clocksource::{DateTime, SecondsFormat};
use core::sync::atomic::{AtomicBool, Ordering};
use ringlog::File;
use ringlog::Level;
use ringlog::LogBuilder;
use ringlog::MultiLogBuilder;
use ringlog::Output;
use ringlog::Record;
use ringlog::Stdout;

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

gauge!(CONNECT_CURR);
counter!(CONNECT_OK);
counter!(CONNECT_TIMEOUT);

pub fn default_format(
    w: &mut dyn std::io::Write,
    now: DateTime,
    record: &Record,
) -> Result<(), std::io::Error> {
    writeln!(
        w,
        "{} {} [rpc-perf] {}",
        now.to_rfc3339_opts(SecondsFormat::Millis, false),
        record.level(),
        record.args()
    )
}

fn main() {
    let config = Config::new("configs/redis.toml");

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
        Box::new(Stdout::new())
    };

    let level = config.debug().log_level();

    let debug_log = if level <= Level::Info {
        LogBuilder::new().format(default_format)
    } else {
        LogBuilder::new()
    }
    .output(debug_output)
    .log_queue_depth(config.debug().log_queue_depth())
    .single_message_size(config.debug().log_single_message_size())
    .build()
    .expect("failed to initialize debug log");

    let log = MultiLogBuilder::new()
        .level_filter(config.debug().log_level().to_level_filter())
        .default(debug_log)
        .build()
        .start();

    info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

    info!("exection model: async with tokio executor");
    let _ = execution::tokio::run(config, log);
}
