#[macro_use]
extern crate metriken;

#[macro_use]
extern crate ringlog;

use ringlog::MultiLogBuilder;
use ringlog::Stdout;
use ringlog::LogBuilder;
use ringlog::LevelFilter;

mod execution;

type Instant = clocksource::Instant<clocksource::Nanoseconds<u64>>;

heatmap!(
    RESPONSE_LATENCY,
    1_000_000_000,
    "distribution of response latencies in nanoseconds"
);

fn main() {
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
    let _ = execution::tokio::run(log);
}
