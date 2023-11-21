use crate::clients::launch_clients;
use crate::pubsub::launch_pubsub;
use crate::workload::{launch_workload, Generator};
use async_channel::{bounded, Sender};
use backtrace::Backtrace;
use clap::{Arg, Command};
use core::sync::atomic::{AtomicBool, Ordering};
use metriken::{AtomicHistogram, Counter, Gauge};
use once_cell::sync::Lazy;
use ringlog::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::sync::RwLock;
use tokio::time::sleep;

mod admin;
mod clients;
mod config;
mod metrics;
mod net;
mod output;
mod pubsub;
mod workload;

use config::*;
use metrics::*;

static RUNNING: AtomicBool = AtomicBool::new(true);

static METRICS_SNAPSHOT: Lazy<Arc<RwLock<MetricsSnapshot>>> =
    Lazy::new(|| Arc::new(RwLock::new(Default::default())));

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

    // initialize async runtime for control plane
    let control_runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("failed to initialize tokio runtime");

    // spawn logging thread
    control_runtime.spawn(async move {
        while RUNNING.load(Ordering::Relaxed) {
            sleep(Duration::from_millis(1)).await;
            let _ = log.flush();
        }
        let _ = log.flush();
    });

    // spawn thread to maintain histogram snapshots
    {
        let interval = config.general().interval();
        control_runtime.spawn(async move {
            while RUNNING.load(Ordering::Relaxed) {
                // acquire a lock and update the snapshots
                {
                    let mut snapshots = METRICS_SNAPSHOT.write().await;
                    snapshots.update();
                }

                // delay until next update
                sleep(interval).await;
            }
        });
    }

    // TODO: figure out what a reasonable size is here
    let (client_sender, client_receiver) = bounded(128);
    let (pubsub_sender, pubsub_receiver) = bounded(128);

    output!("Protocol: {:?}", config.general().protocol());

    debug!("Initializing workload generator");
    let workload_generator = Generator::new(&config);

    let workload_ratelimit = workload_generator.ratelimiter();

    let workload_components = workload_generator.components().to_owned();

    // spawn the admin thread
    control_runtime.spawn(admin::http(config.clone(), workload_ratelimit.clone()));

    // launch json log output
    {
        let config = config.clone();
        control_runtime.spawn_blocking(move || output::json(config, workload_ratelimit.as_deref()));
    }

    // begin cli output
    {
        let config = config.clone();
        control_runtime.spawn_blocking(move || {
            // provide output on cli and block until run is over
            output::log(&config);
            // signal to other threads to shutdown
            RUNNING.store(false, Ordering::Relaxed);
        });
    }

    // start the workload generator(s)
    let workload_runtime =
        launch_workload(workload_generator, &config, client_sender, pubsub_sender);

    // start client(s)
    let client_runtime = launch_clients(&config, client_receiver);

    // start publisher(s) and subscriber(s)
    let mut pubsub_runtimes = launch_pubsub(&config, pubsub_receiver, &workload_components);

    while RUNNING.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_secs(1));
    }

    if let Some(client_runtime) = client_runtime {
        client_runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    }

    pubsub_runtimes.shutdown_timeout(std::time::Duration::from_millis(100));

    workload_runtime.shutdown_timeout(std::time::Duration::from_millis(100));

    // delay before exiting
    std::thread::sleep(std::time::Duration::from_millis(100));
}
