use crate::config::Config;
use std::path::PathBuf;
use clap::value_parser;
use crate::workload::{launch_workload, Generator, Ratelimit};
use async_channel::{bounded, Sender};
use backtrace::Backtrace;
use clap::Command;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
mod viewer;
mod workload;

use config::*;
use metrics::*;

static RUNNING: AtomicBool = AtomicBool::new(true);
static WAIT: AtomicUsize = AtomicUsize::new(0);

static METRICS_SNAPSHOT: Lazy<Arc<RwLock<MetricsSnapshot>>> =
    Lazy::new(|| Arc::new(RwLock::new(Default::default())));

// queue depth per client thread
static QUEUE_DEPTH: usize = 64;

fn main() {
    // custom panic hook to terminate whole process after unwinding
    std::panic::set_hook(Box::new(|s| {
        eprintln!("{s}");
        eprintln!("{:?}", Backtrace::new());
        std::process::exit(101);
    }));

    #[allow(clippy::expect_used)]
    tokio_rustls::rustls::crypto::CryptoProvider::install_default(
        tokio_rustls::rustls::crypto::aws_lc_rs::default_provider(),
    )
    .expect("rustls _why_");

    // parse command line options
    let cli = Command::new(env!("CARGO_BIN_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .long_about("Rezolus provides high-resolution systems performance telemetry.")
        .subcommand_negates_reqs(true)
        .arg(
            clap::Arg::new("CONFIG")
                .help("Configuration file")
                .value_parser(value_parser!(PathBuf))
                .action(clap::ArgAction::Set)
                .required(true)
                .index(1),
        )
        .subcommand(viewer::command())
        .get_matches();

    match cli.subcommand() {
        None => {
            let config: PathBuf = cli.get_one::<PathBuf>("CONFIG").unwrap().to_path_buf();
            let config = Config::new(&config);

            run(config)
        }
        Some(("view", args)) => {
            let config = viewer::Config::try_from(args.clone()).expect("failed to configure");

            viewer::run(config)
        }
        _ => {
            unreachable!()
        }
    }
}

fn run(config: Config) {
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

    let (client_sender, client_receiver) = bounded(
        config
            .client()
            .map(|c| c.threads() * QUEUE_DEPTH)
            .unwrap_or(1),
    );
    let (pubsub_sender, pubsub_receiver) = bounded(
        config
            .pubsub()
            .map(|c| c.publisher_threads() * QUEUE_DEPTH)
            .unwrap_or(1),
    );
    let (store_sender, store_receiver) = bounded(
        config
            .storage()
            .map(|c| c.threads() * QUEUE_DEPTH)
            .unwrap_or(1),
    );
    let (leaderboard_sender, leaderboard_receiver) = bounded(
        config
            .leaderboard()
            .map(|c| c.threads() * QUEUE_DEPTH)
            .unwrap_or(1),
    );
    let (oltp_sender, oltp_receiver) = bounded(
        config
            .oltp()
            .map(|c| c.threads() * QUEUE_DEPTH)
            .unwrap_or(1),
    );

    output!("Protocol: {:?}", config.general().protocol());

    debug!("Initializing workload generator");
    let workload_generator = Generator::new(&config);

    let workload_ratelimit = workload_generator.ratelimiter();

    let workload_components = workload_generator.components().to_owned();

    // spawn the admin thread
    control_runtime.spawn(admin::http(config.clone(), workload_ratelimit.clone()));

    // launch metrics file output
    control_runtime.spawn(output::metrics(config.clone()));

    // begin cli output
    control_runtime.spawn(output::log(config.clone()));

    debug!("Running workload generator");
    // start the workload generator(s)
    let workload_runtime = launch_workload(
        workload_generator,
        &config,
        client_sender,
        pubsub_sender,
        store_sender,
        leaderboard_sender,
        oltp_sender,
    );

    debug!("Starting clients");
    // start client(s)
    let client_runtime = clients::cache::launch(&config, client_receiver);

    // start store client(s)
    let store_runtime = clients::store::launch(&config, store_receiver);

    // start leaderboard client(s)
    let leaderboard_runtime = clients::leaderboard::launch(&config, leaderboard_receiver);

    // start OLTP clients
    let oltp_runtime = clients::oltp::launch(&config, oltp_receiver);

    // start publisher(s) and subscriber(s)
    let mut pubsub_runtimes =
        clients::pubsub::launch(&config, pubsub_receiver, &workload_components);

    // start ratelimit controller thread if a dynamic ratelimit is configured
    {
        if let Some(mut ratelimit_controller) = Ratelimit::new(&config) {
            control_runtime.spawn(async move {
                while RUNNING.load(Ordering::Relaxed) {
                    let _ = admin::handlers::update_ratelimit(
                        ratelimit_controller.next_ratelimit(),
                        workload_ratelimit.clone(),
                    )
                    .await;
                    // delay until next step function
                    sleep(ratelimit_controller.interval()).await;
                }
            });
        }
    }

    debug!("Waiting for test to complete");
    while RUNNING.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_secs(1));
    }

    // shutdown thread pools

    if let Some(client_runtime) = client_runtime {
        client_runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    }

    if let Some(store_runtime) = store_runtime {
        store_runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    }

    if let Some(leaderboard_runtime) = leaderboard_runtime {
        leaderboard_runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    }

    if let Some(rt) = oltp_runtime {
        rt.shutdown_timeout(std::time::Duration::from_millis(100));
    }

    pubsub_runtimes.shutdown_timeout(std::time::Duration::from_millis(100));

    workload_runtime.shutdown_timeout(std::time::Duration::from_millis(100));

    // delay before exiting

    while WAIT.load(Ordering::Relaxed) > 0 {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    std::thread::sleep(std::time::Duration::from_millis(100));
}
