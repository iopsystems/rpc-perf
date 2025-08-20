use async_channel::Sender;
use bytes::{Bytes, BytesMut};
use momento::IntoBytes;
use rand::{RngCore, SeedableRng};
use rand_xoshiro::Xoshiro512PlusPlus;
use ringlog::info;
use std::{
    io::{BufRead, BufReader},
    str::FromStr,
    sync::atomic::Ordering,
    time::Duration,
};
use tokio::runtime::{Builder, Runtime};

use crate::{
    config::{Config, Replay},
    metrics::REQUEST_DROPPED,
    replay::{
        parser::CommandLogLine,
        replay_speed::{RateController, SpeedController, TimingController},
    },
    workload::{
        client::{Delete, Get, Set},
        ClientRequest, ClientWorkItemKind,
    },
    RUNNING,
};

pub struct ReplayEngine {
    command_log_path: String,
    timing_controller: TimingController,
    rng: Xoshiro512PlusPlus,
}

impl ReplayEngine {
    pub fn new(replay: &Replay) -> Self {
        let timing_controller = match (replay.speed(), replay.ratelimit()) {
            (Some(speed), None) => TimingController::Speed(SpeedController::new(speed)),
            (None, Some(ratelimit)) => TimingController::Rate(RateController::new(ratelimit)),
            (Some(_), Some(_)) => {
                eprintln!("speed and ratelimit cannot be specified at the same time");
                std::process::exit(1);
            }
            (None, None) => {
                eprintln!("neither speed or ratelimit are specified, defaulting to realtime speed");
                TimingController::Speed(SpeedController::new(1.0))
            }
        };
        Self {
            command_log_path: replay.command_log().clone(),
            timing_controller,
            rng: Xoshiro512PlusPlus::seed_from_u64(0),
        }
    }

    pub async fn generate(&mut self, replay_sender: &Sender<ClientWorkItemKind<ClientRequest>>) {
        let command_log =
            std::fs::File::open(&self.command_log_path).expect("failed to open command log file");
        let reader = BufReader::new(command_log);
        info!("replaying from file: {}", self.command_log_path);

        let mut lines_iterator = reader.lines().map_while(Result::ok);
        if let Some(first_line) = lines_iterator.next() {
            // Read first line to get initial timestamp first
            let first_command = CommandLogLine::from_str(&first_line).unwrap();
            let first_timestamp = first_command.timestamp();

            // Execute command in first line
            if replay_sender
                .send(self.generate_cache_request(&first_command))
                .await
                .is_err()
            {
                eprintln!("Failed to send first command");
                std::process::exit(1);
            }

            for line in lines_iterator {
                let command: CommandLogLine = CommandLogLine::from_str(&line).unwrap();
                let delay_time = command.timestamp() - first_timestamp;
                self.timing_controller
                    .delay(delay_time.num_milliseconds() as u64);
                if replay_sender
                    .send(self.generate_cache_request(&command))
                    .await
                    .is_err()
                {
                    REQUEST_DROPPED.increment();
                }
            }
        } else {
            eprintln!("No lines in command log file");
            std::process::exit(1);
        }

        info!("replay workload complete, ending replay");
        RUNNING.store(false, Ordering::Relaxed);
    }

    fn generate_cache_request(
        &mut self,
        command_log_line: &CommandLogLine,
    ) -> ClientWorkItemKind<ClientRequest> {
        let command = match command_log_line.operation() {
            "get" => ClientRequest::Get(Get {
                key: command_log_line.key().into_bytes().into(),
            }),
            "delete" => ClientRequest::Delete(Delete {
                key: command_log_line.key().into_bytes().into(),
            }),
            "set" => {
                // generate a random value of same size as current command
                let len = command_log_line.value_size().unwrap() as usize;
                ClientRequest::Set(Set {
                    key: command_log_line.key().into_bytes().into(),
                    value: self.gen_value(len),
                    ttl: command_log_line.ttl().map(Duration::from_secs),
                })
            }
            _ => {
                eprintln!("unimplemented operation: {}", command_log_line.operation());
                return ClientWorkItemKind::Reconnect;
            }
        };

        ClientWorkItemKind::Request {
            request: command,
            sequence: 0,
        }
    }

    fn gen_value(&mut self, len: usize) -> Bytes {
        let mut buf = BytesMut::zeroed(len);
        self.rng.fill_bytes(&mut buf[0..len]);
        buf.into_bytes().into()
    }
}

pub fn launch_replay_workload(
    config: Config,
    replay_sender: Sender<ClientWorkItemKind<ClientRequest>>,
) -> Runtime {
    // spawn the request drivers on their own runtime
    let rt = Builder::new_multi_thread()
        .enable_all()
        .thread_name("rpc-perf-replay")
        .worker_threads(config.workload().threads())
        .build()
        .expect("failed to initialize tokio runtime");

    // spawn the request generators on a blocking threads
    // Note: each thread will replay the same command log
    for _ in 0..config.workload().threads() {
        let replay_sender = replay_sender.clone();

        // ReplayEngine does not implement Clone due to the TimingController,
        // so we need to create a new instance for each thread.
        let mut replay_engine = ReplayEngine::new(config.replay().unwrap());
        rt.spawn(async move {
            replay_engine.generate(&replay_sender).await;
        });
    }
    rt
}
