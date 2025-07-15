use async_channel::Sender;
use momento::IntoBytes;
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
    replay::parser::CommandLogLine,
    workload::{
        client::{Delete, Get, Set},
        ClientRequest, ClientWorkItemKind,
    },
    RUNNING,
};

#[derive(Clone)]
pub struct ReplayEngine {
    // TODO: timing controllers: a multiple of realtime or a specific requests per second rate
    command_log_path: String,
}

impl ReplayEngine {
    pub fn new(replay: &Replay) -> Self {
        Self {
            command_log_path: replay.command_log().clone(),
        }
    }

    pub async fn generate(&self, replay_sender: &Sender<ClientWorkItemKind<ClientRequest>>) {
        let command_log =
            std::fs::File::open(&self.command_log_path).expect("failed to open command log file");
        let reader = BufReader::new(command_log);

        let mut lines_iterator = reader.lines().map_while(Result::ok);
        if let Some(first_line) = lines_iterator.next() {
            // Read first line to get initial timestamp first
            let first_command = CommandLogLine::from_str(&first_line).unwrap();
            let mut prev_timestamp = first_command.timestamp();

            // Execute command in first line
            if replay_sender
                .send(self.generate_cache_request(first_command))
                .await
                .is_err()
            {
                eprintln!("Failed to send first command");
                std::process::exit(1);
            }

            for line in lines_iterator {
                let command = CommandLogLine::from_str(&line).unwrap();

                let delay_time = command.timestamp() - prev_timestamp;
                std::thread::sleep(Duration::from_millis(
                    delay_time.abs().num_milliseconds() as u64
                ));

                prev_timestamp = command.timestamp();

                if replay_sender
                    .send(self.generate_cache_request(command))
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
        &self,
        command_log_line: CommandLogLine,
    ) -> ClientWorkItemKind<ClientRequest> {
        let command = match command_log_line.operation() {
            "get" => ClientRequest::Get(Get {
                key: command_log_line.key().into_bytes().into(),
            }),
            "delete" => ClientRequest::Delete(Delete {
                key: command_log_line.key().into_bytes().into(),
            }),
            "set" => {
                // generate string of 'X's to set value of same size as current command
                let value =
                    String::from_utf8(vec![b'X'; command_log_line.value_size().unwrap() as usize])
                        .unwrap();
                ClientRequest::Set(Set {
                    key: command_log_line.key().into_bytes().into(),
                    value: value.into_bytes().into(),
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
}

pub fn launch_replay_workload(
    replay_engine: ReplayEngine,
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
        let replay_engine = replay_engine.clone();
        rt.spawn(async move {
            replay_engine.generate(&replay_sender).await;
        });
    }
    rt
}
