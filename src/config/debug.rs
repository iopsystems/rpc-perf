pub const B: usize = 1;
pub const KB: usize = 1024 * B;
pub const MB: usize = 1024 * KB;
pub const GB: usize = 1024 * MB;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[serde(deny_unknown_fields)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub fn to_level_filter(self) -> tracing_subscriber::filter::LevelFilter {
        match self {
            LogLevel::Error => tracing_subscriber::filter::LevelFilter::ERROR,
            LogLevel::Warn => tracing_subscriber::filter::LevelFilter::WARN,
            LogLevel::Info => tracing_subscriber::filter::LevelFilter::INFO,
            LogLevel::Debug => tracing_subscriber::filter::LevelFilter::DEBUG,
            LogLevel::Trace => tracing_subscriber::filter::LevelFilter::TRACE,
        }
    }
}

// constants to define default values
const LOG_LEVEL: LogLevel = LogLevel::Info;
const LOG_FILE: Option<String> = None;
const LOG_BACKUP: Option<String> = None;
const LOG_MAX_SIZE: u64 = GB as u64;

// helper functions
fn log_level() -> LogLevel {
    LOG_LEVEL
}

fn log_file() -> Option<String> {
    LOG_FILE
}

fn log_backup() -> Option<String> {
    LOG_BACKUP
}

fn log_max_size() -> u64 {
    LOG_MAX_SIZE
}

// struct definitions
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Debug {
    #[serde(default = "log_level")]
    log_level: LogLevel,
    #[serde(default = "log_file")]
    log_file: Option<String>,
    #[serde(default = "log_backup")]
    log_backup: Option<String>,
    #[serde(default = "log_max_size")]
    log_max_size: u64,
    // Retained for config compatibility but unused with tracing
    #[serde(default)]
    log_queue_depth: Option<usize>,
    #[serde(default)]
    log_single_message_size: Option<usize>,
}

// implementation
impl Debug {
    pub fn level_filter(&self) -> tracing_subscriber::filter::LevelFilter {
        self.log_level.to_level_filter()
    }

    pub fn log_file(&self) -> Option<String> {
        self.log_file.clone()
    }

    pub fn log_max_size(&self) -> u64 {
        self.log_max_size
    }
}

// trait implementations
impl Default for Debug {
    fn default() -> Self {
        Self {
            log_level: log_level(),
            log_file: log_file(),
            log_backup: log_backup(),
            log_max_size: log_max_size(),
            log_queue_depth: None,
            log_single_message_size: None,
        }
    }
}
