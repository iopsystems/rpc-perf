use serde::{Deserialize, Serialize};
use tracing::Level;

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Debug {
    #[serde(default = "default_log_level")]
    log_level: String,
}

impl Debug {
    pub fn log_level(&self) -> Level {
        match self.log_level.to_lowercase().as_str() {
            "error" => Level::ERROR,
            "warn" => Level::WARN,
            "info" => Level::INFO,
            "debug" => Level::DEBUG,
            "trace" => Level::TRACE,
            other => {
                eprintln!("invalid log level: {other}, defaulting to info");
                Level::INFO
            }
        }
    }
}

impl Default for Debug {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
        }
    }
}
