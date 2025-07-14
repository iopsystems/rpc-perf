use chrono::{DateTime, FixedOffset};
use std::{
    io::{Error, ErrorKind},
    str::FromStr,
};

// replaying only memcached operations for now, TODO: add RESP
pub const VALID_OPERATIONS: &[&str] = &["get", "set", "delete"];

#[derive(Debug, Clone)]
pub struct CommandLogLine {
    timestamp: DateTime<FixedOffset>,
    operation: String,
    key: String,
    _flags: Option<u64>,
    ttl: Option<u64>,
    value_size: Option<u64>,
    _response_status: u64,
    _response_size: u64,
}

impl CommandLogLine {
    pub fn timestamp(&self) -> DateTime<FixedOffset> {
        self.timestamp
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn ttl(&self) -> Option<u64> {
        self.ttl
    }

    pub fn value_size(&self) -> Option<u64> {
        self.value_size
    }
}

// Parses a line from the momento memcached proxy command log
impl FromStr for CommandLogLine {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.split(' ').collect::<Vec<&str>>();
        let timestamp = parts[0]
            .parse::<DateTime<FixedOffset>>()
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let operation = parts[1].replace("\"", "").to_string();
        if !VALID_OPERATIONS.contains(&operation.as_str()) {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid operation"));
        }
        if operation == "set" && parts.len() == 8 {
            Ok(CommandLogLine {
                timestamp,
                operation,
                key: parts[2].replace("\"", "").to_string(),
                _flags: if parts[3] == "0" {
                    None
                } else {
                    Some(
                        parts[3]
                            .parse::<u64>()
                            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                    )
                },
                ttl: if parts[4] == "0" {
                    None
                } else {
                    Some(
                        parts[4]
                            .parse::<u64>()
                            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                    )
                },
                value_size: Some(
                    parts[5]
                        .replace("\"", "")
                        .parse::<u64>()
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                ),
                _response_status: parts[6]
                    .parse::<u64>()
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                _response_size: parts[7]
                    .parse::<u64>()
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            })
        } else if parts.len() == 5 {
            Ok(CommandLogLine {
                timestamp,
                operation,
                key: parts[2].replace("\"", "").to_string(),
                _flags: None,
                ttl: None,
                value_size: None,
                _response_status: parts[3]
                    .parse::<u64>()
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
                _response_size: parts[4]
                    .parse::<u64>()
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?,
            })
        } else {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Invalid command log line",
            ));
        }
    }
}
