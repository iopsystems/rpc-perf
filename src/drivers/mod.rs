// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

pub mod memcache;
pub mod momento;
pub mod ping;
pub mod resp;

use crate::workload::WorkItem;
use crate::*;
use ::momento::MomentoError;
use async_channel::Receiver;
use std::io::{Error, ErrorKind, Result};
use tokio::io::*;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::time::{timeout, Duration};

pub enum ResponseError {
    /// Some exception while reading the response
    Exception,
    /// A timeout while awaiting the response
    Timeout,
    /// Some backends may have rate limits
    Ratelimited,
    /// Some backends may have their own timeout
    BackendTimeout,
}

impl From<MomentoError> for ResponseError {
    fn from(other: MomentoError) -> Self {
        match other {
            MomentoError::LimitExceeded { .. } => ResponseError::Ratelimited,
            MomentoError::Timeout { .. } => ResponseError::BackendTimeout,
            _ => ResponseError::Exception,
        }
    }
}
