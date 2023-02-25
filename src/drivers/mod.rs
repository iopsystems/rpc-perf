// Copyright 2023 IOP Systems, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

pub mod memcache;
pub mod momento;
pub mod ping;
pub mod resp;

use crate::workload::WorkItem;
use crate::*;
use async_channel::Receiver;
use std::io::{Error, ErrorKind, Result};
use tokio::io::*;
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::time::{timeout, Duration};

pub enum ResponseError {
    Exception,
    Timeout,
}
