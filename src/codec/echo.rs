// Copyright 2021 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

use crate::codec::*;
use crate::config::Keyspace;
use crate::*;
use crc::{Crc, CRC_32_ISO_HDLC};
use std::io::BufRead;
use std::io::Write;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::Alphanumeric;

const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_ISO_HDLC);

pub struct Echo {
    config: Arc<Config>,
    rng: SmallRng,
}

impl Echo {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            rng: SmallRng::from_entropy(),
        }
    }

    pub fn echo(rng: &mut SmallRng, keyspace: &Keyspace, buf: &mut Session) {
        let value = rng
            .sample_iter(&Alphanumeric)
            .take(keyspace.length())
            .collect::<Vec<u8>>();

        let mut digest = CRC.digest();
        digest.update(&value);
        let _ = buf.write_all(&value);
        let _ = buf.write_all(&digest.finalize().to_be_bytes());
        let _ = buf.write_all(b"\r\n");
    }
}

impl Codec for Echo {
    fn encode(&mut self, buf: &mut Session) {
        let keyspace = self.config.choose_keyspace(&mut self.rng);
        Self::echo(&mut self.rng, keyspace, buf)
    }

    fn decode(&self, buffer: &mut Session) -> Result<(), ParseError> {
        // no-copy borrow as a slice
        let buf: &[u8] = (*buffer).buffer();

        // check if we got a CRLF
        let mut double_byte_windows = buf.windows(2);
        if let Some(response_end) = double_byte_windows.position(|w| w == b"\r\n") {
            if response_end < 5 {
                Err(ParseError::Unknown)
            } else {
                let message = &buf[0..(response_end - 4)];
                let crc_received = &buf[(response_end - 4)..response_end];
                let mut digest = CRC.digest();
                digest.update(message);
                let crc_calculated = digest.finalize();
                let crc_calculated: [u8; 4] =
                    unsafe { std::mem::transmute(crc_calculated.to_be()) };
                if crc_calculated != crc_received[..] {
                    debug!(
                        "Response has bad CRC: {:?} != {:?}",
                        crc_received, crc_calculated
                    );
                    metrics::RESPONSE_EX.increment();
                    Err(ParseError::Error)
                } else {
                    let _ = buffer.consume(response_end + 2);
                    Ok(())
                }
            }
        } else {
            Err(ParseError::Incomplete)
        }
    }
}
