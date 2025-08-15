use std::time::{Duration, Instant};

use ratelimit::Ratelimiter;
use ringlog::{warn};

use crate::{config::workload::Ratelimit, metrics::RATELIMIT_CURR, workload::BUCKET_CAPACITY};

pub enum TimingController {
    Rate(RateController),
    Speed(SpeedController),
}

impl TimingController {
    pub fn delay(&mut self, ts: u64) {
        match self {
            TimingController::Rate(controller) => controller.delay(ts),
            TimingController::Speed(controller) => controller.delay(ts),
        }
    }
}

pub struct RateController {
    ratelimiter: Ratelimiter,
}

impl RateController {
    pub fn new(ratelimit: &Ratelimit) -> Self {
        let ratelimiter = ratelimit.start().map(|rate| {
            let rate = rate.get();
            let amount = (rate as f64 / 1_000_000.0).ceil() as u64;
            RATELIMIT_CURR.set(rate as i64);

            // even though we might not have nanosecond level clock resolution,
            // by using a nanosecond level duration, we achieve more accurate
            // ratelimits.
            let interval = Duration::from_nanos(1_000_000_000 / (rate / amount));

            Ratelimiter::builder(amount, interval)
                .max_tokens(amount * BUCKET_CAPACITY)
                .build()
                .expect("failed to initialize ratelimiter")
        });
        Self {
            ratelimiter: ratelimiter.unwrap(),
        }
    }

    pub fn delay(&mut self, _ts: u64) {
        match self.ratelimiter.try_wait() {
            Ok(_) => (),
            Err(duration) => {
                std::thread::sleep(duration);
            }
        }
    }
}

pub struct SpeedController {
    ts_ms: u64,
    next: Instant,
    speed: f64,
}

impl Default for SpeedController {
    fn default() -> Self {
        Self::new(1.0)
    }
}

impl SpeedController {
    pub fn new(speed: f64) -> Self {
        let next = Instant::now();
        warn!("Created next: {:?}", next);
        Self {
            ts_ms: 0,
            next: next,
            speed,
        }
    }

    pub fn delay(&mut self, ts: u64) {
        // handle new timestamp in log
        if ts > self.ts_ms {
            let mut now = Instant::now();
            if self.ts_ms != 0 {
                let log_dur = Duration::from_nanos(
                    (((ts - self.ts_ms) * 1_000_000) as f64 / self.speed) as u64,
                );
                self.next += log_dur;
                // we probably don't care if the replay engine is falling behind
                // by milliseconds, warn only if it exceeds 1 second
                let diff = now - self.next;
                if diff > Duration::from_secs(1) {
                    warn!("falling behind by {:?} milliseconds ... try reducing replay speed", diff.as_millis());
                    // reset next to now so that we don't continue to fall behind
                    // by an increasing amount
                    self.next = now;
                }
            }
            self.ts_ms = ts;
            // delay if needed
            while now < self.next {
                std::thread::sleep(core::time::Duration::from_micros(100));
                now = Instant::now();
            }
        }
    }
}
