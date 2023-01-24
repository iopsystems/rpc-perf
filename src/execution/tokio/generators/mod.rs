use super::*;

mod get;
mod hash_delete;
mod hash_get;
mod hash_multi_get;
mod hash_set;
mod set;

pub use get::*;
pub use hash_delete::*;
pub use hash_get::*;
pub use hash_multi_get::*;
pub use hash_set::*;
pub use set::*;

pub fn convert_ratelimit(rate: NonZeroU64) -> (u64, Interval) {
	let rate = u64::from(rate);

    // TODO: this gives approximate rates
    //
    // timer granularity should be millisecond level on most platforms
    // for higher rates, we can insert multiple work items every interval
    let (quanta, interval) = if rate <= 1000 {
        (1, 1000 / rate)
    } else {
        (rate / 1000, 1)
    };

    (quanta, ::tokio::time::interval(Duration::from_millis(interval)))
}