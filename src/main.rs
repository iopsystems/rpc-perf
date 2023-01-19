#[macro_use]
extern crate rustcommon_metrics;

mod execution;

fn main() {
    println!("rpc-perf alpha");

    print!("deciding execution model: ");

    println!("async with tokio executor");

    let _ = execution::tokio::run();
}
