# rpc-perf

rpc-perf is a tool for measuring the performance of RPC services and is
primarily used to benchmark caching systems.

[![License: Apache-2.0][license-badge]][license-url]
[![Build Status: CI][ci-build-badge]][ci-build-url]

# Content
* [Getting rpc-perf](#getting-rpc-perf)
* [Configuration](#configuration)
* [Sample Usage](#sample-usage)
* [Sample Output](#sample-output)
* [Practices](#practices)
* [Features](#features)
* [Future Work](#future-work)
* [Contributing](#contributing)

# Getting rpc-perf

rpc-perf is built through the `cargo` command which ships with rust. If you 
don't have Rust installed, you can use [rustup][rustup] to manage your Rust
installation. Otherwise, follow the instructions on
[rust-lang.org](https://rust-lang.org) to get Rust and Cargo installed. rpc-perf
is developed and tested against the stable Rust toolchain.

## Build from source

With Rust installed, clone this repo, and cd into this folder:

```shell
git clone https://github.com/iopsystems/rpc-perf.git
cd rpc-perf
cargo build --release
```

This will produce a binary at `target/release/rpc-perf` which can be run
in-place or copied to a more convenient location on your system.

# Configuration

rpc-perf takes a configuration file to define the test parameters and runtime
options.

## Sample Usage

**BEWARE** rpc-perf can write to its target and can generate many requests
* *run only* if data in the server can be lost/destroyed/corrupted/etc
* *run only* if you understand the impact of sending high-levels of traffic
  across your network

```shell
# run rpc-perf using the specified configuration file
rpc-perf configs/memcache.toml
```

## Practices

* Start with a short test before moving on to tests spanning larger periods of
  time
* If comparing latency between two setups, be sure to set a ratelimit that's
  achievable on both
* Keep `--clients` below the number of cores on the machine generating workload
* Increase `--poolsize` as necessary to simulate production-like connection
  numbers
* You may need to use multiple machines to generate enough workload and/or
  connections to the target
* Log your configuration and results to make repeating and sharing experiments
  easy
* Use waterfalls to help visualize latency distribution over time and see
  anomalies

## Features

* high-resolution latency metrics
* supports memcache and redis protocols
* optional waterfall visualization of latencies
* powerful workload configuration

## Contributing

If you want to submit a patch, please follow these steps:

1. create a new issue
2. fork on github & clone your fork
3. create a feature branch on your fork
4. push your feature branch
5. create a pull request linked to the issue

## License

This software is licensed under the Apache 2.0 license, see [LICENSE](LICENSE) for details.

[ci-build-badge]: https://img.shields.io/github/workflow/status/iopsystems/rpc-perf/CI/main?label=CI
[ci-build-url]: https://github.com/iopsystems/rpc-perf/actions/workflows/cargo.yml?query=branch%3Amain+event%3Apush
[contributors]: https://github.com/iopsystems/rpc-perf/graphs/contributors?type=a
[license-badge]: https://img.shields.io/badge/license-Apache%202.0-blue.svg
[license-url]: https://github.com/iopsystems/rpc-perf/blob/main/LICENSE
[rustlang]: https://rust-lang.org/
[rustup]: https://rustup.rs
