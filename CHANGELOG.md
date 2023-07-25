## [Unreleased]

## [5.0.0] - 2023-07-25

### Changed

- Rewritten implementation of rpc-perf using Tokio to improve support for
  protocols where async libraries are prevelant.

### Added

- Support Redis pubsub and Momento topics.
- Basic HTTP/1.1 and HTTP/2.0 load generation.
