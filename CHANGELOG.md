## [Unreleased]

## [5.1.0] - 2023-08-23

### Added

- rpc-perf can now generate zrange commands with the by_score option for both
  the momento and redis protocols.
- Dataspec crate which allows easy consumption of JSON output.

### Changed

- rpc-perf will now set TCP_NODELAY by default on all TCP connections it
  creates.
- The default buffer size for the memcached client has been increased from 4KB
  to 16KB.

### Fixed

- Hit/miss statistics in the output json will now have their correct values
  instead of always being 0.

## [5.0.0] - 2023-07-25

### Changed

- Rewritten implementation of rpc-perf using Tokio to improve support for
  protocols where async libraries are prevelant.

### Added

- Support Momento topics.
- Basic HTTP/1.1 and HTTP/2.0 load generation.

[unreleased]: https://github.com/iopsystems/rpc-perf/compare/v5.1.0...HEAD
[5.1.0]: https://github.com/iopsystems/rezolus/compare/v5.0.0...v5.1.0
[5.0.0]: https://github.com/iopsystems/rezolus/releases/tag/v5.0.0