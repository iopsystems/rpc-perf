## [Unreleased]

## [5.3.0] - 2023-11-21

### Changed

- Metrics endpoints now use a configurable integration period controlled by the
  value for the `interval` in the general config section. This means that
  percentile metrics will be reflect the behavior across that period of time.
- Use rustcommon compact histogram instead of the local implementation.

### Added

- Support for benchmarking Apache Kafka.

### Fixed

- Early connection attempts are now reflected in the stats for the first window.

## [5.2.0] - 2023-10-17

### Changed

- Metrics library (`metriken`) updated to replace heatmaps with histograms,
  which reduces cost on the metrics write path.

### Added

- Dataspec now supports merging of histograms.

### Fixed

- RUSTSEC-2023-0065

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

[unreleased]: https://github.com/iopsystems/rpc-perf/compare/v5.3.0...HEAD
[5.3.0]: https://github.com/iopsystems/rezolus/compare/v5.2.0...v5.3.0
[5.2.0]: https://github.com/iopsystems/rezolus/compare/v5.1.0...v5.2.0
[5.1.0]: https://github.com/iopsystems/rezolus/compare/v5.0.0...v5.1.0
[5.0.0]: https://github.com/iopsystems/rezolus/releases/tag/v5.0.0