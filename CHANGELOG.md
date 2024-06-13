## [Unreleased]

## [5.9.1] - 2024-06-13

### Fixed

- Fixes a bug that results in some metrics not appearing on the HTTP endpoints.
  (#231)
- Updates metriken and histogram dependencies. (#230)

## [5.9.0] - 2024-06-12

### Changed

- Moved metrics file exposition settings to separate config section. (#221)
- Default metrics interval is now one second. (#222)
- Updated Momento SDK to version 0.39.7. (#223)

## [5.8.0] - 2024-05-22

### Changed

- Kafka client improvements including TLS and compression support. (#208)

### Added

- A /quitquitquit handler for graceful early termination. (#212)

## [5.7.0] - 2024-04-23

### Changed

- OpenSSL is now the default SSL/TLS provider. BoringSSL is still available
  through the use of feature flags. (#194)

### Added

- Support for configuring the interval for file exposition allowing for finer
  resolution artifacts. (#192)
- Approximate compression ratio for message and value payloads. (#191)
- Support the `list_remove` command for Momento. (#198)
- New metrics for dropped requests. (#201)

## [5.6.0] - 2024-04-03

### Changed

- `metriken-exposition` is updated which results in differences for the
  file-based metric exposition. (#185 #187)
- Metrics reporting for both stdout and file output are now roughly aligned to
  the top of the second. This allows easier correlation with other metric
  artifacts. (#188)

## Fixed

- `h2` updated to address RUSTSEC-2024-0019

## [5.5.0] - 2024-03-27

### Added

- Support for writing metrics to a Parquet file.

### Fixes

- Updates `mio` to address RUSTSEC-2024-0019

## [5.4.0] - 2024-02-09

### Added

- Advanced ratelimit controller that can produce ramps, loops, mirrored ramps,
  and random steps.
- Latency percentiles now include max latency and are included in the json
  output.

### Fixed

- Fixes incorrect implementation of list_length for Momento.

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

[unreleased]: https://github.com/iopsystems/rpc-perf/compare/v5.9.1...HEAD/
[5.9.1]: https://github.com/iopsystems/rezolus/compare/v5.9.0...v5.9.1
[5.9.0]: https://github.com/iopsystems/rezolus/compare/v5.8.0...v5.9.0
[5.8.0]: https://github.com/iopsystems/rezolus/compare/v5.7.0...v5.8.0
[5.7.0]: https://github.com/iopsystems/rezolus/compare/v5.6.0...v5.7.0
[5.6.0]: https://github.com/iopsystems/rezolus/compare/v5.5.0...v5.6.0
[5.5.0]: https://github.com/iopsystems/rezolus/compare/v5.4.0...v5.5.0
[5.4.0]: https://github.com/iopsystems/rezolus/compare/v5.3.0...v5.4.0
[5.3.0]: https://github.com/iopsystems/rezolus/compare/v5.2.0...v5.3.0
[5.2.0]: https://github.com/iopsystems/rezolus/compare/v5.1.0...v5.2.0
[5.1.0]: https://github.com/iopsystems/rezolus/compare/v5.0.0...v5.1.0
[5.0.0]: https://github.com/iopsystems/rezolus/releases/tag/v5.0.0
