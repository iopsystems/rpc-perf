## [Unreleased]

## [5.19.0] - 2025-01-16

### Changed

- Changes to the Momento Cache HTTP client to fix some issues where PUT requests
  saw unexpectedly high latency. (#344)

## [5.18.0] - 2024-12-30

### Changed

- Increased S3 HTTP1 client buffer size. (#324)
- Increased client work queue depth and ratelimiter token bucket capacity.
  (#326)

### Added

- RPM packaging workflow. (#327)

## [5.17.0] - 2024-12-20

### Changed

- Improved the performance of the Momento Cache HTTP client by tuning some HTTP2
  settings. Results in improved latency for large reads. (#320)

### Fixed

- Removed a noisy log event in the Momento Cache HTTP client. (#319)

## [5.16.0] - 2024-11-08

### Added

- Storage client Time-to-First-Byte now included in cli output. (#308)
- S3 Express is now supported. (#309)

### Fixed

- Momento Cache HTTP API now enforces a 15 minute default TTL unless one is set
  in the workload config. (#310)

## [5.15.0] - 2024-10-16

### Changed

- Improved speed of value generation which could have been a bottleneck when
  large values are used. (#301)

### Added

- Add support for connecting to redis over unix socket. (#300)

### Fixed

- Fixed bugs in client initialization which prevented OLTP workload from
  functioning. (#301)


## [5.14.1] - 2024-10-11

### Fixed

- Fix handling of Momento HTTP endpoint DNS resolution when port number is
  specified. (#295)

## [5.14.0] - 2024-10-10

### Added

- Added sysbench compatible OLTP workload with MySQL support. (#277)

### Fixed

- Improved compatibility with older distributions by pinning openssl dependency.
  (#291)

## [5.13.0] - 2024-10-08

### Added

- Adds a metric for time-to-first-byte for S3 client. (#286)

### Fixed

- Improves ratelimit stability by reducing queue size and max tokens. (#287)
- Fixes handling of large payloads for Momento HTTP client. (#288)

## [5.12.1] - 2024-10-07

### Fixed

- Fixed runtime panic in Momento HTTP load test due to missing default TLS
  provider. (#280)
- Fix to consume complete response body before calculating response latency for
  Momento HTTP client. (#283)

## [5.12.0] - 2024-10-03

### Changed

- Removed the HTTP1/HTTP2 clients and reorganized the client code. (#266)

### Added

- Adds support for gRPC ping using `tonic`, directly using `h2`, and a pre-draft
  implementation that is gRPC ping over HTTP3/QUIC. (#275)

### Fixed

- Fixes a bug in Redis `set` commands when a TTL is specified. (#278)

## [5.11.1] - 2024-10-02

### Fixed

- Fixes release packaging workflow (#273)

## [5.11.0] - 2024-10-01

### Added

- Units added for histograms (#253)
- Support for Momento Cache HTTP API (#264)
- Support for S3 REST API (#264)

## [5.10.0] - 2024-08-05

### Fixed

- Updated OpenSSL to address RUSTSEC-2024-0357 (#242)

### Added

- `zincr` and `zmscore` re-added for Momento Cache (#237)
- Metrics sampling interval added to parquet metadata (#245)
- Momento Store API is now supported (#240)

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

[unreleased]: https://github.com/iopsystems/rpc-perf/compare/v5.19.0...HEAD
[5.19.0]: https://github.com/iopsystems/rpc-perf/compare/v5.18.0...v5.19.0
[5.18.0]: https://github.com/iopsystems/rpc-perf/compare/v5.17.0...v5.18.0
[5.17.0]: https://github.com/iopsystems/rpc-perf/compare/v5.16.0...v5.17.0
[5.16.0]: https://github.com/iopsystems/rpc-perf/compare/v5.15.0...v5.16.0
[5.15.0]: https://github.com/iopsystems/rpc-perf/compare/v5.14.1...v5.15.0
[5.14.1]: https://github.com/iopsystems/rezolus/compare/v5.14.0...v5.14.1
[5.14.0]: https://github.com/iopsystems/rezolus/compare/v5.13.0...v5.14.0
[5.13.0]: https://github.com/iopsystems/rezolus/compare/v5.12.1...v5.13.0
[5.12.1]: https://github.com/iopsystems/rezolus/compare/v5.12.0...v5.12.1
[5.12.0]: https://github.com/iopsystems/rezolus/compare/v5.11.1...v5.12.0
[5.11.1]: https://github.com/iopsystems/rezolus/compare/v5.11.0...v5.11.1
[5.11.0]: https://github.com/iopsystems/rezolus/compare/v5.10.0...v5.11.0
[5.10.0]: https://github.com/iopsystems/rezolus/compare/v5.9.1...v5.10.0
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
