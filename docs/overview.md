# rpc-perf Architecture Overview

## High-Level Module Dependency Diagram

```mermaid
graph TD
    main[main.rs]

    config[config]
    workload[workload]
    clients[clients]
    metrics[metrics]
    net[net]
    admin[admin]
    output[output]
    replay[replay]

    main --> config
    main --> workload
    main --> clients
    main --> metrics
    main --> net
    main --> admin
    main --> output
    main --> replay

    workload --> config

    clients --> config
    clients --> workload
    clients --> metrics
    clients --> net

    admin --> config
    admin --> metrics

    output --> config
    output --> metrics

    net --> config

    replay --> config
    replay --> workload
    replay --> clients
```

## Module Descriptions

| Module | Description |
|--------|-------------|
| **config** | Handles TOML configuration parsing and validation, defining structures for all configurable aspects including protocols, targets, TLS settings, workloads, and output formats. |
| **workload** | Generates client requests based on configured distributions and patterns, managing ratelimiting and request orchestration across different protocol types. |
| **clients** | Contains protocol-specific client implementations (Memcache, Redis, Momento, S3, MySQL, gRPC ping variants, PubSub) that execute requests and record latency metrics. |
| **metrics** | Provides centralized metrics collection infrastructure using metriken, managing periodic snapshots for latency histograms and counters. |
| **net** | Abstracts network connectivity with TLS/SSL provider selection (BoringSSL or OpenSSL) and TCP connector configuration. |
| **admin** | Runs an HTTP server exposing metrics endpoints (`/metrics`, `/vars`, `/metrics.json`) and providing runtime ratelimit control via admin APIs. |
| **output** | Handles CLI display of statistics and file-based metrics export in multiple formats (Prometheus, JSON, Parquet, msgpack). |
| **replay** | Implements command log replay functionality, allowing traffic patterns to be recorded and replayed for reproducible benchmarking. |

## Clients Submodule Hierarchy

```mermaid
graph TD
    clients[clients]

    cache[cache]
    ping[ping]
    pubsub[pubsub]
    store[store]
    leaderboard[leaderboard]
    oltp[oltp]

    clients --> cache
    clients --> ping
    clients --> pubsub
    clients --> store
    clients --> leaderboard
    clients --> oltp

    memcache[memcache]
    redis[redis]
    momento_cache[momento]

    cache --> memcache
    cache --> redis
    cache --> momento_cache

    ascii[ascii]
    grpc_tonic[grpc/tonic]
    grpc_h2[grpc/h2]
    grpc_h3[grpc/h3]

    ping --> ascii
    ping --> grpc_tonic
    ping --> grpc_h2
    ping --> grpc_h3

    blabber[blabber]
    momento_pubsub[momento]

    pubsub --> blabber
    pubsub --> momento_pubsub

    s3[s3]
    store --> s3

    momento_lb[momento]
    leaderboard --> momento_lb

    mysql[mysql]
    oltp --> mysql
```
