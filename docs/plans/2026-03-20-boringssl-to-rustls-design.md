# Replace BoringSSL/OpenSSL with rustls

## Goal

Unify all TLS in rpc-perf onto rustls, eliminating BoringSSL, OpenSSL, and native-tls dependencies entirely.

## Background

Three TLS stacks coexist today:
- **OpenSSL/BoringSSL** — `Connector` in `src/net/mod.rs` for memcache and ping ASCII clients, gated behind mutually exclusive `boringssl`/`openssl` feature flags
- **native-tls** — redis (`tls-native-tls` feature) and sqlx/MySQL (`tls-native-tls` feature)
- **rustls** — h2 pool, h3/QUIC, S3/hyper, and global crypto provider (aws-lc-rs)

rustls is already the majority provider. The others are legacy.

## Changes

### Cargo.toml

**Remove:**
- `boring`, `boring-sys`, `tokio-boring` (optional)
- `openssl`, `openssl-src`, `openssl-sys`, `tokio-openssl` (optional)
- `[features]` section (`default`, `boringssl`, `openssl`)

**Change:**
- `pelikan-net` — from `{ version = "0.4.1", default-features = false }` to `{ git = "https://github.com/pelikan-io/pelikan" }` (0.5.0 uses rustls natively, not yet on crates.io)
- `redis` features — swap `tls-native-tls`, `tokio-native-tls-comp` for `tls-rustls`, `tokio-rustls-comp`
- `sqlx` features — swap `tls-native-tls` for `tls-rustls`

**Add (if not already present):**
- `rustls-pemfile` — for loading PEM certificates and private keys in the Connector

### src/net/mod.rs

Delete `SslProvider` enum, `BoringsslTlsTcp`, `OpensslTlsTcp`, and all `#[cfg(feature)]` branches.

Replace with a single rustls-based implementation:

```rust
enum ConnectorImpl {
    Tcp,
    Tls(TlsConnectorState),
}

struct TlsConnectorState {
    inner: tokio_rustls::TlsConnector,
    server_name: Option<rustls::pki_types::ServerName<'static>>,
}

enum StreamImpl {
    Tcp(tokio::net::TcpStream),
    Tls(tokio_rustls::client::TlsStream<tokio::net::TcpStream>),
}
```

The `Connector::tls()` method builds a `rustls::ClientConfig`:
- Load CA from `ca_file` if provided, otherwise use `rustls_native_certs` for system roots
- Load client cert + key for mTLS if `private_key` + `certificate`/`certificate_chain` are set
- `verify_hostname: false` uses `rustls::client::danger::DangerousClientConfigBuilder` to install a no-verify provider
- `use_sni: false` handled by passing `ServerName::IpAddress` instead of DNS name

### src/main.rs

No change — already calls `aws_lc_rs::default_provider().install_default()`.

### Config (src/config/tls.rs)

No change — the `Tls` struct fields map cleanly to rustls configuration.

## What This Eliminates

- ~250 lines of `#[cfg]`-gated code in `net/mod.rs`
- 6 optional C/C++ TLS dependencies (boring, boring-sys, openssl, openssl-sys, openssl-src, tokio-boring/tokio-openssl)
- native-tls transitive dependency (via redis and sqlx feature swaps)
- The entire `[features]` section
- C compiler requirement for vendored OpenSSL builds

## Risks

- **pelikan-net 0.5.0 not on crates.io** — using git dep, same as session/protocol crates already do. Acceptable until upstream publishes.
- **redis `tls-rustls` feature** — well-supported in redis 1.0, no behavioral difference expected.
- **sqlx `tls-rustls` feature** — well-supported, no behavioral difference expected.
- **`verify_hostname: false`** — requires implementing a custom `ServerCertVerifier` via the danger API. Slightly more code than OpenSSL's one-liner, but straightforward.

## Effort Estimate

- ~250 lines deleted from `net/mod.rs`
- ~80 lines added (rustls connector implementation)
- 1 file rewritten (`src/net/mod.rs`)
- Cargo.toml dep changes (remove 6, change 3, add 1)
- No config file changes
- No client code changes (memcache, ping, redis, mysql all transparent)
