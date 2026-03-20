# Migration: ringlog to tracing

## Motivation

- **Ecosystem compatibility**: `tracing` integrates with tokio-console, OpenTelemetry,
  and the broader Rust async ecosystem. `ringlog` does not.
- **Maintenance**: `ringlog` is a niche crate with limited community support. `tracing`
  is the de facto standard for Rust async applications.

## Scope

Replace `ringlog 0.8.0` with `tracing`, `tracing-subscriber`, and `tracing-appender`.
Macro-only swap (no structured fields or spans in this pass). The `output!()` macro in
`src/output/mod.rs` is unrelated to logging and stays untouched.

## Dependency Changes

**Remove:**
- `ringlog = "0.8.0"`

**Add:**
- `tracing = "0.1"`
- `tracing-subscriber = "0.3"` (features: `env-filter`)
- `tracing-appender = "0.2"`

## Initialization

Replace the `LogBuilder` / `MultiLogBuilder` / async flush task in `main.rs` (~30 lines)
with:

```rust
use tracing_appender::non_blocking;
use tracing_subscriber::{fmt, EnvFilter};

let (non_blocking_writer, _guard) = non_blocking(std::io::stderr());

tracing_subscriber::fmt()
    .with_env_filter(
        EnvFilter::from_default_env()
            .add_directive(config.debug().log_level().into())
    )
    .with_writer(non_blocking_writer)
    .init();
```

The `_guard` must be held in `main()` scope for the program's lifetime. Drop flushes
remaining buffered logs. This replaces ringlog's custom async flush thread entirely.

## Source File Changes

### Explicit import swaps (5 files)

| File | Old | New |
|------|-----|-----|
| `src/main.rs` | `use ringlog::*;` | `use tracing::{info, debug, warn, error, trace};` |
| `src/config/debug.rs` | `use ringlog::Level;` | `use tracing::Level;` |
| `src/replay/replay_engine.rs` | `use ringlog::info;` | `use tracing::info;` |
| `src/replay/replay_speed.rs` | `use ringlog::warn;` | `use tracing::warn;` |
| `src/clients/leaderboard/momento.rs` | `use ringlog::debug;` | `use tracing::debug;` |

### Transitive consumers (~17 files)

Files that get logging macros through `use crate::*;` from `main.rs` need **zero changes**.
The `tracing` macros accept the same `format_args!` syntax as `ringlog` macros.

### Untouched

- `output!()` macro in `src/output/mod.rs` -- this is the tool's timestamped stats output
  channel (println-based), not diagnostic logging.

## Config Simplification

`config/debug.rs` shrinks from 6 fields to 1:

```rust
pub struct Debug {
    log_level: Level,
}
```

**Removed fields** (all ringlog-specific):
- `log_file` / `log_backup` / `log_max_size` (file rotation -- dropped per decision)
- `log_queue_depth` / `log_single_message_size` (ringlog channel tuning)

**Backwards compatibility**: Use `#[serde(deny_unknown_fields)]` to surface a clear error
if old config files still specify removed fields. Prefer a loud failure over silent
behavioral change.

**Level deserialization**: `tracing::Level` does not derive `Deserialize`. Need a small
serde helper (e.g., deserialize from string, map to `tracing::Level`).

## Deleted Code

- ringlog `LogBuilder` / `MultiLogBuilder` initialization block in `main.rs`
- Async flush task (`tokio::spawn` loop flushing every 1ms)
- `ringlog::File`, `ringlog::Stderr`, `ringlog::Output` usage
- `ringlog::default_format` reference

## Risk Assessment

- **Level mapping**: Both crates use the same 5 levels. No semantic mismatch.
- **non_blocking channel capacity**: Defaults to 128k messages. More than adequate for a
  perf tool that logs sparingly during execution.
- **Guard lifetime**: Straightforward -- hold in main() scope.
- **Macro compatibility**: `tracing::{info, debug, warn, error, trace}!` accept the same
  `format_args!` invocations as ringlog. No call-site changes needed.

## Future Work (not in scope)

- Add structured fields to log call sites (`info!(file = %path, "replaying")`)
- Add spans around key operations for tokio-console / OTel integration
- Both are additive changes with zero plumbing impact.

## Effort Estimate

- ~30 lines deleted
- ~10 lines added
- 5 files get import swaps
- 1 config struct simplified
- 3 new deps, 1 removed
