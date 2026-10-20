# redi

[![CI](https://github.com/ajakash/redi/actions/workflows/ci.yml/badge.svg)](https://github.com/ajakash/redi/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust Edition](https://img.shields.io/badge/rust-2024%20edition-orange.svg)](Cargo.toml)

**redi** is a lightweight, asynchronous, sharded in-memory key-value cache written in Rust. Inspired by Redis, it is designed for low latency, zero lock contention, and high concurrent throughput using an actor-based sharding model and a custom binary wire protocol.

---

## Features

- **Actor-Based Sharded Architecture**: The database is partitioned into independent shard actors running on dedicated Tokio tasks. There are no shared mutexes between shards.
- **Fast Deterministic Routing**: Uses 64-bit XXH3 hashing ([`xxhash-rust`](https://crates.io/crates/xxhash-rust)) to distribute keys evenly and deterministically across shards.
- **Ordered Storage**: Each shard uses a `BTreeMap` engine for efficient in-memory key-value lookups, inserts, and deletions.
- **TTL Support with Nanosecond Precision**: Keys support TTL (time-to-live) specified in nanoseconds, with passive/lazy eviction upon read or update.
- **Dual Connection Modes**:
  - **Quick Mode**: Single request-response cycle per connection, automatically closed upon completion (minimal connection overhead for one-off commands).
  - **Long Session Mode (`CONN`)**: Persistent connection supporting pipelining and multiplexing, with 1-byte request IDs (`req_id`) to correlate asynchronous responses.
- **Graceful Shutdown**: Coordinated termination using Tokio `CancellationToken`, cleanly shutting down listener loops, active sessions, and database shard workers on `SIGINT` (Ctrl+C).
- **Safety Limits**: Built-in validation guarding against memory exhaustion (max 1 KiB key length, max 1 MiB value length).

---

## Architecture

```
                  +-----------------------+
                  |      TCP Client       |
                  +-----------+-----------+
                              | TCP (Wire Protocol)
                              v
                  +-----------------------+
                  |  Network Listener     |  (Default 127.0.0.1:6380)
                  |  Quick / Long Session |
                  +-----------+-----------+
                              |
                              | DatabaseEvent
                              v
                  +-----------------------+
                  |       Conductor       |  xxh3_64(key) % shard_count
                  +---+-------+-------+---+
                      |       |       |
            +---------+       |       +---------+
            | mpsc            | mpsc            | mpsc
            v                 v                 v
     +--------------+  +--------------+  +--------------+
     |   Shard 1    |  |   Shard 2    |  |   Shard N    |
     | (Tokio Task) |  | (Tokio Task) |  | (Tokio Task) |
     |   BTreeMap   |  |   BTreeMap   |  |   BTreeMap   |
     +--------------+  +--------------+  +--------------+
```

1. **Listener & Session Manager** ([`src/network/net.rs`](file:///home/ajaka/Documents/codes/rust/redi/src/network/net.rs)): Accepts incoming TCP connections. If the client sends `CMD_CONN` (`0x05`), the connection transitions into a persistent session with background response streaming; otherwise, it operates in one-shot quick mode.
2. **Conductor** ([`src/database/conductor.rs`](file:///home/ajaka/Documents/codes/rust/redi/src/database/conductor.rs)): Routes incoming [`DatabaseEvent`](file:///home/ajaka/Documents/codes/rust/redi/src/database/types.rs#L9-L34) requests to the responsible shard channel by calculating `xxh3_64(key) % shard_count`.
3. **Database Shard Actor** ([`src/database/db.rs`](file:///home/ajaka/Documents/codes/rust/redi/src/database/db.rs)): Each shard runs in an isolated asynchronous event loop listening on a bounded `mpsc::channel(100)`, executing operations sequentially without lock contention.

---

## Wire Protocol

redi communicates via a compact, big-endian binary wire protocol.

### Command Codes

| Code (`u8`) | Name      | Description                                                     |
| :---------: | :-------- | :-------------------------------------------------------------- |
|   `0x01`    | **SET**   | Insert or overwrite a key-value pair with a TTL                 |
|   `0x02`    | **SETNX** | Insert key-value pair only if key does not exist or has expired |
|   `0x03`    | **GET**   | Retrieve the value of a key                                     |
|   `0x04`    | **DEL**   | Delete a key                                                    |
|   `0x05`    | **CONN**  | Switch connection to persistent session mode                    |

### Limits

- **Max Key Length**: 1,024 bytes (1 KiB)
- **Max Value Length**: 1,048,576 bytes (1 MiB)
- Exceeding these limits causes the server to immediately close the connection.

---

### Request Frames

#### `SET` (`0x01`) and `SETNX` (`0x02`)

```
+----------+------------+---------------+-----------------+---------+-----------+----------------+
| cmd (1B) | req_id(1B) | key_len (2B)  | val_len (4B)    | key     | value     | ttl_ns (16B)   |
| 0x01/02  | u8         | u16 BE        | u32 BE          | bytes   | bytes     | u128 BE        |
+----------+------------+---------------+-----------------+---------+-----------+----------------+
```

_Note: A `ttl` of `0` is rejected._

#### `GET` (`0x03`) and `DEL` (`0x04`)

```
+----------+------------+---------------+---------+
| cmd (1B) | req_id(1B) | key_len (2B)  | key     |
| 0x03/04  | u8         | u16 BE        | bytes   |
+----------+------------+---------------+---------+
```

#### `CONN` (`0x05`)

```
+----------+
| cmd (1B) |
| 0x05     |
+----------+
```

Switches connection from one-shot mode into a persistent long session. Subsequent requests in this session send standard command frames without the connection being terminated after a reply.

---

### Response Frames

All responses begin with a 1-byte signal header:

| Signal (`u8`) | Name             | Response Body Format                                                                           |
| :-----------: | :--------------- | :--------------------------------------------------------------------------------------------- |
|    `0x01`     | `Err`            | `[code: 1B]`                                                                                   |
|    `0x02`     | `Set`            | `[req_id: 1B][ok: 1B]`                                                                         |
|    `0x03`     | `SetNx`          | `[req_id: 1B][ok: 1B]`                                                                         |
|    `0x04`     | `Get`            | `[req_id: 1B][ok: 1B][val_len: 4B BE][value: val_len bytes]` _(if not found: ok=0, val_len=0)_ |
|    `0x05`     | `Del`            | `[req_id: 1B][ok: 1B]` _(ok=1 if removed, 0 if not present)_                                   |
|    `0x06`     | `KeyTooLong`     | None (connection dropped)                                                                      |
|    `0x07`     | `ValueTooLong`   | None (connection dropped)                                                                      |
|    `0x08`     | `InvalidCommand` | None                                                                                           |

---

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/) (2024 edition compatible, Rust 1.85+ recommended)

### Build and Run

To compile and launch the standalone server:

```bash
cargo run --release
```

By default, the server binds to `127.0.0.1:6380`. Press `Ctrl+C` to cleanly shut down.

---

## Testing

The codebase includes comprehensive unit tests and integration tests covering TTL expiration, concurrency, network protocols, shard routing, and graceful shutdown.

Run the test suite:

```bash
cargo test
```

Check code formatting and clippy lints:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
```

---

## Project Structure

```text
redi/
├── Cargo.toml
├── src/
│   ├── main.rs              # Server binary entry point and signal handling
│   ├── lib.rs               # Library root and entry points
│   ├── common/
│   │   ├── limits.rs        # Key and value length limits
│   │   ├── mod.rs
│   │   └── types.rs         # Command enums and actions
│   ├── database/
│   │   ├── conductor.rs     # Shard router and XXH3 dispatch
│   │   ├── db.rs            # Shard actor and BTreeMap store
│   │   ├── mod.rs
│   │   └── types.rs         # Database events and responses
│   └── network/
│       ├── net.rs           # TCP listener, parser, session dispatch
│       ├── mod.rs
│       └── types.rs         # Response signal definitions
└── tests/
    ├── common/mod.rs        # Wire-protocol client harness
    └── integration.rs       # End-to-end integration tests
```

---

## License

This project is licensed under the [MIT License](LICENSE).
