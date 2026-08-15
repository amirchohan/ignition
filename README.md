# Ignition

Ignition is a deterministic, single-symbol central limit order book written in
Rust. It accepts SBE order entry over TCP, applies pre-match risk controls,
matches in strict price-time priority, publishes execution reports and
multicast market data, and supports byte-identical replay and tail-latency
measurement.

## Quick Start

Run the test suite:

```bash
cargo test
```

Start the server and the two interactive clients in separate terminals:

```bash
just server
just trading-client
just control-client
```

Or run the same topology with Docker:

```bash
docker compose up -d server
docker compose run --rm trading-client
docker compose run --rm control-client
```

The default live endpoints are gateway TCP `127.0.0.1:19000`, control TCP
`127.0.0.1:19001`, and market-data multicast UDP `239.255.0.1:19002`.

For a richer demo, `trading-client` currently consumes Binance market data and generates buy orders for account 1 and sell orders for account 2; manual order entry remains available, and automatic order generation can be disabled by setting `synthetic_traffic.type` to `noop` in `configs/trading-client.yaml`.

## Documentation

| I want to understand… | Read |
| --- | --- |
| the components, runtime flow, and client/server topology | [Architecture](docs/architecture.md) |
| the SBE framing, message layouts, and sequencing contract | [Wire protocol](docs/protocol.md) |
| matching rules, order lifecycle, and order-book data structure | [Matching engine](docs/matching-engine.md) |
| risk policies, kill switch, heartbeat, and snapshots | [Risk and control](docs/risk-and-control.md) |
| why the system makes its major performance and market-structure choices | [Design decisions](docs/design-decisions.md) |
| how recording and byte-identical replay work | [Replay](docs/replay.md) |
| how latency, throughput, and allocations are measured | [Benchmark methodology and results](BENCH.md) |

The SBE XML schema is the byte-level protocol source of truth:
[`protocol/ignition.xml`](protocol/ignition.xml).

## Common Commands

```bash
just replay-compare 1786831955
just bench
cargo test
```

Use `docker compose down` to stop the Docker server.
