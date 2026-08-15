# Ignition

Ignition is a single-symbol, in-memory CLOB matching engine written in Rust. It
accepts order-entry and control messages over a real SBE binary protocol,
matches with deterministic price-time priority, emits execution reports and
market-data frames, supports deterministic replay, and includes a local
tail-latency benchmark.

## Architecture

The runtime is a single-writer loop with explicit phases:

```text
control ingress -> gateway ingress -> control/risk gate -> matching engine
-> execution-report egress + market-data egress -> flush
```

Core ownership boundaries are:

- `domain`: protocol-agnostic IDs, prices, quantities, timestamps, order-entry
  commands, execution reports, control events, market-data events, and reject
  reasons.
- `protocol`: SBE schema integration, generated codecs, message headers, and
  pure encode/decode adapters.
- `io`: TCP, UDP, file replay, recording, and in-memory framed byte movement.
- `control`: kill switch, heartbeat liveness, control acknowledgments, and
  snapshot request ingress.
- `gateway`: order-entry endpoint ingress and execution-report egress.
- `risk`: pre-match open-order, notional, price-band, and self-trade checks.
- `engine`: deterministic matching, price-time priority, order lifecycle, and
  book storage.
- `market_data`: trade-print, L1, and L2 market-data sequencing and egress.
- `runtime`: poll-driven orchestration across the components above.

More detail is in `docs/architecture.md`, `docs/matching.md`, and
`docs/datastructure.md`.

## Protocol

The wire protocol is SBE over little-endian fixed-width integer fields. The
schema source is `protocol/ignition.xml`; committed generated Rust lives under
`protocol/generated/`; handwritten adapters live under `src/protocol/`.

Endpoint families:

- Control ingress: `KillSwitch`, `Snapshot`, `Heartbeat`.
- Trading ingress: `NewOrder`, `CancelOrder`, `CancelReplace`, `MassCancel`.
- Trading egress: `ExecutionReport` and protocol errors.
- Market-data egress: `TradePrint`, `L1Update`, `L2Update`.
- Control egress: control responses, heartbeat status, snapshot rows, and
  protocol errors.

Every visible price-level mutation emits an absolute L2 update followed by an
L1 top-of-book snapshot, including when the best bid and ask tuple is unchanged.

Order-entry messages are scoped to one configured instrument. Prices and
quantities are unsigned integer-scaled values; zero is invalid in domain types
and is used only as a no-price sentinel in outbound optional price fields.
`protocol/README.md` documents message families, template ID ranges, framing,
endianness, and compatibility rules.

`Snapshot` provides an inspection-grade point-in-time view of the book and
selected runtime metadata. Its begin/order/end stream is deterministic and can
detect truncation, but it is not a restorable checkpoint or replay seed. It
does not contain all sequencing, control, account-ordering, or configuration
state required to continue with byte-identical outputs.
Recorded input streams remain the sole supported replay source.

## Matching And Risk Rules

Matching uses strict price-time priority. Aggressive orders walk executable
opposite levels in FIFO order. GTC limit residuals rest; market and IOC
residuals expire; would-cross post-only orders are canceled without execution.

Cancel-replace preserves FIFO priority only when the price is unchanged and the
new quantity is equal or lower. Price changes or quantity increases lose
priority and re-enter as a fresh GTC limit order.

Self-trade prevention is cancel-aggressive: if projected executable liquidity
contains a maker order from the same account, the incoming command is rejected
before the book mutates. Risk also enforces per-account open-order count,
per-account raw open notional, and an inclusive price band around the configured
startup reference price or latest deterministic trade price.

The kill switch is observed before risk and matching for risk-increasing
commands. Cancels and mass cancels remain available while halted so accounts can
reduce exposure.

## Running

Format and lint:

```bash
just fix
```

Run tests:

```bash
cargo test
```

Start the server:

```bash
just server
```

Start clients in separate shells:

```bash
just trading-client
just control-client
```

Run without local Rust setup:

```bash
docker compose up -d server
docker compose run --rm trading-client
docker compose run --rm control-client
```

The Docker server publishes gateway TCP on `19000`, control TCP on `19001`,
and market-data UDP on `19002`. The client containers use Docker-specific
configs that connect to the `server` compose service. Stop the server with:

```bash
docker compose down
```

## Replay

Replay uses recorded binary input frames and reinjects them through the same
control and gateway runtime paths as live traffic.

```bash
just replay <input_id>
just replay-compare <input_id>
```

Docker replay against the checked-in sample:

```bash
REPLAY_RUN=1786693693 docker compose run --rm replay
```

Replay file layout and byte-identical comparison behavior are documented in
`docs/replay.md`. Checked-in sample streams live under `fixtures/replay/`.

## Benchmark

Run the release benchmark:

```bash
just bench
```

A smaller smoke run:

```bash
cargo run --release --bin benchmark -- --orders 10000 --warmup 1000 --workload mixed
```

An open-loop run with scheduled-arrival-to-report HDR latency:

```bash
just bench-open-loop 100000
```

Docker benchmark:

```bash
docker compose run --rm benchmark
ORDERS=10000 WARMUP=1000 docker compose run --rm benchmark
SCHEDULE=open-loop TARGET_RATE=100000 docker compose run --rm benchmark
```

The benchmark reports p50, p99, p99.9, p99.99, max latency, throughput, and
measured allocation counts for the order-entry-to-execution-report hot path.
Burst mode remains the default. Open-loop mode schedules arrivals independently
of response time and measures each order from its intended arrival, directly
including backlog delay without post-hoc coordinated-omission correction.
Methodology and local observed numbers are in `BENCH.md`.

## Microstructure Notes

A CLOB is the right primitive for this slice because the venue needs continuous
price discovery, deterministic public priority rules, low-latency order entry,
and a transparent audit trail of executions and book updates. The implemented
design keeps the matching core single-writer and deterministic, which also
makes replay and future ZK-verifiable settlement inputs easier to reason about.

A frequent batch auction would be better when latency races become the dominant
fairness problem or when discrete clearing intervals are acceptable. RFQ or a
hybrid model is better for large, illiquid, or relationship-priced flow where
continuous displayed liquidity would be thin or easy to game. A crypto-native
venue may eventually want both: CLOB for liquid pairs and RFQ/FBA mechanisms for
less liquid pairs or batch-friendly settlement windows.

The main limitation is single-symbol, in-memory scope. The natural evolution is
to shard by instrument, keep each symbol on a single writer, add a versioned
recovery-checkpoint format with validated restore plus market-data replay, and
then put kernel-bypass networking or FPGA-assisted parsing in front of the same
protocol/domain boundary.
