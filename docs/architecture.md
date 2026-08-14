# Architecture

Ignition is a single-symbol, in-memory CLOB matching engine. The architecture
keeps byte-level protocol handling, runtime orchestration, matching, risk,
market-data, and control-plane concerns separated so each layer has an explicit
ownership boundary.

## Component Ownership

### `protocol`

- Owns the SBE schema integration, generated codecs, and handwritten Rust facade.
- `protocol/ignition.xml` is the source of truth for wire messages.
- `protocol/generated/` contains committed generated Rust and is not edited
  by hand.
- `src/protocol/frame.rs` owns SBE header/template handling.
- `src/protocol/wire/` exposes endpoint-specific encode/decode helpers:
  control request/response paths and trading entry/event paths.
- Does not own business logic, matching decisions, risk checks, IO, or runtime
  orchestration.

### `domain`

- Owns protocol-agnostic types consumed by control, risk, matching, market data,
  replay, and benchmarking.
- The domain root owns integer-scaled `Price`, `Qty`, and raw notional types.
- `domain::rd` owns single-instrument reference data.
- `domain::control` owns `ControlRequest` and `ControlEvent`.
- `domain::trading::order` owns `OrderEntry` and execution reports.
- `domain::trading::md` owns trade prints, L1/L2 updates, and market-data
  sequencing.
- Shared deterministic values such as `RecvTs`, `EngineSeq`, reject reasons,
  and protocol-error events live at the domain root.

### `gateway`

- Owns trading frame/session ingress and egress for the order-entry endpoint.
- Trading ingress decodes only order-entry messages: new, cancel,
  cancel-replace, and mass cancel.
- Trading egress encodes execution reports and protocol errors for the trading
  endpoint.
- The live TCP path decodes a borrowed complete frame, normalizes it once, and
  lets runtime process/publish through a borrowed gateway publisher.
- The current live TCP adapter keeps one active client per endpoint. Production
  multi-client fan-in should sit in an upstream order gateway/session layer
  responsible for authentication, account binding, rate limits, backpressure,
  disconnect policy, and deterministic sequencing before frames reach this
  matcher runtime.
- Does not own matching, risk, market-data feed state, or control-plane policy.

### `runtime`

- Owns orchestration state, including the matching engine and market-data feed.
- Connects the separate control and trading queues to control state, risk
  checks, engine execution, market-data output, and protocol egress in explicit
  phases.
- Does not parse wire bytes directly and does not own order-book internals.

### `engine`

- Owns order-book state, order lifecycle mutations, and price-time priority
  matching.
- Accepts internal typed `OrderEntry` values, not raw protocol buffers.
- Exposes `Engine::execute(entry, sequencer, market_data, emit_report)`, where
  the caller owns sequence state, market-data feed state, and order-entry report
  egress.
- Stores the book directly as two `SideLadder` values, one shared resting-order
  arena, a `client_order_id -> handle` lookup, and a dense account table.
- Uses a fixed storage envelope from `configs/server.yaml` for direct price
  addressing; dynamic risk price bands are enforced outside the matching data
  structure.
- Emits deterministic execution reports and delegates trade-print/L1/L2 shaping
  to the caller-owned market-data feed runtime.
- Does not read wall-clock time, parse protocol bytes, perform transport IO, or
  make risk-policy decisions.

### `risk`

- Owns pre-match account limits, notional checks, price bands, and
  self-trade-prevention policy evaluation.
- Produces typed accept/reject decisions before matching.
- Reads book-derived state through `Engine` views, but does not mutate the book
  or emit protocol frames directly.

### `market_data`

- Owns outbound trade-print, L1, and L2 market-data shaping and multicast UDP
  egress.
- Owns market-data sequencing separately from the matching engine's execution
  report sequencing.
- Centralizes top-of-book change output, absolute level-update output, encoding,
  queueing, and flush/error state.
- Tracks only last-published L1 state; the matching book remains the source of truth.
- Does not make matching or risk decisions.

### `control`

- Owns operational state: kill switch, snapshot requests, heartbeat liveness,
  and status responses.
- Does not mutate the book directly; runtime applies control outcomes such as
  shutdown drains and snapshots in order.

### Client Tooling

- File, CLI, benchmark, replay, TCP, and in-memory clients are adapters from
  input sources to protocol frames.
- Clients must use the same SBE protocol path as the server runtime.
- Clients do not call privileged internal engine APIs.

## Runtime Direction

The intended flow is:

1. control and gateway receive bytes on their respective endpoints,
2. protocol wire adapters decode into `ControlRequest` or `OrderEntry`,
3. runtime drains control and trading queues in deterministic order,
4. control state and risk gates are observed before matching,
5. engine mutates book state and emits internal execution/book effects,
6. market-data and execution outputs are sequenced,
7. control and gateway encode endpoint-specific egress frames.

Replay and benchmark traffic exercise the same protocol/runtime boundary as
live clients, using file and in-memory I/O backends instead of privileged engine
entrypoints.

Concurrent submission is modeled as deterministic serialization before the
single-writer matcher. The runtime-assigned input stream, not client-provided
timestamps or socket scheduling, defines same-price FIFO priority.
