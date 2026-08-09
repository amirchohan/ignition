# Architecture

Ignition is a single-symbol, in-memory CLOB matching engine. The architecture
keeps byte-level protocol handling, runtime orchestration, matching, risk,
market-data, and control-plane concerns separated so later milestones can fill
in behavior without changing ownership.

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
- `domain::rd` owns single-instrument reference data and integer-scaled
  `Price`, `Qty`, and raw notional types.
- `domain::control` owns `ControlRequest` and `ControlEvent`.
- `domain::trading::order` owns `OrderEntry` and execution reports.
- `domain::trading::md` owns trade prints, book updates, and market-data
  sequencing.
- Shared deterministic values such as `RecvTs`, `EngineSeq`, reject reasons,
  and protocol-error events live at the domain root.

### `gateway`

- Owns frame/session ingress and egress for two endpoint families:
  control and trading.
- Control ingress decodes only control requests: kill switch, snapshot, and
  heartbeat.
- Trading ingress decodes only order-entry messages: new, cancel,
  cancel-replace, and mass cancel.
- Control egress encodes control responses, heartbeat status, and protocol
  errors for the control endpoint.
- Trading egress encodes execution reports, trade prints, book updates, and
  protocol errors for the trading endpoint.
- Does not own matching, risk, market-data sequencing, or control-plane policy.

### `runtime`

- Owns the future poll-driven orchestration loop.
- Connects the separate control and trading queues to control state, risk
  checks, engine execution, market-data output, and protocol egress in explicit
  phases.
- Does not parse wire bytes directly and does not own order-book internals.

### `engine`

- Owns future order-book state, order lifecycle mutations, and price-time
  priority matching.
- Accepts internal typed order entries, not raw protocol buffers.
- Emits internal execution/book events for market-data and protocol conversion.
- Does not read wall-clock time, parse protocol bytes, or perform transport IO.

### `risk`

- Owns future pre-match gating: account limits, notional checks, price bands,
  kill-switch observation, and self-trade-prevention policy evaluation.
- Produces typed accept/reject decisions before matching.
- Does not mutate the order book or emit protocol frames directly.

### `market_data`

- Owns future outbound trade-print and book-update shaping.
- Centralizes top-of-book output and keeps the event model ready for later L2
  incrementals.
- Does not make matching or risk decisions.

### `control`

- Owns future operational state: kill switch, snapshot requests, heartbeat
  liveness, and status responses.
- The exact missed-heartbeat fail-fast behavior is deferred to the control
  milestone.
- Does not mutate the book directly; runtime applies control outcomes in order.

### Client Tooling

- Future file, CLI, benchmark, replay, TCP, and UDS clients are adapters from
  input sources to protocol frames.
- Clients must use the same SBE protocol path as the server runtime.
- Clients do not call privileged internal engine APIs.

## Runtime Direction

The intended flow is:

1. gateway receives bytes on the control or trading endpoint,
2. protocol wire adapters decode into `ControlRequest` or `OrderEntry`,
3. runtime drains control and trading queues in deterministic order,
4. control state and risk gates are observed before matching,
5. engine mutates book state and emits internal execution/book effects,
6. market-data and execution outputs are sequenced,
7. gateway encodes control or trading egress frames.

Milestones 1 and 2 define this ownership contract, protocol/domain boundary,
single-symbol reference data, and deterministic core model. Matching, risk
behavior, real transport IO, clients, replay, and benchmarking are later
milestones.
