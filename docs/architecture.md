# Architecture

Ignition is a single-symbol, in-memory CLOB matching engine. The architecture
keeps byte-level protocol handling, runtime orchestration, matching, risk,
market-data, and control-plane concerns separated so later milestones can fill
in behavior without changing ownership.

## Component Ownership

### `protocol`

- Owns the SBE schema integration, generated codecs, and handwritten Rust facade.
- `protocol/ignition.xml` is the source of truth for wire messages.
- `src/protocol/generated/` contains committed generated Rust and is not edited
  by hand.
- `src/protocol/` exposes stable encode/decode helpers to the rest of the crate.
- Does not own business logic, matching decisions, risk checks, IO, or runtime
  orchestration.

### `gateway`

- Owns frame/session ingress and egress.
- Decodes complete input frames and forwards typed commands to the runtime.
- Encodes outbound events into protocol frames.
- Does not own matching, risk, market-data sequencing, or control-plane policy.

### `runtime`

- Owns the future poll-driven orchestration loop.
- Connects gateway input, control state, risk checks, engine execution,
  market-data output, and protocol egress in explicit phases.
- Does not parse wire bytes directly and does not own order-book internals.

### `engine`

- Owns future order-book state, order lifecycle mutations, and price-time
  priority matching.
- Accepts internal typed commands, not raw protocol buffers.
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

1. gateway receives bytes and decodes protocol frames,
2. runtime translates frames into internal commands,
3. control state and risk gates are observed before matching,
4. engine mutates book state and emits internal events,
5. market-data and execution outputs are sequenced,
6. gateway encodes outbound protocol frames.

Milestone 1 defines this ownership contract and module shell only. Matching,
risk behavior, real transport IO, clients, replay, and benchmarking are later
milestones.
