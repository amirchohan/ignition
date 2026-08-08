# Milestones

This document breaks the worktest brief into implementation milestones for a
coherent submission. The goal is not maximum feature coverage; it is a small,
serious exchange-style CLOB engine with deterministic behavior, clear
boundaries, measurable latency, and honest documentation.

## Milestone 1: Architecture And Protocol Contract

- Define the server/client split: the engine runtime consumes binary input
  frames and emits binary output frames.
- Keep input source selection outside the engine. File replay, benchmark
  workloads, scripted CLI commands, and future socket clients should all feed
  the same protocol path.
- Establish a bespoke but SBE-like frame format with a fixed header, message
  type, version, length, documented endianness, and fixed-width integer fields.
- Keep protocol encode/decode functions pure and side-effect-free so they are
  easy to test, replay, benchmark, and fuzz later.
- Define the outbound message categories up front: execution reports, trade
  prints, book updates, control replies, heartbeat/status messages, and errors.
- Add metrics/event hook points as Noop-ready extension points so later
  OpenMetrics or Prometheus export does not require rewiring the hot path.

## Milestone 2: Deterministic Core Model

- Define the internal domain model: order IDs, account IDs, side, price,
  quantity, order type, TIF, order state, reject reasons, sequence numbers,
  trades, execution reports, and book updates.
- Lock the single-symbol scope, tick size, lot size, and integer scaling rules.
- Keep prices and quantities as integers in all core logic; floats do not belong
  in the matching or risk path.
- Use injected deterministic engine timestamps and sequence assignment.
- Keep wall-clock reads, randomness, and unordered iteration out of the matching
  core and any output-producing path.
- Make byte-identical replay an architectural constraint from the start, not a
  late testing feature.

## Milestone 3: Matching Engine

- Implement price-time priority with strict FIFO ordering within each price
  level.
- Support limit and market orders.
- Support GTC and IOC as required TIFs; treat POST_ONLY as an optional plus if
  it fits cleanly.
- Let aggressive orders walk the book across price levels until filled or
  stopped by TIF and liquidity constraints.
- Support CancelOrder, CancelReplace, and MassCancel.
- Document cancel-replace queue-priority behavior before relying on it.
- Document market-order behavior in a thin book before relying on it.
- Emit deterministic execution reports and trade events from the same output
  path used by replay and clients.

## Milestone 4: Market Data Output, L2-Ready

- Emit a top-of-book update on every book change.
- Emit a trade print on every execution.
- Include monotonic market-data sequence numbers and gap-detection fields.
- Centralize book mutation eventing so add, cancel, replace, fill, and mass
  cancel operations all pass through consistent market-data output logic.
- Design the event model so L2 incremental updates can be added later without
  rewriting the matching engine.
- Keep full L2 snapshot and delta recovery as stretch unless the required slice
  is already complete.

## Milestone 5: Risk And Control Plane

- Add per-account maximum open-order count checks.
- Add per-account maximum notional exposure checks.
- Add price-band or fat-finger checks against a documented reference price.
- Add self-trade prevention with a documented policy.
- Add a kill switch that is observed before matching, not only at ingress.
- Add Snapshot control behavior for inspecting or seeding book and engine state.
- Define heartbeat messages for the control plane.
- Treat missed control-plane heartbeat as visible fail-fast behavior. The exact
  policy remains a later design decision: halt matching, reject new orders,
  drain and exit, or terminate immediately.

## Milestone 6: Client Tooling

- Build client tooling around the same binary protocol used by the engine
  runtime.
- Support file-driven input for deterministic replay and benchmark workloads.
- Support scripted or interactive CLI commands for NewOrder, CancelOrder,
  CancelReplace, MassCancel, KillSwitch, Snapshot, and heartbeat messages.
- Keep order-entry and control-plane clients allowed to share protocol code even
  if they become separate binaries.
- Treat clients as adapters from input sources to protocol frames; the engine
  runtime should only read frames and write frames.
- Ensure benchmark, replay, and manual operation exercise the same protocol
  boundary rather than privileged internal Rust APIs.

## Milestone 7: Replay And Recorded Streams

- Record ordered binary input frames.
- Replay recorded frames through the same runtime and protocol path used by
  clients.
- Prove byte-identical execution report and market-data output for the same
  ordered input stream.
- Include deterministic engine-assigned timestamps in replay, or document and
  implement an explicit timestamp-exclusion mode.
- Add a checked-in sample input stream.
- Document the replay command and expected output behavior.

## Milestone 8: Benchmark And Tail-Latency Evidence

- Benchmark the hot path from ingress frame received to execution report
  emitted.
- Report p50, p99, p99.9, and p99.99 latency; averages alone are not useful for
  this project.
- Report sustained throughput under a burst workload.
- Include an allocation signal for the hot path, aiming for zero or near-zero
  allocations once the path is warm.
- Use file/client-driven workloads so benchmark traffic exercises the real
  protocol path.
- Include warmup and methodology notes.
- Leave CPU pinning, NUMA placement, huge pages, and deeper ingress optimization
  as stretch toggles unless the required slice is already complete.

## Milestone 9: Submission Documentation

- Write a README that covers architecture, protocol framing, message layout,
  endianness, run commands, replay commands, benchmark commands, matching rules,
  STP policy, cancel-replace policy, risk controls, assumptions, and tradeoffs.
- Write BENCH.md with workload shape, benchmark methodology, observed tail
  latency numbers, throughput, allocation notes, and a short interpretation of
  what drives the tail.
- Include the required microstructure write-up: why a CLOB is appropriate here,
  when FBA, RFQ, or a hybrid would be better, and one honest limitation of the
  current design.
- Keep optional stretch features clearly labeled so the submission reads as a
  coherent slice rather than unfinished scope.

## Stretch Items To Design In Early

- L2-ready market-data eventing, even if v1 emits only top-of-book.
- SBE-like bespoke framing, even if the protocol stays small.
- Noop metrics/event hooks, even if Prometheus export comes later.
- Pure protocol decoders, so a future conformance fuzzer is straightforward.
- Replay-first determinism, because retrofitting it after the core uses
  wall-clock time or unordered output is expensive.

## Stretch Items To Defer

- Full L2 snapshot and delta recovery.
- Prometheus or OpenMetrics export.
- CPU pinning, NUMA-aware placement, and huge page setup.
- Deep ingress optimization beyond clean fixed-frame parsing and reusable
  buffers.
- A conformance fuzzer target.
- Frequent batch auction mode.
- FIX, ITCH, OUCH, or full SBE compatibility.
