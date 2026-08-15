# Design Decisions

This document records why Ignition makes its major market-structure,
performance, safety, and operability choices. Detailed mechanics live in
[Architecture](architecture.md), [Matching engine](matching-engine.md),
[Risk and control](risk-and-control.md), and [Wire protocol](protocol.md).

## Continuous CLOB as the Market Model

**Decision.** Use continuous price-time-priority matching for the configured
instrument.

**Why.** A CLOB is appropriate when liquidity is sufficiently continuous,
participants benefit from transparent displayed prices, and the venue needs
immediate deterministic execution. Price-time priority is legible to clients,
produces an auditable ordering rule, and maps naturally to ordered execution
and market-data events. A single-writer CLOB also creates a compact,
deterministic transition history that can feed later settlement or
verification systems.

**Consequence.** Continuous matching rewards speed at the margin and can expose
thin displayed liquidity. The implementation must define queue priority,
self-trade handling, cancel-replace, market residuals, and crossed-order
behaviour precisely rather than treating them as incidental code paths.

### When another primitive is better

A frequent batch auction is preferable when reducing latency races and
discrete-time fairness matters more than continuous execution. Orders collected
over a short interval can clear at one price, reducing the value of being a few
microseconds earlier. It also aligns naturally with systems that settle or
prove state in batches, but introduces auction cadence, imbalance, clearing
price, and information-leakage decisions.

RFQ is preferable for large, bespoke, or illiquid trades where displaying size
would move the market and bilateral price competition is more useful than a
thin public queue. It sacrifices continuous anonymous price discovery and
requires quote eligibility, expiry, disclosure, and dealer-conduct policy.

A hybrid venue can use a CLOB for liquid continuous flow, RFQ for block or
illiquid flow, and scheduled auctions for opening, volatility interruption, or
settlement-aligned clearing. Those mechanisms should produce explicit typed
orders/events rather than add hidden exceptions to the CLOB core.

## One Mutable Owner of the Book

**Decision.** Run matching, account-book mutation, and sequence allocation on a
single writer.

**Why.** Book locks would add contention and scheduler-dependent ordering to
the most latency-sensitive state. One owner makes price-time priority, output
order, risk observations, and replay straightforward. External concurrency is
serialized before matching, where its order becomes explicit.

**Consequence.** One symbol cannot scale across cores by sharing its book.
Scaling should shard independent instruments across pinned single-writer
runtimes and place deterministic session fan-in ahead of them.

## Bounded Paged Ladder and Intrusive Storage

**Decision.** Combine paged direct price addressing, occupancy bitmaps, a
fixed-capacity arena, intrusive price/account lists, a dense account table, and
a pre-sized active-order hash lookup.

**Why.** This gives direct exact-price addressing, a cached top of book,
expected constant-time cancel, FIFO without per-node allocation, and mass
cancel proportional only to the target account's orders. Paging avoids
committing one full `PriceLevel` for every tick in a wide configured range.

**Consequence.** The book has explicit storage and active-page limits. Hash
lookup is expected rather than worst-case constant time, and bitmap traversal
has a small configured bound rather than a universal constant. The full model
and alternatives are in [Matching engine](matching-engine.md).

## No Steady-State Allocation on the Measured Path

**Decision.** Allocate capacities and buffers at startup, then reuse them.

**Why.** Allocator calls introduce variable latency, contention, and hidden
capacity growth. The order-entry path borrows complete frames, decodes into
copyable domain values, publishes through borrowed handles, and reuses encoding
buffers and bounded output rings. Engine pages and order slots come from free
lists. Snapshots stream rows instead of collecting them.

**Consequence.** Ignition does use `Vec`, `Box`, and `HashMap`; the promise is
not “no container types.” They are cold-path allocated and bounded or reused.
The benchmark measures allocator calls around the prepared runtime and reports
zero calls on the current measured path. Capacity exhaustion is an explicit
operational event instead of permission to grow.

## Integer-Scaled Domain Values

**Decision.** Represent prices and quantities as positive `u64` scaled
integers, calculate raw notional in `u128`, use `u32` account IDs, and use
`u16` instrument/asset IDs.

**Why.** Integer arithmetic is exact, deterministic, wire-compatible, and
avoids floating-point rounding in matching and risk. `u128` safely contains a
`u64 × u64` notional product. A `u32` account ID matches the public protocol;
the configured maximum bounds dense account memory. Smaller `u16` reference
identifiers are sufficient for the current instrument/asset namespace and are
loaded once rather than repeated on every order.

**Consequence.** Users must apply configured price and quantity decimals when
displaying values, and raw notional retains both scales. Changing an established
identifier width is a wire compatibility change, not a local optimization.

## SBE Instead of Text Protocols

**Decision.** Use generated SBE codecs with fixed-width messages and an
explicit little-endian header.

**Why.** The wire is compact, bounded, schema-versioned, and decodable into
copyable values without JSON parsing or heap-backed strings. Pure buffer-based
adapters can be replayed, benchmarked, fuzzed, or replaced by accelerated
parsing without changing the domain model.

**Consequence.** Schema evolution is deliberate: generated code is committed,
existing template meanings stay stable, and schema regeneration is an explicit
tooling step. The human contract is in [Wire protocol](protocol.md).

## Separate Gateway, Control, and Market-Data Endpoints

**Decision.** Give order entry, administrative control, and multicast market
data separate adapters and transport capacity.

**Why.** A congested or malformed trading session should not consume the
control listener's connection or output ring. Control is polled first, giving
halt and shutdown requests deterministic priority over the next gateway batch.
Market-data fan-out has different delivery and backpressure needs from
request/response sessions.

**Consequence.** The endpoints still share one process and runtime thread, so
this is isolation of transport resources and phase priority, not independent
failure domains. Production deployment may isolate session gateways and feed
handlers in separate processes while retaining the same typed matcher boundary.

## TCP Sessions and UDP Multicast Market Data

**Decision.** Use nonblocking TCP for control and order entry, and UDP multicast
for market data.

**Why.** TCP provides ordered session delivery and a natural response channel
for commands. Multicast lets one encoded market-data stream reach multiple
consumers without matcher-owned per-subscriber queues.

**Consequence.** TCP head-of-line blocking and client scheduling remain outside
the core latency benchmark. UDP can lose or reorder datagrams; `mdSeq` and
`prevMdSeq` expose gaps, but the current system does not yet provide snapshot
and delta recovery.

## One Runtime Path Across Live, Replay, and Benchmark

**Decision.** Select I/O and metrics adapters at startup, then use the same
component graph and order-processing path in every mode.

**Why.** A replay or benchmark that calls privileged engine methods can miss
framing, normalization, risk, sequencing, and encoding failures. Enum-selected
TCP, file, and in-memory leaves keep those behaviours in scope while changing
only the external mechanism.

**Consequence.** The benchmark intentionally excludes socket and filesystem
cost by choosing in-memory leaves; it is a hot-path service-time benchmark, not
an end-to-end round trip. [Benchmark methodology](../BENCH.md) states the exact
boundary.

## Explicit Input Time and Output Order

**Decision.** Assign one global input sequence to live control/gateway frames,
record it with `recvTs`, and allocate one global `engineSeq` across all logical
outbound events.

**Why.** Same inputs must reconstruct the same inter-endpoint ordering,
timestamps, reports, market data, and control output. Inputs, rather than wall
clock or unordered collection traversal, define the state transition history.

**Consequence.** Replay artifacts include deterministic core configuration and
input metadata in addition to raw SBE frames. A visual book snapshot alone
cannot continue the sequence byte-identically.

## State Before Publication

**Decision.** Apply a mutation completely before publishing the resulting
trade, book state, or report.

**Why.** Every output then describes a coherent post-event state. L2 is an
absolute level value, L1 is computed from the same mutated book, and snapshot
rows cannot observe partially linked orders.

**Consequence.** Output ordering is part of observable semantics and must be
preserved during optimization. Publication errors are surfaced before a later
command can make them easy to overlook.

## Fail Closed at Capacity

**Decision.** Preflight bounded engine resources and perform graceful shutdown
when an incoming command needs unavailable capacity.

**Why.** Runtime allocation, partial insertion, or silently dropped orders are
all worse for deterministic safety. Projection makes the rejection atomic and
accounts for resources released by the same command.

**Consequence.** A capacity event sacrifices availability and currently reuses
`KillSwitchActive` because the established protocol has no dedicated capacity
reason. Production evolution should add explicit capacity telemetry and a
versioned reject/status surface.

## Unsafe Code Is Isolated to Measurement

**Decision.** Keep the matching/runtime library safe Rust. The benchmark binary
uses a small `unsafe` `GlobalAlloc` wrapper that increments counters and
delegates allocation/deallocation to `System`.

**Why.** Process-global allocation measurement requires implementing the
allocator interface. Keeping it in the benchmark entry point prevents
instrumentation from becoming a matcher dependency.

**Consequence.** Allocation counts cover the measured process interval, not a
static proof that allocation is impossible. The implementation remains simple
enough to audit and is exercised outside matching semantics.

## Current Limitations and Evolution

The most important limitation is the single-symbol, in-memory deployment with
one active trading and one active control client. It demonstrates deterministic
matching and operational boundaries, not a complete venue edge.

The natural evolution is:

1. add authenticated multi-session gateways that assign deterministic ingress
   order and account identity;
2. shard symbols across independent single-writer runtimes;
3. add a versioned checkpoint containing complete continuation state and
   validate restore against the ordered log;
4. add L2 snapshot-plus-delta market-data recovery;
5. measure transport-inclusive latency before choosing busy polling,
   kernel-bypass, CPU isolation, NUMA placement, or FPGA parsing;
6. expose operational metrics and capacity alerts without inserting dynamic
   work into the matching path.

These are intentionally deferred capabilities, not partially implemented
features.
