# Design Decisions

This document records why Ignition makes its major market-structure,
performance, and operability choices. Detailed mechanics live in
[Architecture](architecture.md), [Matching engine](matching-engine.md),
[Risk and control](risk-and-control.md), and [Wire protocol](protocol.md).

## Contents

- [Decision summary](#decision-summary)
- [Matching core](#matching-core)
- [Hot-path representation](#hot-path-representation)
- [Runtime boundaries and determinism](#runtime-boundaries-and-determinism)
- [Market-structure choice](#market-structure-choice)
- [Current limitations and evolution](#current-limitations-and-evolution)

## Decision Summary

This table is the short version. The sections below are supporting rationale
for readers who want to inspect a particular tradeoff.

| Area | Decision | Optimizes for | Main tradeoff |
| --- | --- | --- | --- |
| Matching ownership | One writer per instrument | Lock-free ordering and replayable transitions | Scale by sharding instruments rather than one book across cores |
| Order book | Paged ladder, bitmaps, arena, and intrusive lists | Expected `O(1)` common operations and cache locality | Fixed price and capacity bounds |
| Allocation | Startup allocation plus callbacks and borrowed publishers | Zero observed steady-state allocator calls | Capacity cannot grow invisibly at runtime |
| Scalars | Smallest practical integer width per field | Compact frames and more hot data per cache line | Width changes become compatibility decisions |
| Protocol | Fixed-width SBE in little-endian order | Bounded parsing and native x86-64 integer order | Explicit schema evolution and generated codecs |
| Endpoints | Separate gateway, control, and multicast feeds | Control priority and transport-resource isolation | Still one process and failure domain |
| Runtime modes | Same processing path for live, replay, and benchmark | Comparable behaviour and deterministic evidence | Transport is excluded only through leaf adapters |
| Sequencing | Recorded input order and one global outbound sequence | Byte-identical replay and auditable publication | Inspection snapshots cannot resume execution |
| Market model | Continuous price-time-priority CLOB | Transparent, immediate, deterministic execution | Rewards speed and assumes continuous liquidity |

---
## Matching Core

### One Mutable Owner of the Book

**Decision.** Run matching, account-book mutation, and sequence allocation on a
single writer.

**Why.** Book locks would add contention and scheduler-dependent ordering to
the most latency-sensitive state. One owner makes price-time priority, output
order, risk observations, and replay straightforward. External concurrency is
serialized before matching, where its order becomes explicit.

**Consequence.** One symbol cannot scale across cores by sharing its book.
Scaling should shard independent instruments across pinned single-writer
runtimes and place deterministic session fan-in ahead of them.

### O(1)-Oriented Bounded Book Storage

**Decision.** Combine paged direct price addressing, occupancy bitmaps, a
fixed-capacity arena, intrusive price/account lists, a dense account table, and
a pre-sized active-order hash lookup.

**Why.** The common operations should not pay an ordered-tree traversal. Direct
price-to-page mapping and a cached best quote make exact-level access and
top-of-book reads constant time. The active-order hash gives expected `O(1)`
identity lookup; intrusive links then make cancel and same-price quantity
reduction expected `O(1)` without searching a price queue. Matching visits only
real fills and occupied levels, while mass cancel visits only the target
account's orders.

A `BTreeMap<Price, PriceLevel>` would be considerably simpler and would support
an unbounded sparse price domain, but exact lookup, insertion, and next-level
navigation would cost `O(log n)` and touch pointer-heavy tree nodes. A price
hash map would make exact lookup expected `O(1)` but still require a second
ordered structure. The chosen design deliberately spends startup memory and
configuration complexity to remove those costs from normal order flow.

**Consequence.** Efficiency depends on explicit bounds:

- `storage_min_price..=storage_max_price` defines the addressable tick window;
- `max_active_pages` bounds simultaneously committed price pages per side;
- `max_resting_orders` bounds arena and active-order lookup entries;
- `max_account_id` bounds the dense account table.

The price window is static book storage, not the risk reference. Risk starts
from a configured reference price (a cold-start substitute for a live mid/last
trade) and then follows the latest execution. A deployment must choose both the
storage envelope and starting reference sensibly for its instrument. In return,
normal operations avoid tree growth and per-level allocation. Hash lookup is
expected rather than worst-case constant time, and bitmap traversal has a small
configured bound rather than a universal constant. The full model and
alternatives are in [Matching engine](matching-engine.md).
---
## Hot-Path Representation

### No Steady-State Allocation on the Measured Path

**Decision.** Allocate capacities and buffers at startup, then reuse them.

**Why.** Allocator calls introduce variable latency, contention, and hidden
capacity growth. The order-entry path borrows complete frames, decodes into
copyable domain values, and immediately invokes the next component through a
callback. Execution reports use caller-provided callbacks; gateway, control,
and market-data output use borrowed publishers and reusable encoding buffers.
No order-entry command builds a result `Vec`, stages work in a `VecDeque`, or
transfers ownership through a heap-backed per-message queue. Engine pages and
order slots come from free lists, and snapshots stream rows through a visitor
callback instead of first materialising the book.

The callback flow also makes determinism structural: processing and publication
happen in the explicit call order, rather than relying on later iteration over
an intermediate collection whose ordering or drain policy could change.

**Consequence.** Ignition does use `Vec`, `Box`, and `HashMap` as
startup-allocated backing storage; saying that the codebase contains no vectors
would be incorrect. The precise property is that the steady-state order path
does not construct or grow them. The benchmark measures allocator calls around
the prepared runtime and reports zero calls on the current measured path.
The terminal full-book shutdown drain is an off-hot-path exception: it collects
stable arena handles before unlinking them. Capacity exhaustion is an explicit
operational event instead of permission to grow during normal order flow.

### Deliberate Integer Widths

**Decision.** Choose each public scalar for its required range instead of using
`u64` by default, while retaining exact integer-scaled prices and quantities.

| Value | Wire/arithmetic width | Reason |
| --- | ---: | --- |
| Side, order type, TIF, status, and reason discriminants | `u8` | Small closed enumerations |
| Instrument and asset IDs | `u16` | Sufficient configured reference-data namespace |
| Account ID, control request ID, heartbeat sequence | `u32` | Larger operational namespace without doubling every field to 64 bits |
| Order ID, price, quantity, timestamps, and sequences | `u64` | Long-lived counters or financial ranges where 32 bits is insufficient |
| Raw open notional | `u128` | Exact overflow-safe product and sum of `u64` price × quantity |

**Why.** Narrow fields reduce SBE frame size and the amount of data copied,
decoded, and retained in hot structures. More identifiers and order metadata
fit in a cache line than they would under an indiscriminate all-`u64` model,
improving cache and memory-bandwidth efficiency. Integer arithmetic is exact
and deterministic, while `u128` is used only where the arithmetic range—not
convenience—requires it. Rust struct padding can reduce some in-memory savings,
so wire size and measured layouts matter more than merely counting field widths.

**Consequence.** Users must apply configured price and quantity decimals when
displaying values, and raw notional retains both scales. Changing an established
identifier width is a wire compatibility change, not a local optimization.

### SBE and Little-Endian Wire Order

**Decision.** Use generated SBE codecs with fixed-width messages and define
every multi-byte wire field as little-endian.

**Why.** The wire is compact, bounded, schema-versioned, and decodable into
copyable values without JSON parsing or heap-backed strings. Little-endian
matches the native byte order of the Linux x86-64 deployment target, avoiding a
byte swap for each ordinary integer encode/decode while remaining explicit and
portable in the schema. It is also a conventional SBE deployment choice and is
supported directly by generated codecs. Pure buffer-based adapters can be
replayed, benchmarked, fuzzed, or replaced by accelerated parsing without
changing the domain model.

**Consequence.** Code must use the schema codecs or explicit little-endian
operations rather than transmuting native Rust structs. A big-endian host would
perform conversions but preserve identical wire bytes. Schema evolution is
deliberate: generated code is committed, existing template meanings stay
stable, and schema regeneration is an explicit tooling step. The human contract
is in [Wire protocol](protocol.md).
---
## Runtime Boundaries and Determinism

### Separate Gateway, Control, and Market-Data Endpoints

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

### TCP Sessions and UDP Multicast Market Data

**Decision.** Use nonblocking TCP for control and order entry, and UDP multicast
for market data.

**Why.** TCP provides ordered session delivery and a natural response channel
for commands. Multicast lets one encoded market-data stream reach multiple
consumers without matcher-owned per-subscriber queues.

**Consequence.** TCP head-of-line blocking and client scheduling remain outside
the core latency benchmark. UDP can lose or reorder datagrams; `mdSeq` and
`prevMdSeq` expose gaps, but the current system does not yet provide snapshot
and delta recovery.

### One Runtime Path Across Live, Replay, and Benchmark

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

### Explicit Input Time and Output Order

**Decision.** Assign one global input sequence to live control/gateway frames,
record it with `recvTs`, and allocate one global `engineSeq` across all logical
outbound events.

**Why.** Same inputs must reconstruct the same inter-endpoint ordering,
timestamps, reports, market data, and control output. Inputs, rather than wall
clock or unordered collection traversal, define the state transition history.

**Consequence.** Replay artifacts include deterministic core configuration and
input metadata in addition to raw SBE frames. A visual book snapshot alone
cannot continue the sequence byte-identically.
---
## Market-Structure Choice

### Continuous CLOB as the Market Model

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

#### When another primitive is better

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
---
## Current Limitations and Evolution

### Single instrument per matcher

One runtime intentionally owns one instrument. This keeps the book, sequence,
reference price, and matching state local to one writer and provides a simple
horizontal scaling model: assign instruments to independent matcher instances
and add capacity by adding shards.

The tradeoff is cross-instrument balance and exposure. A matcher cannot decide
from local state whether an account has already committed the same quote balance
on another symbol. The clean production boundary is a sequenced account-risk
or credit-reservation service ahead of the matchers. It would own global
balances, atomically reserve credit for an accepted command, and issue a
reservation ID or per-shard budget with the order. Each matcher could retain
constant-time local checks against its granted budget; fills, cancels, and
rejects would deterministically consume or release reservations back to the
global ledger. Pre-allocating bounded credit to shards avoids putting a remote
round trip inside every matching decision, while reconciliation and sequence
numbers make recovery auditable.

### One active TCP trading session

The live adapter intentionally accepts one trading connection. A production
venue would normally place an order-entry/session gateway in front of the
matcher to own authentication, account binding, permissions, throttling,
session recovery, protocol negotiation, and deterministic multi-client fan-in.
The matcher would still receive one canonical ordered stream from that layer.

Supporting multiple direct TCP clients here would duplicate part of that
gateway, force an early fairness/ordering policy, and add connection-level work
without improving matching semantics. Within the bounded implementation
timebox, one ingress was therefore the sharper boundary. The same reasoning
applies independently to the single active control connection.

### Other deferred capabilities

State is in memory and market data currently detects gaps without recovering
them. The natural evolution is:

1. add a versioned checkpoint containing complete continuation state and
   validate restore against the ordered log;
2. add L2 snapshot-plus-delta market-data recovery;
3. measure transport-inclusive latency before choosing busy polling,
   kernel-bypass, CPU isolation, NUMA placement, or FPGA parsing;
4. expose operational metrics and capacity alerts without inserting dynamic
   work into the matching path.

These are intentionally deferred capabilities, not partially implemented
features.
