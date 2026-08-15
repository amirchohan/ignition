# Order Book Data Structure

## Chosen Design

Ignition uses a bounded, paged `SideLadder` for each side of the book.

The book has a fixed storage envelope: `storage_min_price..=storage_max_price`.
These values come from `configs/server.yaml`. This is not a risk price band and
does not move with the market. It is the finite address space required for
direct tick indexing without trees or price hashing.

Each side maps prices to levels as:

```text
price -> absolute tick -> page index -> offset within page -> PriceLevel
```

Pages are fixed-size dense mini-ladders. A page-level bitmap tracks which pages
contain liquidity, and each page has a level bitmap tracking which price levels
inside that page are non-empty. Best bid/ask discovery uses these bitmaps rather
than scanning empty price levels.

Each `PriceLevel` stores only FIFO metadata and aggregate quantity. Resting
orders live in a shared arena and are linked into two intrusive lists:

- a FIFO doubly linked list for price-time priority at the price level
- a per-account doubly linked list for efficient mass cancel

Each `RestingOrder` stores its side. The order already belongs to one
`SideLadder`, but keeping side on the order avoids a parallel side index and
lets fill, cancel, and free paths update the correct ladder from a single arena
handle.

Client order IDs are unique only among live orders within one account. The
dense `AccountTable` therefore owns one preallocated
`(account_id, client_order_id) -> OrderHandle` map. It does not allocate a map
per configured account. IDs may coexist across accounts and may be reused by an
account after the previous order leaves the book.

Cancels and cancel-replaces physically unlink orders immediately. The book does
not use lazy cancellation as the default because lazy cleanup pushes
cancellation work into later aggressive matching and can create tail-latency
spikes.

The public engine boundary accepts caller-owned market-data state and an
execution-report callback: `Engine::execute(entry, sequencer, market_data,
emit_report)`. The engine emits directly into caller-owned sinks instead of
allocating a fresh result vector for every command.

## Why This Design

This gives predictable hot-path behavior:

- Direct price lookup without `BTreeMap` traversal.
- No hash computation for price-level access.
- No scan across empty price ticks when moving best bid/ask.
- Strict FIFO at each price level.
- Constant-time arbitrary cancel after `(account_id, order_id) -> handle` lookup.
- Mass cancel walks only the target account's active orders.
- Caller-owned output sinks keep normal command execution allocation-free once
  endpoint buffers and book storage are preallocated.
- GTC orders remain valid inside the fixed storage envelope even if risk price
  bands move later.
- Resting insertion preflights the shared order arena and side-local page pool
  before reserving either resource, so capacity pressure cannot leak an arena
  slot or partially link an order.

Capacity projection considers the command's actual storage requirement. Market
and IOC orders never need a resting slot; fully executable GTC orders do not
either. A crossing GTC with residual quantity may reuse a slot released by a
fully consumed maker. Priority-losing replacements also account for the
original slot and for a source page that becomes empty when the original is
removed.

The dynamic price band/fat-finger policy belongs in the risk/control layer, not
inside the matching data structure. The matching engine only rejects prices
outside its fixed storage envelope because they cannot be represented by this
book instance.

## Complexity

Let:

```text
F = maker orders filled
L = non-empty price levels depleted/crossed
A = active orders for one account
E = emitted events/reports
```

Expected costs:

```text
new passive limit: O(1)
new aggressive limit/market: O(F + L + E)
cancel order: O(1)
cancel-replace: O(1) for unlink/rest path, plus matching cost if crossing
mass cancel: O(A + E)
best bid/ask lookup: O(1) over the configured storage envelope via bitmaps
top-of-book read: O(1)
snapshot: O(active levels + active orders), off hot path
```

Matching cannot be constant-time independent of fills because every maker fill
must mutate state and emit deterministic output. The goal is no unrelated work:
no searching through unrelated orders, no tree walk, no price hash path, and no
linear scan across empty prices.

## Alternatives Considered

### `BTreeMap<Price, PriceLevel>`

A `BTreeMap` naturally supports ordered price discovery and sparse books, but
price lookup, insertion, and next-level traversal are `O(log n)`. It is also
pointer-heavy compared with an indexed ladder. This is simple and defensible for
a general-purpose book, but it does not match the constant-time low-latency
goal.

### `HashMap<Price, PriceLevel>`

A hash map gives average `O(1)` exact price lookup, but it does not solve ordered
best-price discovery. The book would need a second ordered structure for best
bid/ask and next executable level. Hashing prices, resize policy, lazy cleanup,
and poorer locality also make tail latency less predictable.
