# Risk and Control

Ignition separates pre-match financial policy from operational control. Risk
reads projected book state and returns a typed decision without mutation.
Control owns halt, shutdown, and heartbeat state. The runtime observes both
before allowing any risk-increasing command to reach matching.

## Contents

- [Gate ordering](#gate-ordering)
- [Account exposure](#account-exposure)
- [Price band](#price-band)
- [Self-trade prevention](#self-trade-prevention)
- [Policy ownership and rejections](#policy-ownership-and-rejections)
- [Kill switch](#kill-switch)
- [Heartbeat](#heartbeat)
- [Inspection snapshot](#inspection-snapshot)
- [Configuration](#configuration)

## Gate Ordering

For `NewOrder` and `CancelReplace`, the runtime applies:

```text
decoded command
→ control/heartbeat gate
→ read-only risk projection
→ risk decision
→ engine capacity preflight
→ matching and book mutation
→ output publication
```

Control is polled before gateway on every runtime iteration. The gate is also
checked immediately before risk and matching, satisfying the requirement that
a kill switch be observed at the matching boundary rather than only at
network ingress.

`CancelOrder` and `MassCancel` bypass the risk-increasing control and risk
checks. Operators and accounts can therefore reduce exposure while trading is
halted or heartbeat liveness has failed.

## Account Exposure

Risk limits describe live resting exposure, not balances or positions.

### Open-order count

`max_open_orders_per_account` limits the projected number of orders that would
remain resting after the command. A fully executable GTC, an IOC, and a market
order add no resting order. A cancel-replace subtracts the original before
adding the replacement's projected residual.

An excess is rejected with `RiskOpenOrderLimit` before book mutation.

### Open notional

Open notional is the sum of unsigned raw integer products:

```text
open_notional = Σ(resting_price_integer × resting_remaining_qty_integer)
```

The sum uses `u128` so multiplying two `u64` wire values cannot overflow the
intermediate. It intentionally tracks resting order exposure, not currency
conversion or filled position. For replace, the old resting notional is removed
before the projected residual is added.

An excess over `max_open_notional_per_account` is rejected with
`RiskNotionalLimit`.

## Price Band

The fat-finger band is inclusive and expressed in basis points around a
deterministic reference:

```text
lower = reference × (10,000 - price_band_bps) / 10,000
upper = reference × (10,000 + price_band_bps) / 10,000
```

The configured `startup_reference_price` is used until the first execution.
After each order finishes publishing its trades, the latest emitted trade price
becomes the next reference.

Limit and replacement prices are checked directly. Risk also projects the
lowest and highest executable maker prices, so a market order or a marketable
limit cannot sweep outside the band merely because its submitted price is
absent or still inside it. A breach is rejected with `PriceBand`.

## Self-Trade Prevention

The implemented policy is cancel-aggressive. Before mutation, risk projects
the incoming order across visible executable liquidity. If any maker belongs
to the same account, the entire incoming command is rejected with
`SelfTradePrevention`; resting liquidity is preserved and no trade or book
update is emitted.

Post-only orders that would cross are excluded from STP rejection because the
engine cancels them without executing. STP is account-based, so matching client
order IDs across different accounts are unrelated.

## Policy Ownership and Rejections

| Owner | Examples |
| --- | --- |
| Protocol/normalization | malformed frame, wrong template, unsupported schema/version, invalid enum, zero or misaligned scalar |
| Control | `KillSwitchActive`, `HeartbeatLost` for new/replace |
| Risk | `RiskOpenOrderLimit`, `RiskNotionalLimit`, `PriceBand`, `SelfTradePrevention` |
| Engine lifecycle | duplicate account-scoped ID, unknown cancel/replace, invalid account, price outside storage envelope |
| Engine capacity | atomic preflight failure followed by graceful shutdown |

Risk deliberately passes unknown cancels/replaces, duplicate IDs, and invalid
accounts through to the engine so the lifecycle owner produces the appropriate
structural report. Every command-level rejection receives the shared global
`engineSeq` and does not mutate the book.

## Kill Switch

The control endpoint supports three actions:

| Action | Effect |
| --- | --- |
| `HaltTrading` | Reject subsequent new orders and replacements; retain the book; continue cancels, mass cancels, snapshots, heartbeat, and resume |
| `ResumeTrading` | Clear a prior halt; normal gating resumes |
| `Shutdown` | Acknowledge the request, stop the control batch, deterministically drain all resting orders, flush all sinks, and terminate |

Shutdown is terminal. The drain emits cancellation reports and book updates in
deterministic book order. No later frame from the same already-buffered control
batch is applied.

### Capacity exhaustion

`max_resting_orders` and `max_active_pages` are operational safety bounds. A new
or priority-losing replacement that cannot be represented is rejected before
mutation using the existing `KillSwitchActive` reason. The runtime then stops
the gateway batch, drains the book, flushes output, and terminates. Commands
that need no new storage, such as cancels and fully executable/IOC flow, remain
valid while capacity is full.

This fail-closed policy avoids allocator growth or a partially linked order at
the cost of process availability. A production system would likely expose a
dedicated capacity status/reject code and alerting path.

## Heartbeat

The control client sends monotonically numbered `Heartbeat` requests and
receives `HeartbeatStatus`. With `heartbeat_timeout_ns > 0`, liveness
enforcement begins only after the first heartbeat; endpoint construction time
does not start the deadline. The next new order or replacement whose recorded
receive timestamp is beyond the timeout is rejected with `HeartbeatLost`.

`heartbeat_timeout_ns: 0` disables heartbeat gating. Cancels remain available
after a timeout. Receive timestamps are runtime-assigned in live mode and
recorded for deterministic replay.

## Inspection Snapshot

A `Snapshot` request streams:

1. `SnapshotBegin` with top of book, resting-order count, last market-data
   sequence, next trade ID, and active risk reference price;
2. one `SnapshotOrder` per resting order;
3. `SnapshotEnd` with the emitted row count.

Rows are bids best-to-worst with FIFO inside each level, then asks
best-to-worst with FIFO inside each level. They stream directly from the engine
without building a snapshot-sized collection. Each row consumes a global
sequence, and the terminal count lets the client detect truncation.

This is an inspection snapshot, not a checkpoint or replay seed. It omits the
input cursor, next global sequence, kill-switch and heartbeat state, complete
configuration identity, and other continuation state required for
byte-identical restoration. [Replay](replay.md) starts from recorded inputs and
their captured core configuration instead.

## Configuration

Deterministic policy is loaded once from [`configs/server.yaml`](../configs/server.yaml):

```yaml
risk:
  max_open_orders_per_account: 1000
  max_open_notional_per_account: 100000000000000000
  startup_reference_price: 6500000
  price_band_bps: 1000
runtime:
  control:
    heartbeat_timeout_ns: 0
    max_messages_per_poll: 1024
```

With the default two price decimals, `startup_reference_price: 6500000` is
`$65,000.00`; `price_band_bps: 1000` is ±10%. Open notional remains raw scaled
integer notional and must be interpreted with both price and quantity scales.

Configuration is validated and normalized at startup. It is not read from the
environment or filesystem inside the runtime loop. Recorded replay manifests
embed these deterministic values so later configuration edits do not alter a
historical run.
