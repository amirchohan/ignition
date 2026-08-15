# Matching Behavior

The single-symbol matching core consumes one normalized `OrderEntry` at a time,
emits deterministic `ExecutionReport` values through a caller-provided
callback, and writes market data through the caller-owned `MarketDataRuntime`.

## Priority

Matching uses price-time priority. The best opposite price executes first, and
orders at the same price execute in FIFO order. Resting orders are physically
unlinked when canceled, replaced with lost priority, mass-canceled, or fully
filled.

## New Orders

Limit orders may match immediately if they cross the opposite best price. Any
remaining GTC quantity rests at its limit price. If a GTC order partially fills
and then rests, the final taker trade report remains `PartiallyFilled` with the
live remaining quantity; the engine does not emit a separate accepted report for
the residual.

Market orders walk available opposite liquidity until filled or until the book
runs out. Market orders never rest. If a market order receives no fill, it
expires with `ExecType::Expired` and `OrdStatus::Expired`. If it partially fills
and the residual cannot execute, the final taker trade report is marked
`OrdStatus::Expired` and its remaining quantity is set to zero.

IOC orders use the same residual behavior as market orders: executable quantity
trades immediately and any remainder expires instead of resting.

Post-only limit orders are accepted only if they do not cross the opposite best
price. A would-cross post-only order is canceled without execution.

## Risk Checks

The runtime applies risk before matching. Risk rejects emit execution reports
with `ExecType::Rejected` / `OrdStatus::Rejected` and do not mutate the book.

Per-account open-order and notional limits are based on live resting book state,
not account balances. Open notional is tracked as `price_integer * qty_integer`
for resting quantity. GTC limit orders are checked using their projected
post-match residual. Market and IOC orders never rest, so they do not add open
order count or open notional.

Price-band checks use a configured startup reference price until the first
trade, then the latest trade price. The band is inclusive. Limit orders are
checked against their submitted limit price; projected executable prices are
also checked so market orders cannot sweep outside the band.

Self-trade prevention uses a cancel-aggressive policy. If an incoming matching
order would execute against resting liquidity from the same account, the
incoming command is rejected with `SelfTradePrevention` before any trade or book
mutation occurs. Resting orders are preserved.

Configured book-capacity exhaustion uses the existing operational kill-switch
surface rather than adding a public reject code. Before a new order or
priority-losing replacement mutates the book, the engine projects whether its
residual needs a resting-order slot and ladder page. If either bounded resource
is unavailable, the command is rejected with `KillSwitchActive`; the runtime
then drains resting orders, flushes outputs, and terminates gracefully. The
triggering command itself emits no trades or market-data mutations.

## Cancels

Live client order IDs are scoped by account. Two accounts may use the same
numeric ID simultaneously; duplicate IDs are rejected only when the same
account already has that ID resting. A cancel or replace resolves the original
using both `account_id` and `orig_client_order_id`.

`CancelOrder` removes the original resting order when it is active for the same
account. Unknown orders, wrong-account cancels, and structurally invalid cancel
requests are rejected.

`MassCancel` walks the account's active-order chain and cancels each resting
order for that account. It emits one cancel report per removed order, followed
by a terminal cancel report for the mass-cancel command itself.

## Cancel-Replace

Cancel-replace follows exchange-style queue-priority rules:

- Same price with equal or lower quantity preserves FIFO priority.
- Price changes lose priority.
- Quantity increases lose priority.

When priority is preserved, the existing arena node is updated in place and the
engine emits `ExecType::Replaced` with `OrdStatus::Replaced`. When priority is
lost, the original order is canceled and the replacement re-enters as a new GTC
limit order.

## Reports

Execution reports separate event type from order status:

- `ExecType::New` / `OrdStatus::New` for passive accepted orders.
- `ExecType::Trade` with `PartiallyFilled`, `Filled`, or terminal `Expired`
  status for executions.
- `ExecType::Canceled` / `OrdStatus::Canceled` for explicit cancels,
  mass-cancel removals, and would-cross post-only orders.
- `ExecType::Replaced` / `OrdStatus::Replaced` for priority-preserving
  cancel-replace.
- `ExecType::Rejected` / `OrdStatus::Rejected` for rejected commands.
- `ExecType::Expired` / `OrdStatus::Expired` for market or IOC orders that do
  not execute at all.

Every fill emits a trade print plus maker and taker execution reports with the
actual execution price and quantity. Reports without an execution have no fill
price and zero fill quantity.

## Complexity

Matching cost is proportional to real work: filled maker orders, crossed
non-empty price levels, and emitted events. The ladder avoids tree walks, price
hashing, and scans across empty ticks. Storage details and operation complexity
are documented in `docs/datastructure.md`.
