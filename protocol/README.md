# Ignition Protocol

Ignition uses Simple Binary Encoding (SBE) for its control and trading endpoint
protocols. The schema is shared, but ingress/egress adapters are bucketed by
endpoint so control traffic can use separate sockets and queues from the trading
hot path.

## Source Of Truth

- `protocol/ignition.xml` is the schema source of truth.
- Generated Rust codecs are committed under `protocol/generated/`.
- Generated files must not be edited by hand.
- Handwritten protocol integration lives under `src/protocol/`.
- `src/protocol/wire/control.rs` owns the control endpoint adapter.
- `src/protocol/wire/trading/` owns the trading endpoint adapter.

## Build Policy

Normal `cargo build` and `cargo test` should not require Java or SBE tooling.
When the XML schema changes, regenerate codecs explicitly with:

```bash
scripts/generate_sbe.sh
```

The generation script downloads or uses the pinned `sbe-all` tool and writes
Rust output into `protocol/generated/`.

## Scope

The current schema is scoped to exactly one configured symbol. Order-entry
messages do not carry a symbol field; all order flow in a session is for the
configured runtime symbol. Multi-symbol support is a future schema/version
change.

The configured instrument lives in `../configs/server.yaml` under
`reference_data.instrument`. The wire protocol carries only integer price and
quantity fields; `price_decimals`, `qty_decimals`, `price_tick`, and `qty_lot`
define the per-instrument scale and minimum legal increments. Parsed core
state uses numeric IDs only: no symbol or asset strings enter the matching
engine. `instrument_id`, `base_asset_id`, and `quote_asset_id` are `u16` in
the domain/config model.

Every transport enforces `MAX_ACCEPTED_WIRE_FRAME_LEN`, a documented 4 KiB
safety envelope. The largest current fixed-width schema frame is 92 bytes; the
larger envelope preserves bounded extension headroom. TCP's separate 4 KiB
read chunk is a syscall-batching choice and may contain partial or multiple
frames; it is not the wire-frame limit.

Prices and quantities are unsigned integer-scaled values. Zero is not a valid
domain `Price` or `Qty`; outbound optional price fields use wire value `0` as
the current no-price sentinel.

`clientOrderId` is scoped by `accountId` while an order is live. Cancel and
replace requests resolve `origClientOrderId` within the supplied account; the
same numeric client order ID may therefore be active in different accounts.

## Endpoint Model

The protocol is exposed as two ingress endpoint families and three egress
endpoint families:

- Control ingress decodes `KillSwitch`, `Snapshot`, and `Heartbeat` into
  `domain::control::ControlRequest`.
- Trading ingress decodes `NewOrder`, `CancelOrder`, `CancelReplace`, and
  `MassCancel` into `domain::trading::order::OrderEntry`.
- Control egress encodes `domain::control::ControlEvent`.
- Order-entry egress encodes `domain::trading::order::ExecutionReport` and
  order-entry `ProtocolError` responses on the trading TCP connection.
- Market-data egress encodes `domain::trading::md::MarketDataEvent` on the
  multicast UDP feed.

A known template sent to the wrong endpoint is rejected as an unexpected
template. Unknown template IDs are still rejected as unknown templates.

## Message Families

- Control requests: `KillSwitch`, `Snapshot`, `Heartbeat`
- Control events: `ControlResponse`, `HeartbeatStatus`, `SnapshotBegin`,
  `SnapshotOrder`, `SnapshotEnd`, `ProtocolError`
- Order entry: `NewOrder`, `CancelOrder`, `CancelReplace`, `MassCancel`
- Trading events: `ExecutionReport`, `TradePrint`, `L1Update`, `L2Update`,
  `ProtocolError`

## Market Data

`TradePrint` reports each execution with a market-data sequence, global engine
sequence, trade ID, maker order ID, taker order ID, price, and quantity. Maker
and taker client order IDs may have the same numeric value when they belong to
different accounts; the globally unique trade reference is `tradeId`.

`L1Update` is the top-of-book change stream. It carries the current best bid
price and aggregate quantity plus the current best ask price and aggregate
quantity only when that top-of-book tuple changes. Empty sides encode price and
quantity as zero.

`L2Update` is an absolute aggregate price-level update. It carries one
`side + price` level and the current visible aggregate `qty` at that level after
every visible level mutation. `qty = 0` means the level is deleted.

Outbound trading events do not currently carry timestamps. `mdSeq` is strictly
monotonic across `TradePrint`, `L1Update`, and `L2Update`. `prevMdSeq` on
`L1Update` and `L2Update` carries the previous market-data sequence for simple
gap detection. `engineSeq` is allocated from the same deterministic sequencer
used by execution reports, control responses, snapshots, and protocol errors,
so it is strictly monotonic across all logical outbound engine events.

## Control Snapshots

`Snapshot` is an inspection-only control operation. Requests produce a sequence
of fixed-width control events:
`SnapshotBegin`, zero or more `SnapshotOrder` rows, then `SnapshotEnd`.
`SnapshotBegin` carries top-of-book aggregates, resting order count, last
market-data sequence, next trade ID, and the active risk reference price.
`SnapshotOrder` rows are emitted in deterministic book order: bids from high to
low price, FIFO within each bid level, then asks from low to high price, FIFO
within each ask level. `rowIndex` is zero-based. `SnapshotEnd.rowCount` equals
the number of emitted rows, allowing a consumer to detect a truncated batch.

This stream is not a checkpoint or replay seed, and there is no restore path.
It does not serialize the runtime input cursor, the global sequencer's next
value, kill-switch or heartbeat state, the market-data L1 publication cache,
per-account mass-cancel ordering, or the static configuration identity needed
to validate a restore. Deterministic replay starts from a complete recorded
input stream instead.

## Execution Reports

`ExecutionReport` uses separate `execType` and `ordStatus` fields. `execType`
describes what this report represents, while `ordStatus` describes the order's
state after applying the report.

Current `execType` values are:

- `New`: passive order accepted onto the book.
- `Trade`: execution occurred.
- `Canceled`: order canceled or a post-only order canceled before resting.
- `Replaced`: cancel-replace preserved priority and updated the resting order.
- `Rejected`: command rejected.
- `Expired`: market or IOC order did not execute and cannot rest.

Current `ordStatus` values are `New`, `PartiallyFilled`, `Filled`, `Canceled`,
`Replaced`, `Rejected`, and `Expired`.

Every fill carries the concrete execution price and fill quantity. Reports that
do not represent an execution encode no fill price with wire value `0` and
carry zero fill quantity.

Market and IOC residual quantity never rests. If such an order partially fills
and then expires, the final taker trade report has `execType = Trade`,
`ordStatus = Expired`, the final fill price and quantity, and zero remaining
quantity. If no quantity fills, the report is `Expired` / `Expired`.

## Template ID Ranges

- `1-49`: control requests
- `50-89`: control responses
- `90-99`: protocol/session responses
- `100-149`: order-entry requests
- `150-199`: order-entry responses
- `200-249`: market-data messages

## Numeric Field Policy

- SBE byte order is little-endian.
- Message header fields remain `uint16`: block length, template ID, schema ID,
  and schema version.
- `accountId`, control `requestId`, and `heartbeatSeq` are `uint32`.
- `clientOrderId`, `origClientOrderId`, global engine sequence, market-data
  sequence, trade ID, timestamps, prices, and quantities are `uint64`.
- Price fields are `uint64`; negative prices are impossible on the wire.
- Quantity fields are `uint64`.

## Compatibility Rules

- Schema ID and version are explicit in the SBE message header.
- Template IDs are stable after introduction.
- Generated Rust must be regenerated from XML, never patched manually.
- Later compatible changes should prefer new templates or versioned fields over
  changing the meaning of existing fields.
