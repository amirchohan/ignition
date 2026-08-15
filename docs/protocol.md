# Wire Protocol

Ignition uses Simple Binary Encoding (SBE) with fixed-width, little-endian
messages. The XML schema at [`protocol/ignition.xml`](../protocol/ignition.xml)
is the byte-level source of truth; this page is its human-readable contract.

## Contents

- [Framing](#framing)
- [Endpoint contract](#endpoint-contract)
- [Numeric policy](#numeric-policy)
- [Enumerations](#enumerations)
- [Message layouts](#message-layouts)
- [Sequencing and gap detection](#sequencing-and-gap-detection)
- [Snapshots](#snapshots)
- [Compatibility and generation](#compatibility-and-generation)

## Framing

Every message is one SBE header followed by one fixed block:

```text
0               2               4               6               8
+---------------+---------------+---------------+---------------+
| blockLength   | templateId    | schemaId      | version       |
| u16 LE        | u16 LE        | u16 LE        | u16 LE        |
+---------------+---------------+---------------+---------------+
| fixed message block: blockLength bytes                        |
+---------------------------------------------------------------+
```

- Schema ID: `7001`.
- Schema version: `1`.
- Frame length: `8 + blockLength` bytes.
- Maximum accepted frame: 4 KiB.
- Largest current frame: 92 bytes (`SnapshotBegin`).
- All numeric fields are little-endian.

TCP is a byte stream: a read may contain a partial frame or several frames.
The server waits for the complete eight-byte header, derives the frame length
from `blockLength`, and dispatches only complete frames. An oversized frame
disconnects the active session so following bytes cannot be misinterpreted.
UDP market-data datagrams contain one complete SBE frame.

## Endpoint Contract

| Endpoint | Transport | Accepted ingress | Egress |
| --- | --- | --- | --- |
| Control | TCP, default `19001` | `KillSwitch`, `Snapshot`, `Heartbeat` | responses, heartbeat status, snapshots, protocol errors |
| Gateway | TCP, default `19000` | `NewOrder`, `CancelOrder`, `CancelReplace`, `MassCancel` | execution reports and protocol errors |
| Market data | UDP multicast, default `239.255.0.1:19002` | None | trade prints, L1, and L2 |

A known template sent to the wrong endpoint is rejected as an unsupported
template. An unknown template ID is rejected as unknown. The current TCP
adapter permits one active client on each ingress endpoint.

## Numeric Policy

| Value | Wire type | Meaning |
| --- | --- | --- |
| `instrumentId`, asset IDs | `u16` | Startup reference-data identifiers; not repeated on orders |
| `accountId` | `u32` | Account identity, bounded by `max_account_id` at startup |
| control `requestId`, `heartbeatSeq` | `u32` | Control correlation and liveness counters |
| client order IDs | `u64` | Scoped to an account while the order is live |
| `engineSeq`, `mdSeq`, `tradeId` | `u64` | Monotonic outbound identities |
| timestamps | `u64` | Opaque client timestamp or engine receive nanoseconds |
| price and quantity | `u64` | Positive integer-scaled values |

The configured instrument is BTC-USD by default. `price_decimals: 2` means a
wire price of `6_500_000` displays as `$65,000.00`; `qty_decimals: 8` means a
wire quantity of `100_000_000` displays as `1.00000000 BTC`. `price_tick` and
`qty_lot` are the minimum legal increments. Values must be positive and aligned
to those increments before they enter the domain model.

The runtime is single-symbol, so order messages do not carry a symbol. A
multi-symbol protocol would need an explicit instrument/session contract.
Optional outbound prices use zero as the no-price sentinel; zero is never a
valid domain `Price` or `Qty`. Negative prices and quantities are not
representable on the wire.

## Enumerations

| Enum | Values |
| --- | --- |
| `Side` | `Buy=1`, `Sell=2` |
| `OrderType` | `Limit=1`, `Market=2` |
| `TimeInForce` | `Gtc=1`, `Ioc=2`, `PostOnly=3` |
| `ExecType` | `New=1`, `Trade=2`, `Canceled=3`, `Replaced=4`, `Rejected=5`, `Expired=6` |
| `OrdStatus` | `New=1`, `PartiallyFilled=2`, `Filled=3`, `Canceled=4`, `Replaced=5`, `Rejected=6`, `Expired=7` |
| `KillSwitchAction` | `HaltTrading=1`, `Shutdown=2`, `ResumeTrading=3` |
| `ControlResult` | `Ok=1`, `Rejected=2` |

Reject reasons are `None=0`, `Malformed=1`, `UnknownOrder=2`,
`InvalidPrice=3`, `InvalidQuantity=4`, `RiskOpenOrderLimit=5`,
`RiskNotionalLimit=6`, `PriceBand=7`, `SelfTradePrevention=8`,
`KillSwitchActive=9`, and `HeartbeatLost=10`.

Protocol error codes are `DecodeError=1`, `UnsupportedTemplate=2`,
`UnsupportedVersion=3`, and `InternalError=4`.

## Message Layouts

The block size excludes the eight-byte SBE header. Fields appear in the order
shown.

### Control ingress

| ID | Message | Block | Fields |
| ---: | --- | ---: | --- |
| 1 | `KillSwitch` | 5 | `requestId:u32`, `action:KillSwitchAction:u8` |
| 2 | `Snapshot` | 4 | `requestId:u32` |
| 3 | `Heartbeat` | 4 | `heartbeatSeq:u32` |

### Control egress

| ID | Message | Block | Fields |
| ---: | --- | ---: | --- |
| 50 | `ControlResponse` | 22 | `engineSeq:u64`, `requestId:u32`, `recvTs:u64`, `result:ControlResult:u8`, `reason:RejectReason:u8` |
| 51 | `HeartbeatStatus` | 21 | `engineSeq:u64`, `heartbeatSeq:u32`, `recvTs:u64`, `result:ControlResult:u8` |
| 52 | `SnapshotBegin` | 84 | `engineSeq:u64`, `requestId:u32`, `recvTs:u64`, `bidPrice:u64`, `bidQty:u64`, `askPrice:u64`, `askQty:u64`, `restingOrderCount:u64`, `lastMdSeq:u64`, `nextTradeId:u64`, `riskReferencePrice:u64` |
| 53 | `SnapshotOrder` | 49 | `engineSeq:u64`, `requestId:u32`, `rowIndex:u64`, `side:Side:u8`, `clientOrderId:u64`, `accountId:u32`, `price:u64`, `remainingQty:u64` |
| 54 | `SnapshotEnd` | 30 | `engineSeq:u64`, `requestId:u32`, `recvTs:u64`, `rowCount:u64`, `result:ControlResult:u8`, `reason:RejectReason:u8` |

### Gateway ingress

| ID | Message | Block | Fields |
| ---: | --- | ---: | --- |
| 100 | `NewOrder` | 39 | `clientOrderId:u64`, `accountId:u32`, `clientTs:u64`, `side:Side:u8`, `orderType:OrderType:u8`, `timeInForce:TimeInForce:u8`, `price:u64`, `qty:u64` |
| 101 | `CancelOrder` | 28 | `clientOrderId:u64`, `accountId:u32`, `origClientOrderId:u64`, `clientTs:u64` |
| 102 | `CancelReplace` | 44 | `clientOrderId:u64`, `accountId:u32`, `origClientOrderId:u64`, `clientTs:u64`, `price:u64`, `qty:u64` |
| 103 | `MassCancel` | 20 | `clientOrderId:u64`, `accountId:u32`, `clientTs:u64` |

`clientTs` is carried into the normalized command but does not determine
priority. The server assigns `recvTs` when the complete frame is accepted.
Market orders ignore their wire price. Limit and replacement prices must be
positive, tick-aligned, and otherwise valid for the configured instrument.

### Gateway egress

| ID | Message | Block | Fields |
| ---: | --- | ---: | --- |
| 150 | `ExecutionReport` | 47 | `engineSeq:u64`, `clientOrderId:u64`, `accountId:u32`, `execType:ExecType:u8`, `ordStatus:OrdStatus:u8`, `rejectReason:RejectReason:u8`, `fillPrice:u64`, `fillQty:u64`, `remainingQty:u64` |

`execType` describes the event represented by the report; `ordStatus` describes
the order state after applying it. Reports without a fill encode
`fillPrice=0` and `fillQty=0`. The complete lifecycle semantics are in
[Matching engine](matching-engine.md).

### Shared protocol error

| ID | Message | Block | Fields |
| ---: | --- | ---: | --- |
| 99 | `ProtocolError` | 19 | `engineSeq:u64`, `templateId:u16`, `recvTs:u64`, `errorCode:ErrorCode:u8` |

Protocol errors are returned on the endpoint that received the invalid frame.

### Market-data egress

| ID | Message | Block | Fields |
| ---: | --- | ---: | --- |
| 200 | `TradePrint` | 56 | `engineSeq:u64`, `mdSeq:u64`, `tradeId:u64`, `makerOrderId:u64`, `takerOrderId:u64`, `price:u64`, `qty:u64` |
| 201 | `L1Update` | 56 | `engineSeq:u64`, `mdSeq:u64`, `prevMdSeq:u64`, `bidPrice:u64`, `bidQty:u64`, `askPrice:u64`, `askQty:u64` |
| 202 | `L2Update` | 41 | `engineSeq:u64`, `mdSeq:u64`, `prevMdSeq:u64`, `side:BookSide:u8`, `price:u64`, `qty:u64` |

Every execution emits a `TradePrint`. Every visible price-level mutation emits
an absolute `L2Update` followed by a complete `L1Update`, even if the best bid
and ask tuple did not change. `L2Update.qty=0` deletes a level. Empty L1 sides
encode both price and quantity as zero.

## Sequencing and Gap Detection

`engineSeq` is allocated from one deterministic sequencer shared by execution
reports, control events, protocol errors, snapshots, and market data. It is
strictly increasing across the logical outbound event history, even though the
events travel on different transports.

`mdSeq` is strictly increasing across all `TradePrint`, `L1Update`, and
`L2Update` messages. Consumers detect a gap by comparing each received
`mdSeq` with their last accepted value. L1 and L2 additionally carry the
previous feed sequence in `prevMdSeq`; a trade print is still part of the same
sequence even though it has no `prevMdSeq` field. `tradeId` independently
identifies each execution.

The current feed offers gap detection but not recovery. A full L2 snapshot and
delta-recovery protocol is a deferred extension.

## Snapshots

A `Snapshot` request produces exactly:

```text
SnapshotBegin
SnapshotOrder × restingOrderCount
SnapshotEnd
```

Rows are zero-indexed and deterministic: bids from highest to lowest, FIFO
within each bid level, then asks from lowest to highest, FIFO within each ask
level. `SnapshotEnd.rowCount` lets a client detect truncation. The stream is an
inspection view, not a restorable checkpoint; see [Risk and control](risk-and-control.md)
and [Replay](replay.md).

## Compatibility and Generation

- Template IDs remain stable after introduction.
- Compatible evolution should add versioned fields or new templates rather
  than change an existing field's meaning.
- Schema ID and version are validated on ingress.
- Generated Rust under `protocol/generated/` is committed and never edited by
  hand.
- Normal builds and tests do not require Java or SBE tooling.

Regenerate codecs after an intentional XML schema change with:

```bash
scripts/generate_sbe.sh
```

Changing framing, layouts, endianness, or existing public protocol types is a
compatibility decision and must not be made as an incidental implementation
change.
