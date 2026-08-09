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

The configured instrument lives in `configs/server.yaml` under
`reference_data.instrument`. The wire protocol carries only integer price and
quantity fields; `price_decimals`, `qty_decimals`, `price_tick`, and `qty_lot`
define the per-instrument scale and minimum legal increments. Parsed core
state uses numeric IDs only: no symbol or asset strings enter the matching
engine. `instrument_id`, `base_asset_id`, and `quote_asset_id` are `u16` in
the domain/config model.

Prices and quantities are unsigned integer-scaled values. Zero is not a valid
domain `Price` or `Qty`; outbound optional price fields use wire value `0` as
the current no-price sentinel.

## Endpoint Model

The protocol is exposed as two ingress endpoint families and two egress endpoint
families:

- Control ingress decodes `KillSwitch`, `Snapshot`, and `Heartbeat` into
  `domain::control::ControlRequest`.
- Trading ingress decodes `NewOrder`, `CancelOrder`, `CancelReplace`, and
  `MassCancel` into `domain::trading::order::OrderEntry`.
- Control egress encodes `domain::control::ControlEvent`.
- Trading egress encodes `domain::trading::TradingEvent`.

A known template sent to the wrong endpoint is rejected as an unexpected
template. Unknown template IDs are still rejected as unknown templates.

## Message Families

- Control requests: `KillSwitch`, `Snapshot`, `Heartbeat`
- Control events: `ControlResponse`, `HeartbeatStatus`, `ProtocolError`
- Order entry: `NewOrder`, `CancelOrder`, `CancelReplace`, `MassCancel`
- Trading events: `ExecutionReport`, `TradePrint`, `BookUpdate`,
  `ProtocolError`

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
- `clientOrderId`, `origClientOrderId`, engine sequence, market-data sequence,
  trade ID, timestamps, prices, and quantities are `uint64`.
- Price fields are `uint64`; negative prices are impossible on the wire.
- Quantity fields are `uint64`.

## Compatibility Rules

- Schema ID and version are explicit in the SBE message header.
- Template IDs are stable after introduction.
- Generated Rust must be regenerated from XML, never patched manually.
- Later compatible changes should prefer new templates or versioned fields over
  changing the meaning of existing fields.
