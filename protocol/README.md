# Ignition Protocol

Ignition uses Simple Binary Encoding (SBE) for its order-entry, control, and
outbound event protocol.

## Source Of Truth

- `protocol/ignition.xml` is the schema source of truth.
- Generated Rust codecs are committed under `src/protocol/generated/`.
- Generated files must not be edited by hand.
- Handwritten protocol integration lives under `src/protocol/`.

## Build Policy

Normal `cargo build` and `cargo test` should not require Java or SBE tooling.
When the XML schema changes, regenerate codecs explicitly with:

```bash
scripts/generate_sbe.sh
```

The generation script downloads or uses the pinned `sbe-all` tool and writes
Rust output into `src/protocol/generated/`.

## Scope

Version 1 is scoped to exactly one configured symbol. Order-entry messages do
not carry a symbol field; all order flow in a session is for the configured
runtime symbol. Multi-symbol support is a future schema/version change.

## Message Families

- Order entry: `NewOrder`, `CancelOrder`, `CancelReplace`, `MassCancel`
- Control: `KillSwitch`, `Snapshot`, `Heartbeat`
- Outbound: `ExecutionReport`, `TradePrint`, `BookUpdate`, `ControlResponse`,
  `HeartbeatStatus`, `ProtocolError`

## Compatibility Rules

- Schema ID and version are explicit in the SBE message header.
- Template IDs are stable after introduction.
- Generated Rust must be regenerated from XML, never patched manually.
- Later compatible changes should prefer new templates or versioned fields over
  changing the meaning of existing fields.
