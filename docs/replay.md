# Recording and Replay

Ignition reconstructs a recorded live run by feeding its original control and
gateway frames through the same protocol, runtime, risk, matching, sequencing,
and publication path. Given the same run artifact, each replay produces
byte-identical control, execution-report, and market-data files.

## Contents

- [Determinism contract](#determinism-contract)
- [Recorded input format](#recorded-input-format)
- [Live recording](#live-recording)
- [Running replay](#running-replay)
- [Byte-identical verification](#byte-identical-verification)
- [Why snapshot is not a replay seed](#why-snapshot-is-not-a-replay-seed)
- [Failure conditions](#failure-conditions)

## Determinism Contract

A complete replay input consists of:

- every accepted control and gateway SBE frame;
- one global input sequence across both endpoints;
- the engine-assigned receive timestamp for each frame;
- the exact deterministic core configuration used by the run.

The matching core reads no clock or random source. Output-producing traversals
use explicit book, FIFO, or input-sequence order rather than hash-map iteration.
Replay injects recorded `recvTs` values; there is no timestamp-exclusion mode.
As a result, timestamps and all sequence values remain part of the byte-level
comparison.

## Recorded Input Format

Each endpoint input file is a sequence of little-endian records:

```text
inputSeq : u64
endpoint : u8     # 1=control, 2=gateway
recvTs   : u64
frameLen : u32
frame    : frameLen bytes of the original SBE message
```

This wrapper is a recording format, not part of the client wire protocol. The
file backend exposes the original SBE `frame` to the same control or gateway
decoder used in live mode.

Control and gateway records are stored in separate files but share one
`inputSeq`. During replay, each backend peeks at its next record and releases it
only when that record is globally due. The sequence advances only after its
handler succeeds, preserving the original inter-endpoint order.

Captured output files contain repeated records of:

```text
frameLen : u32 little-endian
frame    : frameLen bytes of encoded SBE output
```

## Live Recording

[`configs/live.yaml`](../configs/live.yaml) selects file recording:

```yaml
recording:
  type: file
  root_dir: "fixtures/replay/runs"
```

At startup, live mode creates a numeric run directory:

```text
fixtures/replay/runs/<run-id>/
├── run.yaml
└── input/
    ├── control.bin
    └── gateway.bin
```

The two endpoint files are created when recording starts and may remain empty.
`run.yaml` contains manifest format version `1`, the first global input
sequence, and the complete `CoreConfig`: instrument scaling, engine capacity,
risk policy, heartbeat, and poll budgets. Replay never consults the current
`configs/server.yaml`, so later edits cannot change historical behaviour.

The replay reader also tolerates an absent endpoint file for one-sided or older
artifacts. A run is invalid if neither control nor gateway input exists.

## Running Replay

Replay a run with:

```bash
just replay 1786831955
```

The direct command is:

```bash
cargo run --bin replay -- --run fixtures/replay/runs/1786831955
```

Docker can replay the checked-in run without a local Rust toolchain:

```bash
REPLAY_RUN=1786831955 docker compose run --rm replay
```

Every invocation creates the next four-digit output directory without
overwriting earlier evidence:

```text
fixtures/replay/runs/<run-id>/output/
├── 0001/
│   ├── control.bin
│   ├── gateway.bin
│   └── market_data.bin
└── 0002/
    ├── control.bin
    ├── gateway.bin
    └── market_data.bin
```

The files are created for configured output endpoints even when no frames are
emitted.

## Byte-Identical Verification

Compare every numbered output after `0001` with the baseline:

```bash
just replay-compare 1786831955
```

The script compares control, gateway, and market-data files with `cmp`. A file
may be absent only when it is absent in both baseline and candidate. A mismatch
or asymmetric file fails immediately.

The checked-in `0001` and `0002` outputs currently share these SHA-256 values:

| Output | SHA-256 |
| --- | --- |
| `control.bin` | `6110add72a6e81a436e86a2f9e0f0ecfd8d1e45697167cfcd4e8caef783a058d` |
| `gateway.bin` | `7ef2557dbb6dc2e31bfe37d669fc35177c30625e6ae7294e986185d61807a216` |
| `market_data.bin` | `ecf651fb945db42a171381f934385c31b5ef6fc5b89eef651ff8734a38340558` |

The fixture exercises passive limit orders, replacement and cancellation,
an aggressive market IOC, fills and trade prints, L1/L2 publication,
heartbeats, and a 40-order control snapshot through the normal runtime
boundary. It does not exercise post-only orders, mass cancel, or kill-switch
behaviour; those policies are covered by deterministic tests rather than this
recorded live run.

## Why Snapshot Is Not a Replay Seed

The control snapshot is designed for inspection. It includes price-time-ordered
resting rows and selected sequence/risk metadata, but omits the global input
cursor, next engine sequence, control and heartbeat state, configuration
identity, account-chain continuation details, and other state needed to resume
with byte-identical output.

A restorable checkpoint would require a versioned complete-state schema,
validation against the ordered log, and tests proving checkpoint-plus-suffix
equivalence. Until that exists, the recorded input stream is the only supported
replay source.

## Failure Conditions

Replay rejects:

- an unsupported manifest version;
- a zero first input sequence;
- invalid recorded core configuration;
- a missing input directory or both endpoint files missing;
- wrong-endpoint records or gaps in the global input sequence;
- truncated, oversized, or invalid SBE frames;
- output directory identifier overflow;
- a finite input that stalls before all expected records are consumed.

Replay always writes to a fresh directory, so a failed attempt does not replace
earlier evidence.
