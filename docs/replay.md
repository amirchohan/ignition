# Replay And Recorded Streams

Deterministic replay starts from a complete recorded input stream plus the
versioned core configuration captured with that stream. Inspection snapshots
remain output-only and are not replay seeds.

Each input record wraps one raw SBE frame with deterministic metadata:

```text
input_seq: u64 little-endian
endpoint: u8          # 1=control, 2=gateway
recv_ts: u64 little-endian
frame_len: u32 little-endian
frame: [u8; frame_len]
```

The wrapper is not part of the client wire protocol. Replay reinjects the raw
frame and recorded `recv_ts` through the same control/gateway path used by live
traffic. Captured outputs remain a `u32` little-endian length followed by the
encoded frame.

## Recording

`configs/live.yaml` selects either disabled or file recording:

```yaml
recording:
  type: file
  root_dir: "fixtures/replay/runs"
```

At live startup the server creates a numeric run ID and writes:

```text
fixtures/replay/runs/<run-id>/run.yaml
fixtures/replay/runs/<run-id>/input/control.bin
fixtures/replay/runs/<run-id>/input/gateway.bin
```

`run.yaml` stores manifest format version 1, the first input sequence, and the
exact validated `CoreConfig`. This prevents later edits to `../configs/server.yaml`
from changing historical replay behaviour. The runtime still assigns one
global input sequence across control and gateway.

## Replay

Replay a checked-in or newly recorded run with:

```bash
just replay <run-id>
# Direct equivalent:
cargo run --bin replay -- --run fixtures/replay/runs/<run-id>
```

Each invocation creates the next numbered output directory:

```text
fixtures/replay/runs/<run-id>/output/0001/control.bin
fixtures/replay/runs/<run-id>/output/0001/gateway.bin
fixtures/replay/runs/<run-id>/output/0001/market_data.bin
```

Run `just replay-compare <run-id>` to compare every later output against
`output/0001`. Missing optional endpoint files are allowed only when absent on
both sides. The binary input/output formats, recorded timestamps, and global
sequence enforcement are identical between live recording and replay.
