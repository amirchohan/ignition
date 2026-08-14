# Replay And Recorded Streams

Ignition records live TCP input before endpoint decoding. Each record wraps one
raw SBE frame with replay metadata:

```text
input_seq: u64 little-endian
endpoint: u8          # 1=control, 2=gateway
recv_ts: u64 little-endian
frame_len: u32 little-endian
frame: [u8; frame_len]
```

The wrapper is not part of the client wire protocol. Replay reinjects the raw
SBE frame and recorded `recv_ts` through the same control/gateway runtime path
used by live clients. Execution-report and market-data outputs are captured as:

```text
frame_len: u32 little-endian
frame: [u8; frame_len]
```

Outbound trading and market-data messages currently do not carry receive
<<<<<<< HEAD
timestamps. Control responses and protocol errors that do carry `recv_ts` use
the recorded timestamp.
=======
timestamps. Execution reports, control events, protocol errors, and
market-data messages all carry a globally monotonic `engineSeq`; market-data
messages also carry a feed-local `mdSeq` for gap detection. Control responses
and protocol errors that do carry `recv_ts` use the recorded timestamp.
>>>>>>> single_engine_sequencer

## Recording

Recording is opt-in. Set `mode.record_path_dir` in the live config:

```yaml
mode:
  type: live
  next_input_seq: 1
  record_path_dir: "fixtures/replay/runs"
```

Then start the server normally:

```bash
just server
```

When `record_path_dir` is present, server startup uses epoch seconds as the
replay `input_id`, creates a run directory, and prints the selected ID.
Recorded files are written to:

```text
fixtures/replay/runs/<input_id>/input/control.bin
fixtures/replay/runs/<input_id>/input/gateway.bin
```

The runtime assigns one global `input_seq` across control and gateway as frames
are accepted. Recorders are flushed during normal endpoint polling and flushing.
If the epoch-second run directory already exists, startup fails instead of
overwriting recorded input. If `record_path_dir` is absent, the live server
records nothing.

## Replay

Replay a recorded input stream with:

```bash
just replay <input_id>
```

Each replay creates the next output directory automatically:

```text
fixtures/replay/runs/<input_id>/output/0001/control.bin
fixtures/replay/runs/<input_id>/output/0001/gateway.bin
fixtures/replay/runs/<input_id>/output/0001/market_data.bin
```

Running replay again for the same input writes `output/0002`, then `0003`, and
so on. The replay command reads `mode.record_path_dir` from
`configs/replay.yaml` and applies the run-specific input/output paths in
memory.

Compare deterministic replay outputs with:

```bash
just replay-compare <input_id>
```

Comparison uses `output/0001` as the baseline and compares every later output
directory against it. Missing optional endpoint files are allowed only when
they are missing on both sides; byte mismatches or one-sided files fail.
