# Replay Fixtures

`sample-gateway-input.bin` is a recorded gateway input stream in the file-record
format documented in `docs/replay.md`.

The sample sends:

1. passive sell GTC
2. passive buy GTC
3. aggressive buy IOC that trades
4. cancel of the resting buy
5. mass cancel for the remaining sell account

`sample-gateway-output.bin` and `sample-market-data-output.bin` are golden
outputs for the checked-in sample stream. Live run captures now use the
directory layout documented in `docs/replay.md`.

`runs/1786693693` is the checked-in replay-run fixture for the operator flow:

```bash
just replay-compare 1786693693
```

It contains recorded live input under `input/` and two replay output passes
under `output/0001` and `output/0002`. The comparison command uses `0001` as
the baseline and verifies later output directories byte-for-byte.

Each directory under `runs/` also contains `run.yaml`, a versioned snapshot of
the exact core configuration and first input sequence used for replay. The
legacy fixtures preserve the former replay reference price so their golden
outputs remain stable after the canonical configuration was consolidated.
