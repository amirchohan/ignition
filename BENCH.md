# Benchmark Methodology

Ignition's primary benchmark is `benchmark`. It measures the runtime hot
path after a complete binary gateway frame is available in memory:

```text
SBE gateway frame
-> gateway decode and domain normalization
-> control/risk gate
-> matching engine
-> execution-report encode
-> in-memory egress sink accepts the frame
```

The benchmark intentionally excludes TCP loopback scheduling, disk reads/writes,
logging, and per-order stdout. Replay/file determinism is validated separately
by `just replay-compare <input_id>`. This benchmark is for the order-entry to
execution-report hot path, not for transport-inclusive round-trip latency.

## Commands

Default release benchmark:

```bash
just bench
```

Smaller smoke run:

```bash
cargo run --release --bin benchmark -- --orders 10000 --warmup 1000 --workload mixed
```

Optional CPU pinning on Linux:

```bash
taskset -c 2 cargo run --release --bin benchmark -- --orders 1000000 --warmup 50000 --workload mixed
```

Supported workloads:

```text
mixed           cancel-replace, cancel, passive add, and aggressive IOC fill mix
cancel          alternating passive add and cancel
aggressive-ioc  alternating passive sell and crossing IOC buy
passive-add     passive GTC adds only; keep count within configured book capacity
```

## Measurement

Input frames are generated deterministically as real SBE order-entry messages
and loaded into an `InMemory` IO backend before measurement. The backend still
uses the normal runtime/gateway callback path, but it does not clone input
frames or write output frames during the measured run. Output egress is counted
in memory.

Latency samples are recorded into an HDR histogram from inside the gateway frame
handler. The timer starts when the complete frame is handed to gateway decoding
and stops after execution report emission/queueing for that frame. The
benchmark prints p50, p99, p99.9, p99.99, and max latency in nanoseconds.

Warmup runs before the measured run. Allocation counters are reset immediately
before the measured run, so reported allocations exclude workload generation,
runtime construction, and warmup. The measured hot path should report zero
allocation calls once buffers and book storage are preallocated.

The workload is pre-generated rather than request/response paced, so the
benchmark does not hide slow samples by waiting to create the next order after a
stall. For future rate-limited/open-loop runs, HDR coordinated-omission
correction should use the configured target inter-arrival interval.

## Local Result

Machine:

```text
OS: Linux Prime 7.0.0-28-generic x86_64
CPU: Intel Core i5-4670 @ 3.40GHz, 4 cores, SMT disabled
Rust: rustc 1.95.0
Command: cargo run --release --bin benchmark -- --orders 100000 --warmup 10000 --workload mixed
CPU pinning: not used
```

Observed output:

```text
environment:
  cpu_model: Intel(R) Core(TM) i5-4670 CPU @ 3.40GHz
  logical_cpus: 4
  physical_cores: 4
  cpu_governor: schedutil
  l1d_cache: 32K
  l1i_cache: 32K
  l2_cache: 256K
  l3_cache: 6144K
  memory_total: 15781676 kB
  numa_nodes: 1
  kernel: 7.0.0-28-generic
  cpu_pinning: 0-3

workload: mixed
orders: 100000
warmup_orders: 10000
throughput_orders_per_sec: 687802

latency_ns:
  p50: 1700
  p99: 2559
  p99.9: 11079
  p99.99: 43135
  max: 125311

allocations:
  calls: 0
  bytes: 0
  calls_per_order: 0.000000
```

## Interpretation

Median and p99 are dominated by SBE decode/normalize, risk projection, book
mutation, market-data encode, and execution-report encode. The wider p99.99/max
tail on this unpinned local run is most likely host scheduling and cache effects
around occasional aggressive fills and runtime phase transitions. The benchmark
does not claim transport-inclusive latency; a separate TCP loopback benchmark
would be needed to characterize socket buffering, wakeups, and client/server
scheduling.
