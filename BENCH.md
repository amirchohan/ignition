# Benchmark Methodology and Results

Ignition's `benchmark` binary measures the latency-sensitive order-entry path
with deterministic, prebuilt SBE input and the production runtime graph. It
reports HDR histogram percentiles, sustained throughput, and allocator calls;
it does not report an average as a substitute for tail behaviour.

## Measured Boundary

The timed path begins after one complete gateway frame is available from the
in-memory adapter:

```text
complete SBE gateway frame
→ header and message decode
→ domain normalization
→ control and risk gate
→ liquidity/capacity projection
→ matching and book mutation
→ trade/L2/L1 shaping and encoding
→ execution-report encoding
→ in-memory egress accepts the report
```

Timing stops after all handling for that input frame and after deferred gateway
publication errors are checked. Every generated input contributes one sample,
including cancels and commands that produce several logical outputs.

The benchmark intentionally excludes:

- TCP reads, writes, buffering, and client/server scheduling;
- UDP syscalls and multicast fan-out;
- filesystem recording and replay I/O;
- workload generation and SBE input encoding;
- configuration parsing, runtime construction, and book preallocation;
- environment inspection and report formatting.

It is therefore a service-time/scheduled-arrival benchmark of the normal
gateway-to-report path, not a transport-inclusive round-trip benchmark.

## Workloads

All workloads generate valid binary gateway messages before measurement and
cycle across bounded accounts and 64 nearby price levels.

| Workload | Deterministic shape |
| --- | --- |
| `mixed` | Five-message cycle: passive buy GTC, priority-losing cancel-replace, cancel, passive sell GTC, aggressive buy IOC fill |
| `cancel` | Alternating passive buy add and cancel |
| `aggressive-ioc` | Alternating passive sell and crossing buy IOC |
| `passive-add` | Passive sell GTC only; order count must fit configured resting capacity |

The primary reported workload is `mixed`, giving each message in its
five-command cycle a 20% share. It exercises decoding, price mapping, arena and
lookup mutation, replace, cancel, risk projection, matching, trade and book
data encoding, and execution reports.

Workload preflight rejects a configuration that cannot represent its generated
accounts, 64-level price range, or passive resting count. A benchmark should
measure the intended path, not accidental capacity shutdown.

## Arrival Schedules

### Burst

Burst mode releases all prepared frames as fast as the runtime can consume
them. Each sample starts at gateway-handler entry, so the histogram represents
per-command service time. Throughput is measured from the first sample start to
the last sample end.

Burst mode is useful for saturated local throughput and core-path latency, but
it does not model independent external arrivals and therefore cannot reveal
queue delay relative to an offered schedule.

### Open loop

Open-loop mode assigns every input an absolute arrival deadline from the start
of the run. Deadlines do not move when an earlier order is slow. The runtime
releases overdue frames immediately until it catches up, and each latency sample
starts at its intended deadline rather than handler entry.

This directly includes scheduler delay and backlog in every observation. It
avoids coordinated omission without a post-hoc correction: a slow period does
not cause the load generator to stop scheduling the requests that should have
arrived during it.

## Histogram and Samples

Latency is recorded in an HDR histogram with a one-nanosecond lower bound, a
60-second upper bound, and three significant digits. The report includes:

- p50;
- p99;
- p99.9;
- p99.99;
- maximum.

After the run, the binary requires the histogram sample count to equal the
requested order count. A partial finite run cannot produce a plausible-looking
report.

## Warmup

Warmup uses the same workload and resolved core configuration but a separate
runtime, disabled metrics, and a disjoint order-ID range. The measured runtime
is then constructed with all input frames already allocated and encoded.

This warms compiled and shared machine paths without contaminating book state
or the measured histogram. It does not eliminate operating-system scheduling,
frequency scaling, or cold effects in the newly constructed measured book.

## Allocation Measurement

The benchmark binary installs a counting wrapper around Rust's system global
allocator. Immediately before the measured runtime begins, it resets total
allocation-call and requested-byte counters; it reads them immediately after
the runtime completes.

The counters exclude workload generation, configuration, runtime construction,
preallocation, warmup, environment inspection, and printing. They include any
allocator call made anywhere in the process while the measured runtime is
active. A zero result demonstrates no observed steady-state allocation in that
experiment; it is not a static proof that every possible error or shutdown path
is allocation-free.

## Commands

Primary 1M-order burst run:

```bash
cargo run --release --bin benchmark -- \
  --orders 1000000 \
  --warmup 50000 \
  --workload mixed \
  --schedule burst
```

The equivalent shortcut is:

```bash
just bench
```

Open loop at 100,000 scheduled arrivals per second:

```bash
cargo run --release --bin benchmark -- \
  --orders 1000000 \
  --warmup 50000 \
  --workload mixed \
  --schedule open-loop \
  --target-rate 100000
```

The equivalent shortcut is `just bench-open-loop 100000`.

Docker runs use the same release binary and configuration:

```bash
docker compose run --rm benchmark
SCHEDULE=open-loop TARGET_RATE=100000 docker compose run --rm benchmark
```

A short smoke run can reduce `--orders` and `--warmup`, but should not replace
the primary evidence. Optional Linux pinning is explicit, for example:

```bash
taskset -c 2 cargo run --release --bin benchmark -- \
  --orders 1000000 --warmup 50000 --workload mixed
```

Pinned and unpinned results must not be presented as the same experiment.

## Observed Environment

Results below were collected on 2026-08-15 from source revision `8d5473a`
with documentation-only working-tree changes.

```text
CPU: Intel Core i5-4670 @ 3.40GHz
Logical CPUs: 4
Physical cores: 4
CPU governor: schedutil
L1d / L1i: 32K / 32K
L2 / L3: 256K / 6144K
Memory: 15781676 kB
NUMA nodes: 1
Kernel: 7.0.0-28-generic
Rust: rustc 1.95.0 (59807616e 2026-04-14)
CPU affinity: 0-3 (not pinned to an isolated core)
```

The host was not isolated from normal operating-system activity. Frequency was
managed by `schedutil`, and no claim is made that these are production-hardware
or best-case numbers.

## Observed Results

Both runs used 1,000,000 measured mixed commands and 50,000 warmup commands.

| Metric | Burst service time | Open loop at 100k/s |
| --- | ---: | ---: |
| Throughput | 692,729 orders/s | 100,000 orders/s |
| p50 | 1,687 ns | 1,958 ns |
| p99 | 2,523 ns | 6,406,143 ns |
| p99.9 | 13,799 ns | 19,415,039 ns |
| p99.99 | 37,183 ns | 25,427,967 ns |
| max | 1,598,463 ns | 26,034,175 ns |
| Allocation calls | 0 | 0 |
| Allocated bytes | 0 | 0 |

The open-loop expected inter-arrival interval was 10,000 ns. The output sample
count matched 1,000,000 in both runs.

## Interpretation

The burst result shows a narrow core distribution through p99 and sustained
throughput around 693k commands/s for this mixed workload. The p99.9 and above
widen as occasional matching, page/lookup mutation, runtime phase boundaries,
cache effects, and host preemption enter the sample. The 1.60 ms maximum is an
outlier on an unpinned, non-isolated host rather than representative median
service time.

At 100k/s, offered load is far below measured burst capacity, and median
open-loop latency remains close to burst service time. Its millisecond p99 tail
shows transient scheduling backlog that a closed-loop or handler-entry timer
would omit. The runtime yields while waiting for the next scheduled arrival;
on this unpinned `schedutil` host, descheduling or delayed wakeup can make many
deadlines overdue and those commands then include the accumulated delay. The
exact cause was not traced with scheduler/performance counters, so this is the
most plausible interpretation rather than a proven attribution.

The contrast is intentional evidence: service time alone describes matcher
cost, while scheduled-arrival latency exposes the environment and queueing seen
under an independent arrival process. A production performance campaign should
add isolated-core pinning, a performance governor, repeated trials, hardware
counters, scheduler traces, and a separate TCP round-trip benchmark before
using these figures for capacity planning.
