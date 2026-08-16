# Benchmark Methodology and Results

Ignition's `benchmark` binary measures the latency-sensitive order-entry path
with deterministic, prebuilt SBE input and the production runtime graph. It
reports HDR histogram percentiles, sustained throughput, and allocator calls;
it does not report an average as a substitute for tail behaviour.

## Contents

- [Measured boundary](#measured-boundary)
- [Workloads](#workloads)
- [Arrival schedules](#arrival-schedules)
- [Histogram and samples](#histogram-and-samples)
- [Warmup](#warmup)
- [Allocation measurement](#allocation-measurement)
- [Commands](#commands)
- [Observed environment](#observed-environment)
- [Observed results](#observed-results)
- [Interpretation](#interpretation)

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

Results below were collected on 2026-08-16. The baseline used source revision
`dce8aad` with documentation-only working-tree changes. The Noop condition used
the same revision plus the live idle-policy implementation described below.

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
or best-case numbers. CPU 3 pinning and CPU 3 isolation are the next planned
conditions.

## Observed Results

Each condition consists of five alternating burst/open-loop trial pairs. Every
trial used 1,000,000 measured mixed commands and 50,000 warmup commands. Values
are the median of the five trials, followed by the full `(min–max)` range.

| Condition | Live idle policy | CPU placement | CPU isolation | Release binary SHA-256 |
| --- | --- | --- | --- | --- |
| A — baseline | `std::thread::yield_now()` | Unpinned (`0-3`) | None | `90412e688c1d7ea…` |
| B — Noop | `Yield::Noop` | Unpinned (`0-3`) | None | `a6ef8b3cbb04f44b…` |

### Burst Service Time

| Condition | Throughput (orders/s) | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | max (ns) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A — baseline | 828,210 (823,447–831,691) | 1,600 (1,597–1,602) | 1,920 (1,917–1,954) | 4,283 (4,103–5,159) | 12,535 (11,599–12,671) | 39,583 (23,231–86,463) |
| B — Noop | 818,775 (817,026–824,595) | 1,601 (1,600–1,603) | 1,992 (1,987–1,996) | 4,427 (4,355–8,583) | 12,751 (12,447–12,879) | 56,959 (22,879–142,591) |

### Open Loop At 100k/s

The expected inter-arrival interval was 10,000 ns.

| Condition | Throughput (orders/s) | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | max (ns) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A — baseline | 100,000 (100,000–100,000) | 1,660 (1,645–1,666) | 2,045 (2,029–2,053) | 8,159 (6,547–10,423) | 13,503 (12,655–14,663) | 85,887 (37,183–122,239) |
| B — Noop | 100,000 (100,000–100,000) | 1,652 (1,647–1,659) | 2,089 (2,073–3,987) | 8,303 (7,227–9,975) | 13,799 (12,087–16,239) | 132,863 (30,127–133,887) |

All twenty trials produced exactly 1,000,000 samples with zero allocation calls
and zero allocated bytes during measurement.

## Interpretation

Condition B's burst median throughput was 1.1% lower than baseline. Its burst
p50 was effectively unchanged, while p99 through p99.99 were 1.7–3.8% higher.
Open-loop p50 was 0.5% lower, while p99 through p99.99 were 1.8–2.2% higher.
Maximum latency varied much more than the percentiles across both conditions.

This is a negative control, not evidence that `Yield::Noop` changed hot-path
latency. The benchmark uses finite in-memory input: each poll either makes
progress or detects fully drained input and exits. It never executes the live
idle policy. The observed differences therefore represent run-to-run host
variation and possible binary-layout effects, not removal of `yield_now()` from
the measured path.

At 100k/s, offered load remains far below measured burst capacity. Open-loop
tail variability shows the scheduler and other host activity delaying some
absolute arrival deadlines on this unpinned `schedutil` system. Pinning and then
isolating CPU 3 will test those environmental effects while keeping condition
B's source and release binary fixed.

The contrast is intentional evidence: service time alone describes matcher
cost, while scheduled-arrival latency exposes the environment and queueing seen
under an independent arrival process. A production performance campaign should
add isolated-core pinning, a performance governor, repeated trials, hardware
counters, scheduler traces, and a separate TCP round-trip benchmark before
using these figures for capacity planning.
