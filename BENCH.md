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

Results below were collected on 2026-08-16. All three conditions used source
revision `7ff2c82` with documentation-only working-tree changes and the same
release binary.

```text
CPU: Intel Core i5-4670 @ 3.40GHz
Logical CPUs: 4
Physical cores: 4
CPU governor: schedutil
L1d / L1i: 32K / 32K
L2 / L3: 256K / 6144K
Memory: 15781676 kB
NUMA nodes: 1
Rust: rustc 1.95.0 (59807616e 2026-04-14)
CPU affinity: 0-3 for condition A; CPU 3 for conditions B/C
```

Frequency was managed by `schedutil`, and no claim is made that these are
production-hardware or best-case numbers. Conditions A/B ran without CPU
isolation. Condition C reserved CPU 3 from scheduler load balancing, enabled
full dynticks there, and directed default and managed interrupts to CPUs 0–2.

## Observed Results

Each condition consists of five alternating burst/open-loop trial pairs. Every
trial used 1,000,000 measured mixed commands and 50,000 warmup commands. Values
are the median of the five trials, followed by the full `(min–max)` range.

| Condition | Live idle policy | CPU placement | CPU isolation | Release binary SHA-256 |
| --- | --- | --- | --- | --- |
| A — baseline | `Yield::Noop` | Unpinned (`0-3`) | None | `a6ef8b3cbb04f44b…` |
| B — pinned | `Yield::Noop` | CPU 3 | None | `a6ef8b3cbb04f44b…` |
| C — isolated | `Yield::Noop` | CPU 3 | CPU 3 scheduler/tick/IRQ | `a6ef8b3cbb04f44b…` |

### Burst Service Time

| Condition | Throughput (orders/s) | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | max (ns) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A — baseline | 818,775 (817,026–824,595) | 1,601 (1,600–1,603) | 1,992 (1,987–1,996) | 4,427 (4,355–8,583) | 12,751 (12,447–12,879) | 56,959 (22,879–142,591) |
| B — pinned | 818,257 (813,112–826,194) | 1,596 (1,596–1,600) | 1,999 (1,981–2,011) | 4,807 (4,295–9,127) | 12,663 (11,791–12,919) | 24,399 (23,519–91,967) |
| C — isolated | 826,290 (822,041–831,923) | 1,596 (1,594–1,599) | 1,938 (1,929–1,962) | 9,727 (2,161–10,447) | 12,703 (11,783–12,943) | 81,855 (18,751–89,215) |

### Open Loop At 100k/s

The expected inter-arrival interval was 10,000 ns.

| Condition | Throughput (orders/s) | p50 (ns) | p99 (ns) | p99.9 (ns) | p99.99 (ns) | max (ns) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A — baseline | 100,000 (100,000–100,000) | 1,652 (1,647–1,659) | 2,089 (2,073–3,987) | 8,303 (7,227–9,975) | 13,799 (12,087–16,239) | 132,863 (30,127–133,887) |
| B — pinned | 100,000 (100,000–100,000) | 1,648 (1,645–1,656) | 2,385 (2,081–3,985) | 9,775 (8,695–10,847) | 15,063 (14,031–15,423) | 86,015 (23,775–86,719) |
| C — isolated | 100,000 (100,000–100,000) | 1,654 (1,646–1,658) | 2,063 (2,034–2,069) | 10,711 (9,231–11,655) | 14,071 (12,759–14,839) | 82,431 (23,375–106,495) |

All thirty reported trials produced exactly 1,000,000 samples with zero
allocation calls and zero allocated bytes during measurement.

## Interpretation

### What CPU placement changed

Pinning and isolation barely changed throughput or the central latency
percentiles because they do not shorten the gateway-to-report code path; they
only remove CPU migration and some competing work. On this otherwise quiet
host there was little such interference to remove. The inconsistent movement
in the higher percentiles and maxima is therefore not evidence of a reliable
placement improvement or regression.

### What the tail suggests

The similar p99.99 service-time tail across all three placements shifts the
next optimization effort toward the code and its memory-access patterns. The
benchmark already excludes transport and workload generation, and it observed
no steady-state allocations. Plausible remaining causes are:

- cache or TLB misses in the preallocated book and lookup structures;
- long active-order lookup probe chains;
- price-level creation or removal;
- branch misprediction; and
- state-dependent matching and encoding work that emits several outputs.

A rare service-time stall can exceed the open-loop arrival interval and pass
backlog into subsequent samples. Reducing the core-path tail should therefore
improve scheduled-arrival latency as well.

### Next measurements

1. Run `cancel`, `aggressive-ioc`, and `passive-add` separately to identify the
   operation class responsible for the tail.
2. Add a small preallocated outlier trace recording command kind, match count,
   output count, lookup probes, and price-level changes above a fixed latency
   threshold.
3. Add per-stage timing around decode, risk, book/matching, and encoding.
4. Collect hardware counters for cache, TLB, and branch misses.
