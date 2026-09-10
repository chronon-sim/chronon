---
sidebar_label: "Clock Domains and CDC"
---

# Clock Domains and CDC

Chronon supports static, independently phased hardware clocks and finite typed
asynchronous FIFOs. Physical-time scheduling and CDC circuits are separate:
the scheduler chooses edges; each FIFO implements its own pointer synchronizers,
registered full/empty flags, RAM and output register. An ordinary delayed queue
is not a CDC circuit.

The initial implementation executes explicitly configured clock-domain graphs
serially. Requests for parallel execution have a visible fallback reason. The
existing default single-clock sequential and epoch-free paths remain available.
This infrastructure does not choose or calibrate SAGE/GPU clock parameters.

## Configure and run

```cpp
#include "chronon/Chronon.hpp"
using namespace chronon;

TickSimulationConfig config;
config.num_threads = 4;
TickSimulation sim(config);
sim.addClockDomain(ClockDomain::fromHz(1, "sm", 914'000'000));
sim.addClockDomain(ClockDomain::fromHz(
    2, "lts", 1'326'000'000, 1, SimTime::picoseconds(137)));

// Unit classes declare AsyncWritePort<uint64_t> and AsyncReadPort<uint64_t>.
auto* source = sim.createUnitInDomain<Source>(1);
auto* sink = sim.createUnitInDomain<Sink>(2);
sim.connectAsyncFifo(17, source->out, sink->in,
                     {.depth = 8, .synchronizer_stages = 2});
sim.initialize();
sim.runUntilTime(SimTime::nanoseconds(1000)); // exclusive upper bound
sim.runDomainCycles(1, 100);                  // 100 additional SM edges
```

Domain IDs are supplied by the model and remain independent of creation order.
ID 0 is the legacy default clock, with period `1 / config.tick_frequency_hz`
seconds and phase zero. `UINT32_MAX` identifies external scheduler termination
and is not a hardware domain ID. Domain names must be unique ASCII filename
components. Units require unique `fullPath()` identities in a clock-domain graph.

`addClockDomain`, `assignClockDomain`, connection creation and stream registration
are configuration operations. Rebinding clocks or adding connections after
initialization is rejected. Recreate the simulation to change its clock
configuration. A trace checkpoint is only an encoder checkpoint, not a model
checkpoint.

## Exact time and limits

For a domain with nonnegative phase and strictly positive period:

```text
E_d(n) = phase_d + n * period_d, n >= 0
```

`SimTime` is a reduced pair of unsigned 64-bit integers representing **seconds**.
`ClockDomain::fromHz(id, name, a, b, phase)` accepts the exact rational frequency
`a / b` Hz and therefore period `b / a` seconds. For example, frequency `5/3` Hz
is representable. Arbitrary mathematical real numbers are not claimed to be
representable. Inputs are nonnegative integers; negative times are not an API
representation.

Each clock precomputes a common denominator for its own period and phase:

```text
L = lcm(period.denominator, phase.denominator)
p = period.numerator * (L / period.denominator)
h = phase.numerator  * (L / phase.denominator)
E(n) = (h + n*p) / L seconds
maxEdgeIndex = floor((UINT64_MAX - h) / p)
```

`L`, `p`, `h`, and the unreduced edge numerator must fit `uint64_t`. Construction,
edge evaluation, rational addition, and edge queries check these bounds and throw
on overflow. Addition deliberately checks its common-denominator intermediates;
it can reject an expression whose final reduced fraction would fit. There is
no silent wrap, rounding, saturation, or fallback to floating point.
The scheduler keeps a representable successor edge, so executing an edge also
requires that its successor and the incremented local edge count fit.

Cross-clock comparisons use unsigned 128-bit products (GCC/Clang extension).
There is no floating-point accumulation. No global frequency LCM is constructed
and no fine-grained LCM lattice is traversed. A heap contains one next edge per
active domain. Unused domains have no scheduled work or executed edges.

`edgeAtOrAfter(t)` returns the first index with `E(n) >= t`; `edgeAfter(t)` returns
the first with `E(n) > t`. Both are bounded-width integer calculations, not
searches through preceding edges. Before the initial phase they return zero.

Representation range depends on the period **and phase denominator**. At
914 MHz with phase zero, an edge numerator is simply `n`. At 1326 MHz with
137 ps phase, `L = 663000000000000` and `p = 500000`, giving about 7.73 hours
of representable physical time. Check `maxEdgeIndex()` for the actual configured
clock; do not assume every phase has the default clock's horizon. The tests check
a million consecutive calendar batches and direct mappings at edge `10^12`.

For `D` active domains, `K` coincident edges, `U` units on those edges and `F`
FIFOs, a batch costs `O(K log D + U + F*K + active synchronizer registers)`.
Tracing additionally visits newly visible entries, at most the FIFO depth per
read edge. Calendar memory is `O(D)`; FIFO storage is `O(depth + stages)` packets
and registers. The clock arithmetic has fixed 64/128-bit storage.

## Three different notions of progress

| API / field | Meaning |
| --- | --- |
| `Unit::localCycle()` | Current edge index during `tick()`, next edge index outside it |
| `Unit::physicalTime()` | Exact `clockDomain().edge(localCycle())` |
| `domainCycleCount(id)` | Number of that domain's completed edges |
| `lastCommittedTime()` | Physical time of the last completed batch; zero before the first |
| `schedulerSteps()` | Number of completed physical-time batches |
| `currentCycle()` | Legacy cycle count; in explicit clock mode, compatibility spelling for scheduler steps |
| Ordinary `Connection::delay` | Edges of the endpoints' **same hardware domain** |
| `sleepUntil` / `wakeAt` | Edges of the owning unit's domain |

Never compare different domains' `localCycle()` values to decide causality.
`run(N)` and `runUntilTermination(N)` reject explicit clock-domain mode because
their cycle limit is ambiguous. Use `runClockEvents(N)`, `runUntilTime(t)`, or
`runDomainCycles(id, N)`. They return completed **batches**, honor termination
after the entire coincident batch, and do not manufacture a termination request
when their budget runs out. `runUntil(predicate, N)` uses a batch budget in this
mode and retains the configured predicate polling interval. For an exact stopping
edge, request termination from a unit.

Termination records include a domain ID, local edge number, and exact physical
time. An external request uses the last completed physical instant, scheduler
progress and domain sentinel `UINT32_MAX`. Simultaneous unit requests have a
deterministic winner: domain ID, then the domain's canonical zero-delay topological
order, with `fullPath()` as the tie-break. All units and CDC commits at that
instant finish before stopping. Resume after a normal stop with
`resetTermination()`. A failed evaluation or time overflow cannot be resumed.

## Coincident edges and pipeline registers

At each physical instant:

1. Every CDC circuit captures its pre-edge remote pointer values.
2. Units on participating domains evaluate their local interfaces.
3. Every CDC circuit commits RAM operations, pointer flops, synchronizer flops,
   flag flops and its read output register from the captured state.

Thus a pointer newly committed at time `t` cannot be sampled by another domain
at that same `t`, regardless of host execution order. This is a deterministic
digital convention, not a prediction of setup/hold behavior or metastability.

Ordinary zero-delay connections remain same-domain combinational eligibility
paths, evaluated in acyclic producer-before-consumer order. They are distinct
from CDC sampling. Ordinary connections between different IDs are rejected even
when the clocks happen to have identical frequencies and phases. Validation also
discovers connections created directly through `OutPort::connect`.

`PhasedTickableUnit` dispatches Phase0/Phase1 from **local** edge parity. Its
`StageReg` instances advance with that domain's ticks, not global batch parity.
Call `beginCycle<P>()` according to the existing register contract. Do not share
live register objects or other mutable unit state between domains. Arbitrary C++
member access is outside the port/CDC contract and cannot be made safe by a
scheduler. Sleeping units still have their local edge counts advanced; CDC
synchronizers continue clocking and wake sleeping endpoints when visibility or
backpressure changes make progress possible.

## FIFO interface and finite resources

```cpp
// Members of the source and destination units respectively:
AsyncWritePort<uint64_t> out{this, "out"};
AsyncReadPort<uint64_t> in{this, "in"};

// Source tick: at most one accepted write per source edge.
CdcPacket<uint64_t> packet{transaction_id, payload};
if (out.send(std::move(packet))) { /* accepted at this write edge */ }
// On false, send has not moved from packet.

// Destination tick: consume the OLD output before issuing a replacement read.
if (auto packet = in.take()) { /* now owned by this unit */ }
in.requestRead(); // bool: read command accepted at this read edge
```

The FIFO owns a finite array of dual-port RAM entries, independent local binary
pointers and their Gray encodings, two bounded synchronizer chains, and one
registered RAM output. RAM depth must be a power of two in `[2, 2^30]`; the
practical memory budget is the caller's responsibility. This is a storage
constraint, **not a frequency-ratio constraint**. Each pointer has one wrap bit.
`synchronizer_stages` is the number of registers in **each** pointer chain, in
`[2, 64]`; it does not include flag flops or the output register.

Full and empty initialize to false and true respectively. Their registered next
states compare the next **local** pointer to the **old synchronized** remote
pointer. The full comparator inverts the top two Gray bits of the synchronized
read pointer. Simulator occupancy is only an invariant/diagnostic and never
decides acceptance, full, or empty.

The read mode is **ordinary synchronous registered read**, not FWFT. An accepted
read samples the addressed RAM entry and commits it to the output register at
that read edge. The consumer can take it on its next edge. If the old output is
not taken, no new read is accepted. Taking an old output and accepting its
replacement read on the same edge is supported. The RAM slot is freed by the
accepted RAM read, not by later output consumption. Total persistent packet
storage is therefore `depth + 1`, including the output register.

Ports accept operations only while their owner is executing its clock edge.
`diagnostics()` is host-only between batches. Packets require a nonzero stable
transaction ID; preserve it when forwarding the same transaction through more
than one FIFO. Do not reuse IDs for unrelated transactions in a recorded run.

`CdcPayloadTraits` accepts owning scalar, enum, string, vector, array and supported
`unique_ptr` payloads. Custom aggregates require an explicit ownership attestation:

```cpp
struct Request { uint64_t address; std::array<uint32_t, 8> words; };
template <> struct chronon::sender::CdcPayloadTraits<Request> : std::true_type {};
```

This trait attests ownership, not a serialized bus width. Models must choose a
bounded packet representation/maximum payload size for their hardware protocol.
Live raw pointers, references, shared mutable objects and callbacks are not CDC
payloads. Neither endpoint receives a reference to RAM or the other unit.

Construction initializes empty RAM, zeroed pointers and synchronizer registers,
and an invalid output register before the first edge. Stop producers, continue
the read-side handshake, then call `drainCdc(batch_budget)`.
`cdcDrained()` includes RAM, output registers, flags and synchronized pointer
settling. It does not drain model-owned queues or stop producers automatically.

## Complete latency derivation

Let `q_j(n-)` be synchronizer register `j` immediately before destination edge
`n`. All right-hand sides below use pre-edge state:

```text
q_0(n+) = source_gray sampled before E_dst(n)
q_j(n+) = q_(j-1)(n-), j = 1 .. N-1
empty(n+) = (Gray(read_pointer(n-)+accepted_read) == q_(N-1)(n-))
```

Write-side full uses the analogous old read-pointer synchronizer tail and the
next write pointer. A source write accepted at physical time `t` updates RAM and
its write pointer at the commit of `t`. Under the same-time-old-state convention:

```text
k = min { n >= 0 | E_read(n) > t }
first synchronizer register commits the new pointer: E_read(k)
register j commits that sampled value:               E_read(k+j)
last register commits it:                             E_read(k+N-1)
local registered empty comparator observes it:        E_read(k+N)
earliest read acceptance when initially empty:         E_read(k+N+1)
registered RAM output becomes valid:                  after E_read(k+N+1) commit
earliest consumer take:                               E_read(k+N+2)
```

The last two operations are different events even though read acceptance and
output commit have the same physical edge timestamp. The output cannot be taken
in that edge's evaluation phase.

For return credits, let a read acceptance advance the read pointer at time `u`:

```text
j = min { m >= 0 | E_write(m) > u }
read-pointer synchronizer tail update: E_write(j+N-1)
registered full may deassert:          E_write(j+N)
earliest replacement write:            E_write(j+N+1)
```

These are earliest bounds assuming the relevant pointer update remains
observable, output space is available, the local request is asserted, and no
intervening operations reassert the flag. Arbitrary traffic requires the actual
state recurrence, not one fixed total delay.

Hand timeline, 1 GHz, equal phase zero, `N=2`, initially empty:

| Time / read edge | Pointer synchronizer after commit | Empty after commit | Action |
| --- | --- | --- | --- |
| 0 ns / 0 | `[0,0]` | true | Source writes packet 1; destination samples old zero |
| 1 ns / 1 | `[1,0]` | true | First stage samples the pointer |
| 2 ns / 2 | `[1,1]` | true | Tail updates; comparator still sampled old tail |
| 3 ns / 3 | `[1,1]` | false | Empty deasserts |
| 4 ns / 4 | `[1,1]` | true | Read accepted; registered output valid after commit |
| 5 ns / 5 | `[1,1]` | true | Consumer takes packet 1 |

With depth two, writes at edges 0 and 1 assert full at edge 1. A read at edge 4
releases RAM space; write-side full deasserts at edge 7 and a replacement write
is first accepted at edge 8. Both timelines are executable assertions in
`test_async_fifo_circuit.cpp`.

The model samples stable digital Gray words. It does not simulate analog
metastability, pointer-bus skew or MTBF. A real implementation still needs proper
CDC physical/timing constraints. No general two-flop synchronizer API is exposed;
this pointer circuit does not claim reliable transport of arbitrary multibit
buses or short pulses.

## Scheduler safety and compatibility

The existing dependency graph, SCC analysis, lookahead progress counters,
cross-thread queue headroom and termination accounting assume one comparable
cycle scale. A CDC edge has state-dependent visibility and return credit delays;
assigning it a fixed local-cycle weight would be unsound.

CDC components are therefore stored separately from ordinary connections. The
clock-domain mode gate runs before parallel partitioning/progress installation,
selects single-thread queue adapters for same-domain ports, and never launches
epoch-free workers or dynamic rebalancing. Canonical same-domain zero-delay
ordering remains active; zero-delay cycles are rejected. All cross-domain
visibility is evaluated on the physical-time calendar.

`useParallelExecution()` is false, `epochFreeRunCount()` stays zero, and
`parallelFallbackReason()` explains the policy. When parallelism was requested,
initialization also writes the reason to `std::clog`, independently of tracing.
Tests compare the independent reference, explicit serial execution and requested
1/2/4-worker configurations including reversed unit/domain creation. This is
fallback equivalence, **not a claim that parallel CDC simulation is implemented**.

Default single-domain graphs keep their existing scheduling, ordinary port
delays, phase behavior and trace API. Opting into clock-domain mode is explicit.
Native clock tracing uses `ClockTraceRecorder`; combining this mode with the
legacy `ObservationManager` backend is rejected because that backend's raw-cycle
reorder logic is not domain aware. Legacy `event<>`, `debug<>`, pipeline slices,
periodic counter CSV, YAML clock declarations and scheduler wall-time capture
have not been migrated to the native clock recorder in this version.

## Native recording

```cpp
ClockTraceRecorder::Config trace;
trace.output_dir = "out/my-new-clock-run"; // must be new or empty
trace.run_id = "experiment-42";
trace.text = true;
trace.perfetto = true;
trace.lossless = true;
trace.stream_capacity = 4096;
trace.drain_batch = 256;
sim.configureClockTrace(trace); // before initialize()
// In a unit's tick(), for an optional additional low-cardinality event:
clockEvent(ClockEventKind::User, transaction_id, value);
// After producers stop, flush/join and surface I/O errors:
sim.closeClockTrace();
```

Define additional event names before initialization with
`clockTraceRecorder()->defineEvent(static_cast<ClockEventKind>(257), "sm.issue")`.
Metadata names are bounded printable ASCII strings. Record construction carries
40 bytes of integers/enums, no payload references, string formatting, heap
allocation or global event sequence. The per-stream ordinal is local, not a
cross-domain order. Guard expensive observation-only argument calculations with
`if (clockTraceStream())`.

Each unit has a bounded SPSC record stream. Separate streams can be written by
separate workers, including workers in the same domain. One backend drains
batches without a global time heap or per-event global lock. Text is formatted
and written immediately in batches; native Perfetto records use bounded open
nanosecond buckets, closed by explicit producer progress. Exactly one text sink
exists per domain:

```text
clock-manifest.json
text-domain-sm.log
text-domain-lts.log
text-domain-dram.log
timeline.pftrace
clock-stats.json
```

The manifest records run identity, exact periods/phases, unit/domain/sequence/
track mappings and event names. Text rows contain local cycle, unit ID, event,
evaluation/commit phase, transaction ID, FIFO ID, value and stream ordinal.
Only per-stream order is guaranteed; different streams in one domain need not
be cycle-sorted. There are no per-worker text shards and no global order across
domain files. FIFO write, registered destination visibility, read, output and
consume are separate events on the appropriate units' domain tracks.

On a full observation ring, lossless recording blocks/yields the **host** until
space exists; no simulated edge is added. Lossy recording drops the record and
counts the drop. Metadata is not placed in a lossy queue. `clock-stats.json`
reports total retained/dropped events and ingress memory. Close/join only after
all producer threads have stopped. Native Perfetto packet prefixes become available
during execution; `close()` / `closeClockTrace()` finishes the tail. Backend failures unblock waiting producers
and are surfaced as errors. Static metadata plus ring capacity are fixed before
running; ring allocation is capped at 256 MiB across streams.

The ingress peak is the sum of per-stream high-water marks, a conservative bound
on simultaneous queued bytes. It excludes bounded text/Perfetto encoder batches
and dictionaries. Text buffers flush around 64 KiB per domain; the existing
Perfetto writer flushes at 4096 packets and splits compressed wrappers at packet
boundaries around 256 KiB of uncompressed input. Report process RSS as well as
ingress statistics when assessing total memory.

Native Perfetto uses no temporary disk files and no whole-trace external sort.
The buffer holds at most `perfetto_options.clock_buffer_records` owned records
(default 8192, range 2..65536), plus a 4 MiB aggregate metadata/accounting budget.
One record is limited to 64 KiB. Fixed record storage, string allocator overhead,
encoder batches and dictionaries are additional; buffer statistics report a
conservative bound and the benchmark also reports process RSS. Buffer exhaustion
throws an explicit error and wakes blocked producers, never silently spills or
drops accepted events. In particular, all events in one open ns bucket must fit;
reduce publication batch size or increase the record bound for dense models.

### Progress publication and bounded streaming

`PerfettoTraceWriter::advanceClockWatermark(W)` promises that **all streams** have
submitted every event with `floor(physical_time / 1 ns) < W`, and will never submit
another. Only buckets strictly below W can be encoded. Bucket W remains open,
including exact-edge phase ties. A decreasing promise or a late event is rejected.
The writer allows out-of-order input within its bounded open window, not unlimited
disorder without progress. `flush()` writes closed buckets already encoded;
`close()` supplies the final promise and writes the unclosed tail.

Independent producer threads using `ClockTraceRecorder` call
`stream->advance(next_local_cycle)` after bounded batches, promising no future
record below that cycle. It publishes `floor(E_domain(next_local_cycle) / 1 ns)`
and **waits for the global safe frontier**. This is host backpressure, not a
simulated delay. Quiet streams must publish progress too; permanently completed
streams call nonblocking `finish()`, after which recording is rejected. Do not
call blocking stream advances sequentially from one coordinator thread: use
`recorder.advance(W)` at a safe point covering all producers instead.

The serial multi-clock scheduler automatically publishes such a coordinator
promise after every 64 complete physical-time edge batches, after all unit queue
publishes and CDC commits. It covers inactive/quiet domains as well. It converts
only this batch boundary to display ns; hardware time stays exact. No per-record
global atomic, lock or ordering heap is added. Text-only mode does not wait on
these promises. For custom execution drivers, producers must join before close.

The backend acquires progress **before** capturing queue heads, drains every
captured record (possibly in several configured batches), and only then closes
buckets and acknowledges progress. Thus a watermark cannot overtake its published
records. Merely seeing the last event, or an empty queue, never implies progress.
Acknowledged batches bound skew from independent workers: fast workers wait while
slower workers or explicitly progressing quiet streams catch up. Publication
batch sizes and the aggregate in-flight records across streams must fit the
configured bucket budget; violation fails explicitly rather than deadlocking.

Closed windows are ordered by integer ns, with exact time/phase tie ordering
inside a ns bucket. This retains native flow causality without changing simulation
or CDC behavior. This is limited backend cross-stream coordination, replacing the
previous offline-only contract, not a global order assigned by simulation producers.

Existing `TickSimulation` users need no progress calls. For manually managed,
independent producer threads, a typical bounded-batch loop is:

```cpp
for (uint64_t cycle = 0; cycle < count; ++cycle) {
    stream->record(cycle, ClockEventKind::User);
    if ((cycle & 15) == 15) stream->advance(cycle + 1);
}
stream->finish();
```

The batch sizes across all producers must fit the native bucket buffer. Each
quiet producer also needs a progress/finish path; do not omit it from the
protocol just because its queue is empty. The old `clock_sort_run_records`
option and disk spool implementation have been removed. Direct writer callers
with large, arbitrarily disordered input must now submit bounded windows and
publish `advanceClockWatermark()` between them; there is no disk fallback.

## Perfetto clocks, precision and import

The SDK is pinned to **v56.1** in the root CMake file. Its
[ClockSnapshot protocol](https://github.com/google/perfetto/blob/v56.1/protos/perfetto/trace/clock_snapshot.proto)
defines clocks 64..127 as sequence-local, `unit_multiplier_ns` as an integer, and
the scope of incremental state. The corresponding
[ClockSynchronizer implementation](https://github.com/google/perfetto/blob/v56.1/src/trace_processor/util/clock_synchronizer.cc)
applies integer scaling and snapshot translations. Sparse snapshots are not a
general rational-frequency slope conversion. See also the
[official clock synchronization explanation](https://perfetto.dev/docs/concepts/clock-sync).

For each native stream the encoder computes:

```text
timestamp_ns = floor((phase + local_cycle * period) * 1,000,000,000)
```

It then uses sequence-local clock 64 for incremental **nanoseconds**, clock 65
for absolute fallback on regressing timestamps, and the trace-file reference
clock (`BUILTIN_CLOCK_TRACE_FILE`, 11) for the simulation's zero-based physical
timeline. `ClockSnapshot` declares all three equal at the same quantized physical
instant, with multiplier 1 on the local clocks. The snapshot and defaults appear
on the owning packet sequence at first use and every incremental-state reset.
Static clocks need no snapshot per event. No global custom-clock hash namespace
is needed. A hardware domain can have several recording sequences; tracks and
host worker IDs are separate concepts.

Interning, incremental timestamps, checkpoint limits, compression and batched
file writes remain in `PerfettoTraceWriter`. Native events cannot be mixed with
legacy raw-cycle or host wall-clock events in that writer. The simulation
reference is not synchronized to host execution time. Use separate files for
host performance diagnostics.

Display timestamps have a floor error in `[0,1)` ns, including over long runs.
Encoding rejects values above `INT64_MAX` nanoseconds (about 292 years), and
clock representation limits may be lower. Subnanosecond edges can share a display
timestamp. `domain_id`, `local_cycle`, phase and ordinal retain their exact
identity; reconstruct rational time using the manifest. Native events are
ordered by exact physical time, phase, stable track name, and local ordinal
before quantization. This matters because Perfetto chains equal-timestamp native
flows in import order: `--full-sort` cannot recover lost subnanosecond order.
The bounded bucket pass preserves FIFO flow direction even when several causal steps
display at the same ns. For custom events, same-instant causal stages must be
expressed by their Uint `phase`; independent events sharing a phase use stable
track-name/ordinal presentation order, not an inferred hardware dependency.
No picoseconds are mislabeled as nanoseconds. Unsigned debug fields can appear
signed in PerfettoSQL; the validator restores their 64-bit representation.

Real **Trace Processor v57.2** is tested with **full sorting**, including input
streams hundreds of seconds apart, 4 GHz same-ns collisions, reversed drain
orders, and exact-time phase ties. Native output is finalized in the defined
order as progress permits; arbitrary externally reordered files are not promised to work in every
default UI/import mode:

```bash
trace_processor --full-sort query out/my-new-clock-run/timeline.pftrace \
  "SELECT ts, name FROM slice ORDER BY ts LIMIT 20"
```

Stable transaction IDs generate native flows between the per-domain events.
The validator checks imported times against the independent reference, checks
flow edges and causal order at exact reconstructed times, and normalizes stream
identities instead of comparing output bytes.

## Offline tools and recovery

```bash
python3 scripts/merge_clock_logs.py out/my-new-clock-run --output /tmp/merged.tsv
python3 scripts/recover_clock_trace.py damaged.pftrace /tmp/recovered.pftrace
```

The text merge uses `fractions.Fraction` and an explicitly offline sort. It emits
exact time numerators/denominators and quantized display nanoseconds. Ties use
phase, domain ID, unit identity and stream ordinal for presentation; this does
not add hardware causality. Memory is proportional to the offline event count.

Recovery applies to complete encoded prefixes during execution as well as closed
traces. Open buckets and the current unflushed encoder batch are not crash durable;
no fsync durability is promised. Text remains independently available. Recovery copies a
complete prefix of top-level protobuf packets verbatim,
preserving sequence, clock, track and intern identities. An incomplete compressed
wrapper is discarded in full. A file containing only one torn wrapper can
legitimately recover zero events. Complete retained records must still match the
reference and import without errors. Missing initial metadata, corrupted middle
packets, arbitrary suffix extraction and model-state restart are unsupported.
Do not concatenate independent runs' Perfetto files: their identity namespaces
can collide.

## Validation and migration

`test/sender/clock_reference.hpp` includes no production Chronon headers. Its
serial oracle uses unwrapped binary pointer counts, history deques and a packet
deque, rather than the production Gray/ring transition functions. Separate time,
circuit and integrated scheduler tests cover same-phase and shifted 1:1 clocks,
fast/slow directions, 3:2, 5:3, 1000/1001 MHz and 914/1326 MHz, stage/depth sweeps,
bursts, deterministic random backpressure, simultaneous operations, repeated
wrap, termination and drain. A mismatch prints its seed/configuration and last
32 batch states. Native trace fixtures also cover long-time encoding, concurrent
recording, reverse drains, checkpoints, compression, loss and prefix recovery.

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release \
  -DCHRONON_BUILD_BENCHMARKS=ON \
  -DCHRONON_TRACE_PROCESSOR=/absolute/path/to/trace_processor
cmake --build build-release -j4
ctest --test-dir build-release --output-on-failure --parallel 8
./build-release/examples/multiclock_cdc_example /tmp/new-chronon-clock-example
python3 scripts/validate_clock_traces.py \
  --trace-processor /absolute/path/to/trace_processor \
  --multiclock-binary build-release/test/sender/sender_test_multiclock \
  --recorder-binary build-release/test/observe/test_clock_trace --check-prefix
```

For SAGE integration, assign explicit SM/GPC, L2/LTS and memory-controller domain
IDs to existing units before initialization. Retain ordinary connections within
a domain; replace each domain crossing with declared FIFO endpoints and select
depth/synchronizer stages as model parameters. Change readers to the explicit
registered-read handshake and preserve owned transaction identities. Migrate
native event recording to `clockEvent`/`ClockTraceRecorder`, and use physical-time
or named-domain run limits. No SAGE sources or product defaults are changed by
this implementation. The three-domain example is illustrative, not calibration.

See [validation and measurements](multiclock-validation.md) for measured costs,
commands, current acceptance results and the remaining limitations.
