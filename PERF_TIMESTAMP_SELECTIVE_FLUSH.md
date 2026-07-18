# Unified Selective Flush Evaluation

## Finding

The former selective-cancel API exposed policy-specific behavior. The legacy
path used receiver generations, atomics, and a mutex, while StageSelective used
enqueue-cycle predicates. StageSelective retirement also assumed dequeue order
was monotonic in enqueue cycle; heterogeneous-delay MPSC is ordered by arrival
cycle, so a short-delay post-flush message could appear before a long-delay
pre-flush message and retire the predicate too soon.

## Change

`InPort::flush<KeyFn>(FlushRange)` is now the first-class contract for every
policy and transport. `FlushRange` stores inclusive/exclusive bounds directly:

```text
youngerThan(uid)       keep [0, uid]
atAndYounger(uid)      keep [0, uid)
olderThan(uid)         keep [uid, UINT64_MAX]
atAndOlder(uid)        keep (uid, UINT64_MAX]
outsideInclusive(a,b)  keep [a, b]
```

Each receiver-owned predicate contains:

```text
{ flush_cycle, keep_range, key_extractor }
```

Predicates for the same extractor and cycle intersect. Predicates from
different cycles or extractors remain independent, so overlapping flushes are
monotonic and a compatibility reset cannot resurrect messages. A cycle-F
predicate applies only when `enqueue_cycle < F`.

Retirement uses the next queue arrival and the maximum incoming delay. Once the
frontier is beyond `F - 1 + max_delay`, epoch-free MPSC also requires every
producer's existing scheduler progress publication to reach `F`; this prevents
an empty long-delay lane from publishing an older arrival behind an apparently
future frontier. This supports arbitrary positive SPSC and heterogeneous MPSC
delays without adding an arrival field to the envelope.

The sender no longer reads selective receiver state. The change removes the
receiver-generation field from every envelope, five receiver-side atomics/the
mutex, and shared-broadcast generation scoping. No lock or RMW is added to the
send or receive hot path. Acquire-loads of already-existing scheduler progress
are made only when a live MPSC predicate reaches a possible retirement frontier.

## Verification

- Full-width UID zero and `UINT64_MAX` inclusive/exclusive boundaries
- Both PortPolicy values and both overlap orders
- delay-1/2/4 edges, including SPSC and heterogeneous-delay MPSC fan-in
- messages produced before, in, and after the flush cycle
- sequential oracle versus epoch-free workers 2 through 8
- deterministic migration before, during, and after a live predicate
- runtime epoch-free rebalance with cycle-for-cycle event digest equivalence

## Empty-receive microbenchmark

`chronon_typed_mpsc_dispatch_benchmark` was pinned to CPU 16 (physical core 8,
outside the SMT-paired CPU 0-15 range) and run three times per tree. The
16-lane empty-receive result was stable in every run:

| Tree | ns/receive |
|---|---:|
| merged-main baseline | 14.36 |
| unified FlushRange | 13.91 |

The ordinary no-flush path therefore shows no regression in this focused test
and is 3.1% lower here. This microbenchmark is a hot-path guard, not a claim
about whole-simulator workload speedup.
