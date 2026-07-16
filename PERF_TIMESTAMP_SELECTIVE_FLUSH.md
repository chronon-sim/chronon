# Timestamp-Scoped Selective Flush Evaluation

## Finding

Chronon `main` already routes `StageSelective::cancelYoungerThan()` through a
receiver-only timestamp predicate, avoiding sender-side reads of receiver
cancellation state. However, `StageSelective::cancelOlderThan()` writes the
legacy atomic lower bound while the StageSelective pop path ignores that
bound. Consequently, `cancelOlderThan()` is ineffective for StageSelective
ports, and `cancelOutsideInclusive()` enforces only its upper bound.

## Change

The StageSelective predicate is extended from:

```text
{ flush_cycle, max_keep }
```

to:

```text
{ flush_cycle, min_keep, max_keep }
```

Both selective directions now use the same receiver-only timestamp path.
Same-cycle installs intersect their keep ranges, so
`cancelOutsideInclusive()` requires one live predicate and one pop-side walk.
Messages with `enqueue_cycle >= flush_cycle` remain outside the old flush, as
before.

This does not replace `OutPort::cancelInFlight()`: a cycle timestamp cannot
distinguish messages sent before and after an exact mid-cycle epoch bump.

## Verification

- Added lower-only and inclusive-range tests, including post-flush messages
- Existing overlapping-flush, predicate-retirement, and sender-filter tests
  pass
- Complete Chronon Release build
- Complete Chronon test suite: 42/42 passed

No standalone performance claim is made for this branch. It fixes the missing
general StageSelective semantics while retaining the existing no-sender-read
hot path.
