# Managed-vector journal qualification

These generated tests measure journal bytes for property-only updates. They do
not establish throughput or heap-allocation improvements.

A complete first anchor preserves exact vectors for recovery. Each client update
then carries a fingerprint and complete properties, metadata and named vectors.
Subsequent updates can reuse the anchor through a fixed 64-entry proof cache.
Eviction, embedding audits or journal retirement require another complete anchor.

| Workload | Managed coordinates | Named vector | Client record bytes |
| --- | ---: | ---: | ---: |
| Native property update | 16 × 256 floats | 256 floats | 1,760 |
| Native property update | 64 × 256 floats | 256 floats | 1,760 |
| Native property update | 256 × 256 floats | 256 floats | 1,762 |
| Public Cypher property update | 16 × 256 floats | None | 469 |
| Public Cypher property update | 256 × 256 floats | None | 471 |

The public 256-chunk case with a named vector adds **331,216 total journal
bytes on first use**, including its anchor; its client record is 1,760 bytes.
The first-use total must not be confused with the small client record. A separate
three-update test proves one complete anchor is reused, and replay restores the
exact final vectors and property value.

Explicit 17,000 × 256-vector qualification passes under both standard and large
record policies: the anchor spans frames while the client record remains below
8 KiB. Ordinary oversized records still fail standard-policy admission.

## Correctness checks

- Journal-only recovery compares every managed and named vector value.
- Missing or mismatched predecessors fail without regenerating vectors.
- Duplicate replay checks its durable receipt before resolving an older image.
- Staged replacement vectors remain complete in their own transaction image.
- Worker replacement and snapshot retirement invalidate previous proof reuse.
- Malformed reference indices, duplicate entries and version mismatches fail.
- Existing full-image admission runs before an anchor or reference is written.

The test fixtures are disposable. The dogfood database is not used.

## Remaining qualification

Runtime allocations, CPU profiles, first-use versus steady-state timings and
working sets exceeding the proof cache remain unqualified. Portable replication
still carries complete images; these results claim no replication bandwidth gain.
Full CI and duplicate-review dispositions are separate gates.
