# Restore MATCH/CREATE/DELETE throughput (#1472)

Reproduce the reported 9,831/11,424 ops/s against the existing 15,000 ops/s
floor with both parsers. Compare the immutable release base and PR source on
the same machine, profile the actual query, and correct the measured causal
owner without weakening the floor, transactional semantics or query results.
Preserve current release work and mandatory live commit hooks.

The isolated PR and immutable release base both clear the floor locally; the
original PR also passes its full Nornic-parser suite without a waiver. This does
not reproduce the shared worker's failure. Measure improvements without claiming
that local results establish the worker's throughput.

An experiment extending the keyword-position cache was rejected: despite faster
repeated queries, distinct query texts cost about 10% more validation time and 1.8 KB more
allocation per validation. Restore that owner unchanged.

Measure the remaining redundant predicate-scope construction in the canonical
WITH projection owner independently. Reject any variant whose mutation
improvement causes a regression in normal routing or first-use validation.
Integrate retained changes on current release/v1.4.4 and link the common
correction from #1472. Do not call the shared worker's failure resolved without
its job evidence and a strict rerun there.
