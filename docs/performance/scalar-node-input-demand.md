# Scalar node input qualification

Scalar identity, labels, existence and list-cardinality expressions now request
only their required node data. A property expression retains the named property;
whole-node escapes and custom callbacks retain the image their contract needs.
These decisions are shared by label scans and traversal tails.

Custom function registration declares its input contract independently of
PROFILE observation. Full input includes stored vectors; reviewed metadata-only
arguments do not acquire embedding sidecars. Count aggregates retain their own
existence/identity contract rather than inheriting a scalar function's policy.

## Current release comparison

Apple M2 Max, Go 1.27.3, arm64. Production baseline: `a492f2acd`. Both checkouts
used the same existing CodeIntel fixture and benchmark body. Each benchmark ran
200 executions, with CPU and heap profiles enabled. Development checks
could contend for resources; these are short workload samples, not isolated
throughput qualification.

| Workload / metric | Release baseline | Candidate |
| --- | ---: | ---: |
| First eager count, 65,536 edges: allocated bytes | 29,246,360 | 29,276,400 |
| Repeated eager count, 2,048 edges: B/op | 1,081,302 | 1,082,092 |
| Repeated eager count: allocations/op | 11,674 | 11,714 |
| Repeated eager count: ns/op | 1,500,125 | 1,524,002 |

The candidate's repeated-read byte difference is 0.073%; allocation count is
0.34% higher and time is 1.6% higher in this sample. No zero-allocation or
throughput improvement is claimed. The unchanged 96 MiB first-count guard passes.
An intermediate candidate allocated 157,940,400 bytes because it treated the
aggregate's relationship argument as a whole property value; that candidate was
rejected and the shared count-demand contract corrected.

Public result and recording-reader tests establish the reduction in demanded
unused property bodies. They also cover aliases, nested expressions, full-value
escapes, custom registration replacement and detached results. Those controls do
not qualify every application query or the unavailable private workload.
