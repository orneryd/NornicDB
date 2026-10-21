# Tune ordered post-commit publication (#1406)

The existing per-key publication stage fixes out-of-order caches, indexes and
unique registries. Earlier write-path measurements showed regressions. Profile the integrated current stage before optimizing its bookkeeping.

Preserve the commit-order prefix for every entity and unique value, publication
before acknowledgment, parallel disjoint publication, quota-admission ordering,
and reentrant listeners. Do not change storage formats or public behavior.

Measure indexed/unique direct and transactional create/update/delete,
read-after-write, 10k-node transactions, eight distinct/overlapping writers, and
small writes beside large commits against both the original PR and its recorded
develop baseline. Record allocations, CPU profiles and measurement limitations.

The maintainer clarified that there is no fixed 5% release threshold: report
measured residual costs and their causes for the maintainer’s release decision.
