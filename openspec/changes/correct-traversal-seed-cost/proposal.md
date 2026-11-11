# Correct traversal seed cost after NOR-414

## Required outcome

Restore collection/document traversal performance without losing the selective-end improvements of #1771. The reported ordinary workload has one collection, hundreds of selected documents, and large unrelated document adjacencies. Choosing an indexed end solely because the start has no index multiplies work; start inline keys must also participate before choosing an end.

## Scope

Extend the existing unbound-end traversal access decision. Reuse the canonical node index chooser, count-store metadata and label-ordering owner to compare candidate populations and the adjacency work the current traversal actually performs. Keep bound-end, chained traversal, final WHERE/projection, visibility, transaction and authoritative-empty behavior intact. No persisted statistics, cache, public API or storage-format changes.

## Evidence

NOR-414's 10 November regression comment reports up to 153.7x slower collection membership reads on main896fa3a1f after #1771. Reproduce with generated in-memory data through public Execute; retain baseline counters and exact rows. Private lab reports are reference evidence, not local qualification.
