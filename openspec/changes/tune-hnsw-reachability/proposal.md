# Profile the current HNSW reachability correction (#1446)

The reported 44% remove/reinsert regression belongs to a repair implementation
already removed from the current PR. The spanning-tree experiment was measured
and rejected. Audit and profile current head c0c97becc against develop 16923aef7,
preserving its approved reachability, insertion, population-wide completeness,
generator-owned exhaustion and continuation semantics. Do not reintroduce a
superseded repair, tree or persistence format.

Measure remaining costs before optimizing. Preserve exact live-set completeness
after mutations and Save/Load, narrow-search recall and existing lexical hints.
Report paired latency, allocations, retrieval quality and remaining costs. The
maintainer clarified that there is no fixed 5% release threshold.
