# Reduce retrieval continuation allocation and qualify public screening

## Problem and evidence

NOR-406 reports multi-second retrieval over 49,000 nodes and a 16-way screening workload. Historical provider round trips, statement parsing and engine search were not separated. The current two-dimensional, provider-free probe does not reproduce that external latency: first ten results cost about 0.36 ms. A selective eleven-hit continuation does reproduce population-sized allocation on later pulls, about 6.35 MB/op and 9.0 ms/op on the Apple M2 Max baseline.

CPU and allocation profiles identify the HNSW candidate selection maps and exact-stage qualifying heap. The heap reserves the requested depth even when canonical node grouping leaves only eleven nodes. Candidate selection materializes weak single-member nodes that the existing exact-stage membership proof already rejects.

## Correction

Reuse one `candidateNodeBoundIsComplete` proof in candidate selection and exact grouping. Only service-indexed canonical single-member nodes can be rejected before selection. Missing membership, encoded aliases, multi-member nodes, custom member resolvers and unknown quantization bounds retain the existing expansion path. Capacity follows plausible qualifiers, with growth available for hidden-member nodes. The qualifying heap cannot hold more than one score per grouped node.

Do not reduce population coverage, candidate caps, threshold accuracy or cancellation semantics. Do not add a second retrieval implementation, compatibility wrapper, cache policy, background producer or storage format.

## Qualification

Retain behavior-based deterministic probes, a public singleton-selection regression and existing hidden-member/alias/exact-stage tests. Generate larger public Cypher screening workloads from repository request and EXISTS examples, with 1024-dimensional vectors, approximately 1 KB source text, 356 groups and selectable concurrency 1/16. Discard every cursor and keep fixtures temporary.

Report engine-only controlled costs separately from unavailable external provider timings. A bounded optimization does not establish that the reported external latency is fixed; keep NOR-406 open while its external case remains unqualified. CI make-test and advisory duplicate review apply to the delivered candidate. Current graph retrieval fails HTTP-to-HTTPS routing and supplies no valid baseline; manual current-source reuse review must be explicit.
