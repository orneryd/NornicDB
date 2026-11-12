# Maintained literal-candidate preparation tradeoff

The candidate narrows literal/field/Boolean fulltext matching without replacing
exact score predicates or reintroducing vector reads. On a generated 16K-node
corpus (4K indexed documents / 400 hits, rotating two-term queries, result cache
off), repeated public reads fall from approximately 11.9 ms to 3.5 ms (70%).
Prepared literal and compound selection uses zero per-query plan allocations;
public hydration allocation variation overlaps the matched baseline range.

For first construction of 1K indexed distinct ~2KiB bodies among 4K nodes,
matched 3x100 public measurements are 7.42–7.54 ms baseline and 7.89–8.00 ms
with direct cached-chunk merging (median +6.6%, about 0.49 ms). A preceding
combined-source measurement was 8.13–8.25 ms (+9.5% median); preserve that
variation rather than selecting only the fastest sample. First construction
adds about 176KB allocated bytes (+1.6%) and 82–83 allocations. This is corpus
preparation, repeated after corpus replacement or pressure retirement, not on
every retained query. The index and bounded scratch are included in retention
accounting and cannot republish after memory pressure retirement.

Two alternatives were measured and rejected:

- Smaller signatures reduced construction bytes ~32KB but did not improve timing
  beyond variance and increase candidate-collision density on diverse bodies.
- Delaying construction until first reuse restored cold timing (~7.63ms), but
  first reuse rose from ~1.06ms to ~1.78ms (+68%) and deferred ~160KB allocation.
  This moved cost and worsened that phase; it is not retained.

The earlier decision request is superseded by the final integrated measurement
in docs/performance/v150-rc-corrections.md. Five fresh paired 300-operation
samples measure cold 7.599 -> 7.805 ms (+2.71%, within the 5% gate), warm
11.997 -> 3.460 ms (-71.16%), and cold bytes +1.60% for construction. Warm
allocation ranges overlap. No performance exception is needed for these named
results. Public-result/race checks and exact-head CI remain independent gates;
private application replay remains unavailable.
