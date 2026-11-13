# Historical reads and no-overlap constraint costs

Historical reads and interval constraints are independent. A temporal constraint
indexes user-selected interval values; it does not index database commit history.
An ordinary property index describes current values; it cannot establish complete
historical matches after a property or label changes.

## Grouping width and comparison scope

```cypher
CREATE CONSTRAINT account_intervals
FOR (n:Account)
REQUIRE temporal.epoch(n.starts_at, n.ends_at)
GROUP BY n.tenantId, n.accountId

CREATE CONSTRAINT ownership_intervals
FOR ()-[r:OWNS]-()
REQUIRE temporal.epoch(r.starts_at, r.ends_at)
GROUP BY r.tenantId, r.accountId, r.assetId
```

All grouping values must match before intervals compete. Different tenants,
accounts or assets can have simultaneous intervals. `GROUP BY` here is a schema
comparison scope, not result aggregation. One or more grouping properties are
supported; the tests cover widths 1, 2, 4, 8 and 16, forward and reverse order,
for nodes and relationships. Changing a component or interval is checked again
on updates. Missing/null grouping values are rejected. Adjacent half-open
intervals `[start, end)` are permitted.

Property order defines the stored key layout, so preserve it when replacing a
constraint. Reordering all key components consistently preserves which records
compete; it does not make two differently declared schema definitions identical.

## Algorithmic costs

Let `N` be the scoped candidate population, `g` the intervals in one group,
`w` the number of grouping properties, and `s` their total encoded value size.
Storage seeks also depend on the underlying LSM/cache state; `seek` below is not
an assertion that each access is a physical disk read.

| Operation | Applicable index | Work and limitations |
| --- | --- | --- |
| Creating a constraint over existing data | Label/type acquisition where available | Read the relevant population, form groups, then sort intervals within each group: expected `O(N * (w + s) + sum(g log g))`; retain interval/group validation state. Index acquisition does not remove validation of existing records. |
| Node write, complete temporal index, scalar-valued grouping components | Constraint-owned temporal history index | Encode the group in `O(w + s)`, seek predecessor/successor and compare intervals. Deleted/stale/excluded entries can require additional examination; this is not an unconditional two-record bound. |
| Node write, index cannot establish completeness | Complete label reader | Inspect the relevant label population and compare complete group keys: up to `O(N * (w + s))`, plus read/decode costs. Ordinary property indexes do not replace temporal-index completeness. |
| List-valued grouping components | Complete indexed group history, or complete label reader | Compare actual values, not hashes alone; a group history can require `O(g)` comparisons. Unsupported exact hash domains share candidate coverage and require equality filtering. |
| Relationship write | Relationship-type population | Current relationship enforcement examines relationships of the relevant type and verifies namespace, complete grouping key and interval. Ordinary property indexes do not supply the node temporal-index neighbor shortcut. |
| Transaction staging | Pending and committed candidates | Pending mutations and deletions participate in validation; committed-index lookup cost alone is not full transaction/commit cost. |
| Historical point read by element ID | MVCC head/version keys | Seek the entity, check current-incarnation eligibility, inspect retained newer version records for a deletion boundary, then resolve the selected version. Cost grows with examined history and undo resolution. |
| Historical label/property/type query | Scoped MVCC candidate enumeration | Stream candidates and test historical labels/properties/types. Current indexes cannot exclude old matches. Work grows with the examined population and each candidate's history; no whole-graph snapshot is constructed. |
| Historical traversal | Retained adjacency membership plus entity versions | Resolve candidate relationships and eligible endpoints at the same cutoff. Cost includes examined adjacency history, relationship history and endpoint history. |

These bounds describe the source paths, not measured end-to-end throughput.
Grouping-width costs include value encoding and equality: a large list/string
can cost more than several small scalar components. Result memory also depends
on the requested rows and Cypher operators (`ORDER BY`, `collect`, etc.); absence
of a materialized graph snapshot does not mean every query uses constant memory.

## Reproducing the overlap-kernel measurement

`BenchmarkTemporalOverlapCheckByArity` exercises the same native overlap owner
with its authoritative temporal index and the complete label-scan oracle, using
identical non-overlapping groups. Widths are 1, 2, 4 and 8; populations are 100
and 1,000 intervals. It measures validation only, not durable writes, protocol
latency, or historical `asOf` execution.

```sh
go test ./pkg/storage -run '^$' \
  -bench '^BenchmarkTemporalOverlapCheckByArity$' \
  -benchmem -benchtime=10x -count=3 -cpuprofile=temporal.cpu
go tool pprof -top -focus=scanForTemporalOverlapInTxn temporal.cpu
```

The Go CPU profile includes fixture setup as well as timed benchmark iterations;
inspect call stacks when separating index construction from the measured kernel.
Do not interpret the full process profile as a pure query profile.

## Recorded kernel sample

Apple M2 Max, macOS arm64, Go 1.27.3, 10 iterations per sample, three samples per case. Values below are medians; short runs are descriptive, not a statistical throughput qualification. CPU profiling was enabled. Both modes use the same implementation and generated group; no production implementation was disabled.

| Group width | Intervals | Indexed µs/op | Indexed B/op | Indexed allocs/op | Full scan µs/op | Full scan B/op | Full scan allocs/op |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 100 | 6.35 | 6808 | 109 | 174.33 | 176448 | 3828 |
| 1 | 1000 | 6.15 | 6808 | 109 | 1871.95 | 1753293 | 38028 |
| 2 | 100 | 6.18 | 7096 | 116 | 186.12 | 185280 | 4031 |
| 2 | 1000 | 7.15 | 7096 | 116 | 1925.67 | 1841295 | 40031 |
| 4 | 100 | 7.04 | 7224 | 121 | 205.24 | 195088 | 4037 |
| 4 | 1000 | 7.50 | 7224 | 121 | 2034.62 | 1937533 | 40037 |
| 8 | 100 | 7.85 | 8360 | 131 | 235.74 | 240968 | 4245 |
| 8 | 1000 | 8.39 | 8360 | 131 | 2427.78 | 2393813 | 42045 |
