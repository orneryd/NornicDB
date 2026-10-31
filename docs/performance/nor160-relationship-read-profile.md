<!-- Author: Codex, NOR-160 implementation sub-agent, 2026-10-26. -->
# Relationship reads in primed explicit transactions

This fixture investigates [NOR-160](https://linear.app/nornicdb/issue/NOR-160/relationship-reads-in-explicit-transactions-are-5-10x-slower-than-in).
The correction reuses the transaction's existing bounded committed-snapshot
images and transfers private relationship properties through namespace conversion
without another deep copy. The measured cause classes were allocated before
product changes. Corrected-head qualification is in progress; no speedup claim
is made yet. The merged NOR-150 fresh-transaction fix is a separate investigation.

Both node read forms also distinguish a missing MVCC head from a hidden or
tombstoned image. Only an absent head permits the existing legacy body fallback;
a canonical visibility miss cannot be turned back into a visible primary body.

Set `NOR160_MAX_RATIO=2` with `NOR160_FULL=1` to assert that the primed explicit
read costs at most twice autocommit across the timing rounds. Complete row
multisets are checked independently of that timing assertion. The benchmark
`BenchmarkNOR160RelationshipRead` uses the same setup, query and result oracle;
its timer excludes setup, BEGIN, priming and row-oracle encoding.

The node cache retains the existing byte and entry limits. Each entry records
its hydration and known properties, so a partial projection cannot answer a
complete read and metadata without vectors cannot answer a vector read. Already
hydrated sidecar images stay pinned to the original snapshot. Cached point and
adjacency images still pass the existing live visibility policy; projected hits
apply it to exactly the requested properties. Header capability decline rules
remain unchanged. Historical
providers and latest-committed edge identity reads keep defensive conversion
because their maps may be borrowed. Write-behind adjacency detaches at the
transaction boundary before namespace conversion. These ownership distinctions
must be included in the final population and performance report.

Merge after #1644 and rebase when it lands. Preserve its view/capability contract
and #1548's engine-cache publication ownership. Relative to #1548, whichever
lands second restacks. No engine publication or invalidation path changes.

`TestNOR160RelationshipReadProfile` sends queries through the Neo4j driver and
the existing Bolt integration server with real explicit transaction support.
Both session routes use write access mode. The explicit route starts one
transaction, consumes two priming statements, runs the pair statement, and
rolls back. The autocommit route consumes those priming reads as independent
statements before the identical pair read. No writes are pending in the read
transaction. Both modes check every returned value and duplicate against the
same independently generated expected multiset.

The default test uses a small synthetic graph. `NOR160_FULL=1` creates 70,802
nodes and 96,160 relationships, with 10,000 Held nodes, 106 active endpoints,
5,101 requested pairs and 1,390 expected results. These are deterministic
synthetic data; matching global cardinalities does not make this the original
reporter's graph. One label/property index exists on each endpoint's thing_id;
the document id used in priming is also indexed. Disk-backed Badger uses
SyncWrites, without a write-behind wrapper. Data loading is outside timing.

The two measured statements are:

```cypher
UNWIND $rows AS row
MATCH (:Held {thing_id: row.i})-[co:COHELD]->(:Held {thing_id: row.t})
RETURN row.i AS i, row.t AS t, co.n AS value
```

```cypher
UNWIND $rows AS row
MATCH (a:Held {thing_id: row.i})
MATCH (b:Held {thing_id: row.t})
MATCH (a)-[co:COHELD]->(b)
RETURN row.i AS i, row.t AS t, co.n AS value
```

The first priming statement gathers document holdings and citations in two
correlated CALL bodies. The second reads the active Held nodes' holders values.
Their complete texts are constants in the test. The pair parameters stay
identical; a changing unused nonce prevents result-cache hits. Two timing rounds
reverse route order. These observations are diagnostics, not a statistically
qualified base/head benchmark or an assertion that a timing threshold is met.

After committing and pushing the fixture, open its draft WIP PR and submit
through the shared workspace's `sig/tnrun.sh`. Never run the workload locally.
Set `TNRUN_NO_SUPERSEDE=1` and force both parsers for this Bolt-only fixture:

```bash
TNRUN_NO_SUPERSEDE=1 TNRUN_FORCE_BOTH=1 bash sig/tnrun.sh go \
  wt-nor160-relationship-profile-v144 --pr <PR> --expected-head <SHA> \
  --parser both --shards 1 --fresh --timeout 45m \
  --env NOR160_FULL=1 --env NOR160_PROFILE=1 \
  -- ./pkg/bolt -run '^TestNOR160RelationshipReadProfile$' -count=1 -v
```

Use `NOR160_SHAPE=original` or `bound` to select one statement, and
`NOR160_ROUNDS=1..6` to change the diagnostic repeat count. CPU and sampled
allocation profiles start after setup and priming. Each profiles at most three
complete reads and stops once one second has elapsed. Allocation sampling uses
64 KiB; its profile is the difference between cumulative allocated-space
snapshots, not retained heap. Runtime allocation counters include the server,
driver and other goroutines in this isolated test process.

`NOR160_FIXTURE`, `NOR160_MEASURE`, and `NOR160_PROFILE_SUMMARY` are JSON log
records. `NOR160_PROFILE_DATA <shape> <mode> <cpu|alloc> <base64>` preserves raw
gzip pprof bytes in the durable job output. Decode that field to a `.pprof` file
for analysis. No worker-local profile file is needed. Inspect both flat and
cumulative stacks and distinguish the synthetic observation from the original
saved-store evidence. Keep the PR draft and report causes before product edits.
