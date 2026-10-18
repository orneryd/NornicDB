# Relationship identity projection consumes ~12 GiB and fails connections

Draft for the orchestrator; do not post. Pinned NornicDB v1.4.3 server
`f2ddd27cfae3788076778a0a0d48c827df046a3b`, digest
`sha256:b8fa89956239e6d20d0738a94d4e7c74e59d377814bb0fdf06b7a89f90221d73`.

## Reproduction and observed result

Full develop source `16923aef778025fab3490f6a57549763b536f3d5` in its private
`nornicdbcode` database: 90,032 code/function nodes, 3,250,722 relationships
(1,306,328 graphify relationships, 1,944,394 CALLS_GO), about 32 million source
body characters, plus 1024-dimension native embeddings as available. Nodes have
unique `(id,repo)` and `_ci_key` constraints and native property indexes.
Read identities/hashes through Bolt with fetch size 10,000:

```cypher
MATCH ()-[r]->()
RETURN startNode(r).id AS a,endNode(r).id AS b,type(r) AS t,
       r.site AS site,r.props_hash AS hash
```

Explicit query timeout **900 s** is reached; whole refresh request
`c1792331813760-43fb8300` lasts **1084.464 s**, failing before edge writes.
Other connections report socket read timeouts / SessionExpired. The database
**remains running**; this incident is not a measured OOM kill or restart.
At the time the server limit was 12 GiB / Go soft limit 10 GiB.

Live pprof: HeapAlloc **8,067,670,560**, HeapInuse **10,442,850,304**,
HeapSys **13,027,016,704**, MaxRSS **12,840,038,400 bytes** (~11.96 GiB).
Whole-process samples include other activity; this is not an isolated incremental
allocation measurement. Raw pprof and EXPLAIN: `sig/codeintel/native-diagnostics-latest.json`.

## Plan/profile and smaller control

EXPLAIN: `AllNodesScan → Expand → ProduceResults`. Live stacks include
`relationshipEndpoint → rowEndpointNode.eval → rowPropertyNode.eval →
pipelineProjectReturnRow`, within the Bolt result stream. In v1.4.3,
`pkg/cypher/row_expression.go:StorageExecutor.relationshipEndpoint` calls
`GetEdge`, then `GetNode`, for each endpoint expression. This identifies repeated
endpoint work; it is not a complete allocation attribution.

`sig/codeintel/read-probe.py` gives a bounded comparison on the same graph,
PROFILE enabled, 10,000 returned rows per statement. Receipt `read-probe-latest.json`:

| Projection | Seconds |
|---|---:|
| `MATCH (a)-[r]->(b) RETURN a.id,b.id,... LIMIT 10000` | 6.50958 |
| `MATCH ()-[r]->() RETURN r.site,r.props_hash LIMIT 10000` | 2.71654 |
| Original endpoint-function projection with `LIMIT 10000` | 9.25031 |

These are concurrent-load samples, not extrapolated full-scan times. To reproduce
the large case, load the data shape above and execute the exact original query
with a 900 s driver query timeout while collecting pprof and container cgroup
metrics. Keep results streaming; do not introduce client per-row HTTP requests.

Expected: stream scalar identities with memory proportional to working set or
fetch window, reuse endpoint identity/property reads, honor cancellation, and keep
other connections usable. A small output projection should not require retaining
gigabytes of repeatedly materialized node bodies.

## Branch status

The original full-develop runtime above is measured on v1.4.3. Release/v1.4.4 at
`24d08776cdb9cb624da1073007da437306518426` retains the same GetEdge/GetNode
implementation; RC `9f00d5456ea8dca4e64306c861f91fa325ff9b51` has no change to
that v1.4.3 source file. Develop `16923aef778025fab3490f6a57549763b536f3d5`
**differs**: row endpoint memoization, `getQueryNode`, and a GetEdge check only
for potential deleted-relationship views. Do not claim identical develop behavior:
repeat the large query on its central build to establish whether those changes
resolve time and memory. Separate develop/release runtime is not measured.

**Current RC runtime comparison:** on exact
`39e3f7647e034ebe7bf23b067cd161a915700b98`, a separate scratch instance with
54,000 indexed nodes, 4096-byte bodies and 1,300,000 CALLS relationships executes
the exact same identity query. At a 5 GiB cap / Go soft limit 4 GiB it returns
no rows before **95.920167 s**, then loses Bolt; sampled peak **5,369,012,224**
bytes. Supervisor records exit **137**, and OOM-kill counter increments 1 → 2
after the prior labelled-count test. EXPLAIN is still AllNodesScan → Expand →
ProduceResults. This is an additional, smaller synthetic dataset and a smaller
memory cap than the original full-develop observation: do not compare 95.9 s
directly to the original 900 s deadline as a speed improvement.

Raw receipt `sig/codeintel/compare-repros-39e3f7647e034ebe7bf23b067cd161a915700b98.json`,
executable source `compare-repros.py`. A matching fresh v1.4.3 same-shape run is
in progress to supply a direct comparison. The RC does not eliminate this
identity-projection memory failure.
