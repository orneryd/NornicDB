# Graphify relationship transaction conflict

## Current conclusion

The repeated dogfood ingestion failure is confirmed by retained workflow logs,
but its root cause is **not established**. No production correction, data
corruption finding, or completed reingest is claimed by this investigation
checkpoint. The dogfood database and its volume have not been changed.

Release examined: `8f5835d7f2fbb9c1253a046ef4b9b64c2da06af1`.
This work is separate from CI PR #1736 and accepted-command draft #1691.

## Observed failure

- [Run 42020171895](https://github.com/orneryd/NornicDB/actions/runs/42020171895)
  and [run 42108551854](https://github.com/orneryd/NornicDB/actions/runs/42108551854)
  fail importing the rolling release code graph into `nornicdbcode`.
- The latter logs 3,060,000 processed edges, then returns
  `Neo.TransientError.Transaction.Outdated: SET r +=: conflict detected: edge
  nornicdbcode:merge-0624e1ecab8de2941dd9ea20a37bd4007b41153f2f283d37bb7facf33c46fb4e
  changed after transaction start`.
- The reported relationship is `IMPORTS_FROM`, from
  `pkg_search_rerank_mmr_extra_test` to `internal_testfixture_archive`, scoped
  to repository `orneryd/NornicDB`.
- The operator reports the last completed ingest marker remains
  `35e3bddbf092f7cf9dd56ecfd15a32795cabc0d6`. This has not been independently
  re-read from the dogfood database in this investigation.

The importer's progress counter also counts skipped unchanged edges. Therefore
3.06 million processed edges is not evidence that the failing request wrote that
many relationships or that a storage-size threshold was crossed.

## Relationship to existing work

[NOR-288](https://linear.app/nornicdb/issue/NOR-288) and
[draft #1691](https://github.com/orneryd/NornicDB/pull/1691), reference head
`182893c0dd15db761ef604c7a6eefb285e4d7b64`, own settlement after durable acceptance,
native application conflicts, retained authority, semantic replay, and DROP.

The observed `SET r +=` prefix identifies statement-time execution. Relevant
current owners are `BadgerTransaction.UpdateEdge`,
`getCommittedEdgeForUpdateLocked`, `readEntityAtWithView`, and `getEdgeAtInTxn`.
A snapshot read reporting absent/not-visible followed by a live latest read is
classified as a retryable conflict. That classification is intentional for a
real post-BEGIN peer write. #1691 retains this classification; its accepted
settlement loop is not a direct correction for the observed statement error.

This establishes different immediate paths, not proof that accepted-command
history or another writer cannot contribute indirectly. No #1691 stack or codec
change is copied into this work. NOR-213's broader writer lock-wait contract and
NOR-371's replay-eligibility work are also not silently absorbed.

## Isolated evidence

All probes use disposable native Badger storage through the public Cypher
executor and the importer's exact statement:

```cypher
UNWIND $rows AS row
MATCH (a:Code {id: row.src, repo: $repo}),
      (b:Code {id: row.tgt, repo: $repo})
MERGE (a)-[r:IMPORTS_FROM]->(b)
SET r += row.props
```

| Probe | Result | What it establishes |
| --- | --- | --- |
| Four rounds over 2,000 distinct relationships, 1,000 rows/request | Pass, 0.81 s test | Clean native batched creation and updates work; exact counts and property bounds checked. |
| One relationship repeated 1,000 times/request, three rounds | Pass, 0.13 s test | Duplicate pairs alone do not reproduce the failure; one final relationship and last-row values checked. |
| Surrounding 2,000-row batch from local release extraction, three rounds | Pass, native 0.98 s and WAL-backed 0.92 s | Local captured scalar payload works on clean native and WAL-backed stores; final count checked. |
| 1,550 rounds over 2,000 relationships, 3.1 million total updates | Pass, 336.34 s test | Accumulated sequential updates alone do not reproduce it in this fixture. This is not 3.1 million distinct relationships or the dogfood history. |
| Six supported-history controls: memory, persistent retained/pruned history, edge deletion/recreation, endpoint deletion/recreation, reopen, head-only pruning with ID reuse | Parent independently passed 24-iteration matrix, 0.78 s test; source SHA-256 unchanged before/after | These supported serial histories do not reproduce the failure in the probe. They do not qualify dogfood history or concurrent writers. |

The local release extraction contains 72,209 nodes and 3,734,421 edges. It places
the watched relationship at processed edge 3,064,551, in the Code-to-Code
IMPORTS_FROM bucket at position 2,928,715. This aligns with the reported failure
region but does not reproduce the failure. The extraction differs from the CI
population (71,730 nodes, 3,724,083 edges), including platform/header parsing
differences. Stored hashes determine which rows the incremental importer writes;
without that inventory the captured full bucket is not the exact failed HTTP
request. No private or full dogfood replay is claimed.

Initial CGO-disabled Mac qualification did not build because of unavailable
Metal symbols. Native CGO checks with `-tags 'noui,nolocalllm'` passed as listed;
UI assets, local LLM integration, complete affected packages, `make test`, and
`make verify` are not qualified by these selected probes. `make verify-list`
only previews checks and is not a verification pass.

Probe source, captured rows, complete logs, and the independent DeepSeek source
comparison are retained in the local task evidence directory. Passing diagnostic
probes are not presented as a regression for a defect they have not reproduced.

## Required next evidence

The active Mac Docker context and its default context currently list no running
containers. The operator has been asked for the actual dogfood Docker host/context
or a disposable native backup. A native copy must retain the MVCC head, version,
prune-floor and sequence metadata; a Cypher-only export cannot establish the
original history state.

On that isolated copy, capture the failing request's rows, BEGIN physical and
logical read versions, current relationship head/floor, corresponding retained
record, and concurrent writer activity. Distinguish a genuine intervening commit
from absent/inconsistent historical metadata before choosing a correction.
Preserve the real peer-write conflict and cancellation controls. Do not bypass
the conflict, silently refresh a pinned snapshot, delete the relationship, or
increase retry counts as a substitute for fixing the cause.

## Reingest remains gated

Do not reset or reingest dogfood as an experiment. After the owning correction
is reproduced and verified, review a single-writer incremental reingest against
a frozen release commit, using the existing certificate/authentication and
deletion safeguards. Only a completed run followed by a read of the repository's
exact `last_commit` marker can establish completion. Neither a proposed run nor
partial progress is a completed graph refresh.
