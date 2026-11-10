# Acknowledged writes, replay and recovery integrity (PR #1690)

`RecoverIntoEngine` restores snapshot batches through the existing engine bulk
creation APIs. On a write-behind destination those calls acknowledge buffered
work; they do not establish physical completion. Buffered WAL replay instead
applies physical records through the existing native replay owner.

The snapshot phase must finish before that replay starts. Otherwise a pending
snapshot overlay masks the newer replayed properties, and its eventual drain
writes the old snapshot body over the replayed body. Retention is part of the
reported reproduction, not a reason to special-case the solution.

Recovery now completes the snapshot phase with the existing `FlushWriteBehind`
capability of the canonical unwrapped engine. This runs once at the phase
boundary, not per node, batch, or WAL entry. A completion failure is returned
before reading or applying WAL records. Engines without buffered completion
retain their synchronous behavior. Recovery completion uses its existing
independent lifetime, rather than borrowing a request that could retract already
accepted work.

No old Badger key, persisted body, WAL format, storage version or public Go API changes. The two former transaction signature adapters are
retired: tests call `UpdateGraphNode` with `NodeUpdateRequest` directly. Test-only
scheduling accessors retain the original public-consumer interleaving without
copying replay, cache, or publication logic.

## Reproduction and coverage

The original PR head is `56b1e8e2416b5c4b36d7d7751b24a08d8f1c429b`.
Ported assertions reproduce the failure on release
`b098f7e708b4c701a743ffb0310a9b179a84c8f2` under both parsers: both buffered
snapshot orders return `alpha=before-alpha` and no `beta`, including after flush
and reopen. Both synchronous controls return the acknowledged updated values.

`TestNOR140RetainedHistoryRestoreCacheAgreement` compares cached, query-node,
Cypher and direct stored-scan values after recovery, explicit flush and reopen;
it also checks the acknowledged relationship and label count. The matrix changes
snapshot ordering and destination write-behind while retaining history.
`TestNOR140BufferedReplayCacheAgreement` covers both low-memory modes with the
original controlled schedule. `TestNOR140ConcurrentReplayCacheAgreement` retains
the original concurrent implicit/direct writes and public read comparisons.
`TestRecoveryStopsBeforeWALWhenSnapshotCompletionFails` checks direct and
namespaced destinations and verifies that no WAL entry is applied after a failed
completion boundary.

The operator expanded PR #1690 to all NOR-136 criteria, including the consolidated
NOR-138 and NOR-143 restart/count, conflict, resurrection, protocol and stress
obligations. The original NOR-140 checks alone do not qualify that broader scope.
Current commands and actual results are recorded in the PR; historical receipts
and the superseded, incomplete slice-only full gate are not promoted to final
qualification.

## Reuse review

The correction reuses `UnwrapEngine` and `FlushWriteBehind`. Snapshot export also
waits for buffered completion, before capturing a snapshot and truncating WAL;
restore waits after accepting snapshot batches and before applying later WAL.
Those are opposite sides of the snapshot lifecycle. Their common work is already
owned by `FlushWriteBehind`; neither duplicates its implementation. Sharing the
surrounding export/restore orchestration would mix different inputs, operations,
error contracts and resource ownership without reducing the common work.

The remote code graph is stale relative to this release. No clean current-head
duplicate-checker result is claimed until an actual completed analysis provides
one. The checker remains informative under the operator policy and is unmodified.

## Acknowledged peers participate in conflict validation

The existing buffered-delete conflict owner now validates competing creates,
updates and embedding updates as well as deletes. A persisted MVCC head cannot
represent an acknowledged update still waiting in the buffer. Previously two
implicit writers could both succeed using the same old image; the later image
then erased the peer's property or increment. The existing lazy write sets,
version ordering and transient conflict path are reused, with no additional map
or alternate commit path. Canonical callers use `bufferedWriteConflictLocked`.

The release reproduction loses acknowledged properties over Execute, Bolt and
HTTP with either parser. Six-worker, 120-increment runs return 120 successes but
store only 37–47 increments. Corrected tests require all 120 after flush and
reopen for autocommit, explicit and mixed producers. Explicit stale transactions
and synchronous writes were passing controls, not newly attributed fixes.

## Recovery completes bodies and their derived counts

Semantic WAL records can queue writes before a physical buffered record. Those
writes now complete through `FlushWriteBehind` before physical replay, including
WAL-only recovery. Native accepted-commit recovery also completes preceding
writes before replay. The shared `completeRecoveredCounts` finalizer completes
remaining accepted writes and reuses the existing authoritative label and
relationship-type count verification once per recovery batch, including an
applied prefix followed by a reader failure. Original errors are retained.

A reduced release reproduction restores a relabel from Old to New but leaves
Old=1 and New=0. Deleting that recovered node then underflows the New counter;
Close and the next Open both fail. The regression covers native synchronous and
buffered destinations with and without snapshots, plus ordinary and
transaction-aware recovery. It checks node labels, relationship types, label and
positional relationship counters before the next delete, then close/reopen and
absence of the deleted node and incident relationship.

## Broader controls and canonical test owners

Native tests exercise write-behind off/on and LowMemory off/on, independent
physical-body recounts, mixed updates, actual property renames, relabels, cascade
deletes, crash recovery, old-image prevention, no resurrection and label-scoped
UNIQUE transitions. Concurrent stress uses seeds 136, 138 and 143, each with buffered flush/reopen,
buffered crash/replay, and synchronous flush/reopen routes: nine runs total, with
three workers and 13 operations each (39 acknowledgements per run). Public mutation tests repeat relabel, key reuse, duplicate rejection,
property rename and cascade deletion across Execute/Bolt/HTTP and autocommit/
explicit producers before and after reopen. Both parsers execute the same public
matrix in separate processes. A sole Doc holder relabeled Parked releases the
Doc-scoped key; a real remaining Doc holder must reject a duplicate.

The common protocol server fixture owns real startup/shutdown and directories;
the existing PR1655 fault tests use that owner rather than copying startup code.
One physical-node reader and one label/property identity lookup serve all tests.
Native test commits reuse `bufferedWrite` or the existing bulk transaction owner.
No new local replay, publication, count-rebuild or transport implementation was
introduced in the test fixtures.

The initial advisory checker extracted the repository but timed out after 60 seconds;
its graph revision was older than the release base. That is incomplete analysis,
not a clean result. Manual ownership review above covers the changed causal
owners; checker rules, hooks, thresholds and exclusions remain unchanged.

## Workload cost

Baseline `b098f7e708b4c701a743ffb0310a9b179a84c8f2`, Apple M2 Max,
GOMAXPROCS=1, GOGC=100. Identical real-WAL benchmark harness, fixed 1/64-node
populations and 128-byte property payloads. Six samples per side/case in ABBA
order; 2,000 iterations at one node, 500 at 64 nodes. Medians:

| Nodes | Mode | ns/op base → fix | B/op base → fix | allocs/op base → fix |
|---|---|---:|---:|---:|
| 1 | buffered-ack | 29,748 → 29,261 (-1.6%) | 15,161 → 15,160 | 125 → 125 |
| 1 | buffered-ack-drain | 47,335 → 47,518 (+0.4%) | 24,314 → 24,314 | 315 → 315 |
| 1 | synchronous | 30,219 → 30,239 (+0.1%) | 21,156 → 21,156 | 333 → 333 |
| 64 | buffered-ack | 551,200 → 546,542 (-0.8%) | 549,496 → 549,494 | 4,266 → 4,266 |
| 64 | buffered-ack-drain | 957,174 → 959,778 (+0.3%) | 801,870 → 801,866 | 8,573 → 8,573 |
| 64 | synchronous | 1,173,248 → 1,170,459 (-0.2%) | 940,624 → 940,624 | 14,075 → 14,075 |

Buffered ACK excludes the eventual drain; ACK+drain includes it. All cases verify
latest values and the fixed physical population outside the timed loop. Allocation
counts are identical; small byte differences come from persistence bookkeeping.
This qualifies the named update workloads, not every concurrency or ingestion
shape. Separate CPU profiles are dominated by syscalls and memory copying; the
short profiles include setup and are not evidence of a new hot-path regression.

The controlled public schedules stop the existing background worker explicitly;
a long adaptive interval alone does not prove a peer remains unflushed. They
assert the physical body is still the seed and the logical read is the accepted
peer value before resuming the stale writer. Concurrent increment stress retains
the normal worker. A separate copied-WAL test appends an invalid record after a
valid relabel/type update, verifies the failure is retained in ReplayResult and
requires the successfully applied prefix's bodies and counts to agree.

Final review reproduced explicit-ID create/create races for both nodes and
relationships on the release baseline: both creators were acknowledged. The same
conflict owner now includes creates in both the committing write set and the
accepted-peer scan. Tests require a conflict/already-exists error, unchanged
buffer population after rejection, one peer-owned entity, exact counts and
indexes, and only the peer relationship's adjacency before and after reopen.

## Completed remote duplicate analysis

The later unchanged checker completed against `192.168.1.30` for review commit
`332543ff5`: 76 functions analyzed, two reuse findings, no refactor targets and
no skipped candidates. Both were verified against current source despite the
stale indexed baseline, then consolidated: UNIQUE checks share the existing
parameterized assertion and typed extractor, and the corruption-fixture tests
share one setup with the union of their assertions. The discovered list-error
forwarder is retired as well. See the [accepted completion tests](../../pkg/storage/accepted_completion_fence_test.go) and [snapshot completion tests](../../pkg/storage/wal_snapshot_completion_test.go).
These are test-only consolidations; the qualified production runtime is unchanged.
