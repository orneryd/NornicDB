# #1406 / #1526 parked WIP — 19 October 2026

User explicitly PARKED this lane under `sig/handoff/lane-cost-discipline.md`, item 4. No work resumes until the orchestrator wakes the lane. No new jobs were submitted after that instruction. Existing remote jobs remain durable. No PR-head push, PR-state change, maintainer comment, or merge in this round. This WIP is NOT ready or approved.

## Recovery and exact publication guards

Repository root: `C:/Users/mm0ns/claude/nornic-pr387-test`; product repository `src`.

- Release WIP branch: `wip-1406-command-owner-r13-20261019`.
- Develop WIP branch: `wip-1526-command-owner-r13-20261019`.
- Mutable release checkout: `wt-f1406-r13-rel`, HEAD `0ed42a61bec12c9179aa3ce78ae81cc509c6205d`; staged complete WIP saved separately without moving this pinned HEAD.
- Mutable develop checkout: `wt-f1406-r13-dev`, HEAD `6d9fcd8096f5f88ff79860a396b6e426588033e6`; same strategy.
- PR #1406 branch `fix/unindex-deleted-node-snapshot`, exact own last-pushed lease **d86154868fca7f2a47c1055b8cc97e8e8f3b9dae**.
- PR #1526 branch `fix/unindex-deleted-node-snapshot-develop`, exact lease **f00c63dfb41cf81380a40e3b35d143a663911492**.
- Future authorized PR pushes must use `--force-with-lease=<branch>:<full-own-sha>` exactly. Never bare force; preserve external updates. Leave READY unchanged. Latest instruction forbids posting to the maintainer.

WIP commits preserve TJ's existing ancestors. WIP saving must not move any worktree HEAD with live jobs. Use the WIP branches for source recovery, not a PR-branch reset. Current handoff is also copied into both WIP trees at `docs/plans/publication-1406-r13-resume.md`. The local pending-job ledger is `scratch1017-pub/r13-parked-pending.json`.

## Scope and accepted findings

Independent xhigh review: `sig/handoff/reviews/review-1406-1526-xhigh.md`. Rule 73 is ACTIVE; the earlier LOW-effort r8 approval does not count. Three accepted P1s must be handled as one accounting/replication causal unit:

1. Implicit/explicit public Cypher transactions bypassed replication. Raw `BadgerTransaction` commits skipped the write router. Retained `TestXHighCypherTransactionsPreserveReplication`: release red i20261019183614-83496-f442 / i20261019183622-83496-3334, develop i20261019183656-80116-74f9 / i20261019183703-80116-6952. Older matched-base red i20261019183830-91840-63c4 / i20261019183838-91840-e418.
2. Timed-out replicated proposals lost their quota reservation while still able to commit. Retained `TestXHighLateReplicationQuotaAdmission`: release i20261019183117-91668-4274, develop i20261019183221-82136-ab22; both red. Pending logical growth must remain reserved until definitive rejection/publication, including callback retirement and fresh replay.
3. Rejected staged managed writes remained replayable from standalone WAL envelopes. Retained `TestXHighAbortedManagedWriteDoesNotReplay`: release i20261019183305-48936-7317, develop i20261019183349-52836-dfa8; both red. Journal begin/body/outcome must distinguish accepted physical commits from aborted/incomplete attempts.

Earlier bot P1 replication-wrapper bypass is included in (1). Failed committed DROP fencing belongs to #1503/#1504; inherit their final lifecycle correction and resolve the #1406 thread only with actual composed evidence. Bot threads: #1406 review comments 4314944634 (DROP fence), 4316004706 (wrapper preservation). Do not falsely claim the current parents are final.

## Ordinary-DDL boundary: explicit user decision

**Ordinary autocommit CREATE/DROP INDEX stays native metadata work.** Remove our temporary implicit-transaction wrapping; do NOT implement complete automatic-DDL replication in this family. Implicit/explicit DATA transactions and EXPLICIT schema transactions remain in scope.

Done on both mutable copies: `executor_entry.go` restored to native `isSystemCommandNoGraph(...) || isOrdinarySchemaDDL(...)` path. `TestManagedSchemaTransactionReplicatesAndBackfills` now uses public BEGIN / statement / COMMIT for CREATE INDEX, DROP INDEX, CREATE CONSTRAINT and rejection. No production auto-DDL replication feature is claimed.

Reason: temporary wrapping regressed `TestMonster531ConcurrentIndexNameAdmission` (guarded name admission returned snapshot conflicts) and `TestCreateLookupIndexReportsPersistFailure` (isolated schema view hid the native persister error). Both existing tests red under both parsers: i20261019204130-42468-446f / i20261019204142-42468-8245. Other broad schema chunks passed. Keep their assertions unchanged.

User separately requested public replicated autocommit-DDL repros on release and develop and bases. If pre-existing, record behavior, jobs, paths; orchestrator decides 1.4.4 versus 1.5.0. Do not fix it in this PR. Investigation fixture retained at `sig/handoff/reviews/f1406-autocommit-ddl-replication-probe_test.go`. A CREATE row checks catalog convergence; an independently seeded DROP row checks DROP without depending on CREATE propagation. Latest root fixture is self-contained and includes a successful data-replication control.

Submitted current/before-r13 jobs used the earlier equivalent fixture with the existing managedRouteReplicator helper (no data-control addition). Results NOT collected yet; do not call the gap confirmed. Current probes temporarily lived at `pkg/replication/autocommit_ddl_investigation_test.go` in the dirty snapshots; that file was REMOVED from both WIP product trees after all ACKs. This is investigation evidence, not a permanent knowingly failing test.

Public native paths: `cypher/executor_entry.go` selects executeWithoutTransaction; `cypher/schema.go:executeSchemaCommand` serializes shared schema mutation and uses native SchemaManager/persister; `cypher/schema_transaction.go:prepareSchemaTransaction`/StageSchemaChanges applies only for explicit transaction wrapper; ReplicatedEngine embeds/delegates GetSchema while command routing owns graph writes. These are SOURCE inventory, not a code-intel completeness claim.

Prepared but NOT submitted (park occurred first): `r13-ddl-plain-rel.json` / `r13-ddl-plain-dev.json` for clean mainline release e04cf3a7b / develop 16923aef7 with the self-contained fixture. Worktrees `wt-f1406-r13-ddl-plain-rel` / `-dev`. Submit only after wake authorization if still useful. Existing pre-r13 published-base worktrees are `wt-f1406-r13-ddl-base-rel @d86154868` / `-dev @f00c63dfb`.

## Implemented owner design (five lines)

1. `multidb.commitTransaction` remains the single admission/accounting owner; it chooses revision-only, existence/count, or canonical stored-image/byte work from active limits and initialization.
2. Unlimited/uninitialized direct CRUD uses a shared lease and the existing atomic native command through replication/WAL/ordered-publication wrappers; no general Badger transaction, SI validation, or extra namespace metadata commit.
3. Count/byte modes fold one hidden-inclusive stored prior and one final stored image per ID; the same accounting batch owns admission/publication for managed, transactional and policy-maintenance writers.
4. Replicated logical commands retain positive capacity reservations across uncertain outcomes; retired snapshots never run again, and late apply uses immutable detached replay through the same owner.
5. Atomic command journaling publishes a committed outcome only with physical success; replay excludes aborted/incomplete attempts and preserves wrapper, notification, cache and accounting publication boundaries.

## Source inventory and WIP implementation

- Accounting: `pkg/multidb/storage_transaction.go`, storage_accounting_batch.go, storage_command_reservation.go, storage_size_tracking_engine.go, enforcement.go, quota_typed_properties.go; storage entity_accounting.go projection protocol. Develop retains its own storage_accounting_observer.go differences.
- Storage protocol: managed_command.go, managed_write_transaction.go, managed_replay.go, managed_schema_replay.go, native_command.go; command_journal.go, replication_wal_replay.go, WAL/commit writer, Badger publication/buffer paths.
- Replication: managed_write.go, managed_transaction.go, storage_adapter.go, namespace_publication.go, codec.go, raft.go, ha_standby.go, replicated_engine.go; version identities in storage replication_versions.go/MVCC/transaction/buffer.
- Public commits: both `cypher/executor.go` implicit commit and explicit transaction owner call `commitStorageTransaction`; manager wraps raw transactions in NewManagedTransaction and preserves full transaction atomicity.
- **Latest bare-engine completion, green not yet collected:** commitStorageTransaction retains the ORIGINAL full wrapper chain. If no manager committer exists, `storage.CommitWrappedTransaction` selects the same managed command/journal protocol only when a router exists. Read-only/nonreplicated raw transactions keep their direct commit boundary. Late apply captures immutable ReplayCommand and full outer engine, preserving outer WAL; no divergent quota formulas added.
- Latest bare-engine red i20261019213410-27396-9bcf showed all8 cases Apply_calls=0 (WB x outer WAL x implicit/explicit). ANTLR red twin i20261019213415-27396-b665 not yet collected. Same batch's concurrent guarded-DDL row was red before boundary restoration.
- **Latest UNTESTED addition after submitted green-snapshot capture:** bare-engine test now also rejects a command, times out another, retires caller snapshot, then applies the retained command on local/peer and expects exactly2 accepted records. This late-apply extension has NOT been submitted. Do not claim it passed.
- Latest concurrent schema test drains all8 goroutine results before assertions so a failing assertion cannot strand writers during cleanup.
- Conditions carry per-command UUIDs across peers/snapshots to catch ABA and original-snapshot conflicts without equating peer-local MVCC clocks. Nonreplicated writes avoid that cost. Count-only uses hidden-inclusive existence projections; byte mode still normalizes BOTH stored prior and final through codec because acknowledged overlays may not be canonical.
- Typed quota values use the existing durable typed-property projection for logical size calculation; scalar primitive quota semantics/storage bytes are unchanged. Temporal/point concrete types are registered with gob for transaction wire commands.
- WAL attempted transaction is begin/body/outcome, committed only at ordered successful publication; incomplete/abort attempts do not become standalone accepted redo. Recovery keeps legacy undo/error handling. Accepted hidden relationship replacement is journaled as physical update.
- Preserve guarded CREATE cache hook `cacheOnEdgeCreatedWithCounts(op.Edge, !op.ReplacesExisting)` and develop `HoldsMergeKey: len(tx.mergeKeyReleases) > 0`. F3 single lock order and whole-buffer ordering remain. Develop adaptation keeps early exclusive gate/deferred unlock, embedding sidecars, notification copies and DROP cleanup; do not copy release-only drain/WAL behavior wholesale.

## Storage-stack public quota acceptance (required again after composition)

Review `review-1520-1532-1536-1541-xhigh.md`: release filtered prior reads counted suppressed prior as absent; develop raw implicit commit bypassed byte publication. Permanent stronger probe is already in BOTH family trees: `pkg/server/suppressed_relationship_quota_test.go`, blob **91a3b0b5e8cb1d01d9542e1e479d677ed23d9392**, release commit7c7752068. Versus retained stack_xhigh_quota_probe_test.go, adds explicit Bolt/HTTP routes. **16 cases = routes4 x write-behind2 x quotas2**, with unchanged-payload retry. Do not add a duplicate.

Both current older-parent source copies PASS under race, both parsers:
- release default i20261019210414-90632-0595 / ANTLR i20261019210424-90632-f9c5;
- develop default i20261019210527-52484-139d / ANTLR i20261019210535-52484-5f01.

These are NOT final combined-stack proof. After final predecessor relay, verify actual ancestry/content of #1520/#1532/#1536/#1541 (and develop twins as applicable), then rerun permanent public16case acceptance both parsers on both FINAL compositions. The shared stored-pair owner uses GetStoredNode/GetStoredEdge and implicit/explicit commits enter the publisher.

## Verification already collected (limits matter)

All jobs remote through tnrun, no local workloads. Focused checks passed on pre-latest-bare/schema source:
- release replication typed/hidden images, hidden replacement peer and journal checks default i20261019211318-85928-99d7 / ANTLR i20261019211317-85928-4bdb; develop i20261019211350-87308-24f8 / i20261019211353-87308-3ccc PASS.
- storage journal/recovery default+ANTLR release i20261019211317-85928-958d / i20261019211323-85928-32eb; develop i20261019211350-87308-16fa / i20261019211358-87308-f99b PASS.
- all60 then-changed Go files remote gofmt release i20261019211334-59760-4f4c / develop i20261019211401-80856-1a61 clean. Latest bare helper, test extensions and explicit-schema fixture edits postdate that formatter and need remote format check after wake.
- focused multidb both parsers release i20261019210116-61928-44ef / i20261019210124-61928-f6f9; develop i20261019210228-33060-6141 / i20261019210235-33060-ffa7 PASS.
- Existing broader schema chunk0 default/ANTLR i20261019204009-82032-d103 / i20261019204021-82032-213e PASS; chunk2 i20261019204227-85696-eb19 / i20261019204238-85696-fea5 PASS. Chunk1's two real DDL regressions above are not waived.
- Older broad focused replication/storage jobs failed on our new fixture assumptions and a legacy missing-delete error fixture. Those fixtures were corrected and narrow image/recovery reruns passed; do not call the older failed packages PASS. Retain logs `r13-converged-results.log` / `r13-images-results.log`.

## TJ tuning, attribution and cost

Read both maintainer comments before redesign:
- https://github.com/orneryd/NornicDB/pull/1406#issuecomment-6244578041 (claimed tuning lane; held parallel work at that time).
- https://github.com/orneryd/NornicDB/pull/1406#issuecomment-6252598000 (measurements and residual update overhead).

TJ's original0e4c36b90 -> final7b556e21b, baseline128bd67aa: direct deletes20.1% faster and transactional deletes9.8% faster than original; direct updates+5.0%, transactional updates+8.3% versus baseline;8writer throughput effectively unchanged. M2Max/Go1.26.9/5rotatingrounds, allocations not RSS. Cite the result comment in final cost body; do not compare absolute timings across hardware or call all residual overhead inherent.

Keep TJ Sweet <timothyswt@gmail.com>, author dates, complete attributed patches:
- original perf5874f2db6df1ecf05007715b80d63bf05b219d02, stable patch ID **2f16129f56e87c262d2adad9ff13e391ca1f94aa**;
- original docs7b556e21b57ad1162d6c17d6132026cbdd340845, stable patch ID **5a7dffa0d05aeb098854d48d5b830c12c34afe43**;
- release attributed440ae0edda60f8171d1aa36d7446eefc39e226dc /4479a24b6c92b7f2d4e85d141c4345e246d185b1;
- develop attributedab5ec7219fafa45b57fe79dda4161c77a064cbd4 /725068ce4d181c263697fbeb4e0ca498fde2d162.

Both patch IDs/blame tuned lines55–60 badger_publication_stage.go rechecked on clean candidates at21:23; exact. That file is untouched in this round. Keep historical docs/performance/publication-1406/README.md intact. Absorbed#1548 deterministic cache/delete test blob **9d6230e6b30768ecdb0c16c9cd4bb4d656bcfa44**, release4b3dc29b6 /developba15767cb3777b67a975f2b63d493612b720ca4e retained.

Cost baseline before r13: final-source A/B **a20261019171415-63132-f0de**, unlimited node41.3225->57.812us (+39.9%), relationship31.3495->51.0545us (+62.9%); count43/63.8%, tracked43.2/42.7%, bytes13.5/18.1%; tx controls2.14/.81% nonsignificant. Profile **i20261019183305-48200-e9d9** confirmed avoidable general transaction envelope/prior images/SI/extra metadata/exclusive admission. R13 native dispatch removes those costs in unlimited/uninitialized mode while retaining one accounting owner.

Diagnostic10+10 A/B **a20261019211757-65940-7a08** submitted and pending; matched base `wt-f1406-r12-matched-base @00d536820e8a8f7bd073c68bd5f0b5e6b78f95e7` -> release clean candidate69a8,64cases BenchmarkManagedQuotaWritePath,200ms/benchmem, benchmark blob **fb7a95dcfc16ad7f0d9e7a3e3b046a31eecee393** identical. This is current-old-parent diagnostic feedback, NOT final-source/restacked cost acceptance. No result yet, no claim of recovered percentage.

## Pending jobs — do not resubmit unchanged input

**No active collector after parking.** Own collector exec55729 (bash root32336, child29304 plus descendants) was stopped; this also prevents automatic discovery from submitting more shards while parked. No remote job cancelled. Prior collector8711 also terminal. All submission clients are terminal. No own services started. `r13-expanded-results.log` is retained and partial. It includes release storage discovery PASS, which is ONLY a list, not a full-storage pass.

Clean old-source pins MUST stay fixed:
- release `wt-f1406-r13-candidate-rel @69a8a164acb1029ba6cc32bdc43826ac3ce66217`, treecc594cb2c077174268031641e77579f7d524b538;
- develop `wt-f1406-r13-candidate-dev @403884b1ef96087384d7956b0c08308495d311ec`, tree832a400c5bf3b297bfc416aeb0eab50277a50915.

Full-race roots (default parser), `r13-full-{1406,1526}.json/log`:
- release storage discovery i20261019211910-89868-f0e7 LIST PASS and13children below; multidb i20261019211910-89868-ee65; replication i20261019211910-89868-d2f8.
- develop storage discovery i20261019211920-91276-62dd LIST produced2386 names;13planned shards, only5 ACKs collected before stopping; multidb i20261019211920-91276-bd03; replication i20261019211920-91276-fa97.

Release13 durable storage shards (none collected):
```
i20261019214325-82264-c7fc i20261019214333-82264-fe0a i20261019214334-82264-fe1b
i20261019214335-82264-aaad i20261019214337-82264-22bb i20261019214338-82264-c42e
i20261019214340-82264-b6ac i20261019214341-82264-17b3 i20261019214342-82264-669a
i20261019214343-82264-2c2c i20261019214345-82264-0de9 i20261019214346-82264-504a
i20261019214348-82264-4472
```
Develop5 known durable child ACKs (remaining8 NOT claimed submitted):
```
i20261019214355-82264-09ec i20261019214356-82264-f881 i20261019214357-82264-987e
i20261019214359-82264-29c8 i20261019214400-82264-0911
```
On authorized resume, collect exact roots/children with ONE batch and official client, which can reconcile/rejoin remaining planned shards. Do not count discovery or partial shards as full pass. Never manually duplicate unknown durable submissions.

Bare/schema RED: default **i20261019213410-27396-9bcf collected FAIL** (all8 bare bypasses + old concurrent-DDL wrapping); ANTLR **i20261019213415-27396-b665 pending receipt**. `r13-schema-red.json/log`, partial expanded-results.

Latest restored-boundary / bare fallback / explicit-schema checks, BOTH parsers, all ACKed, not collected:
- release `r13-boundary-1406`: **i20261019214143-67616-5b76**, **i20261019214143-67616-1573**, **i20261019214145-67616-b788**, **i20261019214150-67616-c4f3**.
- develop `r13-boundary-1526`: **i20261019214159-91584-60f7**, **i20261019214159-91584-c0f4**, **i20261019214203-91584-3e15**, **i20261019214207-91584-c6cd**.

Each manifest isolates replication and cypher binaries, default/ANTLR. Replication includes the deliberately desired-convergence autocommit-DDL investigation, so may FAIL solely for pre-existing DDL while accepted scoped tests PASS. Inspect individual tests; do not blindly say the whole package passed. Cypher selectors include original concurrent-name and persister-error regressions. Latest bare test's added late-apply step postdates these snapshots.

Published-before-r13 autocommit-DDL probe only, both parsers:
- release d861: **i20261019214211-86244-ba78 / i20261019214217-86244-16e9** (`r13-ddl-base-rel`).
- develop f00: **i20261019214220-64088-e4fe / i20261019214227-64088-644b** (`r13-ddl-base-dev`).

Diagnostic A/B pending **a20261019211757-65940-7a08**. Retain queued earlier **a20261019150223-45856-7f9a** as historical infrastructure association request if later asked; it is not a substitute for the final-source comparison.

## Resume order (only after wake)

1. Read this handoff and cost-discipline file. Inspect existing logs/new receipts before doing anything else. Use ONE collector batch. If no useful new evidence and the next action waits on results/parked parent, checkpoint and end immediately; no idle turn.
2. Triage full-suite and boundary results. Fix only scoped defects at shared owners, preserve native ordinary DDL. Record confirmed pre-existing automatic-DDL behavior separately; run prepared plain mainline probes only if resumed and needed. Do not implement automatic-DDL replication.
3. Latest helper/late-apply fixture needs focused race both copies/parsers and remote gofmt. Finish public replication/reservation/rejected-WAL coverage; all accepted P1s remain unapproved until xhigh Rule73 review.
4. WAIT for orchestrator's canonical POST-R73 #1503 and #1504 relay. Current WIP parents are STALE #1503 **5133708b8c2b17973e8fa770b6cb0ee33a0eaf6d** and #1504 **17c302d83e049f508ce5eb25186f856a83dca9f7**. Latest announced #1503 **684533cc4** (same44730c862 on#1536) will move again; do not spend a restack-only push. #1503 is parked too.
5. Final real paired correction must restack on canonical parents with predecessor commits unchanged; per-hunk preserve merged#1511/composite/raft/commit-buffer/schema and new lifecycle lock/admission behavior; verify actual storage-stack composition, TJ attribution/patch IDs and cache test. No source squashing of predecessor commits into family.
6. Final source: full storage, multidb, replication race per head; public16case quota/cache plus scoped schema/data checks both parsers; paired10+10 A/B on exact final source and matched base on same canonical stack. Explain residual measured cost, cite TJ results. Do not use old diagnostic results as final acceptance.
7. PR bodies/handoff truthful about pending Rule73 review, evidence and code-intel limitation. Source/rg inventory used; graph completeness NOT claimed. Retained ingest IDs release c1792433845752-c806f1ed / develop c1792433845737-8ca59be6; final PUSHED heads may be ingested under existing authorization, no waiting for graph queue.
8. Only then paired PR-head push under exact own full leases, preserve READY, no maintainer comment. Verify heads/mergeability/base and report heads to orchestrator because U6 must restack#1558/#1559 on them. No merge. Register worked PR layers with T3; all current worked layers already linked and unwatched.

No local go build/test/vet/gofmt, Python/Node workloads or Docker. Tests/formatters/benchmarks only official tnrun; new submissions after wake use --pr1406/1526. Keep detached job pins. No subagents, side work, new tests or polling while parked.
