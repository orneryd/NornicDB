# NOR-34 / PR #1548 release qualification

Target: `release/v1.4.4`. Baseline: `343ca141f4e2516883e5f83cac12efe022a4a3b1`.
Readiness gate: full `make test`, per maintainer instruction. The duplicate
checker remains informative and unchanged; full `make verify` is not claimed.

- [x] Select oldest open Urgent release PR using live GitHub/Linear evidence.
- [x] Assign NOR-34 to Tim and mark development in progress.
- [x] Preserve a failing public selected-property ownership reproduction on the
  current release: mutating a returned list changes a subsequent stored read.
- [x] Rebase onto current release owners, preserving typed observed mutations,
  immutable runtime metadata, unique-key ACK publication and donated WAL leases.
- [x] Reconcile the complete affected producer/consumer population and each AC
  against integrated source; distinguish #1676's separately owned dependency.
- [x] Verify nested property ownership through public cached and buffered reads.
- [x] Verify affected-scope invalidation, final images, unrelated cache retention,
  rejected publication and cancellation through supported commit paths.
- [x] Measure affected cache/admission workloads against the release baseline,
  including allocations and CPU profiles; correct demonstrated regressions.
- [x] Review informative duplicate findings and document concrete retained
  responsibility boundaries without changing the checker.
- [x] Run full `make test` on frozen integrated source and resolve all failures.
- [x] Correct and reverify accepted-command completion after preparation waiter
  cancellation. The first full run was canceled as superseded after a focused
  fail-first test reproduced `context canceled` from native owner completion.
  Submission must arbitrate its current context exactly once; an accepted
  owner's native completion must not retain the preparation waiter's context.
- [x] Publish rebased head and update PR/Linear AC, test matrix and evidence.
- [x] Correct generic entity/diagnostic copies without changing Go scalar/list
  types or nil container identity; remove redundant prepared diagnostic cloning.
- [x] Remove the unnecessary per-candidate projected-scan copy. Its callback is
  explicitly read-only; the existing allocation test passes unchanged without
  the speculative candidate-cache workaround.
- [x] Distinguish storage-cache warm-up from stream retention in the existing
  memory test. The limit remains 16 MiB; an out-of-tree retaining-row mutation
  still fails at about 38 MB. Three integrated repetitions pass.
- [x] Complete scoped workspace cleanup after verification, preserve required logs
  and report the active-work/cache/Docker/immutable cleanup deferrals below.

Original failing evidence is retained outside the worktree in
`~/.local/share/nornicdb-pr-readiness/next-after1655/release-projected-red.log`.
The overlay adds the original PR's public regression test without changing the
release checkout. The failure establishes returned/cache ownership corruption;
it does not establish that persisted on-disk bytes changed.

## Integrated regression correction evidence

- [x] Reproduce the full-suite storage failures and preserve complete Go output
  rather than the Makefile's filtered summary.
- [x] Correct the replay fixture's cache-lock scheduling cycle without changing
  product lock ordering. A captured-overlay removal control still fails both
  stored-view and public-Cypher regressions. Three race-enabled repetitions of
  replay, footprint growth, temporal trust and detached publication pass.
- [x] Preserve pre-admission temporal trust on cancellation and verify that an
  interrupted rebuild revokes trust and uses authoritative scan enforcement.
- [x] Separate the footprint fixture's manually held peer lease from the growth
  writer's listener context; keep the listener lock guard enabled.
- [x] Consolidate accepted-replication fixture lifetime ownership across the
  affected snapshot, veto, forwarded and catalog-error cases.
- [x] Consolidate atomic detached-client admission while preserving both the
  pre-lock rejection and the under-lock race recheck.
- [x] Qualify the integrated write-request and count-fold owner migrations,
  metadata ownership consolidation and storage-census convergence on frozen
  source. Earlier package runs do not qualify later source changes.

The frozen checkpoint tree `290873df99813d754dc7f89d5655280c84b344b0` passed
full `make test` (exit 0, 1046.3 seconds, source unchanged). Later convergence
changes are not covered by that run; a fresh frozen-source gate remains required.
Focused integrated AC tests pass for storage (34.804s) and replication (3.647s).
Count-fold/close/restore controls pass (5.067s). These are focused checks, not
substitutes for the required full `make test` push gate.

## Current convergence follow-up

- [x] Retire context forwarding entry points and migrate canonical flush,
  accepted-authority and publication-wait callers.
- [x] Reproduce and correct public Cypher BEGIN dropping cancellation before
  physical flush admission; three race-enabled repetitions pass.
- [x] Unify full-text index creation with an explicit entity kind, migrate
  callers, and retain rollback/idempotence behavior.
- [x] Unify named count repair with a typed key-family proof; census passes
  without changing its acceptance rules.
- [x] Share MVCC rebuild/bootstrap/migration algorithms across entity kinds,
  retaining typed batches and existing keys/payloads.
- [x] Share adjacency cache direction handling and repeated test fixtures.
- [x] Restore the compiled-population test to ordinary suite execution and
  migrate it from the deleted workcensus package to the current SSA owner.
- [x] Finish buffered mutation API consolidation with private replay ownership.
- [x] Correct rejected policy persistence and concurrent native/direct schema
  publication, including derived-index preservation on rejection.
- [x] Complete dispositions for all 87 whole-PR duplicate candidates and verify
  any further shared owners against integrated source.
- [x] Requalify final performance costs, generated census and full make test;
  publish the final rebased head with AC evidence.

- [x] Extend rejected-policy/no-op/later-write checks to all thirteen mutation
  forms (twelve Go mutators), with non-vacuous staged-definition assertions,
  all SHOW projections and binding-table allocation preservation.
- [x] Establish native/direct policy lost-ACK sensitivity with an earlier-source
  control, and verify the correction across the actual pre-publication gap.
- [x] Correct rejected temporal constraint/contract drops retaining incomplete
  history trust. A pre-fix public node write accepts an overlap; the corrected
  case rejects it and retains the authoritative-scan fallback.
- [x] Register new disk-backed policy fixtures through the existing lifetime
  owner; repository constructor ownership lint fails before and passes after.
- [x] Replace the demonstrated generic SHOW identity-copy cost with a qualified
  shared owner. Do not retain the current regression or an unmeasured experiment.

- [x] Complete the shared schema publication boundary: a stronger current-source
  pre-publication control found a native policy ACK missing after reopen when a
  full-text mutation intervenes. Serialize all persistent manager mutations and
  native schema publication with one per-manager gate; preserve token fast paths
  and avoid upgrading a caller-held engine barrier. Reverify the complete family.

## Refreshed whole-PR inventory

The unchanged analyzer completed on staged tree
`a9689668c79bd8e8b836acd9f76d340d3b5bfc81`, analyzing
980 functions with 75 reuse candidates and no cap skips. The SHOW pointer
replacement and regenerated census were still unstaged during that run.

- [x] Resolve newly exposed Cypher full-text handler and transaction-test overlap;
  retain procedure-specific errors and public dispatch entity kind.
- [x] Resolve newly exposed storage fixture overlap without forwarding helpers
  or losing scenario assertions and lifetime ownership.
- [x] Consolidate remaining vector/property-type schema API entry points and
  migrate canonical callers/docs; share the profile validation/publication owner.
- [x] Share protocol success/flush fixture operations while preserving distinct
  repeated-MERGE, quota, counter and reopen assertions; focused tests pass.
- [x] Complete refreshed per-candidate dispositions; record the final unchanged
  checker timeout with the authorized prior-inventory fallback, then pass the
  full `make test` push gate.

- [x] Retire 165 redundant ignored-close registrations in owned memory fixtures
  using AST binding/closure/order guards; preserve explicit close/reopen and
  multi-effect cleanup. All 136 touched tests and lifetime guards pass focused checks.
- [x] Extend the nested ownership matrix with AllNodes after reproducing its
  aliasing on release. First-by-label passes release and remains external
  qualification evidence, without a speculative new correction.
- [x] Make unrelated-cache retention assertions non-vacuous and check buffered
  ACK before replay as well as after flush. Buffered whole-type cache bypass is
  asserted as the actual contract; public type results remain correct.
- [x] Reconcile the reviewed lexical population after function retirement;
  four removed/four added members reviewed, rules/assertions unchanged, check passes.
- [x] Freeze final runtime/test source, record build input fingerprints and
  record the final advisory timeout and completed-inventory fallback, then pass
  full make test before publication.

## Final-gate recovery

- [x] Preserve the completed final `make test` failure: the sole failure was
  `TestEdgesBetweenFallback_CostIsTheSmallerDegree` (timing ratio 4.86 > 4).
  The original calibration discarded its qualifying sample and subsequently
  measured a 5 ms denominator during concurrent package execution.
- [x] Verify actual Badger lookup work: zero entity reads for an empty endpoint
  at both 1,000/16,000 hub degrees, three reads for endpoint degree three.
- [x] Replace the scheduler-sensitive timing assertion with allocated-work
  scaling on the same runtime path and degree fixtures. Three repetitions use
  28 allocations at either degree. A deliberately restored outgoing-hub scan
  fails the unchanged ratio bound: 15,038 versus 240,055 allocations (15.96x).
  Production source is restored byte-for-byte after that negative control.
  The test now also executes in short mode; no skip or relaxed bound is added.
- [x] Record final remote-checker timeout honestly and use the maintainer-approved
  completed 980-function/75-candidate inventory plus the initial 87-row review.
  No final-source clean checker pass is claimed; both scripts remain unchanged.
- [x] Re-run full `make test` after this test correction and publish its receipt.

Final gate: full `make test` exit 0 in 677.4 seconds, source unchanged, tree
`95f60e14a9e24dcb6774edef4538e18c948ed1a9`. 134 packages passed (9 fresh,
125 Go-cached). The qualification report records fingerprints, measured ownership
costs, cached/unexecuted checks, negative controls and the incomplete final remote
scan. Documentation below the gate records results; runtime/test source is unchanged.

Published implementation commit `7189928a86e153e0d92aba72f2f9e317cc4f676b` to
PR #1548 with an exact lease on the old remote head. PR description/AC evidence
is updated and NOR-34 is Test Complete (not Merged/Released), assigned to Tim on
v1.4.4. The normal unchanged advisory commit hook also reached its comparison
deadline; the authorized completed-inventory fallback remains explicit.

Cleanup: five expanded extraction snapshots and 28 obsolete experiment binaries
were compressed and CRC-verified before original copies were removed. Raw logs,
profiles, reproduction sources and final release/current benchmark binaries remain.
Evidence footprint decreased from about 11 GiB to 3.1 GiB. The managed baseline
worktree archive was rejected because another active task still uses it; preserve
that checkout and the shared Go cache. Other active worktrees and the open PR
branch remain. Docker is unavailable (no images were created by this PR). No
matching task-prefix files remain in /private/tmp. The previously immutable old
archive residual remains untouched; no flag change was authorized. Deferrals are
recorded in the external cleanup receipt, not bypassed.
