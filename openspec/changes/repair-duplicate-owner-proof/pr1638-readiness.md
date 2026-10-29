# PR #1638 / NOR-51 readiness

Base: release/v1.4.4 at cd67390c0e4aacf8a12ea34a141d752d637dd08f.
Prior proposal: c70549944ab858aa9edb0f61ce7ae98094c28b40.

- [x] Assign NOR-51 to Tim and protect PR #1689 / #1676 ownership.
- [x] Preserve the old PR head and preview composition conflicts.
- [x] Include approved checker correction in this existing PR worktree.
- [x] Complete current-owner composition audit; port required behavior without reviving obsolete APIs.
- [x] Reproduce partial composite writes on the release through public execution paths.
- [x] Ensure later local, remote and procedure failures roll back every local write.
- [x] Verify successful read-your-writes and one-write-database enforcement.
- [x] Preserve transaction terminal/session ownership without nested cleanup locks.
- [ ] Resolve four source findings from the completed corrected checker sweep.
- [x] Complete and independently verify current release code-graph refresh.
- [ ] Run focused correctness/race/performance checks for changed contracts.
- [ ] Run applicable make verify-list and make verify against frozen release base; report all skips.
- [ ] Run fresh make test and enabled duplicate checker on the integrated staged tree.
- [ ] Push verified changes to existing PR #1638 and update evidence and Linear status.

The old draft's test receipts do not qualify this composition. PR #1689 and
#1676 and their worktrees are owned by another agent and must remain untouched.
No new checker PR will be created.

### Storage ownership source port

- [x] Preserve current native participant creation and typed finalizers; attach statement ownership to issued storage.
- [x] Discover ownership through transparent wrappers before context hints and stop at native boundaries.
- [x] Join rebuilt/USE/FunctionStorage/router/remote executions to the existing actor and coordinator.
- [x] Port object-bound mode observation and query read leases onto current parser and storage APIs.
- [x] Retire the forwarding Fabric ExecuteQuery entrypoint and obsolete context-only promotion path.
- [x] Migrate the old constructive storage-membership population and mode-owner cases without weakening assertions.
- [x] Complete full constructive ownership and current causal/public checks on frozen source in both parsers; focused race differences pass. Full property race and broader parent gates are reported separately, with original failed/canceled/timed-out evidence retained.

### Integrated evidence and remaining gates

The remote release graph independently reports commit
`cd67390c0e4aacf8a12ea34a141d752d637dd08f`, 64,793 nodes and 2,579,410
relationships. Refresh completed before the final checker; incomplete earlier
imports are not used as proof.

Both frozen parser populations passed the 7,080 constructive storage-ownership
cases, 56 issued-view cases and their causal/public regressions. Focused race
checks passed. Earlier exhaustive race runs timed out at 300 and 600 seconds;
they are incomplete and are not reported as race qualification.

Search reservations retain 32-byte tokens and zero allocations. In five quiet
500 ms repetitions, node reservations averaged 81.522 ns versus release
82.560 ns, and relationship reservations 80.830 ns versus 81.094 ns. These are
reservation microbenchmarks, not ingest measurements. Final Fabric performance
and broader integrated verification remain pending.

The transport audit found additional retained acceptance gaps: recovered statement
panics could leave earlier explicit writes commit-admissible; session mutexes could
remain held; rollback-worker panics escaped the request goroutine; and HTTP cleanup
errors were discarded. Fail-first evidence is preserved. The canonical session and
executor owners have been corrected and focused public race controls pass; the
full integrated gate remains required.


### Cypher owner convergence and suppression source freeze

- [x] Retire cloneForStorage; migrate selected database callers to the canonical cloneWithStorage owner, retaining their distinct cache/search/catalog policies.
- [x] Converge lifecycle and data/administration entrance in admitTransactionStatement before script cloning; preserve script parameters and error classifications.
- [x] Route resolved mode observation/binding through authoritative storage membership; retain immutable wire-result framing as its distinct owner.
- [x] Correct administration takeover to transfer the existing issued native wrapper/controller; preserved failed administration/data COMMIT regression and green proof.
- [x] Claim physical native write admission before queuing suppression completion on authoritative issued storage. Prove node/relationship completion, outer failure, explicit commit/rollback and both second-writer orders through physical leaf visibility and retained payload assertions.
- [x] Assert rejected decay/promotion changes leave physical schema absent and prior other-database writes uncommitted.
- [x] Resolve reveal exploration using the existing supported on-disk Badger fixture: direct, selected and fragment reveal return the hidden target, composite unsuppress restores physical visibility. The earlier MemoryEngine fixture cannot open a Badger reveal scope; exact exploratory failures remain in evidence, with no reveal production change.
- [x] Final affected focused race population passes both parsers: nornic 6.083s, ANTLR 7.268s. No full-gate claim; original exhaustive ownership proof predates this final convergence.

Exact source hashes, red/green/compile/exploratory logs and command are preserved under
`pr1638-ownership-port/cypher-convergence` in the external readiness evidence root.
Source and CPU runs are frozen for parent performance/checker/full gates. No staging or commit.

### Second convergence pass

The unchanged checker on staged tree `602b2779792e` analyzed 225 functions,
accepted 113 ownership proofs and returned 15 reuse findings, with zero cap skips
or warnings. That run is a completed inventory, not a clean check.

The next source pass unifies selected/same-storage executor cloning, transaction
admission and native operation admission; migrates schema callers to typed
`Schema(SchemaPurpose)`; gives WAL manifest access one persistence owner; and
shares real Fabric, remote-engine and procedure-test fixtures. Canonical callers
replace the retired entry points. Manifest JSON bytes, Badger keys and stored
payloads remain unchanged.

The accepted suppression criterion now has physical visibility/retained-payload,
commit/rollback and both second-writer-order controls. Supported on-disk Badger
reveal and composite unsuppress controls pass; an earlier MemoryEngine reveal
exploration lacked that fixture's required native reveal capability and is not
reported as a product defect.

Current constructive ownership and registered-procedure populations passed with
`NORNICDB_PARSER=nornic` (55.494s) and `NORNICDB_PARSER=antlr` (55.606s).
Focused affected race tests passed separately in both parsers. A worker's build-tag
runs do not establish a second parser; these environment-selected reruns provide
that proof. Final performance, refreshed checker and full gates remain pending.

### Integrated full-run fallout and fourth convergence pass

Frozen staged tree `e6be5e52739adb2df47b019bce743f0c54842459` failed fresh
`GOFLAGS=-count=1 make test`: 86 top-level failed tests in eight packages. The
storage census completed in 824.328s; it was not canceled. Original failures and
unfiltered focused reproductions remain in the readiness evidence inventory.
This run is failing evidence, not a suite pass.

The subsequent source pass corrects the shared causes: inherited outer ownership
at independent batch boundaries; deferred completion lost by autocommit; lost
storage event/access capabilities; database identity through owned views; lazy
search registries discarded with temporary views; and inline embeddings bypassed
by owned local fragments. Typed native and public HTTP/Bolt controls pass in
focused runs. Completion errors reach public callers and all queued actions run
once. Canonical database-name consumers replace the overlapping helper.

Fixtures now open composite explicit transactions on their composite database,
retain separate native-to-composite rejection controls, use the public native
BEGIN/COMMIT owner, and assert procedure-view reentry rather than controller
pointer identity. All original data, rollback, arity, namespace and mutation
assertions remain. Remote credential workflow assertions now verify READ rollback
and distinguish the owned transaction ID from preflight autocommit requests.

The latest unchanged duplicate sweep analyzed 388 functions, verified 251
ownership proofs, skipped none, warned on none and reported four findings. The
read-finishing fragment has been folded into its canonical query capability and
the two unresolvable-header fixture paths now share one table owner with both
assertion sets. The retired generic reservation owner and the graph importer /
prediction consumer still require the recorded ownership assessment. No finding
is waived and no additional analyzer edit has been applied.

The reviewed URL-sink inventory and lexical population reflect current typed
returns and credential-redacted transport/error boundaries. The scanners and
negative assertions remain unchanged. A final frozen make test, duplicate sweep,
performance qualification and committed-head make verify are still pending.

The final two previously failing public security cases identified authorization
ordering: an early native-to-composite refusal bypassed the existing graph gate.
The refusal now follows authorization at that same gate and still precedes
participant enlistment. The original HTTP/Bolt Forbidden and read-only assertions
pass unchanged. The complete native ownership population passes again (29.175s).
All 86 originally failing tests have focused passing rechecks after their causal
corrections; these do not substitute for the next frozen full run.
