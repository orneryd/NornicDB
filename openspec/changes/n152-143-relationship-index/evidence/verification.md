# N152-143 validation ledger

Base: `f77ec7706de099588ac232b32b6908860511d3da`.
All Go commands use `GOMAXPROCS=2 GOMEMLIMIT=1500MiB`, `-p=1` and
`-tags=noui,nolocalllm` unless they run an already compiled test binary.

## Original RED

`go test ./pkg/cypher -run '^(TestNOR42OnlineOrdinaryRelationshipIndexSuppliesEqualityLookup|TestRelationshipPropertyEqualityCountedReadsStayAtTheSelectedRelationship|TestRelationshipPropertyEqualityOrdinaryRelationshipIndexIsDeclaredNotOwned)$' -count=1 -v -timeout=5m`

Exit 1, package 1.673s. Both parsers fail the intended ordinary-index flat-read
assertion: 11 reads at 8 unrelated relationships versus 515 at 512. Constraint
controls pass at 2/2 in both parsers and both transaction modes. No-index
controls remain 11/515. The old bug-observation assertion passes before correction.
Exact originals are preserved under `fixtures/testdata/repo/`; log:
`/tmp/n152-143-original-red.log`.

After this RED, the old bug-observation assertion was changed to require the
intended 2/2 ordinary-index reads, retaining 11/515 no-index sensitivity and
catalog/row assertions. The desired selective test also now runs both transaction
modes. This changes the expected fixed behavior, not the selective target.

## Matched benchmark baseline

The test-only ordinary declaration option and shared benchmark body were compiled
before production changes into `/tmp/n152-143-baseline.test`; compile exit 0.
Production was still at the base above. Existing constraint benchmark defaults
remain unchanged. The new benchmark covers both parsers, 8/512 unrelated edges,
and autocommit/explicit transactions, with caches off and exact row assertions.

`/tmp/n152-143-baseline.test -test.run='^$' -test.bench='^BenchmarkNOR42OrdinaryRelationshipEquality$' -test.benchtime=100x -test.count=5 -test.benchmem -test.cpuprofile=/tmp/n152-143-baseline.cpu`

Exit 0 during a coordinated quiet Go window after N145 race and before its vet
or parent N138 reproduction. Log `/tmp/n152-143-baseline-bench.log`. CPU profiles
include setup; query attribution must use the `phase=relationship-read` label.
Final matched after comparison is recorded in `benchmark.md`; the first after run overlapped another Go job and is preserved separately as superseded.

## Focused current-source checks

Storage selector, existing equality owner and surrogate tests passed (3.313s):
`go test ./pkg/storage -run 'TestRelationshipEqualityIndexProperties|TestRelationshipPropertyEquality' -count=1 -timeout=5m`.
Log `/tmp/n152-143-storage-green.log`.

Cypher focused current-source checks passed (5.644s):
`go test ./pkg/cypher -run 'TestOrdinaryRelationshipIndex|TestNOR42OnlineOrdinaryRelationshipIndex|TestRelationshipPropertyEquality|TestRelationshipEqualityLookup' -count=1 -timeout=5m`.
Log `/tmp/n152-143-cypher-green.log`. This includes single-hit 2/2 reads,
11/515 sensitivity controls, both parsers/transaction modes, ordinary mutation,
duplicates, DROP/restart and build failure/IF NOT EXISTS, plus existing durability
and pinned-snapshot controls. The after binary compiled successfully at
`/tmp/n152-143-after.test`.

The staged duplicate checker ran on the complete staged change and exited 1:
`AnalysisError: analysis/ingestion failed`, endpoint `http://localhost:7474`,
database `nornicdbcode`. Log `/tmp/n152-143-duplicate-check.log`. Current-source
manual owner analysis continues to support reuse; graph analysis is incomplete. Full release verification, allocation ratchet, conformance and differential
checks are not claimed. Focused race and vet completed as recorded below. `make verify-list BASE=origin/release/v1.5.2` succeeded and selected the
full applicable check set; it is a plan, not a verification pass.


## Review and evidence packaging

Applied the Go review checklist to current staged production/test changes and
surrounding callers. No actionable production finding remains in the approved
bounded slice. Catalog selection is under the schema read lock and returns a
copy; build and committed-write membership share the existing index-set mutex.
Preparation precedes catalog publication and does not depend on a schema row.
Duplicate-value lookup retains complete traversal; existing pinned snapshots,
namespace gates and unsupported-value/view fallbacks are unchanged.

Archived original Go fixtures were moved unchanged under
`fixtures/testdata/repo` so `go test ./...` cannot treat them as incomplete
standalone packages. All original source/log SHA-256 values still match the
manifest. Original logs are `.log.txt` so the repository preserves them without
changing ignore rules. Global diff whitespace checking reports only exact
original testify output whitespace in those logs; production, tests, Markdown,
Python and JSON pass `git diff --cached --check` on their paths. No evidence bytes
were rewritten to suppress those warnings.


Focused race completed successfully on the same production/test source:
`go test -race ./pkg/storage ./pkg/cypher -run 'TestRelationshipEqualityIndexProperties|TestOrdinaryRelationshipIndex|TestNOR42OnlineOrdinaryRelationshipIndex|TestRelationshipPropertyEquality|TestRelationshipEqualityLookup' -count=1 -timeout=5m`.
Storage 17.292s; Cypher 67.284s. Log `/tmp/n152-143-race.log` and
`current-race.log.txt`. This is selected runtime race evidence, not a full suite.
Current RED and normal logs are also copied alongside this ledger unchanged;
raw current RED retains the same testify whitespace style as ticket logs.


Package vet passed (exit 0): `go vet -p=1 -tags=noui,nolocalllm ./pkg/storage ./pkg/cypher`.
Scoped canonical verification passed:
`GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOFLAGS='-p=1 -tags=noui,nolocalllm' make verify BASE=origin/release/v1.5.2 ONLY=localization FETCH=0`.
Localization tests ran freshly (4.060s), catalog 0.281s, inventory 0.241s;
generated manifest and catalog checks passed. UI, llama-libs, build, tests,
allocations, conformance, differential and docs were explicitly unselected.
This is not a full `make verify` pass. Logs are copied beside this ledger.

No production/test edits followed the passing normal/race checks or retained
after binary. Subsequent changes only package evidence and update documentation.
Existing constraint benchmark guard completed in a final quiet window; see
`benchmark.md` for all medians and the accepted 2.5% ANTLR 128 small-case overhead.
No active N143 Go processes or other consumers remain. Baseline/after binaries,
profiles and superseded-overlap evidence remain in /tmp for parent review and
later task-owned cleanup. No child commit, push, PR or external tracker mutation.
