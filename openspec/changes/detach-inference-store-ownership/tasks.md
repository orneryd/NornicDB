# NOR-279 / N15-82 readiness (v1.5.2)

Worktree `fix/nor279-owner-detachment-v152`, base `43a39d64f9374dce08ea5fd17b87b17c268137e9`.
Scope: `EdgeMetaStore` and `NodeConfigStore` in `pkg/storage`. No commit, push,
PR, reingest or checker change in this lane; the parent owns acceptance.

## Fail-first reproduction at current base 43a39d64

- [x] Extract the reported sources from the Plane input unchanged as
      `pkg/storage/nor279_metadata_ownership_test.go` (sha256
      `277b732339ef1c5905a238c8b096b68e294cbb0b6018aeed793f18e4f7fa3bc9`, the
      hash recorded in the ticket) and
      `pkg/storage/nor279_filtered_read_ownership_test.go` (sha256
      `205ff39f70034776f0a10d05a92e053f29a263b8db4ca9678142641b6991697d`).
      Both are gofmt-clean as extracted; no source line edited, and both hashes
      are unchanged after the parent-review correction.
- [x] Record the failing current-head run: `go test -tags=noui,nolocalllm
      ./pkg/storage -run '^TestNor279' -count=1 -timeout=3m -v` →
      `FAIL github.com/orneryd/nornicdb/pkg/storage 1.357s`, 23 of the 26
      reported subtests failing. Caller mutation was observable in
      `GetHistory`/`GetLatest`/`Export`/all five filtered getters, and in
      `Set`/`Get`/`GetOrCreate`/`Export` for node configuration. Passing at
      baseline: the `EdgeMeta` nil-metadata control and both `NodeConfig`
      nil/empty controls.
- [x] Record the pre-fix cost of the unsafe ownership with the reported
      fixtures: `-bench '^BenchmarkNor279' -benchmem -benchtime=100x -count=3`,
      plus one `-benchtime=1s` CPU profile for sampling.

## Correction

- [x] One `NodeConfig` copy pair (`snapshotNodeConfig` +
      `detachNodeConfigMetadata`, applied together as `copyNodeConfig`): the
      snapshot copies value fields, `PinList`, `DenyList` and `LabelConfigs`,
      and the detach replaces `Metadata` with the shared value owner's copy.
      Collection representation is unchanged from base: nil input collections
      are delivered as allocated empty ones.
- [x] One `EdgeMeta` copy pair (`snapshotEdgeMeta` + `detachEdgeMeta`, applied
      together as `copyEdgeMeta`), with `snapshotEdgeMetas`/`detachEdgeMetas` for
      delivered slices; used by `Append`, `Import`, `GetHistory`, `filterMetas`
      and `Export`.
- [x] One typed metadata boundary (`copyCallerMetadata`) added to the existing
      caller-copy owner file; no second deep copier, no JSON round trip, no new
      reflection or cycle policy.
- [x] Copy placement corrected after parent review: ingress detaches before the
      write lock, reads snapshot the store's own fields under the lock and detach
      metadata after releasing it, retained metadata is frozen after the ingress
      copy, and a disabled provenance flag still returns before any copy.
- [x] Withdrawn: the unapproved nil-preservation expectation for `NodeConfig`
      collections. `TestNor279NodeConfigCollectionRepresentation` now pins the
      published base representation (`[]`/`{}` in `Get`/`Export` JSON, non-nil
      empty in Go) plus isolation of those delivered collections.
- [x] Coverage beyond the reported fixtures: collection representation and
      isolation through every boundary and round trip, pin/deny list getter
      detachment, `Import` replacing an existing configuration, callback reentry
      on all 13 ingress/egress paths, the disabled-provenance no-op, and a
      bounded value-snapshot consistency check under `MarkMaterialized`/counter
      changes.

## Verification performed locally

- [x] Focused contract tests (canonical command, unchanged):
      `env GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1500MiB go test ./pkg/storage
      -tags noui,nolocalllm -run '^TestNor279' -count=1 -timeout=3m -p 1` →
      `ok ... 1.268s`, 14 test functions passing.
- [x] Existing owner tests: `-run
      '^(TestEdgeMeta|TestNewEdgeMeta|TestNodeConfig|TestNewNodeConfig|TestCopyNodeConfig|TestNewNodeConfigStore|TestGlobal|TestTrustLevel|TestSignalType|TestCopyValue|TestCallerCopy)'`
      → `ok ... 1.122s`.
- [x] Affected consumer package: `go test ./pkg/inference` → `ok ... 1.719s`.
- [x] `go vet ./pkg/storage ./pkg/inference` and
      `go build ./pkg/storage ./pkg/inference`: clean; `gofmt -l` on the changed
      and added Go files: empty.
- [x] Callback reentry fail-first on the pre-correction staged proposal (build
      overlay of the staged files, no worktree change):
      `TestNor279CopyProtocolRunsOutsideStoreLock` → 13/13 paths failed with
      "the store held its lock while it ran the caller's copy protocol"
      (1.076s, no hang); `TestNor279StoreCopyCallbackReentry` → 13/13 isolated
      children never returned and were stopped at the 5s bound, printing
      `NOR279_REENTRY_LOCK_HELD` and, on egress, `NOR279_REENTRY_WRITER_BLOCKED`.
- [x] Same two bounds on the corrected tree: 13/13 pass (0.00s and 0.25s).
- [x] Base classification (build overlay of `HEAD` = 43a39d64f): the reported
      reproduction reproduces 23 failing subtests and 3 passing controls;
      `TestNor279EdgeMetaNilAndEmptyFidelity`, both
      `TestNor279NodeConfigStoreNilAndEmptyControls` cases,
      `TestNor279NodeConfigCollectionRepresentation`,
      `TestNor279NodeConfigListGettersDetach` and the disabled-provenance and
      value-snapshot checks pass at base (regression guards, not added defects);
      `TestNor279NodeConfigImportReplacementDetachesCallerConfig` fails at base.
      The reentry cases fail at base only with "the store must reach the caller's
      copy protocol" — base runs no copy protocol, so that contract starts with
      this change.
- [x] Post-correction cost, same fixtures and machine, `-benchtime=100x
      -count=3`: `GetHistory` 69718/72910/80326 ns/op, 89693-89695 B/op,
      837 allocs/op; `NodeConfig.Get` 854.6/875.8/1008 ns/op, 2048 B/op,
      17 allocs/op. Base 43a39d64f on the same benchmark file:
      `GetHistory` 1964/1998/3619 ns/op, 15944 B/op, 69 allocs/op; `Get`
      395.8/399.2/445.8 ns/op, 1232 B/op, 7 allocs/op.
- [x] Separate 1s CPU profiles (`-benchtime=1s -count=1`): corrected
      `CopyValue` cum 0.32s/7.53% of 4.25s samples, `GetHistory` cum
      0.22s/5.18%; base `copyNodeConfig` cum 0.33s/6.93% of 4.76s samples,
      `GetHistory` cum 0.28s/5.88%. Both runs are dominated by runtime/GC under
      `GOGC=50` and are reported as sampling shape only.
- [x] No committed behavior outside the two stores changed; no unrelated API,
      storage schema, ranking or feature-flag edit.

## Mandatory staged duplicate check

- [x] Unchanged checker on the corrected staged tree:
      `python3 /tmp/nornic-use-configured-graph.py <venv>/bin/python
      scripts/code_intelligence.py check-staged --repo-root <this worktree>` →
      exit 0, "Analyzed 56 added or rewritten functions. PR commit `43a39d64f`,
      compared against code ingested at `5ca76aad3`. No duplicates found."
- [x] Dispositions reported by the checker, verified in current source:
      `snapshotNodeConfig` is the `extracted_owner` for `copyNodeConfig`, `Get`
      and `GetOrCreate`; `Import` and `Set` are `current_owner_call`s of
      `copyNodeConfig`; `copyCallerMetadata` is a `current_owner_call` of
      `CopyValue`; `snapshotEdgeMetas` is the `extracted_owner` for `Export`.
- [x] Staleness recorded, not hidden: the graph was ingested at `5ca76aad3`
      (the release/v1.5.2 tip) while this work sits on `43a39d64f`, so the tool's
      own note says matches may be stale. `edge_meta.go`, `node_config.go`,
      `property_copy.go` and the two existing owner test files are byte-identical
      between `43a39d64f` and `5ca76aad3`; this uncommitted correction is not
      indexed. No reingest was run in this lane and no current-index claim is
      made.
- [ ] Re-run the check on any tree changed after this run (its receipt is keyed
      to the staged tree).

## Open before release acceptance

- [ ] Parent review and acceptance of the corrected proposal, including the
      restored `NodeConfig` collection representation and the copy-placement
      contract.
- [ ] Linux confirmation in the allocated remote slot once coordination grants
      it; local host is darwin/arm64 only, and no remote container was used.
- [ ] Affected-package `make verify`/CI qualification on the accepted head. This
      lane runs focused tests plus affected-package vet/build only, by
      instruction; `make verify-list BASE=origin/release/v1.5.2` selects
      localization, ui, llama-libs, build, tests, allocations, conformance and
      differential, and skips docs.
- [ ] Push, PR to `release/v1.5.2` with milestone `v1.5.2`, and the full
      verification record; all parent-owned.

## Parent integration on current release

- [x] Independently reviewed both owner files, shared CopyValue contract, public ingress/egress, collection representation and inference consumers.
- [x] Integrated unchanged correction onto release 7c61a23f5054f020c0c355593cc55c4dfdd6ba35. Three production owner files are identical to the reproduced 43a39 baseline.
- [x] Fresh focused macOS storage/owner tests PASS 1.438s; affected inference package PASS 2.176s; storage/inference vet and build PASS, using noui,nolocalllm tags.
- [x] Preserved complete original and additional test source inline in both existing Plane descriptions; no attachments or duplicate issues.
- [x] Fresh fixed Linux confirmation: TestNor279 PASS 0.464s, container exit 0, not OOM (one 2-CPU / 4-GiB container; no stress).
- [ ] Refreshed final staged duplicate check and exact-head approved CI.

The earlier worker-lane entries above describe their immutable baseline, not the current integrated head. Full local repository verification, race/stress and allocation ratchet remain unexecuted by user instruction.
