# NOR-405 / PR #1745 readiness

Release base: release/v1.4.4 at
`83b8c5884deaa12bc686ac40512d689a7d44a20f` (original reproduction rebased
here as `8218df2e1 test: reproduce NOR-405 on current release`).

- [x] Read AGENTS.md, CONTRIBUTING.md and the standing design principles; work limited to the assigned worktree, no third-party, hook/checker, storage-format or key-space edits.
- [x] Reproduce the original storage failure on the rebased release base; RED evidence in `/tmp/pr1745-nor405-before-storage-repro.log` (`compact edge: start numID 1 not in dict`).
- [x] Trace the lifecycle: the deletion is executed by `pruneMVCCKeyspaceInTxn`'s tombstone-reclaim branch (`pkg/storage/badger_mvcc.go`), which trusts a head that `RebuildMVCCHeads` produced from archived versions; the wrong head comes from the bootstrap skip in `applyMVCCBootstrapBatch` and `bootstrapMVCCFromCurrentStateInTxn`.
- [x] Confirm the startup path that reaches it: `prepareDerivedIndexes` calls `RebuildMVCCHeads` before serving traffic when the clean-shutdown marker is absent (`pkg/nornicdb/db_lifecycle.go`); `TestStartupRebuildsDerivedIndexesWithoutCleanShutdown` and `TestStartupSkipsDerivedIndexRebuildAfterCleanShutdown` still pass unchanged.
- [x] Reject symptom-level alternatives: neither disabling the prune reclaim, nor forcing a new numeric ID at prune time, nor repairing the dictionary after the fact corrects the head that every head consumer reads.
- [x] Correct the canonical owner: one `mvccBootstrapHeadPending` decision used by both bootstrap forms; a tombstoned head over a live body is rewritten from that body, an absent head is seeded as before, an existing live head is left untouched.
- [x] Replace the ticket-named reproduction with behaviour-named regression tests; add the recreated-relationship variant, an on-disk reopen with a repeated rebuild/prune, and `CheckIDDictionary` identity controls.
- [x] RED before GREEN on the committed tests: storage tests fail on the rebuilt tombstoned head (and `compact edge: start numID 1 not in dict` after reopen) and the public Cypher traversal returns zero rows; evidence in `/tmp/pr1745-nor405-before-storage-tests.log` and `/tmp/pr1745-nor405-before-cypher-read.log`.
- [x] GREEN after correction: the three storage tests and the Cypher traversal/label reads pass; evidence in `/tmp/pr1745-nor405-after-storage-tests.log` and `/tmp/pr1745-nor405-after-cypher-read.log`.
- [x] Focused regression controls pass: `pkg/storage -run 'RebuildMVCC|Bootstrap|PruneMVCC|PruneAfterRebuild|IDDictionary|NumericFreelist|Freelist_|Recreate|Recreated'` (73 tests, benchmarks not run), including the existing tombstone-reclaim and dictionary-reclaim controls that prove pruning still reclaims genuinely deleted entities; evidence in `/tmp/pr1745-nor405-after-storage-regression.log`.
- [x] The new storage and Cypher tests also pass under `-race`; evidence in `/tmp/pr1745-nor405-after-storage-race.log` and `/tmp/pr1745-nor405-after-cypher-race.log`.
- [x] `go vet -tags nolocalllm ./pkg/storage ./pkg/cypher` and `gofmt -l` are clean. This worktree has no `lib/llama` static library, so Go tests run with `-tags nolocalllm`; the skipped localllm package is unrelated to this change.
- [ ] Exact-head sharded `make test` CI; broader release verification remains separate.
- [ ] Push, PR body and Linear NOR-405 acceptance evidence.

Limitations: the bounded code-graph and documentation retrieval endpoints
answered with HTTP 400, so no graph evidence was available; the conclusions
rest on current checked-out source and the executed tests. No full test suite,
full race run or benchmark was executed by this worker.
