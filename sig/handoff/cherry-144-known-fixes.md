# Cherry-picks of merged develop bug fixes into release/v1.4.4 (18 Oct 2026)

Owner: Claude implementation sub-agent (claude-opus-5-5), parent thread 075cf8e3-9632-45e4-a74b-ecbe4e778f84.
Decision (maintainer, 02:34): known bug fixes that already work go into 1.4.4 instead of a separate 1.4.5.
Base for every branch: origin/release/v1.4.4 at 24d08776c. One PR per fix, `git cherry-pick -x` of the develop commits.
Not merged: the orchestrator arranges a Codex review and the 1.4.4 RC verify for each one.
All PRs: base release/v1.4.4, milestone v1.4.4, label bug, head on origin (orneryd), linked to the parent thread.

| Fix | develop commits | branch | head | TrueNAS jobs (all pass) | PR |
|---|---|---|---|---|---|
| #1383 | 4e329179a | cherry/1.4.4-1383-correlated-call-imports | c5bb8e269 | tests both parsers i20261018013750-65332-5d8b; gofmt i20261018014653-19872-5441; vet i20261018014719-98860-fec7 | #1458 |
| #1384 | 159cfe273 | cherry/1.4.4-1384-pattern-predicates | f418585d4 | tests both i20261018013753-104844-c309; gofmt i20261018014733-79960-6913; vet i20261018014739-42436-d4c7 | #1459 |
| #1405 | 93ef1f592, 4683f8ef1, 0b41e5e13 | cherry/1.4.4-1405-case-after-map-colon | b3f69f3d3 | tests both i20261018013940-79120-72d4; gofmt i20261018014753-37904-454a; vet i20261018014758-100616-a4b1 | #1460 |
| #1350 | baf737d49, fdfa34e3f, 365c07c4f, ff8d7049a, 349683a7f | cherry/1.4.4-1350-per-db-read-checks-r2 | 92cfda713 | tests both -race i20261018013942-89588-6b46 (graphql/resolvers, server, heimdall, auth, nornicdb, plugins/heimdall); plugins/heimdall full -race j20261018015736-60904-36ec; gofmt i20261018015651-99032-f716; vet i20261018015714-80196-1691 | #1461 |
| #1285 | 5f05b130d, 211b04379 (+ test update 0db6fe5a9 from #1365) | cherry/1.4.4-1285-embedding-keys-r2 (pushed, no PR) | c77b08b35 | not run (stopped) | **none: stopped** |

## #1383, #1384, #1405, #1350
No conflicts. Worktrees: wt-cp144-<n>.

#1350 applies and passes on its own. It does not need #1352/#1393 or any other F-PRIV/RBAC piece.
For the orchestrator: #1364 (merged on develop, "router executor sharing, async invoker context,
incomplete principals") is a follow-up to #1350's Heimdall router code. The earlier release copy #1416
carried both. #1350 doesn't need #1364 to build or pass, and #1364 isn't in this task, so it isn't included.
Whether it goes into 1.4.4 is the orchestrator's call.

## #1285: stopped, no PR (old stores are not readable both ways)
- **Upgrade direction (1.4.3 store opened by 1.4.4): OK without #1436.** #1285 reads the older key
  families (0x08 released, 0x27 dev) as a fallback when the current key is missing.
- **Downgrade direction (1.4.4 store opened by 1.4.3): NOT OK.** #1285 writes every new embedding key
  to a new family 0x28 (`badger_embedding_key_codec.go`: "All new writes use one disjoint family"). That
  includes the per-node embedding metadata record (`embeddingMetaKey` = `embeddingKey(id, reserved)`).
  A 1.4.3 binary knows only 0x08. In `loadEmbeddingChunksFrom` it treats a missing key as "no chunk"
  (`ErrKeyNotFound` → continue), and it finds no sidecar metadata, so nodes embedded or re-embedded by
  1.4.4 silently show no embeddings after a downgrade. 1.4.3's node delete and DROP DATABASE also don't
  know 0x28, so they would leave those keys behind (the bug #1285 fixes).
- #1436 doesn't change this: it adds a one-way V3→V4 storage version, planned for 1.5.0.
- Also still open without #1436: the read cost #1285 adds (one allocation and one extra Badger lookup per
  whole-node read; StreamNodes_Full 20,764 → 25,764 allocs/op, +32% time, measured on develop
  705a19cfc → 211b04379, see #1418/#1436).
- The branch is pushed so the conflict work is kept. If the orchestrator decides that downgrade
  readability isn't required, it is ready for tests:
  - Conflicts were in six pkg/storage files. Release-only commit e8590597b ("release transaction
    admission before listeners and classify quotas") added its own ordered post-commit notification
    queue (same field names), which #1285 also adds in a more complete form.
  - Resolution: #1285's queue. badger.go, badger_commit_notifications.go, badger_commit_writer.go,
    badger_stats.go and badger_write_behind.go are byte-identical to the earlier tested release copy
    #1418 (21dd030db). badger_transaction.go is #1418's version plus release's `afterPublication` hook,
    which still runs before listeners.
  - Release's `reserveCommitOperations` / `prepareCommitOperations` / `enqueueCommitOperation` /
    `publish*` wrappers had no callers left and were removed, so there is one queue, not two. #1333's
    pending-embedding fields are left out, as in #1418.
  - Audit: for every file #1285 touches, (HEAD vs 21dd030db) equals (release vs f2ddd27cf), apart from
    those five overlap files.
  - Tests to run if it goes ahead: `./pkg/storage` and `./pkg/multidb` with -race (storage needs ~20 GB),
    including `TestNodeCopyPreservesEntityFields` and release's `badger_transaction_publication_test.go`.
