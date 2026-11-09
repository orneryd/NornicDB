# Preserve WAL recovery diagnostic context

## Observed failure

The default `make test` run on the release rebase candidate `73509f1e4` passed 154 packages and failed two audit tests. `TestStorageNilContextCensus` reports that `logWALRecoveryEvent` creates `context.Background()` even when a request or engine-owned operation context exists in the reader/recovery caller chain. Current release `906a4ed2c` retains the same implementation.

## Required result

Carry the real operation context through the canonical WAL reader and diagnostic owner. Startup and accepted recovery keep their existing independently owned completion lifetime. Request cancellation must not retract an accepted write or discard recovery-pending work. The census and its negative controls remain unchanged.

No WAL format, Badger key, persisted payload, storage version, HTTP or Bolt contract change is proposed.

## Contract decision under investigation

The public Go WAL reader/recovery entry points currently cannot receive a context. The previously prepared context-first proposal changes ten exported signatures and migrates all callers without old-signature forwarding wrappers. That compatibility decision remains pending; this draft does not implement it or claim the census passes. Investigation must verify the current complete caller population and the lifetime each caller already owns.

## Separately based integration findings

The numeric-ownership finding concerns `docs-site/tools/source-graph/main.go`, inherited from main and absent on current release. Main's docs-only PR-diff assertion and publishing-policy mismatch also need a main-based maintenance correction. Their implementation is not silently bundled into this release fix. Integrated numeric ownership must be rechecked when main is actually incorporated into release.

## Verification

Preserve the failing census evidence. Add diagnostic-context regressions for torn tails and skipped corrupt embeddings, and verify valid reads, error/format behavior, cancellation, and independent accepted completion through public consumers. Run the unchanged compiled census, affected-package/race checks, then exact-head `make test` CI. A helper-only test or earlier release run is not final qualification.
