# Preserve recovery after historical WAL upgrades (N152-343)

- [x] Reproduce the b6d8e762a embedded Cypher loss before correction on the release head; retain fixture and baseline log.
- [x] Identify the transaction-only historical WAL gap and establish a small genuine old-build fixture.
- [x] Publish a durable pending marker before the WAL migration and a fresh snapshot before serving the upgraded store.
- [x] Refuse automatic corruption rebuild while the post-upgrade baseline is pending.
- [x] Prove focused recovery with automatic compaction enabled and disabled, plus a pending-marker negative path.
- [x] Recheck the original large API fixture through upgrade, damage, rebuild, and second open.
- [x] Recheck genuine b6 API, Qdrant, and async clean fixtures plus API and Qdrant interrupted-writer fixtures through upgrade, damage, rebuild, and second open; all five match exactly.
- [x] Identify that the old async interrupted-writer store already lacks 11 nodes and has two changed records before upgrade; this old-writer gap is outside post-upgrade recovery.
- [x] Keep encrypted historical stores on their existing upgrade path, which does not use automatic snapshot/WAL corruption rebuild; prove a genuine old encrypted fixture opens without a forced snapshot.
- [x] Complete final affected-package rerun after the encrypted-store boundary and native restore prerequisite; `pkg/nornicdb` (513.653s), `pkg/storage` (896.664s), and docs pass, with integrated source and duplicate candidates reviewed manually.
- [ ] Commit, push a release/v1.5.3 PR, and update N152-343 with exact validation and limitations.

The old writer's pre-upgrade store must still be readable for a fresh baseline.
If that store is already damaged before the upgrade, no journal-only recovery
can reconstruct entity images the historical writer never recorded; startup
must preserve the store and refuse an incomplete rebuild.
