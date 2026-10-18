# Parked orphan-key probes — PR #1474

Parked for **1.5.0** on **2026-10-18** by explicit user decision. No throwaway old-layout correction for 1.4.4. PR #1474 is closed and must remain closed; no merge, reopening or replacement release implementation is requested.

- Closed PR: https://github.com/orneryd/NornicDB/pull/1474
- Original PR head: `c26247cfc4447fe0468738103ce4bb2db45c2af4`; original branch `cherry/1.4.4-1285-embedding-keys-r2` remains unchanged.
- Release base: `24d08776cdb9cb624da1073007da437306518426`.
- Checked develop: `16923aef778025fab3490f6a57549763b536f3d5` (all 63 storage checks pass).
- WIP branch: `wip-1.5.0-1474-orphan-key-probes` on https://github.com/orneryd/NornicDB, from the release probe checkout at c26247cfc. Resolve the commit containing this handoff from that branch; the final pushed SHA is recorded in the outer workspace handoff and parking report.
- Local worktrees kept: `wt-cp144-1285`, `wt-release-1285-owner-probe` (WIP branch), `wt-release-1285-owner-red`, `wt-release-1285-owner-develop`, and earlier task worktrees. The shared Graphify graph and container copy are retained.

## Approved scope and Rule 73 status

The complete family ships in 1.5.0, where #1285's new layout and canonical transaction/publication owner already pass the probes. The release KeyCopy sites are already fixed by a80749ed9. Do not implement the historical 0x08 owner proposal or transpose develop's transaction/WAL machinery back into 1.4.4.

The mechanical family inventory, graph/source evidence, causes and proposed owner are preserved in [release-1285.md](release-1285.md), under "Rule 73 family analysis", and [the review](reviews/sol-review-1474.md). The Rule 73 analysis gate stopped production implementation; it is now superseded by the parking decision. No unfinished production extension exists. The WIP branch is an evidence archive, not a release fix or a ready-to-merge 1.5.0 PR.

## Done, unfinished, and next steps

Done: inventoried every transaction embedding-key staging path, retained reviewer probes, added staged/buffered/recovery/form/publication/durable-read probes, and reproduced release/head failures. Direct publication and durable large replacement gaps were found beyond the original review. Develop passes all 63 storage checks. The full earlier release race/compatibility/A/B evidence is retained as historical c26247cfc evidence, not proof of a completed family correction. No production code was changed after review CHANGES.

Unfinished probe adaptation:

- `pkg/cypher/sol_review_1474_public_test.go:37`: `sol1474PublicCount` hardcodes prefix 0x08. Its persisted public Cypher overlap check must count the new layout correctly for 1.5.0, or it could falsely report no orphan keys.
- `pkg/storage/release_1285_owner_probe_test.go:13`: transition matrix initially uses warm node-cache reads for vector equality. Those apparent large/large passes on release were invalid as durable proof; the separate close/reopen probe below establishes the real failure and must remain.
- `pkg/storage/release_1285_persisted_probe_test.go:10`: retain all four created/committed × sync/buffered close/reopen cases; they fail on release/head and pass on develop.
- Other probes are analysis overlays with WIP names/formatting. Promote and integrate them deliberately into the maintained 1.5.0 suites, preserving meaningful postconditions, rather than merging this release-based archive wholesale.

Next, in order:

1. Resume from current develop in a proper 1.5.0 checkout; use the existing #1285 new-layout owner. Keep this branch as reproducible release-era evidence.
2. Adopt the storage probes as permanent new-layout regressions, including staged create/delete/shrink, replacements, shard/metadata forms, concurrency, rollback, buffered flush/recovery and the stale large-vector-after-reopen case.
3. Adapt the persisted Cypher physical-key counter to the new layout and retain BEGIN / DETACH DELETE / embed / COMMIT on both parsers. Prove the counter can observe newly written embeddings before asserting their deletion.
4. Add the inventory's remaining repeated metadata-freshness/delete and cross-generation buffered replacement controls if needed for complete regression coverage; keep the unchanged passing preservation/rollback controls.
5. Run the 1.5.0 verification when the orchestrator releases the lab. No new tests or benchmarks during parking; do not schedule a release backport.

## Saved probes and evidence

Active test-only files in this WIP snapshot:

- `pkg/storage/sol_review_1474_test.go`: original reviewer transaction and overlapping sidecar probes, unchanged.
- `pkg/cypher/sol_review_1474_public_test.go`: original persisted public paths and Cypher overlap probe, unchanged and release-layout-specific as above.
- `pkg/storage/release_1285_owner_probe_test.go`: 33 sync/buffered/recovery transitions plus 12 shard/sidecar/metadata cases.
- `pkg/storage/release_1285_publication_probe_test.go`: four direct publication interleavings and injected second-batch rollback.
- `pkg/storage/release_1285_persisted_probe_test.go`: four cold close/reopen large-vector replacement cases.

Inventory and result lists: `sig/handoff/reviews/release-1285-owner-*.txt`. Bounded Graphify outputs and the original review/graph queries are under `sig/handoff/reviews/`. Fresh probe logs and historical compatibility/A/B sources/results are in `sig/handoff/reviews/release-1285-evidence/` in the pushed archive. The original workspace also retains all scratch files and the separate probe copies under `sig/handoff/reviews/release-1285-probes/`.

## Jobs and outcomes

All owned jobs are terminal; no queued/running/held jobs remain. **Cancelled job IDs: none. Jobs requiring resubmission: none.** No parking-time jobs or benchmarks were submitted. Every completed job remains reusable historical evidence; a later source change requires appropriately scoped new verification.

Fresh self-attacks (all `-race`):

| Job | Input | Result |
| --- | --- | --- |
| `i20261018134614-65172-3f1a` | PR head, storage + persisted Cypher, both parsers | Expected FAIL: 43 pass / 20 fail per parser |
| `i20261018134633-79276-1bca` | Release base, same probes, both parsers | Same expected FAIL |
| `i20261018134632-10344-ff35` | Develop, storage family | PASS all 54 cases |
| `i20261018134841-10824-cc63` | PR head, direct publication + large rollback | Four expected FAILs; rollback PASS |
| `i20261018134908-96812-2244` | Release base, same | Same expected failures; rollback PASS |
| `i20261018134908-13120-1bd0` | Develop, same | PASS all five checks |
| `i20261018135157-97132-a88f` | PR head, persisted large replacement | Expected FAIL all four |
| `i20261018135234-8396-e297` | Release base, same | Expected FAIL all four |
| `i20261018135234-18328-bba3` | Develop, same | PASS all four |

Earlier task-owned jobs, all terminal (details and commands in release-1285.md):

- `i20261018112508-86008-7afc`: historical pre-KeyCopy original regressions, expected FAIL.
- `i20261018112329-88168-b042`: actual v1.4.3 original regressions, PASS.
- `j20261018112434-93260-8d6d`: actual v1.4.3 bidirectional store compatibility, PASS.
- `i20261018112306-91204-e90b`: gofmt, PASS; `i20261018112538-52056-825a`: storage/multidb vet, PASS.
- `j20261018112306-52956-b800`: initial unsharded full race run, owner-stopped, exit 143; incomplete and superseded by the completed standard-shard run, no resubmission needed.
- `i20261018120307-98792-bb7e`: unchanged release label fixture control, PASS.
- `i20261018120649-53332-8fc3`: complete storage shard inventory, PASS.
- `j20261018120712-53332-c0aa`, `j20261018120712-53332-84d8`, `j20261018120713-53332-d35a`, `j20261018120714-53332-a483`: all full storage race shards, both parsers, PASS (2,174 pass, 2 existing opt-in skips per parser).
- `j20261018120648-20852-0500`: full multidb race, both parsers, PASS.
- `a20261018112328-105884-3ed8`: paired A/B completed, exit 0, all 20 rounds; measured costs/noise retained in release-1285.md and evidence. No queued A/B to cancel.
- `bash sig/release-parity.sh`: exit 0 / PARITY CLEAN at the recorded release/develop heads; historical open PR mappings were planned, not shipped. Reconciliation after closing #1474 belongs to the orchestrator's release ledger update, not renewed implementation here.

Review-owned job IDs remain in sol-review-1474.md; they were already complete and were not cancelled or resubmitted by this agent.

## Risks, questions and dependencies

No usable work at risk: probes, inventory/design and useful result artifacts are committed/pushed; all worktrees and the local graph are kept. The only required semantic adaptation before adopting the public probe is its old-layout raw key counter. Current develop already passes the storage family, so no additional develop bug fix is established. No unresolved implementation permission or cross-agent API dependency remains for this parked task. Future work depends on the orchestrator scheduling 1.5.0 regression adoption and lab access, not on a 1.4.4 fix.
