# PR #1503 / NOR-44 readiness for release/v1.4.4

Base: merged PR #1406 release commit `fc48d85d1cda694689e67e0d1ccc7b2b02ca1cd7`.
Keep T3 and #1679 outside this work. The full local verification gate is `make test`.

- [x] Select the next urgent user-assigned release PR with unfinished acceptance: #1503 / NOR-44.
- [x] Pull the merged release head and isolate an immutable checkout for remote graph refresh.
- [x] Complete and independently verify remote release graph refresh before the duplicate check.
- [x] Migrate Go access-metadata reads to typed `ReadAccessMeta`; migrate interface, callers, mocks and docs without compatibility wrappers.
- [x] Resolve the remaining relationship-type snapshot and adjacency-cache owner findings from #1406's last duplicate check.
- [x] Reproduce #1502's count scoping and DROP/recreate acceptance on current release; port required behavior into current owners rather than importing the old parent stack.
- [x] Reconcile #1503 review comments and unfinished acceptance criteria; retain failing evidence and fix regressions.
- [x] Qualify affected reads/count/lifecycle workloads with matched measurements and CPU profiles.
- [x] Run the enabled duplicate checker against the refreshed graph and resolve every finding.
- [x] Run uncached `make test` on the integrated candidate and fix every failure.
- [x] Push a reviewable PR against release/v1.4.4 and update Linear with exact evidence.

#1406's final-source concurrent UNIQUE requalification remains tracked under NOR-37;
this task does not claim it complete or transfer its other acceptance criteria.

Progress: the remote code graph is independently confirmed at `fc48d85d`; the detached checkout has no branch label. Metadata and count forwarding, public Cypher/composite/transaction/lifecycle acceptance, and catalog failure/recovery checks pass focused normal/race runs. Initial public count reproduction returned 8 instead of 1. A rejected catalog write also reproduced data loss before the catalog-intent correction. Checker rounds 1–3 required further complete-family consolidation; all findings are being resolved with the analyzer and hooks unchanged. The first full run failed source guards/census; those causes are corrected. The subsequent full run was canceled as incomplete when checker round 3 required production changes. A fresh final full gate is still required. Earlier adjacency timings are provisional for the pre-consolidation source; final qualification remains pending. Evidence: `/Users/timothysweet/.local/share/nornicdb-pr-readiness/post1406/pr1503`.

Final source freeze: shared stored/staged suppression, visibility and indexed-overlay owners pass focused race and census mutation proof. The census reports no violations. The final matched native measurements show −9.13%/−2.96% cached adjacency time; other measured timings are −2.55% to +0.86%, with effectively unchanged allocations. No manager disk DROP or concurrent UNIQUE performance claim is made. Fresh enabled checker and uncached full suite remain pending.

Full-run correction round: `make-test-publish` completed with 110 package passes
and four failures; the full census (including retained mutants) passed. This is
a failed full gate, not readiness evidence. Unfiltered isolated reruns reproduced
the inventory/Restore failures; search and server passed in isolation.

- [x] Review and refresh the five changed endpoint inventory expressions without changing detection or adding exemptions; observability package passes.
- [x] Preserve native prefix-DROP/Restore quota assertions and add manager catalog-DROP/Restore offline coverage; focused storage/multidb races pass.
- [x] Recover the empty-destination assertion and add a deterministic empty-label control; select real populations and preserve exact destination/nonzero membership assertions.
- [x] Own the readiness test service before producer events; cancel in captured baseline iteration with a phase witness. All original error/status assertions pass 30 race repetitions.
- [x] Re-run the enabled checker and uncached full gate, retaining unfiltered Go output as well as normal make output.

Full-run corrections: catalog Restore coverage now distinguishes native prefix deletion (catalog/quota retained) from admitted catalog DROP (offline across Restore). The cluster axis experiment was rejected for ranking regressions and reverted; original noisy vectors remain, and the forced-empty-label case reproduces the old move precondition failure. Final focused cluster restore/first-build checks pass ten race repetitions; the earlier complete generation family passes three. No production Go source changed from the measured/full-census candidate.

Final integrated result: `checker-shared-move` passed (203 functions, no duplicates).
The remaining movement helpers were consolidated into `moveClusterMember`, retaining
same-cluster coordinate and cross-cluster membership assertions; the complete
cluster-generation family passes three race repetitions. `make-test-final2` passed
uncached (`GOFLAGS=-count=1 make test`), including the retained census mutants.
Checked source SHA-256: `5868553fb169a98c55e90209d8521d0d2ee754709774cdf52653b12704ba419c`.
All production Go hashes remain identical to the measured candidate. Earlier failed
and canceled runs remain preserved and are not counted as passing evidence.

Published correction commit `694e166fc748cfbc53b823b429469e7477eed53f` to PR #1503, retargeted to release/v1.4.4, and marked ready for review. NOR-44 is Dev Complete / Urgent with current acceptance evidence; obsolete NOR-36 blocker removed. Public issue #1502 remains open until release-branch merge. This completion update changes documentation only.
