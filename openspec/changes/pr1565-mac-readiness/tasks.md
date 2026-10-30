# NOR-28 / PR #1565 release readiness

- [x] Confirm release target, Tim's Linear assignment and non-T3 scope.
- [x] Audit PR body, conversation, inline review and unresolved threads.
- [x] Restack procedure ownership changes onto current release, preserving newer release owners.
- [x] Adapt obsolete admission and periodic context dependencies to current source.
- [x] Resolve all in-scope integration failures without weakening assertions or skipping failures.
- [x] Run make test on the final integrated head; preserve logs and report cache/skips.
- [x] Prepare the reviewed branch, updated PR description and NOR-28 readiness evidence for publication; the final published head and result receipt are recorded on PR #1565.

Verification policy: maintainer explicitly selected make test as the only local gate; broader qualification is deferred to CI. Initial make test was canceled after the obsolete txphase import surfaced; log /tmp/nornic-1565-initial-make-test.log is incomplete.

Focused evidence: /tmp/nornic-1565-family-after2.log (Cypher pass; HTTP's remaining missing-mode case retained); /tmp/nornic-1565-mode-after.log (HTTP remote mode pass after preserving includeStats in the typed request serializer). Fixture ownership, storage mutator classification, periodic boundary and rollback tests pass in their focused runs. The complete earlier integrated run failed and is retained at /tmp/nornic-1565-integrated-make-test.log. No failed tests or assertions were removed.

Current owners: retained ANTLR statement syntax for admission, live procedure registry for declared modes, one Fabric execution entry and callback binder, standard HTTP statistics unchanged with query_type as a separate result extension. The staged-code duplicate check passed; final check-in reruns it on the complete staged correction.

The corrected-source make test run at b81c0ec8e also failed; retained at /tmp/nornic-1565-corrected-make-test.log. Complete-suite feedback required publishing resolved mode before caching (preserving warm result identity), walking retained syntax without per-node temporary allocations, retaining the SHOW terminal owner, and teaching the source inventory to follow immediate executor callbacks while excluding deferred compiled handlers. All eight failing Cypher roots pass in focused runs after those corrections; the inventory still discovers and checks all 19 executable-text procedures. The unchanged repeated-property budget passes at 554 allocations versus 41,389 before; the allocation profile identified SyntaxNode.Walk as the owner. The remote fixture uses the same separate query_type field as production.

The search restore-coordinate test failed once in that full run. Twenty focused baseline repetitions passed; no baseline-failure waiver is claimed. The next complete make test must establish the integrated result. The separately started full Cypher diagnostic was canceled as redundant and remains incomplete (/tmp/nornic-1565-cypher-full-detail.log).

Final combined-source gate: make test PASS (exit 0) at 0544903ad7f6300cca78580987647dc7dbc2cdd3 on release base fc3eb17c9d6e538a017602bd77c2de1f807bfe04. Log: /tmp/nornic-1565-final-make-test.log. Every package passed, including the previously failing search test's complete package; no failure waiver was used. Existing opt-in/reference/subprocess skips remain; make test does not emit a full per-test skip census. Broader qualification is deferred to CI per maintainer direction. This completion record changes no product source; make test is rerun after committing it and the resulting exact published head is recorded in the PR receipt.
