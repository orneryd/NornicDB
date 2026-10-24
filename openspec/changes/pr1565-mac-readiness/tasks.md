# NOR-28 / PR #1565 release readiness

- [x] Confirm release target, Tim's Linear assignment and non-T3 scope.
- [x] Audit PR body, conversation, inline review and unresolved threads.
- [x] Restack procedure ownership changes onto current release, preserving newer release owners.
- [x] Adapt obsolete admission and periodic context dependencies to current source.
- [x] Resolve all in-scope integration failures without weakening assertions or skipping failures.
- [ ] Run make test on the final integrated head; preserve logs and report cache/skips.
- [ ] Push the reviewed head, refresh PR evidence and update NOR-28.

Verification policy: maintainer explicitly selected make test as the only local gate; broader qualification is deferred to CI. Initial make test was canceled after the obsolete txphase import surfaced; log /tmp/nornic-1565-initial-make-test.log is incomplete.

Focused evidence: /tmp/nornic-1565-family-after2.log (Cypher pass; HTTP's remaining missing-mode case retained); /tmp/nornic-1565-mode-after.log (HTTP remote mode pass after preserving includeStats in the typed request serializer). Fixture ownership, storage mutator classification, periodic boundary and rollback tests pass in their focused runs. The complete earlier integrated run failed and is retained at /tmp/nornic-1565-integrated-make-test.log. No failed tests or assertions were removed.

Current owners: retained ANTLR statement syntax for admission, live procedure registry for declared modes, one Fabric execution entry and callback binder, standard HTTP statistics unchanged with query_type as a separate result extension. The staged-code duplicate check passed; final check-in reruns it on the complete staged correction.
