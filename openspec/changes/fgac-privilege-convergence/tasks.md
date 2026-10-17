## Scheduling and Review Gate

Implementation is deferred until after 1.4.4 ships. These unchecked tasks belong
to the combined 1.5.0/FGAC family; writing this plan completes none of them.
F-PRIV remains under Rule 73 and #1352 must not merge independently.

## 1. Re-establish the Integrated Baseline

- [ ] 1.1 Preserve the supplied inventory counts (227 correct, 319 wrong,
      1,120 unchecked, 3 runtime unknown; total 1,669), prior Codex/Daybreak
      reviews and exact failing reproductions with their source revisions.
- [ ] 1.2 Rebase the implementation route onto current develop, including merged
      #1350/#1364. Re-extract the complete operation population and reconcile
      every addition/removal/change against the historical inventory.
- [ ] 1.3 Map every unresolved family finding, including Daybreak's reported
      P1 on #1393, to exact public regression IDs and expected outcomes.
      Verify current status rather than assuming an older PR description proves
      resolution. Reproduce the defects before modifying implementation.
- [ ] 1.4 Pin PD's commit-time visibility contract and preserve its failing
      transaction reproduction, including committed/rolled-back side effects.
      Pin path-selector and WRITE_RECORD regressions and the Neo4j Enterprise
      reference behavior for own-transaction termination and privilege classes.

## 2. Converge Classification with FGAC

- [ ] 2.1 Give every inventoried command, procedure, function and transport
      operation an explicit reviewed class/effect and canonical evaluator path.
      Unclassified and runtime-installed operations fail closed until admitted
      through that contract; enumerate aliases and nested execution as well.
- [ ] 2.2 Integrate #935's canonical grant/deny, principal/policy revision and
      graph-visibility model; replace the old RBAC model and superseded ad-hoc
      checks across the whole family. Define and verify the existing-record migration through the
      shared migration owner before cutover; do not retain dual authorities.
- [ ] 2.3 Correct path-selector and WRITE_RECORD classification/propagation
      through both parsers and public entry points without weakening the
      existing client-text/internal-execution boundary.
- [ ] 2.4 Implement and verify the requested transaction commit-time visibility
      check, preserving accepted atomicity and rollback semantics.
- [ ] 2.5 Deny inaccessible HTTP database operations before existence disclosure
      or database/service setup; test existing and nonexistent inaccessible names
      and assert the absence of persistent setup effects.
- [ ] 2.6 Correct own-transaction termination without requiring unrelated general
      read permission; independently test denial for another user's transaction
      without its required management permission.
- [ ] 2.7 Resolve and test the index-procedure and APOC-export classes, including
      effects, scopes, deny/missing-grant cases and persistent side effects.
- [ ] 2.8 Integrate dependent UI and impersonation consumers with the same
      canonical contract; satisfy the full #935 acceptance, not only collection
      filtering or one transport's checks.

## 3. Whole-family Acceptance

- [ ] 3.1 Run complete inventory reconciliation and public-path regression
      matrices, including runtime registration, nested execution, aliases,
      supported parsers, autocommit/explicit transactions and impersonation.
- [ ] 3.2 Verify #935 behavior against the supported Neo4j Enterprise reference;
      preserve exact errors, observable graph results, commit/rollback state,
      policy/cache isolation and fresh-session persistence evidence.
- [ ] 3.3 Record workload-scoped before/after latency and allocation evidence for
      affected admission/query paths. Complete applicable repository checks on
      the integrated immutable head; report failures, caches, skips and limits.
- [ ] 3.4 Obtain one passing review of the entire integrated F-PRIV/FGAC family
      after all retained Codex/Daybreak findings are corrected. Keep the family
      blocked if review fails; do not merge #1352 alone or claim completion from
      individually reviewed pieces.
