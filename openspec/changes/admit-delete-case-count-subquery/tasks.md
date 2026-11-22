# N152-147 / NOR-80 — TODO

Artifact creation is not implementation. Recovery, ownership tracing and separation are done;
the follow-up sections below track approved implementation and verification.

## Done in this change (artifacts only)

- [x] Recover the ticket-supplied reproduction matrix to `evidence/nor79_to_82_static_claims_test.go.txt`
  and record its exact SHA256 (`ced90f8c…18c5`); recover NOR-80's scoped probe block
  (`857747c4…d941`) and the recorded Neo4j outcomes (`15aaf511…4615`).
- [x] Prove the recovered text parses as Go and is gofmt-identical (`gofmt < file` digest equals the
  file digest). Not compiled into the checkout.
- [x] Record the reference verdicts and the proven scope per probe: which are fresh Neo4j 5.26.30
  results, which are reconstructed, which are historical or canonical in-repository data, and which
  named populations were never supplied.
- [x] Trace the recorded false rejection to one heuristic and one call site
  (`validateDeleteClauseScope` → `deleteExpressionRootIdentifier`), and show that the canonical
  `expressionFreeVariables` scanner the runtime DELETE step already uses yields the correct answer.
- [x] Establish continuing applicability: the four causal/dependent files are unchanged between the
  ticket head `d6ec7766…` and this checkout; the only in-range differences are unrelated (FGAC SET
  path, SHOW/admin).
- [x] Separate the sibling defects: NOR-79/81/82 cells and owners are marked out of scope; other
  worktrees own them.
- [x] Record the validation contract, the existing tests that constrain the change, and the open
  items that static reading could not settle.

## Proposed correction (parent decides; proposed, not accepted architecture)

- [x] Accept, amend or reject the minimal correction in `proposal.md`: replace the leading-identifier
  question in `validateDeleteClauseScope` with `firstUndefinedSetExpressionVariable` /
  `expressionFreeVariables`, keeping `deleteUndefinedVariableError` as the error owner.
- [x] Decide the fate of `deleteExpressionRootIdentifier`
  (`pkg/cypher/executor_mutations_delete_guard.go:156-175`) and its comment reference
  (`pkg/cypher/set_semantic_validation.go:47-49`) once it has no caller.
- [x] Decide whether the stricter nested-reference admission (`DELETE n.list[x]`, `DELETE id(x)`)
  belongs to this causal unit or needs its own ticket.
- [x] Decide where the regression lives: the recovered matrix file, `staticTypeNeo4jVerdicts`
  (no DELETE entry today), or an existing DELETE test. Do not create a second verdict owner.
- [x] Establish the owner of the second DELETE message shape ("Variable \`m\` not defined",
  `subquery_update_scope_test.go:26-31`) before the message contract is called unchanged.

## Verification (must be run before anything is marked done)

- [x] Fail-first: add the reproduction at the public path (`Execute`, both parsers, auto and
  explicit transaction, RUN/EXPLAIN/PROFILE) and record the failing baseline on the actual base.
- [x] After the correction, show all 12 NOR-80 DELETE cells matching the reference in one run, with
  the two procedure-filter probes' 24 cells still matching.
- [ ] `TestDeleteRejectsInvalidTargetsBeforeMutation`, `TestUndefinedVariableInUpdateClauseIsRejected`,
  `TestMalformedOperandsAreSyntaxErrors`, `deleteClauseTargets` cases,
  `TestQueryBodyContainersMatchGrammar`, `TestNor79To82NegativeControlsReuseCanonicalVerdicts` pass.
- [x] Non-empty-input behaviour of a DELETE `CASE` with a `COUNT { … }` operand is either verified or
  explicitly excluded from the claim; the recorded matrix only proves empty-input static admission.
- [ ] Run the focused package command and `make verify BASE=<pr-base>` (or record which steps were
  skipped and why); post head, base, command, results and skips as `CONTRIBUTING.md` requires.
- [ ] Re-establish continuing applicability of the recovered/recorded evidence against the integrated
  head, and keep the ticket's own limits (reference container stop, unrecovered populations,
  attachment-only triage) in the final report.


## Parent-approved implementation follow-up

- [x] Fast-forward tracked-clean tree from `122130b27` to release
      `3aa1f1dceb758155f249d6c984c546e088ae40f9`, preserving untracked OpenSpec.
- [x] Establish the earlier bound-compilation owner of `Variable ... not defined`;
      retain the semantic DELETE expression message separately.
- [x] Prepare `delete_count_admission_test.go`: exact 36-cell recovered matrix,
      24 non-empty mutation cells and 60 scoped negative cells.
- [x] Run original 36-cell matrix RED: 12 DELETE failures, 24 procedure-filter
      controls accepted; 1.285s, `/tmp/n152-147-red.log`.
- [x] Apply approved canonical scope-reader correction and remove orphaned helper
      and its unused import; preserve the existing error argument.
- [x] Verify non-empty mutations, null/unchosen arms, negative scopes, existing
      DELETE/update-subquery/malformed operand and body-container tests.
- [ ] Record final tests, race, vet, verification/duplicate-check limits and cleanup.

No build/test execution or production edit occurred in this follow-up yet.


## Runtime gap and convergence follow-up

- [x] Preserve first integrated failure: only 24 non-empty CASE/COUNT cells failed,
      5.025s, `/tmp/n152-147-focused-green.log`.
- [x] Reuse the canonical context-aware expression evaluator and existing failure
      slot at the DELETE mutation boundary; preserve unresolved/type/error behavior.
- [x] Defer context-dependent targets to reached rows using existing context-needs
      owner; retain the context-free constant precheck.
- [x] Add all-null non-empty target and empty/reached COUNT-body error boundaries.
- [x] Integrated normal selection passes, 3.889s, 132 new cells plus existing guards;
      `/tmp/n152-147-focused-final.log`.
- [x] Focused race selection, including public/direct canceled adjacency guards,
      bound DELETE, FOREACH/connected deletion and existing message controls.
- [ ] Package vet and parent canonical verification / staged duplicate check.


## Final bounded handoff

- [x] Normal focused suite: 84 selected top-level tests, PASS 3.889s; 132 new cells.
- [x] Race focused suite: 15 selected top-level tests, PASS 6.057s.
- [x] Package vet: PASS, empty output.
- [x] Source-only staged whitespace check passes; full diff retains eight known
      historical transcript heading warnings by explicit parent instruction.
- [ ] Parent-owned canonical verify, integrated release-head verification and
      duplicate-check result incorporation. Focused checks are not full release proof.
- [x] Preserve fixture, raw transcript and failure/success logs; keep active worktree
      and fix branch for parent integration. No task-created cache/container/image.


- [x] Run `make verify-list BASE=origin/release/v1.5.2` (exit 0) and scoped
      `make verify BASE=origin/release/v1.5.2 ONLY=localization` (localization pass,
      eight other steps unexecuted); record exact logs and skips.
- [x] Run staged duplicate hook: exit 1, analysis/ingestion incomplete. Canonical
      CONTRIBUTING treats this as advisory; do not claim a clean result or readiness.
- [ ] Parent commit/rebase onto advanced release 70ea82df6 and integrated race/full
      verification. Current source ownership released to parent.
