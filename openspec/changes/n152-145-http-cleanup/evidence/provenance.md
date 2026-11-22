# Provenance — N152-145 (NOR-78) reproduction evidence

This records the worker's historical extraction and source investigation before the
approved implementation. Fresh reproduction and candidate results are in
`current-verification.md`; references to no tests below describe the worker's activity only.

## Ticket

- Plane work item `21ba9ac0-f7ef-437f-b30e-b276d99aceab`, sequence 145 (N152-145).
- Title: `NOR-78 · Composite HTTP statement errors include a spurious TransactionNotFound
  cleanup failure`; priority medium; not a draft.
- Created `2026-11-18T16:53:33Z`, updated `2026-11-20T17:34:38Z`.
- Body is a release tracking card for N151-42; the original issue retains its discussion,
  history and attachments. The card records the 2026-11-19 disposition ("Ready narrowed
  current error-reporting defect") and states: existing ticket Ready in 1.5.2, Medium,
  entire original and both comments reviewed, no partial commit or data-loss result
  established.

## Extraction

Source: the ticket's `description_html` (30,314 characters, four `<pre><code>` blocks).
Read locally from the authorized file `/tmp/n152-145-ticket.json`.

Only transformation applied: HTML-unescape the block and strip the surrounding
`<code>`/`</code>` tags. No line, whitespace or escape was edited or reflowed.

| Evidence file | Ticket block | SHA-256 |
|---|---|---|
| `nor78_remote_http_rollback_test.go.txt` | 1 of 4 (fixture) | `862fc5d55c45d83e07fba6c0b95c72e6228aa9174672f6958c73b3192200eeda` |
| `reproduction-command.sh.txt` | 2 of 4 (commands) | `6507c0d44e4ba7efd9fb61e946fb77f550a86fc83e820b3637c06d6a24f68c53` |
| `observed-failure-excerpt.txt` | 3 and 4 of 4 (macOS, Linux logs) | `db027b2afd47a46e3384df0f8b347a9032044768c25da8c6e8e05fc6d8752075` |

The excerpt keeps only the original test-run and failure lines (INFO lines omitted);
kept lines are byte-identical to the ticket, including tabs and literal `\n`/`\t` escapes.

Cross-check that the extracted fixture is the file that produced the log: the archived
output cites `nor78_remote_http_rollback_test.go:84`, `:87` and `:98`, which are exactly the
`statement HTTP=` log line, the `assert.Len(t, failed.Errors, 1, …)` assertion and the
`subsequent DELETE HTTP=` log line in the extracted source.

## Fixture in this worktree

An untracked `pkg/server/nor78_remote_http_rollback_test.go` is present in this worktree and
is byte-identical to the extracted evidence
(`sha256 862fc5d55c45d83e07fba6c0b95c72e6228aa9174672f6958c73b3192200eeda`, 114 lines,
`diff` empty). Its mtime is inside this session, and no command in this task created,
modified or ran it; this investigation only compared it read-only. It is recorded here as
the reproduction fixture already staged in the worktree, not as an artifact of this
investigation, and no production edit or test run is claimed.

## Revisions

- Ticket pinned revision: `d6ec7766e6e73dbae401ae547aa3c8b6e4c327b5` ("Sync v1.5.2 release
  with main (#1860)"), present in this repository.
- Ticket compares against shipped v1.5.1 `cb0a574f75b183dfebfba6c4726b72244f7f374b`,
  present in this repository.
- Worker investigation checkout: `70ea82df61970944f017290e59ec1cea7f2d61fa`
  (`v1.5.1-63-g70ea82df6`, "fix(cypher): preserve temporal addition types (N152-149) (#1919)").
- The reproduction fixture is committed neither at the pinned revision nor at the current
  checkout. The archived log paths confirm where it ran: macOS from
  `…/worktrees/nor78-v151-remote-rollback/NornicDB/pkg/server/`, Linux from
  `/workspace/pkg/server/`.

## Freshness at the worker investigation checkout

Traced files compared with the pinned revision `d6ec7766…`:

- `pkg/storage/remote_engine.go`, `pkg/fabric/remote_executor.go`,
  `pkg/cypher/transaction.go`, `pkg/cypher/executor_internal.go`,
  `pkg/server/composite_transport_cleanup_test.go`: unchanged.
- `pkg/txsession/manager.go`: only the `Session.ClientPolicy` binding was added; the
  rollback owner `rollbackAndDelete` is unchanged.
- `pkg/errors/status.go`: only `storage.ErrStorageClosed` was added to the transient-status
  rule; the embedded-`Neo.…:`-code fallback used for the spurious error's code is unchanged.
- `pkg/server/server_db.go`: changes are authorization plumbing (`canAccessGraphForRequest`,
  fine-grained permissions) and query-API result metadata. The transaction statement,
  rollback and commit handling cited in `proposal.md` is unchanged.

Conclusion: the traced causal path is present in the current checkout. This is source
analysis only; the archived failure was measured at the pinned revision and was **not**
reproduced at the current checkout by this change (no tests or builds were run).

## Ticket qualifications carried forward

- "Exact historical expired D11 binary fixture is not claimed recovered."
- "Authentication header injection follows canonical independent-test-user-store fixture; it
  does not qualify forwarding/RBAC."
- The parent fixed helper ownership to use the current subtest `T` before the final
  macOS/Linux run; the initial pre-helper-correction log is preserved in the ticket but
  superseded. The macOS/Linux runs in the excerpt are the post-correction runs
  (`macOS 2.313s` / `Linux 2.356s`, both parsers failing only at `d=0`, active controls
  passing, Linux exit 1 and not OOM).
- The ticket records no full suite, race, stress or benchmark run and no production edits;
  logs and inspect output were archived and the exited container was removed.

## Tooling limitation

The supplied Graphify retrieval was incomplete (`status: incomplete`, empty code baseline,
remote search unreachable). No graph or duplicate-checker evidence is claimed; owners were
traced directly in the checkout (see `duplicate-review.md`).
