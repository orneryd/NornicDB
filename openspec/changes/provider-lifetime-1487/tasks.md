Resumed at maintainer request after macOS installation work. Mandatory live duplicate checking is restored; automatic ingestion hooks remain paused. Prior canceled runs remain incomplete.

- [x] Delete temporary task artifacts/worktrees, including dirty temporary worktrees as explicitly requested; preserve branches.
- [x] Read #1487 and the #1465/#1469 family, unresolved review and shutdown consumers.
- [x] Reproduce late publication on original #1487 and premature provider Close on the prior lifecycle owner for ordinary/typed background calls.
- [x] Integrate the existing shared DB provider owner; keep actual batch calls synchronous within joined workers. Validate current-release integration before publication.
- [x] Restore DB-side embeddings in localization tests and remove the queue-publication wait.
- [x] Run deterministic lifecycle/race checks, affected packages and native checks.
- [ ] Complete final frozen-head required verification and post exact head/base results and skips.
- [x] Publish integrated source and update #1487 title/description with the full runtime scope and verification limits.

- [x] Apply maintainer instruction: remove forwarding constructor APIs, migrate every Go consumer and DB factory to the single context-taking constructor, update examples.
- [x] Replace DB.Open salt generation with existing encryption.GenerateSalt owner; encryption regressions pass.

- [x] Converge single-flight construction/wait assertions into one test owner, retaining both cases; 10-repeat race check passes.
- [x] Consolidate model warmup launch/loop/completion into one owner; native warmup/Close race checks pass.
- [x] Share bounded HTTP shutdown between database HTTP and standalone MCP; MCP cancellation regression fails before correction and both public Stop paths pass 10-repeat race checks.

- [x] Consolidate handler/resolver database-manager fixtures into one cleanup owner; remove both duplicate helpers and migrate their callers.

- [x] Preserve both current-release GraphQL cleanup regressions; consolidate subscription available/missing-broker assertions as explicit cases.

- [x] Release held constructor/provider fixtures before DB cleanup on assertion failure, so lifecycle regressions cannot deadlock their own cleanup.

- [x] Move the lifecycle implementation write-up to this OpenSpec change, retain the constructor migration note here, revert the PR's edit to the pre-existing Local GGUF plan and remove the changelog's stray list blank line.
