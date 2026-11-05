- [x] Reproduce the installed hook's unborn-HEAD failure through actual Git commits.
- [x] Keep the installed hooks and configured live database contract active.
- [x] Verify first commits, existing matches, absent/partial baselines, missing
      configuration, failed authentication, staged snapshots and cursor exhaustion.
- [x] Remove the experimental empty-database allowance; commit checks create no databases.
- [x] Verify shared worktree identity, no-origin fallback and existing database overrides.
- [x] Install the tested runtime and attempt a live divergent leaf-patch commit;
      prove rejection without changing HEAD or losing staged changes.
- [x] Rerun the verification preview with live hooks and report remaining blockers:
      the new `plan` self-test repository has no ingested graph; enforcement remains active.
- [x] Document mandatory failures, shared worktree identity and baseline requirements.
- [ ] Publish the validated tooling correction.
- [x] Preserve test/production separation (explicit maintainer decision); a
      benchmark-to-script copy is outside that comparison population.
- [ ] Resume #1406's ordered-publication performance work.
