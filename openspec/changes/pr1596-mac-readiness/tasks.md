# PR 1596 release readiness

- [x] Confirm NOR-21 assignment and exclude T3 and PR 1612.
- [x] Audit all PR review channels and restack only the UNION correction onto current release.
- [x] Port schema/iterator handling to the existing canonical owners without restoring deleted readers.
- [x] Validate nested rows, schemas, error status and planner ownership; port obsolete-parent controls to exact release baselines.
- [x] Prepare the frozen-source make test invocation and preserve baseline/focused diagnostic evidence.
- [x] Prepare PR publication evidence and current NOR-21 tracking.

The maintainer-selected local gate is make test only; broader verification remains for CI.

The acceptance result for make test and the remote publication outcome are recorded in the PR result comment and NOR-21 after execution. This preparation checklist does not assert an unexecuted pass. PR 1612 is owned by another agent and is untouched.
