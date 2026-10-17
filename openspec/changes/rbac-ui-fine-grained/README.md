# Fine-grained RBAC administration UI

Draft implementation plan for the UI half of
[#935](https://github.com/orneryd/NornicDB/issues/935): replace the coarse
`admin`/`editor`/`viewer` role checkboxes on the users list with an expandable
per-user fine-grained RBAC editor, and give the controls dirty state plus a
themed, confirming save. No implementation is included.

- [Proposal and scope](proposal.md)
- [Code-mapped architecture, component breakdown and data flow](design.md)
- [Dependency-ordered implementation checklist](tasks.md)
- Behavioral contract:
  [RBAC administration UI](specs/rbac-administration-ui/spec.md)

The first implementation gate is the backend privilege model from #935. Until
its canonical persisted privilege/evaluator subset exists, the UI is built
against the contract in `design.md` with a feature flag and explicit
"unsupported" empty states; no privilege toggle is rendered as authoritative
before its backend route accepts and persists it.

## Shared FGAC enforcement foundation

The [FGAC/F-PRIV convergence spec](../fgac-privilege-convergence/README.md)
records the 17 October decision to perform the permissions refactor with #935.
Its complete operation classification and canonical evaluator are shared backend
dependencies, not a second permissions implementation in this plan. F-PRIV
resumes after 1.4.4 ships for the planned 1.5.0 release and requires passing
whole-family review under Rule 73;
#1352 does not merge alone.
