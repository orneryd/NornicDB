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
