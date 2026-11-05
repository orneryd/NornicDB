# FGAC and privilege-enforcement convergence

**Planned release: 1.5.0**, introducing FGAC and replacing the legacy RBAC
model/checks in one convergence effort.

Spec extension for [FGAC #935](https://github.com/orneryd/NornicDB/issues/935):
perform the F-PRIV permissions refactor together with fine-grained access control.
This records the maintainer's 17 October 2026 decision; it does not implement
permissions, resolve review findings, or reopen the family for 1.4.4.

- [Scope and recorded status](proposal.md)
- [Canonical classification and integration boundaries](design.md)
- [Implementation and whole-family acceptance checklist](tasks.md)
- [Observable privilege-enforcement requirements](specs/privilege-enforcement/spec.md)

Resume implementation after 1.4.4 ships, as 1.5.0/FGAC work. F-PRIV remains
under [Rule 73](../../../.agents/rule73.md): integrate the complete family and
obtain one passing review of the whole correction. #1352 must not merge alone.
The spec branch starts at `release/v1.4.4`; implementation must refresh the
inventory and integration route against current `develop` before proceeding.

Related plans: [RBAC administration UI](../rbac-ui-fine-grained/README.md) and
[enterprise impersonation/CDC](../enterprise-cdc-impersonation-query-api/README.md).
