# PR 1623 release readiness

- [x] Confirm NOR-18 assignment, exclude T3 and audit all review channels.
- [x] Rebase onto release/v1.4.4 f66f9aede; preserve owning fixture helpers.
- [x] Reproduce and fix remaining failed-open WAL, manifest and constructor-inventory findings.
- [ ] Run make test on the integrated source; fix PR regressions without added skips.
- [ ] Publish exact-head/base/results and update NOR-18.

Local gate is make test only per maintainer. Broader qualification remains for CI.
