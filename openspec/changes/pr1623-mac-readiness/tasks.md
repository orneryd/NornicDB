# PR 1623 release readiness

- [x] Confirm NOR-18 assignment, exclude T3 and audit all review channels.
- [x] Rebase onto release/v1.4.4 f66f9aede; preserve owning fixture helpers.
- [x] Reproduce and fix remaining failed-open WAL, manifest and constructor-inventory findings.
- [x] Run the initial integrated make test and diagnose its five borrowed-fixture regressions.
- [x] Prepare exact-head/base/result templates and current NOR-18 tracking for publication.

Local gate is make test only per maintainer. Broader qualification remains for CI.

All five runtime findings are corrected by owning the allocated engine while services/namespaces retain their borrowing contract. The final acceptance run and publication outcome are recorded in the PR result comment and NOR-18; this checklist records completed implementation and preparation, not an unexecuted gate pass. Broader checks remain for CI.
