# Release audit inventory repair

- [x] Capture completed release CI failures and pin current release head.
- [x] Reproduce numeric ownership failures, including the newer WAL scanner revision.
- [x] Review typed auth string metadata and historical WAL envelope ownership.
- [x] Update existing reviewed numeric records; pass convert and auth packages.
- [x] Review and reconcile lexical and endpoint population records.
- [x] Pass integrated affected packages and focused WAL migration checks.
- [x] Open and push a separate release/v1.5.0 PR (#1777).
- [ ] Request final candidate exact-head CI.
- [ ] Record CI and duplicate-review results without claiming pending checks passed.

Scope: reviewed source manifests only; no scanner, assertion, threshold, hook,
production behavior, persisted payload, or public API changes.
