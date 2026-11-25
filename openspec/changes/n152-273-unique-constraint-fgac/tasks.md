# N152-273: FGAC unique constraint oracle

- [x] Reproduce held-versus-absent hidden value outcomes through public HTTP and Bolt writes on the release base.
- [x] Reject writes to unreadable unique and key properties before storage validation, including composite siblings and label additions.
- [x] Preserve unconstrained property writes and readable unique property writes.
- [x] Redact node and relationship uniqueness diagnostics on restricted requests without changing the violation class.
- [x] Cover CREATE, SET, MERGE, relationship writes, label additions, direct and transactional storage paths, and both parsers with focused tests.
- [x] Cover hidden relationship endpoints and concurrent schema additions through direct, bulk, staged, and deferred-commit writes.
- [x] Run full affected package checks and review the final integrated source for security and duplicate owners.
- [x] Open a release/v1.5.3 PR with exact head/base verification and update Plane.
