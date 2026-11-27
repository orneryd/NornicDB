# N152-362: proportional parked failure listing

- [x] Trace the public listing, sidecar scan, count, retry, and storage wrappers on the release base.
- [x] Capture a fail-first test and current workload baseline with few failures and many vectors.
- [x] Add a failure-only durable lookup and upgrade path that preserves count, retry, namespace, reopen, backup, and delete behavior.
- [x] Verify the public HTTP path and focused storage/worker tests, then compare the same workload after the fix.
- [x] Run affected package checks and review duplicate owners, security, and the integrated diff.
- [ ] Open the release/v1.5.3 PR and update Plane with exact verification and remaining release gates.
