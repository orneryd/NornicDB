# N152-344: bounded change-feed reads after retention

- [x] Reproduce the legacy prefix-seek cost on a tombstone-heavy store and profile both bound seeks.
- [x] Record the first retained event in each prune batch and the last event in its capturing commit, in snapshot-visible metadata.
- [x] Make bounds and event reads use the retained cursor; backfill metadata on the next retention pass for older stores.
- [x] Cover pruning, old-store backfill, transaction snapshots, reopen, and namespace drop.
- [x] Run focused Cypher consumers and the full affected storage package on the final head.
- [ ] Review the integrated diff, push the branch, open the release PR, and post exact-head verification.
