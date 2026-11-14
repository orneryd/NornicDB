# Preserve appended WAL sequence boundaries

- [x] Reproduce an acknowledged post-restart node disappearing after a refused record and retained snapshot on release ddc59dc20.
- [x] Correct canonical record publication: derive the candidate number under the existing lock; publish it only after the complete append.
- [x] Reproduce the same omission with an old sealed range that includes an unwritten number; preserve persisted range floors on reopen.
- [x] Focused recovery, large-record admission, batch numbering and pure-encoder refusal checks pass.
- [x] Cover declined large records, rejected batches and buffered/property-dictionary refusal paths.
- [x] Verify property-epoch ordering, concurrent rollovers and affected WAL package behavior (171.770s affected storage package run).
- [x] Independent source review: same canonical append owner; no new sequence owner or forwarding layer.
- [x] Open PR1813 against release/v1.5.0 and dispatch exact-head CI after focused/affected-package checks pass.
- [x] Propagate the shared prune regression correction; three repeats pass16.117s.
- [x] Apply the explicitly approved rollback closure receipt refresh only; installed-record and stale-source/new-caller controls pass133.160s.
- [ ] Complete fresh exact-head CI after these integration follow-ups.

No Badger keys, WAL encoding or storage version change. The correction prevents new omissions; it cannot reconstruct writes already lost from an earlier replay output. The public storage recovery route is reproduced; a Bolt-path loss is not established.

Historical compatibility is limited to floors persisted in retained segment ranges or RetiredThrough. Absent old snapshot cuts cannot be inferred.
