- [x] Read existing full reproduction and current ListNamespaces, retire, releaseClosedStateLocked and canonical storage work admission owners.
- [x] Copy exact existing reproduction into isolated latest-release worktree.
- [x] Observe fail-first on current release base.
- [x] Use canonical lifetime owner for the full public read.
- [x] Verify focused public/lifecycle controls on macOS and Linux.
- [x] Run unchanged duplicate checker and inspect reuse candidates; report limitations.
- [ ] Commit and open release/v1.5.2 PR after parent review, update Plane inline evidence.

Evidence: base e8cd436e5, Mac baseline failed250000/250001; corrected focused Mac1.883s/Linux1.334s PASS. Linux container exit0/notOOM, logs archived then removed. Existing complete storage WorkLifetime owner reused; source-level comparison with Size/Sync/EstimateSize confirms the same admission/retirement contract, with only output/access semantics differing. Existing checker against configured live HTTPS service at matching graph baseline e8cd436e5: three changed functions, no duplicates,192.47s analysis. Initial inactive localhost endpoint and stale credentials failed before comparison, not passes. Final staged-tree check repeated after this note/changelog; no full suite/race/stress/benchmark claims.
