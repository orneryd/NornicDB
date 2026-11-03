# PR #1670 / NOR-139 readiness

- [x] Select oldest eligible Urgent open PR using current Linear release scope; older PR tickets are deferred to v1.4.5.
- [x] Assign NOR-139 to Tim; preserve original PR head e8a15ee59 and its historical evidence.
- [x] Start rebase of the single correction onto release f0074ec55; no old stacked parents.
- [x] Establish current release outcome for unchanged-key and committed-endpoint failures; retain original assertions and RED/GREEN provenance.
- [x] Complete production/test API integration preserving request cancellation and accepted completion ownership.
- [ ] Verify Execute/Bolt/HTTP auto/explicit, both parsers, moves/deletes/readiness and exact counters.
- [ ] Profile allocation/retained-memory costs and remove avoidable endpoint-fold overhead; rerun timing sequentially now host contention is removed.
- [ ] Regenerate/review inventories and run informative best-effort duplicate analysis; no checker changes.
- [ ] Freeze integrated source and pass make test, fixing every encountered regression.
- [ ] Push with exact lease and update PR/Linear AC evidence; maintainer owns merge.
- [ ] Clean completed owned resources; preserve shared Go cache and other active agents.

- [x] Revalidate NOR-374/375/376/377: unchanged claims, ACK composite reads and three direct constraint cases remain defective; ordered group visibility already passes.
- [ ] Correct all newly reproduced related gaps in this PR and qualify post-drain/key-reuse controls.
