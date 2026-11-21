# N152-56 / NOR-175 — exported APOC signature admission

- [x] Reproduce the original ticket's regression against release base 1a6eb031471688a71966a68147740ead6c43d6cb.
- [x] Add the two missing signature plans to the existing apocArgumentPlans owner; preserve strict source strings and declared map binding.
- [x] Preserve the original complete ten-name population extractor and typed CypherArguments reference to the same callable.
- [x] Preserve handler errors, identity, invalid-call rejection, no-backend-operation sentinel and single real-node backend controls.
- [x] Parent review replaced the proposed worker fixture with the stronger original ticket suite: Initialize is once-only, so it cannot establish the active backend; retained tests use SetStorage and restore it in cleanup.
- [x] Independently pass all APOC packages, focused race checks and final root/registry tests; production source is byte-identical between the parent verification checkout and final branch.
- [x] Run canonical verification discovery and explicitly record unselected release checks.
- [x] Run the existing staged duplicate checker; record its analysis/ingestion failure without claiming clearance.
- [ ] Full release qualification and available graph duplicate analysis before merge readiness.
