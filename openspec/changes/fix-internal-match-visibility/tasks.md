# NOR-177 internal MATCH visibility

- [x] Assign NOR-177 and reproduce indexed range and bound MATCH failures on release 0a01ce500.
- [x] Consolidate matching visibility at canonical row admission owners
      (pipelineNodeMatchesPattern, its initial-node candidate loop, and the
      ordered index window that truncates before rows are admitted).
- [x] Verify visible ordering/pagination and privileged system reads on both parsers
      (nornic and ANTLR package runs; system namespace asserted on the visibility
      decision and the admission test because the system database refuses graph reads).
- [ ] Review integrated changes, duplicate candidates and affected tests.
- [ ] Push release PR with AC evidence; complete CI and update Linear.
