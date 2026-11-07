# APOC calls containing relationship COLLECT subqueries

Release base: 83b8c5884deaa12bc686ac40512d689a7d44a20f.

- [x] Preserve fail-before evidence for reported RETURN, CASE and WITH forms under both parsers.
- [x] Trace repeated row/shared evaluator handoff to missing dispatch ownership on the relationship-pattern fallback.
- [x] Make both fallback routes reuse one dispatch-state writer, retaining the existing depth/error contract.
- [x] Verify exact populated-graph results, including controls that distinguish a skipped subquery from correct subtraction.
- [x] Verify both parsers, pre-canceled requests and cancellation originating inside actual subquery work; stop within 100 ms after cancellation.
- [x] Complete focused race (three repeats) and affected-expression regression checks.
- [ ] Push candidate and run exact-head full make test CI.
- [ ] Publish Linear AC evidence and advisory duplicate dispositions.

Verification limits: original delegated full-package investigation was canceled
when the parent took over integration; it is not a passing full-suite gate.
A parent run inadvertently compiled the worker's restored baseline; it was
terminated as superseded. The restored fixed production source and final
regressions pass the independent focused run in /tmp/pr1748-independent-fixed-controls.log.
