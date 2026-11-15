# Isolate range row-materialization allocation

- [x] Reproduce the explicit range allocation guard failing alone on release2950aa88f (11,978,608 bytes versus4MiB).
- [x] Share syntax-preparation measurement across implicit, explicit and aggregation guards without executing or caching a result.
- [x] Preserve the4MiB row-materialization assertion and exact empty aggregate result.
- [x] Verify implicit/explicit guards and aggregation cases in separate fresh processes with NORNICDB_PARSER=nornic and antlr.
- [x] Verify nearby range/aggregation semantics and go vet. Full suite/censuses remain in CI; no local full verification claimed.
- [x] Describe the exact measurement boundary and retained cold-parser cost; do not close B34 production cost from this test correction.
- [ ] Push candidate and dispatch exact-head CI.
