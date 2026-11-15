# Preserve correlated CALL imports

- [x] Reproduce scoped FOREACH and transactional wildcard failures on release7f565720f, both parsers.
- [x] Seed implicit-body inputs at admission; remove the proposed unknown-input fallback.
- [x] Reuse readableBindings for FOREACH, transferring only private maps to avoid extra copies.
- [x] Migrate all internal binding-owner callers; no exported Go API changes.
- [x] Independent current-source scoping/ownership review complete.
- [x] Public result and stored-mutation controls pass, including outer-bound unimported variables, iterator shadowing and nested FOREACH (1.623s).
- [x] Current integrated focused race passes2.844s.
- [ ] Current-head CI.

Worker proposal's full Cypher package passed646.513s before parent ownership convergence;
that is not a current-head full-package pass. Parent focused checks qualify the final
source; CI owns broad verification. The first default600s package attempt timed out
and is incomplete. No runtime failure was established by that timeout.
Graph retrieval returns HTTP-to-HTTPS400; manual canonical-owner investigation is
complete, but no clean graph duplicate result is claimed.
