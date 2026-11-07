# Relationship mutation preimage validation

- [x] Rebase the preserved reproduction onto release 83b8c5884.
- [x] Reproduce the stale implicit writer; retain the explicit conflict control.
- [x] Share the existing node/relationship first-mutation fold and coherent read cut.
- [x] Reject supplied stale observations before staging; validate staged predecessors before publication.
- [x] Cover repeated updates and in-frame CREATE followed by UPDATE.
- [x] Exercise buffered/replayed peer publication and successful retry with final counter=2.
- [x] Independently review canonical callers and public query coverage; both parsers pass conflict/retry controls.
- [ ] Run focused race and affected-package tests; report exact source and skips.
- [ ] Push the fixed candidate and run full make test CI.
- [ ] Document advisory duplicate findings and ticket acceptance evidence.

Independent review follow-up:
- [x] Preserve a retained observation across repeated edits in the same frame; fail-before reproduction and corrected controls pass.
- [x] Preserve existing strict hidden-CREATE property equality; share only the common type/endpoint identity projection.
- [x] Avoid node-schema constraint reads for relationship-only or create-only mutations.
- [x] Use caller-owned namespace scope prefixes rather than re-deriving separators.
- [x] Cover real peer publication after staging and before COMMIT through public queries, and assert the peer commit actually succeeded.
- [x] Reviewed-source focused race controls pass twice (storage 11.846s, Cypher 6.819s); source census regenerated with unchanged rules.

Profiled preparation cost:
- [x] Compare five isolated runs of the existing node preimage workload against unchanged release storage source, with CPU profiles and no concurrent local test runs.
- [x] Reuse the existing bulk cancellation poller after profiling per-record context checks; retain final error checks and a single-operation fast path.
- [x] Verify real in-flight fold cancellation and the 100 ms stop budget, plus canceled single-operation admission.
- [x] Bytes and allocation counts are unchanged in all six workloads. Distinct 100/1000-operation folds improve 26–28%; one distinct operation is unchanged. Repeated 1/100 folds cost about 9 ns/214 ns more (9–10%); 1000 repeated operations cost 3.5% more. This is a preparation microbenchmark, not an end-to-end commit latency claim.
