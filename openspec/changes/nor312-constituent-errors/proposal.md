# NOR-312: propagate composite participant access failures

Direct composite operations with error-returning APIs silently skipped failed getConstituent calls. Qualified entity selectors also dropped those errors and reported missing data. Keep getConstituent as the access owner and propagate failures through its existing error-returning consumers and canonical fan-out helpers. Qualified routing skips only unrelated failed sources; it reports failure for the source owning the qualified database. Preserve the original error identity, namespaces, cancellation, and healthy empty results.

The exact 22 failing direct-storage assertions are retained. Public Cypher USE/participant failure and cleanup tests were already passing on the base and remain controls, not new production-path reproductions. The static inventory is a classification of 78 storage access sites, not 78 reproduced defects.

Eleven APIs with no error channel retain their audited partial behavior; their signatures, hidden state and approved product contract are not changed here. This is an explicit remaining issue scope, so do not close the umbrella ticket on this PR alone. No global error cache, implicit query fan-out or invented error owner.
