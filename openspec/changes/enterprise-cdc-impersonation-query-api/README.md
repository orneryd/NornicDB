# Enterprise CDC, impersonation and Query API v2

Draft implementation plan for #936 and #937, including the requested Query API
v2 expansion. Target: Neo4j Enterprise 5.26.30. No implementation is included.

- [Proposal and scope](proposal.md)
- [Code-mapped architecture, replacement decisions and migration](design.md)
- [Dependency-ordered implementation checklist](tasks.md)
- [Source findings, version corrections and differential fixture matrix](reference-contract.md)
- Behavioral contracts:
  [impersonation](specs/user-impersonation/spec.md),
  [transaction attribution](specs/transaction-attribution/spec.md),
  [CDC](specs/change-data-capture/spec.md),
  [Query API v2](specs/query-api-v2/spec.md).

The first implementation gate is live Enterprise contract capture; exact
unverified edge cases are identified in the reference matrix. #935's canonical
privilege/effective-graph implementation is a hard acceptance dependency.
Conflicting existing behavior is replaced, not retained behind legacy fallbacks.
