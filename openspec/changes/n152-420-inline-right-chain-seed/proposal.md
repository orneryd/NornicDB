# N152-420: seed fixed chains from an indexed right endpoint

## Outcome

For a fixed two-hop MATCH with an inline equality key at the right endpoint,
the default Cypher executor can begin at the indexed right node and walk the
chain backward. This preserves the original result and avoids reading every
left-side chain. The same shared executor path serves both parser modes.

## Scope and owner

Reuse the existing `chooseTraversalEndSeeds` and indexed node candidate plan.
Only a plan with zero or one end candidate selects the reverse walk for a fixed
chain. Wider postings retain the existing start-side path. The index read and
normal traversal retain their existing snapshot and authorization checks.

This does not change ANTLR grammar or add an ANTLR-specific optimizer. It does
not attempt cost estimates for arbitrary chain lengths or multiple end seeds.

## Evidence and release gate

The borrowed C25 public probe on the v1.5.3 release head read 9,000 entities
for 1,000 chain pairs and 72,000 for 8,000 pairs; the equivalent left-keyed
control read seven. An added at-most-32-hit assertion failed before the fix.
After the correction, the right-keyed form reads seven at both sizes. The
focused regression also checks missing keys, wide postings, one-hop behavior,
an explicit transaction, and both parser modes. Full release verification is
deferred to the combined release gate.
