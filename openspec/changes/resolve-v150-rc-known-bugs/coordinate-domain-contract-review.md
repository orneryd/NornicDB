# Construction coordinate-reader contract review

The operator approved adding only hnswConstructionCoordinates.lookup argument0
to the vector guard's existing semantic-contract inventory. No backend receiver
exemption, inference algorithm, duplicate-checker rule or threshold changes.

The frozen baseline originates in lease.Lookup or canonical snapshot pairs;
HNSW installs the callback and calls it with internalToID entries. Catch-up keys
come from typed vectorEntryID. The exact private reader contract therefore names
entry keys, not graph node identities. Existing call-site analysis still checks
every supplied argument against that contract.

The new control fails before the contract addition: a node-ID argument to this
new reader is not rejected. After the addition, typed entry keys remain valid,
while direct and aliased node-ID calls are rejected. All existing domain guard
and bypass controls pass together (5.909s).

The unchanged lexical discovery identifies exactly one additional member at the
6b878f157 checkpoint: readSchemaTokenOrder. Its row documents graph label/type
ordering, static counter descriptions and unchanged public outputs. This is a
reviewed population record, not new lexical authority or a scanner exclusion.
The lexical population and credential-field policy tests pass together (1.889s).

CI43180429799 passed six actual test shards and lint; OTHER failed this inventory
mismatch and three assertions rooted in the missing new reader contract. That
failed head is not a passing make-test result. The follow-up pushed head must run
its own CI gate.
