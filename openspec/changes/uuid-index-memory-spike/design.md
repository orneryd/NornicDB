# Investigation design

The canonical owner is `pkg/storage/id_dictionary.go`. Its sharded pair operations
publish string-to-numeric and numeric-to-string entries and retain pending
transaction state. The experiment models committed pairs only; no candidate
implements the transactional owner or changes the disk dictionary.

Separate `property_key_dictionary.go` (distinct property names) from
`SchemaManager`/`CompositeIndex` in `schema.go` (indexed values and postings).
Measure each actual owner, then evaluate alternative representations outside the
production module. No Cypher consumers are patched and no additional query path
is created. Source, commands, results and limitations belong in the experiment
report; any implementation decision must extend this owner with the same callers.

Disk invariants: retain existing string keys, forward/reverse values, counters,
freelist and numeric assignments. Reconstructing a different in-memory view of
those same bytes is not a storage migration. This does not prove the unimplemented
view preserves commit/rollback behavior; those checks are a later required gate.
