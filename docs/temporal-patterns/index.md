# Temporal patterns

Three temporal decisions are separate in NornicDB: the epochs you record as graph
data, the interval rules you declare as constraints, and the history the database
retains. Each decision has its own guide and its own cost, and choosing one does
not require the other two.

## Learning path

1. **Record epochs explicitly.** Put each validity window on the node or
   relationship it describes, and keep the closed window when the fact changes.
   The graph then answers which window applied without separate current-pointer
   bookkeeping. Start with
   [Canonical graph ledger patterns](../user-guides/canonical-graph-ledger.md).
2. **Declare the interval rules you enforce.** A no-overlap constraint compares
   user-selected interval values within a declared grouping key. It constrains
   writes against current values; it does not record history. Continue with
   [Managing constraints](../user-guides/managing-constraints.md).
3. **Read retained history where it exists.** MVCC version chains answer
   historical reads at a commit-time cutoff, within current entity lifetimes
   and retained history. Deleted entities are excluded. See
   [Historical reads and MVCC retention](../user-guides/historical-reads-mvcc-retention.md).
4. **Budget the cost of each layer.** Interval validation and historical reads
   take different paths and scale with different populations. See
   [Historical reads and no-overlap constraint costs](../performance/temporal-read-and-constraint-costs.md).

## Open epoch bounds

A null start means all earlier time; a null end means all later time. Both null
is invalid. Two finite bounds must increase. Epochs are half-open, so one owner
can hand over at the exact instant the next owner begins. Exclusivity depends on
the full grouping tuple, not the identity of the owner.

## Keep the layers distinct

- **Explicit epoch records** answer which window applies to a fact you wrote.
  Retaining older epochs is an application decision, not a database default.
- **Independent constraints** answer whether two windows may coexist for one
  grouping key. The constraint indexes the interval values you declare, not
  commit history.
- **Retained as-of history** answers what had been committed at a point in time.
  Pruning removes old versions; the current head and current search results stay
  current.

## Compatibility

Constraint and read forms written for 1.4.x and earlier are deprecated from 1.5.0
and remain supported. The tuple spelling
`(key, valid_from, valid_to) IS TEMPORAL NO OVERLAP` and the positional
`db.temporal.asOf` call remain accepted; no removal version is set. The tuple
declaration maps to the same constraint as `temporal.epoch`. The old
positional read remains an interval lookup; the new `at`/query form reads retained
database versions. Existing graphs and indexes need no storage migration.

## Executable examples

The guides include a downloadable ownership script and examples run through the
executor, authenticated HTTP and Bolt. See the [example verification map](../development/temporal-example-verification.md)
for cases, edge conditions and the commands used to check them.
