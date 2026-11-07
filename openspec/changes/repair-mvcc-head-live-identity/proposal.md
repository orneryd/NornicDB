# Repair the MVCC head rebuild for live recreated entities

## Problem

On release base `83b8c5884`, a node deleted and then recreated with the same
string ID keeps an archived MVCC tombstone marker as the newest version record;
the recreation writes only the current body and its non-tombstoned head.
`BadgerEngine.RebuildMVCCHeads` reconstructs heads from the version keyspace
first, so the recreated node's head becomes `Tombstoned: true`, and the
bootstrap pass that follows skips it because *any* head already exists.

That head is wrong for a stored body: deletion removes the body, so a body means
the entity is live. MVCC pruning trusts the head, treats the live node as a
deleted entity, and drops its MVCC head, prune floor, ID dictionary entry and
numeric ID reuse record. The ID dictionary is then missing an entry that
compact relationship bodies (which store endpoint numbers) address, so reads
of the recreated node's relationships fail with `compact edge: start numID ...
not in dict` and public Cypher traversals return no rows. Rebuilding heads at
startup (`prepareDerivedIndexes` → `RebuildMVCCHeads`) runs whenever the
clean-shutdown marker is absent, so an unclean restart after a delete/recreate
reaches this state during ordinary operation.

## Proposed outcome

The head bootstrap that seeds heads from the stored current state must treat a
tombstoned head beside a live body as stale and rewrite it from that body. The
same decision must serve the batched collector and the single-transaction
bootstrap so the two cannot diverge. Pruning keeps reclaiming genuinely deleted
entities, their MVCC history and their numeric IDs; no pruning, dictionary or
read path is changed, and no reclamation is disabled.

## Verification

Storage-level reproduction (delete, recreate, edge, rebuild, prune), the
public Cypher traversal and label reads over the same lifecycle, an on-disk
reopen with a repeated rebuild/prune, the recreated relationship variant, and
`CheckIDDictionary` report no missing identities. Focused regression controls
cover the existing rebuild, bootstrap, prune, freelist and dictionary suites.
Full `make verify ALL=1` remains the shared release gate on the frozen
integrated head.

Code-graph retrieval is unavailable in this environment (HTTP 400 from the
nornicdbcode/docs endpoints); the causal chain above rests on the current
checked-out source and executed tests.
