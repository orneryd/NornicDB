# Preserve embedded query journal publication

## Problem

Report section 3 reproduces acknowledged embedded Cypher mutations that persist
in Badger but are absent from journal-only rebuild. The release baseline
ce22592d28cf5ca2e651491e174367df8781dcb0 reproduces the durable failure; write-behind
is an already-correct control for the generated workload.

## Correction

Expose the existing transaction-commit capability on WALEngine. Bind the native
transaction to the same beginNativeMutation/final publication owner used by
direct CRUD. Validate native ownership before binding. Cypher's existing
commitStorageTransaction discovers the capability before unwrapping the journal.
Do not add an encoder, replay route, compatibility wrapper or storage format.

## Scope and proof

Public embedded and scoped explicit create/update/delete/relationship writes
must replay; rolled-back writes must not. Refuse a foreign transaction, canceled
caller or closed journal before publication. Existing managed/replicated commit
owners remain authoritative. Full release qualification remains in CI.
