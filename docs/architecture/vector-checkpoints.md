# Vector checkpoint integrity and recovery

Vector index files are derived data. The graph database remains authoritative.
A matching vector count, dimension, file size, or modification time does not prove
that a saved index belongs to the current database.

## Publication

The vector payload and its ID-to-ordinal metadata form one checkpoint. Saving
syncs the payload before publishing integrity-checked metadata. Compaction stages
a replacement payload and metadata before publishing a durable decision. Reopen
completes that decision before installing either file, so a process interruption
cannot pair compacted rows with the preceding ordinal map.

Publication, compaction, and population relocation use the same checkpoint owner.
An interrupted publication leaves its recovery files in place. Operational file
errors are returned; a failed writer is not reused for subsequent mutations.

Population artifacts are admitted together by their completion certificate. If
a process stops or cancellation arrives after the vector tuple publishes but
before the remaining population files publish, the previous certificate cannot
authorize that new tuple. Restart rebuilds the population from the database; it
does not promise to retain the previous derived vector file in this case.

## Source validation

Admission checks both payload/association integrity and the source certificate
of the completed database population. Unsupported custom reader views cannot
implicitly inherit source authority. They rebuild through their own readers.
Time-dependent selection cannot be certified as a static population.

The producer policy is part of the certificate. Certificates from the earlier
count-based producer are rejected: that producer could certify stale live
vectors after rejecting their saved checkpoint. The first build repopulates
those derived vectors; current certified populations keep the warm-load path.
An uncertified in-process store is not current merely because it has entries.

A complete, unchanged certified population can be reopened without hydrating all
nodes. Source validation still reads source metadata; zero population scans does
not mean constant-time startup. Source-validation costs are reported separately
from population construction.

Live search updates do not provide durable, complete acknowledgement of every
committed database change. A checkpoint modified after population construction
is therefore not granted a new certificate merely because saving succeeded.
When the source or saved tuple no longer matches its certificate, restart rebuilds
from the database. The rebuilt, unchanged checkpoint is reusable on the next
restart. This deliberately avoids a durable acknowledgement ledger and its
normal-write bookkeeping costs. It trades restart work for steady-state cost
without trusting potentially incomplete results.

## Storage mode and recovery

Explicit disk mode returns operational create/load failures rather than silently
switching to RAM. Auto mode reports an allowed fallback through the existing
search storage status, including its reason. Corrupt or stale derived data is
reconstructed through the population owner; unreadable storage is distinguished
from a reconstructible checkpoint mismatch.

This protocol changes derived search files, not authoritative entity payloads or
existing Badger keys. Old derived files without the required integrity/source
proof are rebuilt from the database.

## Validation boundaries

Process-crash tests cover reordered and empty compactions before publication,
after each protocol boundary, and after completion. They assert exact vectors
and IDs, absence of deleted entries, and successful subsequent mutation/save.
These tests exercise abrupt process exit; they do not emulate hardware power
loss. Durability additionally relies on the filesystem's documented sync and
atomic-rename guarantees.
