# v1.5.3 release verification follow-up

Release verification on head `851326b1e` found a repeatable Bolt auto-commit
`db.awaitIndexes()` timeout after creating a four-dimensional vector index.
The fresh index uses storage while the search service has not completed its
lazy build; the procedure polls the service's POPULATING state without
requesting that build. The correction should use the existing lazy warming
owner when the current index has no population record, preserving the
procedure timeout and the startup readiness promise.

The current run also exposed a storage allocation inventory drift from the
encrypted durability wrappers, a stale generated stored-entity census from
the integrated storage changes, and repeated coverage artifact upload errors
on the trusted runner after coverage itself passed. The upload should retry
transient transport failures while still failing the job if every attempt fails.
Review each exact-head failure,
correct product or census ownership at its source, and rerun affected checks
before qualifying the release. Do not change the differential corpus or weaken
the release gates to obtain a pass.
