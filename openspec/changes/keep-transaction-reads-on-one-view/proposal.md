# Keep relationship reads and counts on one transaction view

An ordinary durable transaction must read directional adjacency, between-edge
lookups, projected matching and count reductions at its pinned view. Its own
pending writes/deletes overlay that same view. Write-owned latest reads and
write-behind retain their separate existing authority rules.

A count read carries an opaque snapshot capability through the actual reader
chain. The native counter kernel consumes it under transaction synchronization
and composes the current staged delta there. Custom errors/declines remain
final. Detached, expired and changed-cut handles cannot certify a read.

No entity population is materialized solely because a peer committed. Counters
without persisted snapshot-addressable state are not covered by this fast path.
No storage-format change is made.
