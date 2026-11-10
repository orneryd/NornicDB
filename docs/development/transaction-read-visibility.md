# Transaction data snapshots and live visibility

An explicit transaction pins the committed data it can read. Peer updates,
deletes and node recreation do not replace that data snapshot. Its own staged
writes and deletions take precedence over the committed image.

Manual suppression is a live read policy. Suppressing a node in another
session must hide that node from subsequent visible reads in an already-open
transaction, including label scans, cached label populations and point reads.
Removing suppression makes the pinned image visible again; it does not advance
the transaction to a newer image. A reveal scope uses the existing reveal
policy. Stored-entity reads intentionally bypass visibility for physical
maintenance and accounting.

A cached population must therefore preserve its physical membership and apply
the visibility policy when delivering each candidate. A cached, already-filtered
answer cannot recover members that later become visible. Pending transaction
images require the same delivery decision as committed images.

The regression checks exercise public Cypher under both parsers and the native
transaction label/equality readers. They distinguish data-version isolation from
live visibility, and include visibility changes while a label visitor consumes
its population. They do not establish a new transaction isolation level or a
new storage format.
