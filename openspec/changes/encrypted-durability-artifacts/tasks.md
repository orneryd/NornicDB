# Encrypted durability artifacts

- [x] Reproduce plaintext WAL and automatic recovery snapshots from encrypted `nornicdb.Open` on the prior release and current release branch.
- [x] Write authenticated, versioned WAL frames for primary, isolated, and replication journals; reject missing or wrong keys without destructive repair.
- [x] Stream automatic snapshots into authenticated encrypted files and reject damaged or keyless reads.
- [x] Convert retained WAL segments and snapshots before encrypted stores accept traffic; resume interrupted conversion safely.
- [x] Assert live write, restart replay, retained history, snapshot, upgrade, wrong-key, tamper, and plaintext absence through public paths.
- [ ] Run focused and package checks, then `make verify ALL=1 BASE=<frozen-release-base>` on the integrated head and resolve every failure.
- [ ] Record complete verification evidence and open the release PR; keep the advisory private until release publication.
