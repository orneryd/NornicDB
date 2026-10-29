# Pre-identity upgrade fixtures

These ZIPs contain actual Badger data files and 20 acknowledged, unapplied
buffered WAL updates written by unchanged NornicDB bases. They were generated
on TrueNAS with the base storage tests and `-race`. No new binding fields were
stripped from a current-format record.

The base generator creates `upgrade:a` with two persisted names (`value` and
`stamp`), starts an empty WAL, acknowledges 20 implicit updates, then uses the
base's `crashClose` test helper to stop without draining the buffer. All 20
records have empty historical `PropKeys` and no complete table or dictionary ID.
The temporal value checks that the upgrade preserves the typed physical bytes.
For v1.4.4 these fixtures verify that an unauthorized open preserves the old
files, while an authorized V4 upgrade converts their WAL framing, recovers all
20 updates, retains the original WAL, and reopens without repeating the writes.

| Fixture | Writer base | ZIP SHA-256 |
| --- | --- | --- |
| release.zip | ba8d6d13b48452ec4e38d826869ad63a64063848 | 0cfd8aef8ab787f239a5621bdfff5f115e196a366a269f2e9567ba4ec4481991 |
| develop.zip | 16923aef778025fab3490f6a57549763b536f3d5 | 12c5ed07619ab47eec920313cdcf620ba1fd61790fe2380964e60ff3b8691381 |

Generation receipts: `i20261023100104-65480-9c68` and
`i20261023100135-59016-6f1c`. The ANTLR generation controls also pass:
`i20261023100114-65480-1df1` and `i20261023100144-59016-76f3`.
The implementation handoff retains the generator and complete logs under
`sig/tnrun-operator-assets/wal-bindings-base-fixture/` and
`sig/wal-bindings/r73-base-fixture-collector.log`.
