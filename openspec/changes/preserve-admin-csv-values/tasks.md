# Preserve admin CSV property values

- [x] Public export/discover/import fail-first reproduces invented empty properties and float64 narrowing on release7f565720f.
- [x] Use standard quoted-empty versus bare-empty CSV semantics, not the rejected nullToken extension.
- [x] Keep encoding/csv as the sole grammar, using FieldPos/InputOffset for quoted provenance.
- [x] Preserve float64 lists through double[]; preserve float32 vector and named-embedding contracts.
- [x] Round-trip/CRLF/gzip/multifile/multiline/quotes/backslash/ignore-empty controls pass.
- [x] Parent reproduces and fixes retained raw-buffer capacity after a large record.
- [x] Parent package checks pass: adminimport2.439s, admin CLI2.061s.
- [x] Operator approved one exact CSV-owner inventory entry; unchanged guard fails only fieldQuoted before the entry, passes3.371s afterward. No analyzer logic changes.
- [x] Stream encoded fields through encoding/csv; fail-first short-write/retry controls corrected; adminimport and admin CLI pass (2.212s/1.082s), race passes (3.713s).
- [x] Approved scanner inventory and quoting controls pass on integrated source (4.177s).
- [ ] Push candidate and run current-head CI.

No Badger keys, stored payloads, migration version or public Go API changes.
Old ambiguous CSV exports cannot recover property absence that was not recorded.
Graph retrieval remains unavailable (HTTP-to-HTTPS400); manual owner review is
required, and no clean graph duplicate report is claimed.

- [x] Consolidate node/relationship row construction; isolated ABBA benchmark lowers bytes29.1%/32.1% and allocations10.0%; timing ranges overlap.
- [x] Node IDs use the existing canonical ID reader; truncated rows now report their source column before engine admission.
- [x] Reuse one testfixture.ErrorWriter across IO/transport fixtures; parent fail-first concurrency control caught non-atomic call counting, now corrected with atomic counts.
- [x] Full adminimport/admin CLI race checks and selected fixture/storage/convert/replication/heimdall/server consumer races pass.
- [ ] Push the convergence candidate and complete its new-head CI.
