# Maintained JSON dependency

Fork: https://github.com/orneryd/go-json
Upstream base: `goccy/go-json` v0.11.2 (`3ce7333e2c03dcb8801817bf516c60545154a66c`).
Pinned fork commit: `ec39caf` (full immutable module revision in go.mod/go.sum).

The maintainer authorized choosing the cleanest architecture for exact UTF16
identities and correct JSON behavior. NornicDB uses a direct fork module
requirement so applications embedding public Go packages inherit the dependency
without additional replace rules. The original upstream source, license and
tests remain in the fork; this repository does not vendor a second JSON binder.

## Maintenance delta

- DecodePreserveInvalidUTF8 is off by default and belongs to a decode request or
  stream. Cached decoders hold no mutable mode. It preserves raw string bytes
  while retaining upstream grammar and escape handling. NornicDB opts in only
  after validating/projecting original JSON through internal/jsontext. Ordered
  token reads reuse that binding owner. Path-selected values receive their
  decode options.
- Cyclic map encoding records the underlying map identity after the existing
  depth threshold, rather than a newly allocated interface slot. The actual WAL
  error-path regression timed out before this correction; direct/indirect/mixed
  cycles now return the existing UnsupportedValueError. Shared acyclic maps
  remain accepted. Ordinary shallow encoding takes no new cycle-check path.

No storage keys, payload format, additional migration arm, reflective binder or
alternate lexer is introduced. The existing user-record reader owns its stored
metadata contract: successful string entries apply in source order, stopping
before a rejected entry. Its original344-case fixture remains unchanged.

## Qualification and upgrades

Complete fork module tests and focused race checks pass on Go1.26.9, including
upstream defaults, lossless map/string identities, long/streamed input, paths,
concurrent isolation, grammar rejection and map cycles. First-party codec/public
graph proof and the unchanged fixture are recorded in the PR1515 evidence
receipts. The integrated uncached make test passed after dependency pinning (121 packages); see the release checkpoint task receipt.
Benchmarking is deferred until this major refactor is pushed; no comparative
performance pass is claimed.

Review/reapply this small delta against every upstream upgrade and retain the
license/tests. Prefer removing a maintained delta when upstream provides the
same supported contract. The exact upstream-to-fork diff is kept in
`go-json-lossless-decoding.patch`. Do not patch module caches.

Upstream recursive-struct/map encoder issue: https://github.com/goccy/go-json/issues/559
