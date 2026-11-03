# Maintained JSON dependency

Fork: https://github.com/orneryd/go-json
Upstream base: `goccy/go-json` v0.11.2 (`3ce7333e2c03dcb8801817bf516c60545154a66c`).
Pinned fork commit: `3e94695582bd` (full immutable module revision in go.mod/go.sum).

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

The release numeric correction also preserves destination-precision float32
binding ([fork PR #2](https://github.com/orneryd/go-json/pull/2)). Decimal text
that rounds to MaxFloat32 is accepted, and midpoint decimals round directly at
32 bits. The fast float64 result is reused only when already exactly float32;
other float32 decimals use the existing shared parser at destination precision.
Full fork tests and focused float/number race tests pass. Matched public decode
benchmarks retain zero allocations for ordinary, fractional and midpoint
values; a valid maximum no longer allocates an error. Inexact float32 parsing
has a measured timing cost (about 103 ns versus 24–28 ns for 1.1 on the shared
M2 Max); this correction does not claim a timing improvement. No new state,
wrapper decoder, storage format or migration arm is added.

## Qualification and upgrades

The original lossless/cycle correction passed complete fork module tests and
focused race checks on Go1.26.9, including
upstream defaults, lossless map/string identities, long/streamed input, paths,
concurrent isolation, grammar rejection and map cycles. First-party codec/public
graph proof and the unchanged fixture are recorded in the PR1515 evidence
receipts. That original integrated uncached make test passed after dependency pinning
(121 packages); see the historical release checkpoint task receipt. It does not
qualify the current numeric correction: PR #1665 records its own current-head
gate, including focused float32 race checks on Go 1.27.3.
The original major refactor deferred benchmarking until publication. The current
numeric correction has separate [allocation and correctness-cost measurements](../docs/performance/pr1665-numeric-identity.md); they do not retroactively
qualify the historical refactor.

Review/reapply this small delta against every upstream upgrade and retain the
license/tests. Prefer removing a maintained delta when upstream provides the
same supported contract. The original lossless-decoding patch is retained in
`go-json-lossless-decoding.patch`; subsequent maintained corrections are pinned
by the full module revision. Review the complete upstream-to-pinned-fork Git
diff when upgrading. Do not patch module caches.

Upstream recursive-struct/map encoder issue: https://github.com/goccy/go-json/issues/559
