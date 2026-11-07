# Label initial-codepoint normalization

- [x] Reproduce the original accented-initial corruption on release 83b8c5884.
- [x] Use the existing decoded-name codepoint owner instead of slicing one byte.
- [x] Preserve admitted lone UTF-16 name units as required by the existing lossless-name contract.
- [x] Cover ASCII, empty/whitespace, multibyte remainder, supplementary letters and combining marks.
- [x] Verify Go helper and registered APOC invocation agree.
- [x] Verify the registered handler through public Cypher on both parsers.
- [x] Preserve and explicitly test existing malformed Go-byte handling; no rejection API or stronger validation contract is introduced.
- [x] Profile and compare five isolated baseline/current runs: all five helper workloads improve latency and reduce bytes/allocations. ASCII 2->1 allocations; unchanged and admitted-unit strings allocate zero; changed Unicode requires one result allocation.
- [x] Final focused race/coverage: three race-enabled runs pass; Normalize has 100% statement coverage.
- [x] Reproduce the original corruption through both parsers on release 78ed35480; disposable worktree removed after retaining its failing log.
- [x] Pin simple non-expanding uppercase and supplementary/admitted-unit results through public query calls.
- [x] Correct the CI cascade-window fixture to retain its unapplied generation using the existing manual-drain owner; preserve every conflict/count/ownership assertion. Parent repeated race check passes (10 runs).
- [ ] Exact-head CI make test gate.

APOC text.capitalize is not an interchangeable owner: it uppercases the first
UTF-16 code unit and supports expanding string mappings, whereas label
normalization trims Unicode whitespace and uppercases the first decoded name
codepoint using the existing strings.ToUpper simple mapping. The codepoint
reader is shared with the database's admitted-name owner; no parallel decoder
or storage representation is introduced.
