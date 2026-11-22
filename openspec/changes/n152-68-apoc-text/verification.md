# N152-68 verification ledger (in progress)

Branch fix/n152-68-apoc-text-compatibility; source base a5b7980e89171764091a6d47d0b8a8ee3ca19c88. Not committed/published yet.

- Original exact owner RED at fec41082d: text38 failures0.230s, util5 failures0.230s; oracle guards pass.
- Original public RED at fec41082d:90 case failures over two parsers,1.490s. One utility ambiguity/parser retained unasserted.
- Fresh pinned owner reference: original59 confirmed;40 supplement controls;63 decoder/CompareCleaned controls plus8 encoder controls. Owned1CPU512MiB containers removed; first two transport-start failures preserved and excluded.
- Decoder prototype on25 malformed byte groups: x/text alone5 mismatches; scoped surrogate-prefix correction0. Production91-row matrix passed owner normal0.414s. Encoding functions100% measured statements; all package42.6%, not package-wide90% claim.
- Initial scoped owner full normal text0.450s/util0.437s/registry0.904s.
- Broad public normal23.583s: nonregex cases pass; four historical casing rows ×two modes fail as expected, plus Dijkstra defaultWeight message assertion. Same Dijkstra failure reproduced in clean a5b7980e8 control1.030s; no production scope change.
- First final scoped normal: apoc0.441s/text0.408s/util0.385s/Cypher1.401s pass; registry selection had no tests (earlier full package pass applies).
- Closed policy review found three XML root-call signatures newly admitted by unnamed shape. Dedicated XML admission test RED0.410s preserved; named checked-string registration corrects this. Final policy normal/race/vet pending.
- `make verify-list BASE=origin/release/v1.5.2` exits0: localization/UI/native-libs/build/tests/allocations/conformance/differential selected; docs path skips. This is listing only. Verifier execution pending; no full-release qualification.
- Matched nonregex baseline binary captured before production. Timing/profile comparison pending quiet window. Placeholder baseline semantics are incorrect; no cost-free/no-regression claim.
- Mandatory staged checker pending; worker Graph RAG unavailable (DNS), independent source owner review documented in proposal.

Raw exact logs and research archive: ~/.codex/artifacts/n152-68-20261121. Active logs /tmp/n152-68-*.log. Original65 corpus and original3 test sources remain in the SHA-verified local archive; unchanged reference corpus stays as live test input. Live scope excludes17 exact regex row IDs enumerated in proposal;40 direct text rows,47 asserted public rows/parser plus1 existing ambiguous row/parser. Whole ticket remains open.

## Final scoped gate results

Named-policy final normal passes apoc0.555s/text0.405s/util0.629s/Cypher1.472s. Scoped race passes apoc1.635s/text1.507s/util1.485s/Cypher3.564s. Root caller positives added afterward passed final race; runtime source unchanged. Registry selectors contain no matching tests in these scoped runs; earlier full registry normal0.904s passed. Scoped five-package vet exits0.91-row owner coverage run passes0.414s; all encoding functions100%, overall apoc/text42.6%.

Staged checker executed and failed ingestion at localhost:7474 (health connection refused), so no indexed verdict. Current CONTRIBUTING51/117/119 treats local hooks as optional and indexed release analysis as advisory; existing hook configuration is untouched. Independent source duplicate review remains recorded. Paired costs completed and disclosed in costs.md; performance gate remains open by parent decision, not waived.

Broad make verify planned on committed candidate with FAIL_FAST=1; no full-suite pass claimed. Known independent clean-base failures link to https://github.com/orneryd/NornicDB/pull/1937 (observability inventory) and https://github.com/orneryd/NornicDB/pull/1938 (Dijkstra assertion).
