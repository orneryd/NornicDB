# N152-68 scoped non-regex correction

Base: a5b7980e89171764091a6d47d0b8a8ee3ca19c88. Fresh original RED at fec41082d is preserved; fresh unpublished branch fast-forwarded before production to reuse merged canonical LowerCaseText.

## Outcome and scope

Correct registered APOC text normalization, camel conversions, URL/Base64 encoding and checked decode failures, scalar-substring UTF16 offsets, empty capitalize failure and null decapitalize. Preserve exported convenience signatures. Utility Base64 peers reuse text owners; utility URL encoder retains its advertised %20 behavior, decoder retains observed plus-to-space. No universal Java regex, Unicode-version or UTF16 substring-matching claim.

## Owners and duplicate decisions

- Existing fn.LowerCaseText remains the general lowercase owner; no second case mapper. Camel tokens reuse appendInitialCase with ToTitle, upperCamel reuses Capitalize. Existing SnakeCase/word-initial owners remain outside this correction.
- Clean applies pinned APOC transliteration, NFD, alphanumeric filter and canonical lowercase. CompareCleaned directly compares the same owner. Five fresh pinned compare rows cover shared behavior.
- encoding.go owns text encoding and checked decoding. Utility Base64 delegates; URL decoding shares the same checked owner. Utility percent encoding remains distinct because its advertised URI escaping differs from Java form safe characters and space handling.
- Existing x/text UTF8 decoder handles maximal invalid subparts; only the measured surrogate-prefix classification difference is adapted. 25 actual pinned malformed byte inputs: plain decoder5 differences, adapter0. URL decoding applies conversion only to percent-byte runs; raw admitted UTF16 units survive.
- IndexOf uses existing strings.Index search and ReadCodepoint for UTF16 offset projection. Scalar needles are in the approved65 corpus. Searching a lone surrogate needle within a supplementary scalar is not implemented or claimed; IndexesOf is out of scope.
- Existing registry callable error handling carries checked outputs; add only the two needed signature policies. The checked-string shape is a named registry.CheckedTextFunction so unrelated XML root-call admission stays unchanged. Catalog extraction asserts exactly five marked checked registrations and only one nullable-interface signature (Decapitalize). No new invoker or parser-specific dispatch.
- DeepSeek0c982aecd5fb409e938638a0304c40b3 completed188.037s, source-only with graphDNS unavailable. Its whitespace-clean and supplementary-decapitalize claims were contradicted by pinned source and rejected. Out-of-scope hash/indexes changes were rejected. Staged checker remains required.

## Evidence and remaining work

Original full corpus and test sources remain untouched in the SHA-verified local task archive (original-source-hashes.json); original RED:38 text failures,5 util failures,90 public failures across both parsers. Scoped live tests exclude the17 regex rows below explicitly. One utility plus ambiguity remains unasserted as before. Current owner normal passes apoc/text, apoc/util, apoc/registry including91 fresh pinned encoding/compare rows. Public/neighbor/race/vet/verification and matched costs pending.

Pinned Java reference: https://raw.githubusercontent.com/neo4j/apoc/5.26.30/core/src/main/java/apoc/text/Strings.java . Fresh reference used exact already-cached image/JAR identified in original corpus,1CPU512MiB task-owned --rm container with no DB/ports; cleanup confirmed. Two transport-start failures preserved; neither started a reference container.

## Remaining regex row IDs

- split_recorded
- split_empty_pattern
- split_no_match
- split_multiple_matches
- split_invalid_pattern
- replace_recorded
- replace_multiple_literals
- replace_no_match
- replace_capture_group
- replace_invalid_pattern
- parent_extra_split_empty
- parent_extra_split_nomatch
- parent_extra_split_repeated_trailing
- parent_extra_replace_groups
- parent_extra_replace_empty
- parent_extra_replace_zero_width
- parent_extra_split_invalid

Existing Split/Replace are literal owners; RegexGroups uses Go RE2. The original patterns are RE2-expressible, but Java replacement expansion, zero-width UTF16 stepping, and adjacent empty matches differ. Supplemental a* returns ["","","b"] / "XXbX" in Java; Go FindAll suppresses adjacent empty matches. Java lookaround syntax is unsupported by RE2. No new dependency/runtime or partial matcher was introduced. Parent's anchored-prefix empty-match probe is a follow-up candidate, not accepted implementation or general parity evidence.

## Caller policy and reference gate

Fresh root/Cypher controls retain root CompareCleaned unsupported admission while checking its typed/Cypher values, existing typed arity/type rejection, and observed utility plus decoding. First broad public run found the expected four stale NOR207 casing rows plus unrelated Dijkstra defaultWeight message mismatch. Four oracle expectations were updated with old source/log archived; Dijkstra reproduces identically on clean a5b7980e8 and remains a separate issue. Parent confirmed clean-base observability structural-inventory failure too; no release-wide verification pass claimed.
