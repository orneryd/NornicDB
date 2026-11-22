# N152-128 verification ledger

Final result: approved correction passes expanded focused normal (2.675s), race
(8.517s), package vet and scoped canonical localization verification. Original
RED is preserved. Full release verification and graph duplicate analysis remain
incomplete; no performance qualification is claimed. All direct Go jobs used
GOMAXPROCS=2, GOMEMLIMIT=1500MiB and -p=1 with noui,nolocalllm tags.

Base and origin/release/v1.5.2: f77ec7706de099588ac232b32b6908860511d3da.
Exact reproduced test: SHA256 b843d45c956edc54d036f11bebe9afbb9af77074c3093ff691e2f7fc45e63805.
The ticket and all five embedded source/script/log blocks are preserved with
hashes in manifest.json; archived Go sources live under testdata and do not
become standalone test packages.

## Original RED

`GOMAXPROCS=2 GOMEMLIMIT=1500MiB go test -p=1 -tags=noui,nolocalllm ./pkg/storage -run '^TestNOR330EmbeddingEncodeKeepsCallerEmbedMetaOwned$' -count=1 -v -timeout=5m`

Exit 1, package 1.392s. Direct CreateNode, BulkCreateNodes and native transaction
CreateNode+Commit fail caller metadata assertions. Namespaced create/update and
inline controls pass. Every oversized route first passes stored count 60,
60 hydrated chunks and first/last vector-bit comparisons. Log current-red.log.txt.
The original test source remains unchanged in the package and in testdata.

## Current correction and oracle contract

Parent approved the V2-only maps.Clone correction after this RED. Parent also
approved replacing the old measured-encoding test's caller-mutation parity
assertion with an original metadata snapshot comparison (including nil/error
cases). The reference encoder and all encoded-byte, size, flag, decoded-node
and error comparisons remain unchanged. This corrects an assertion of the bug;
it does not relax the encoding oracle. Existing V1 fixture writer controls and
actual V1→V2 migration tests will run with the V2 correction.

Focused normal checks passed (2.686s):
`go test -p=1 -tags=noui,nolocalllm ./pkg/storage -run 'TestNOR330EmbeddingEncodeKeepsCallerEmbedMetaOwned|TestEncodeNode|TestEmbeddingChunk|TestMigrateV1ToV2|TestTransactionNodeUpdateChunkPersistence|TestStoredImageEmbedding|TestPropertyUpdate.*Sidecar' -count=1 -v -timeout=5m`.
The initial `TestStoredImageEmbedding` alternative matches no test; the actual
stored-image consumer matrix was separately run with its correct name below.
This does not imply broader test coverage than the matching names in the log.

Stored-image consumer guard passed (1.913s):
`go test -p=1 -tags=noui,nolocalllm ./pkg/storage -run '^TestUpdateKeepingVectorsKeepsStoredEmbeddingState$/writeBuffer=false/.*/plain_commit$' -count=1 -v -timeout=5m`.
All ten synchronous plain-commit shapes retain stored state after update/reopen.
Other buffered/replay/snapshot permutations of that test were not selected.

At that intermediate checkpoint, race, vet and scoped checks were pending; final results follow below. No timing or performance claim is made.
The staged duplicate checker exited 1 with analysis/ingestion readiness failure
at localhost:7474/nornicdbcode; manual source review is in duplicate-review.md.
Full verification, platform variants, broad wrapper qualification and nested
metadata/vector ownership are not claimed.

Parent accepted correctness-only validation without a new benchmark. A missing
chunk_count on externalization now requires a map clone; inline/preexisting-count
paths keep their existing allocation behavior by source inspection. This is not
measured allocation or performance qualification, which remains deferred.


Initial focused race passed (8.381s) before the two requested update-route controls
were appended. Log initial-race.log.txt. Production is unchanged; the expanded
normal/race checks will supersede this narrower test population. The original
6114-byte fixture remains an exact prefix of the package test file and is also
preserved separately with its original hash.


Expanded final normal passed (2.675s), adding the requested
TestEmbeddingEncodeUpdateKeepsCallerEmbedMetaOwned direct/native update controls:
`go test -p=1 -tags=noui,nolocalllm ./pkg/storage -run 'TestNOR330EmbeddingEncodeKeepsCallerEmbedMetaOwned|TestEmbeddingEncodeUpdateKeepsCallerEmbedMetaOwned|TestEncodeNode|TestEmbeddingChunk|TestMigrateV1ToV2|TestTransactionNodeUpdateChunkPersistence|TestPropertyUpdateKeepsUnchangedSidecarRecords' -count=1 -v -timeout=5m`.
The new controls report stored chunk_count=60 and 60 hydrated chunks and preserve
caller metadata. Final normal log is copied beside this ledger. Expanded race
uses the same selector. The staged checker was invoked again after these tests
were staged; it still fails analysis/ingestion readiness, as recorded in
final-duplicate-check.log.txt. Worker docs are staged unchanged as historical
proposals, with parent decisions in proposal.md and duplicate-review.md.


## Final current-source checks

Expanded final race passed 8.517s using the exact final normal selector above,
with `-race -count=1 -timeout=5m`. See final-race.log.txt. Package
`go vet -p=1 -tags=noui,nolocalllm ./pkg/storage` exited 0 with no output.

`GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOFLAGS='-p=1 -tags=noui,nolocalllm' make verify BASE=origin/release/v1.5.2 ONLY=localization FETCH=0`
passed: localization 3.655s, catalog 0.285s, inventory 0.252s, generated manifest
and catalog checks. UI, llama-libs, build, full tests, allocations, conformance,
differential and docs were explicitly unselected. This is not a full verifier
pass. The verification-plan command also completed successfully; its selected
steps are recorded separately and do not constitute passes.

Go checklist review found no actionable issue in the approved correction:
only absent-count insertion detaches the shared map, nil/count-present/inline
and encoding-error boundaries retain their contracts, and all production callers
reuse the same encoder. Adjacent V1 is a test-fixture writer, not a supported
production path; existing V1 and actual migration controls passed. No nested
metadata mutation/retention or vector/timestamp ownership changes were added.

All production/test source is unchanged after the final normal/race runs.
Subsequent edits only finalize evidence. Exact ticket/fixture hashes and the
unchanged original test prefix were verified again. Production/test/Markdown/
JSON whitespace checks pass; preserved raw failure logs retain original testify
whitespace. No child commit, push, PR or external tracker mutation. No active
N128 Go processes or other child consumers remain; parent owns integration and
later task workspace cleanup.
