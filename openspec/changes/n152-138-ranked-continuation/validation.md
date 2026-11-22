# N152-138 public validation

Base: `f77ec7706de099588ac232b32b6908860511d3da` plus the staged callback fix and public regression. These are focused working-tree results, not full release qualification.

## Fail first

`GOMAXPROCS=2 GOMEMLIMIT=1536MiB go test -p 1 -tags noui,nolocalllm ./pkg/cypher -run '^TestRetrieveContinuationOwnsRerankWindows$' -count=1`

FAIL 1.497s before the production edit. Both parsers returned 4 instead of 12 for embedded and multi-chunk requests. Caller ceiling delivered 3 but submitted 4 provider candidates. Supplied-embedding and plain-retrieval controls passed. Log: `/tmp/n152-138-public-red.log`.

## Integrated focused pass

`GOMAXPROCS=2 GOMEMLIMIT=1536MiB go test -p 1 -tags noui,nolocalllm ./pkg/cypher -run '^(TestRetrieveContinuationOwnsRerankWindows|TestCallDbRetrieve.*|TestRerankTruthfulness.*|TestRetrieveRowsReportQueryChunks)$' -count=1`

PASS 1.448s with the callback condition fixed. Log: `/tmp/n152-138-public-green.log`. The new regression covers both parsers. Existing selected tests additionally exercise one-shot configured reranking, provider filtering, and chunk metadata. Linker emitted its duplicate `-lobjc` warning; no test failures.

## Other checks and remaining gates

- `git diff --cached --check`: PASS.
- `make verify-list BASE=origin/release/v1.5.2`: exit 0. Selected localization, UI, llama libraries, build, tests, allocations, conformance and differential; docs path skipped. The verifier's own fixture checks emitted missing temporary CI-file warnings but passed. Log: `/tmp/n152-138-verify-list.log`.
- Required staged duplicate checker: FAILED before candidate analysis because graph analysis/ingestion at localhost:7474 was unavailable. Manual comparison is recorded in proposal; it is not a clean checker result. Log: `/tmp/n152-138-duplicates.log`.
- Focused race passed as recorded below. Dedicated vet and scoped localization also passed as recorded below. Full `make verify` remains pending. Parent coordinates further jobs, exact private-fixture checks and release qualification. No private fixture or query content was read or copied into the public regression.
- This is a correctness fix; no performance claim or benchmark result is made.

## Parent-owned original-fixture evidence

The parent independently reports that the original local Bolt fixture passes on the fix with both parsers. The parent also augmented that fixture with terminal projected `total` assertions: 150 unbounded and 50 bounded; that run passed (`/tmp/n152-138-local-evidence/terminal-total-green.log`). This augmented check is separate from the original unchanged-fixture runs. These results are parent-reported; this delegated worker did not read the private fixture or its logs. The public synthetic regression independently asserts totals and completion with both parsers.

## Final focused race

`GOMAXPROCS=2 GOMEMLIMIT=1536MiB go test -race -p 1 -tags noui,nolocalllm ./pkg/cypher -run '^(TestRetrieveContinuationOwnsRerankWindows|TestCallDbRetrieve.*|TestRerankTruthfulness.*|TestRetrieveRowsReportQueryChunks)$' -count=1`

Initial race PASS 3.222s (`/tmp/n152-138-public-race.log`). Review then added explicit per-page known-total and terminal total/discovered/ranked-count/completion assertions; the same command on the final test source PASS 3.274s (`/tmp/n152-138-public-totals-race.log`), with both parsers, and no race warnings. The ordinary 1.448s pass predates these added assertions; the final race run executes them. Both race builds emitted only the duplicate `-lobjc` linker warning.

## Dedicated vet and scoped localization

- `GOMAXPROCS=2 GOMEMLIMIT=1536MiB go vet -p 1 -tags noui,nolocalllm ./pkg/cypher`: PASS, exit 0, no output. `/tmp/n152-138-vet.log`.
- `GOMAXPROCS=2 GOMEMLIMIT=1536MiB GOFLAGS='-p=1 -tags=noui,nolocalllm' make verify ONLY=localization BASE=origin/release/v1.5.2`: PASS, exit 0. Localization package tests 4.023s; catalog tool tests 0.263s; inventory tests 0.237s; generated manifest unchanged and catalog check passed. `/tmp/n152-138-localization.log`; verifier artifacts `.verify/runs/20261121-202146`.
- UI, llama libraries, build, full tests, allocations, conformance, differential and docs were explicitly unselected in that scoped run. This is not a full-verification pass.

Final staged-source duplicate-check retry also failed during analysis/ingestion before candidates (`/tmp/n152-138-duplicates-final.log`). No indexed duplicate pass is claimed.
