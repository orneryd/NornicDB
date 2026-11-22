# Validation

Base f0f5a2f4fd2bf8cb8a9344af9923cb0f4efb5210. Go checks use
GOMAXPROCS=2 GOMEMLIMIT=1500MiB, -p=1 -tags=noui,nolocalllm; tests use -count=1.

Original source extracted from the ticket, no production edits before RED:

- Native `^TestNOR335`: FAIL 1.134s, same two failing command owners and five
  passing PROFILE/index/no-effects control owners; /tmp/n152-75-native-red.log.
- HTTP `^TestNOR335HTTP`: FAIL 2.319s, metadata/invalid-index owner fails;
  both-parser privilege-state control passes; /tmp/n152-75-http-red.log.
- Bolt `^TestNOR335Bolt`: FAIL 0.879s, same metadata/invalid-index defects;
  /tmp/n152-75-bolt-red.log.

First corrected native run: FAIL 1.219s solely because a new OPTIONS 7 assertion
required byte-identical diagnostics across modes. The mode's grammar parser
rejects before target admission: ANTLR offsets include EXPLAIN (49 versus57),
while native ordinary execution reports localized invalid CREATE INDEX syntax.
Original ticket cases and all other new boundaries passed in that run; this is
not a focused-green claim. /tmp/n152-75-native-green.log preserves the failure.
Parent approved correcting only the new OPTIONS 7 case to rejection/no-effects;
exact diagnostic equality remains for the five malformed target cases. Original
fixture oracle remains unchanged; no production diagnostic exception was added.

Broad selection is repeatably assembled from the ticket's exact 172-owner runner
plus the affected SHOW/schema/index-options owners and all NOR335 regressions:
246 unique tests, enumerated in fixtures/broad-selection.json. The original
TestProfileMatchCreateDelete 20,000-iteration stress harness remains explicitly
excluded. No historical 16k/172k population replay is claimed.

Fresh Neo4j 5.26.30: nineteen exact probes, independently compared query-by-query,
with raw records in fixtures/fresh-neo4j-5.26.30.json. Log
/tmp/n152-75-reference.log ended CLEANUP_COMPLETE after bounded task-owned remote
container, volume and newly pulled image cleanup. Historical ticket output is
stored separately in original-source-blocks.json. Legacy syntax and GRANT are
NornicDB established contracts, not claimed as fresh Neo4j community parity.

DeepSeek job 7cda015523a34973aba5e668da40d0e5 completed in85.644s, read-only;
no code/test acceptance inferred. Source routing limitations and rejected report
claims are in proposal.md. Automatic graph retrieval incomplete (DNS, no baseline).
Unchanged staged duplicate checker FAIL exit1 at localhost:7474/nornicdbcode during analysis/ingestion; /tmp/n152-75-duplicate.log. No candidate verdict, no clean analysis claim.

Broad native normal PASS 10.941s, combined246 selection; /tmp/n152-75-broad-normal.log. HTTP green PASS 2.643s and Bolt green PASS 0.875s; /tmp/n152-75-http-green.log and /tmp/n152-75-bolt-green.log. Broad246 native race PASS 49.998s, /tmp/n152-75-broad-race.log. Verbose top-level PASS names exactly match all246 selected owners; zero FAIL and zero SKIP at any level. HTTP race PASS 15.335s and Bolt race PASS 2.232s; /tmp/n152-75-http-race.log and /tmp/n152-75-bolt-race.log. Three-package vet PASS exit0: go vet -p=1 -tags=noui,nolocalllm ./pkg/cypher ./pkg/server ./pkg/bolt, /tmp/n152-75-vet.log.

Scoped verifier: GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOFLAGS='-p=1 -tags=noui,nolocalllm' make verify BASE=origin/release/v1.5.2 FETCH=0 ONLY=localization, PASS; /tmp/n152-75-localization.log. Other steps explicitly unselected; this is not a full verification pass. make verify-list also passed, /tmp/n152-75-verify-list.log. Full release verify,
full coverage, performance/allocation qualification, conformance and differential
remain unexecuted. No performance improvement or merge-readiness claim.

SHOW full-column projections retain the existing NornicDB extensions (including the procedure option column); the original star-header contract compares EXPLAIN with its runtime listing. Fresh reference headers do not imply every NornicDB extension exists in Neo4j.

Parent source review approved the shared declarations and syntax-only extraction.
Publication is authorized after the scoped checks; PR/Plane contain final head and
base reconciliation. Raw temporary evidence cited above is archived under
/Users/timothysweet/.codex/task-evidence/n152-75 with a SHA-256 manifest and verified
identical copies before removing redundant task-only temporary files. Remote owned
resources are already cleaned. Open review worktree/head branch and the shared
/Users/timothysweet/Library/Caches/go-build remain preserved for active release
work. No task subagent worktree exists. The old proposal worktree stays read-only.
