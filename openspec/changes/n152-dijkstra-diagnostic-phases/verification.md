# Scoped verification

Base a5b7980e89171764091a6d47d0b8a8ee3ca19c88; fetched release base and original
test blob verified equal before edits. Production remains unchanged.

Go environment: GOMAXPROCS=2 GOMEMLIMIT=1500MiB.
Selection:
`^Test(ApocDijkstraHonorsAcceptedDefaultWeight|ProcedureArgumentStaticTypes|ProcedureParameterAcceptsStaticType|IndexProcedureArgumentErrorCodes)$`

- Original unmodified release witness: FAIL1.030s; original-evidence.json
  preserves supplied log and exact original source.
- Superseded first authored-test run: FAIL1.389s, only the new handler detail
  assertion misused Neo4jStatus's message return as detail. Corrected to typed
  BoltErrorCode/BoltErrorDetail accessors; /tmp/n152-dijkstra-superseded-test-api.log.
  This was a test-authoring error, not a production diagnostic change.
- Final source normal: `go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run <selection> -count=1 -v`, PASS1.074s;
  /tmp/n152-dijkstra-normal.log. Original weights preserved; both parsers cover
  static literal/bound/WITH errors, accepted rebinding and named handler TypeError.
- Same selection with -race: PASS2.728s; /tmp/n152-dijkstra-race.log.
- go vet -p=1 -tags=noui,nolocalllm ./pkg/cypher: PASS exit0; /tmp/n152-dijkstra-vet.log.
- GOMAXPROCS=2 GOMEMLIMIT=1500MiB GOFLAGS='-p=1 -tags=noui,nolocalllm' make verify BASE=origin/release/v1.5.2 FETCH=0 ONLY=localization: PASS9s; /tmp/n152-dijkstra-localization.log. All other steps explicitly unselected; not a full verify pass.
- `make verify-list BASE=origin/release/v1.5.2 FETCH=0`: PASS;
  /tmp/n152-dijkstra-verify-list.log. Not a full verification run.
- Unchanged required `scripts/code-intelligence-hook.sh check-staged`: FAIL exit1,
  analysis/ingestion at localhost:7474/nornicdbcode, no verdict;
  /tmp/n152-dijkstra-duplicate.log. Worker graph retrieval also incomplete.

No helper/production owner was added or duplicated. The test directly exercises
existing static and evaluated-handler contracts. Independent source review and
DeepSeek's bounded read-only findings are documented in proposal.md; unsupported
worker suggestions were not integrated.

Full release qualification remains separate. No benchmark is claimed or needed
for this test-only change. No exact Neo4j/APOC runtime replay was run; pinned
source evidence is identified separately in proposal.md. No Plane item created,
reopened or changed. Parent reviewed the test diff and authorized draft publication after these checks.

Cleanup: raw task-only temporary evidence above is SHA-256 verified and archived under /Users/timothysweet/.codex/task-evidence/n152-dijkstra-diagnostic before removal of redundant temporary files. The supplied /tmp/n152-68-dijkstra-base.log belongs to another task and remains untouched. Open review worktree/branch and shared /Users/timothysweet/Library/Caches/go-build are retained for concurrent release work. No remote task resources or child worktrees were created.
