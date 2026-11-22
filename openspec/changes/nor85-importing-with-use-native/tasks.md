# NOR-85 tasks

Implementation uses the parent-approved existing Fabric owner. Verification status below records only completed runs.

- [x] Recover the exact inline fixture: ticket `description_html` first `<pre><code>`
      block, HTML-unescaped, 13711 bytes / 294 lines,
      sha256 `9195b583bca79eead8d4ddcf2bb932bcae5b1223d19175c902fcc12da9061626` — identical
      to the sha256 printed in the ticket, so the recovery is byte-exact. Written to
      `pkg/cypher/nor85_importing_with_use_test.go`; read back with the same sha256.
- [x] Establish the causal owner and the reuse plan from current source (see
      `proposal.md`; baseline HEAD `18b36a85d10e775b9d5d3500ecf3482682754a8d`).
- [x] Parent acceptance of the route decision and the convergence plan.
- [x] Add the one canonical `fabric.ParseAcceptedSubqueryUseClause` composition and converge the
      three inline two-step call sites in `pkg/fabric/planner.go`.
- [x] Add the `pkg/cypher/executor_use.go` wrapper (single `UseSyntaxError` mapping) and
      call it from `pkg/cypher/pipeline_call_operator.go:81`.
- [x] Fail-first baseline before the change:
      `go test -p=1 -tags=noui,nolocalllm ./pkg/cypher -run '^TestNOR85ImportingWithUSE' -count=1 -timeout=4m -v`
      (actual RED, 1.125s: 8 subtests per parser, `query could not be planned as a clause pipeline`).
- [x] Original matrix GREEN for both parser modes after the change, including the five
      controls per parser and the six composite-route controls per parser.
- [x] Fabric regression (actual combined command in evidence.md):
      `go test ./pkg/fabric -run 'TestParseAcceptedSubqueryUseClause|TestParseLeadingWithUse|TestPlan_.*Use' -count=1 -v`
- [x] Focused Cypher regression (actual combined command in evidence.md):
      `go test ./pkg/cypher -run 'TestUse|TestSubqueryUseClauseGrammar|TestDynamicGraphLookupOutsideComposite|TestCallSubquery|TestNOR85' -count=1 -v`
- [x] Focused race for shared entry, importing-WITH planning, original native/composite matrix, and USE error controls (exact command in evidence.md).
- [x] `go vet ./pkg/cypher ./pkg/fabric` and `gofmt -l pkg/cypher pkg/fabric`.
- [ ] `make verify-list BASE=origin/release/v1.5.2`, then `make verify BASE=origin/<pr-base>` for
      the fixing branch; post the exact head, base, command, results and skips.
- [x] Confirm in the fixing change that no production file gains a second USE reader and
      that no error text/status code changed.

Baseline RED, combined focused GREEN, and focused race completed; dedicated vet and canonical verification remain pending with the parent. Commands run with GOMAXPROCS=2 and GOMEMLIMIT=1500MiB.
