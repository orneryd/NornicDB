package cypher

// Root-cause diagnostic (campaign v1.5.2, lane A, group G-procedure-yield-validation).
// Test-side only.
// P-N152-122: on the registry path (call.go:1276-1315) the YIELD items are
// compared with the handler's *result* columns (applyYieldFilter ->
// validateYieldColumnsExist, procedure_call_parsing.go:91) only after the
// handler ran (call.go:1305); validateProcedureYieldBindings
// (procedure_validation.go:143) never compares them with the declared
// proc.Spec.Returns. EXPLAIN (explain.go:99-139) runs neither.
// P-N152-54: logs which check answers each YIELD * statement first.

import (
	"context"
	"sync/atomic"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

// rcaYieldAgainstDeclaration is the missing step, test-side: reject a YIELD
// item that the registered declaration does not list.
func rcaYieldAgainstDeclaration(ctx context.Context, cypher string) (string, bool) {
	yield := parseYieldClause(ctx, cypher)
	procName, err := extractProcedureName(ctx, cypher)
	if err != nil || yield == nil || yield.yieldAll {
		return "", true
	}
	proc, found := globalProcedureRegistry.Get(procName)
	if !found {
		return "", true
	}
	declared := map[string]bool{}
	for _, c := range proc.Spec.Returns {
		declared[c.Name] = true
	}
	for _, item := range yield.items {
		if !declared[item.name] {
			return item.name, false
		}
	}
	return "", true
}

func TestRCAUnknownYieldCheckedAfterHandler(t *testing.T) {
	const name = "rca.yieldAdmission"
	calls := nor315Register(t, name, nor315EmptyHandler)
	exec := newCallScopeExecutor(t)
	ctx := t.Context()
	bad := "CALL " + name + "() YIELD missing RETURN missing"

	before := calls.Load()
	_, err := exec.Execute(ctx, bad, nil)
	t.Logf("RCA_YIELD plain: handlerCalls=%d err=%v", calls.Load()-before, err)

	before = calls.Load()
	res, err := exec.Execute(ctx, "EXPLAIN "+bad, nil)
	cols := []string(nil)
	if res != nil {
		cols = res.Columns
	}
	t.Logf("RCA_YIELD explain: handlerCalls=%d err=%v columns=%v", calls.Load()-before, err, cols)

	yield := parseYieldClause(ctx, "CALL "+name+"() YIELD missing")
	t.Logf("RCA_YIELD validateProcedureYieldBindings(declared-unaware)=%v", validateProcedureYieldBindings(yield, true))
	missing, ok := rcaYieldAgainstDeclaration(ctx, "CALL "+name+"() YIELD missing")
	t.Logf("RCA_YIELD corrected pre-invocation declaration check: ok=%v undeclared=%q", ok, missing)
	_, okValid := rcaYieldAgainstDeclaration(ctx, "CALL "+name+"() YIELD value")
	t.Logf("RCA_YIELD corrected check on declared output value: ok=%v", okValid)
}

func TestRCAYieldWildcardFirstAnsweringCheck(t *testing.T) {
	exec, _ := nor168RouteExecutor(t, "direct")
	var invocations atomic.Int64
	registerNor168FrameProcedure(t, &invocations)
	ctx := context.Background()
	for _, q := range []string{
		"CALL review.frame($n) YIELD * WITH x RETURN x AS k",
		"CALL review.frame($n) YIELD * WITH missing RETURN missing",
	} {
		_, colErr := exec.StatementResultColumnsContext(ctx, q, AcceptedSource)
		yield := parseYieldClause(ctx, "CALL review.frame($n) YIELD *")
		bindErr := validateProcedureYieldBindings(yield, true)
		_, execErr := exec.Execute(ctx, q, nor168FrameParameters())
		code, msg := nornicerrors.Neo4jStatus(execErr)
		t.Logf("RCA_WILDCARD query=%q resultColumnsCompiler=%v yieldBindings=%v execute=[%s] %q", q, colErr, bindErr, code, msg)
	}
}
