package cypher

// Lane L6b (closeout):
//   G05 L-H2-006  apoc.periodic.commit: the batch error at call_apoc_periodic.go:204-206.
//   G07 L-H9-008 / L-H2-007  apoc.periodic.iterate: the copying step
//                 periodic_transaction_owner.go:84-89 (recordExpressionFailure on the caller).
//   C08 NOR-82    where head(n.k + date()) loses its type (follow-up to TestL6StaticTypeLossOwners).

import (
	"context"
	"testing"
)

func TestL6bPeriodicBatchErrorOwners(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	for _, q := range []string{
		"CREATE CONSTRAINT l6b_cu FOR (n:CU) REQUIRE n.k IS UNIQUE",
		"CREATE (:CU {k: 1})",
		"UNWIND range(1, 3) AS i CREATE (:Todo {i: i})",
	} {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, tc := range []struct{ name, statement string }{
		{"G05 commit-statement-violates-constraint", "MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT 1 SET n.done = true CREATE (:CU {k: 1}) RETURN count(*)"},
		{"G07 iterate-action-division-by-zero", "CREATE (:L6bPI {v: 1 / 0})"},
		{"control-succeeds", "CREATE (:L6bOK {v: 1})"},
	} {
		caller := context.WithValue(ctx, expressionFailureKey{}, &expressionFailure{})
		_, batchErr := exec.executePeriodicBatch(caller, "apoc.periodic.commit", func(batchCtx context.Context, batch *StorageExecutor) (*ExecuteResult, error) {
			return batch.executeUserStatement(batchCtx, tc.statement, nil)
		})
		t.Logf("L6B_PERIODIC %s executePeriodicBatch(periodic_transaction_owner.go:46) err=%v | caller expression failure recorded (:84-89)=%v", tc.name, batchErr, getExpressionFailure(caller))
	}
	commit, commitErr := exec.Execute(ctx, "CALL apoc.periodic.commit('MATCH (n:Todo) WHERE n.done IS NULL WITH n LIMIT $limit SET n.done = true CREATE (:CU {k: 1}) RETURN count(*)', {limit: 1})", nil)
	var commitRows, commitCols any
	if commit != nil {
		commitRows, commitCols = commit.Rows, commit.Columns
	}
	t.Logf("L6B_PERIODIC G05 Execute apoc.periodic.commit -> columns=%v rows=%v err=%v", commitCols, commitRows, commitErr)
	iterate, iterateErr := exec.Execute(ctx, "CALL apoc.periodic.iterate('UNWIND [0, 1] AS i RETURN i', 'CREATE (:L6bFuPI {v: 1 / i})', {batchSize: 1}) YIELD batches, failedBatches, errorMessages RETURN batches, failedBatches, errorMessages", nil)
	var iterRows any
	if iterate != nil {
		iterRows = iterate.Rows
	}
	t.Logf("L6B_PERIODIC G07 Execute apoc.periodic.iterate (1/i) -> rows=%v err=%v", iterRows, iterateErr)
	if commitErr == nil {
		t.Errorf("CAUSE G05: the batch returns the constraint error (logged above), callApocPeriodicCommit breaks on it (call_apoc_periodic.go:204-206) and the call answers %v with no error", commitRows)
	}
	if iterateErr != nil {
		t.Errorf("CAUSE G07: the failing batch's expression failure is copied to the caller context (periodic_transaction_owner.go:84-89), so the whole CALL fails: %v", iterateErr)
	}
}

func TestL6bNOR82ElementStep(t *testing.T) {
	ctx := context.Background()
	scope := staticTypeScope{kinds: matchSemanticScope{"n": matchBindingNode}, complete: true}
	dateSum := scope.operandOf(ctx, "n.k + date()")
	listSum := scope.operandOf(ctx, "n.k + [date()]")
	for _, tc := range []struct {
		name string
		op   staticOperand
	}{{"n.k + date()", dateSum}, {"n.k + [date()]", listSum}, {"corrected union Date or List<Any>", typedOperand("Date or List<Any>")}} {
		list := tc.op.listPart()
		elem := tc.op.elementOperand()
		t.Logf("L6B_C08 %s typeName=%q mayBeNull=%v listPart(static_operand.go:376)=%q elementOfType(%q)=%q elementOperand(:360)=%q",
			tc.name, tc.op.typeName(), tc.op.mayBeNull(), list.typeName(), list.typeName(), elementOfType(list.typeName()), elem.typeName())
	}
	if dateSum.elementOperand().typeName() == "" && elementOfType("List<T>") == "" {
		t.Errorf("CAUSE C08 NOR-82: addition with an unknown left operand types n.k + date() as %q; its list alternative List<T> has no element type, so head(...) is untyped and toUpper accepts it", dateSum.typeName())
	}
}
