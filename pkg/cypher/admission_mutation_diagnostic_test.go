package cypher

import (
	"context"
	"testing"
)

func TestAdmissionMutationDiagnostic(t *testing.T) {
	query := "MATCH (n:T) SET n.v = 1 + RETURN n.v"
	e := newFabricCallRowsExecutor(t)
	input := newSemanticStatementInput(query, nil)
	clauses, ok := input.pipeline(true)
	t.Logf("ADMISSION clauses=%d ok=%v syntax=%+v", len(clauses), ok, input.syntax())
	for _, clause := range clauses {
		t.Logf("CLAUSE kind=%v text=%q", clause.kind, clause.text)
		if clause.kind == pipelineClauseSet {
			scope := newSemanticBindingScope()
			scope.bind("n")
			t.Logf("SET direct=%v", e.validateSetClauseScope(scope, clause.text))
		}
	}
	t.Logf("SYNTAX err=%v", e.validateSyntax(context.Background(), query, false))
	t.Logf("SCOPE err=%v", e.validateMatchSemanticScopes(query, false))
	actual, err := newFabricCallRowsExecutor(t).Execute(context.Background(), query, nil)
	t.Logf("PUBLIC result=%+v err=%v", actual, err)
}
