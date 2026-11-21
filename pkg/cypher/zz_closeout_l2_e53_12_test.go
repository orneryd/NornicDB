//go:build closeout_l2_e53_12

package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E53.12: EXPLAIN of a node-key constraint accepts requirements that are not
// properties of its variable ((m.a, m.b) with m undeclared; (n.a, 7)).
// Executor path of the entry; the parser is the process's (NORNICDB_PARSER).

var l2E53_12Bad = []string{
	"CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (m.a, m.b) IS NODE KEY",
	"CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, 7) IS NODE KEY",
}

var l2E53_12Good = []string{
	"CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, n.b) IS NODE KEY",
	"CREATE CONSTRAINT review_key FOR ()-[r:Review]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
	"CREATE CONSTRAINT review_key FOR (n:Review) REQUIRE (n.a, n.b) IS UNIQUE",
}

// l2E53_12Run runs statement on a fresh executor and then SHOW CONSTRAINTS;
// it returns the statement's error and the number of constraints afterwards.
func l2E53_12Run(t *testing.T, statement string) (int, error) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2explain"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, statement, nil)
	show, showErr := exec.Execute(ctx, "SHOW CONSTRAINTS YIELD name RETURN name", nil)
	if showErr != nil {
		t.Fatalf("SHOW CONSTRAINTS after %q: %v", statement, showErr)
	}
	return len(show.Rows), err
}

func TestL2_e53_12_Repro(t *testing.T) {
	var accepted []string
	var figures []interface{}
	for _, statement := range l2E53_12Bad {
		constraints, err := l2E53_12Run(t, "EXPLAIN "+statement)
		figures = append(figures, statement, err, constraints)
		if err == nil {
			accepted = append(accepted, statement)
		}
		if constraints != 0 {
			t.Errorf("EXPLAIN %s left %d constraint(s)", statement, constraints)
		}
	}
	verdict := "OK"
	if len(accepted) > 0 {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E53.12 %s parser=%s explain_accepted=%d/%d %v", verdict, config.GetParserType(), len(accepted), len(l2E53_12Bad), figures)
	for _, statement := range accepted {
		t.Errorf("EXPLAIN %s returned no error; want the refusal the statement gets without EXPLAIN", statement)
	}
}

// Control: valid key/unique requirements under EXPLAIN return no error and
// create nothing; the bad requirements without EXPLAIN are refused and
// create nothing (the entry's 2dbc67e96 behaviour, the reference here).
func TestL2_e53_12_Control(t *testing.T) {
	for _, statement := range l2E53_12Good {
		constraints, err := l2E53_12Run(t, "EXPLAIN "+statement)
		t.Logf("control EXPLAIN %s -> err=%v constraints=%d", statement, err, constraints)
		if err != nil || constraints != 0 {
			t.Errorf("control EXPLAIN %s: err=%v constraints=%d, want nil and 0", statement, err, constraints)
		}
	}
	for _, statement := range l2E53_12Bad {
		constraints, err := l2E53_12Run(t, statement)
		t.Logf("control %s -> err=%v constraints=%d", statement, err, constraints)
		if err == nil || constraints != 0 {
			t.Errorf("control %s: err=%v constraints=%d, want a refusal and 0", statement, err, constraints)
		}
	}
}

// Cause: executeExplain (explain.go:98) now calls prepareCreateConstraint for
// a CREATE CONSTRAINT before building the plan (:107-111). This logs, for
// each bad requirement, whether isCreateConstraintStatement recognises the
// EXPLAIN body and what the typed preparation (prepareCreateConstraint,
// schema_constraint_preparation.go:22; the refusal is
// constraintReferenceMismatchError, schema_constraint_requirement.go:83)
// returns for it, next to the EXPLAIN outcome; a nil preparation error or a
// false recognition locates the skipped validation.
func TestL2_e53_12_Cause(t *testing.T) {
	ctx := context.Background()
	for _, statement := range l2E53_12Bad {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "zzei2explain"))
		recognised := isCreateConstraintStatement(ctx, statement)
		_, prepErr := exec.prepareCreateConstraint(ctx, statement)
		_, explainErr := exec.Execute(ctx, "EXPLAIN "+statement, nil)
		t.Logf("cause parser=%s %q isCreateConstraint=%t prepare=%v explain=%v", config.GetParserType(), statement, recognised, prepErr, explainErr)
	}
}
