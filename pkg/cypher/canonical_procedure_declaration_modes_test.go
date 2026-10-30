package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A procedure CALL's declaration facts are decided once, before anything is
// published and independently of input rows. Every mode and every position of
// the same CALL must therefore fail with the same code, detail and message,
// whether the input is empty, non-empty or another procedure's output.
//
// Each leaf logs its outcome so a failing run shows which decider answered.
func TestCanonicalProcedureDeclarationModes(t *testing.T) {
	facts := []struct{ name, call string }{
		{"arity", "CALL db.labels(1) YIELD label RETURN label"},
		{"unknown procedure", "CALL db.noSuchProcedure() YIELD label RETURN label"},
		{"unknown yield column", "CALL db.labels() YIELD noSuchColumn RETURN noSuchColumn"},
		{"static argument type", "CALL db.resampleIndex(1.5) RETURN 1 AS done"},
		// Inline local expressions: each kind the statement's own local scope
		// types (list and pattern comprehension, quantifier, reduce).
		{"list comprehension argument", "CALL db.resampleIndex([x IN [1] | x]) RETURN 1 AS done"},
		{"pattern comprehension argument", "CALL db.resampleIndex([(a)-->(b) | b]) RETURN 1 AS done"},
		{"quantifier argument", "CALL db.resampleIndex(all(x IN [1] WHERE x > 0)) RETURN 1 AS done"},
		{"reduce argument", "CALL db.resampleIndex(reduce(s = 0, x IN [1] | s + x)) RETURN 1 AS done"},
	}
	positions := []string{"", "MATCH (n) ", "UNWIND [] AS ignored ", "UNWIND [0] AS ignored ", "CALL db.labels() YIELD label AS first "}
	modes := []string{"", "EXPLAIN ", "PROFILE "}
	for _, fact := range facts {
		t.Run(fact.name, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			baseline := fact.call
			_, err := exec.Execute(ctx, baseline, nil)
			require.Error(t, err, baseline)
			wantCode, wantDetail, wantMessage := boundRuleError(err)
			for _, position := range positions {
				for _, mode := range modes {
					query := mode + position + fact.call
					t.Run(mode+position, func(t *testing.T) {
						exec, ctx := newUnitExecutor(t)
						result, err := exec.Execute(ctx, query, nil)
						code, detail, message := boundRuleError(err)
						t.Logf("PROCEDURE_DECLARATION_LEAF query=%q code=%q detail=%q message=%q result=%t", query, code, detail, message, result != nil)
						require.Error(t, err, query)
						require.Nil(t, result, query)
						require.Equal(t, wantCode, code, "%s vs %s", query, baseline)
						require.Equal(t, wantDetail, detail, "%s vs %s", query, baseline)
						require.Equal(t, wantMessage, message, "%s vs %s", query, baseline)
					})
				}
			}
		})
	}
}
