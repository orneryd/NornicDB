package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// rowValue evaluates expr with the row evaluator for a test, failing the
// test on an evaluation error: tests of values never see an error as
// "unresolved".
func rowValue(t testing.TB, e *StorageExecutor, expr string, values map[string]interface{}) (interface{}, bool) {
	t.Helper()
	fixtureAdmission849Context, fixtureAdmission849 := expressionTestRow(rowStatementContextOrBackground(values), expr, values)
	_ = fixtureAdmission849
	value, ok, err := e.evaluateRowValue(fixtureAdmission849, expr, fixtureAdmission849Context)
	require.NoError(t, err, expr)
	return value, ok
}
