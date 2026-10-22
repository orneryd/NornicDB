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
	fixtureAdmission840Context, fixtureAdmission840 := expressionTestRow(rowStatementContextOrBackground(values), expr, values)
	_ = fixtureAdmission840
	value, ok, err := e.evaluateRowValue(fixtureAdmission840, expr, fixtureAdmission840Context)
	require.NoError(t, err, expr)
	return value, ok
}
