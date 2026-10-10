package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The db.temporal procedures read validity bounds and their as-of argument
// as the TEMPORAL NO OVERLAP constraint does (storage.CoerceTemporalTime):
// datetime(), date() and localdatetime() values as well as ISO strings, on a
// label with the constraint (the temporal index) and without it (the scan).
func TestTemporalProceduresReadTemporalValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_procedures_datetime"))
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE CONSTRAINT fact_validity FOR (f:Fact) REQUIRE (f.key, f.valid_from, f.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE (:Fact {key: 'addr', v: 'A', valid_from: datetime('2020-01-01T00:00:00Z'), valid_to: datetime('2023-01-01T00:00:00Z')})",
		"CREATE (:Fact {key: 'addr', v: 'B', valid_from: datetime('2023-01-01T00:00:00Z')})",
		"CREATE (:Note {key: 'addr', v: 'A', valid_from: datetime('2020-01-01T00:00:00Z'), valid_to: datetime('2023-01-01T00:00:00Z')})",
		"CREATE (:Note {key: 'addr', v: 'B', valid_from: date('2023-01-01')})",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	for _, label := range []string{"Fact", "Note"} {
		for asOf, want := range map[string]interface{}{
			"'2021-05-01T00:00:00Z'":                "A",
			"datetime('2021-05-01T00:00:00Z')":      "A",
			"datetime('2023-01-01T01:00:00+02:00')": "A",
			"date('2024-05-01')":                    "B",
			"localdatetime('2022-12-31T23:59:59')":  "A",
			"datetime('2019-12-31T23:59:59Z')":      nil,
		} {
			query := "CALL db.temporal.asOf('" + label + "', 'key', 'addr', 'valid_from', 'valid_to', " + asOf + ") YIELD node RETURN node.v AS v"
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			if want == nil {
				require.Empty(t, result.Rows, query)
				continue
			}
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
	}
	// assertNoOverlap reads datetime() bounds the same way.
	_, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('Note', 'key', 'valid_from', 'valid_to', 'addr', datetime('2022-06-01T00:00:00Z'), datetime('2022-07-01T00:00:00Z')) YIELD ok RETURN ok", nil)
	require.Error(t, err)
	result, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('Note', 'key', 'valid_from', 'valid_to', 'addr', datetime('2019-06-01T00:00:00Z'), datetime('2019-07-01T00:00:00Z')) YIELD ok RETURN ok", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('Fact', 'key', 'addr', 'valid_from', 'valid_to', 'not a time') YIELD node RETURN node", nil)
	require.ErrorContains(t, err, "asOf must be a valid datetime")
}
