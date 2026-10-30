package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// OPTIONAL CALL keeps an input row the call produces no row for, with null
// for the call's columns, for subqueries and procedures (Neo4j 5.26.30,
// #907).
func TestOptionalCallMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "optional_call"))
	ctx := context.Background()
	for _, testCase := range []struct {
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{"UNWIND [1, 2] AS i OPTIONAL CALL (i) { WITH i WHERE i > 1 RETURN i AS j } RETURN i, j", []string{"i", "j"}, [][]interface{}{{int64(1), nil}, {int64(2), int64(2)}}},
		{"UNWIND [1, 2] AS i OPTIONAL CALL (i) { UNWIND range(1, i) AS k RETURN k } RETURN i, k", []string{"i", "k"}, [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}, {int64(2), int64(2)}}},
		{"UNWIND [1] AS i OPTIONAL CALL (i) { MATCH (n:NoSuchLabel) RETURN n } RETURN i, n", []string{"i", "n"}, [][]interface{}{{int64(1), nil}}},
		{"UNWIND [1, 2] AS i OPTIONAL CALL (i) { WITH i WHERE i > 1 RETURN count(*) AS c } RETURN i, c", []string{"i", "c"}, [][]interface{}{{int64(1), int64(0)}, {int64(2), int64(1)}}},
		{"UNWIND [1, 2] AS i OPTIONAL CALL (*) { WITH i WHERE i > 1 RETURN i AS j } RETURN i, j", []string{"i", "j"}, [][]interface{}{{int64(1), nil}, {int64(2), int64(2)}}},
		{"UNWIND [1] AS i OPTIONAL CALL (i) { RETURN 1 AS a, 2 AS b UNION RETURN 3 AS a, 4 AS b } RETURN i, a, b", []string{"i", "a", "b"}, [][]interface{}{{int64(1), int64(1), int64(2)}, {int64(1), int64(3), int64(4)}}},
		{"OPTIONAL CALL db.labels() YIELD label WHERE label = 'NoSuchLabel' RETURN label", []string{"label"}, [][]interface{}{{nil}}},
		{"UNWIND [1] AS i OPTIONAL CALL (i) { CREATE (:O) } RETURN i", []string{"i"}, [][]interface{}{{int64(1)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.columns, result.Columns)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
	// optional stays a variable name.
	result, err := exec.Execute(ctx, "WITH 1 AS optional CALL { RETURN 2 AS x } RETURN optional, x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}}, result.Rows)
}

// pipelineApplyOptionally passes an error from the call through and gives up
// (ok = false) when the call can't run per row.
func TestPipelineApplyOptionallyStops(t *testing.T) {
	rows := []pipelineRow{{"i": int64(1)}, {"i": int64(2)}}
	failure := errors.New("call failed")
	ok := true
	_, err := pipelineApplyOptionally(rows, func([]pipelineRow) ([]pipelineRow, []string, bool, error) {
		return nil, nil, true, failure
	}, &ok)
	require.ErrorIs(t, err, failure)
	require.True(t, ok)

	out, err := pipelineApplyOptionally(rows, func([]pipelineRow) ([]pipelineRow, []string, bool, error) {
		return nil, nil, false, nil
	}, &ok)
	require.NoError(t, err)
	require.Nil(t, out)
	require.False(t, ok)
}
