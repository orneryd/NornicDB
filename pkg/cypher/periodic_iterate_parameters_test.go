package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// RC report 24 / 53.1 (B23): apoc.periodic.iterate never read the `params`
// entry of its config map. The action statement saw only the outer statement's
// parameters, so a configured value was ignored (or silently missing), and a
// failed batch was counted without its message. These tests drive the public
// Cypher path (StorageExecutor.Execute with real statements and parameters), as
// the report's reproduction does.

const iterateTestSource = "UNWIND [1,2,3] AS i RETURN i"

// iterateStoredInts reads one property of every ProcessedItem node, ordered, so the
// test sees what the action actually stored.
func iterateStoredInts(t *testing.T, exec *StorageExecutor, ctx context.Context, property string) []int64 {
	t.Helper()
	result, err := exec.Execute(ctx, "MATCH (n:ProcessedItem) RETURN n."+property+" AS value ORDER BY value", nil)
	require.NoError(t, err)
	values := make([]int64, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 1)
		value, ok := row[0].(int64)
		require.True(t, ok, "stored %s is %T, want int64", property, row[0])
		values = append(values, value)
	}
	return values
}

// TestApocPeriodicIterateConfiguredParameters covers the config `params` entry:
// it supplies the parameters of the inner statements, an expression in it is
// evaluated with the outer statement's parameters, and a configured value
// overrides a same-named outer parameter. The iterate row's columns stay
// available to the action alongside the configured parameters.
func TestApocPeriodicIterateConfiguredParameters(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		tests := []struct {
			name    string
			iterate string
			outer   map[string]interface{}
			config  string
			action  string
			want    []int64
		}{
			{
				name:   "configured parameter overrides the outer parameter",
				outer:  map[string]interface{}{"v": int64(1)},
				config: "{batchSize: 1, params: {v: 7}}",
				action: "CREATE (:ProcessedItem {i: i, v: $v})",
				want:   []int64{7, 7, 7},
			},
			{
				name:   "configured parameter expression reads the outer parameter",
				outer:  map[string]interface{}{"v": int64(7)},
				config: "{batchSize: 1, params: {w: $v + 1}}",
				action: "CREATE (:ProcessedItem {i: i, v: $w})",
				want:   []int64{8, 8, 8},
			},
			{
				name:   "configured parameter without outer parameters",
				config: "{batchSize: 1, params: {v: 7}}",
				action: "CREATE (:ProcessedItem {i: i, v: $v})",
				want:   []int64{7, 7, 7},
			},
			{
				name:   "iterate column overrides a same-named configured parameter",
				config: "{batchSize: 2, params: {i: 999}}",
				action: "CREATE (:ProcessedItem {v: i})",
				want:   []int64{1, 2, 3},
			},
			{
				name:   "iterate columns stay bound beside the configured parameters",
				config: "{batchSize: 2, params: {factor: 10}}",
				action: "CREATE (:ProcessedItem {i: i, v: i * $factor})",
				want:   []int64{10, 20, 30},
			},
			{
				name:    "configured parameters reach the iterate statement",
				iterate: "UNWIND [1,2,3] AS i RETURN i + $offset AS i",
				config:  "{batchSize: 2, params: {offset: 10}}",
				action:  "CREATE (:ProcessedItem {i: i, v: i})",
				want:    []int64{11, 12, 13},
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
				iterate := tc.iterate
				if iterate == "" {
					iterate = iterateTestSource
				}
				statement := fmt.Sprintf(
					"CALL apoc.periodic.iterate('%s', '%s', %s) YIELD failedBatches, errorMessages RETURN failedBatches, errorMessages",
					iterate, tc.action, tc.config)
				result, err := exec.Execute(ctx, statement, tc.outer)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Equal(t, int64(0), result.Rows[0][0], "no batch may fail")
				require.Equal(t, map[string]interface{}{}, result.Rows[0][1], "no error may be reported")
				require.Equal(t, tc.want, iterateStoredInts(t, exec, ctx, "v"))
			})
		}
	})
}

// TestApocPeriodicIterateReportsFailedBatchMessages is the report's 53.1
// reproduction: two batches break a uniqueness constraint. The batches are
// counted as failed and each failure's message must be reported, not dropped
// into an empty errorMessages map.
func TestApocPeriodicIterateReportsFailedBatchMessages(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ctx := context.Background()
		exec := newAsyncStackTestExecutor(t)
		for _, statement := range []string{
			"CREATE CONSTRAINT piu_k IF NOT EXISTS FOR (u:PIU) REQUIRE u.k IS UNIQUE",
			"UNWIND range(1,4) AS i CREATE (:PI {i:i})",
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}

		result, err := exec.Execute(ctx, `CALL apoc.periodic.iterate(
		'MATCH (n:PI) RETURN n ORDER BY n.i',
		'CREATE (:PIU {k: n.i % 2})',
		{batchSize:1})
		YIELD batches, total, committedOperations, failedOperations, failedBatches, errorMessages, batch, operations
		RETURN batches, total, committedOperations, failedOperations, failedBatches, errorMessages, batch, operations`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		observed := make(map[string]interface{}, len(result.Columns))
		for index, column := range result.Columns {
			observed[column] = result.Rows[0][index]
		}

		require.Equal(t, int64(4), observed["batches"])
		require.Equal(t, int64(4), observed["total"])
		require.Equal(t, int64(2), observed["committedOperations"])
		require.Equal(t, int64(2), observed["failedOperations"])
		require.Equal(t, int64(2), observed["failedBatches"])

		errorMessages, ok := observed["errorMessages"].(map[string]interface{})
		require.True(t, ok, "errorMessages is %T", observed["errorMessages"])
		t.Logf("errorMessages: %v", errorMessages)
		require.Len(t, errorMessages, 2, "each failed batch's own message: %v", errorMessages)
		for message, count := range errorMessages {
			require.Contains(t, message, "UNIQUE on PIU.[k]")
			require.Contains(t, message, "already exists")
			require.Equal(t, int64(1), count, message)
		}

		batch, ok := observed["batch"].(map[string]interface{})
		require.True(t, ok, "batch is %T", observed["batch"])
		t.Logf("batch: %v", batch)
		require.Equal(t, int64(2), batch["failed"])
		require.Len(t, batch["errors"], 2)
		operations, ok := observed["operations"].(map[string]interface{})
		require.True(t, ok, "operations is %T", observed["operations"])
		t.Logf("operations: %v", operations)
		require.Equal(t, int64(2), operations["failed"])
		require.Len(t, operations["errors"], 2)
	})
}

func TestPeriodicIterateFailureMessageCountIsNotBatchRowCount(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newAsyncStackTestExecutor(t)
		for _, query := range []string{"CREATE CONSTRAINT batch_key IF NOT EXISTS FOR (n:BatchKey) REQUIRE n.k IS UNIQUE", "CREATE (:BatchKey {k:0})"} {
			_, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err)
		}
		result, err := exec.Execute(t.Context(), `CALL apoc.periodic.iterate('UNWIND [1,2] AS i RETURN i','CREATE (:BatchKey {k:0})',{batchSize:2}) YIELD failedOperations,failedBatches,errorMessages RETURN failedOperations,failedBatches,errorMessages`, nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(2), result.Rows[0][0])
		require.Equal(t, int64(1), result.Rows[0][1])
		messages, ok := result.Rows[0][2].(map[string]interface{})
		require.True(t, ok)
		require.Len(t, messages, 1)
		for _, count := range messages {
			require.Equal(t, int64(1), count, "one failed batch execution, not one error per rolled-back row")
		}
	})
}
