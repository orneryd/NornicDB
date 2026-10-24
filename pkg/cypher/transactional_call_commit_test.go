package cypher_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionalCallKeepsCommittedBatches(t *testing.T) {
	for _, batch := range []string{"1 ROW", "1 ROWS", "1\tROW", "1\tROWS", "2 ROW", "2 ROWS"} {
		for _, body := range []string{"WITH x CREATE (:B {v: 10/x})", "WITH x CREATE (:B {v: 10/x}) RETURN x AS value"} {
			t.Run(batch+"/"+body, func(t *testing.T) {
				base, err := storage.NewBadgerEngineInMemory()
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, base.Close()) })
				store := storage.NewNamespacedEngine(base, "tenant")
				exec := cypher.NewStorageExecutor(store)
				_, err = exec.Execute(context.Background(), "UNWIND [1,2,0] AS x CALL { "+body+" } IN TRANSACTIONS OF "+batch, nil)
				require.Error(t, err)
				result, err := exec.Execute(context.Background(), "MATCH (n:B) RETURN n.v ORDER BY n.v", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(5)}, {int64(10)}}, result.Rows)
			})
		}
	}
}

func TestTransactionalCallCommittedBatchesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	base, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "tenant"))
	_, queryErr := exec.Execute(context.Background(), "UNWIND [1,2,3,0] AS x CALL { WITH x CREATE (:B {v: 12/x}) } IN TRANSACTIONS OF 2 ROW", nil)
	closeErr := base.Close()
	require.Error(t, queryErr)
	require.NoError(t, closeErr)
	reopened, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	reader := cypher.NewStorageExecutor(storage.NewNamespacedEngine(reopened, "tenant"))
	result, err := reader.Execute(context.Background(), "MATCH (n:B) RETURN n.v ORDER BY n.v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(6)}, {int64(12)}}, result.Rows)
}

func TestTransactionalCallSingularRowRetainsTail(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "tenant"))
	result, err := exec.Execute(context.Background(), "UNWIND [1,2] AS x CALL { WITH x CREATE (:B {v:x}) } IN TRANSACTIONS OF 1 ROW RETURN x ORDER BY x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
}

func BenchmarkTransactionalCallBatchCommit(b *testing.B) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, base.Close()) })
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "tenant"))
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(ctx, "UNWIND [1,2] AS x CALL { WITH x RETURN x AS value } IN TRANSACTIONS OF 1 ROWS RETURN value", map[string]interface{}{"nonce": i})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestTransactionalCallBatchSizeExpressionContract(t *testing.T) {
	cases := []struct {
		name, size string
		invalid    bool
		params     map[string]any
		errorCode  string
	}{
		{"parameter_rows", "$n ROWS", false, map[string]any{"n": int64(1)}, ""},
		{"parameter_row", "$n ROW", false, map[string]any{"n": int64(1)}, ""},
		{"addition", "1+0 ROWS", false, nil, ""},
		{"parentheses", "(1) ROWS", false, nil, ""},
		{"function", "toInteger(1) ROWS", false, nil, ""},
		{"zero", "0 ROWS", true, nil, ""},
		{"negative", "-1 ROWS", true, nil, ""},
		{"fraction", "1.5 ROWS", true, nil, ""},
		{"integral_float", "1.0 ROWS", true, nil, ""},
		{"expression_zero", "1-1 ROWS", true, nil, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_zero", "$n ROWS", true, map[string]any{"n": int64(0)}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_negative", "$n ROWS", true, map[string]any{"n": int64(-1)}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_float", "$n ROWS", true, map[string]any{"n": 1.5}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_null", "$n ROWS", true, map[string]any{"n": nil}, "Neo.ClientError.Statement.TypeError"},
		{"parameter_string", "$n ROWS", true, map[string]any{"n": "1"}, "Neo.ClientError.Statement.SyntaxError"},
	}

	for _, sample := range cases {
		t.Run(sample.name, func(t *testing.T) {
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "tenant"))
			input := "[1,2,0,4]"
			code := "Neo.ClientError.Statement.ArithmeticError"
			if sample.invalid {
				input = "[1,2]"
				code = "Neo.ClientError.Statement.SyntaxError"
			}
			if sample.errorCode != "" {
				code = sample.errorCode
			}
			statement := "UNWIND " + input + " AS x CALL { WITH x CREATE (:BatchSizeRecord {v:12/x}) } IN TRANSACTIONS OF " + sample.size

			_, err = exec.Execute(context.Background(), statement, sample.params)
			require.Error(t, err)
			var classified interface{ BoltErrorCode() string }
			require.ErrorAs(t, err, &classified)
			require.Equal(t, code, classified.BoltErrorCode())
			result, err := exec.Execute(context.Background(), "MATCH (n:BatchSizeRecord) RETURN n.v ORDER BY n.v", nil)
			require.NoError(t, err)
			if sample.invalid {
				require.Empty(t, result.Rows)
			} else {
				require.Equal(t, [][]interface{}{{int64(6)}, {int64(12)}}, result.Rows)
			}
		})
	}
}
