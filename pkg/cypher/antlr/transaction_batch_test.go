package antlr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionalBatchStructuralReader(t *testing.T) {
	for _, sample := range []struct{ expression, kind string }{
		{"1", "Integer"}, {"((+1))", "Integer"}, {"(-1)", "Integer"},
		{"(1.0)", "Float"}, {"null", "Null"}, {"(null)", "Null"},
		{"true", "Boolean"}, {"'1'", "String"}, {"[1]", "List"}, {"{}", "Map"},
		{"$m.row", ""}, {"$m.rows", ""}, {"$m.`rows`", ""}, {"{rows:1}.rows", ""},
		{"1-1", ""}, {"toInteger(1)", ""}, {"CASE WHEN true THEN 1 ELSE 0 END", ""},
	} {
		t.Run(sample.expression, func(t *testing.T) {
			for _, row := range []string{"ROW", "ROWS"} {
				call, err := ReadCallClause(context.Background(), "CALL { RETURN 'é}' AS value } IN /*gap*/ TRANSACTIONS OF "+sample.expression+" "+row+" RETURN value")
				require.NoError(t, err)
				require.NotNil(t, call.Batch)
				require.Equal(t, "RETURN 'é}' AS value", call.Body)
				require.Equal(t, "RETURN value", call.After)
				require.Equal(t, sample.expression, call.Batch.Expression)
				require.Equal(t, sample.kind, call.Batch.LiteralKind)
			}
		})
	}
	call, err := ReadCallClause(context.Background(), "CALL { RETURN 1 AS x } IN 2 CONCURRENT TRANSACTIONS ON ERROR CONTINUE REPORT STATUS AS status")
	require.NoError(t, err)
	require.True(t, call.Batch.Concurrent)
	require.Equal(t, "2", call.Batch.ConcurrencyExpression)
	require.Equal(t, "CONTINUE", call.Batch.ErrorMode)
	require.Equal(t, "status", call.Batch.ReportVariable)
	require.Equal(t, "1000", call.Batch.Expression)
	_, err = ReadCallClause(context.Background(), "CALL { RETURN 1 AS x } IN TRANSACTIONS OF ROWS")
	require.Error(t, err)
	_, err = ReadTransactionalCalls(context.Background(), "CALL { RETURN 1 AS x } IN TRANSACTIONS OF ) ROWS")
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ReadCallClause(ctx, "CALL { RETURN 1 AS x }")
	require.ErrorIs(t, err, context.Canceled)
	_, err = ReadTransactionalCalls(ctx, "RETURN 1")
	require.ErrorIs(t, err, context.Canceled)
}
