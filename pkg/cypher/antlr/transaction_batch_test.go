package antlr

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
				call, err := ReadCallClause(context.Background(), "CALL { RETURN 'é}' AS value } IN /*gap*/ TRANSACTIONS OF "+sample.expression+" "+row+" RETURN value", RawSource)
				require.NoError(t, err)
				require.NotNil(t, call.Batch)
				require.Equal(t, "RETURN 'é}' AS value", call.Body)
				require.Equal(t, "RETURN value", call.After)
				require.Equal(t, sample.expression, call.Batch.Expression)
				syntax, retained := call.Batch.ExpressionSyntax()
				require.True(t, retained)
				start, end := syntax.Span()
				require.Equal(t, sample.expression, syntax.Source()[start:end])
				require.Equal(t, sample.kind, call.Batch.LiteralKind)
			}
		})
	}
	call, err := ReadCallClause(context.Background(), "CALL { RETURN 1 AS x } IN 2 CONCURRENT TRANSACTIONS ON ERROR CONTINUE REPORT STATUS AS status", RawSource)
	require.NoError(t, err)
	require.True(t, call.Batch.Concurrent)
	require.Equal(t, "2", call.Batch.ConcurrencyExpression)
	require.Equal(t, "CONTINUE", call.Batch.ErrorMode)
	require.Equal(t, "status", call.Batch.ReportVariable)
	require.Equal(t, "1000", call.Batch.Expression)
	_, err = ReadCallClause(context.Background(), "CALL { RETURN 1 AS x } IN TRANSACTIONS OF ROWS", RawSource)
	require.Error(t, err)
	_, err = ReadTransactionalCalls(context.Background(), "CALL { RETURN 1 AS x } IN TRANSACTIONS OF ) ROWS", RawSource)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ReadCallClause(ctx, "CALL { RETURN 1 AS x }", RawSource)
	require.ErrorIs(t, err, context.Canceled)
	_, err = ReadTransactionalCalls(ctx, "RETURN 1", RawSource)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCallReadersRetainTheirSourceCoordinates(t *testing.T) {
	raw := `CALL { RETURN '\u005cu0061' AS value } IN TRANSACTIONS OF \u0031 ROWS RETURN value`
	accepted := cyphertext.NewSource(t.Context(), raw, RawSource).Text
	require.NotEqual(t, raw, accepted)
	for _, input := range []struct {
		name, source, body, size string
		stage                    SourceStage
	}{
		{"raw", raw, `RETURN '\u005cu0061' AS value`, `\u0031`, RawSource},
		{"accepted", accepted, `RETURN '\u0061' AS value`, "1", AcceptedSource},
	} {
		t.Run(input.name, func(t *testing.T) {
			call, err := ReadCallClause(t.Context(), input.source, input.stage)
			require.NoError(t, err)
			require.Equal(t, input.body, call.Body)
			require.Equal(t, "RETURN value", call.After)
			require.Equal(t, input.size, call.Batch.Expression)
			expression, ok := call.Batch.ExpressionSyntax()
			require.True(t, ok)
			start, end := expression.Span()
			require.Equal(t, input.size, input.source[start:end])
			calls, err := ReadTransactionalCalls(t.Context(), input.source, input.stage)
			require.NoError(t, err)
			require.Len(t, calls, 1)
			require.Equal(t, input.body, calls[0].Body)
		})
	}
}
