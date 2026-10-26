// Author: Codex R17 implementation agent, 26 October 2026.
package antlr

import (
	"context"
	"testing"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"
)

func TestStatementSyntaxPredictionPreservesFullContextProduct(t *testing.T) {
	for _, sample := range []struct {
		name, source string
		entry        SyntaxEntry
	}{
		{"nested_calls", "WITH 1 AS x CALL (x) { OPTIONAL MATCH (n:Record {id: x}) WITH x, collect(n.id) AS ids RETURN [id IN ids | {id: id, values: [v IN [x] | CASE WHEN v = x THEN v ELSE 0 END]}] AS items } RETURN items", StatementEntry},
		{"exists_case", "MATCH (n:Record) WHERE CASE WHEN n.at IS NOT NULL THEN datetime(n.at) <= datetime('2026-01-01T00:00:00Z') ELSE EXISTS { MATCH (m:Record) WHERE m.id = n.id } END RETURN n.id", fragmentEntry},
		{"union_body", "WITH x RETURN x AS value UNION ALL WITH x RETURN x AS value", ClauseEntry},
		{"call_scope", "CALL (`a,b`, x) { RETURN x AS value } IN TRANSACTIONS OF 2 ROWS", CallEntry},
		{"unicode", "/* é */ RETURN [x IN [1,2] | {`名`: x, text: 'عربي'}] AS `値`", StatementEntry},
		{"scalar", "reduce(total = 0, n IN [1,2] | total + CASE WHEN n > 0 THEN n ELSE 0 END)", ExpressionEntry},
		{"pattern", "(n:Record {id: $id})-[r:LINK]->(m)", PatternEntry},
		{"procedure", "CALL db.labels() YIELD label RETURN label", fragmentEntry},
		{"malformed_scope", "CALL (x,) { RETURN 1 AS value } RETURN value", StatementEntry},
		{"malformed_literal", "RETURN 'unterminated", StatementEntry},
		{"trailing_expression", "1 ROWS", ExpressionEntry},
		{"trailing_body", "RETURN 1 INVALID_TAIL", ClauseEntry},
	} {
		t.Run(sample.name, func(t *testing.T) {
			ctx := context.Background()
			want, wantErr := parseStatementSyntaxPrediction(ctx, sample.source, sample.entry, nil, antlr4.PredictionModeLL)
			observations := 0
			got, gotErr := parseStatementSyntax(ctx, sample.source, sample.entry, func(antlr4.Tree) { observations++ })
			if wantErr != nil {
				require.EqualError(t, gotErr, wantErr.Error())
			} else {
				require.NoError(t, gotErr)
			}
			require.Equal(t, want, got, "prediction must preserve every node, token, source span and diagnostic")
			require.Equal(t, 1, observations, "a failed fast attempt cannot publish a partial tree")
		})
	}
}

func TestStatementSyntaxPredictionCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	observed := false
	data, err := parseStatementSyntax(ctx, "RETURN 1", StatementEntry, func(antlr4.Tree) { observed = true })
	require.False(t, observed)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, data)
}
