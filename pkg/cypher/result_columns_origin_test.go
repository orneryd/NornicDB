// Author: Codex R17 implementation agent, 27 October 2026.
package cypher

import (
	"context"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRetainedSyntaxFactsKeepCacheOnlyHeadersAndRebindNodeOrigins(t *testing.T) {
	source := "WITH 161803 AS retainedOriginInput CALL (retainedOriginInput) { RETURN retainedOriginInput AS value } RETURN value"
	outer := "CALL () { " + source + " } RETURN value"
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "retained_fact_origin"))
	ctx := cypherantlr.WithStatementSyntax(context.Background(), outer)
	_, err := cypherantlr.ParseSyntax(ctx, outer, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	known := func(names resultmetadata.Names) []string {
		var columns []string
		names.Visit(func(values []string) { columns = values }, func() { t.Fatal("cached header is unknown") })
		return columns
	}
	executor.CachedResultColumns(ctx, source, nil).Visit(func([]string) { t.Fatal("cold hint became known") }, func() {})
	body, err := cypherantlr.QuerySyntax(ctx, source, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, cypherantlr.CypherParserRULE_subqueryBody, body.Root().Rule(), "the nested query uses its enclosing canonical tree")
	first := executor.projectedStatementScope(ctx, source, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
	require.True(t, first.complete)
	require.NotNil(t, first.bound)
	require.Equal(t, body.Origin(), first.bound.syntax.Origin())
	require.Equal(t, []string{"value"}, known(executor.CachedResultColumns(ctx, source, nil)))

	// An independent execution of the same text owns a whole Script instead
	// of the earlier nested window. The cache key is unchanged; its node IDs
	// must be rebound while cache-only headers remain reusable.
	ctx = cypherantlr.WithStatementSyntax(ctx, source)
	script, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, false, cypherantlr.AcceptedSource)
	require.NoError(t, err)
	require.NotEqual(t, body.Origin(), script.Origin())
	second := executor.projectedStatementScope(ctx, source, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
	require.True(t, second.complete)
	require.NotNil(t, second.bound)
	require.NotSame(t, first.bound, second.bound, "a node-bearing program must be rebound to the promoted grammar product")
	require.Equal(t, script.Origin(), second.bound.syntax.Origin())
	require.Equal(t, []string{"value"}, known(executor.CachedResultColumns(ctx, source, nil)))
	result, err := executor.Execute(ctx, source, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"value"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(161803)}}, result.Rows)
}
