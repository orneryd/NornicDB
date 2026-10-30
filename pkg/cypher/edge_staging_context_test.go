package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipBatchStagingCancellation(t *testing.T) {
	for _, owner := range []string{"createEdgesWithContext", "bulkCreateEdgesWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			_, err = e.Execute(context.Background(), "CREATE (:A), (:B)", nil)
			require.NoError(t, err)
			patterns := make([]string, 20)
			for i := range patterns {
				patterns[i] = fmt.Sprintf("(a)-[:R {k:%d}]->(b)", i)
			}
			query := "MATCH (a:A), (b:B) CREATE " + strings.Join(patterns, ", ")
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			ctx.cypherOwner = owner == "createEdgesWithContext"
			_, err = e.Execute(ctx, query, nil)
			t.Logf("owner=%s hits=%d execute=%v", owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			edges, err := b.GetStoredEdgesByType("R")
			require.NoError(t, err)
			require.Empty(t, edges, "failed staging must not publish")
			_, err = e.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			edges, err = b.GetStoredEdgesByType("R")
			require.NoError(t, err)
			require.Len(t, edges, 20)
		})
	}
}
