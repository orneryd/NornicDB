// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// Protocol executors can read through a quota/namespace wrapper while the DB
// resolver returns the same service created with the underlying scoped view.
func TestFulltextMaintainedPublicSharedServiceView(t *testing.T) {
	exec, store := fulltextMaintainedFixture(t, 80)
	svc := search.NewService(store.NamespacedEngine, search.DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	exec.SetSearchServiceResolver("fulltext_fixture", func(string) *search.Service { return svc })
	ctx := context.Background()
	query := "CALL db.index.fulltext.queryNodes('source_text', 'hotel') YIELD node, score RETURN node.id, score"
	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 2)
	scans := store.scans.Load()
	for repeat := 0; repeat < 3; repeat++ {
		got, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.Equal(t, first.Rows, got.Rows)
		require.Equal(t, scans, store.scans.Load(), "a committed view must be reusable through the DB service resolver")
	}
	node, err := store.GetNode("n000040")
	require.NoError(t, err)
	node.Properties["text"] = "routine"
	require.NoError(t, store.UpdateNode(node))
	updated, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, updated.Rows, 1)
	_, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	stable := store.scans.Load()
	_, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, stable, store.scans.Load())
}
