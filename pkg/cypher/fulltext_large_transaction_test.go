// Author: #1683 lane (Claude), 2026-10-30 (NOR-148).
package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// One explicit transaction larger than one Badger batch is one write. On a
// synchronous low-memory engine, through a namespaced view, the ranked
// corpus refreshes the changed documents by ID without rereading the store.
// The engine is on disk: in memory, Badger keeps every value inline, and the
// low-memory profile's inline limit is below these document bodies.
func TestFulltextMaintainedPublicLargeTransactionRefresh(t *testing.T) {
	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
	require.NoError(t, err)
	exec, store := fulltextMaintainedFixtureOn(t, base, 4000)
	ctx := context.Background()
	query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score ORDER BY score DESC, node.id"
	warm, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
	require.NoError(t, err)
	require.NotEmpty(t, warm.Rows)
	t.Logf("corpus scans after warming: %d", store.scans.Load())

	// Every node's body is rewritten (about 2 KB each), several Badger
	// batches on this engine; existing labels and property names only.
	for _, statement := range []string{
		"BEGIN",
		"MATCH (n) SET n.title = CASE n.id WHEN 'n000040' THEN 'newunique' ELSE 'Correspondence revised' END",
		"COMMIT",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	// The statement itself may read the store; only the ranked queries after
	// the commit are measured.
	scans := store.scans.Load()
	t.Logf("corpus scans after the transaction: %d", scans)
	changed, err := exec.Execute(ctx, query, map[string]any{"query": "newunique"})
	require.NoError(t, err)
	require.Len(t, changed.Rows, 1)
	require.Equal(t, "n000040", changed.Rows[0][0])
	t.Logf("corpus scans after the first ranked query: %d", store.scans.Load())
	require.Equal(t, scans, store.scans.Load(), "one large transaction must refresh by ID without rereading the whole store")
	revised, err := exec.Execute(ctx, query, map[string]any{"query": "revised"})
	require.NoError(t, err)
	require.Len(t, revised.Rows, 999, "every other Source document carries the revised title")
	require.Equal(t, scans, store.scans.Load(), "a repeated ranked query must reuse the refreshed documents")
}
