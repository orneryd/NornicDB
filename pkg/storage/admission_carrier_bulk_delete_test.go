package storage

import (
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// The admission carrier keeps the wrapped engine's one bulk deletion: through
// the public WAL path (which admits its inner handle), missing, repeated and
// empty IDs are skipped exactly as the native bulk owner skips them, and a
// bulk call that fails part-way deletes nothing.
func TestAdmissionCarrierBulkDeleteKeepsNativeSemantics(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base := newTestEngine(t)
	wal, err := NewWAL(filepath.Join(t.TempDir(), "wal"), &WALConfig{SyncMode: "immediate"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })
	engine := NewWALEngine(base, wal)

	nodes := bulkTestNodes(4, "carrier-del")
	require.NoError(t, engine.BulkCreateNodes(nodes))
	edges := bulkTestEdges(4, "carrier-del-e", nodes)
	require.NoError(t, engine.BulkCreateEdges(edges))

	t.Run("edges: missing, repeated and empty IDs", func(t *testing.T) {
		require.NoError(t, engine.BulkDeleteEdges([]EdgeID{edges[0].ID, "test:carrier-no-such-edge", edges[0].ID, "", edges[1].ID}))
		requireBulkCounts(t, base, 4, 2)
		require.NoError(t, engine.BulkDeleteEdges(nil))
		requireBulkCounts(t, base, 4, 2)
	})
	t.Run("nodes: missing, repeated and empty IDs", func(t *testing.T) {
		require.NoError(t, engine.BulkDeleteNodes([]NodeID{nodes[0].ID, "test:carrier-no-such-node", nodes[0].ID, "", nodes[1].ID}))
		requireBulkCounts(t, base, 2, 1)
		require.NoError(t, engine.BulkDeleteNodes(nil))
		requireBulkCounts(t, base, 2, 1)
	})
	t.Run("a bulk call that fails deletes nothing", func(t *testing.T) {
		remaining := []NodeID{nodes[2].ID, nodes[3].ID}
		// The public path rejects a call spanning two databases before writing.
		require.Error(t, engine.BulkDeleteNodes([]NodeID{remaining[0], "other:carrier-node"}))
		require.Error(t, engine.BulkDeleteEdges([]EdgeID{edges[2].ID, "other:carrier-edge"}))
		requireBulkCounts(t, base, 2, 1)

		// Below the WAL, the carrier hands the call to the native owner's one
		// transaction: a failure on the second ID rolls back the first.
		carrier, err := EngineWithVectorWorkOrigin(base, CaptureVectorWorkOrigin())
		require.NoError(t, err)
		require.Error(t, carrier.BulkDeleteNodes([]NodeID{remaining[0], "other:carrier-node"}))
		require.Error(t, carrier.BulkDeleteEdges([]EdgeID{edges[2].ID, "other:carrier-edge"}))
		requireBulkCounts(t, base, 2, 1)
		for _, id := range remaining {
			_, err := base.GetNode(id)
			require.NoError(t, err, "a failed bulk deletion leaves %s", id)
		}
	})
}
