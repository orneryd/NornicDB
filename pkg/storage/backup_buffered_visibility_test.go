package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// requirePhysicalKeyState reads the raw Badger keys, bypassing every engine
// cache and the write-behind overlay, so the test can state what storage
// physically holds at that moment.
func requirePhysicalKeyState(t *testing.T, engine *BadgerEngine, present bool, keys ...[]byte) {
	t.Helper()
	for _, key := range keys {
		err := engine.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			return err
		})
		if present {
			require.NoError(t, err, "physical key %q", key)
			continue
		}
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "physical key %q", key)
	}
}

// TestNativeBackupIncludesAcknowledgedBufferedWrites proves that the
// context-free exported BadgerEngine.Backup includes every write the engine
// acknowledged before it, including node, relationship and property writes
// that are still unapplied in the write-behind buffer. The buffer's
// background replay is an hour away, so only Backup's own preflush can make
// those writes part of the snapshot; the backup is then restored through the
// exported Restore and the full graph, bodies and counts are compared.
func TestNativeBackupIncludesAcknowledgedBufferedWrites(t *testing.T) {
	source, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{
		DataDir:             t.TempDir(),
		WriteBehind:         true,
		WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)

	// Acknowledged commits: two nodes and one relationship with properties,
	// then a later commit that rewrites properties on both the node and the
	// relationship.
	bufferedWrite(t, source, func(tx *BadgerTransaction) error {
		if _, err := tx.CreateNode(t.Context(), &Node{
			ID:         "test:anchor",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"name": "anchor", "revision": int64(1)},
		}); err != nil {
			return err
		}
		if _, err := tx.CreateNode(t.Context(), &Node{
			ID:         "test:peer",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"name": "peer", "weight": int64(2)},
		}); err != nil {
			return err
		}
		return tx.CreateEdge(t.Context(), &Edge{
			ID:         "test:edge",
			StartNode:  "test:anchor",
			EndNode:    "test:peer",
			Type:       "LINK",
			Properties: map[string]any{"since": int64(2024), "durable": false},
		})
	})
	bufferedWrite(t, source, func(tx *BadgerTransaction) error {
		anchor, err := tx.GetNode(t.Context(), "test:anchor")
		if err != nil {
			return err
		}
		anchor.Properties["revision"] = int64(2)
		anchor.Properties["note"] = "acknowledged"
		if err := tx.UpdateGraphNode(t.Context(), NodeUpdateRequest{After: anchor, Policy: ClientNodeEdits}); err != nil {
			return err
		}
		edge, err := tx.GetEdge(t.Context(), "test:edge")
		if err != nil {
			return err
		}
		edge.Properties["since"] = int64(2025)
		edge.Properties["durable"] = true
		return tx.UpdateEdge(t.Context(), EdgeUpdateRequest{After: edge})
	})

	// Before the backup every write is acknowledged through the overlay and
	// still absent from Badger itself: the pending state is genuine, not an
	// artifact of the read path.
	require.Positive(t, source.writeBehind.PendingOps(), "the acknowledged commits must still be buffered")
	anchor, err := source.GetNode(t.Context(), "test:anchor")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"name": "anchor", "revision": int64(2), "note": "acknowledged"}, anchor.Properties)
	edge, err := source.GetEdge(t.Context(), "test:edge")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"since": int64(2025), "durable": true}, edge.Properties)
	requirePhysicalKeyState(t, source, false, nodeKey("test:anchor"), nodeKey("test:peer"), edgeKey("test:edge"))

	backup := filepath.Join(t.TempDir(), "buffered.backup")
	require.NoError(t, source.Backup(backup))

	restored, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	require.NoError(t, restored.Restore(backup))

	nodes, err := restored.AllNodes(context.Background())
	require.NoError(t, err)
	restoredNodes := make(map[NodeID]*Node, len(nodes))
	for _, node := range nodes {
		restoredNodes[node.ID] = node
	}
	require.Len(t, restoredNodes, 2, "every buffered node reaches the restored graph")
	require.Contains(t, restoredNodes, NodeID("test:anchor"))
	require.Contains(t, restoredNodes, NodeID("test:peer"))
	require.Equal(t, []string{"Doc"}, restoredNodes["test:anchor"].Labels)
	require.Equal(t, map[string]any{"name": "anchor", "revision": int64(2), "note": "acknowledged"}, restoredNodes["test:anchor"].Properties)
	require.Equal(t, []string{"Doc"}, restoredNodes["test:peer"].Labels)
	require.Equal(t, map[string]any{"name": "peer", "weight": int64(2)}, restoredNodes["test:peer"].Properties)

	edges, err := restored.AllEdges(context.Background())
	require.NoError(t, err)
	require.Len(t, edges, 1, "the buffered relationship reaches the restored graph")
	require.Equal(t, EdgeID("test:edge"), edges[0].ID)
	require.Equal(t, NodeID("test:anchor"), edges[0].StartNode)
	require.Equal(t, NodeID("test:peer"), edges[0].EndNode)
	require.Equal(t, "LINK", edges[0].Type)
	require.Equal(t, map[string]any{"since": int64(2025), "durable": true}, edges[0].Properties)

	nodeCount, err := restored.NodeCount(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(2), nodeCount)
	edgeCount, err := restored.EdgeCount(t.Context())
	require.NoError(t, err)
	require.Equal(t, int64(1), edgeCount)
	docCount, err := restored.NodeCountByLabel(t.Context(), "Doc")
	require.NoError(t, err)
	require.Equal(t, int64(2), docCount)
	linkCount, err := restored.EdgeCountByType(context.Background(), "LINK")
	require.NoError(t, err)
	require.Equal(t, int64(1), linkCount)

	// The actual physical outcome after Backup: its own preflush applied the
	// acknowledged commits before streaming, leaving the buffer empty.
	require.Zero(t, source.writeBehind.PendingOps(), "Backup's preflush must leave nothing buffered")
	requirePhysicalKeyState(t, source, true, nodeKey("test:anchor"), nodeKey("test:peer"), edgeKey("test:edge"))
}
