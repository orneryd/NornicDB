// Author: Codex, NOR-160 implementation sub-agent, 2026-10-26.
package storage

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func nor160SnapshotFixture(t *testing.T) (*MemoryEngine, *BadgerTransaction) {
	t.Helper()
	engine := newTestMemoryEngine(t)
	writer, err := engine.BeginTransaction()
	require.NoError(t, err)
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := writer.CreateNode(&Node{ID: id, Labels: []string{"Held"}, Properties: map[string]any{"value": int64(1), "complete": int64(42)}})
		require.NoError(t, err)
	}
	require.NoError(t, writer.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "COHELD", Properties: map[string]any{"value": int64(1)}}))
	require.NoError(t, writer.Commit())
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, reader.SetNamespace("test"))
	t.Cleanup(func() { _ = reader.Rollback() })
	return engine, reader
}

func TestNOR160SnapshotReadsOwnImagesAndPreservePendingWrites(t *testing.T) {
	engine, tx := nor160SnapshotFixture(t)
	// Prime point reads, then mutate each returned representation. Neither
	// another read nor the old image used by a subsequent write may change.
	nodeReads := []func() (*Node, error){
		func() (*Node, error) { return tx.GetQueryNode("test:a") },
		func() (*Node, error) { return tx.GetNode("test:a") },
		func() (*Node, error) { return tx.GetNodeProjected("test:a", []string{"value"}) },
		func() (*Node, error) { return tx.GetStoredNode("test:a") },
	}
	for pass := 0; pass < 2; pass++ {
		for _, read := range nodeReads {
			node, err := read()
			require.NoError(t, err)
			require.Equal(t, int64(1), node.Properties["value"])
			node.Properties["value"] = int64(99)
			node.Labels[0] = "Caller"
		}
	}
	edgeReads := []func() ([]*Edge, error){
		func() ([]*Edge, error) { edge, err := tx.GetEdge("test:e"); return []*Edge{edge}, err },
		func() ([]*Edge, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
		},
		func() ([]*Edge, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"})
		},
		func() ([]*Edge, error) { return tx.GetEdgesByType("COHELD") },
		func() ([]*Edge, error) { return tx.AllEdges() },
		func() ([]*Edge, error) { return tx.GetStoredOutgoingEdges("test:a") },
		func() ([]*Edge, error) { return tx.GetStoredIncomingEdges("test:b") },
	}
	for pass := 0; pass < 2; pass++ {
		for _, read := range edgeReads {
			edges, err := read()
			require.NoError(t, err)
			require.Len(t, edges, 1)
			require.Equal(t, int64(1), edges[0].Properties["value"])
			edges[0].Properties["value"] = int64(99)
		}
	}
	node, err := tx.GetNode("test:a")
	require.NoError(t, err)
	node.Properties["value"] = int64(2)
	require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
	edge, err := tx.GetEdge("test:e")
	require.NoError(t, err)
	edge.Properties["value"] = int64(2)
	require.NoError(t, tx.UpdateEdge(edge))
	for _, read := range nodeReads {
		node, err := read()
		require.NoError(t, err)
		require.Equal(t, int64(2), node.Properties["value"])
	}
	for _, read := range edgeReads {
		edges, err := read()
		require.NoError(t, err)
		require.Len(t, edges, 1)
		require.Equal(t, int64(2), edges[0].Properties["value"])
	}
	require.NoError(t, tx.DeleteEdge("test:e"))
	_, err = tx.GetEdge("test:e")
	require.ErrorIs(t, err, ErrNotFound)
	for _, read := range edgeReads[1:] {
		edges, err := read()
		require.NoError(t, err)
		require.Empty(t, edges)
	}
	require.NoError(t, tx.Commit())
	stored, err := engine.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(2), stored.Properties["value"])
}

func TestNOR160SnapshotReadsPreserveIsolationAndWriteConflicts(t *testing.T) {
	engine, tx := nor160SnapshotFixture(t)
	oldNode, err := tx.GetQueryNode("test:a")
	require.NoError(t, err)
	oldEdge, err := tx.GetEdge("test:e")
	require.NoError(t, err)
	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	peerNode, err := peer.GetNode("test:a")
	require.NoError(t, err)
	peerNode.Properties["value"] = int64(3)
	require.NoError(t, peer.UpdateGraphNode(peerNode, ClientNodeEdits))
	peerEdge, err := peer.GetEdge("test:e")
	require.NoError(t, err)
	peerEdge.Properties["value"] = int64(3)
	require.NoError(t, peer.UpdateEdge(peerEdge))
	require.NoError(t, peer.Commit())
	gotNode, err := tx.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, oldNode, gotNode)
	gotEdge, err := tx.GetEdge("test:e")
	require.NoError(t, err)
	require.Equal(t, oldEdge, gotEdge)
	oldNode.Properties["value"] = int64(4)
	require.NoError(t, tx.UpdateGraphNode(oldNode, ClientNodeEdits))
	// A retained committed body must never suppress commit-time validation.
	require.ErrorIs(t, tx.Commit(), ErrConflict)
}

func TestNOR160SnapshotReadsApplyLiveSuppression(t *testing.T) {
	for _, primed := range []bool{false, true} {
		name := "cold"
		if primed {
			name = "primed"
		}
		t.Run(name, func(t *testing.T) { nor160CheckSnapshotSuppression(t, primed) })
	}
}

func nor160CheckSnapshotSuppression(t *testing.T, primed bool) {
	t.Helper()
	engine, tx := nor160SnapshotFixture(t)
	if primed {
		_, err := tx.GetQueryNode("test:a")
		require.NoError(t, err)
		_, err = tx.GetEdge("test:e")
		require.NoError(t, err)
		_, err = tx.GetNodeProjected("test:a", []string{"value"})
		require.NoError(t, err)
		_, err = tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
		require.NoError(t, err)
		_, err = tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"})
		require.NoError(t, err)
	}
	require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:a", true))
	require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:e", true))
	_, err := tx.GetNode("test:a")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = tx.GetNodeProjected("test:a", []string{"value"})
	require.ErrorIs(t, err, ErrNotFound)
	visible, answered := tx.RelationshipEndpointVisible("test:a")
	require.False(t, visible && answered)
	_, err = tx.GetEdge("test:e")
	require.ErrorIs(t, err, ErrNotFound)
	storedNode, err := tx.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(1), storedNode.Properties["value"])
	storedEdge, err := tx.GetStoredEdge("test:e")
	require.NoError(t, err)
	require.Equal(t, int64(1), storedEdge.Properties["value"])
	// Even a stored read's retained hidden image must pass the visible-read
	// policy again, including through either cached adjacency direction.
	_, err = tx.GetEdge("test:e")
	require.ErrorIs(t, err, ErrNotFound)
	for _, read := range []func() ([]*Edge, error){
		func() ([]*Edge, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
		},
		func() ([]*Edge, error) {
			return tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"})
		},
	} {
		edges, err := read()
		require.NoError(t, err)
		require.Empty(t, edges)
	}
	require.NoError(t, engine.SetManualSuppression(StoredEntityNodeKind, "test:a", false))
	require.NoError(t, engine.SetManualSuppression(StoredEntityEdgeKind, "test:e", false))
	engine.SetDecayEnabled(true)
	defer engine.SetDecayEnabled(false)
	projected, err := tx.GetNodeProjected("test:a", []string{"value"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": int64(1)}, projected.Properties)
	visibleEdge, err := tx.GetEdge("test:e")
	require.NoError(t, err)
	require.Equal(t, int64(1), visibleEdge.Properties["value"])
	outgoing, err := tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	incoming, err := tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"})
	require.NoError(t, err)
	require.Len(t, incoming, 1)
}

func TestNOR160SnapshotProjectionOwnsKnownPropertyMask(t *testing.T) {
	_, tx := nor160SnapshotFixture(t)
	properties := []string{"value"}
	first, err := tx.GetNodeProjected("test:a", properties)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": int64(1)}, first.Properties)
	properties[0] = "complete"
	second, err := tx.GetNodeProjected("test:a", properties)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"complete": int64(42)}, second.Properties)
	full, err := tx.GetNodeProjected("test:a", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": int64(1), "complete": int64(42)}, full.Properties)
}

func TestNOR160SnapshotReadersAndIteratorsDoNotShareCallerMaps(t *testing.T) {
	_, tx := nor160SnapshotFixture(t)
	_, err := tx.GetQueryNode("test:a")
	require.NoError(t, err)
	_, err = tx.GetEdge("test:e")
	require.NoError(t, err)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for pass := 0; pass < 20; pass++ {
				node, err := tx.GetQueryNode("test:a")
				if err != nil {
					t.Error(err)
					return
				}
				node.Properties["value"] = int64(99)
				err = tx.StreamEdgesByType(context.Background(), "COHELD", func(edge *Edge) error {
					require.Equal(t, int64(1), edge.Properties["value"])
					edge.Properties["value"] = int64(99)
					return nil
				})
				require.NoError(t, err)
			}
		}()
	}
	workers.Wait()
}

func TestNOR160PartialSnapshotReadAcrossConcurrentCommit(t *testing.T) {
	engine, tx := nor160SnapshotFixture(t)
	partial, err := tx.GetNodeProjected("test:a", []string{"value"})
	require.NoError(t, err)
	require.NotContains(t, partial.Properties, "complete")
	peer, err := engine.BeginTransaction()
	require.NoError(t, err)
	updated, err := peer.GetNode("test:a")
	require.NoError(t, err)
	updated.Properties["value"], updated.Properties["complete"] = int64(3), int64(43)
	require.NoError(t, peer.UpdateGraphNode(updated, ClientNodeEdits))
	start := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		<-start
		done <- peer.Commit()
	}()
	close(start)
	for pass := 0; pass < 30; pass++ {
		partial, err := tx.GetNodeProjected("test:a", []string{"value"})
		require.NoError(t, err)
		require.Equal(t, map[string]any{"value": int64(1)}, partial.Properties)
	}
	require.NoError(t, <-done)
	// The old reader retains its complete old snapshot. Read quality must
	// force a canonical full read rather than promote the partial map.
	full, err := tx.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": int64(1), "complete": int64(42)}, full.Properties)
	// Reading the old pinned image must not make it visible through either
	// current reader, including after the full-read hydration upgrade.
	current, err := engine.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": int64(3), "complete": int64(43)}, current.Properties)
	fresh, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = fresh.Rollback() })
	current, err = fresh.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"value": int64(3), "complete": int64(43)}, current.Properties)
}

func TestNOR160SnapshotReadQualityPreservesProjectionAndEmbeddingHydration(t *testing.T) {
	engine := newTestMemoryEngine(t)
	vector := [][]float32{{1, 2, 3}}
	_, err := engine.CreateNode(&Node{ID: "test:embedded", Labels: []string{"Embedded"}, Properties: map[string]any{"value": int64(1), "complete": int64(42)}, ChunkEmbeddings: vector, EmbedMeta: map[string]any{"chunk_count": int64(1)}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	for pass := 0; pass < 2; pass++ {
		projected, err := tx.GetNodeProjected("test:embedded", []string{"value"})
		require.NoError(t, err)
		require.Equal(t, map[string]any{"value": int64(1)}, projected.Properties)
		metadata, err := tx.GetQueryNode("test:embedded")
		require.NoError(t, err)
		require.Equal(t, int64(42), metadata.Properties["complete"])
		require.NotEmpty(t, metadata.EmbedMeta)
		full, err := tx.GetNode("test:embedded")
		require.NoError(t, err)
		require.Equal(t, vector, full.ChunkEmbeddings)
		require.Equal(t, int64(42), full.Properties["complete"])
		full.ChunkEmbeddings[0][0] = 99
		full.Properties["value"] = int64(99)
		again, err := tx.GetNode("test:embedded")
		require.NoError(t, err)
		require.Equal(t, vector, again.ChunkEmbeddings)
		require.Equal(t, int64(1), again.Properties["value"])
		projected, err = tx.GetNodeProjected("test:embedded", []string{})
		require.NoError(t, err)
		require.Empty(t, projected.Properties)
		require.Empty(t, projected.ChunkEmbeddings)
	}
}
