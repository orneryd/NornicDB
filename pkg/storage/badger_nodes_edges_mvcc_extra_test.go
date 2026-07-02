package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_WriteEmbeddingChunksBatched_OversizedChunk(t *testing.T) {
	engine := newTestEngine(t)

	// Oversized payload should be sharded instead of failing on Badger's value-size guardrail.
	huge := make([]float32, 300_000)
	const nodeID = NodeID("test:huge")
	_, err := engine.CreateNode(&Node{
		ID:     nodeID,
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"path": "/tmp/huge.md",
		},
	})
	require.NoError(t, err)
	err = engine.UpdateNodeEmbedding(&Node{
		ID:              nodeID,
		ChunkEmbeddings: [][]float32{huge},
	})
	require.NoError(t, err)

	got, err := engine.GetNode(nodeID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 1)
	require.Len(t, got.ChunkEmbeddings[0], len(huge))
}

func TestBadgerEngine_DeleteNodeInTxn_NotFoundStillCleansEmbeddingChunks(t *testing.T) {
	engine := newTestEngine(t)
	id := NodeID("test:ghost")

	err := engine.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(embeddingKey(id, 0), []byte{1, 2, 3}); err != nil {
			return err
		}
		if err := txn.Set(embeddingKey(id, 1), []byte{4, 5, 6}); err != nil {
			return err
		}
		_, _, _, _, err := engine.deleteNodeInTxn(txn, id)
		return err
	})
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = embeddingPrefix(id)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			t.Fatalf("embedding chunk still present: %q", it.Item().Key())
		}
		return nil
	}))
}

func TestBadgerEngine_DeleteNodeInTxn_ErrorBranches(t *testing.T) {
	t.Run("read-only txn fails deleting embedding chunks", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(embeddingKey("test:embed-node", 0), []byte{1, 2, 3})
		}))
		readTxn := engine.db.NewTransaction(false)
		defer readTxn.Discard()
		_, _, _, _, err := engine.deleteNodeInTxn(readTxn, "test:embed-node")
		require.ErrorContains(t, err, "failed to delete embedding chunk")
	})

	t.Run("corrupt node body bubbles decode error", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(nodeKey("test:corrupt-node"), []byte("bad-node"))
		}))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, _, _, _, err := engine.deleteNodeInTxn(txn, "test:corrupt-node")
			require.Error(t, err)
			return nil
		}))
	})

	t.Run("read-only txn fails archiving current body", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:archive-node", Labels: []string{"N"}})
		require.NoError(t, err)
		readTxn := engine.db.NewTransaction(false)
		defer readTxn.Discard()
		_, _, _, _, err = engine.deleteNodeInTxn(readTxn, "test:archive-node")
		require.Error(t, err)
	})

	t.Run("corrupt mvcc head bubbles out", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:bad-head-node", Labels: []string{"N"}})
		require.NoError(t, err)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			nodeNum, ok := engine.idDict.lookupNodeNumID("test:bad-head-node")
			require.True(t, ok)
			return txn.Set(mvccNodeHeadKey(nodeNum), []byte("bad-head"))
		}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			_, _, _, _, err := engine.deleteNodeInTxn(txn, "test:bad-head-node")
			require.Error(t, err)
			return nil
		}))
	})

	t.Run("read-only txn fails deleting label indexes after tombstoned head", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:label-delete-node", Labels: []string{"N"}})
		require.NoError(t, err)
		head, err := engine.GetNodeCurrentHead("test:label-delete-node")
		require.NoError(t, err)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return engine.writeNodeMVCCHeadWithFloorInTxn(txn, "test:label-delete-node", head.Version, true, head.FloorVersion)
		}))
		readTxn := engine.db.NewTransaction(false)
		defer readTxn.Discard()
		_, _, _, _, err = engine.deleteNodeInTxn(readTxn, "test:label-delete-node")
		require.Error(t, err)
	})

	t.Run("read-only txn fails deleting primary node after tombstoned head", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:primary-delete-node"})
		require.NoError(t, err)
		head, err := engine.GetNodeCurrentHead("test:primary-delete-node")
		require.NoError(t, err)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return engine.writeNodeMVCCHeadWithFloorInTxn(txn, "test:primary-delete-node", head.Version, true, head.FloorVersion)
		}))
		readTxn := engine.db.NewTransaction(false)
		defer readTxn.Discard()
		_, _, _, _, err = engine.deleteNodeInTxn(readTxn, "test:primary-delete-node")
		require.Error(t, err)
	})

	t.Run("outgoing edge delete helper error bubbles out", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:out-mid", "test:out-end"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:out-edge", StartNode: "test:out-mid", EndNode: "test:out-end", Type: "R"}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey("test:out-edge"), []byte("bad-edge"))
		}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			_, _, _, _, err := engine.deleteNodeInTxn(txn, "test:out-mid")
			require.Error(t, err)
			return nil
		}))
	})

	t.Run("incoming edge delete helper error bubbles out", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:in-start", "test:in-mid"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:in-edge", StartNode: "test:in-start", EndNode: "test:in-mid", Type: "R"}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey("test:in-edge"), []byte("bad-edge"))
		}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			_, _, _, _, err := engine.deleteNodeInTxn(txn, "test:in-mid")
			require.Error(t, err)
			return nil
		}))
	})
}

func TestBadgerEngine_DeleteEdgeFamily_AdjacencyLoadErrors(t *testing.T) {
	t.Run("DeleteEdge propagates corrupt edge decode", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:de-a", "test:de-b"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:de-edge", StartNode: "test:de-a", EndNode: "test:de-b", Type: "R"}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey("test:de-edge"), []byte("bad-edge"))
		}))
		err := engine.DeleteEdge("test:de-edge")
		require.Error(t, err)
	})

	t.Run("BulkDeleteEdges propagates corrupt edge decode", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:bde-a", "test:bde-b"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:bde-edge", StartNode: "test:bde-a", EndNode: "test:bde-b", Type: "R"}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey("test:bde-edge"), []byte("bad-edge"))
		}))
		err := engine.BulkDeleteEdges([]EdgeID{"test:bde-edge"})
		require.Error(t, err)
	})

	t.Run("deleteEdgesWithPrefix propagates adjacency edge load errors", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:dp-a", "test:dp-b"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:dp-edge", StartNode: "test:dp-a", EndNode: "test:dp-b", Type: "R"}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey("test:dp-edge"), []byte("bad-edge"))
		}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			prefix := engine.outgoingIndexPrefixString("test:dp-a")
			require.NotNil(t, prefix)
			_, _, _, err := engine.deleteEdgesWithPrefix(txn, prefix)
			require.Error(t, err)
			return nil
		}))
	})
}

func TestBadgerEngine_UpdateEdgeAndDeletePrefixHelpers_Branches(t *testing.T) {
	engine := newTestEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"N"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))

	// UpdateEdge endpoint-change branch that validates new endpoint existence.
	err = engine.UpdateEdge(&Edge{ID: "test:e1", StartNode: "test:missing", EndNode: "test:b", Type: "REL"})
	require.ErrorIs(t, err, ErrNotFound)

	// deleteEdgesWithPrefix: malformed index key should be skipped while valid edge deletes.
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		prefix := engine.outgoingIndexPrefixString("test:a")
		require.NotNil(t, prefix)
		if err := txn.Set(append(append([]byte{}, prefix...), []byte("bad")...), []byte{}); err != nil {
			return err
		}
		deleted, ids, _, delErr := engine.deleteEdgesWithPrefix(txn, prefix)
		if delErr != nil {
			return delErr
		}
		require.Equal(t, int64(1), deleted)
		require.Equal(t, []EdgeID{"test:e1"}, ids)
		return nil
	}))

	_, err = engine.GetEdge("test:e1")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestBadgerEngine_DeleteNodeInTxn_ReturnsDeletedEdgeBodiesForAdjacencyTombstones(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	for _, nodeID := range []NodeID{"test:left", "test:center", "test:right"} {
		_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:left-center", StartNode: "test:left", EndNode: "test:center", Type: "REL"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:center-right", StartNode: "test:center", EndNode: "test:right", Type: "REL"}))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		deletedCount, deletedIDs, deletedEdges, deletedNode, err := engine.deleteNodeInTxn(txn, "test:center")
		require.NoError(t, err)
		require.Equal(t, int64(2), deletedCount)
		require.Equal(t, NodeID("test:center"), deletedNode.ID)
		require.Len(t, deletedIDs, 2)
		require.Len(t, deletedEdges, 2)
		seen := map[EdgeID]*Edge{}
		for i, edgeID := range deletedIDs {
			seen[edgeID] = deletedEdges[i]
		}
		require.Contains(t, seen, EdgeID("test:left-center"))
		require.Contains(t, seen, EdgeID("test:center-right"))
		require.NotNil(t, seen[EdgeID("test:left-center")])
		require.NotNil(t, seen[EdgeID("test:center-right")])
		require.Equal(t, NodeID("test:left"), seen[EdgeID("test:left-center")].StartNode)
		require.Equal(t, NodeID("test:right"), seen[EdgeID("test:center-right")].EndNode)
		return nil
	}))
}

func TestBadgerEngine_BulkDeleteEdges_MixedIDsStillWriteAdjacencyTombstones(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	for _, nodeID := range []NodeID{"test:bulk-start", "test:bulk-end"} {
		_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Node"}})
		require.NoError(t, err)
	}
	edgeID := EdgeID("test:bulk-edge")
	require.NoError(t, engine.CreateEdge(&Edge{ID: edgeID, StartNode: "test:bulk-start", EndNode: "test:bulk-end", Type: "REL"}))

	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{"", "test:missing", edgeID}))
	_, err = engine.GetEdge(edgeID)
	require.ErrorIs(t, err, ErrNotFound)

	outPrefix := engine.mvccOutgoingAdjacencyPrefixString("test:bulk-start")
	inPrefix := engine.mvccIncomingAdjacencyPrefixString("test:bulk-end")
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, outPrefix))
	require.Equal(t, 2, countAdjacencyMVCCVersions(t, engine, inPrefix))
}

func TestBadgerEngine_MVCCIteratorsAndVisibilityExtraBranches(t *testing.T) {
	t.Run("node head decode error", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Doc"}, Properties: map[string]any{"v": 1}})
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			num, ok := engine.idDict.lookupNodeNumID("test:n1")
			require.True(t, ok)
			return txn.Set(mvccNodeHeadKey(num), []byte("corrupt-head"))
		}))

		err = engine.withView(func(txn *badger.Txn) error {
			return engine.iterateNodesVisibleAtInTxn(txn, MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 999}, func(*Node) error {
				return nil
			})
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "msgpack")
	})

	t.Run("yield error and floor skip", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"Doc"}, Properties: map[string]any{"v": 1}})
		require.NoError(t, err)

		old := MVCCVersion{CommitTimestamp: time.Unix(0, 0).UTC(), CommitSequence: 0}
		nodes, err := engine.GetNodesByLabelVisibleAt("Doc", old)
		require.NoError(t, err)
		require.Empty(t, nodes)

		wantErr := errors.New("stop-yield")
		err = engine.withView(func(txn *badger.Txn) error {
			return engine.iterateNodesVisibleAtInTxn(txn, MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 999}, func(*Node) error {
				return wantErr
			})
		})
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("missing primary falls back to version record", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:n3", Labels: []string{"Doc"}, Properties: map[string]any{"v": "old"}})
		require.NoError(t, err)
		require.NoError(t, engine.UpdateNode(&Node{ID: "test:n3", Labels: []string{"Doc"}, Properties: map[string]any{"v": "new"}}))

		head, err := engine.GetNodeCurrentHead("test:n3")
		require.NoError(t, err)
		current, err := engine.GetNode("test:n3")
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			if err := engine.writeNodeMVCCVersionInTxn(txn, current, head.Version); err != nil {
				return err
			}
			return txn.Delete(nodeKey("test:n3"))
		}))

		node, err := engine.GetNodeVisibleAt("test:n3", head.Version)
		require.NoError(t, err)
		require.Equal(t, "new", node.Properties["v"])
	})

	t.Run("edge suppression and edge iteration filter", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		engine.SetDecayEnabled(true)

		_, err := engine.CreateNode(&Node{ID: "test:s1", Labels: []string{"Doc"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:s2", Labels: []string{"Doc"}})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:se", StartNode: "test:s1", EndNode: "test:s2", Type: "REL", VisibilitySuppressed: true}))

		head, err := engine.GetEdgeCurrentHead("test:se")
		require.NoError(t, err)

		_, err = engine.GetEdgeVisibleAt("test:se", head.Version)
		require.ErrorIs(t, err, ErrNotFound)

		edges, err := engine.GetEdgesByTypeVisibleAt("REL", head.Version)
		require.NoError(t, err)
		require.Empty(t, edges)
	})
}

func TestBadgerEngine_MaterializeMVCCCommitInTxn_AllOperationKinds(t *testing.T) {
	engine := createMVCCBadgerEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:m1", Labels: []string{"Doc"}, Properties: map[string]any{"v": 1}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:m2", Labels: []string{"Doc"}, Properties: map[string]any{"v": 2}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:m3", Labels: []string{"Doc"}, Properties: map[string]any{"v": 3}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:me1", StartNode: "test:m1", EndNode: "test:m2", Type: "REL", Properties: map[string]any{"w": 1}}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:me2", StartNode: "test:m2", EndNode: "test:m3", Type: "REL", Properties: map[string]any{"w": 2}}))

	oldN1, err := engine.GetNode("test:m1")
	require.NoError(t, err)
	oldN3, err := engine.GetNode("test:m3")
	require.NoError(t, err)
	oldE1, err := engine.GetEdge("test:me1")
	require.NoError(t, err)
	oldE2, err := engine.GetEdge("test:me2")
	require.NoError(t, err)

	version := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 40_001}
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		ops := []Operation{
			{Type: OpCreateNode, Node: &Node{ID: "test:newfresh", Labels: []string{"Doc"}}, FreshID: true},
			{Type: OpCreateNode, Node: &Node{ID: "test:newplain", Labels: []string{"Doc"}}, FreshID: false},
			{Type: OpUpdateNode, Node: &Node{ID: "test:m1", Labels: []string{"Doc"}, Properties: map[string]any{"v": 11}}, OldNode: oldN1},
			{Type: OpDeleteNode, NodeID: "test:m3", OldNode: oldN3, DeletedEdgeIDs: []EdgeID{"test:me2"}},
			{Type: OpCreateEdge, Edge: &Edge{ID: "test:efresh", StartNode: "test:m1", EndNode: "test:m2", Type: "REL"}, FreshID: true},
			{Type: OpCreateEdge, Edge: &Edge{ID: "test:eplain", StartNode: "test:m1", EndNode: "test:m2", Type: "REL"}, FreshID: false},
			{Type: OpUpdateEdge, Edge: &Edge{ID: "test:me1", StartNode: "test:m1", EndNode: "test:m2", Type: "REL2", Properties: map[string]any{"w": 11}}, OldEdge: oldE1},
			{Type: OpDeleteEdge, EdgeID: "test:me2", OldEdge: oldE2},
		}
		return engine.materializeMVCCCommitInTxn(txn, version, ops)
	}))

	_, err = engine.GetNodeCurrentHead("test:newfresh")
	require.NoError(t, err)
	_, err = engine.GetNodeCurrentHead("test:newplain")
	require.NoError(t, err)
	_, err = engine.GetEdgeCurrentHead("test:efresh")
	require.NoError(t, err)
	_, err = engine.GetEdgeCurrentHead("test:eplain")
	require.NoError(t, err)
}

func TestBadgerEngine_BulkCreateNodes_LargeEmbeddingChunk(t *testing.T) {
	engine := newTestEngine(t)
	huge := make([]float32, 300_000)

	err := engine.BulkCreateNodes([]*Node{
		{
			ID:              "test:bulk-huge",
			Labels:          []string{"Doc"},
			ChunkEmbeddings: [][]float32{huge},
			Properties:      map[string]any{"path": "/tmp/bulk-huge.md"},
		},
	})
	require.NoError(t, err)

	got, err := engine.GetNode("test:bulk-huge")
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 1)
	require.Len(t, got.ChunkEmbeddings[0], len(huge))
}

func TestBadgerEngine_MVCCAdjacencyHelperBranches(t *testing.T) {
	version := MVCCVersion{CommitTimestamp: time.Unix(123, 456).UTC(), CommitSequence: 7}

	record, err := decodeMVCCAdjacencyRecord(encodeMVCCAdjacencyRecord(true))
	require.NoError(t, err)
	require.True(t, record.Tombstoned)

	record, err = decodeMVCCAdjacencyRecord(encodeMVCCAdjacencyRecord(false))
	require.NoError(t, err)
	require.False(t, record.Tombstoned)

	_, err = decodeMVCCAdjacencyRecord(nil)
	require.ErrorContains(t, err, "invalid mvcc adjacency record length")

	logical, gotVersion, err := extractMVCCLogicalKeyAndVersion(mvccOutgoingAdjacencyKey(11, 22, version))
	require.NoError(t, err)
	require.Equal(t, mvccOutgoingAdjacencyKey(11, 22, version)[:17], logical)
	require.Equal(t, version, gotVersion)

	_, _, err = extractMVCCLogicalKeyAndVersion([]byte{prefixMVCCOutgoingAdj})
	require.ErrorContains(t, err, "invalid mvcc key length")

	edgeNum, gotVersion, err := extractEdgeNumIDAndMVCCVersionFromAdjacencyKey(mvccIncomingAdjacencyKey(11, 22, version))
	require.NoError(t, err)
	require.Equal(t, uint64(22), edgeNum)
	require.Equal(t, version, gotVersion)

	_, _, err = extractEdgeNumIDAndMVCCVersionFromAdjacencyKey([]byte{prefixMVCCIncomingAdj})
	require.ErrorContains(t, err, "invalid mvcc adjacency key length")

	engine := createMVCCBadgerEngine(t)
	require.Nil(t, engine.mvccOutgoingAdjacencyPrefixString("test:missing-node"))
	require.Nil(t, engine.mvccIncomingAdjacencyPrefixString("test:missing-node"))

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		outKey, err := engine.mvccOutgoingAdjacencyKeyString(txn, "test:n1", "test:e1", version)
		require.NoError(t, err)
		require.Equal(t, byte(prefixMVCCOutgoingAdj), outKey[0])

		inKey, err := engine.mvccIncomingAdjacencyKeyString(txn, "test:n1", "test:e1", version)
		require.NoError(t, err)
		require.Equal(t, byte(prefixMVCCIncomingAdj), inKey[0])
		return nil
	}))

	require.NotNil(t, engine.mvccOutgoingAdjacencyPrefixString("test:n1"))
	require.NotNil(t, engine.mvccIncomingAdjacencyPrefixString("test:n1"))
}

func TestBadgerEngine_MVCCAdjacencyTxnErrorBranches(t *testing.T) {
	engine := createMVCCBadgerEngine(t)
	version := MVCCVersion{CommitTimestamp: time.Unix(456, 789).UTC(), CommitSequence: 9}
	edge := &Edge{ID: "test:txn-edge", StartNode: "test:txn-start", EndNode: "test:txn-end", Type: "REL"}

	txn := engine.db.NewTransaction(true)
	txn.Discard()

	_, err := engine.mvccOutgoingAdjacencyKeyString(txn, edge.StartNode, edge.ID, version)
	require.Error(t, err)
	_, err = engine.mvccIncomingAdjacencyKeyString(txn, edge.EndNode, edge.ID, version)
	require.Error(t, err)
	require.Error(t, engine.writeOutgoingAdjacencyMVCCVersionInTxn(txn, edge.StartNode, edge.ID, version, false))
	require.Error(t, engine.writeIncomingAdjacencyMVCCVersionInTxn(txn, edge.EndNode, edge.ID, version, false))
	require.Error(t, engine.writeEdgeAdjacencyLiveInTxn(txn, edge, version))
	require.Error(t, engine.writeEdgeAdjacencyTombstoneInTxn(txn, edge, version))
	require.Error(t, engine.writeEdgeAdjacencyDeltaInTxn(txn, edge, nil, version))
	require.Error(t, engine.writeEdgeAdjacencyDeltaInTxn(txn, nil, edge, version))
}

func TestBadgerEngine_LoadEdgeForAdjacencyTombstone_ErrorBranches(t *testing.T) {
	t.Run("corrupt primary body returns decode error", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return txn.Set(edgeKey("test:bad-edge"), []byte("bad-edge-payload"))
		}))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := engine.loadEdgeForAdjacencyTombstoneInTxn(txn, "test:bad-edge")
			require.Error(t, err)
			return nil
		}))
	})

	t.Run("corrupt mvcc head returns error after primary missing", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:head-start", "test:head-end"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Node"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:head-edge", StartNode: "test:head-start", EndNode: "test:head-end", Type: "REL"}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			headKey, err := engine.mvccEdgeHeadKeyString(txn, "test:head-edge")
			if err != nil {
				return err
			}
			if err := txn.Delete(edgeKey("test:head-edge")); err != nil {
				return err
			}
			return txn.Set(headKey, []byte("bad-head"))
		}))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := engine.loadEdgeForAdjacencyTombstoneInTxn(txn, "test:head-edge")
			require.Error(t, err)
			return nil
		}))
	})
}

func TestBadgerEngine_MVCCAdjacencyVisibilityExtraBranches(t *testing.T) {
	t.Run("invalid ids and nil adjacency writes", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			require.NoError(t, engine.writeEdgeAdjacencyLiveInTxn(txn, nil, MVCCVersion{}))
			require.NoError(t, engine.writeEdgeAdjacencyTombstoneInTxn(txn, nil, MVCCVersion{}))
			return nil
		}))

		_, err := engine.GetOutgoingEdgesVisibleAt("", MVCCVersion{})
		require.ErrorIs(t, err, ErrInvalidID)
		_, err = engine.GetIncomingEdgesVisibleAt("", MVCCVersion{})
		require.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("public visible adjacency surfaces malformed keys", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:bad-out", Labels: []string{"Node"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:bad-in", Labels: []string{"Node"}})
		require.NoError(t, err)

		outPrefix := engine.mvccOutgoingAdjacencyPrefixString("test:bad-out")
		inPrefix := engine.mvccIncomingAdjacencyPrefixString("test:bad-in")
		require.NotNil(t, outPrefix)
		require.NotNil(t, inPrefix)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			if err := txn.Set(append(append([]byte{}, outPrefix...), []byte("bad")...), []byte{0}); err != nil {
				return err
			}
			return txn.Set(append(append([]byte{}, inPrefix...), []byte("bad")...), []byte{0})
		}))

		_, err = engine.GetOutgoingEdgesVisibleAt("test:bad-out", maxMVCCVersion())
		require.ErrorContains(t, err, "invalid mvcc adjacency key length")
		_, err = engine.GetIncomingEdgesVisibleAt("test:bad-in", maxMVCCVersion())
		require.ErrorContains(t, err, "invalid mvcc adjacency key length")
	})

	t.Run("archived edge fallback and mismatched adjacency filter", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:a", "test:b", "test:c", "test:ghost"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Node"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"}))
		head, err := engine.GetEdgeCurrentHead("test:e1")
		require.NoError(t, err)
		current, err := engine.GetEdge("test:e1")
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			require.NoError(t, engine.writeEdgeMVCCVersionInTxn(txn, current, head.Version))
			require.NoError(t, txn.Delete(edgeKey("test:e1")))

			loaded, err := engine.loadEdgeForAdjacencyTombstoneInTxn(txn, "test:e1")
			require.NoError(t, err)
			require.Equal(t, current.StartNode, loaded.StartNode)

			visible, err := engine.getEdgeVisibleAtInTxn(txn, "test:e1", head.Version)
			require.NoError(t, err)
			require.Equal(t, current.EndNode, visible.EndNode)
			return nil
		}))

		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:b", EndNode: "test:c", Type: "REL"}))
		head2, err := engine.GetEdgeCurrentHead("test:e2")
		require.NoError(t, err)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return engine.writeOutgoingAdjacencyMVCCVersionInTxn(txn, "test:ghost", "test:e2", head2.Version, false)
		}))
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return engine.writeIncomingAdjacencyMVCCVersionInTxn(txn, "test:ghost", "test:e2", head2.Version, false)
		}))

		outgoing, err := engine.GetOutgoingEdgesVisibleAt("test:ghost", maxMVCCVersion())
		require.NoError(t, err)
		require.Empty(t, outgoing)
		incoming, err := engine.GetIncomingEdgesVisibleAt("test:ghost", maxMVCCVersion())
		require.NoError(t, err)
		require.Empty(t, incoming)

		require.NoError(t, engine.DeleteEdge("test:e2"))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := engine.loadEdgeForAdjacencyTombstoneInTxn(txn, "test:e2")
			require.ErrorIs(t, err, ErrNotFound)
			return nil
		}))
	})

	t.Run("malformed adjacency key surfaces decode error", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:malformed", Labels: []string{"Node"}})
		require.NoError(t, err)
		prefix := engine.mvccOutgoingAdjacencyPrefixString("test:malformed")
		require.NotNil(t, prefix)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			badKey := append(append([]byte{}, prefix...), []byte("bad")...)
			return txn.Set(badKey, []byte{0})
		}))

		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := engine.collectVisibleAdjacencyEdgeIDsInTxn(txn, prefix, maxMVCCVersion())
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid mvcc adjacency key length")
			return nil
		}))
	})

	t.Run("direct adjacency writes and edge visibility helpers", func(t *testing.T) {
		engine := createMVCCBadgerEngine(t)
		for _, nodeID := range []NodeID{"test:direct-a", "test:direct-b"} {
			_, err := engine.CreateNode(&Node{ID: nodeID, Labels: []string{"Node"}})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:direct-edge", StartNode: "test:direct-a", EndNode: "test:direct-b", Type: "REL"}))
		head, err := engine.GetEdgeCurrentHead("test:direct-edge")
		require.NoError(t, err)
		current, err := engine.GetEdge("test:direct-edge")
		require.NoError(t, err)

		later := MVCCVersion{CommitTimestamp: head.Version.CommitTimestamp.Add(time.Millisecond), CommitSequence: head.Version.CommitSequence + 1}
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			if _, err := engine.mvccOutgoingAdjacencyKeyString(txn, current.StartNode, current.ID, head.Version); err != nil {
				return err
			}
			if _, err := engine.mvccIncomingAdjacencyKeyString(txn, current.EndNode, current.ID, head.Version); err != nil {
				return err
			}
			if err := engine.writeEdgeAdjacencyLiveInTxn(txn, current, head.Version); err != nil {
				return err
			}
			if err := engine.writeEdgeAdjacencyTombstoneInTxn(txn, current, later); err != nil {
				return err
			}
			loaded, err := engine.loadEdgeForAdjacencyTombstoneInTxn(txn, current.ID)
			if err != nil {
				return err
			}
			require.Equal(t, current.StartNode, loaded.StartNode)
			visible, err := engine.getEdgeVisibleAtInTxn(txn, current.ID, head.Version)
			if err != nil {
				return err
			}
			require.Equal(t, current.EndNode, visible.EndNode)
			return nil
		}))

		require.NoError(t, engine.DeleteEdge(current.ID))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			_, err := engine.getEdgeVisibleAtInTxn(txn, current.ID, maxMVCCVersion())
			require.ErrorIs(t, err, ErrNotFound)
			return nil
		}))
	})
}

func TestBadgerEngine_DeleteNode_WithPropertiesCascadesAdjacencyAndSchemaCleanup(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&Node{ID: "test:owner", Labels: []string{"Person"}, Properties: map[string]any{"email": "owner@example.com"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:peer", Labels: []string{"Person"}, Properties: map[string]any{"email": "peer@example.com"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:owner-peer", StartNode: "test:owner", EndNode: "test:peer", Type: "KNOWS"}))

	require.NoError(t, engine.DeleteNode("test:owner"))
	_, err = engine.GetNode("test:owner")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge("test:owner-peer")
	require.ErrorIs(t, err, ErrNotFound)
}
