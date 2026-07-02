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
