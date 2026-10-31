package storage

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerStats_RefreshPendingAndFindNodeBranchCoverage(t *testing.T) {
	engine := newTestEngine(t)
	engine.SetEmbeddingsEnabled(true)

	_, err := engine.CreateNode(&Node{ID: "test:add", Labels: []string{"Doc"}, Properties: map[string]any{"title": "needs embedding"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:embedded", Labels: []string{"Doc"}, Properties: map[string]any{"title": "already embedded"}, ChunkEmbeddings: [][]float32{{0.1, 0.2}}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:internal", Labels: []string{"_Internal"}, Properties: map[string]any{"title": "skip"}})
	require.NoError(t, err)
	engine.MarkNodeEmbedded("test:add")

	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		// First-pass pending index cleanup branches.
		require.NoError(t, txn.Set([]byte{prefixPendingEmbed}, []byte{})) // len(key)<=1
		require.NoError(t, txn.Set(pendingEmbedKey("system:idx"), []byte{}))
		require.NoError(t, txn.Set(pendingEmbedKey("test:missing"), []byte{}))
		require.NoError(t, txn.Set(pendingEmbedKey("test:bad"), []byte{}))
		require.NoError(t, txn.Set(nodeKey("test:bad"), []byte("not-a-node")))  // decode error branch
		require.NoError(t, txn.Set(pendingEmbedKey("test:embedded"), []byte{})) // already embedded branch
		// Force txn.Get(nodeKey(nodeID)) unexpected error branch in pending-index readers.
		require.NoError(t, txn.Set([]byte{prefixPendingEmbed, 'b', 0x00, 'x'}, []byte{}))

		// Second-pass node scan branches.
		require.NoError(t, txn.Set([]byte{prefixNode}, []byte{}))             // len(key)<=1
		require.NoError(t, txn.Set(nodeKey("test:broken-node"), []byte("x"))) // decode error branch
		return nil
	}))

	added := engine.RefreshPendingEmbeddingsIndex()
	require.GreaterOrEqual(t, added, 1)

	// The valid pending candidate should be discoverable.
	n := engine.FindNodeNeedingEmbedding()
	require.NotNil(t, n)
	require.Equal(t, NodeID("test:add"), n.ID)

	// Mark as embedded and ensure no candidate remains from seeded entries.
	engine.MarkNodeEmbedded("test:add")
	n = engine.FindNodeNeedingEmbedding()
	require.Nil(t, n)

	// InvalidatePendingEmbeddingsIndex is a documented no-op.
	engine.InvalidatePendingEmbeddingsIndex()
}

func TestBadgerStats_StreamNodesByPrefixAndClosedBranchCoverage(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "tenant:a", Labels: []string{"Doc"}, Properties: map[string]any{"title": "a"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "tenant:b", Labels: []string{"Doc"}, Properties: map[string]any{"title": "b"}})
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		require.NoError(t, txn.Set([]byte{prefixNode}, []byte{}))            // len(key)<=1 callback path
		require.NoError(t, txn.Set(nodeKey("tenant:broken"), []byte("bad"))) // decode error continue
		return nil
	}))

	t.Run("context canceled path", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := engine.StreamNodesByPrefix(ctx, "tenant:", func(*Node) error { return nil })
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("iteration stopped and callback error paths", func(t *testing.T) {
		errStop := engine.StreamNodesByPrefix(context.Background(), "", func(n *Node) error {
			if n != nil && n.ID == "tenant:a" {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, errStop)

		errBoom := errors.New("boom")
		err := engine.StreamNodesByPrefix(context.Background(), "tenant:", func(*Node) error {
			return errBoom
		})
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("closed-engine stream guards", func(t *testing.T) {
		closed, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
		if closed != nil {
			fixtureowner.Own(t, closed)
		}
		require.NoError(t, err)
		require.NoError(t, closed.Close())

		err = closed.StreamNodes(context.Background(), func(*Node) error { return nil })
		require.ErrorIs(t, err, ErrStorageClosed)
		err = closed.StreamNodesByPrefix(context.Background(), "tenant:", func(*Node) error { return nil })
		require.ErrorIs(t, err, ErrStorageClosed)
		err = closed.StreamEdges(context.Background(), func(*Edge) error { return nil })
		require.ErrorIs(t, err, ErrStorageClosed)
		err = closed.StreamNodeChunks(context.Background(), 2, func([]*Node) error { return nil })
		require.ErrorIs(t, err, ErrStorageClosed)
	})
}

func TestBadgerStats_UnexpectedClosedDBErrorBranches(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"L"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n1", Type: "R"}))

	// Close only the underlying DB to exercise error returns where ensureOpen still passes.
	require.NoError(t, engine.db.Close())

	require.Error(t, engine.initializeCounts())

	_, _ = engine.NodeCountByPrefix("test:")
	_, _ = engine.EdgeCountByPrefix("test:")
	_, _ = engine.ClearAllEmbeddingsForPrefix("test:", CaptureVectorWorkOrigin())
}

func TestBadgerStats_IterateAndStreamDecodeBranches(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:n1", Labels: []string{"Doc"}, Properties: map[string]any{"title": "ok"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:n2", Labels: []string{"Doc"}, Properties: map[string]any{"title": "ok2"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "REL"}))

	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		// Trigger len(key)<=1 and decode-error branches in node/edge streaming methods.
		require.NoError(t, txn.Set([]byte{prefixNode}, []byte{}))
		require.NoError(t, txn.Set(nodeKey("test:bad-node"), []byte("bad-node-payload")))
		require.NoError(t, txn.Set([]byte{prefixEdge}, []byte{}))
		require.NoError(t, txn.Set(edgeKey("test:bad-edge"), []byte("bad-edge-payload")))
		return nil
	}))

	t.Run("iterate nodes skips malformed entries", func(t *testing.T) {
		seen := 0
		err := engine.IterateNodes(func(*Node) bool {
			seen++
			return true
		})
		require.NoError(t, err)
		require.GreaterOrEqual(t, seen, 2)
	})

	t.Run("stream nodes handles canceled context and malformed entries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := engine.StreamNodes(ctx, func(*Node) error { return nil })
		require.ErrorIs(t, err, context.Canceled)

		err = engine.StreamNodes(context.Background(), func(*Node) error { return nil })
		require.NoError(t, err)
	})

	t.Run("stream edges handles canceled context and decode errors", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := engine.StreamEdges(ctx, func(*Edge) error { return nil })
		require.ErrorIs(t, err, context.Canceled)

		err = engine.StreamEdges(context.Background(), func(*Edge) error { return nil })
		require.NoError(t, err)
	})

	t.Run("stream node chunks stop paths", func(t *testing.T) {
		// Stop on full chunk path (len(chunk) >= chunkSize).
		err := engine.StreamNodeChunks(context.Background(), 1, func(nodes []*Node) error {
			if len(nodes) > 0 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)

		// Stop on trailing chunk path (remaining chunk after scan).
		err = engine.StreamNodeChunks(context.Background(), 100, func(nodes []*Node) error {
			if len(nodes) > 0 {
				return ErrIterationStopped
			}
			return nil
		})
		require.NoError(t, err)
	})
}

func TestBadgerStats_ClearAllEmbeddingsPrefixAdditionalBranches(t *testing.T) {
	engine := newTestEngine(t)

	// Valid node that should be cleared.
	_, err := engine.CreateNode(&Node{
		ID:              "tenant:good",
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{{1, 2, 3}},
	})
	require.NoError(t, err)

	require.NoError(t, engine.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
		// len(key)<=1 branch.
		require.NoError(t, txn.Set([]byte{prefixNode}, []byte{}))

		// Decode-success entry with empty Node.ID and embeddings:
		// append empty ID to nodeIDs, then GetNode("") fails and hits skip branch.
		emptyIDNode := &Node{ID: "", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{9}}}
		enc, _, err := engine.encodeNodeInTxn(txn, "tenant", emptyIDNode)
		require.NoError(t, err)
		require.NoError(t, txn.Set(nodeKey("tenant:empty-id-record"), enc))
		return nil
	}))

	cleared, err := engine.ClearAllEmbeddingsForPrefix("tenant:", CaptureVectorWorkOrigin())
	require.NoError(t, err)
	require.GreaterOrEqual(t, cleared, 1)
}
