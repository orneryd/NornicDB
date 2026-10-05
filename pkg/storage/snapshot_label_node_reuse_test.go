package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func streamTxLabel(t *testing.T, tx *BadgerTransaction, label string) []*Node {
	t.Helper()
	var nodes []*Node
	require.NoError(t, tx.StreamNodesByLabelProjected(label, nil, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	}))
	return nodes
}

// A write to a node that a completed whole-node label stream of the same
// transaction already read takes the node from that stream instead of
// reading it again; the result is the same.
func TestTransactionWritesReuseNodesFromCompletedLabelStream(t *testing.T) {
	eng := newTestEngine(t)
	for i := 0; i < 3; i++ {
		_, err := eng.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"V"}, Properties: map[string]any{"id": int64(i)}})
		require.NoError(t, err)
	}
	// n2 has worker-sidecar embedding state, so it is read again.
	require.NoError(t, eng.UpdateNodeEmbeddingSidecar(embeddingWriteback(t, eng, "test:n2", [][]float32{{0.1, 0.2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})))

	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer func() { _ = tx.Rollback() }()
	require.Len(t, streamTxLabel(t, tx, "V"), 3)
	require.Len(t, tx.snapshotLabelNodeByID, 3)

	tx.mu.Lock()
	reused, err := tx.getCommittedNodeLocked("test:n0")
	tx.mu.Unlock()
	require.NoError(t, err)
	require.Equal(t, int64(0), reused.Properties["id"])
	require.NotSame(t, tx.snapshotLabelNodeByID["test:n0"], reused)
	reused.Properties["id"] = int64(99)
	require.Equal(t, int64(0), tx.snapshotLabelNodeByID["test:n0"].Properties["id"])

	tx.mu.Lock()
	reread, err := tx.getCommittedNodeLocked("test:n2")
	tx.mu.Unlock()
	require.NoError(t, err)
	require.Equal(t, int64(2), reread.Properties["id"])
	require.Len(t, reread.ChunkEmbeddings, 1)

	updated := copyNode(tx.snapshotLabelNodeByID["test:n1"])
	updated.Properties["x"] = int64(1)
	require.NoError(t, tx.UpdateNode(updated))
	require.NoError(t, tx.DeleteNode("test:n0"))
	require.NoError(t, tx.Commit())

	node, err := eng.GetNode("test:n1")
	require.NoError(t, err)
	require.Equal(t, int64(1), node.Properties["x"])
	_, err = eng.GetNode("test:n0")
	require.ErrorIs(t, err, ErrNotFound)
}

// With decay filtering on, label streams are not reused for writes.
func TestTransactionLabelStreamNotReusedWithDecay(t *testing.T) {
	eng := newTestEngine(t)
	_, err := eng.CreateNode(&Node{ID: "test:n0", Labels: []string{"V"}})
	require.NoError(t, err)
	eng.SetDecayEnabled(true)
	tx, err := eng.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer func() { _ = tx.Rollback() }()
	require.Len(t, streamTxLabel(t, tx, "V"), 1)
	require.Empty(t, tx.snapshotLabelNodeByID)
}
