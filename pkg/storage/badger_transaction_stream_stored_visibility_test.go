package storage

// BadgerTransaction.StreamNodesWithOptions serves one read-visibility contract
// with the engine scan: the options select it (StreamNodesOptions.readVisibility
// in badger_stats.go/query_node_scan.go), and every population it streams
// answers to that one selection. StreamStoredNodes is the stored selection
// (ApplyDecayFilter false, readVisibility storedRead): it reads what storage
// holds, the entities the visibility layer hides included, for integrity work.
// fullNodeStreamOptions is the visible selection (ApplyDecayFilter true,
// readVisibility visibleRead): user reads leave the hidden entities out.
//
// The committed rows already follow the options. These regressions require the
// transaction's pending overlay to follow them too: a stored stream must carry
// a hidden pending write, and a hidden committed image the transaction read
// through a no-op write lock (writeOwnedReadImagesLocked), while the visible
// stream keeps leaving both out.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	storedStreamVisibilityVisibleID   = NodeID("test:visible")
	storedStreamVisibilityPendingID   = NodeID("test:pending")
	storedStreamVisibilityRefreshedID = NodeID("test:refreshed")
)

// storedStreamVisibilityImages streams opts and returns the emitted population
// with its images, so a caller can assert both membership and the image the
// stream chose for an ID.
func storedStreamVisibilityImages(t *testing.T, tx *BadgerTransaction, opts StreamNodesOptions) ([]string, map[NodeID]*Node) {
	t.Helper()
	images := map[NodeID]*Node{}
	nodes := collectStreamNodes(t, func(visit func(*Node) error) error {
		return tx.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
			images[node.ID] = node
			return visit(node)
		})
	})
	return nodeIDs(nodes), images
}

func TestBadgerTransactionStreamStoredNodesIncludesHiddenPendingAndRefreshedImages(t *testing.T) {
	ctx := context.Background()
	engine := newTestMemoryEngine(t)
	for _, node := range []*Node{
		{ID: storedStreamVisibilityVisibleID, Labels: []string{"Doc"}, Properties: map[string]interface{}{"marker": "visible", "value": int64(1)}},
		{ID: storedStreamVisibilityPendingID, Labels: []string{"Doc"}, Properties: map[string]interface{}{"marker": "pending", "value": int64(1)}},
		{ID: storedStreamVisibilityRefreshedID, Labels: []string{"Doc"}, Properties: map[string]interface{}{"marker": "refreshed", "value": int64(1)}},
	} {
		_, err := engine.CreateNode(ctx, node)
		require.NoError(t, err, node.ID)
	}
	// The visibility layer hides two of the three; the stored read keeps them.
	for _, id := range []NodeID{storedStreamVisibilityPendingID, storedStreamVisibilityRefreshedID} {
		require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), true))
	}
	require.True(t, engine.visibilityFiltered())

	// The contract owner for the stored options, before any transaction view:
	// the same population the transaction's stored stream must serve.
	var storedByEngine []NodeID
	require.NoError(t, StreamStoredNodes(ctx, engine, StreamNodesOptions{WithEmbeddings: true}, func(node *Node) error {
		storedByEngine = append(storedByEngine, node.ID)
		return nil
	}))
	require.ElementsMatch(t,
		[]NodeID{storedStreamVisibilityVisibleID, storedStreamVisibilityPendingID, storedStreamVisibilityRefreshedID},
		storedByEngine, "StreamStoredNodes reads what storage holds, hidden entities included")

	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))

	// A peer commit after the transaction's snapshot is the premise of a
	// refreshed read image: the transaction's no-op write lock then reads the
	// committed image instead of the stale snapshot row.
	refreshed, err := engine.GetStoredNode(ctx, storedStreamVisibilityRefreshedID)
	require.NoError(t, err)
	refreshed.Properties["value"] = int64(2)
	require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{After: refreshed, Policy: StoredNodeImages}))

	latest, err := tx.LockNodeForWrite(ctx, storedStreamVisibilityRefreshedID)
	require.NoError(t, err)
	require.NotNil(t, latest, "premise: the peer commit makes the lock hand back the committed image")
	require.Equal(t, int64(2), latest.Properties["value"])
	require.NotContains(t, tx.pendingNodes, storedStreamVisibilityRefreshedID,
		"premise: a no-op lock stages no write, so the stream only sees the refreshed read image")

	// One staged write to the other hidden node: a pending image the stream
	// emits through the pending overlay, hidden exactly as its committed row is.
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{
		ID:         storedStreamVisibilityPendingID,
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"marker": "pending", "value": int64(2)},
	}, Policy: ClientNodeEdits}))
	require.Contains(t, tx.pendingNodes, storedStreamVisibilityPendingID)

	// Stored options: every stored node, the hidden pending write included and
	// the hidden committed image the no-op lock refreshed.
	storedIDs, storedImages := storedStreamVisibilityImages(t, tx, StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: false})
	require.Equal(t, []string{
		string(storedStreamVisibilityPendingID),
		string(storedStreamVisibilityRefreshedID),
		string(storedStreamVisibilityVisibleID),
	}, storedIDs, "a stored full stream includes the suppressed pending and lock-refreshed nodes")
	require.Equal(t, int64(2), storedImages[storedStreamVisibilityPendingID].Properties["value"],
		"the stored stream emits the pending image")
	require.Equal(t, int64(2), storedImages[storedStreamVisibilityRefreshedID].Properties["value"],
		"the stored stream emits the lock-refreshed committed image")

	// The visible full stream is unchanged: the same options with the visible
	// selection still leave both hidden nodes out, committed rows and overlay
	// alike.
	visibleIDs, _ := storedStreamVisibilityImages(t, tx, fullNodeStreamOptions(""))
	require.Equal(t, []string{string(storedStreamVisibilityVisibleID)}, visibleIDs,
		"a visible full stream leaves the suppressed pending and lock-refreshed nodes out")
}
