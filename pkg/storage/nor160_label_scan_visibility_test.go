package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR160 #1688: a write-capable transaction caches the label population its
// stream produced, and the point reads that reuse that population apply the
// live visibility layer when they return an image. The label stream itself must
// carry the same contract: another session's SetManualSuppression takes effect
// on the next scan of the same transaction, exactly as it does on the point
// read of the same row. Stored (physical) reads keep their documented bypass.

// nor160LabelScanRoutes are the public label-scan projections a write-capable
// transaction exposes to its consumers.
var nor160LabelScanRoutes = []struct {
	name string
	scan func(t *testing.T, tx *BadgerTransaction, label string) []NodeID
}{
	{
		name: "full",
		scan: func(t *testing.T, tx *BadgerTransaction, label string) []NodeID {
			t.Helper()
			var ids []NodeID
			require.NoError(t, tx.StreamNodesByLabelProjected(context.Background(), label, nil, func(node *Node) error {
				ids = append(ids, node.ID)
				return nil
			}))
			return ids
		},
	},
	{
		name: "projected",
		scan: func(t *testing.T, tx *BadgerTransaction, label string) []NodeID {
			t.Helper()
			var ids []NodeID
			require.NoError(t, tx.StreamNodesByLabelProjected(context.Background(), label, []string{"n"}, func(node *Node) error {
				require.NotContains(t, node.Properties, "hidden")
				ids = append(ids, node.ID)
				return nil
			}))
			return ids
		},
	},
	{
		name: "query",
		scan: func(t *testing.T, tx *BadgerTransaction, label string) []NodeID {
			t.Helper()
			var ids []NodeID
			// The streaming query row read (streamQueryNodes): embedding
			// metadata settled without stored vectors.
			require.NoError(t, tx.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
				Label:            label,
				QueryNodes:       true,
				ApplyDecayFilter: true,
			}, func(node *Node) error {
				ids = append(ids, node.ID)
				return nil
			}))
			return ids
		},
	},
	{
		name: "materialized",
		scan: func(t *testing.T, tx *BadgerTransaction, label string) []NodeID {
			t.Helper()
			nodes, err := tx.GetNodesByLabel(context.Background(), label)
			require.NoError(t, err)
			ids := make([]NodeID, 0, len(nodes))
			for _, node := range nodes {
				ids = append(ids, node.ID)
			}
			return ids
		},
	},
}

// nor160LabelScanFixture commits two "Held" nodes: one that another session
// suppresses during the transaction, and one that stays visible.
func nor160LabelScanFixture(t *testing.T, engine *MemoryEngine) (hidden, kept NodeID) {
	t.Helper()
	ctx := context.Background()
	hidden, kept = "test:hidden", "test:kept"
	for _, id := range []NodeID{hidden, kept} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Held"}, Properties: map[string]any{"n": int64(1), "hidden": id == hidden}})
		require.NoError(t, err)
	}
	return hidden, kept
}

// A label stream stopped early caches the prefix it delivered; that prefix is
// replayed by the next scan under the live policy too.
func TestNOR160PartialLabelScanReplayHonorsLiveSuppression(t *testing.T) {
	engine := newTestMemoryEngine(t)
	ctx := context.Background()
	first, second := NodeID("test:a"), NodeID("test:b")
	for _, id := range []NodeID{first, second} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Held"}})
		require.NoError(t, err)
	}
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	var stopped []NodeID
	err = tx.StreamNodesByLabelProjected(ctx, "Held", nil, func(node *Node) error {
		stopped = append(stopped, node.ID)
		return ErrIterationStopped
	})
	require.Equal(t, ErrIterationStopped, err)
	require.Equal(t, []NodeID{first}, stopped, "label index order")
	require.NotEmpty(t, tx.snapshotLabelPrefixNodes, "the stopped prefix is the cached population")

	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(first), true))
	require.True(t, engine.visibilityFiltered())

	var rescanned []NodeID
	require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Held", nil, func(node *Node) error {
		rescanned = append(rescanned, node.ID)
		return nil
	}))
	require.Equal(t, []NodeID{second}, rescanned, "a cached prefix must not outlive a live suppression")

	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(first), false))
	rescanned = nil
	require.NoError(t, tx.StreamNodesByLabelProjected(ctx, "Held", nil, func(node *Node) error {
		rescanned = append(rescanned, node.ID)
		return nil
	}))
	require.Equal(t, []NodeID{first, second}, rescanned)
	require.NoError(t, tx.Rollback())
}

// A transaction's own pending image is a row like any other: the live policy
// decides whether the label read delivers it, and reveal reopens it.
func TestNOR160PendingLabelRowHonorsLiveSuppression(t *testing.T) {
	for _, route := range nor160LabelScanRoutes {
		t.Run(route.name, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			ctx := context.Background()
			hidden, kept := nor160LabelScanFixture(t, engine)
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })

			// The transaction rewrites the node another session will hide.
			before, err := tx.GetNode(ctx, hidden)
			require.NoError(t, err)
			after := CopyNode(before)
			after.Properties = map[string]any{"n": int64(2), "hidden": true}
			require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: before, After: after}))
			require.Contains(t, tx.pendingNodes, hidden)
			require.ElementsMatch(t, []NodeID{hidden, kept}, route.scan(t, tx, "Held"))

			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), true))
			require.True(t, engine.visibilityFiltered())
			_, err = tx.GetNode(ctx, hidden)
			require.ErrorIs(t, err, ErrNotFound, "the point read hides the pending image")

			require.ElementsMatch(t, []NodeID{kept}, route.scan(t, tx, "Held"),
				"a pending image follows the same live policy as its point read")
			var stored []NodeID
			require.NoError(t, tx.StreamStoredNodesByLabelProjected(ctx, "Held", nil, func(node *Node) error {
				stored = append(stored, node.ID)
				return nil
			}))
			require.ElementsMatch(t, []NodeID{hidden, kept}, stored,
				"physical stored reads bypass suppression")

			end := engine.BeginQueryRevealScope(true)
			require.ElementsMatch(t, []NodeID{hidden, kept}, route.scan(t, tx, "Held"))
			end()

			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), false))
			require.ElementsMatch(t, []NodeID{hidden, kept}, route.scan(t, tx, "Held"))
			pending, err := tx.GetNode(ctx, hidden)
			require.NoError(t, err)
			require.Equal(t, int64(2), pending.Properties["n"], "the pending body is retained")
			require.NoError(t, tx.Rollback())
		})
	}
}
