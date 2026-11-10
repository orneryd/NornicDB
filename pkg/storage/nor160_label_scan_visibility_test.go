package storage

import (
	"context"
	"slices"
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

func nor160SortedIDs(ids []NodeID) []NodeID {
	sorted := append([]NodeID(nil), ids...)
	slices.Sort(sorted)
	return sorted
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

// A label population a transaction has already cached follows the live
// visibility policy on every later scan, and reveal reopens it.
func TestNOR160LabelScanReplayHonorsLiveSuppression(t *testing.T) {
	for _, route := range nor160LabelScanRoutes {
		t.Run(route.name, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			ctx := context.Background()
			hidden, kept := nor160LabelScanFixture(t, engine)
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })

			// The first scan builds the transaction's population caches.
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(route.scan(t, tx, "Held")))

			// Another session hides one of them.
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), true))
			require.True(t, engine.visibilityFiltered())

			// The point read is the contract the scan must agree with.
			_, err = tx.GetNode(ctx, hidden)
			require.ErrorIs(t, err, ErrNotFound, "the point read hides the row")

			// The repeated scan reads the same pinned snapshot with the live
			// policy, so the hidden row is not delivered.
			require.Equal(t, []NodeID{kept}, nor160SortedIDs(route.scan(t, tx, "Held")),
				"a cached label population must not outlive a live suppression")
			// Its stored (physical) read keeps the documented bypass.
			var stored []NodeID
			require.NoError(t, tx.StreamStoredNodesByLabelProjected(ctx, "Held", nil, func(node *Node) error {
				stored = append(stored, node.ID)
				return nil
			}))
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(stored),
				"physical stored reads bypass suppression")

			// An open reveal scope reopens the same population.
			end := engine.BeginQueryRevealScope(true)
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(route.scan(t, tx, "Held")))
			end()

			// Shown again, the row returns without re-reading anything else.
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), false))
			require.False(t, engine.visibilityFiltered())
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(route.scan(t, tx, "Held")))

			// The transaction's snapshot is untouched by the suppression.
			revealed, err := tx.GetNode(ctx, hidden)
			require.NoError(t, err)
			require.Equal(t, int64(1), revealed.Properties["n"])
			require.NoError(t, tx.Rollback())
		})
	}
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

// A population captured while another session hid a row never answers a later
// scan that must show it: the scan that captured it could not have seen the
// row, so its population is not reusable.
func TestNOR160LabelScanCapturedWhileSuppressedIsNotReused(t *testing.T) {
	for _, route := range nor160LabelScanRoutes {
		t.Run(route.name, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			ctx := context.Background()
			hidden, kept := nor160LabelScanFixture(t, engine)
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), true))
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })

			// The transaction's first scan already meets the hidden row.
			require.Equal(t, []NodeID{kept}, nor160SortedIDs(route.scan(t, tx, "Held")))

			// The row is shown again: the next scan delivers what the earlier
			// scan could not have captured, then hides it again.
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), false))
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(route.scan(t, tx, "Held")))
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), true))
			require.Equal(t, []NodeID{kept}, nor160SortedIDs(route.scan(t, tx, "Held")))
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), false))

			// The snapshot itself never lost the row.
			revealed, err := tx.GetNode(ctx, hidden)
			require.NoError(t, err)
			require.Equal(t, int64(1), revealed.Properties["n"])
			require.NoError(t, tx.Rollback())
		})
	}
}

// Cache the complete physical population even when delivery hides a row.
// This preserves reuse without making a live policy decision permanent.
func TestTransactionLabelCacheRetainsPhysicalPopulationDuringSuppression(t *testing.T) {
	for _, route := range nor160LabelScanRoutes {
		t.Run(route.name, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			ctx := t.Context()
			hidden, kept := nor160LabelScanFixture(t, engine)
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), true))
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.Equal(t, []NodeID{kept}, route.scan(t, tx, "Held"))
			if route.name != "materialized" {
				var populations int
				for _, nodes := range tx.snapshotLabelNodes {
					require.Len(t, nodes, 2)
					populations++
				}
				for _, nodes := range tx.snapshotProjectedLabelNodes {
					require.Len(t, nodes, 2)
					populations++
				}
				require.Equal(t, 1, populations, "retain physical membership instead of disabling the cache")
			}
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), false))
			require.ElementsMatch(t, []NodeID{hidden, kept}, route.scan(t, tx, "Held"))
		})
	}
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
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(route.scan(t, tx, "Held")))

			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), true))
			require.True(t, engine.visibilityFiltered())
			_, err = tx.GetNode(ctx, hidden)
			require.ErrorIs(t, err, ErrNotFound, "the point read hides the pending image")

			require.Equal(t, []NodeID{kept}, nor160SortedIDs(route.scan(t, tx, "Held")),
				"a pending image follows the same live policy as its point read")
			var stored []NodeID
			require.NoError(t, tx.StreamStoredNodesByLabelProjected(ctx, "Held", nil, func(node *Node) error {
				stored = append(stored, node.ID)
				return nil
			}))
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(stored),
				"physical stored reads bypass suppression")

			end := engine.BeginQueryRevealScope(true)
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(route.scan(t, tx, "Held")))
			end()

			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), false))
			require.Equal(t, nor160SortedIDs([]NodeID{hidden, kept}), nor160SortedIDs(route.scan(t, tx, "Held")))
			pending, err := tx.GetNode(ctx, hidden)
			require.NoError(t, err)
			require.Equal(t, int64(2), pending.Properties["n"], "the pending body is retained")
			require.NoError(t, tx.Rollback())
		})
	}
}
