package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// A complete point-read image should be reusable without returning the retained
// mutable image. A projected read must never masquerade as a complete image.
func TestNOR160CompletePointSnapshotReuse(t *testing.T) {
	engine := newTestMemoryEngine(t)
	ctx := t.Context()
	id, err := engine.CreateNode(ctx, &Node{ID: "test:point-reuse", Labels: []string{"Held"}, Properties: map[string]any{"thing_id": "one", "n": int64(7)}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	projected, err := tx.GetNodeProjected(ctx, id, []string{"thing_id"})
	require.NoError(t, err)
	require.NotContains(t, projected.Properties, "n")
	require.NotContains(t, tx.snapshotPrefixNodeByID, id, "partial images cannot satisfy full reads")
	_, err = tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	first, err := tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Contains(t, tx.snapshotPrefixNodeByID, id, "reuse complete bounded images instead of decoding every endpoint again")
	first.Properties["n"] = int64(99)
	second, err := tx.GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(7), second.Properties["n"])
	require.NotSame(t, first, second)
	before := CopyNode(second)
	second.Properties["n"] = int64(8)
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: before, After: second}))
	own, err := tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(8), own.Properties["n"], "pending write precedes retained committed snapshot")
}

func TestNOR160PointReuseKeepsSnapshotAndLiveVisibility(t *testing.T) {
	engine := newTestMemoryEngine(t)
	ctx := t.Context()
	id, err := engine.CreateNode(ctx, &Node{ID: "test:point-visible", Labels: []string{"Held"}, Properties: map[string]any{"n": int64(1)}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	first, err := tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Properties["n"])
	_, err = tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Contains(t, tx.snapshotPrefixNodeByID, id)
	peer, err := engine.GetNode(ctx, id)
	require.NoError(t, err)
	before := CopyNode(peer)
	peer.Properties["n"] = int64(2)
	require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{Before: before, After: peer}))
	again, err := tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(1), again.Properties["n"], "peer update must not replace the pinned snapshot")
	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), true))
	_, err = tx.GetQueryNode(ctx, id)
	require.ErrorIs(t, err, ErrNotFound, "live suppression must override a previously cached visible node")
	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), false))
	again, err = tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(1), again.Properties["n"])
	require.NoError(t, tx.DeleteGraphNode(ctx, id, CascadeEdges))
	_, err = tx.GetQueryNode(ctx, id)
	require.ErrorIs(t, err, ErrNotFound, "own deletion precedes a cached snapshot")
	require.NoError(t, tx.Rollback())
	latest, err := engine.GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(2), latest.Properties["n"], "rollback preserves the peer's committed image")
}

func TestNOR160StructuralCopyPreservesCyclesWithoutSharing(t *testing.T) {
	t.Run("map", func(t *testing.T) {
		source := map[string]any{"n": int64(1)}
		source["self"] = source
		copied := CopyValue(source, nil).(map[string]any)
		copied["self"].(map[string]any)["n"] = int64(2)
		require.Equal(t, int64(2), copied["n"])
		require.Equal(t, int64(1), source["n"])
	})
	t.Run("slice", func(t *testing.T) {
		source := make([]any, 1)
		source[0] = source
		copied := CopyValue(source, nil).([]any)
		copied[0].([]any)[0] = nil
		require.Nil(t, copied[0])
		require.NotNil(t, source[0])
	})
	t.Run("record", func(t *testing.T) {
		type record struct {
			N    int
			Next *record
		}
		source := &record{N: 1}
		source.Next = source
		copied := CopyValue(source, nil).(*record)
		require.Same(t, copied, copied.Next)
		require.NotSame(t, source, copied)
		copied.Next.N = 2
		require.Equal(t, 1, source.N)
	})
}

func TestNOR160BufferedBetweenReadOwnsProperties(t *testing.T) {
	for _, route := range []string{"between-list", "between-one", "outgoing-header", "incoming-header"} {
		t.Run(route, func(t *testing.T) {
			// Existing manual-drain fixture stops only replay, keeping ACKed writes
			// in the overlay until an explicit drain.
			b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			ctx := t.Context()
			for _, id := range []NodeID{"test:a", "test:b"} {
				_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"Held"}})
				require.NoError(t, err)
			}
			bufferedWrite(t, b, func(tx *BadgerTransaction) error {
				return tx.CreateEdge(ctx, &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "COHELD", Properties: map[string]any{"n": []int64{7}}})
			})
			_, found, deleted := b.writeBehind.GetEdge("test:e")
			require.True(t, found)
			require.False(t, deleted)
			var edges []*Edge
			var err error
			switch route {
			case "between-list":
				edges, err = b.GetEdgesBetween(ctx, "test:a", "test:b")
			case "between-one":
				var edge *Edge
				edge, err = b.GetEdgeBetween(ctx, "test:a", "test:b", "COHELD")
				if edge != nil {
					edges = []*Edge{edge}
				}
			case "outgoing-header":
				var answered bool
				edges, answered, err = b.OutgoingEdgeHeaders(ctx, "test:a")
				require.True(t, answered)
			case "incoming-header":
				var answered bool
				edges, answered, err = b.IncomingEdgeHeaders(ctx, "test:b")
				require.True(t, answered)
			}
			require.NoError(t, err)
			require.Len(t, edges, 1)
			if edges[0].Properties == nil {
				// Header-only delivery may omit properties; assigning this
				// caller's own map must still leave the buffered body intact.
				edges[0].Properties = map[string]any{"n": []int64{99}}
			} else {
				edges[0].Properties["n"].([]int64)[0] = 99
			}
			accepted, err := b.GetEdge(ctx, "test:e")
			require.NoError(t, err)
			require.Equal(t, int64(7), accepted.Properties["n"].([]int64)[0], "reading must not mutate the acknowledged buffered body")
			require.NoError(t, b.writeBehind.Flush())
			durable, err := b.GetEdge(ctx, "test:e")
			require.NoError(t, err)
			require.Equal(t, int64(7), durable.Properties["n"].([]int64)[0], "drain persists the accepted body, never a caller's read mutation")
		})
	}
}

// One visibility matrix covers the cached point readers and every label
// delivery shape, including populations first captured while already hidden.
func TestTransactionLabelAndPointReadsShareLiveVisibility(t *testing.T) {
	for _, route := range nor160LabelScanRoutes {
		t.Run(route.name, func(t *testing.T) {
			for _, initiallyHidden := range []bool{false, true} {
				name := "captured_visible"
				if initiallyHidden {
					name = "captured_hidden"
				}
				t.Run(name, func(t *testing.T) {
					b := newTestMemoryEngine(t)
					ctx := t.Context()
					hidden, kept := nor160LabelScanFixture(t, b)
					if initiallyHidden {
						require.NoError(t, b.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), true))
					}
					tx, err := b.BeginTransaction(ctx)
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx.Rollback() })
					require.NoError(t, tx.SetNamespace("test"))
					warm := streamTxLabel(t, tx, "Held")
					if initiallyHidden {
						require.Len(t, warm, 1)
					} else {
						require.Len(t, warm, 2)
						require.Contains(t, tx.snapshotLabelNodeByID, hidden, "exercise the complete label snapshot cache for every point reader")
					}
					for _, suppressed := range []bool{true, false} {
						require.NoError(t, b.SetManualSuppression(ctx, StoredEntityNodeKind, string(hidden), suppressed))
						expected := []NodeID{hidden, kept}
						if suppressed {
							expected = []NodeID{kept}
						}
						require.ElementsMatch(t, expected, route.scan(t, tx, "Held"))
						for _, point := range []string{"full", "projected", "query"} {
							var node *Node
							switch point {
							case "full":
								node, err = tx.GetNode(ctx, hidden)
							case "projected":
								node, err = tx.GetNodeProjected(ctx, hidden, []string{"n"})
							case "query":
								node, err = tx.GetQueryNode(ctx, hidden)
							}
							if suppressed {
								require.ErrorIs(t, err, ErrNotFound, point)
							} else {
								require.NoError(t, err, point)
								require.Equal(t, int64(1), node.Properties["n"], point)
							}
						}
						physical, err := tx.GetStoredNode(ctx, hidden)
						require.NoError(t, err)
						require.Equal(t, int64(1), physical.Properties["n"], "physical reads intentionally bypass suppression")
						stored, err := tx.GetStoredNodesByLabel(ctx, "Held")
						require.NoError(t, err)
						require.Len(t, stored, 2)
						end := b.BeginQueryRevealScope(true)
						require.ElementsMatch(t, []NodeID{hidden, kept}, route.scan(t, tx, "Held"))
						end()
						require.ElementsMatch(t, expected, route.scan(t, tx, "Held"), "ending reveal restores live visibility")
						// Useful caches retain physical membership independently of delivery.
						require.NotEmpty(t, tx.snapshotLabelNodes)
						for _, nodes := range tx.snapshotLabelNodes {
							require.Len(t, nodes, 2)
						}
						for _, nodes := range tx.snapshotProjectedLabelNodes {
							require.Len(t, nodes, 2)
						}
					}
					require.NoError(t, tx.Rollback())
				})
			}
		})
	}
}

func TestNOR160PendingEdgePointReadHonorsLiveSuppression(t *testing.T) {
	b := newTestMemoryEngine(t)
	ctx := t.Context()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(ctx, &Node{ID: id, Labels: []string{"Held"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(ctx, &Edge{ID: "test:visible-edge", StartNode: "test:a", EndNode: "test:b", Type: "COHELD", Properties: map[string]any{"n": int64(1)}}))
	tx, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	edge, err := tx.GetEdge(ctx, "test:visible-edge")
	require.NoError(t, err)
	before := CopyEdge(edge)
	edge.Properties["n"] = int64(2)
	require.NoError(t, tx.UpdateEdge(ctx, EdgeUpdateRequest{Before: before, After: edge}))
	require.NoError(t, b.SetManualSuppression(ctx, StoredEntityEdgeKind, string(edge.ID), true))
	_, err = tx.GetEdge(ctx, edge.ID)
	require.ErrorIs(t, err, ErrNotFound, "pending point reads retain the visible-read contract")
	raw, err := tx.GetStoredEdge(ctx, edge.ID)
	require.NoError(t, err)
	require.Equal(t, int64(2), raw.Properties["n"], "stored reads retain the transaction's pending body")
}
