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
		copied := ownedValueCopy(source).(map[string]any)
		copied["self"].(map[string]any)["n"] = int64(2)
		require.Equal(t, int64(2), copied["n"])
		require.Equal(t, int64(1), source["n"])
	})
	t.Run("slice", func(t *testing.T) {
		source := make([]any, 1)
		source[0] = source
		copied := ownedValueCopy(source).([]any)
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
		copied := ownedValueCopy(source).(*record)
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

func TestNOR160LabelSnapshotPointReadsHonorLiveSuppression(t *testing.T) {
	for _, route := range []string{"full", "projected", "query"} {
		t.Run(route, func(t *testing.T) {
			b := newTestMemoryEngine(t)
			ctx := t.Context()
			id, err := b.CreateNode(ctx, &Node{ID: "test:label-visible", Labels: []string{"Held"}, Properties: map[string]any{"n": int64(1)}})
			require.NoError(t, err)
			tx, err := b.BeginTransaction(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.SetNamespace("test"))
			rows := streamTxLabel(t, tx, "Held")
			require.Len(t, rows, 1)
			require.Contains(t, tx.snapshotLabelNodeByID, id, "exercise the complete label snapshot cache")
			require.NoError(t, b.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), true))
			switch route {
			case "full":
				_, err = tx.GetNode(ctx, id)
			case "projected":
				_, err = tx.GetNodeProjected(ctx, id, []string{"n"})
			case "query":
				_, err = tx.GetQueryNode(ctx, id)
			}
			require.ErrorIs(t, err, ErrNotFound, "cached visibility must not override live suppression")
			stored, err := tx.GetStoredNode(ctx, id)
			require.NoError(t, err)
			require.Equal(t, int64(1), stored.Properties["n"], "physical reads intentionally bypass suppression")
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
