package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreparedPopulationCancellation(t *testing.T) {
	for _, owner := range []string{"physicalOperationsLocked", "appendOperationPublicationKeysWithContext", "captureStoredEntityChanges", "prepareEntityOperationsWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b := newTestEngine(t)
			prepare := func() *BadgerTransaction {
				t.Helper()
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback() })
				require.NoError(t, tx.SetNamespace("test"))
				for i := 0; i < 20; i++ {
					id := NodeID(fmt.Sprintf("test:p%d", i))
					_, err := tx.CreateNode(&Node{ID: id, Labels: []string{"P"}})
					require.NoError(t, err)
				}
				if owner == "physicalOperationsLocked" {
					for i := 0; i < 20; i++ {
						id := EdgeID(fmt.Sprintf("test:r%d", i))
						require.NoError(t, tx.CreateEdge(&Edge{ID: id, StartNode: "test:p0", EndNode: "test:p1", Type: "R"}))
						require.NoError(t, tx.DeleteEdge(id))
					}
				}
				return tx
			}
			ctx := preparationContext(t, owner, 2)
			prepared, err := prepare().PrepareEntityCommit(ctx, "canceled-preparation")
			if prepared != nil {
				prepared.Abort()
			}
			t.Logf("owner=%s hits=%d prepare=%v", owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			nodes, err := b.GetStoredNodesByLabel("P")
			require.NoError(t, err)
			require.Empty(t, nodes)
			prepared, err = prepare().PrepareEntityCommit(context.Background(), "complete-preparation")
			require.NoError(t, err)
			require.NoError(t, prepared.Commit(nil))
			nodes, err = b.GetStoredNodesByLabel("P")
			require.NoError(t, err)
			require.Len(t, nodes, 20)
		})
	}
}

func TestBatchPendingDropPreparationCancellation(t *testing.T) {
	b := newTestEngine(t)
	var ids []NodeID
	for i := 0; i < 20; i++ {
		id := NodeID(fmt.Sprintf("peer:p%d", i))
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
		ids = append(ids, id)
	}
	_, err := b.CreateNode(&Node{ID: "victim:n"})
	require.NoError(t, err)
	sentinel := errors.New("hold pending DROP cleanup")
	b.dropPrefixTailHook = func(string) error { return sentinel }
	_, _, err = b.DeleteByPrefix("victim:")
	require.ErrorIs(t, err, sentinel)
	t.Cleanup(func() { b.dropPrefixTailHook = nil; _, _, _ = b.DeleteByPrefix("victim:") })
	ctx := preparationContext(t, "pendingReadConflict", 2)
	nodes, err := b.BatchGetNodesWithContext(ctx, ids)
	t.Logf("owner=pendingReadConflict hits=%d read=%v nodes=%d", ctx.hits, err, len(nodes))
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, nodes)
	nodes, err = b.BatchGetNodesWithContext(context.Background(), ids)
	require.NoError(t, err)
	require.Len(t, nodes, 20, "a different pending namespace must not fence the peer batch")
}

func TestSnapshotPendingDropPreparationCancellation(t *testing.T) {
	b := newTestEngine(t)
	_, err := b.CreateNode(&Node{ID: "peer:p"})
	require.NoError(t, err)
	for _, namespace := range []string{"victim0", "victim1"} {
		_, err = b.CreateNode(&Node{ID: NodeID(namespace + ":n")})
		require.NoError(t, err)
	}
	sentinel := errors.New("hold foreign DROP cleanup")
	b.dropPrefixTailHook = func(string) error { return sentinel }
	for _, namespace := range []string{"victim0", "victim1"} {
		_, _, err = b.DeleteByPrefix(namespace + ":")
		require.ErrorIs(t, err, sentinel)
	}
	t.Cleanup(func() {
		b.dropPrefixTailHook = nil
		for _, namespace := range []string{"victim0", "victim1"} {
			_, _, _ = b.DeleteByPrefix(namespace + ":")
		}
	})
	ctx := preparationContext(t, "pendingPrefixConflictWithContext", 2)
	snapshot, err := OpenReadSnapshot(ctx, NewNamespacedEngine(b, "peer"))
	if snapshot != nil {
		snapshot.Close()
	}
	t.Logf("owner=pendingPrefixConflictWithContext hits=%d open=%v", ctx.hits, err)
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, snapshot)
	retry, err := OpenReadSnapshot(context.Background(), NewNamespacedEngine(b, "peer"))
	require.NoError(t, err)
	defer retry.Close()
	nodes := 0
	require.NoError(t, retry.IterateNodes(func(*Node) bool { nodes++; return true }))
	require.Equal(t, 1, nodes, "foreign pending DROP must not fence a peer snapshot")
}
