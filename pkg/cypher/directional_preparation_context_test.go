package cypher

// Author: #1667 implementation lane (Codex).

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDirectionalStatementPreparationCancellation(t *testing.T) {
	for _, direction := range []string{"outgoing", "incoming", "both"} {
		for _, transactional := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/transaction_%t", direction, transactional), func(t *testing.T) {
				e := newDirectionalPreparationExecutor(t)
				query := "MATCH (a:P {k:0}), (b:Q {k:20}) WITH a,b MATCH p = ANY SHORTEST (a)(()-[:R]->())+(b) RETURN p"
				if direction == "incoming" {
					query = "MATCH (a:Q {k:20}), (b:P {k:0}) WITH a,b MATCH p = ANY SHORTEST (a)(()<-[:R]-())+(b) RETURN p"
				}
				if direction == "both" {
					query = "MATCH (a:P {k:0}), (b:Q {k:20}) WITH a,b MATCH p = ANY SHORTEST (a)(()-[:R]-())+(b) RETURN p"
				}
				if transactional {
					_, err := e.Execute(context.Background(), "BEGIN", nil)
					require.NoError(t, err)
				}
				healthy := func() {
					t.Helper()
					rows, err := e.Execute(context.Background(), query, nil)
					require.NoError(t, err, query)
					require.Len(t, rows.Rows, 1)
				}
				healthy()
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				ctx := &ddlPreparationContext{Context: parent, owner: "materializedValuesWithContext", via: "toUserEdgesInNamespace", after: 2, cancel: cancel}
				if transactional {
					ctx.owner, ctx.via = "transactionStorageWrapper).toUserEdgesWithContext", ""
					ctx.cypherOwner = true
				}
				result, err := e.Execute(ctx, query, nil)
				rows := 0
				if result != nil {
					rows = len(result.Rows)
				}
				t.Logf("direction=%s transaction=%t projection_observations=%d execute=%v rows=%d", direction, transactional, ctx.hits, err, rows)
				require.GreaterOrEqual(t, ctx.hits, 2, "the public directional reader must carry its caller into namespace collection")
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, rows)
				if transactional {
					_, err = e.Execute(context.Background(), "ROLLBACK", nil)
					require.NoError(t, err)
					_, err = e.Execute(context.Background(), "BEGIN", nil)
					require.NoError(t, err)
				}
				healthy()
			})
		}
	}
}

// Snapshot preparation is reached through the same public selected-path query.
// A cold explicit transaction reaches ID collection, private ordering and cache
// admission; a warm one reaches the retained cached image copier.
func TestDirectionalSnapshotPreparationCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, owner, via string
		warm             bool
	}{
		{name: "snapshot IDs", owner: "collectVisibleAdjacencyEdgeIDsInTxnWithContext"},
		{name: "snapshot order", owner: "reverseSliceWithContext"},
		{name: "cache sizing", owner: "snapshotLabelPrefixValueBytesWithContext"},
		{name: "cache admission image", owner: "copyEdgeWithContext", via: "cacheSnapshotEdgeByIDLockedWithContext"},
		{name: "cached image", owner: "copyEdgeWithContext", via: "readCachedSnapshotAdjacentEdgesLockedWithContext", warm: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newDirectionalPreparationExecutor(t)
			query := "MATCH (a:P {k:0}), (b:Q {k:20}) WITH a,b MATCH p = ANY SHORTEST (a)(()-[:R]->())+(b) RETURN p"
			healthy := func() {
				t.Helper()
				result, err := e.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
			}
			healthy()
			_, err := e.Execute(context.Background(), "BEGIN", nil)
			require.NoError(t, err)
			if tc.warm {
				healthy()
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: tc.owner, via: tc.via, after: 2, cancel: cancel}
			result, err := e.Execute(ctx, query, nil)
			rows := 0
			if result != nil {
				rows = len(result.Rows)
			}
			t.Logf("owner=%s via=%s warm=%t observations=%d execute=%v rows=%d", tc.owner, tc.via, tc.warm, ctx.hits, err, rows)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, rows)
			_, err = e.Execute(context.Background(), "ROLLBACK", nil)
			require.NoError(t, err)
			_, err = e.Execute(context.Background(), "BEGIN", nil)
			require.NoError(t, err)
			healthy()
		})
	}
}

func newDirectionalPreparationExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	b, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ns := storage.NewNamespacedEngine(b, "test")
	_, err = ns.CreateNode(&storage.Node{ID: "center", Labels: []string{"P"}, Properties: map[string]any{"k": int64(0)}})
	require.NoError(t, err)
	for i := 1; i <= 20; i++ {
		id := storage.NodeID(fmt.Sprintf("q%d", i))
		_, err = ns.CreateNode(&storage.Node{ID: id, Labels: []string{"Q"}, Properties: map[string]any{"k": int64(i)}})
		require.NoError(t, err)
		require.NoError(t, ns.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("r%d", i)), StartNode: "center", EndNode: id, Type: "R", Properties: map[string]any{"a": int64(i), "b": "edge", "c": []any{int64(1), int64(2), int64(3)}}}))
	}
	// The control must execute preparation, rather than return the healthy
	// query's cached result without entering any storage reader.
	e := NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0})
	t.Cleanup(func() { _, _ = e.Execute(context.Background(), "ROLLBACK", nil) })
	return e
}
