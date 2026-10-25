package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCommitAdjacencyPreparationCancellation(t *testing.T) {
	for _, tc := range []struct {
		owner, via     string
		buffered, warm bool
	}{
		{owner: "collectEdgesByIndexPrefix"},
		{owner: "read.func1"},
		{owner: "cachedAdjacency.func1", warm: true},
		{owner: "visitEntityOverlayLocked.func1", via: "selectEdgeAdjacencyOverlayLockedWithContext", buffered: true},

		{owner: "liveAdjacencyLockedWithContext.func1", buffered: true, warm: true},
	} {
		t.Run(tc.owner, func(t *testing.T) {
			cacheEntries := 10000
			if tc.buffered && !tc.warm {
				cacheEntries = 2
			}
			b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: tc.buffered, WriteBehindInterval: time.Hour, NodeCacheMaxEntries: cacheEntries})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			require.NoError(t, b.GetSchemaForNamespace("test").AddConstraintContractBundle(storage.ConstraintContract{Name: "core_only", TargetEntityType: string(storage.ConstraintEntityNode), TargetLabelOrType: "P", Entries: []storage.ConstraintContractEntry{{Kind: storage.ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}}}, nil, nil, false))
			var seed interface {
				CreateNode(*storage.Node) (storage.NodeID, error)
				CreateEdge(*storage.Edge) error
			} = b
			var seedTx *storage.BadgerTransaction
			if tc.buffered {
				seedTx, err = b.BeginTransaction()
				require.NoError(t, err)
				defer seedTx.Rollback()
				require.NoError(t, seedTx.SetImplicit(true))
				require.NoError(t, seedTx.SetNamespace("test"))
				seed = seedTx
			}
			_, err = seed.CreateNode(&storage.Node{ID: "test:hub", Labels: []string{"P"}, Properties: map[string]interface{}{"team": "core", "k": int64(0)}})
			require.NoError(t, err)
			for i := 0; i < 20; i++ {
				id := storage.NodeID(fmt.Sprintf("test:q%d", i))
				_, err := seed.CreateNode(&storage.Node{ID: id, Labels: []string{"Q"}})
				require.NoError(t, err)
				require.NoError(t, seed.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("test:r%d", i)), StartNode: "test:hub", EndNode: id, Type: "R"}))
			}
			if seedTx != nil {
				require.NoError(t, seedTx.CommitContext(context.Background()))
			}
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(q string) {
				t.Helper()
				_, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
			}
			prepare := func() {
				if !tc.buffered {
					run("BEGIN")
					run("MATCH (n:P) SET n.k = 1")
				}
				if tc.warm {
					_, err := b.GetStoredOutgoingEdges("test:hub")
					require.NoError(t, err)
					_, err = b.GetStoredIncomingEdges("test:hub")
					require.NoError(t, err)
				}
				if tc.buffered && !tc.warm {
					// Exercise the cold captured-overlay branch through public
					// reads and the configured bounded cache. Earlier validation
					// may have warmed hub even before the statement's COMMIT.
					for i := 0; i < 6; i++ {
						id := storage.NodeID(fmt.Sprintf("test:q%d", i))
						_, err := b.GetStoredOutgoingEdges(id)
						require.NoError(t, err)
						_, err = b.GetStoredIncomingEdges(id)
						require.NoError(t, err)
					}
				}
			}
			prepare()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: tc.owner, via: tc.via, cancel: cancel}
			if tc.via != "" {
				ctx.trace = make(map[string]bool)
			}
			query := "COMMIT"
			if tc.buffered {
				query = "MATCH (n:P) SET n.k = 1"
			}
			_, err = e.Execute(ctx, query, nil)
			stored, readErr := b.GetStoredNode("test:hub")
			require.NoError(t, readErr)
			t.Logf("owner=%s via=%s hits=%d commit=%v stored k=%v", tc.owner, tc.via, ctx.hits, err, stored.Properties["k"])
			if ctx.hits < 2 {
				t.Logf("observed storage owners: %v", ctx.trace)
			}
			require.GreaterOrEqual(t, ctx.hits, 2, "observe this exact preparation phase")
			require.ErrorIs(t, err, context.Canceled)
			_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
			node, err := b.GetStoredNode("test:hub")
			require.NoError(t, err)
			require.EqualValues(t, 0, node.Properties["k"])
			prepare()
			run(query)
			node, err = b.GetStoredNode("test:hub")
			require.NoError(t, err)
			require.EqualValues(t, 1, node.Properties["k"])
		})
	}
}
