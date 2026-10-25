package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMutationStatementPreparationCancellation(t *testing.T) {
	for _, tc := range []struct {
		owner, via, query     string
		pending, pendingEdges bool
	}{
		{owner: "pendingCreateNodeOperationIndexLocked", query: "MATCH (n:P) SET n.k = 100", pending: true},
		{owner: "pendingAdjacencyLocked", query: "MATCH (n:P) SET n:Extra", pendingEdges: true},
		{owner: "validatePolicyOnNodeLabelChange", query: "MATCH (n:P) SET n:Extra"},
		{owner: "incidentEdgeTypesTxLocked", query: "MATCH (n:P) SET n:Extra"},
		{owner: "bufferNodeLabelChangeEdgeTypeDeltasLocked", query: "MATCH (n:P) SET n:Extra"},
		{owner: "incidentEdgesLocked.func3", via: "incidentEdgeTypesTxLocked", query: "MATCH (n:P) SET n:Extra"},
		{owner: "incidentEdgesLocked.func3", via: "IncidentEdgesWithContext", query: "MATCH (n:P) DETACH DELETE n"},
		{owner: "executeNodeDeletion", query: "MATCH (n:P) DETACH DELETE n"},
	} {
		t.Run(tc.owner+"_"+tc.via, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(q string) {
				t.Helper()
				_, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
			}
			if !tc.pending {
				_, err = b.CreateNode(&storage.Node{ID: "test:hub", Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(0)}})
				require.NoError(t, err)
				for i := 0; i < 20; i++ {
					id := storage.NodeID(fmt.Sprintf("test:q%d", i))
					_, err = b.CreateNode(&storage.Node{ID: id, Labels: []string{"Q"}})
					require.NoError(t, err)
					require.NoError(t, b.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("test:r%d", i)), StartNode: "test:hub", EndNode: id, Type: fmt.Sprintf("R%d", i)}))
				}
			}
			prepare := func() {
				run("BEGIN")
				if tc.pending {
					run("UNWIND range(1,20) AS i CREATE (:P {k:i})")
				}
				if tc.pendingEdges {
					run("MATCH (n:P) UNWIND range(1,20) AS i CREATE (n)-[:Pending]->(:New {k:i})")
				}
			}
			prepare()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: tc.owner, via: tc.via, cancel: cancel}
			_, err = e.Execute(ctx, tc.query, nil)
			t.Logf("owner=%s via=%s hits=%d execute=%v", tc.owner, tc.via, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
			nodes, err := b.GetStoredNodesByLabel("P")
			require.NoError(t, err)
			if tc.pending {
				require.Empty(t, nodes)
			} else {
				require.Len(t, nodes, 1)
				require.Equal(t, []string{"P"}, nodes[0].Labels)
				require.EqualValues(t, 0, nodes[0].Properties["k"])
			}
			prepare()
			run(tc.query)
			run("COMMIT")
		})
	}
}

func TestCommitRemainingPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"validateAllConstraints", "bufferTemporalIndexWrites", "storeWrittenBodies", "materializeDeferredEdgesLocked", "revalidateEndpointsLocked", "bufferedDeleteConflictLocked.func1", "bufferedDeleteConflictLocked.func2", "commitsSince"} {
		t.Run(owner, func(t *testing.T) {
			buffered := owner != "validateAllConstraints" && owner != "bufferTemporalIndexWrites" && owner != "storeWrittenBodies" && owner != "materializeDeferredEdgesLocked"
			b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, b.Close()) })
			_, err = b.CreateNode(&storage.Node{ID: "test:hub", Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(0)}})
			require.NoError(t, err)
			e := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
			run := func(q string) {
				t.Helper()
				_, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
			}
			prepare := func() {
				run("BEGIN")
				if owner == "revalidateEndpointsLocked" || owner == "materializeDeferredEdgesLocked" {
					run("MATCH (n:P) UNWIND range(1,20) AS i CREATE (n)-[:R]->(:Q {k:i})")
				} else {
					run("MATCH (n:P) SET n.k=1")
					run("UNWIND range(1,20) AS i CREATE (:Q {k:i})")
				}
				if buffered && owner != "revalidateEndpointsLocked" {
					// New ACKs after BEGIN are the conflict check's preparation
					// population. A fresh executor supplies actual peer commits.
					peer := NewStorageExecutor(storage.NewNamespacedEngine(b, "test"))
					for i := 0; i < 3; i++ {
						_, err := peer.Execute(context.Background(), "CREATE (:Peer)", nil)
						require.NoError(t, err)
					}
				}
			}
			prepare()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			_, err = e.Execute(ctx, "COMMIT", nil)
			t.Logf("owner=%s hits=%d commit=%v", owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
			node, err := b.GetStoredNode("test:hub")
			require.NoError(t, err)
			require.EqualValues(t, 0, node.Properties["k"])
			q, err := b.GetStoredNodesByLabel("Q")
			require.NoError(t, err)
			require.Empty(t, q)
			prepare()
			run("COMMIT")
		})
	}
}
