package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionPolicyRewriteOrdersWithDeleteAndPreservesAccounting(t *testing.T) {
	for _, route := range []string{"http-explicit", "bolt-explicit"} {
		for _, buffered := range []bool{false, true} {
			for _, isEdge := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/buffered=%v/edge=%v", route, buffered, isEdge), func(t *testing.T) {
					h := openHiddenDeleteServer(t, t.TempDir(), buffered, route)
					t.Cleanup(func() { h.shutdown(t) })
					base := storage.UnwrapEngine(h.db.GetBaseStorageForManager()).(*storage.BadgerEngine)
					schema := base.GetSchemaForNamespace("nornic")
					scope := knowledgepolicy.ScopeNode
					binding := knowledgepolicy.DecayProfileBinding{Name: "stale", ProfileRef: "stale", TargetLabels: []string{"Stale"}}
					if isEdge {
						scope = knowledgepolicy.ScopeEdge
						binding = knowledgepolicy.DecayProfileBinding{Name: "stale", ProfileRef: "stale", TargetEdgeType: "STALE", IsEdge: true}
					}
					require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{
						Name: "stale", Scope: scope, Function: knowledgepolicy.DecayFunctionExponential,
						HalfLifeSeconds: 1, VisibilityThreshold: .1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true,
					}))
					require.NoError(t, schema.CreateDecayProfileBinding(binding))
					store, err := h.server.dbManager.GetStorage("nornic")
					require.NoError(t, err)
					old := time.Now().Add(-72 * time.Hour)
					_, err = store.CreateNode(t.Context(), &storage.Node{ID: "a", Labels: []string{"Stale"}, Properties: map[string]any{"payload": "original"}, CreatedAt: old, UpdatedAt: old})
					require.NoError(t, err)
					_, err = store.CreateNode(t.Context(), &storage.Node{ID: "b"})
					require.NoError(t, err)
					require.NoError(t, store.CreateEdge(t.Context(), &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "STALE", Properties: map[string]any{"payload": "original"}, CreatedAt: old, UpdatedAt: old}))
					before, beforeNodes, beforeEdges := h.server.dbManager.GetStorageSize(t.Context(), "nornic")
					var rewrites int
					type rewriteResult struct {
						changed bool
						err     error
					}
					rewritten := make(chan rewriteResult, 1)
					waits := fixtureowner.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 1)
					rewriteCtx, cancelRewrite := context.WithTimeout(t.Context(), 20*time.Second)
					defer cancelRewrite()
					h.beforeCommit = func() {
						id := "nornic:a"
						if isEdge {
							id = "nornic:r"
						}
						if !isEdge {
							// DELETE holds this node's body lock until commit. The policy
							// rewrite must queue, not run synchronously in a before-commit
							// hook that prevents its own lock owner from completing.
							rewrites++
							go func() {
								changed, err := base.EnqueueDeindexIfSuppressed(rewriteCtx, id, false)
								rewritten <- rewriteResult{changed, err}
							}()
							fixtureowner.WaitForBarrier(t, waits, 10*time.Second, cancelRewrite, "the node policy rewrite never waited for DELETE")
							return
						}
						changed, err := base.EnqueueDeindexIfSuppressed(t.Context(), id, isEdge)
						require.NoError(t, err)
						require.True(t, changed, "force the ordinary policy callback after the native snapshot and statement, before COMMIT")
						rewrites++
					}
					query := "MATCH (n) WHERE reveal(n) IS NOT NULL DETACH DELETE n"
					outcome := h.run(t, query)
					require.Equal(t, 1, rewrites)
					if !isEdge {
						require.Empty(t, outcome.code, outcome.message)
						require.Equal(t, 2, outcome.nodesDeleted)
						require.Equal(t, 1, outcome.relationshipsDeleted)
						select {
						case result := <-rewritten:
							require.NoError(t, result.err)
							require.False(t, result.changed, "the queued policy job cannot recreate a deleted body")
						case <-time.After(10 * time.Second):
							t.Fatal("policy job did not finish after DELETE committed")
						}
						nodes, err := store.NodeCount(t.Context())
						require.NoError(t, err)
						require.Zero(t, nodes)
						edges, err := store.EdgeCount(t.Context())
						require.NoError(t, err)
						require.Zero(t, edges)
						total, nodeBytes, edgeBytes := h.server.dbManager.GetStorageSize(t.Context(), "nornic")
						require.Zero(t, total)
						require.Zero(t, nodeBytes)
						require.Zero(t, edgeBytes)
						return
					}
					require.Equal(t, "Neo.TransientError.Transaction.Outdated", outcome.code, outcome.message)
					require.Contains(t, outcome.message, "changed after transaction start")
					h.beforeCommit = nil
					nodes, err := store.NodeCount(t.Context())
					require.NoError(t, err)
					require.EqualValues(t, 2, nodes)
					edges, err := store.EdgeCount(t.Context())
					require.NoError(t, err)
					require.EqualValues(t, 1, edges)
					node, err := storage.GetStoredNode(t.Context(), store, "a")
					require.NoError(t, err)
					require.Equal(t, "original", node.Properties["payload"])
					edge, err := storage.GetStoredEdge(t.Context(), store, "r")
					require.NoError(t, err)
					require.Equal(t, "original", edge.Properties["payload"])
					after, afterNodes, afterEdges := h.server.dbManager.GetStorageSize(t.Context(), "nornic")
					require.Equal(t, afterNodes+afterEdges, after)
					require.GreaterOrEqual(t, after, before, "only the independent suppression flag may add stored bytes")
					if isEdge {
						require.Equal(t, beforeNodes, afterNodes)
					} else {
						require.Equal(t, beforeEdges, afterEdges)
					}
					healthy := h.requireSuccess(t, query)
					require.Equal(t, 2, healthy.nodesDeleted)
					require.Equal(t, 1, healthy.relationshipsDeleted)
					nodes, err = store.NodeCount(t.Context())
					require.NoError(t, err)
					require.Zero(t, nodes)
					edges, err = store.EdgeCount(t.Context())
					require.NoError(t, err)
					require.Zero(t, edges)
					total, nodeBytes, edgeBytes := h.server.dbManager.GetStorageSize(t.Context(), "nornic")
					require.Zero(t, total)
					require.Zero(t, nodeBytes)
					require.Zero(t, edgeBytes)
				})
			}
		}
	}
}
