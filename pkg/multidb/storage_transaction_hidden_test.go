package multidb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Quotas describe stored data even when ordinary reads hide it. Exercise the
// real raw-transaction commit owner at capacity, then compare its cached byte
// components with a fresh stored-data calculation after update/deletion.
func TestTransactionQuotaHiddenPriorEntities(t *testing.T) {
	for _, hidden := range []string{"manual", "decayed"} {
		for _, entity := range []string{"node", "edge"} {
			for _, operation := range []string{"update", "delete"} {
				for _, quota := range []string{"count", "bytes"} {
					t.Run(strings.Join([]string{hidden, entity, operation, quota}, "/"), func(t *testing.T) {
						manager, db := setupTestManager(t)
						engine, err := manager.GetStorage(db)
						require.NoError(t, err)
						tracker := engine.(*sizeTrackingEngine)
						base := manager.inner.(*storage.MemoryEngine)
						for _, id := range []storage.NodeID{"a", "b"} {
							_, err := engine.CreateNode(&storage.Node{ID: id})
							require.NoError(t, err)
						}
						old := time.Now().Add(-72 * time.Hour)
						properties := map[string]interface{}{"payload": strings.Repeat("x", 2048)}
						if entity == "node" {
							_, err = engine.CreateNode(&storage.Node{ID: "n", Labels: []string{"Doc"}, Properties: properties, CreatedAt: old, UpdatedAt: old})
							require.NoError(t, err)
						} else {
							require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK", Properties: properties, CreatedAt: old, UpdatedAt: old}))
						}
						if hidden == "manual" {
							if entity == "node" {
								require.NoError(t, base.SetManualSuppression(storage.StoredEntityNodeKind, string(db+":n"), true))
							} else {
								require.NoError(t, base.SetManualSuppression(storage.StoredEntityEdgeKind, string(db+":e"), true))
							}
						} else {
							base.SetDecayEnabled(true)
							scope := knowledgepolicy.ScopeNode
							binding := knowledgepolicy.DecayProfileBinding{Name: "hidden", ProfileRef: "hidden", TargetLabels: []string{"Doc"}}
							if entity == "edge" {
								scope = knowledgepolicy.ScopeEdge
								binding = knowledgepolicy.DecayProfileBinding{Name: "hidden", ProfileRef: "hidden", TargetEdgeType: "LINK", IsEdge: true}
							}
							schema := base.GetSchemaForNamespace(db)
							require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "hidden", Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
							require.NoError(t, schema.CreateDecayProfileBinding(binding))
						}
						if entity == "node" {
							_, err = engine.GetNode("n")
						} else {
							_, err = engine.GetEdge("e")
						}
						require.ErrorIs(t, err, storage.ErrNotFound, "the prior entity must really be hidden")
						total, _, _ := manager.GetStorageSize(context.Background(), db)
						nodes, err := engine.NodeCount()
						require.NoError(t, err)
						edges, err := engine.EdgeCount()
						require.NoError(t, err)
						limits := &Limits{Storage: StorageLimits{MaxNodes: nodes, MaxEdges: edges}}
						if quota == "bytes" {
							limits = &Limits{Storage: StorageLimits{MaxBytes: total}}
						}
						require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, limits))
						tx, err := base.BeginTransaction()
						require.NoError(t, err)
						t.Cleanup(func() { _ = tx.Rollback() })
						require.NoError(t, tx.SetNamespace(db))
						if entity == "node" {
							if operation == "delete" {
								err = tx.DeleteGraphNode(storage.NodeID(db+":n"), storage.CascadeEdges)
							} else {
								prior, getErr := tx.GetStoredNode(storage.NodeID(db + ":n"))
								require.NoError(t, getErr)
								prior.Properties = map[string]interface{}{"payload": "small"}
								err = tx.UpdateGraphNode(prior, storage.ClientNodeEdits)
							}
						} else if operation == "delete" {
							err = tx.DeleteEdge(storage.EdgeID(db + ":e"))
						} else {
							prior, getErr := tx.GetStoredEdge(storage.EdgeID(db + ":e"))
							require.NoError(t, getErr)
							prior.Properties = map[string]interface{}{"payload": "small"}
							err = tx.UpdateEdge(prior)
						}
						require.NoError(t, err)
						require.NoError(t, tracker.CommitTransaction(context.Background(), tx), "update or deletion does not insert the hidden entity")
						wantNodes, wantEdges, err := tracker.manager.calculateStorageSizeFromEngine(context.Background(), tracker.Engine)
						require.NoError(t, err)
						got, gotNodes, gotEdges := manager.GetStorageSize(context.Background(), db)
						require.Equal(t, wantNodes, gotNodes, "node byte delta must include the hidden prior body")
						require.Equal(t, wantEdges, gotEdges, "edge byte delta must include the hidden prior body")
						require.Equal(t, wantNodes+wantEdges, got)
						require.Less(t, got, total, "shrinking or removing stored data releases quota")
					})
				}
			}
		}
	}
}
