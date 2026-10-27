package multidb

import (
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDirectHiddenUpdateByteQuota(t *testing.T) {
	for _, hidden := range []string{"manual", "decayed"} {
		for _, entity := range []string{"node", "edge"} {
			t.Run(hidden+"/"+entity, func(t *testing.T) {
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

				total, nodeBytes, edgeBytes := manager.GetStorageSize(db)
				require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, &Limits{Storage: StorageLimits{MaxBytes: total}}))
				if entity == "node" {
					prior, err := storage.GetStoredNode(engine, "n")
					require.NoError(t, err)
					growth := storage.CopyNode(prior)
					growth.Properties = map[string]any{"payload": strings.Repeat("y", 8192)}
					require.ErrorIs(t, engine.UpdateNode(growth), ErrStorageLimitExceeded)
					after, err := storage.GetStoredNode(engine, "n")
					require.NoError(t, err)
					require.Equal(t, prior, after)
					require.NoError(t, engine.UpdateNode(prior))
				} else {
					prior, err := storage.GetStoredEdge(engine, "e")
					require.NoError(t, err)
					growth := storage.CopyEdge(prior)
					growth.Properties = map[string]any{"payload": strings.Repeat("y", 8192)}
					require.ErrorIs(t, engine.UpdateEdge(growth), ErrStorageLimitExceeded)
					after, err := storage.GetStoredEdge(engine, "e")
					require.NoError(t, err)
					require.Equal(t, prior, after)
					require.NoError(t, engine.UpdateEdge(prior))
				}
				got, gotNodes, gotEdges := manager.GetStorageSize(db)
				require.Equal(t, total, got)
				require.Equal(t, nodeBytes, gotNodes)
				require.Equal(t, edgeBytes, gotEdges)
				wantNodes, wantEdges, err := tracker.manager.calculateStorageSizeFromEngine(tracker.Engine)
				require.NoError(t, err)
				require.Equal(t, wantNodes, gotNodes)
				require.Equal(t, wantEdges, gotEdges)

			})
		}
	}
}
