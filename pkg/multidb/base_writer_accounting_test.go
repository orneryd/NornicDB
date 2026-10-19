package multidb

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The accounting boundary includes physical writers which intentionally bypass
// managed admission: policy/retention maintenance, raw transactions and workers.
func TestBaseWritersPublishStoredPairAccounting(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, route := range []string{"create", "update", "delete", "bulk", "transaction", "implicit-ack", "embedding-sidecar", "separate-embeddings", "prefix-drop", "restore"} {
			t.Run(fmt.Sprintf("buffered=%v/%s", buffered, route), func(t *testing.T) {
				base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered})
				require.NoError(t, err)
				manager, err := NewDatabaseManager(base, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, manager.Close()) })
				const db = "base_accounting"
				require.NoError(t, manager.CreateDatabase(db))
				engine, err := manager.GetStorage(db)
				require.NoError(t, err)
				for _, id := range []storage.NodeID{"a", "b", "n"} {
					_, err = engine.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"payload": "original"}})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "LINK"}))
				require.NoError(t, base.FlushWriteBehind())
				manager.GetStorageSize(db)
				assertExact := func() {
					t.Helper()
					nodes, edges, err := manager.calculateStorageSizeFromEngine(engine)
					require.NoError(t, err)
					total, gotNodes, gotEdges := manager.GetStorageSize(db)
					require.Equal(t, []int64{nodes + edges, nodes, edges}, []int64{total, gotNodes, gotEdges})
					allNodes, err := storage.CollectStoredNodes(context.Background(), engine)
					require.NoError(t, err)
					allEdges, err := storage.CollectStoredEdges(context.Background(), engine)
					require.NoError(t, err)
					nc, err := engine.NodeCount()
					require.NoError(t, err)
					ec, err := engine.EdgeCount()
					require.NoError(t, err)
					require.Equal(t, int64(len(allNodes)), nc)
					require.Equal(t, int64(len(allEdges)), ec)
				}
				node, err := base.GetStoredNode(db + ":n")
				require.NoError(t, err)
				edge, err := base.GetStoredEdge(db + ":e")
				require.NoError(t, err)
				switch route {
				case "create":
					_, err = base.CreateNode(&storage.Node{ID: db + ":extra"})
					require.NoError(t, err)
					require.NoError(t, base.CreateEdge(&storage.Edge{ID: db + ":extra", StartNode: db + ":a", EndNode: db + ":b", Type: "LINK"}))
				case "update":
					node.Properties["payload"] = strings.Repeat("x", 100)
					edge.Properties = map[string]any{"payload": "edge"}
					require.NoError(t, base.UpdateNode(node))
					require.NoError(t, base.UpdateEdge(edge))
				case "delete":
					// Retention uses the same base deletion, including its cascade.
					require.NoError(t, base.DeleteNode(db+":a"))
				case "bulk":
					require.NoError(t, base.BulkCreateNodes([]*storage.Node{{ID: db + ":extra"}}))
					require.NoError(t, base.BulkCreateEdges([]*storage.Edge{{ID: db + ":extra", StartNode: db + ":a", EndNode: db + ":b", Type: "LINK"}}))
				case "transaction", "implicit-ack":
					tx, err := base.BeginTransaction()
					require.NoError(t, err)
					require.NoError(t, tx.SetNamespace(db))
					if route == "implicit-ack" {
						tx.SetImplicit(true)
					}
					node.Properties["payload"] = "new"
					edge.Properties = map[string]any{"payload": "new"}
					require.NoError(t, tx.UpdateNode(node))
					require.NoError(t, tx.UpdateEdge(edge))
					require.NoError(t, tx.Commit())
				case "embedding-sidecar":
					node.ChunkEmbeddings = [][]float32{{1, 2, 3}}
					node.EmbedMeta = map[string]any{"model": "fixed"}
					require.NoError(t, base.UpdateNodeEmbeddingSidecar(node))
				case "separate-embeddings":
					node.ChunkEmbeddings = [][]float32{make([]float32, 20000)}
					for i := range node.ChunkEmbeddings[0] {
						node.ChunkEmbeddings[0][i] = float32(i) + .25
					}
					require.NoError(t, base.UpdateNode(node))
				case "prefix-drop":
					_, _, err = base.DeleteByPrefix(db + ":")
					require.NoError(t, err)
				case "restore":
					backup := filepath.Join(t.TempDir(), "backup.bak")
					require.NoError(t, base.Backup(backup))
					require.NoError(t, engine.DeleteNode("a"))
					require.NoError(t, base.Restore(backup))
				}
				assertExact() // an ACK is accounted before replay, not just at Flush
				require.NoError(t, base.FlushWriteBehind())
				assertExact() // replay never charges the same mutation twice
			})
		}
	}
}

// The real background flusher remains running while it rewrites both bodies.
// The old cache misses two bytes per false->true VisibilitySuppressed field.
func TestAccountingWithActivePolicyFlusher(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint("buffered=", buffered), func(t *testing.T) {
			base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered})
			require.NoError(t, err)
			base.SetDecayEnabled(true) // configure before any worker starts
			manager, err := NewDatabaseManager(base, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			const db = "policy_accounting"
			require.NoError(t, manager.CreateDatabase(db))
			engine, err := manager.GetStorage(db)
			require.NoError(t, err)
			schema := base.GetSchemaForNamespace(db)
			for _, isEdge := range []bool{false, true} {
				name, scope := "nodes", knowledgepolicy.ScopeNode
				binding := knowledgepolicy.DecayProfileBinding{Name: name, ProfileRef: name, TargetLabels: []string{"Expired"}}
				if isEdge {
					name, scope = "edges", knowledgepolicy.ScopeEdge
					binding = knowledgepolicy.DecayProfileBinding{Name: name, ProfileRef: name, IsEdge: true, TargetEdgeType: "EXPIRED"}
				}
				require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: name, Scope: scope, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 1, VisibilityThreshold: .1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
				require.NoError(t, schema.CreateDecayProfileBinding(binding))
			}
			old := time.Now().Add(-72 * time.Hour)
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err = engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Expired"}, CreatedAt: old, UpdatedAt: old})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "EXPIRED", CreatedAt: old, UpdatedAt: old}))
			before, _, _ := manager.GetStorageSize(db)
			require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: 2, MaxEdges: 1, MaxBytes: before}}))
			acc := knowledgepolicy.NewAccessAccumulator(true, 100)
			// Access metadata retains the kind from the entity's earlier visible
			// access; a decayed entity is no longer visible to GetEntityMeta.
			require.NoError(t, base.PutAccessMeta(db+":e", &knowledgepolicy.AccessMetaEntry{TargetID: db + ":e", TargetScope: knowledgepolicy.ScopeEdge}))
			flusher := knowledgepolicy.NewAccessFlusher(acc, base, 5*time.Millisecond)
			flusher.SetPropertySuppression(base.ScorerForNamespace, base, nil)
			flusher.SetSuppressionRecheck(func(id string, meta knowledgepolicy.EntityMeta) {
				_, _ = base.EnqueueDeindexIfSuppressed(id, meta.Scope == knowledgepolicy.ScopeEdge)
			})
			flusher.Start(context.Background())
			t.Cleanup(flusher.Stop)
			for _, id := range []string{db + ":a", db + ":b", db + ":e"} {
				acc.IncrementAccess(id)
			}
			require.Eventually(t, func() bool {
				node, nerr := base.GetStoredNode(db + ":a")
				edge, eerr := base.GetStoredEdge(db + ":e")
				if nerr != nil || eerr != nil || !node.VisibilitySuppressed || !edge.VisibilitySuppressed {
					return false
				}
				nodes, edges, err := manager.calculateStorageSizeFromEngine(engine)
				total, _, _ := manager.GetStorageSize(db)
				return err == nil && total == nodes+edges && total == before+6
			}, 5*time.Second, time.Millisecond)
			current, _, _ := manager.GetStorageSize(db)
			require.NoError(t, manager.SetDatabaseLimits(db, &Limits{Storage: StorageLimits{MaxNodes: 2, MaxEdges: 1, MaxBytes: current}}))
			// The follow-up unchanged-payload writes share the new persisted flags.
			node, err := storage.GetStoredNode(engine, "a")
			require.NoError(t, err)
			require.NoError(t, engine.UpdateNode(node))
			edge, err := storage.GetStoredEdge(engine, "e")
			require.NoError(t, err)
			require.NoError(t, engine.UpdateEdge(edge))
			after, _, _ := manager.GetStorageSize(db)
			require.Equal(t, current, after)
			nodes, edges, err := manager.calculateStorageSizeFromEngine(engine)
			require.NoError(t, err)
			require.Equal(t, nodes+edges, after)
		})
	}
}
