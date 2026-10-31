package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Registered functions interpose real peer commits between MATCH's observation
// and mutation staging, without production test hooks or timing races.
func TestPR1655ExecuteObservedVanishedMutation(t *testing.T) {
	for _, wb := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, tc := range []struct {
				name, match, mutation string
				edge                  bool
				value                 interface{}
			}{
				{"node-property", "MATCH (n:Target)", "SET n.value = audit.erase(n)", false, "changed"},
				{"node-map-merge", "MATCH (n:Target)", "SET n += audit.erase(n)", false, map[string]interface{}{"value": "changed"}},
				{"node-map-replace", "MATCH (n:Target)", "SET n = audit.erase(n)", false, map[string]interface{}{"value": "changed"}},
				{"node-label", "MATCH (n:Target)", "SET n:$(audit.erase(n))", false, "Changed"},
				{"node-remove-property", "MATCH (n:Target)", "REMOVE n[audit.erase(n)]", false, "value"},
				{"node-remove-label", "MATCH (n:Target)", "REMOVE n:$(audit.erase(n))", false, "Old"},
				{"edge-property", "MATCH ()-[r:TARGET]->()", "SET r.value = audit.erase(r)", true, "changed"},
				{"edge-remove-property", "MATCH ()-[r:TARGET]->()", "REMOVE r[audit.erase(r)]", true, "value"},
				{"node-vector", "MATCH (n:Target)", "WITH n, audit.erase(n) AS deleted CALL db.create.setNodeVectorProperty(n, 'emb', [2.0,3.0])", false, int64(1)},
				{"edge-vector", "MATCH ()-[r:TARGET]->()", "WITH r, audit.erase(r) AS deleted CALL db.create.setRelationshipVectorProperty(r, 'emb', [2.0,3.0])", true, int64(1)},
			} {
				t.Run(fmt.Sprintf("wb=%v/explicit=%v/%s", wb, explicit, tc.name), func(t *testing.T) {
					base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: wb, WriteBehindInterval: time.Hour}))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, base.Close()) })
					store := storage.NewNamespacedEngine(base, "observed")
					for _, id := range []storage.NodeID{"a", "b"} {
						_, err = store.CreateNode(&storage.Node{ID: id})
						require.NoError(t, err)
					}
					require.NoError(t, base.FlushWriteBehind(nil))
					if tc.edge {
						require.NoError(t, store.CreateEdge(&storage.Edge{ID: "target", StartNode: "a", EndNode: "b", Type: "TARGET", Properties: map[string]interface{}{"value": "old", "emb": []float64{1, 2}}}))
					} else {
						_, err = store.CreateNode(&storage.Node{ID: "target", Labels: []string{"Target", "Old"}, Properties: map[string]interface{}{"value": "old", "emb": []float64{1, 2}}})
						require.NoError(t, err)
					}
					writer := NewStorageExecutor(store)
					ctx := context.Background()
					if explicit {
						_, err = writer.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = writer.Execute(ctx, "ROLLBACK", nil) })
					}
					created, deleted := 0, 0
					previous := PluginFunctionLookup
					t.Cleanup(func() { PluginFunctionLookup = previous })
					PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
						switch name {
						case "audit.create":
							return func(ctx context.Context) (int64, error) {
								created++
								require.Equal(t, 1, created)
								_, ok := writer.getStorage(ctx).(*transactionStorageWrapper)
								require.True(t, ok, "interposition occurs inside the native writer")
								return 1, nil
							}, true
						case "audit.erase":
							return func(observed interface{}) (interface{}, error) {
								deleted++
								require.Equal(t, 1, deleted)
								peer, err := base.BeginTransaction()
								if err != nil {
									return nil, err
								}
								defer peer.Rollback()
								if tc.edge {
									entity, ok := observed.(*storage.Edge)
									require.True(t, ok)
									require.Equal(t, "old", entity.Properties["value"])
									err = peer.DeleteEdge("observed:target")
								} else {
									entity, ok := observed.(*storage.Node)
									require.True(t, ok)
									require.Equal(t, "old", entity.Properties["value"])
									err = peer.DeleteGraphNode("observed:target", storage.CascadeEdges)
								}
								if err != nil {
									return nil, err
								}
								if err = peer.Commit(); err != nil {
									return nil, err
								}
								return tc.value, nil
							}, true
						}
						if previous != nil {
							handler, ok := previous(name)
							return handler, ok
						}
						return nil, false
					})
					query := "WITH audit.create() AS seed CREATE (:Marker {id:'writer'}) WITH seed " + tc.match + " " + tc.mutation
					_, err = writer.Execute(ctx, query, nil)
					require.Equal(t, 1, created)
					require.Equal(t, 1, deleted, "the target was actually read before peer deletion")
					if err == nil && explicit {
						_, err = writer.Execute(ctx, "COMMIT", nil)
					}
					require.ErrorIs(t, err, storage.ErrConflict)
					code, _ := nornicerrors.Neo4jCommitStatus(err)
					require.Equal(t, nornicerrors.TransientOutdated, code)
					if tc.edge {
						_, err = store.GetEdge("target")
					} else {
						_, err = store.GetNode("target")
					}
					require.ErrorIs(t, err, storage.ErrNotFound, "no resurrection")
					markers, err := store.GetNodesByLabel("Marker")
					require.NoError(t, err)
					require.Empty(t, markers, "failed mutation must not publish its marker")
				})
			}
		}
	}
}

// The live write-behind view can expose committed targets created after BEGIN.
// Read them through the actual statement transaction, then delete them before
// staging. A beginning-of-transaction presence snapshot cannot prove this read.
func TestPR1655ExecuteNewlyObservedTargetDisappears(t *testing.T) {
	for _, edge := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("edge=%v/explicit=%v", edge, explicit), func(t *testing.T) {
				base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour}))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, base.Close()) })
				store := storage.NewNamespacedEngine(base, "new_observation")
				for _, id := range []storage.NodeID{"a", "b"} {
					_, err = store.CreateNode(&storage.Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, base.FlushWriteBehind(nil))
				writer := NewStorageExecutor(store)
				ctx := context.Background()
				if explicit {
					_, err = writer.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = writer.Execute(ctx, "ROLLBACK", nil) })
				}
				observed, deleted := 0, 0
				previous := PluginFunctionLookup
				t.Cleanup(func() { PluginFunctionLookup = previous })
				PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
					switch name {
					case "audit.observe":
						return func(ctx context.Context) (interface{}, error) {
							observed++
							require.Equal(t, 1, observed)
							frame, ok := writer.getStorage(ctx).(*transactionStorageWrapper)
							require.True(t, ok)
							if edge {
								if err := store.CreateEdge(&storage.Edge{ID: "target", StartNode: "a", EndNode: "b", Type: "TARGET", Properties: map[string]interface{}{"value": "old"}}); err != nil {
									return nil, err
								}
								return frame.GetEdge("target")
							}
							if _, err := store.CreateNode(&storage.Node{ID: "target", Labels: []string{"Target"}, Properties: map[string]interface{}{"value": "old"}}); err != nil {
								return nil, err
							}
							return frame.GetNode("target")
						}, true
					case "audit.erase":
						return func(entity interface{}) (string, error) {
							deleted++
							require.Equal(t, 1, deleted)
							if edge {
								record, ok := entity.(*storage.Edge)
								require.True(t, ok)
								require.Equal(t, "old", record.Properties["value"])
								return "new", store.DeleteEdge("target")
							}
							record, ok := entity.(*storage.Node)
							require.True(t, ok)
							require.Equal(t, "old", record.Properties["value"])
							return "new", store.DeleteNode("target")
						}, true
					}
					if previous != nil {
						handler, ok := previous(name)
						return handler, ok
					}
					return nil, false
				})
				_, err = writer.Execute(ctx, "WITH audit.observe() AS entity CREATE (:Marker) WITH entity SET entity.value = audit.erase(entity)", nil)
				require.Equal(t, 1, observed)
				require.Equal(t, 1, deleted)
				require.ErrorIs(t, err, storage.ErrConflict, "the actual post-BEGIN observation must reach storage")
				require.NotErrorIs(t, err, storage.ErrNotFound)
				code, _ := nornicerrors.Neo4jCommitStatus(err)
				require.Equal(t, nornicerrors.TransientOutdated, code)
				markers, err := store.GetNodesByLabel("Marker")
				require.NoError(t, err)
				require.Empty(t, markers)
				if edge {
					_, err = store.GetEdge("target")
				} else {
					_, err = store.GetNode("target")
				}
				require.ErrorIs(t, err, storage.ErrNotFound)
			})
		}
	}
}

func TestPR1655ExecuteOwnDeleteIsNotRetryable(t *testing.T) {
	for _, edge := range []bool{false, true} {
		t.Run(fmt.Sprintf("edge=%v", edge), func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "own_delete")
			exec := NewStorageExecutor(store)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:Target {value:'old'})-[:TARGET {value:'old'}]->(:Other)", nil)
			require.NoError(t, err)
			query := "MATCH (n:Target) DETACH DELETE n WITH n SET n.value = 'new'"
			if edge {
				query = "MATCH ()-[r:TARGET]->() DELETE r WITH r SET r.value = 'new'"
			}
			_, err = exec.Execute(ctx, query, nil)
			require.NotErrorIs(t, err, storage.ErrConflict, "own deletion cannot be repaired by retrying")
			if edge {
				require.ErrorIs(t, err, storage.ErrNotFound)
				edges, readErr := store.GetEdgesByType("TARGET")
				require.NoError(t, readErr)
				require.Len(t, edges, 1, "failed relationship update rolls back its own deletion")
			} else {
				// DETACH DELETE clears the subsequent node view, which is skipped.
				require.NoError(t, err)
				nodes, readErr := store.GetNodesByLabel("Target")
				require.NoError(t, readErr)
				require.Empty(t, nodes, "SET cannot resurrect the node deleted by its own statement")
			}
		})
	}
}

func TestPR1655VectorStringMissingTargetRemainsOrdinary(t *testing.T) {
	for _, procedure := range []string{"db.create.setNodeVectorProperty", "db.create.setRelationshipVectorProperty"} {
		t.Run(procedure, func(t *testing.T) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "missing_vector")
			_, err := NewStorageExecutor(store).Execute(context.Background(), "CALL "+procedure+"('missing', 'emb', [1.0,2.0])", nil)
			require.ErrorIs(t, err, storage.ErrNotFound)
			require.NotErrorIs(t, err, storage.ErrConflict)
		})
	}
}
