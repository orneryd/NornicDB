// Author: Lane #1644 (Codex). A public procedure consumes the issued API.
// Nornic2's contract makes composite bulk deletion elementwise point deletion.
package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFoundationPublicCompositeBulkDeletion(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		for _, word := range []string{"", "T", "W", "TW", "WT"} {
			for _, schedule := range []string{"collision", "qualified", "missing", "refused"} {
				t.Run(kind+"/"+word+"/"+schedule, func(t *testing.T) {
					var pointError error
					var pointState [2]bool
					cause := errors.New("selected physical write owner refused")
					for _, bulk := range []bool{false, true} {
						base := storage.NewMemoryEngine()
						t.Cleanup(func() { _ = base.Close() })
						a := storage.NewNamespacedEngine(base, "first")
						b := storage.NewNamespacedEngine(base, "second")
						for _, child := range []storage.Engine{a, b} {
							if kind == "node" {
								_, err := child.CreateNode(&storage.Node{ID: "shared", Labels: []string{"Doc"}})
								require.NoError(t, err)
							} else {
								for _, id := range []storage.NodeID{"n", "m"} {
									_, err := child.CreateNode(&storage.Node{ID: id})
									require.NoError(t, err)
								}
								require.NoError(t, child.CreateEdge(&storage.Edge{ID: "shared", StartNode: "n", EndNode: "m", Type: "R"}))
							}
						}
						composite := storage.NewCompositeEngine(map[string]storage.Engine{"a": a, "b": b}, map[string]string{"a": "first", "b": "second"}, map[string]string{"a": "read_write", "b": "read_write"})
						refusals := 0
						if schedule == "refused" {
							composite = composite.WithConstituentAccess(func(alias string, child storage.Engine, write bool) (storage.Engine, error) {
								if alias == "first" && write {
									refusals++
									return nil, cause
								}
								return child, nil
							})
						}
						var view storage.Engine = composite
						for _, token := range word {
							if token == 'T' {
								view = storage.NewTracedEngine(view)
							} else {
								wal, err := storage.NewWAL(t.TempDir(), nil)
								require.NoError(t, err)
								t.Cleanup(func() { _ = wal.Close() })
								view = storage.NewWALEngine(view, wal)
							}
						}
						id := "shared"
						if schedule == "qualified" {
							if kind == "node" {
								id = storage.SourceNodeElementID("second", storage.NodeID(id))
							} else {
								id = storage.SourceRelationshipElementID("second", storage.EdgeID(id))
							}
						}
						if schedule == "missing" {
							id = "absent"
						}
						const procedure = "foundation.bulkDeletion"
						cleanupAdmissionProcedure(t, procedure)
						require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: procedure, Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "done", Type: "BOOLEAN"}}}, func(_ context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
							issued, err := NewStorageExecutor(view).FunctionStorage("")
							if err != nil {
								return nil, err
							}
							if kind == "node" {
								if bulk {
									err = issued.BulkDeleteNodes([]storage.NodeID{storage.NodeID(id)})
								} else {
									err = issued.DeleteNode(storage.NodeID(id))
								}
							} else {
								if bulk {
									err = issued.BulkDeleteEdges([]storage.EdgeID{storage.EdgeID(id)})
								} else {
									err = issued.DeleteEdge(storage.EdgeID(id))
								}
							}
							if err != nil {
								return nil, err
							}
							return &ExecuteResult{Columns: []string{"done"}, Rows: [][]interface{}{{true}}, Stats: &QueryStats{}}, nil
						}))
						owner := NewStorageExecutor(storage.NewNamespacedEngine(base, "host"))
						_, err := owner.Execute(context.Background(), "CALL "+procedure+"() YIELD done RETURN done", nil)
						if schedule == "refused" {
							require.Positive(t, refusals, "the public path must reach the selected physical write refusal")
						}
						var state [2]bool
						for index, child := range []storage.Engine{a, b} {
							var readErr error
							if kind == "node" {
								_, readErr = child.GetNode("shared")
							} else {
								_, readErr = child.GetEdge("shared")
							}
							require.True(t, readErr == nil || errors.Is(readErr, storage.ErrNotFound))
							state[index] = readErr == nil
						}
						require.NoError(t, base.Close())
						if !bulk {
							pointError = err
							pointState = state
							continue
						}
						if pointError == nil {
							require.NoError(t, err)
						} else if schedule == "missing" {
							require.NoError(t, err, "bulk retains its documented missing-ID skip")
						} else {
							require.EqualError(t, err, pointError.Error())
						}
						require.Equal(t, pointState, state, "bulk must consume the point source; it cannot broadcast %s", id)
						if schedule == "collision" {
							require.Equal(t, [2]bool{false, true}, state)
						}
						if schedule == "qualified" {
							require.Equal(t, [2]bool{true, false}, state)
						}
						if schedule == "refused" {
							require.ErrorIs(t, err, cause)
							require.Equal(t, [2]bool{true, true}, state)
						}
						if schedule == "missing" {
							require.NoError(t, err)
							require.Equal(t, [2]bool{true, true}, state)
						}
					}
				})
			}
		}
	}
}

// These routes require two different constituent authorities. A direct view
// must refuse before either mutation, since it has no distributed atomic owner.
func TestFoundationPublicCompositeBulkAdmissionIsAtomic(t *testing.T) {
	for _, kind := range []string{"node", "edge"} {
		t.Run(kind, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			children := []storage.Engine{storage.NewNamespacedEngine(base, "first"), storage.NewNamespacedEngine(base, "second")}
			for _, child := range children {
				for _, id := range []storage.NodeID{"n", "m"} {
					_, err := child.CreateNode(&storage.Node{ID: id})
					require.NoError(t, err)
				}
				require.NoError(t, child.CreateEdge(&storage.Edge{ID: "e", StartNode: "n", EndNode: "m", Type: "R"}))
			}
			composite := storage.NewCompositeEngine(map[string]storage.Engine{"a": children[0], "b": children[1]}, map[string]string{"a": "first", "b": "second"}, map[string]string{"a": "read_write", "b": "read_write"})
			cause := errors.New("second constituent write refused")
			composite = composite.WithConstituentAccess(func(database string, child storage.Engine, write bool) (storage.Engine, error) {
				if write && database == "second" {
					return nil, cause
				}
				return child, nil
			})
			procedure := "foundation.atomic_bulk"
			cleanupAdmissionProcedure(t, procedure)
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: procedure, Mode: ProcedureModeWrite, Returns: []ProcedureColumn{{Name: "done", Type: "BOOLEAN"}}}, func(_ context.Context, _ *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
				issued, err := NewStorageExecutor(composite).FunctionStorage("")
				if err != nil {
					return nil, err
				}
				if kind == "node" {
					err = issued.BulkDeleteNodes([]storage.NodeID{storage.NodeID(storage.SourceNodeElementID("first", "n")), storage.NodeID(storage.SourceNodeElementID("second", "n"))})
				} else {
					err = issued.BulkDeleteEdges([]storage.EdgeID{storage.EdgeID(storage.SourceRelationshipElementID("first", "e")), storage.EdgeID(storage.SourceRelationshipElementID("second", "e"))})
				}
				if err != nil {
					return nil, err
				}
				return &ExecuteResult{Columns: []string{"done"}, Rows: [][]interface{}{{true}}, Stats: &QueryStats{}}, nil
			}))
			executor := NewStorageExecutor(storage.NewNamespacedEngine(base, "host"))
			_, err := executor.Execute(context.Background(), "CALL "+procedure+"() YIELD done RETURN done", nil)
			require.Error(t, err)
			for _, child := range children {
				if kind == "node" {
					_, err = child.GetNode("n")
				} else {
					_, err = child.GetEdge("e")
				}
				require.NoError(t, err, "refusal must leave the entire bulk unchanged")
			}
		})
	}
}
