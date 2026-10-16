package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestVectorSettersUpdateVersionTime(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			if direct && explicit {
				// The direct-only engine deliberately has no transaction API.
				continue
			}
			for _, relationship := range []bool{false, true} {
				for _, byID := range []bool{false, true} {
					t.Run(fmt.Sprintf("direct=%v/explicit=%v/relationship=%v/id=%v", direct, explicit, relationship, byID), func(t *testing.T) {
						store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_clock")
						old := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
						for _, id := range []storage.NodeID{"a", "b"} {
							_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Clock"}, Properties: map[string]interface{}{"id": string(id)}, CreatedAt: old, UpdatedAt: old})
							require.NoError(t, err)
						}
						require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "VERSION", Properties: map[string]interface{}{}, CreatedAt: old, UpdatedAt: old}))
						var engine storage.Engine = store
						if direct {
							engine = &nonTransactionalEngine{Engine: store}
						}
						executor := NewStorageExecutor(engine)
						ctx := context.Background()
						t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
						prefix, procedure, argument, suffix := "MATCH (n:Clock {id:'a'}) ", "db.create.setNodeVectorProperty", "n", " RETURN n AS entity"
						if relationship {
							prefix, procedure, argument, suffix = "MATCH ()-[r:VERSION]->() ", "db.create.setRelationshipVectorProperty", "r", " RETURN r AS entity"
						}
						if byID {
							prefix, suffix, argument = "", "", "'a'"
							if relationship {
								argument = "'e'"
							}
						}
						previous := old
						for step, vector := range []string{"[1.0,2.0]", "[1.0,2.0]", "[3.0,4.0]"} {
							if explicit {
								_, err := executor.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
							}
							before := time.Now()
							result, err := executor.Execute(ctx, prefix+"CALL "+procedure+"("+argument+", 'emb', "+vector+")"+suffix, nil)
							require.NoError(t, err)
							after := time.Now()
							if explicit {
								_, err = executor.Execute(ctx, "COMMIT", nil)
								require.NoError(t, err)
							}
							var created, updated time.Time
							var properties map[string]interface{}
							if relationship {
								edge, err := store.GetEdge("e")
								require.NoError(t, err)
								created, updated, properties = edge.CreatedAt, edge.UpdatedAt, edge.Properties
							} else {
								node, err := store.GetNode("a")
								require.NoError(t, err)
								created, updated, properties = node.CreatedAt, node.UpdatedAt, node.Properties
							}
							require.True(t, created.Equal(old), "preserve creation time")
							if step == 1 {
								require.True(t, updated.Equal(previous), "identical vector is a no-op")
							} else {
								require.True(t, updated.After(previous), "changed vector advances version time")
								require.False(t, updated.Before(before), "time is inside write interval")
								require.False(t, updated.After(after), "time is inside write interval")
							}
							want := []float64{1, 2}
							if step == 2 {
								want = []float64{3, 4}
							}
							require.Equal(t, want, properties["emb"])
							if !byID {
								require.Len(t, result.Rows, 1)
								var boundCreated, boundUpdated time.Time
								switch entity := result.Rows[0][0].(type) {
								case *storage.Node:
									boundCreated, boundUpdated = entity.CreatedAt, entity.UpdatedAt
									require.Equal(t, want, entity.Properties["emb"])
								case *storage.Edge:
									boundCreated, boundUpdated = entity.CreatedAt, entity.UpdatedAt
									require.Equal(t, want, entity.Properties["emb"])
								default:
									t.Fatalf("unexpected bound entity: %T", entity)
								}
								require.True(t, boundCreated.Equal(old))
								require.True(t, boundUpdated.Equal(updated), "same-statement RETURN sees stored version time")
							}
							previous = updated
						}
					})
				}
			}
		}
	}
}
