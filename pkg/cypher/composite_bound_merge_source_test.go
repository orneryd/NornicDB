package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A cached pre-match and the create path must choose the same physical source.
// Neither may reinterpret a foreign native endpoint as the selected same-ID node.
func TestCompositeBoundMergeSourceBoundary(t *testing.T) {
	for _, mode := range []string{"autocommit", "explicit"} {
		for _, pattern := range []string{"(a)-[r:R]->(b)", "(b)<-[r:R]-(a)", "(a)-[r:R]-(b)"} {
			for _, scope := range []string{"own", "foreign endpoint", "foreign pair"} {
				t.Run(mode+"/"+pattern+"/"+scope, func(t *testing.T) {
					_, manager := newCompositeIdentityExecutor(t, false)
					one, err := manager.GetStorage("one")
					require.NoError(t, err)
					two, err := manager.GetStorage("two")
					require.NoError(t, err)
					startStore, endStore := one, one
					if scope != "own" {
						endStore = two
					}
					if scope == "foreign pair" {
						startStore = two
					}
					start, err := startStore.GetNode("same")
					require.NoError(t, err)
					end, err := endStore.GetNode("target")
					require.NoError(t, err)
					executor := NewStorageExecutor(one)
					ctx := context.Background()
					if mode == "explicit" {
						_, err = executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
					}
					result, queryErr := executor.Execute(ctx, "WITH $start AS a,$end AS b MERGE "+pattern+" ON MATCH SET r.touched = true RETURN count(*)", map[string]interface{}{"start": start, "end": end})
					if scope == "own" {
						require.NoError(t, queryErr)
						require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
						require.Zero(t, result.Stats.RelationshipsCreated)
					} else {
						require.Error(t, queryErr, "a foreign binding cannot match the selected graph's local relationship")
					}
					if mode == "explicit" {
						terminal := "ROLLBACK"
						if scope == "own" {
							terminal = "COMMIT"
						}
						_, err = executor.Execute(ctx, terminal, nil)
						require.NoError(t, err)
					}
					for _, database := range []struct {
						name  string
						store storage.Engine
					}{{"one", one}, {"two", two}} {
						edge, err := database.store.GetEdge("same-rel")
						require.NoError(t, err)
						if scope == "own" && database.name == "one" {
							require.Equal(t, true, edge.Properties["touched"])
						} else {
							require.NotContains(t, edge.Properties, "touched")
						}
						edges, err := database.store.GetEdgesByType("R")
						require.NoError(t, err)
						require.Len(t, edges, 1)
					}
				})
			}
		}
	}
}
