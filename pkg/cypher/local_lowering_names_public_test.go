package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Native identifier roles are acquired after the existing inline-pattern
// lowering, on every local backend. No guessed roles or extra grammar slots.
func TestLocalLoweringNamesPublic(t *testing.T) {
	old := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(old) })
	for _, norm := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(norm)
		for _, use := range []bool{false, true} {
			for _, tx := range []bool{false, true} {
				for i, item := range []struct {
					query string
					value int64
				}{
					{"MATCH (n:S WHERE `n`.id = 1) RETURN n.id AS v", 1},
					{"MATCH ()-[r:T WHERE `r`.w = 2]->() RETURN r.w AS v", 2},
					{"MATCH p = ANY SHORTEST (a:S {id: 1})(()-[:T]->())+(b:S WHERE `b`.id = 4) RETURN length(p) AS v", 2},
				} {
					t.Run(fmt.Sprintf("norm_%v/use_%v/tx_%v/%d", norm, use, tx, i), func(t *testing.T) {
						e := newFabricCallRowsExecutor(t)
						ctx := context.Background()
						setup := "CREATE (a:S {id: 1})-[:T {w: 2}]->(b:S {id: 2})-[:T {w: 3}]->(d:S {id: 4}), (a)-[:T {w: 3}]->(x:S {id: 3})-[:T {w: 4}]->(y:S {id: 5})-[:T {w: 5}]->(d)"
						if use {
							setup = "USE comp.one " + setup
						}
						_, err := e.Execute(ctx, setup, nil)
						require.NoError(t, err)
						query := item.query
						if use {
							e = fabricCallRowsCompositeExecutor(t, e)
							query = "USE comp.one " + query
						}
						if tx {
							_, err := e.Execute(context.Background(), "BEGIN", nil)
							require.NoError(t, err)
							defer e.Execute(ctx, "ROLLBACK", nil)
						}
						actual, err := e.Execute(ctx, query, nil)
						require.NoError(t, err)
						require.Equal(t, []string{"v"}, actual.Columns)
						require.Equal(t, [][]interface{}{{item.value}}, actual.Rows)
					})
				}
			}
		}
	}
}
