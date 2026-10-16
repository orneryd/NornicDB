package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSelectedPathDecompositions(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "decompositions"))
	ctx := context.Background()
	_, err := e.Execute(ctx, "CREATE (:S {id:1})-[:T]->(:S {id:2})-[:T]->(:S {id:3})-[:T]->(:S {id:4})", nil)
	require.NoError(t, err)
	for _, pattern := range []string{"(()-[r:T]->())+(()-[s:T]->())+", "(()-[r:T]->()){1,2}(()-[s:T]->()){1,2}", "(()-[r:T]->()){0,3}(()-[s:T]->()){0,3}"} {
		for _, prefix := range []string{"", "MATCH (a:S {id:1}), (b:S {id:4}) ", "MATCH (b:S {id:4}) "} {
			query := prefix + "MATCH p = %s(a:S {id:1})" + pattern + "(b:S {id:4}) RETURN size(r) AS r, size(s) AS s ORDER BY r,s"
			plain, err := e.Execute(ctx, fmt.Sprintf(query, ""), nil)
			require.NoError(t, err)
			for _, selector := range []string{"ALL SHORTEST ", "SHORTEST 4 ", "ANY 4 ", "SHORTEST 1 GROUPS "} {
				t.Run(prefix+pattern+selector, func(t *testing.T) {
					r, err := e.Execute(ctx, fmt.Sprintf(query, selector), nil)
					require.NoError(t, err)
					require.Equal(t, plain.Rows, r.Rows)
				})
			}
			for _, selector := range []string{"ANY SHORTEST ", "SHORTEST 1 ", "ANY "} {
				r, err := e.Execute(ctx, fmt.Sprintf(query, selector), nil)
				require.NoError(t, err)
				require.Len(t, r.Rows, 1)
				require.Contains(t, plain.Rows, r.Rows[0])
			}
		}
	}
	for _, call := range []string{"shortestPath", "allShortestPaths"} {
		r, err := e.Execute(ctx, "MATCH (a:S {id:1}), (b:S {id:4}) MATCH p = "+call+"((a)-[r:T*]->(b)) RETURN size(r), length(p)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3), int64(3)}}, r.Rows)
	}
}
