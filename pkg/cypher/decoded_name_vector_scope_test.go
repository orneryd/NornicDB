package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Score and score are different bindings. The vector shortcut must use the
// same binding identity as the general projection, predicate and order owners.
func TestDecodedNameVectorScope(t *testing.T) {
	for _, entity := range []string{"node", "relationship"} {
		t.Run(entity, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_scope"))
			ctx := context.Background()
			indexPattern, match := "(n:VectorScope)", "MATCH (n:VectorScope)"
			if entity == "relationship" {
				indexPattern, match = "()-[n:VectorScope]-()", "MATCH ()-[n:VectorScope]->()"
			}
			_, err := exec.Execute(ctx, fmt.Sprintf("CREATE VECTOR INDEX vector_scope FOR %s ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 2}}", indexPattern), nil)
			require.NoError(t, err)
			for _, seed := range []string{"{uuid:'a', rank:10, emb:[1.0,0.0]}", "{uuid:'b', rank:100, emb:[0.0,1.0]}"} {
				query := "CREATE (:VectorScope " + seed + ")"
				if entity == "relationship" {
					query = "CREATE ()-[:VectorScope " + seed + "]->()"
				}
				_, err = exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
			}
			cosine := "vector.similarity.cosine(n.emb,[1.0,0.0])"
			for _, tc := range []struct {
				name, tail, expected string
			}{
				{"order return alias", "RETURN n.uuid AS uuid, " + cosine + " AS Score, n.rank AS score ORDER BY score DESC LIMIT 1", "b"},
				{"order with alias", "WITH n, " + cosine + " AS Score RETURN n.uuid AS uuid, n.rank AS score ORDER BY score DESC LIMIT 1", "b"},
				{"predicate alias", "WITH n, n.rank AS score, " + cosine + " AS Score WHERE score > 50 RETURN n.uuid AS uuid, Score ORDER BY Score DESC LIMIT 1", "b"},
				{"projection alias", "WITH n, n.rank AS score, " + cosine + " AS Score RETURN n.uuid AS uuid, score ORDER BY Score DESC LIMIT 1", "a"},
				{"shadowed score", "WITH n, " + cosine + " AS score RETURN n.uuid AS uuid, n.rank AS score ORDER BY score DESC LIMIT 1", "b"},
				{"renamed entity", "WITH n AS N, " + cosine + " AS score RETURN N.uuid AS uuid, score ORDER BY score DESC LIMIT 1", "a"},
				{"ordinary score", "RETURN n.uuid AS uuid, " + cosine + " AS score ORDER BY score DESC LIMIT 1", "a"},
				{"quoted score", "RETURN n.uuid AS uuid, " + cosine + " AS ` score ` ORDER BY ` score ` DESC LIMIT 1", "a"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := match + " " + tc.tail
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					require.Len(t, result.Rows, 1, query)
					require.Equal(t, tc.expected, result.Rows[0][0], query)
					if tc.name == "projection alias" {
						require.EqualValues(t, 10, result.Rows[0][1], "score is the rank binding, not the cosine binding Score")
					}
				})
			}
		})
	}
}
