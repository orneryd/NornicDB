package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRelationshipDegreeDecodedTypes(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "degree"))
	_, err := exec.Execute(ctx, "CREATE (n:ReviewStart)-[:`A|B`]->(:ReviewEnd), (n)-[:R]->(), (n)-[:`tick``|type`]->(), (n)-[:`$(literal)`]->(), (n)-[:` `]->()", nil)
	require.NoError(t, err)
	start, err := exec.Execute(ctx, "MATCH (n:ReviewStart) RETURN n", nil)
	require.NoError(t, err)
	values := map[string]interface{}{"n": start.Rows[0][0]}
	for _, input := range []struct {
		types string
		want  int64
	}{
		{"`A|B`", 1}, {"A|B", 0}, {"R", 1}, {"`tick``|type`", 1},
		{"`$(literal)`", 1}, {"` `", 1}, {"`A|B`|R", 2}, {"Missing", 0},
	} {
		t.Run(input.types, func(t *testing.T) {
			body := "(n)-[:" + input.types + "]->()"
			// Repeat the actual consumer to cover retained shape facts as well
			// as the first parse, and check that the degree route is admitted.
			for iteration := 0; iteration < 2; iteration++ {
				count, admitted := exec.boundDegreeCount(ctx, body, values)
				require.True(t, admitted)
				require.Equal(t, input.want, count)
				result, err := exec.Execute(ctx, "MATCH (n:ReviewStart) RETURN COUNT { "+body+" } AS c, EXISTS { "+body+" } AS e", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{input.want, input.want != 0}}, result.Rows)
			}
		})
	}
	// Eligibility stays with the expression owner: richer patterns need the
	// traversal consumer to apply endpoints, properties, length and predicates.
	for _, body := range []string{
		"(n)-[:R]->(other)", "(n)-[:R]->(:ReviewEnd)", "(n)-[:R {p:1}]->()",
		"(n)-[:R*1..2]->()", "(n)-[:R]-()", "(n)-[:$($type)]->()",
		"(n)-[:R]->() WHERE true", "(n)-[:R]->()-[:R]->()",
	} {
		require.False(t, parseBoundDegreeShape(body).ok, body)
	}
}
