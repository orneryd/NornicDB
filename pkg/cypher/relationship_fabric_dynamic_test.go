package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationshipProvenanceThroughDynamicLocalFabric(t *testing.T) {
	base := newFabricCallRowsExecutor(t)
	ctx := context.Background()
	for _, end := range []string{"ROLLBACK", "COMMIT"} {
		t.Run(end, func(t *testing.T) {
			exec := fabricCallRowsCompositeExecutor(t, base)
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			defer exec.Execute(context.Background(), "ROLLBACK", nil)
			query := "USE comp CALL { USE comp.one CALL apoc.cypher.doIt($statement, {}) YIELD value RETURN value.kind AS kind } RETURN kind"
			result, err := exec.Execute(ctx, query, map[string]any{
				"statement": "CREATE (a:DynamicRel)-[r:`A|B`]->(b:DynamicRel) RETURN type(r) AS kind",
			})
			require.NoError(t, err)
			require.Equal(t, [][]any{{"A|B"}}, result.Rows)
			result, err = exec.Execute(ctx, "USE comp CALL { USE comp.one MATCH ()-[r:`A|B`]->() RETURN type(r) AS kind UNION ALL USE comp.two MATCH ()-[r:`A|B`]->() RETURN type(r) AS kind } RETURN kind", nil)
			require.NoError(t, err)
			require.Equal(t, [][]any{{"A|B"}}, result.Rows)
			_, err = exec.Execute(ctx, end, nil)
			require.NoError(t, err)
			result, err = base.Execute(ctx, "USE comp.one MATCH ()-[r:`A|B`]->() RETURN count(*) AS n", nil)
			require.NoError(t, err)
			want := int64(0)
			if end == "COMMIT" {
				want = 1
			}
			require.Equal(t, want, result.Rows[0][0])
		})
	}
}
