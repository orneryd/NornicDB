package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestPatternComprehensionWhereTakesIn: an IN in a pattern comprehension's
// WHERE or projection doesn't make it a list comprehension; it is a value in
// every position, as in Neo4j 5.26.30 (#1109). A bare pattern in a
// projection is still Neo4j's SyntaxError.
func TestPatternComprehensionWhereTakesIn(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pcin"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (c:Company {id: 1})<-[:COMPANY]-(:Assignment {role: 'owner'}), (c)<-[:COMPANY]-(:Assignment {role: 'accountant'})", nil)
	require.NoError(t, err)
	params := map[string]interface{}{"roles": []interface{}{"owner", "spouse"}}

	for statement, want := range map[string]interface{}{
		"MATCH (c:Company {id: 1}) RETURN [(c)<-[:COMPANY]-(a:Assignment) WHERE a.role IN ['owner', 'spouse'] | a.role] AS v":  []interface{}{"owner"},
		"MATCH (c:Company {id: 1}) RETURN [(c)<-[:COMPANY]-(a:Assignment) WHERE a.role IN $roles | a.role] AS v":               []interface{}{"owner"},
		"MATCH (c:Company {id: 1}) RETURN [(c)<-[:COMPANY]-(a:Assignment) WHERE NOT a.role IN ['owner'] | a.role] AS v":        []interface{}{"accountant"},
		"MATCH (c:Company {id: 1}) WITH [(c)<-[:COMPANY]-(a:Assignment) WHERE a.role IN ['owner'] | a.role] AS v RETURN v":     []interface{}{"owner"},
		"MATCH (c:Company {id: 1}) RETURN [p = (c)<-[:COMPANY]-(a:Assignment) WHERE a.role IN ['owner'] | length(p)] AS v":     []interface{}{int64(1)},
		"MATCH (c:Company {id: 1}) WHERE size([(c)<-[:COMPANY]-(a) WHERE a.role IN ['owner'] | a]) = 1 RETURN c.id AS v":       int64(1),
		"MATCH (c:Company {id: 1}) SET c.roles = [(c)<-[:COMPANY]-(a) WHERE a.role IN ['owner'] | a.role] RETURN c.roles AS v": []interface{}{"owner"},
	} {
		result, err := exec.Execute(ctx, statement, params)
		require.NoError(t, err, statement)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, statement)
	}

	// Nested in a comparison, a CASE, a list or a function, over a
	// variable-length pattern and reading an outer variable.
	for statement, want := range map[string]interface{}{
		"MATCH (c:Company {id: 1}) RETURN CASE WHEN size([(c)<-[:COMPANY]-(a) WHERE a.role IN $roles | a]) > 0 THEN 1 ELSE 0 END AS v":        int64(1),
		"MATCH (c:Company {id: 1}) WITH c, ['owner'] AS keep WHERE size([(c)<-[:COMPANY]-(a) WHERE a.role IN keep | a]) = 1 RETURN c.id AS v": int64(1),
		"MATCH (c:Company {id: 1}) UNWIND [[(c)<-[:COMPANY*1..2]-(a) WHERE a.role IN ['owner'] | a.role]] AS v RETURN v":                      []interface{}{"owner"},
		"MATCH (c:Company {id: 1}) RETURN head([(c)<-[:COMPANY]-(a) WHERE a.role IN ['owner'] | a.role]) AS v":                                "owner",
	} {
		result, err := exec.Execute(ctx, statement, params)
		require.NoError(t, err, statement)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, statement)
	}

	// A list comprehension with its own IN inside the projection.
	result, err := exec.Execute(ctx, "MATCH (c:Company {id: 1}) RETURN [(c)<-[:COMPANY]-(a:Assignment) | [r IN [a.role] WHERE r IN ['owner'] | r]] AS v", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.ElementsMatch(t, []interface{}{[]interface{}{}, []interface{}{"owner"}}, result.Rows[0][0])

	_, err = exec.Execute(ctx, "MATCH (c:Company {id: 1}) RETURN (c)<-[:COMPANY]-() AS v", nil)
	require.ErrorContains(t, err, "pattern expressions are only valid as predicates in WHERE")
}
