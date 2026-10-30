package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Subquery expressions are evaluated where they stand, with the variables
// bound at that point (#1055): a pattern comprehension's own variables and the
// outer row's, a quantifier's variable, a subscript on the subquery itself.
// Each expectation is Neo4j 5.26.30's answer from the differential sweep.
func TestSubqueryExpressionsSeeTheirScope(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subqscope"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R {w: 1}]->(:Q {id: 2})-[:R {w: 2}]->(:Q {id: 3})", nil)
	require.NoError(t, err)
	const prefix = "MATCH p = (n:Q {id: 1})-[r:R]->(o:Q) WITH n, r, p, o, 1 AS i, 'ab' AS s "
	for query, want := range map[string]interface{}{
		// A pattern comprehension's subquery reads the comprehension's variable
		// and the outer row's scalar.
		"RETURN [(n)-[:R]->(x) WHERE EXISTS { MATCH (a:Q) WHERE a.id = x.id } | x.id] AS v":        []interface{}{int64(2)},
		"RETURN [(n)-[:R]->(x) WHERE EXISTS { MATCH (a:Q) WHERE a.id = x.id + i } | x.id] AS v":    []interface{}{int64(2)},
		"RETURN [(n)-[:R]->(x) WHERE EXISTS { (a:Q {id: x.id + i}) } | x.id] AS v":                 []interface{}{int64(2)},
		"RETURN [(n)-[:R]->(x) WHERE COUNT { MATCH (a:Q) WHERE a.id = x.id + i } > 0 | x.id] AS v": []interface{}{int64(2)},
		// A quantifier's, comprehension's and reduce's own variable.
		"RETURN any(v IN [1, 5] WHERE EXISTS { MATCH (a:Q) WHERE a.id = v }) AS v":             true,
		"RETURN all(v IN [1, 5] WHERE EXISTS { MATCH (a:Q) WHERE a.id = v }) AS v":             false,
		"RETURN [v IN [1, 5] | EXISTS { MATCH (a:Q) WHERE a.id = v }] AS v":                    []interface{}{true, false},
		"RETURN reduce(c = 0, v IN [1, 2, 5] | c + COUNT { MATCH (a:Q) WHERE a.id = v }) AS v": int64(2),
		// A subscript on the subquery itself, and a bound relationship reused
		// as a relationship.
		"RETURN COLLECT { MATCH (n)-[:R]->(x) RETURN x.id }[0] AS v": int64(2),
		"RETURN COUNT { (n)-[r]->() } AS v":                          int64(1),
	} {
		result, err := exec.Execute(ctx, prefix+query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	// An outer variable used in a subquery pattern as another kind is
	// rejected before the statement runs, as Neo4j's "s defined with
	// conflicting type String (expected Relationship)".
	for _, query := range []string{
		"RETURN COUNT { (n)-[s]->() } AS v",
		"RETURN EXISTS { (s)-->() } AS v",
		"RETURN EXISTS { MATCH (n)-[i]->() } AS v",
		"RETURN COUNT { (r)-->() } AS v",
		"RETURN any(v IN [1] WHERE EXISTS { (n)-[s]->() }) AS v",
	} {
		_, err := exec.Execute(ctx, prefix+query, nil)
		require.ErrorContains(t, err, "already bound to a different entity type", query)
	}
}

// A relationship variable bound in the row restricts a COUNT body's hop to
// that relationship: COUNT { (a)-[r]->() } is 1 for each of a's two
// relationships, not a's degree (#1055).
func TestCountBodyWithBoundRelationshipIsNotADegree(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "boundrel"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:D {id: 1})-[:R]->(:D {id: 2}), (a)-[:R]->(:D {id: 3})", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:D {id: 1})-[r:R]->(b) RETURN b.id AS b, COUNT { (a)-[r]->() } AS c, COUNT { (a)-->() } AS degree ORDER BY b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2), int64(1), int64(2)}, {int64(3), int64(1), int64(2)}}, result.Rows)
}
