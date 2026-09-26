package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCloneStringInterfaceMap_IsIndependentCopy(t *testing.T) {
	original := map[string]interface{}{"a": 1, "b": "x"}
	clone := cloneStringInterfaceMap(original)
	require.Equal(t, original, clone)
	clone["a"] = 999
	require.Equal(t, 1, original["a"], "mutating the clone must not affect the original")
}

// TestCallTailPlansEvaluateLikeThePipeline pins CALL tails the compiled
// CALL-tail plans run (WITH … WHERE … RETURN, and MATCH over a yielded
// relationship): their WHERE, projections and ORDER BY / LIMIT evaluate with
// the pipeline's WITH and RETURN appliers, so null tests, string operators,
// IN, comparisons, AND / OR / NOT, labels(), properties(), type() and
// startNode() on yielded nodes and relationships behave as on any other
// route (#530). Expected rows are Neo4j 5.26.30's.
func TestCallTailPlansEvaluateLikeThePipeline(t *testing.T) {
	setup := []string{
		"CREATE FULLTEXT INDEX ct_ft IF NOT EXISTS FOR (n:CT) ON EACH [n.name]",
		"CREATE FULLTEXT INDEX ctr_ft IF NOT EXISTS FOR ()-[r:CTR]-() ON EACH [r.name]",
		"CREATE (a:CT {name:'alpha one', age:37, tags:['math','code']})-[:CTR {name:'beta x', w:1}]->(b:CT {name:'alpha two', age:41, tags:['art']}), (c:CT {name:'alpha lovelace', age:20})-[:CTR {name:'beta y', w:2}]->(a)",
	}
	for _, tc := range []struct {
		query  string
		params map[string]interface{}
		want   [][]interface{}
	}{
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.age IS NOT NULL RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha lovelace"}, {"alpha one"}, {"alpha two"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.missing IS NULL RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha lovelace"}, {"alpha one"}, {"alpha two"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.name STARTS WITH 'alpha b' RETURN node.name AS name ORDER BY name", nil, [][]interface{}{}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.name ENDS WITH 'one' RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha one"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.name CONTAINS 'love' RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha lovelace"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE 'code' IN node.tags RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha one"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.age >= 37 RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha one"}, {"alpha two"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE NOT node.age < 37 RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha one"}, {"alpha two"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.age = 37 AND node.name STARTS WITH 'alpha' RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha one"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.age = 0 OR node.name ENDS WITH 'two' RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha two"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE NOT (node.age IN [37, 41]) RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha lovelace"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.age IN $ages RETURN node.name AS name ORDER BY name", map[string]interface{}{"ages": []interface{}{int64(37), int64(20)}}, [][]interface{}{{"alpha lovelace"}, {"alpha one"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE node.tags IS NULL RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha lovelace"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node WHERE size(node.tags) > 1 RETURN node.name AS name ORDER BY name", nil, [][]interface{}{{"alpha one"}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node, score WITH node, score WHERE score > 0 RETURN node.name AS name, labels(node) AS l, properties(node).age AS age, elementId(node) = elementId(node) AS e ORDER BY name", nil, [][]interface{}{{"alpha lovelace", []interface{}{"CT"}, int64(20), true}, {"alpha one", []interface{}{"CT"}, int64(37), true}, {"alpha two", []interface{}{"CT"}, int64(41), true}}},
		{"CALL db.index.fulltext.queryNodes('ct_ft', 'alpha') YIELD node WITH node.name AS n, node.age AS a WHERE a > 36 RETURN n, a * 2 AS twice ORDER BY twice DESC LIMIT 2", nil, [][]interface{}{{"alpha two", int64(82)}, {"alpha one", int64(74)}}},
		{"CALL db.index.fulltext.queryRelationships('ctr_ft', 'beta') YIELD relationship WITH relationship WHERE type(relationship) = 'CTR' RETURN relationship.name AS name, startNode(relationship).name AS s ORDER BY name", nil, [][]interface{}{{"beta x", "alpha one"}, {"beta y", "alpha lovelace"}}},
		{"CALL db.index.fulltext.queryRelationships('ctr_ft', 'beta') YIELD relationship WITH relationship WHERE relationship.w > 1 RETURN properties(relationship).name AS name", nil, [][]interface{}{{"beta y"}}},
		{"CALL db.index.fulltext.queryRelationships('ctr_ft', 'beta') YIELD relationship MATCH (a:CT)-[r:CTR {name: relationship.name}]->(b:CT) WHERE b.age > 0 WITH a, r, b RETURN a.name AS a, type(r) AS t, b.name AS b ORDER BY a", nil, [][]interface{}{{"alpha lovelace", "CTR", "alpha one"}, {"alpha one", "CTR", "alpha two"}}},
	} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "calltailplans"))
		ctx := context.Background()
		for _, statement := range setup {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}
		result, err := exec.Execute(ctx, tc.query, tc.params)
		require.NoError(t, err, tc.query)
		if strings.Contains(tc.query, "ORDER BY") {
			require.Equal(t, tc.want, result.Rows, tc.query)
		} else {
			require.ElementsMatch(t, tc.want, result.Rows, tc.query)
		}
	}
}
