package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// After a DELETE, a statement reads the deleted node or relationship as Neo4j
// 5.26.30 does: as an empty entity (no labels or properties; keys, n {.*} and
// properties empty), while a property or its labels (or a relationship's
// keys) is EntityNotFound, nested in an expression too (#907).
func TestDeletedEntityReadsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "deleted_reads"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1, s: 'a'})-[:R {w: 1}]->(:Q {id: 2}), (:P {id: 4})", nil)
	require.NoError(t, err)
	inRolledBackTransaction := func(query string) (*ExecuteResult, error) {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		return exec.Execute(ctx, query, nil)
	}

	for _, testCase := range []struct {
		query string
		want  interface{}
	}{
		{"MATCH (n:P) DELETE n RETURN n {.*} AS v", map[string]interface{}{}},
		{"MATCH (n:P) DELETE n RETURN keys(n) AS v", []interface{}{}},
		{"MATCH (n:P) DELETE n RETURN properties(n) AS v", map[string]interface{}{}},
		{"MATCH (n:P) DELETE n RETURN id(n) IS NOT NULL AS v", true},
		{"MATCH (n:P) DELETE n RETURN 'n.id' AS v", "n.id"},
		{"MATCH (n:Q {id: 1}) DETACH DELETE n RETURN n {.*} AS v", map[string]interface{}{}},
		{"MATCH (n:Q {id: 1}) DETACH DELETE n RETURN keys(n) AS v", []interface{}{}},
		{"MATCH (:Q {id: 1})-[r:R]->() DELETE r RETURN r {.*} AS v", map[string]interface{}{}},
		{"MATCH (:Q {id: 1})-[r:R]->() DELETE r RETURN type(r) AS v", "R"},
		{"MATCH p = (:Q {id: 1})-[:R]->(:Q {id: 2}) DETACH DELETE p RETURN [x IN nodes(p) | x {.*}] AS v", []interface{}{map[string]interface{}{}, map[string]interface{}{}}},
		{"MATCH (n:P) WITH collect(n) AS ns UNWIND ns AS n DELETE n RETURN n {.*} AS v", map[string]interface{}{}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := inRolledBackTransaction(testCase.query)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
		})
	}

	// The entity itself is empty.
	result, err := inRolledBackTransaction("MATCH (n:Q {id: 1}) DETACH DELETE n RETURN n")
	require.NoError(t, err)
	node, isNode := result.Rows[0][0].(*storage.Node)
	require.True(t, isNode, "%T", result.Rows[0][0])
	require.Empty(t, node.Labels)
	require.Empty(t, node.Properties)

	for _, query := range []string{
		"MATCH (n:P) DELETE n RETURN n.id AS v",
		"MATCH (n:P) DELETE n RETURN n.id + 1 AS v",
		"MATCH (n:P) DELETE n RETURN labels(n) AS v",
		"MATCH (n:Q {id: 1}) DETACH DELETE n RETURN [n.s] AS v",
		"MATCH (:Q {id: 1})-[r:R]->() DELETE r RETURN r.w AS v",
		"MATCH (:Q {id: 1})-[r:R]->() DELETE r RETURN keys(r) AS v",
		"MATCH (:Q {id: 1})-[r:R]->() DELETE r RETURN properties(r) AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := inRolledBackTransaction(query)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)
		})
	}
}

// What a statement deleted stays deleted for the rest of it: through WITH,
// aliases, collect() and UNWIND, a filter or ORDER BY, the row-at-a-time
// runs of UNWIND ... DELETE, FOREACH and CALL subqueries. n.p IS NULL and a
// SET on a deleted entity don't fail (Neo4j 5.26.30, #907).
func TestDeletedEntityReadsAfterWithMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "deleted_reads_with"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:DQ {id: 1, p: 5})-[:DR {w: 2}]->(:DQ {id: 2, p: 6})", nil)
	require.NoError(t, err)
	inRolledBackTransaction := func(query string) (*ExecuteResult, error) {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		return exec.Execute(ctx, query, nil)
	}
	for _, query := range []string{
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n RETURN n.p AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n RETURN labels(n) AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n AS m RETURN m.p AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n, 1 AS k RETURN n.p AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n WHERE n.p = 5 RETURN 1 AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n ORDER BY n.p RETURN 1 AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH collect(n) AS ns RETURN [x IN ns | x.p] AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH collect(n) AS ns UNWIND ns AS m RETURN m.p AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n UNWIND [1, 2] AS i RETURN n.p AS v",
		"UNWIND [1, 2] AS k MATCH (n:DQ {id: k}) DETACH DELETE n RETURN n.p AS v",
		"UNWIND [1, 2] AS k MATCH (n:DQ {id: k}) DETACH DELETE n WITH n RETURN labels(n) AS v",
		"MATCH (a)-[r:DR]->(b) DELETE r WITH r RETURN properties(r) AS v",
		"MATCH (a)-[r:DR]->(b) DELETE r WITH r RETURN keys(r) AS v",
		"MATCH (a)-[r:DR]->(b) DELETE r WITH r RETURN r.w AS v",
		"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n SET n.q = n.p RETURN 1 AS v",
		"MATCH (n:DQ {id: 1}) FOREACH (x IN [n] | DETACH DELETE x) WITH n RETURN n.p AS v",
		"MATCH (n:DQ {id: 1}) CALL { WITH n DETACH DELETE n } WITH n RETURN n.p AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := inRolledBackTransaction(query)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)
		})
	}
	for _, testCase := range []struct {
		query string
		want  interface{}
	}{
		{"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n RETURN n {.*} AS v", map[string]interface{}{}},
		{"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n RETURN keys(n) AS v", []interface{}{}},
		{"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n RETURN properties(n) AS v", map[string]interface{}{}},
		{"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n RETURN n.p IS NULL AS v", true},
		{"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n RETURN n.p IS NOT NULL AS v", false},
		{"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH n SET n.q = 1 RETURN 1 AS v", int64(1)},
		{"MATCH (n:DQ {id: 1}) DETACH DELETE n WITH collect(n) AS ns RETURN [x IN ns | x {.*}] AS v", []interface{}{map[string]interface{}{}}},
		{"MATCH (a:DQ {id: 1}), (b:DQ {id: 2}) DETACH DELETE a WITH b RETURN b.p AS v", int64(6)},
		{"MATCH (a)-[r:DR]->(b) DELETE r WITH r, a RETURN a.p AS v", int64(5)},
		{"MATCH (a)-[r:DR]->(b) DELETE r WITH r RETURN type(r) AS v", "DR"},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := inRolledBackTransaction(testCase.query)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
		})
	}
}

func TestDeletedEntityReadTextAndIteration(t *testing.T) {
	require.Equal(t, "n.p\n1\n", deletedEntityReadText(pipelineClause{kind: pipelineClauseSet, text: "SET n.q = n.p, m += 1, n:L"}))
	require.Empty(t, deletedEntityReadText(pipelineClause{kind: pipelineClauseRemove, text: "REMOVE n.p"}))
	require.Empty(t, deletedEntityReadText(pipelineClause{kind: pipelineClauseDelete, text: "DELETE n"}))
	require.Equal(t, "RETURN n.p", deletedEntityReadText(pipelineClause{kind: pipelineClauseReturn, text: "RETURN n.p"}))
	require.Equal(t, "xs", iterationSourceVariable("RETURN [x IN xs | x.p]", "x"))
	require.Equal(t, "ys", iterationSourceVariable("RETURN 'x IN zs', any(x  in ys WHERE x.p > 1)", "x"))
	require.Empty(t, iterationSourceVariable("RETURN x.p, xin", "x"))
	require.Empty(t, iterationSourceVariable("RETURN [x IN [1] | x]", "x"))
	require.Empty(t, iterationSourceVariable("RETURN 'x", "x"))
	require.True(t, propertyNullTestFollows("n.p IS NULL", 2))
	require.True(t, propertyNullTestFollows("n.p is  not null", 2))
	require.False(t, propertyNullTestFollows("n.p IS NOT 1", 2))
	require.False(t, propertyNullTestFollows("n.p = 1", 2))
	var none *deletedEntities
	require.True(t, none.empty())
	require.False(t, none.bindsDeleted(pipelineRow{}, "", false))
}

func TestDeletedEntityReadsIn(t *testing.T) {
	require.Equal(t, []deletedEntityRead{{variable: "n"}, {variable: "m", relationshipOnly: true}, {variable: "r"}},
		deletedEntityReadsIn("n.a + size(keys( m )) + 'x.y' + size(labels(r)) + `q`.z"))
	require.Empty(t, deletedEntityReadsIn("range(1..2)"))
	require.Empty(t, deletedEntityReadsIn("keys(f(n))"))
	require.Empty(t, deletedEntityReadsIn("'unterminated"))
}

// A DELETE target must be a node, relationship or path (or null, which
// deletes nothing); a property is a SyntaxError before the statement runs
// (Neo4j 5.26.30, #907). A list of relationships is a NornicDB extension.
func TestDeleteTargetTypesMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_targets"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1, s: 'a'})-[:R]->(:Q {id: 2})-[:R]->(:Q {id: 3})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "DELETE null RETURN 1 AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	for _, query := range []string{
		"MATCH (n:Q {id: 1}) DELETE n.s",
		"MATCH (n:Q {id: 1}) DETACH DELETE n.s",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		})
	}
	result, err = exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)

	// A NornicDB extension kept from before (#907): Neo4j 5.26 rejects a
	// list of relationships as a DELETE target; NornicDB deletes them.
	result, err = exec.Execute(ctx, "MATCH (n:Q {id: 1})-[x*2]->() DELETE x RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	result, err = exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func TestDeletedEntityViewsReplace(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	gone := &storage.Node{ID: "gone", Labels: []string{"L"}, Properties: map[string]interface{}{"p": 1}}
	kept := &storage.Node{ID: "kept"}
	edge := &storage.Edge{ID: "e", Type: "R", StartNode: "gone", EndNode: "kept", Properties: map[string]interface{}{"w": 1}}
	views := deletedEntityViews{executor: exec,
		nodes: map[storage.NodeID]struct{}{"gone": {}},
		edges: map[storage.EdgeID]struct{}{"e": {}}}

	var nilNode *storage.Node
	var nilEdge *storage.Edge
	var nilPath *PathResult
	for _, value := range []interface{}{nilNode, nilEdge, kept, []interface{}{kept, int64(1)},
		map[string]interface{}{"a": gone}, map[string]interface{}{"_pathResult": nilPath},
		map[string]interface{}{"_pathResult": PathResult{Nodes: []*storage.Node{kept}}}} {
		replaced, changed := views.replace(value)
		require.False(t, changed)
		require.Equal(t, value, replaced)
	}
	view, changed := views.replace(edge)
	require.True(t, changed)
	require.Equal(t, &storage.Edge{ID: "e", Type: "R", StartNode: "gone", EndNode: "kept"}, view)

	path := &PathResult{Nodes: []*storage.Node{gone, kept}, Relationships: []*storage.Edge{edge}, Length: 1}
	replaced, changed := views.replace(map[string]interface{}{"_pathResult": path})
	require.True(t, changed)
	parts := replaced.(map[string]interface{})["_pathResult"].(PathResult)
	require.Equal(t, &storage.Node{ID: "gone"}, parts.Nodes[0])
	require.Same(t, kept, parts.Nodes[1])
	require.Equal(t, storage.EdgeID("e"), parts.Relationships[0].ID)
	require.Nil(t, parts.Relationships[0].Properties)

	// A path target deletes its nodes and relationships, collected as the
	// pipeline DELETE collects them.
	res := &ExecuteResult{Stats: &QueryStats{}}
	input := &ExecuteResult{Columns: []string{"p"}, Rows: [][]interface{}{{map[string]interface{}{
		"nodes": []interface{}{gone, "x"}, "rels": []interface{}{edge, int64(1)}}}}}
	exec.applyDeleteReturnProjection(res, "MATCH p = ()-->() DELETE p RETURN size(nodes(p)) AS n", "p", deleteProjectionInfo{ctx: context.Background(), input: input})
	require.Equal(t, [][]interface{}{{int64(2)}}, res.Rows)
}
