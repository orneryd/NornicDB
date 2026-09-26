package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newMergeReturnRouteExecutor is an executor over (a1:A)-[:R]->(b1:B),
// (a1)-[:R]->(b2:B), (a2:A)-[:R]->(b1).
func newMergeReturnRouteExecutor(t *testing.T) (*StorageExecutor, context.Context) {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:A {name: 'a1'})-[:R]->(b:B {name: 'b1'}), (a)-[:R]->(:B {name: 'b2'}), (:A {name: 'a2'})-[:R]->(b)", nil)
	require.NoError(t, err)
	return exec, ctx
}

// withExpressionFailures is ctx with the statement's expression-failure
// record, as Execute sets it up.
func withExpressionFailures(ctx context.Context) context.Context {
	return context.WithValue(ctx, expressionFailureKey{}, &expressionFailure{})
}

// TestCompoundMatchMergeReturnSeesMatchedBindings: the MATCH … MERGE route
// (taken for a MERGE with SET) projects its RETURN over every matched row,
// with the MATCH's relationship variables bound per row and an unmatched
// OPTIONAL MATCH's variables null (#640, #713).
func TestCompoundMatchMergeReturnSeesMatchedBindings(t *testing.T) {
	exec, ctx := newMergeReturnRouteExecutor(t)

	result, err := exec.Execute(ctx, "MATCH (a:A)-[r:R]->(b:B) MERGE (m:M {id: 1}) SET m.x = 1 RETURN a.name AS a, b.name AS b, type(r) AS t ORDER BY a, b", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a1", "b1", "R"}, {"a1", "b2", "R"}, {"a2", "b1", "R"}}, result.Rows)

	result, err = exec.Execute(ctx, "MATCH (a:A {name: 'a1'})-[r:R]->(b:B {name: 'b1'}) MERGE (a)-[s:S]->(b) SET s.x = 1 RETURN type(r) AS t, type(s) AS u", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"R", "S"}}, result.Rows)

	// The compound route itself (a MERGE with ON CREATE / ON MATCH and SET,
	// which the pipeline declines) binds the relationship per row too.
	result, err = exec.executeCompoundMatchMerge(ctx, "MATCH (a:A)-[r:R]->(b:B) MERGE (m:M2 {id: a.name}) ON CREATE SET m.c = 1 ON MATCH SET m.d = 1 SET m.x = 1 RETURN a.name AS a, b.name AS b, type(r) AS t ORDER BY a, b")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a1", "b1", "R"}, {"a1", "b2", "R"}, {"a2", "b1", "R"}}, result.Rows)

	result, err = exec.executeCompoundMatchMerge(ctx, "OPTIONAL MATCH (x:Missing)-[q:Q]->(y) MERGE (c:C {id: 1}) RETURN x, q, y, c.id AS id")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil, nil, nil, int64(1)}}, result.Rows)

	// A MERGE failing for a row (a map property value) fails the
	// statement, with or without matched rows.
	_, err = exec.executeCompoundMatchMerge(ctx, "MATCH (a:A) MERGE (d:D {id: {k: 1}}) RETURN d")
	require.Error(t, err)
	_, err = exec.executeCompoundMatchMerge(ctx, "OPTIONAL MATCH (x:Missing) MERGE (d:D {id: {k: 1}}) RETURN d")
	require.Error(t, err)

	// A MERGE property's expression error is recorded as the statement's
	// error, which Execute returns.
	failures := withExpressionFailures(ctx)
	_, err = exec.executeCompoundMatchMerge(failures, "MATCH (a:A) MERGE (d:D {id: 1 / 0}) RETURN d.id AS id")
	require.NoError(t, err)
	require.Error(t, getExpressionFailure(failures))

	// The RETURN's error is the statement's.
	_, err = exec.executeCompoundMatchMerge(withExpressionFailures(ctx), "MATCH (a:A) MERGE (m:M {id: 1}) RETURN 1 / 0 AS x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.ArithmeticError")
}

// TestCompoundMatchUnwindMergeReturnProjectsAllRows: MATCH … UNWIND … MERGE
// … RETURN projects one RETURN over every (matched row × list item), with
// the matched relationship bound per row.
func TestCompoundMatchUnwindMergeReturnProjectsAllRows(t *testing.T) {
	exec, ctx := newMergeReturnRouteExecutor(t)

	result, err := exec.executeCompoundMatchMerge(ctx, "MATCH (a:A)-[r:R]->(b:B {name: 'b2'}) UNWIND [1, 2] AS i MERGE (m:U {id: i}) RETURN a.name AS a, type(r) AS t, m.id AS id ORDER BY id")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a1", "R", int64(1)}, {"a1", "R", int64(2)}}, result.Rows)

	result, err = exec.executeCompoundMatchMerge(ctx, "MATCH (a:A) UNWIND [1, 2] AS i MERGE (m:U2 {id: i}) RETURN count(*) AS c")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)

	_, err = exec.executeCompoundMatchMerge(ctx, "MATCH (a:A) UNWIND [1] AS i MERGE (d:D {id: {k: i}}) RETURN d")
	require.Error(t, err)
	_, err = exec.executeCompoundMatchMerge(withExpressionFailures(ctx), "MATCH (a:A) UNWIND [1] AS i MERGE (m:U3 {id: i}) RETURN 1 / 0 AS x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.ArithmeticError")
}

// TestMergeReturnProjectionErrors: every MERGE route's RETURN goes through
// projectMergeReturn - an expression error recorded while projecting is the
// statement's error, and an item the projection can't evaluate is a
// SyntaxError.
func TestMergeReturnProjectionErrors(t *testing.T) {
	exec, ctx := newMergeReturnRouteExecutor(t)

	_, err := exec.executeMerge(withExpressionFailures(ctx), "MERGE (n:M {id: 1}) RETURN 1 / 0 AS x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.ArithmeticError")

	_, err = exec.executeMerge(ctx, "MERGE (n:M {id: 1}) RETURN n.id +")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError")

	a1, err := exec.executeMatch(ctx, "MATCH (a:A {name: 'a1'}) RETURN a")
	require.NoError(t, err)
	b1, err := exec.executeMatch(ctx, "MATCH (b:B {name: 'b1'}) RETURN b")
	require.NoError(t, err)
	nodes := map[string]*storage.Node{"a": a1.Rows[0][0].(*storage.Node), "b": b1.Rows[0][0].(*storage.Node)}

	_, err = exec.executeMergeWithContext(withExpressionFailures(ctx), "MERGE (m:N {id: 1}) RETURN 1 / 0 AS x", nodes, map[string]*storage.Edge{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.ArithmeticError")

	_, err = exec.executeMergeWithContext(withExpressionFailures(ctx), "MERGE (a)-[s:S2]->(b) RETURN 1 / 0 AS x", nodes, map[string]*storage.Edge{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.ArithmeticError")
}

// TestMergeReturnSeesFabricRecordBindings: a MERGE run for a fabric record
// reads the record's values in its RETURN.
func TestMergeReturnSeesFabricRecordBindings(t *testing.T) {
	exec, ctx := newMergeReturnRouteExecutor(t)
	exec.fabricRecordBindings = map[string]interface{}{"outer": int64(7)}
	defer func() { exec.fabricRecordBindings = nil }()

	result, err := exec.executeMerge(ctx, "MERGE (n:F {id: 1}) RETURN outer, n.id AS id")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7), int64(1)}}, result.Rows)
}

// TestLegacyRoutesCountDistinct: count(DISTINCT …) counts distinct values on
// the MATCH … WITH, relationship MATCH … WITH and traversal aggregation
// routes.
func TestLegacyRoutesCountDistinct(t *testing.T) {
	exec, ctx := newMergeReturnRouteExecutor(t)

	result, err := exec.executeMatchWithClause(ctx, "MATCH (n:B) WITH n RETURN count(DISTINCT n.name) AS c")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

	result, err = exec.executeMatchRelationshipsWithClause(ctx, "(a:A)-[:R]->(b:B)", "", "WITH a.name AS a, count(DISTINCT b.name) AS c RETURN a, c ORDER BY a")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a1", int64(2)}, {"a2", int64(1)}}, result.Rows)

	result, err = exec.executeMatch(ctx, "MATCH (a:A)-[:R]->(b:B) RETURN count(DISTINCT b) AS c")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}

// TestCompoundMatchMergeRouteShapes: the MATCH … MERGE route's WITH window
// (SKIP / LIMIT before the MERGE) and the UNWIND route's trailing MATCH
// (prepared simple-node and general) keep each row's bindings.
func TestCompoundMatchMergeRouteShapes(t *testing.T) {
	exec, ctx := newMergeReturnRouteExecutor(t)

	result, err := exec.executeCompoundMatchMerge(ctx, "MATCH (a:A) WITH a SKIP 1 LIMIT 1 MERGE (m:W {id: 1}) RETURN count(*) AS c")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

	result, err = exec.executeCompoundMatchMerge(ctx, "MATCH (a:A {name: 'a1'})-[r:R]->(b:B {name: 'b1'}) UNWIND [1] AS i MATCH (c:A) MERGE (m:T1 {id: c.name}) RETURN type(r) AS t, c.name AS c ORDER BY c")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"R", "a1"}, {"R", "a2"}}, result.Rows)

	result, err = exec.executeCompoundMatchMerge(ctx, "MATCH (a:A {name: 'a2'}) UNWIND [1] AS i MATCH (x:A)-[q:R]->(y:B {name: 'b2'}) MERGE (m:T2 {id: i}) RETURN a.name AS a, type(q) AS t, x.name AS x")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a2", "R", "a1"}}, result.Rows)
}

// TestMatchForContextWithRelationshipsReportsMatchErrors: a MATCH that
// fails to run is the context helper's error.
func TestMatchForContextWithRelationshipsReportsMatchErrors(t *testing.T) {
	exec, ctx := newMergeReturnRouteExecutor(t)
	_, _, err := exec.executeMatchForContextWithRelationships(ctx, "MATCH (a:A)-[r:R]->(b) RETURN ,", "(a:A)-[r:R]->(b)")
	require.Error(t, err)
}

// TestMergeBindingRowNullBindings: an unbound node or relationship (a nil
// entry) is null in the MERGE's RETURN row, not a typed nil.
func TestMergeBindingRowNullBindings(t *testing.T) {
	exec, ctx := newMergeReturnRouteExecutor(t)
	row := exec.mergeBindingRow(ctx, map[string]*storage.Node{"n": nil, "": nil}, map[string]*storage.Edge{"r": nil, "": nil})
	require.Equal(t, pipelineRow{"n": nil, "r": nil}, row)
	value, bound := row["r"]
	require.True(t, bound)
	require.Nil(t, value)
}

// TestMatchMergeSetHonorsWithWindow: MATCH … WITH … ORDER BY … SKIP / LIMIT
// … MERGE … SET merges only the rows the window keeps (Neo4j's result); the
// MERGE used to run for every matched row (#640).
func TestMatchMergeSetHonorsWithWindow(t *testing.T) {
	exec, _ := newMergeReturnRouteExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:A {name: 'a3'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (a:A) WITH a ORDER BY a.name SKIP 1 LIMIT 1 MERGE (m:W {id: a.name}) SET m.x = 1 RETURN m.id AS id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a2"}}, result.Rows)
	require.Equal(t, 1, result.Stats.NodesCreated)

	result, err = exec.Execute(ctx, "MATCH (a:A) WITH a ORDER BY a.name DESC LIMIT 2 MERGE (m:W3 {id: a.name}) SET m.x = 1 RETURN m.id AS id ORDER BY id", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a2"}, {"a3"}}, result.Rows)

	result, err = exec.Execute(ctx, "MATCH (w:W) RETURN count(w) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}
