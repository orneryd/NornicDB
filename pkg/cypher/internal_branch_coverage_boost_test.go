package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateWithWhere_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_where")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "r1", Type: "KNOWS", StartNode: "n1", EndNode: "n2"})
	require.NoError(t, err)

	node, err := store.GetNode("n1")
	require.NoError(t, err)
	require.NotNil(t, node)
	edge, err := store.GetEdge("r1")
	require.NoError(t, err)
	require.NotNil(t, edge)

	fixtureAdmission669Context, fixtureAdmission669 := expressionTestContext(ctx, "age = 42")
	_ = fixtureAdmission669
	ok, err := exec.evaluateWithWhere(fixtureAdmission669, fixtureAdmission669Context, "age = 42", map[string]interface{}{
		"n":   node,
		"r":   edge,
		"age": int64(42),
	})
	require.NoError(t, err)
	require.True(t, ok)

	fixtureAdmission670Context, fixtureAdmission670 := expressionTestContext(ctx, "n.name = 'Alice'")
	_ = fixtureAdmission670
	ok, err = exec.evaluateWithWhere(fixtureAdmission670, fixtureAdmission670Context, "n.name = 'Alice'", map[string]interface{}{"n": node})
	require.NoError(t, err)
	require.True(t, ok)

	fixtureAdmission671Context, fixtureAdmission671 := expressionTestContext(ctx, "n.name")
	_ = fixtureAdmission671
	ok, err = exec.evaluateWithWhere(fixtureAdmission671, fixtureAdmission671Context, "n.name", map[string]interface{}{"n": node})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected Boolean")
	require.False(t, ok)

	fixtureAdmission672Context, fixtureAdmission672 := expressionTestContext(ctx, "   ")
	_ = fixtureAdmission672
	ok, err = exec.evaluateWithWhere(fixtureAdmission672, fixtureAdmission672Context, "   ", map[string]interface{}{"n": node})
	require.NoError(t, err)
	require.True(t, ok)
}

func TestApplyUnwindMergeChainEdgeSetAssignment_Branches(t *testing.T) {
	edge := &storage.Edge{ID: "e1", Type: "REL", StartNode: "a", EndNode: "b"}
	rowValues := map[string]interface{}{
		"props": map[string]interface{}{"k": "v", "n": int64(1)},
		"name":  "primary",
	}
	resolver := func(expr string, row map[string]interface{}) interface{} {
		return row[expr]
	}

	fixtureAdmission1617Context, fixtureAdmission1617 := statementTestContext(context.Background(), "RETURN "+("props"))
	_ = fixtureAdmission1617Context
	changed, err := sharedSetAssignmentForTest(fixtureAdmission1617, edge, unwindSimpleSetAssignment{mergeMap: true, expr: "props"}, rowValues, resolver, &setWrites{})
	require.NoError(t, err)
	require.Equal(t, 2, changed)
	require.Equal(t, "v", edge.Properties["k"])
	require.EqualValues(t, 1, edge.Properties["n"])

	fixtureAdmission1618Context, fixtureAdmission1618 := statementTestContext(context.Background(), "RETURN "+("props"))
	_ = fixtureAdmission1618Context
	changed, err = sharedSetAssignmentForTest(fixtureAdmission1618, edge, unwindSimpleSetAssignment{mergeMap: true, expr: "props"}, rowValues, resolver, &setWrites{})
	require.NoError(t, err)
	require.Equal(t, 0, changed)

	fixtureAdmission1619Context, fixtureAdmission1619 := statementTestContext(context.Background(), "RETURN "+("name"))
	_ = fixtureAdmission1619Context
	changed, err = sharedSetAssignmentForTest(fixtureAdmission1619, edge, unwindSimpleSetAssignment{prop: "name", expr: "name"}, rowValues, resolver, &setWrites{})
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	require.Equal(t, "primary", edge.Properties["name"])

	fixtureAdmission1620Context, fixtureAdmission1620 := statementTestContext(context.Background(), "RETURN "+("bad"))
	_ = fixtureAdmission1620Context
	changed, err = sharedSetAssignmentForTest(fixtureAdmission1620, edge, unwindSimpleSetAssignment{mergeMap: true, expr: "bad"}, map[string]interface{}{"bad": int64(5)}, resolver, &setWrites{})
	require.Error(t, err)
	require.Equal(t, 0, changed)
}

func TestTopKRowsHeap_Pop(t *testing.T) {
	h := &topKRowsHeap{}
	h.Push(rowRefForOrder{row: []interface{}{1}, idx: 0})
	h.Push(rowRefForOrder{row: []interface{}{2}, idx: 1})
	require.Equal(t, 2, h.Len())

	popped := h.Pop().(rowRefForOrder)
	require.Equal(t, 1, popped.idx)
	require.Equal(t, 1, h.Len())
}

func TestExecuteDeleteStreaming_NodeAndRelationshipPaths(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_delete")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("v-%d", i)), Labels: []string{"Victim"}})
		require.NoError(t, err)
	}
	_, err := store.CreateNode(&storage.Node{ID: "s", Labels: []string{"Start"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "t", Labels: []string{"Target"}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "r-del", Type: "REL", StartNode: "s", EndNode: "t"})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:Victim)"+" DETACH DELETE "+"n", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Stats)
	assert.Equal(t, 3, res.Stats.NodesDeleted)

	remainingVictims, err := exec.Execute(ctx, "MATCH (n:Victim) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, remainingVictims.Rows[0][0])

	res, err = exec.Execute(ctx, "MATCH ()-[r:REL]->()"+" DELETE "+"r", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Stats)
	assert.Equal(t, 1, res.Stats.RelationshipsDeleted)

	remainingRels, err := exec.Execute(ctx, "MATCH ()-[r:REL]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, remainingRels.Rows[0][0])
}

func TestCollectDeleteWithLimitCandidates_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_collect")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, n := range []struct {
		id   string
		name string
		tag  string
	}{
		{"a", "alice", "x"},
		{"b", "bob", "y"},
		{"c", "carol", "x"},
	} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(n.id), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": n.name, "tag": n.tag}})
		require.NoError(t, err)
	}

	fixtureAdmission1621Context, fixtureAdmission1621 := statementTestContext(ctx, "MATCH (n:Person) WHERE n.name = $name")
	_ = fixtureAdmission1621Context
	nodes, ok, err := exec.collectDeleteWithLimitCandidates(fixtureAdmission1621,
		fixtureAdmission1621Context,
		"MATCH (n:Person) WHERE n.name = $name",
		"n",
		10,
		map[string]interface{}{"name": "alice"},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("a"), nodes[0].ID)

	fixtureAdmission1622Context, fixtureAdmission1622 := statementTestContext(ctx, "MATCH (n:Person) WHERE n.tag IN $tags")
	_ = fixtureAdmission1622Context
	nodes, ok, err = exec.collectDeleteWithLimitCandidates(fixtureAdmission1622,
		fixtureAdmission1622Context,
		"MATCH (n:Person) WHERE n.tag IN $tags",
		"n",
		10,
		map[string]interface{}{"tags": []interface{}{"x"}},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, nodes, 2)

	fixtureAdmission1623Context, fixtureAdmission1623 := statementTestContext(ctx, "MATCH (n:Person) WHERE n.tag IN $tags")
	_ = fixtureAdmission1623Context
	nodes, ok, err = exec.collectDeleteWithLimitCandidates(fixtureAdmission1623,
		fixtureAdmission1623Context,
		"MATCH (n:Person) WHERE n.tag IN $tags",
		"n",
		10,
		map[string]interface{}{},
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, nodes)

	fixtureAdmission1624Context, fixtureAdmission1624 := statementTestContext(ctx, "MATCH (n:Person) WHERE n.name <> 'alice'")
	_ = fixtureAdmission1624Context
	nodes, ok, err = exec.collectDeleteWithLimitCandidates(fixtureAdmission1624,
		fixtureAdmission1624Context,
		"MATCH (n:Person) WHERE n.name <> 'alice'",
		"n",
		10,
		nil,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, nodes)

	fixtureAdmission1625Context, fixtureAdmission1625 := statementTestContext(ctx, "MATCH (:Person) WHERE n.name = 'alice'")
	_ = fixtureAdmission1625Context
	nodes, ok, err = exec.collectDeleteWithLimitCandidates(fixtureAdmission1625,
		fixtureAdmission1625Context,
		"MATCH (:Person) WHERE n.name = 'alice'",
		"n",
		10,
		nil,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, nodes)
}

func TestExecuteCorrelatedCallWithSeedRows_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_subq")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "seed-1", Labels: []string{"Seed"}, Properties: map[string]interface{}{"name": "Alice"}})
	require.NoError(t, err)
	seedNode, err := store.GetNode("seed-1")
	require.NoError(t, err)
	require.NotNil(t, seedNode)

	_, err = exec.sharedCorrelatedCallForTest(ctx, &ExecuteResult{Columns: []string{"seed"}, Rows: [][]interface{}{{int64(1)}}}, "RETURN 1 AS x", []string{"missing"})
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	assert.Contains(t, err.Error(), "missing")

	_, err = exec.sharedCorrelatedCallForTest(ctx, &ExecuteResult{Columns: []string{"seed"}, Rows: [][]interface{}{{}}}, "RETURN 1 AS x", []string{"seed"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown variable: seed")

	res, err := exec.sharedCorrelatedCallForTest(
		ctx,
		&ExecuteResult{Columns: []string{"seed", "extra"}, Rows: [][]interface{}{{int64(7), "x"}}},
		"CREATE (:Tmp {v: seed})",
		[]string{"seed", "extra"},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"seed", "extra"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, int64(7), res.Rows[0][0])
	require.Equal(t, "x", res.Rows[0][1])

	tmpCount, err := exec.Execute(ctx, "MATCH (t:Tmp) RETURN count(t)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, tmpCount.Rows[0][0])

	res, err = exec.sharedCorrelatedCallForTest(
		ctx,
		&ExecuteResult{Columns: []string{"seed", "extra"}, Rows: [][]interface{}{{seedNode, int64(1)}}},
		"RETURN seed.name AS name, 99 AS score",
		[]string{"seed", "extra"},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"seed", "extra", "name", "score"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "Alice", res.Rows[0][2])
	require.EqualValues(t, int64(99), res.Rows[0][3])

	res, err = exec.sharedCorrelatedCallForTest(
		ctx,
		&ExecuteResult{Columns: []string{"seed", "extra"}, Rows: [][]interface{}{{int64(1), int64(2)}}},
		"MATCH (x:NoSuchLabel) RETURN x AS x",
		[]string{"seed", "extra"},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"seed", "extra", "x"}, res.Columns)
	require.Empty(t, res.Rows)
}

func TestCallAndClauseHelpers_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_call_clause")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "a", Labels: []string{"N"}, Properties: map[string]interface{}{"name": "A"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b", Labels: []string{"N"}, Properties: map[string]interface{}{"name": "B"}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "ab", Type: "REL", StartNode: "a", EndNode: "b", Properties: map[string]interface{}{"w": int64(5)}})
	require.NoError(t, err)

	aNode, err := store.GetNode("a")
	require.NoError(t, err)
	abEdge, err := store.GetEdge("ab")
	require.NoError(t, err)

	require.Equal(t, "MATCH (n) RETURN n", buildCallTailPredicateInjection(" MATCH (n) RETURN n ", nil))
	require.Equal(t,
		"MATCH (n) WHERE n.id = '1' AND x = 1 RETURN n",
		buildCallTailPredicateInjection("MATCH (n) WHERE n.id = '1' RETURN n", []string{"x = 1"}),
	)
	require.Equal(t,
		"MATCH (n) WHERE x = 1 RETURN n",
		buildCallTailPredicateInjection("MATCH (n) RETURN n", []string{"x = 1"}),
	)

	varMap := map[string]interface{}{"s": aNode}
	fixtureAdmission1229Context, fixtureAdmission1229 := expressionTestContext(ctx, "t.name")
	_ = fixtureAdmission1229Context
	require.Equal(t, "B", exec.resolveReturnExprFromVarMap(fixtureAdmission1229, fixtureAdmission1229Context, "t.name", varMap, "t", "r", &storage.Node{ID: "t", Properties: map[string]interface{}{"name": "B"}}, abEdge))
	fixtureAdmission1230Context, fixtureAdmission1230 := expressionTestContext(ctx, "r.w")
	_ = fixtureAdmission1230Context
	require.EqualValues(t, 5, exec.resolveReturnExprFromVarMap(fixtureAdmission1230, fixtureAdmission1230Context, "r.w", varMap, "t", "r", nil, abEdge))
	fixtureAdmission1231Context, fixtureAdmission1231 := expressionTestContext(ctx, "s.name")
	_ = fixtureAdmission1231Context
	require.Equal(t, "A", exec.resolveReturnExprFromVarMap(fixtureAdmission1231, fixtureAdmission1231Context, "s.name", varMap, "t", "r", nil, nil))
	fixtureAdmission1232Context, fixtureAdmission1232 := expressionTestContext(ctx, "s")
	_ = fixtureAdmission1232Context
	require.Equal(t, aNode, exec.resolveReturnExprFromVarMap(fixtureAdmission1232, fixtureAdmission1232Context, "s", varMap, "t", "r", nil, nil))
	fixtureAdmission1233Context, fixtureAdmission1233 := expressionTestContext(ctx, "7")
	_ = fixtureAdmission1233Context
	require.EqualValues(t, 7, exec.resolveReturnExprFromVarMap(fixtureAdmission1233, fixtureAdmission1233Context, "7", varMap, "t", "r", nil, nil))

	require.Equal(t, "node:a", joinedValueKey(aNode))
	require.Equal(t, "node:nil", joinedValueKey((*storage.Node)(nil)))
	require.Equal(t, "edge:ab", joinedValueKey(abEdge))
	require.Equal(t, "edge:nil", joinedValueKey((*storage.Edge)(nil)))
	require.Contains(t, joinedValueKey(map[string]interface{}{"k": "v"}), "map")

	replaced := replaceIdentifierOutsideQuotes(`row + row.name + "row" + 'row' + `+"`row`"+` + {row: 1}`, "row", "$row")
	require.Equal(t, `$row + $row.name + "row" + 'row' + `+"`row`"+` + {row: 1}`, replaced)
}

func TestMergeAndMutationHelpers_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_merge_mut")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	edge := &storage.Edge{ID: "r1", Type: "REL", StartNode: "a", EndNode: "b"}
	nodeCtx := map[string]*storage.Node{
		"n": {ID: "a", Properties: map[string]interface{}{"name": "Alice"}},
	}
	relCtx := map[string]*storage.Edge{}
	ctxWithParams := context.WithValue(ctx, paramsKey, map[string]interface{}{
		"m": map[string]interface{}{"a": int64(1)},
		"p": []string{"x", "y"},
	})

	fixtureAdmission1626Context, fixtureAdmission1626 := statementTestContext(ctxWithParams, "r += $m, r.tags = $p, r.owner = n.name, other.k = 1")
	_ = fixtureAdmission1626Context
	propertiesSet, err := exec.applySetToRelationshipWithContext(fixtureAdmission1626,
		fixtureAdmission1626Context,
		edge,
		"r",
		"r += $m, r.tags = $p, r.owner = n.name, other.k = 1",
		nodeCtx,
		relCtx,
	)
	require.NoError(t, err)
	require.Equal(t, 3, propertiesSet)
	require.EqualValues(t, int64(1), edge.Properties["a"])
	require.Equal(t, []string{"x", "y"}, edge.Properties["tags"])
	require.Equal(t, "Alice", edge.Properties["owner"])

	withVar := wherePartNodePattern(nodePatternInfo{variable: ""}, "n")
	require.Equal(t, "n", withVar.variable)
	withExisting := wherePartNodePattern(nodePatternInfo{variable: "x"}, "n")
	require.Equal(t, "x", withExisting.variable)

	_, err = store.CreateNode(&storage.Node{ID: "na", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "nb", Labels: []string{"N"}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "edge-1", Type: "REL", StartNode: "na", EndNode: "nb"})
	require.NoError(t, err)
	matchResult := &ExecuteResult{Rows: [][]interface{}{{map[string]interface{}{"_edgeId": "edge-1"}}}}
	exec.normalizeSetMatchRowsToEdges(matchResult, store)
	_, converted := matchResult.Rows[0][0].(*storage.Edge)
	require.True(t, converted)
}

func TestSubqueryHelperBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_sub_helpers")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	lhs, rhs, ok := splitTopLevelEqualityCallSubquery(`x = "a=b"`)
	require.True(t, ok)
	require.Equal(t, "x", lhs)
	require.Equal(t, `"a=b"`, rhs)

	_, _, ok = splitTopLevelEqualityCallSubquery("x")
	require.False(t, ok)

	idx := indexTopLevelKeywordCallSubquery(`x = "RETURN y" RETURN x`, "RETURN")
	require.Equal(t, 15, idx)

	cache := map[string]map[string]interface{}{"s1": {"x": "cached"}}
	fixtureAdmission673Context, fixtureAdmission673 := statementTestContext(ctx, "MATCH (seed:Seed)")
	_ = fixtureAdmission673
	val, found, err := exec.resolveCorrelatedImportValue(fixtureAdmission673, fixtureAdmission673Context, "MATCH (seed:Seed)", "seed", "s1", "x", cache)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "cached", val)

	_, err = store.CreateNode(&storage.Node{ID: "s1", Labels: []string{"Seed"}, Properties: map[string]interface{}{"v": "vv"}})
	require.NoError(t, err)

	cache = map[string]map[string]interface{}{}
	fixtureAdmission674Context, fixtureAdmission674 := statementTestContext(ctx, "MATCH (seed:Seed {v:'vv'}) WITH seed, seed.v AS x")
	_ = fixtureAdmission674
	val, found, err = exec.resolveCorrelatedImportValue(fixtureAdmission674,
		fixtureAdmission674Context,
		"MATCH (seed:Seed {v:'vv'}) WITH seed, seed.v AS x",
		"seed",
		"s1",
		"x",
		cache,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "vv", val)
	require.Equal(t, "vv", cache["s1"]["x"])

	fixtureAdmission675Context, fixtureAdmission675 := statementTestContext(ctx, "MATCH (")
	_ = fixtureAdmission675
	_, _, err = exec.resolveCorrelatedImportValue(fixtureAdmission675, fixtureAdmission675Context, "MATCH (", "seed", "s1", "x", nil)
	require.Error(t, err)
}

func TestLowerCoverageHelpers_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_helpers")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// match_multi helpers
	fixtureAdmission1892Context, fixtureAdmission1892 := expressionTestContext(ctx, "$v")
	_ = fixtureAdmission1892Context
	require.EqualValues(t, int64(5), exec.resolveWhereValue(fixtureAdmission1892, fixtureAdmission1892Context, "$v", map[string]interface{}{"v": int64(5)}))
	fixtureAdmission1893Context, fixtureAdmission1893 := expressionTestContext(ctx, "'x'")
	_ = fixtureAdmission1893Context
	require.Equal(t, "x", exec.resolveWhereValue(fixtureAdmission1893, fixtureAdmission1893Context, "'x'", nil))
	for _, value := range []interface{}{nil, "abc", 1, int64(2), 3.5, true} {
		require.Equal(t, cypherValueKey(value, cypherEquivalenceContract), cypherValueKey(value, cypherValueEqualityContract))
	}

	// binding_where_compile helpers
	pred, ok := exec.compileBindingNullPredicate("n.missing IS NULL", " IS NULL", false)
	require.True(t, ok)
	require.True(t, pred(binding{"n": {ID: "n1", Properties: map[string]interface{}{"name": "a"}}}, nil))

	pred, ok = exec.compileBindingNullPredicate("n.name IS NOT NULL", " IS NOT NULL", true)
	require.True(t, ok)
	require.True(t, pred(binding{"n": {ID: "n1", Properties: map[string]interface{}{"name": "a"}}}, nil))

	fixtureAdmission1234Context, fixtureAdmission1234 := expressionTestContext(ctx, "$plain")
	_ = fixtureAdmission1234Context
	val, ok := exec.resolveBindingFallbackValueWithOk(fixtureAdmission1234, fixtureAdmission1234Context, "$plain", nil, map[string]interface{}{"plain": "p"})
	require.True(t, ok)
	require.Equal(t, "p", val)
	fixtureAdmission1235Context, fixtureAdmission1235 := expressionTestContext(ctx, "1 + 2")
	_ = fixtureAdmission1235Context
	val, ok = exec.resolveBindingFallbackValueWithOk(fixtureAdmission1235, fixtureAdmission1235Context, "1 + 2", nil, nil)
	require.True(t, ok)
	require.EqualValues(t, int64(3), val)

	// match_with_rel helpers
	fixtureAdmission676Context, fixtureAdmission676 := expressionTestRow(context.Background(), "x = 1 AND y IS NOT NULL", map[string]interface{}{"x": int64(1), "y": "ok"})
	_ = fixtureAdmission676
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission676, "x = 1 AND y IS NOT NULL", fixtureAdmission676Context))
	fixtureAdmission677Context, fixtureAdmission677 := expressionTestRow(context.Background(), "x = 1 OR y = 2", map[string]interface{}{"x": int64(9), "y": int64(2)})
	_ = fixtureAdmission677
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission677, "x = 1 OR y = 2", fixtureAdmission677Context))
	fixtureAdmission678Context, fixtureAdmission678 := expressionTestRow(context.Background(), "NOT x = 1", map[string]interface{}{"x": int64(1)})
	_ = fixtureAdmission678
	require.False(t, exec.evaluateConditionFromValues(fixtureAdmission678, "NOT x = 1", fixtureAdmission678Context))
	// Undefined identifiers evaluate to Cypher null in the shared evaluator
	// (legacy clone returned the raw text, which broke IS NULL semantics).
	fixtureAdmission679Context, fixtureAdmission679 := expressionTestRow(context.Background(), "missing IS NULL", map[string]interface{}{})
	_ = fixtureAdmission679
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission679, "missing IS NULL", fixtureAdmission679Context))
	fixtureAdmission680Context, fixtureAdmission680 := expressionTestRow(context.Background(), "missing IS NOT NULL", map[string]interface{}{})
	_ = fixtureAdmission680
	require.False(t, exec.evaluateConditionFromValues(fixtureAdmission680, "missing IS NOT NULL", fixtureAdmission680Context))
	fixtureAdmission681Context, fixtureAdmission681 := expressionTestRow(context.Background(), "x <> 2", map[string]interface{}{"x": int64(1)})
	_ = fixtureAdmission681
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission681, "x <> 2", fixtureAdmission681Context))
	lit, litOK := parseLiteralValueFromComputedRow(`"hello"`)
	require.True(t, litOK)
	require.Equal(t, "hello", lit)
	lit, litOK = parseLiteralValueFromComputedRow("42")
	require.True(t, litOK)
	require.EqualValues(t, int64(42), lit)

	// match_rows helpers
	_, found := mapLookupCaseInsensitive(map[string]interface{}{"Name": "Alice"}, "name")
	require.True(t, found)
	n := &storage.Node{ID: "x", Properties: map[string]interface{}{"Title": "Doc"}}
	require.Equal(t, "Doc", extractOrderByPropertyValue(n, []string{"title"}))
	rowMap := map[string]interface{}{"properties": map[string]interface{}{"Score": int64(7)}}
	require.EqualValues(t, int64(7), extractOrderByPropertyValue(rowMap, []string{"score"}))
	require.Nil(t, extractOrderByPropertyValue("bad", []string{"x"}))
}

func TestExecuteDeleteStreaming_FallbackNoDeletes(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_delete_fallback")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Victim"}})
	require.NoError(t, err)

	// deleteVars references an unresolved symbol, so rows are returned but no deletes occur;
	// this exercises the safety-valve fallback path and loop break behavior.
	res, err := exec.Execute(ctx, "MATCH (n:Victim)"+" DETACH DELETE "+"missingVar", getParamsFromContext(ctx))
	require.Error(t, err)
	require.Nil(t, res)

	verify, err := exec.Execute(ctx, "MATCH (n:Victim) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, verify.Rows[0][0])
}

func TestResolveCorrelatedImportValue_OptionalFallbackBranch(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_opt_import")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "seed-1", Labels: []string{"Seed"}, Properties: map[string]interface{}{"name": "keep"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "t-1", Labels: []string{"Target"}, Properties: map[string]interface{}{"name": "T1"}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "e-1", Type: "REL", StartNode: "seed-1", EndNode: "t-1"})
	require.NoError(t, err)

	outerPart := "MATCH (seed:Seed) WHERE seed.name = 'none' OPTIONAL MATCH (seed)-[:REL]->(t:Target) WITH seed, t"
	fixtureAdmission682Context, fixtureAdmission682 := statementTestContext(ctx, outerPart)
	_ = fixtureAdmission682
	val, found, err := exec.resolveCorrelatedImportValue(fixtureAdmission682, fixtureAdmission682Context, outerPart, "seed", "seed-1", "t", map[string]map[string]interface{}{})
	require.NoError(t, err)
	require.True(t, found)
	switch target := val.(type) {
	case *storage.Node:
		require.Equal(t, storage.NodeID("t-1"), target.ID)
	case map[string]interface{}:
		require.Equal(t, "t-1", target["_nodeId"])
	default:
		t.Fatalf("unexpected import value type: %T", val)
	}
}

func TestResolveCorrelatedImportValue_DoesNotLeakSingleMismatchedRow(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "cov_opt_mismatch")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "seed-a", Labels: []string{"Seed"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "seed-b", Labels: []string{"Seed"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "target-b", Labels: []string{"Target"}, Properties: map[string]interface{}{"name": "B"}})
	require.NoError(t, err)
	err = store.CreateEdge(&storage.Edge{ID: "edge-b", Type: "REL", StartNode: "seed-b", EndNode: "target-b"})
	require.NoError(t, err)

	// This outer query yields exactly one row for seed-b. Asking for seed-a must
	// not return seed-b's imported value.
	outerPart := "MATCH (seed:Seed {id:'seed-b'}) OPTIONAL MATCH (seed)-[:REL]->(t:Target) WITH seed, t"
	fixtureAdmission683Context, fixtureAdmission683 := statementTestContext(ctx, outerPart)
	_ = fixtureAdmission683
	val, found, err := exec.resolveCorrelatedImportValue(fixtureAdmission683, fixtureAdmission683Context, outerPart, "seed", "seed-a", "t", nil)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, val)
}
