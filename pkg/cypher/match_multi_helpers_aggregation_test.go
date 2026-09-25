package cypher

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCartesianHelpers_ParseAndFilterBranches(t *testing.T) {
	t.Run("parse helpers", func(t *testing.T) {
		v, p, expectNotNull, ok := parseCartesianNullTerm("a.name IS NOT NULL")
		require.True(t, ok)
		require.Equal(t, "a", v)
		require.Equal(t, "name", p)
		require.True(t, expectNotNull)

		v, p, expectNotNull, ok = parseCartesianNullTerm("a.name IS NULL")
		require.True(t, ok)
		require.Equal(t, "a", v)
		require.Equal(t, "name", p)
		require.False(t, expectNotNull)

		_, _, _, ok = parseCartesianNullTerm("a.name = 1")
		require.False(t, ok)

		parts := splitTopLevelAndConjuncts("a.x = 1 AND 'x AND y' = 'x AND y' AND [1,2] = [1,2]")
		require.Equal(t, []string{"a.x = 1", "'x AND y' = 'x AND y'", "[1,2] = [1,2]"}, parts)

		vv, pp, ok := parseCartesianVarProp(" a.`p` ")
		require.True(t, ok)
		require.Equal(t, "a", vv)
		require.Equal(t, "p", pp)

		_, _, ok = parseCartesianVarProp("a")
		require.False(t, ok)

		varName, prop, listVals, ok := parseCartesianInListTerm("a.kind IN ['x', 2, true]")
		require.True(t, ok)
		require.Equal(t, "a", varName)
		require.Equal(t, "kind", prop)
		require.Equal(t, []interface{}{"x", int64(2), true}, listVals)

		_, _, _, ok = parseCartesianInListTerm("a.kind IN bad")
		require.False(t, ok)

		require.True(t, isSimpleIdentifierCartesian("a_1"))
		require.False(t, isSimpleIdentifierCartesian("1a"))

		lv, lp, rv, rp, ok := parseCartesianVarPropEqualityTerm("a.id = b.id")
		require.True(t, ok)
		require.Equal(t, "a", lv)
		require.Equal(t, "id", lp)
		require.Equal(t, "b", rv)
		require.Equal(t, "id", rp)

		_, _, _, _, ok = parseCartesianVarPropEqualityTerm("a.id = 10")
		require.False(t, ok)

		require.Equal(t, "<nil>", cartesianValueKey(nil))
		require.Equal(t, "s:x", cartesianValueKey("x"))
		require.Equal(t, "i:2", cartesianValueKey(2))
		require.Equal(t, "i64:3", cartesianValueKey(int64(3)))
		require.Equal(t, "f:2.5", cartesianValueKey(2.5))
		require.Equal(t, "b:1", cartesianValueKey(true))
		require.Equal(t, "b:0", cartesianValueKey(false))
		require.Equal(t, "[]int:[1 2]", cartesianValueKey([]int{1, 2}))
	})

	t.Run("prop collection and filters", func(t *testing.T) {
		nodes := []*storage.Node{
			nil,
			{ID: "n1", Properties: map[string]interface{}{"k": "a", "nullable": nil}},
			{ID: "n2", Properties: map[string]interface{}{"k": "b", "nullable": "x"}},
			{ID: "n3", Properties: map[string]interface{}{"other": 1}},
		}

		vals := collectPropValues(nodes, "k")
		require.Len(t, vals, 2)
		_, hasA := vals["s:a"]
		_, hasB := vals["s:b"]
		require.True(t, hasA)
		require.True(t, hasB)

		filtered := filterNodesByAllowedPropSet(nodes, "k", map[string]struct{}{"s:b": {}})
		require.Len(t, filtered, 1)
		require.Equal(t, storage.NodeID("n2"), filtered[0].ID)

		noFilter := filterNodesByAllowedPropSet(nodes, "k", map[string]struct{}{})
		require.Len(t, noFilter, len(nodes))

		notNull := filterNodesByNullConstraint(nodes, "nullable", true)
		require.Len(t, notNull, 1)
		require.Equal(t, storage.NodeID("n2"), notNull[0].ID)

		onlyNull := filterNodesByNullConstraint(nodes, "nullable", false)
		require.Len(t, onlyNull, 2)
		require.ElementsMatch(t, []storage.NodeID{"n1", "n3"}, []storage.NodeID{onlyNull[0].ID, onlyNull[1].ID})
	})

	t.Run("apply null constraints", func(t *testing.T) {
		patternMatches := []struct {
			variable string
			nodes    []*storage.Node
		}{
			{variable: "a", nodes: []*storage.Node{{ID: "n1", Properties: map[string]interface{}{"k": "x"}}, {ID: "n2", Properties: map[string]interface{}{"k": nil}}}},
			{variable: "b", nodes: []*storage.Node{{ID: "n3", Properties: map[string]interface{}{"k": "y"}}}},
		}
		varIndex := map[string]int{"a": 0, "b": 1}
		nullConstraints := map[string]cartesianNullConstraint{
			"a|k": {expectNotNull: true, hasValue: true},
			"b|k": {conflict: true},
		}

		applyCartesianNullConstraints(patternMatches, varIndex, nullConstraints)
		require.Len(t, patternMatches[0].nodes, 1)
		require.Equal(t, storage.NodeID("n1"), patternMatches[0].nodes[0].ID)
		require.Empty(t, patternMatches[1].nodes)
	})

	t.Run("where pushdown", func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		patternMatches := []struct {
			variable string
			nodes    []*storage.Node
		}{
			{
				variable: "a",
				nodes: []*storage.Node{
					{ID: "a1", Properties: map[string]interface{}{"id": "x", "v": "keep"}},
					{ID: "a2", Properties: map[string]interface{}{"id": "y", "v": nil}},
				},
			},
			{
				variable: "b",
				nodes: []*storage.Node{
					{ID: "b1", Properties: map[string]interface{}{"id": "x"}},
					{ID: "b2", Properties: map[string]interface{}{"id": "z"}},
				},
			},
		}

		out := exec.applyCartesianWherePushdown(patternMatches, "a.id IN ['x'] AND a.v IS NOT NULL AND a.id = b.id")
		require.Len(t, out[0].nodes, 1)
		require.Equal(t, storage.NodeID("a1"), out[0].nodes[0].ID)
		require.Len(t, out[1].nodes, 1)
		require.Equal(t, storage.NodeID("b1"), out[1].nodes[0].ID)

		conflicted := exec.applyCartesianWherePushdown(patternMatches, "a.v IS NULL AND a.v IS NOT NULL")
		require.Empty(t, conflicted[0].nodes)
	})
}

func TestExecuteCartesianAggregation_Branches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	a1 := &storage.Node{ID: "a1", Properties: map[string]interface{}{"kind": "k1", "name": "a1"}}
	a2 := &storage.Node{ID: "a2", Properties: map[string]interface{}{"kind": "k2", "name": "a2"}}
	b1 := &storage.Node{ID: "b1", Properties: map[string]interface{}{"name": "b1"}}
	b2 := &storage.Node{ID: "b2", Properties: map[string]interface{}{"name": "b2"}}

	allMatches := []map[string]*storage.Node{
		{"a": a1, "b": b1},
		{"a": a1, "b": b2},
		{"a": a2, "b": b2},
	}

	res := &ExecuteResult{Rows: [][]interface{}{}}
	ungroupedItems := []returnItem{{expr: "COUNT(*)"}, {expr: "COLLECT(a.kind)"}}
	out, err := exec.executeCartesianAggregation(ctx, allMatches, ungroupedItems, res)
	require.NoError(t, err)
	require.Len(t, out.Rows, 1)
	require.Equal(t, int64(3), out.Rows[0][0])
	require.Equal(t, []interface{}{"k1", "k1", "k2"}, out.Rows[0][1])

	res = &ExecuteResult{Rows: [][]interface{}{}}
	groupedItems := []returnItem{{expr: "a.kind"}, {expr: "COUNT(*)"}, {expr: "COLLECT(b.name)"}}
	out, err = exec.executeCartesianAggregation(ctx, allMatches, groupedItems, res)
	require.NoError(t, err)
	require.Len(t, out.Rows, 2)

	rowsByKind := make(map[string][]interface{}, 2)
	for _, row := range out.Rows {
		kind := row[0].(string)
		rowsByKind[kind] = row
	}
	require.Equal(t, int64(2), rowsByKind["k1"][1])
	require.Equal(t, []interface{}{"b1", "b2"}, rowsByKind["k1"][2])
	require.Equal(t, int64(1), rowsByKind["k2"][1])
	require.Equal(t, []interface{}{"b2"}, rowsByKind["k2"][2])
}

func TestExecuteAggregation_GroupedAndSingleGroupBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	nodes := []*storage.Node{
		{ID: "n1", Properties: map[string]interface{}{"grp": "a", "val": 1, "num": int64(10), "avg": 2.0, "min": int64(3), "max": int64(11), "tag": "x"}},
		{ID: "n2", Properties: map[string]interface{}{"grp": "a", "val": 2, "num": 5, "avg": 4.0, "min": int64(2), "max": int64(12), "tag": "x"}},
		{ID: "n3", Properties: map[string]interface{}{"grp": "b", "num": 7.5, "avg": 6.0, "min": 1.5, "max": 14.0, "tag": "y"}},
	}

	groupItems := []returnItem{
		{expr: "n.grp"},
		{expr: "COUNT(*)"},
		{expr: "COUNT(n.val)"},
		{expr: "SUM(n.num)"},
		{expr: "AVG(n.avg)"},
		{expr: "MIN(n.min)"},
		{expr: "MAX(n.max)"},
		{expr: "COLLECT(n.tag)"},
	}
	res := &ExecuteResult{Rows: [][]interface{}{}}
	out, err := exec.executeAggregation(ctx, nodes, "n", groupItems, res)
	require.NoError(t, err)
	require.Len(t, out.Rows, 2)

	rowsByGroup := make(map[string][]interface{}, 2)
	for _, row := range out.Rows {
		rowsByGroup[row[0].(string)] = row
	}
	require.Equal(t, int64(2), rowsByGroup["a"][1])
	require.Equal(t, int64(2), rowsByGroup["a"][2])
	require.Equal(t, int64(15), rowsByGroup["a"][3])
	require.Equal(t, 3.0, rowsByGroup["a"][4])
	require.Equal(t, int64(2), rowsByGroup["a"][5])
	require.Equal(t, int64(12), rowsByGroup["a"][6])
	require.Equal(t, []interface{}{"x", "x"}, rowsByGroup["a"][7])

	require.Equal(t, int64(1), rowsByGroup["b"][1])
	require.Equal(t, int64(0), rowsByGroup["b"][2])
	require.Equal(t, 7.5, rowsByGroup["b"][3])
	require.Equal(t, 6.0, rowsByGroup["b"][4])
	require.Equal(t, 1.5, rowsByGroup["b"][5])
	require.Equal(t, 14.0, rowsByGroup["b"][6])
	require.Equal(t, []interface{}{"y"}, rowsByGroup["b"][7])

	singleItems := []returnItem{
		{expr: "COUNT(*)"},
		{expr: "COUNT(DISTINCT n.tag)"},
		{expr: "COUNT(n.val)"},
		{expr: "SUM(n.num)"},
		{expr: "SUM(1)"},
		{expr: "AVG(n.avg)"},
		{expr: "MIN(n.min)"},
		{expr: "MAX(n.max)"},
		{expr: "COLLECT(DISTINCT n.tag)"},
		{expr: "COLLECT(n.tag)"},
		{expr: "n.grp"},
	}
	res = &ExecuteResult{}
	out, err = exec.executeAggregationSingleGroup(ctx, nodes, "n", singleItems, res)
	require.NoError(t, err)
	require.Len(t, out.Rows, 1)
	row := out.Rows[0]
	require.Equal(t, int64(3), row[0])
	require.Equal(t, int64(2), row[1])
	require.Equal(t, int64(2), row[2])
	require.Equal(t, 22.5, row[3])
	require.Equal(t, int64(3), row[4])
	require.Equal(t, 4.0, row[5])
	require.Equal(t, 1.5, row[6])
	require.Equal(t, 14.0, row[7])

	distinct := row[8].([]interface{})
	sort.Slice(distinct, func(i, j int) bool { return fmt.Sprint(distinct[i]) < fmt.Sprint(distinct[j]) })
	require.Equal(t, []interface{}{"x", "y"}, distinct)
	require.Equal(t, []interface{}{"x", "x", "y"}, row[9])
	require.Equal(t, "a", row[10])

	// No rows still uses single-group aggregation path.
	emptyRes := &ExecuteResult{}
	emptyOut, err := exec.executeAggregation(ctx, nil, "n", []returnItem{{expr: "COUNT(*)"}}, emptyRes)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, emptyOut.Rows)
}

func TestExecuteAggregationSingleGroup_ExtraBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	nodes := []*storage.Node{
		{ID: "n1", Properties: map[string]interface{}{"tag": "x", "num": int64(2), "num2": 3.0, "flag": true}},
		{ID: "n2", Properties: map[string]interface{}{"tag": "x", "num": int64(1), "num2": 1.0, "flag": false}},
		{ID: "n3", Properties: map[string]interface{}{"tag": "y", "num": int64(4), "num2": 2.0, "flag": true}},
	}

	items := []returnItem{
		{expr: "SUM(n.num) + SUM(n.num2)"},
		{expr: "COUNT(DISTINCT n)"},
		{expr: "COUNT(CASE WHEN n.flag = true THEN 1 END)"},
		{expr: "SUM(CASE WHEN n.flag = true THEN 1 ELSE 0 END)"},
		{expr: "SUM(1)"},
		{expr: "AVG(n.missing)"},
		{expr: "MIN(n.missing)"},
		{expr: "MAX(n.missing)"},
		{expr: "COLLECT(DISTINCT {tag: n.tag})"},
		{expr: "COLLECT(n.tag)[..2]"},
		{expr: "COLLECT({tag: n.tag})"},
	}

	res := &ExecuteResult{}
	out, err := exec.executeAggregationSingleGroup(ctx, nodes, "n", items, res)
	require.NoError(t, err)
	require.Len(t, out.Rows, 1)
	row := out.Rows[0]
	require.Equal(t, 13.0, row[0]) // 7 + 6
	require.Equal(t, int64(3), row[1])
	require.Equal(t, int64(2), row[2])
	require.Equal(t, int64(2), row[3])
	require.Equal(t, int64(3), row[4])
	require.Nil(t, row[5])
	require.Nil(t, row[6])
	require.Nil(t, row[7])
	require.Len(t, row[8].([]interface{}), 2)
	require.Equal(t, []interface{}{"x", "x"}, row[9])
	require.Len(t, row[10].([]interface{}), 3)
}

func TestExecuteMatchWithUnwind_Branches(t *testing.T) {
	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "nornic:n1", Labels: []string{"Thing"}, Properties: map[string]interface{}{"id": "n1", "labels": []string{"b", "a"}}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "nornic:n2", Labels: []string{"Thing"}, Properties: map[string]interface{}{"id": "n2", "labels": []string{"a"}}})
	require.NoError(t, err)

	res, err := exec.executeMatchWithUnwind(ctx, "MATCH (n:Thing) WITH n, n.labels AS labels UNWIND labels AS label RETURN label, n.id ORDER BY label")
	require.NoError(t, err)
	require.Equal(t, []string{"label", "n.id"}, res.Columns)
	require.Equal(t, [][]interface{}{{"a", "n1"}, {"a", "n2"}, {"b", "n1"}}, res.Rows)

	res, err = exec.executeMatchWithUnwind(ctx, "MATCH (n:Thing) WITH n, n.labels AS labels UNWIND labels AS type WITH type, COUNT(*) AS cnt RETURN type, cnt ORDER BY type")
	require.NoError(t, err)
	require.Equal(t, []string{"type", "cnt"}, res.Columns)
	require.Equal(t, [][]interface{}{{"a", int64(2)}, {"b", int64(1)}}, res.Rows)

	_, err = exec.executeMatchWithUnwind(ctx, "MATCH (n:Thing) WITH n UNWIND [1,2,3] RETURN n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "UNWIND requires AS clause")

	_, err = exec.executeMatchWithUnwind(ctx, "MATCH (n:Thing) RETURN n")
	require.Error(t, err)
	require.Contains(t, err.Error(), "MATCH, WITH, UNWIND, and RETURN clauses required")
}

func TestEvaluateWhereForContext_RelationshipAndBooleanBranches(t *testing.T) {
	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "nornic:a", Labels: []string{"N"}, Properties: map[string]interface{}{"id": "a", "x": int64(1)}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "nornic:b", Labels: []string{"N"}, Properties: map[string]interface{}{"id": "b", "x": int64(2)}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "nornic:r1", Type: "R", StartNode: "nornic:a", EndNode: "nornic:b"}))

	a, err := store.GetNode("nornic:a")
	require.NoError(t, err)
	b, err := store.GetNode("nornic:b")
	require.NoError(t, err)
	nodeMap := map[string]*storage.Node{"a": a, "b": b}

	require.True(t, exec.evaluateWhereForContext(ctx, "(a)-[:R]->(b)", nodeMap))
	require.True(t, exec.evaluateWhereForContext(ctx, "(b)<-[:R]-(a)", nodeMap))
	require.False(t, exec.evaluateWhereForContext(ctx, "(a)<-[:R]-(b)", nodeMap))
	require.True(t, exec.evaluateWhereForContext(ctx, "a.x = 1 AND b.x = 2", nodeMap))
	require.True(t, exec.evaluateWhereForContext(ctx, "a.x = 3 OR b.x = 2", nodeMap))
	require.True(t, exec.evaluateWhereForContext(ctx, "NOT a.x = 3", nodeMap))
}
