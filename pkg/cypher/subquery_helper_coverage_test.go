package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallSubqueryHelpers_SeedLookupAndRowMatching(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")

	_, err := store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"Seed"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "tail", Labels: []string{"Seed"}})
	require.NoError(t, err)

	exec := NewStorageExecutor(store)

	seed := exec.seedNodeFromIDString(" n1 ")
	require.NotNil(t, seed)
	require.Equal(t, storage.NodeID("n1"), seed.ID)

	seed = exec.seedNodeFromIDString("bolt:db:tail")
	require.NotNil(t, seed)
	require.Equal(t, storage.NodeID("tail"), seed.ID)

	require.Nil(t, exec.seedNodeFromIDString("   "))
	require.Nil(t, exec.seedNodeFromMap(map[string]interface{}{"id": "missing"}))

	seed = exec.seedNodeFromMap(map[string]interface{}{"_nodeId": "n1"})
	require.NotNil(t, seed)
	require.Equal(t, storage.NodeID("n1"), seed.ID)

	require.True(t, rowMatchesSeedID(&storage.Node{ID: "n1"}, "n1"))
	require.True(t, rowMatchesSeedID(storage.Node{ID: "n1"}, "n1"))
	require.True(t, rowMatchesSeedID(map[string]interface{}{"elementId": "bolt:db:tail"}, "tail"))
	require.True(t, rowMatchesSeedID("bolt:db:tail", "tail"))
	require.False(t, rowMatchesSeedID((*storage.Node)(nil), "n1"))
	require.False(t, rowMatchesSeedID(map[string]interface{}{"id": 42}, "42"))
	require.False(t, rowMatchesSeedID("other", "n1"))
}

func TestCallSubqueryHelpers_WithParsingAndNullGuards(t *testing.T) {
	vars, inner, hasWith, err := parseLeadingWithImports(" RETURN 1 AS one ")
	require.NoError(t, err)
	require.False(t, hasWith)
	require.Nil(t, vars)
	require.Equal(t, "RETURN 1 AS one", inner)

	vars, inner, hasWith, err = parseLeadingWithImports("WITH a, b AS bee RETURN a, bee")
	require.NoError(t, err)
	require.True(t, hasWith)
	require.Equal(t, []string{"a", "bee"}, vars)
	require.Equal(t, "RETURN a, bee", inner)

	vars, inner, hasWith, err = parseLeadingWithImports("WITH a WHERE a IS NOT NULL CREATE (n)")
	require.NoError(t, err)
	require.True(t, hasWith)
	require.Equal(t, []string{"a"}, vars)
	require.Equal(t, "WITH a WHERE a IS NOT NULL CREATE (n)", inner)

	_, _, hasWith, err = parseLeadingWithImports("WITH a")
	require.True(t, hasWith)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be followed by a query clause")

	whereExpr, rest, ok := splitLeadingWhereNullGuard("WITH a WHERE a IS NOT NULL CREATE (n)")
	require.True(t, ok)
	require.Equal(t, "a IS NOT NULL", whereExpr)
	require.Equal(t, "CREATE (n)", rest)

	whereExpr, rest, ok = splitLeadingWhereNullGuard("WHERE a IS NULL RETURN a")
	require.True(t, ok)
	require.Equal(t, "a IS NULL", whereExpr)
	require.Equal(t, "RETURN a", rest)

	_, _, ok = splitLeadingWhereNullGuard("WHERE a IS NULL")
	require.False(t, ok)

	pass, handled := evalWhereNullGuard("((a IS NULL))", map[string]interface{}{})
	require.True(t, handled)
	require.True(t, pass)

	pass, handled = evalWhereNullGuard("a IS NOT NULL", map[string]interface{}{"a": 1})
	require.True(t, handled)
	require.True(t, pass)

	_, handled = evalWhereNullGuard("a = 1", map[string]interface{}{"a": 1})
	require.False(t, handled)
}

func TestCallSubqueryHelpers_CorrelationAndTopLevelSplits(t *testing.T) {
	parts := splitTopLevelAndCallSubquery("n.id = outer AND size([x IN [1,2] WHERE x = 1]) > 0 AND 'A AND B' = 'A AND B'")
	require.Equal(t, []string{
		"n.id = outer",
		"size([x IN [1,2] WHERE x = 1]) > 0",
		"'A AND B' = 'A AND B'",
	}, parts)

	lhs, rhs, ok := splitTopLevelEqualityCallSubquery("n.id = outer")
	require.True(t, ok)
	require.Equal(t, "n.id", lhs)
	require.Equal(t, "outer", rhs)

	_, _, ok = splitTopLevelEqualityCallSubquery("'a=b'")
	require.False(t, ok)

	matchVar, matchProp, otherWhere, ok := extractCallSubqueryCorrelationWhere("n.id = outer AND outer IS NOT NULL AND n.age > 3", "outer")
	require.True(t, ok)
	require.Equal(t, "n", matchVar)
	require.Equal(t, "id", matchProp)
	require.Equal(t, "outer IS NOT NULL AND n.age > 3", otherWhere)

	sanitized, ok := sanitizeCallSubqueryOtherWhere(otherWhere, "outer")
	require.True(t, ok)
	require.Equal(t, "n.age > 3", sanitized)

	_, ok = sanitizeCallSubqueryOtherWhere("outer > 3 AND n.age > 3", "outer")
	require.False(t, ok)

	require.True(t, isCallSubqueryImportNotNullGuardTerm("`outer` IS NOT NULL", "outer"))
	require.False(t, isCallSubqueryImportNotNullGuardTerm("outer IS NULL", "outer"))
	require.True(t, containsStandaloneIdentifier("outer + 1 > 0", "outer"))
	require.False(t, containsStandaloneIdentifier("n.outer = 1", "outer"))
	require.False(t, containsStandaloneIdentifier("'outer' = name", "outer"))

	v, p, ok := parseCallSubqueryVarProp("`n`.`id`")
	require.True(t, ok)
	require.Equal(t, "n", v)
	require.Equal(t, "id", p)

	_, _, ok = parseCallSubqueryVarProp("1.bad")
	require.False(t, ok)

	require.True(t, isSimpleIdentifier("`outer_name`"))
	require.False(t, isSimpleIdentifier("1outer"))
}

func TestCallSubqueryHelpers_ModifiersJoinAndLookupKeys(t *testing.T) {
	branches, unionAll, ok := splitTopLevelUnionBranches("RETURN 'UNION' AS v UNION ALL RETURN 2 AS v")
	require.True(t, ok)
	require.True(t, unionAll)
	require.Equal(t, []string{"RETURN 'UNION' AS v", "RETURN 2 AS v"}, branches)

	branches, unionAll, ok = splitTopLevelUnionBranches("RETURN 1 AS v UNION RETURN 2 AS v")
	require.True(t, ok)
	require.False(t, unionAll)
	require.Equal(t, 2, len(branches))

	projection, modifiers := splitTopLevelResultModifiersCallSubquery("n.name, 'ORDER BY literal' ORDER BY n.age DESC SKIP 1 LIMIT 2")
	require.Equal(t, "n.name, 'ORDER BY literal'", projection)
	require.Equal(t, "ORDER BY n.age DESC SKIP 1 LIMIT 2", modifiers)

	idx := indexTopLevelKeywordCallSubquery("'ORDER BY' ORDER BY n.age", "ORDER BY")
	require.Equal(t, len("'ORDER BY' "), idx)

	col, desc, ok := parseOrderByModifier("ORDER BY n.age DESC SKIP 1 LIMIT 2")
	require.True(t, ok)
	require.Equal(t, "n.age", col)
	require.True(t, desc)

	e, _ := newTestExecutor(t)
	value, ok := e.parseIntModifier("ORDER BY n.age DESC SKIP 1 LIMIT 2", "SKIP")
	require.True(t, ok)
	require.Equal(t, 1, value)
	value, ok = e.parseIntModifier("ORDER BY n.age DESC SKIP 1 LIMIT 2", "LIMIT")
	require.True(t, ok)
	require.Equal(t, 2, value)
	_, ok = e.parseIntModifier("LIMIT nope", "LIMIT")
	require.False(t, ok)
	value, ok = e.parseIntModifier("ORDER BY n.age SKIP 1 + 1 LIMIT toInteger('3')", "SKIP")
	require.True(t, ok)
	require.Equal(t, 2, value)
	value, ok = e.parseIntModifier("ORDER BY n.age SKIP 1 + 1 LIMIT toInteger('3')", "LIMIT")
	require.True(t, ok)
	require.Equal(t, 3, value)
	_, ok = e.parseIntModifier("LIMIT 1 - 2", "LIMIT")
	require.False(t, ok)

	require.Equal(t, "<empty>", callSubqueryRowDedupKey(nil))
	require.Equal(t, "<nil>", callSubqueryLookupKeyString(nil))
	require.Equal(t, "s:name", callSubqueryLookupKeyString([]byte(" \"name\" ")))
	require.Equal(t, "i:7", callSubqueryLookupKeyString(7))
	require.Equal(t, "i64:8", callSubqueryLookupKeyString(int64(8)))
	require.Equal(t, "f:1.5", callSubqueryLookupKeyString(1.5))
	require.Equal(t, "b:1", callSubqueryLookupKeyString(true))
	require.Contains(t, callSubqueryLookupKeyString(struct{ X int }{X: 1}), "struct")

	dedupKey := callSubqueryRowDedupKey([]interface{}{nil, "s", []byte("b"), 1, int64(2), 3.5, float32(4.5), true})
	require.Equal(t, "n:|s:s|b:b|i:1|i64:2|f:3.5|f32:4.5|t:1", dedupKey)

	joined := crossJoinCallResults(
		&ExecuteResult{Columns: []string{"left", "shared"}, Rows: [][]interface{}{{"l1", "keep"}}},
		&ExecuteResult{Columns: []string{"shared", "right", "extra"}, Rows: [][]interface{}{{"drop", "r1", "x1"}, {"drop2", "r2"}}},
	)
	require.Equal(t, []string{"left", "shared", "right", "extra"}, joined.Columns)
	require.Equal(t, [][]interface{}{{"l1", "keep", "r1", "x1"}, {"l1", "keep", "r2", nil}}, joined.Rows)

	require.Same(t, joined, crossJoinCallResults(joined, nil))
	require.Same(t, joined, crossJoinCallResults(nil, joined))

	require.True(t, callSubqueryQueryIsWrite("MATCH (n) CREATE (m)"))
	require.False(t, callSubqueryQueryIsWrite("MATCH (n) RETURN n"))
}

func TestCallSubqueryHelpers_PropertyExtractionAndOrdering(t *testing.T) {
	require.Equal(t, "Ada", extractPropertyFromValue(map[string]interface{}{"name": "Ada"}, "name"))
	require.Equal(t, 42, extractPropertyFromValue(map[string]interface{}{"properties": map[string]interface{}{"age": 42}}, "age"))
	require.Nil(t, extractPropertyFromValue(map[string]interface{}{"properties": "wrong"}, "age"))
	require.Nil(t, extractPropertyFromValue("not-a-map", "age"))

	require.Equal(t, 1, compareValuesForSort(nil, 1))
	require.Equal(t, -1, compareValuesForSort(2, nil))
	require.Equal(t, -1, compareValuesForSort(1, 2))
	require.Equal(t, 1, compareValuesForSort("b", "a"))
	require.Equal(t, 1, compareValuesForSort(true, false))

	require.Equal(t, -1, compareRowRefsForOrder(rowRefForOrder{row: []interface{}{1}, idx: 0}, rowRefForOrder{row: []interface{}{1}, idx: 1}, 0, false))
	require.Equal(t, -1, compareRowRefsForOrder(rowRefForOrder{row: []interface{}{3}, idx: 0}, rowRefForOrder{row: []interface{}{1}, idx: 1}, 0, true))

	top := selectTopKRowsForOrder([][]interface{}{{3, "c"}, {1, "a"}, {2, "b"}}, 0, false, 2)
	require.Equal(t, [][]interface{}{{1, "a"}, {2, "b"}}, top)

	all := selectTopKRowsForOrder([][]interface{}{{2}, {1}}, 0, true, 5)
	require.Equal(t, [][]interface{}{{2}, {1}}, all)
	require.Empty(t, selectTopKRowsForOrder([][]interface{}{{1}}, 0, false, 0))
}
