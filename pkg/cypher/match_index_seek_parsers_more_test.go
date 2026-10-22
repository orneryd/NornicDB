package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIndexedEqualityResolvesBoundRowValues(t *testing.T) {
	executor, ctx := newUnitExecutor(t)
	ctx = withValueBindings(ctx, map[string]interface{}{
		"key": "bound-key",
		"row": map[string]interface{}{"props": map[string]interface{}{"key": int64(9007199254740993)}},
	})
	for _, test := range []struct {
		predicate string
		want      interface{}
	}{
		{"n.k = key", "bound-key"},
		{"key = n.k", "bound-key"},
		{"n.k = row.props.key", int64(9007199254740993)},
	} {
		t.Run(test.predicate, func(t *testing.T) {
			fixtureAdmission1003Context, fixtureAdmission1003 := expressionTestContext(ctx, test.predicate)
			_ = fixtureAdmission1003Context
			property, value, admitted := executor.parseSimpleIndexedEquality(fixtureAdmission1003, fixtureAdmission1003Context, "n", test.predicate)
			require.True(t, admitted)
			require.Equal(t, "k", property)
			require.Equal(t, test.want, value)
		})
	}
	fixtureAdmission1308Context, fixtureAdmission1308 := expressionTestContext(ctx, "n.k = n.other")
	_ = fixtureAdmission1308Context
	_, _, admitted := executor.parseSimpleIndexedEquality(fixtureAdmission1308, fixtureAdmission1308Context, "n", "n.k = n.other")
	require.False(t, admitted)
}

func TestIndexedWhereKeepsIncomingRowsSeparate(t *testing.T) {
	for _, expression := range []string{"key", "row.props.key"} {
		t.Run(expression, func(t *testing.T) {
			executor := transactionIndexExecutor(t, 3)
			ctx := context.Background()
			query := "UNWIND $keys AS key MATCH (n:P) WHERE n.k = key RETURN n.v AS value ORDER BY value LIMIT 2"
			params := map[string]interface{}{"keys": []interface{}{"k0", "k2"}}
			if expression == "row.props.key" {
				query = "UNWIND $rows AS row MATCH (n:P) WHERE n.k = row.props.key RETURN n.v AS value ORDER BY value LIMIT 2"
				params = map[string]interface{}{"rows": []interface{}{
					map[string]interface{}{"props": map[string]interface{}{"key": "k0"}},
					map[string]interface{}{"props": map[string]interface{}{"key": "k2"}},
				}}
			}
			result, err := executor.Execute(ctx, query, params)
			require.NoError(t, err)
			require.Equal(t, []string{"value"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(0)}, {int64(2)}}, result.Rows)
		})
	}
}

func TestMatchIndexSeek_ParserAdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "seek_parser_more_cov"))
	ctx := context.Background()

	fixtureAdmission1309Context, fixtureAdmission1309 := expressionTestContext(ctx, "")
	_ = fixtureAdmission1309Context
	prop, val, ok := exec.parseSimpleIndexedEquality(fixtureAdmission1309, fixtureAdmission1309Context, "n", "")
	require.False(t, ok)
	require.Empty(t, prop)
	require.Nil(t, val)

	fixtureAdmission1310Context, fixtureAdmission1310 := expressionTestContext(ctx, "n.a >= 1")
	_ = fixtureAdmission1310Context
	_, _, ok = exec.parseSimpleIndexedEquality(fixtureAdmission1310, fixtureAdmission1310Context, "n", "n.a >= 1")
	require.False(t, ok)
	fixtureAdmission1311Context, fixtureAdmission1311 := expressionTestContext(ctx, "n.a IN [1]")
	_ = fixtureAdmission1311Context
	_, _, ok = exec.parseSimpleIndexedEquality(fixtureAdmission1311, fixtureAdmission1311Context, "n", "n.a IN [1]")
	require.False(t, ok)
	fixtureAdmission1312Context, fixtureAdmission1312 := expressionTestContext(ctx, "n.a IS NOT NULL")
	_ = fixtureAdmission1312Context
	_, _, ok = exec.parseSimpleIndexedEquality(fixtureAdmission1312, fixtureAdmission1312Context, "n", "n.a IS NOT NULL")
	require.False(t, ok)

	fixtureAdmission1313Context, fixtureAdmission1313 := expressionTestContext(ctx, "n.a = 'x'")
	_ = fixtureAdmission1313Context
	prop, val, ok = exec.parseSimpleIndexedEquality(fixtureAdmission1313, fixtureAdmission1313Context, "n", "n.a = 'x'")
	require.True(t, ok)
	require.Equal(t, "a", prop)
	require.Equal(t, "x", val)

	fixtureAdmission1314Context, fixtureAdmission1314 := expressionTestContext(ctx, "'x' = n.a")
	_ = fixtureAdmission1314Context
	prop, val, ok = exec.parseSimpleIndexedEquality(fixtureAdmission1314, fixtureAdmission1314Context, "n", "'x' = n.a")
	require.True(t, ok)
	require.Equal(t, "a", prop)
	require.Equal(t, "x", val)

	prop, vals, ok := exec.parseSimpleIndexedInParam("n", "n.k IN $keys", map[string]interface{}{"keys": []interface{}{"a", "a", nil, "b"}})
	require.True(t, ok)
	require.Equal(t, "k", prop)
	require.Equal(t, []interface{}{"a", "b"}, vals)

	_, _, ok = exec.parseSimpleIndexedInParam("n", "n.k IN keys", map[string]interface{}{"keys": []interface{}{"a"}})
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedInParam("n", "n.k IN $missing", map[string]interface{}{"keys": []interface{}{"a"}})
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedInParam("n", "n.k IN $keys", map[string]interface{}{"keys": nil})
	require.False(t, ok)
	_, _, ok = exec.parseSimpleIndexedInParam("n", "n.k IN $keys AND n.x = 1", map[string]interface{}{"keys": []interface{}{"a"}})
	require.False(t, ok)

	fixtureAdmission1315Context, fixtureAdmission1315 := expressionTestContext(ctx, "n.k IN ['a','a',null,'b']")
	_ = fixtureAdmission1315Context
	prop, vals, ok = exec.parseSimpleIndexedInLiteral(fixtureAdmission1315, fixtureAdmission1315Context, "n", "n.k IN ['a','a',null,'b']")
	require.True(t, ok)
	require.Equal(t, "k", prop)
	require.Equal(t, []interface{}{"a", "b"}, vals)

	fixtureAdmission1316Context, fixtureAdmission1316 := expressionTestContext(ctx, "n.k IN 'a'")
	_ = fixtureAdmission1316Context
	_, _, ok = exec.parseSimpleIndexedInLiteral(fixtureAdmission1316, fixtureAdmission1316Context, "n", "n.k IN 'a'")
	require.False(t, ok)
	fixtureAdmission1317Context, fixtureAdmission1317 := expressionTestContext(ctx, "n.k IN ['a'] OR n.x = 1")
	_ = fixtureAdmission1317Context
	_, _, ok = exec.parseSimpleIndexedInLiteral(fixtureAdmission1317, fixtureAdmission1317Context, "n", "n.k IN ['a'] OR n.x = 1")
	require.False(t, ok)

	p, ok := exec.parseSimpleIndexedIsNotNull("n", "n.k IS NOT NULL")
	require.True(t, ok)
	require.Equal(t, "k", p)

	_, ok = exec.parseSimpleIndexedIsNotNull("n", "n.k IS NOT NULL AND n.x IS NOT NULL")
	require.False(t, ok)
	_, ok = exec.parseSimpleIndexedIsNotNull("n", "n.k IS NOT NULL AND n.x > 1")
	require.False(t, ok)

	require.Equal(t, "x", unwrapOuterParens("(((x)))"))
	require.Equal(t, []string{"a=1", "b='x AND y'", "c=2"}, splitTopLevelAndConjuncts("a=1 AND b='x AND y' AND c=2"))
}

func TestMatchIndexSeek_IDEqualityCompoundAndIDInAdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seek_id_comp_more_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, n := range []*storage.Node{
		{ID: storage.NodeID("n1"), Labels: []string{"L"}, Properties: map[string]interface{}{"k": "v1"}},
		{ID: storage.NodeID("n2"), Labels: []string{"L"}, Properties: map[string]interface{}{"k": "v2"}},
	} {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}

	// The value side `'n1' OR` is no constant: the seek leaves the predicate
	// to the row filter instead of looking up its text (#844).
	fixtureAdmission1318Context, fixtureAdmission1318 := expressionTestContext(ctx, "(id(n) = 'n1' OR )")
	_ = fixtureAdmission1318Context
	nodes, used, err := exec.tryCollectNodesFromIDEqualityCompound(fixtureAdmission1318, fixtureAdmission1318Context, nodePatternInfo{variable: "n"}, "(id(n) = 'n1' OR )", nil)
	require.NoError(t, err)
	require.False(t, used)
	require.Empty(t, nodes)

	fixtureAdmission1319Context, fixtureAdmission1319 := expressionTestContext(ctx, "(id(n) = 'n1' AND elementId(n) = '4:nornic:n2')")
	_ = fixtureAdmission1319Context
	nodes, used, err = exec.tryCollectNodesFromIDEqualityCompound(fixtureAdmission1319, fixtureAdmission1319Context, nodePatternInfo{variable: "n"}, "(id(n) = 'n1' AND elementId(n) = '4:nornic:n2')", nil)
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("n1"), nodes[0].ID)

	fixtureAdmission1320Context, fixtureAdmission1320 := expressionTestContext(ctx, "id(n) IN $ids OR n.k='v1'")
	_ = fixtureAdmission1320Context
	nodes, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1320, fixtureAdmission1320Context, nodePatternInfo{variable: "n"}, "id(n) IN $ids OR n.k='v1'", map[string]interface{}{"ids": []interface{}{"n1"}})
	require.NoError(t, err)
	require.False(t, used)
	require.Nil(t, nodes)

	fixtureAdmission1321Context, fixtureAdmission1321 := expressionTestContext(ctx, "id(n) IN $ids")
	_ = fixtureAdmission1321Context
	nodes, used, err = exec.tryCollectNodesFromIDIn(fixtureAdmission1321, fixtureAdmission1321Context, nodePatternInfo{variable: "n"}, "id(n) IN $ids", map[string]interface{}{"ids": []interface{}{nil, 7, "", "n2"}})
	require.NoError(t, err)
	require.True(t, used)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("n2"), nodes[0].ID)
}
