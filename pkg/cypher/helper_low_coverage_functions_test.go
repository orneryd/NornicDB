package cypher

import (
	"context"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLowCoverageHelpers_SmallFunctions(t *testing.T) {
	// call_compat
	edge := &storage.Edge{Properties: map[string]interface{}{"title": "Hello World", "desc": "Graph DB"}}
	require.True(t, edgePropertiesContain(edge, []string{"title"}, "hello"))
	require.False(t, edgePropertiesContain(edge, []string{"missing"}, "hello"))
	require.True(t, edgePropertiesContain(edge, nil, "graph"))

	// call_temporal
	u, err := coerceUint64Arg(7, "seq")
	require.NoError(t, err)
	require.EqualValues(t, 7, u)
	u, err = coerceUint64Arg(int64(9), "seq")
	require.NoError(t, err)
	require.EqualValues(t, 9, u)
	u, err = coerceUint64Arg(float64(10), "seq")
	require.NoError(t, err)
	require.EqualValues(t, 10, u)
	u, err = coerceUint64Arg("11", "seq")
	require.NoError(t, err)
	require.EqualValues(t, 11, u)
	_, err = coerceUint64Arg(-1, "seq")
	require.Error(t, err)
	_, err = coerceUint64Arg(1.5, "seq")
	require.Error(t, err)
	_, err = coerceUint64Arg(struct{}{}, "seq")
	require.Error(t, err)

	// executor_use
	db, rem, hasUse, err := parseLeadingUseClause("USE mydb MATCH (n) RETURN n")
	require.NoError(t, err)
	require.True(t, hasUse)
	require.Equal(t, "mydb", db)
	require.Equal(t, "MATCH (n) RETURN n", rem)

	db, rem, hasUse, err = parseLeadingUseClause("USE `db``name` RETURN 1")
	require.NoError(t, err)
	require.True(t, hasUse)
	require.Equal(t, "db`name", db)
	require.Equal(t, "RETURN 1", rem)

	_, _, hasUse, err = parseLeadingUseClause("MATCH (n) RETURN n")
	require.NoError(t, err)
	require.False(t, hasUse)

	_, _, hasUse, err = parseLeadingUseClause("USE")
	require.Error(t, err)
	require.True(t, hasUse)

	db, rem, hasUse, err = parseLeadingUseClause("USE graph.byName('dbx') RETURN 1")
	require.NoError(t, err)
	require.True(t, hasUse)
	require.Equal(t, "dbx", db)
	require.Equal(t, "RETURN 1", rem)

	_, _, hasUse, err = parseLeadingUseClause("USE graph.byName() RETURN 1")
	require.Error(t, err)
	require.True(t, hasUse)

	_, _, hasUse, err = parseLeadingUseClause("USE `broken RETURN 1")
	require.Error(t, err)
	require.True(t, hasUse)

	// predicate_helpers
	node := &storage.Node{ID: "node-1", Properties: map[string]interface{}{"id": "prop-id", "name": "A"}}
	v, ok := getBindingNodeValue(node, "id")
	require.True(t, ok)
	require.Equal(t, "prop-id", v)
	nodeNoPropID := &storage.Node{ID: "node-2", Properties: map[string]interface{}{"name": "B"}}
	v, ok = getBindingNodeValue(nodeNoPropID, "id")
	require.True(t, ok)
	require.Equal(t, "node-2", v)

	// set_helpers
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "low_helpers"))
	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{"props": map[string]interface{}{"a": int64(1), "drop": nil}})
	n := &storage.Node{ID: "n-1", Properties: map[string]interface{}{}}
	exec.applySetMapMergeToNode(ctx, n, "n", "$props", map[string]*storage.Node{"n": n}, nil, &setWrites{})
	require.EqualValues(t, int64(1), n.Properties["a"])
	_, hasDrop := n.Properties["drop"]
	require.False(t, hasDrop)

	exec.applySetMapMergeToNode(context.Background(), n, "n", "{b: 2}", map[string]*storage.Node{"n": n}, nil, &setWrites{})
	require.EqualValues(t, int64(2), n.Properties["b"])

	// unwind_multi_match_create
	require.Equal(t, "n:", propEqKeyBatch(nil))
	require.Equal(t, "i:2", propEqKeyBatch(int64(2)))
	require.Equal(t, "i:2", propEqKeyBatch(float64(2)))
	require.Equal(t, "f:2.5", propEqKeyBatch(float64(2.5)))
	require.Equal(t, "s:x", propEqKeyBatch("x"))
	require.Equal(t, "b:true", propEqKeyBatch(true))

	// match_with_chain
	nodes := []*storage.Node{{Properties: map[string]interface{}{"score": int64(1)}}, {Properties: map[string]interface{}{"score": nil}}, {Properties: map[string]interface{}{}}}
	count, ok := countForExpr(nodes, "n", "*")
	require.True(t, ok)
	require.EqualValues(t, 3, count)
	count, ok = countForExpr(nodes, "n", "n.score")
	require.True(t, ok)
	require.EqualValues(t, 1, count)
	_, ok = countForExpr(nodes, "n", "m.score")
	require.False(t, ok)

	// knowledgepolicy_ddl tiny helper
	require.Equal(t, 1, kpExpectByte("{x", 0, '{'))
	require.Equal(t, -1, kpExpectByte("{x", 1, '{'))
}

func TestSchemaParsingHelpers_Branches(t *testing.T) {
	vals, err := parseDomainValueList(`'a', "b", 1, 2.5, true, false, bare`)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"a", "b", int64(1), float64(2.5), true, false, "bare"}, vals)

	vals, err = parseDomainValueList("   ")
	require.NoError(t, err)
	require.Nil(t, vals)

	_, err = parseDomainValueList(`'unterminated`)
	require.Error(t, err)

	types, err := parseFulltextRelationshipTypes("OWNS|`MANAGES TEAM`")
	require.NoError(t, err)
	require.Equal(t, []string{"OWNS", "MANAGES TEAM"}, types)

	_, err = parseFulltextRelationshipTypes("OWNS|")
	require.Error(t, err)
	require.ErrorIs(t, err, nerrors.ErrInvalidFulltextRelationshipTypes)

	_, err = parseFulltextRelationshipTypes("`BROKEN")
	require.Error(t, err)
	require.ErrorIs(t, err, nerrors.ErrInvalidFulltextRelationshipTypes)
}

func TestExtractSeedNodesFromResult_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_extract")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{ID: "n1", Labels: []string{"N"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "n2", Labels: []string{"N"}})
	require.NoError(t, err)

	res := &ExecuteResult{
		Columns: []string{"seed"},
		Rows: [][]interface{}{
			{storage.NodeID("n1")}, // ignored type
			{map[string]interface{}{"_nodeId": "n1"}},
			{"n2"},
			{[]interface{}{"n1"}},
			{[]interface{}{map[string]interface{}{"id": "n2"}}},
		},
	}
	nodes, err := exec.extractSeedNodesFromResult(res, "seed")
	require.NoError(t, err)
	require.Len(t, nodes, 4)

	_, err = exec.extractSeedNodesFromResult(&ExecuteResult{Columns: []string{"other"}, Rows: [][]interface{}{{"n1"}}}, "seed")
	require.Error(t, err)

	nodes, err = exec.extractSeedNodesFromResult(nil, "seed")
	require.NoError(t, err)
	require.Empty(t, nodes)
}

func TestAggregatePathSum_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "agg_sum")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	paths := []PathResult{
		{Nodes: []*storage.Node{{ID: "a", Properties: map[string]interface{}{"v": int64(2)}}}},
		{Nodes: []*storage.Node{{ID: "b", Properties: map[string]interface{}{"v": float64(3.5)}}}},
		{Nodes: []*storage.Node{{ID: "c", Properties: map[string]interface{}{}}}},
	}
	m := &TraversalMatch{StartNode: nodePatternInfo{variable: "n"}}
	got := exec.aggregatePathSum(ctx, paths, m, "n.v")
	require.Equal(t, 5.5, got)

	got = exec.aggregatePathSum(ctx, []PathResult{{Nodes: []*storage.Node{{ID: "z", Properties: map[string]interface{}{}}}}}, m, "n.v")
	require.EqualValues(t, int64(0), got)
}
