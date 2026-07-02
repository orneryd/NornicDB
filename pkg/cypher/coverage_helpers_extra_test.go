package cypher

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cypherVisibleMemoryEngine struct {
	*storage.MemoryEngine
	nodes []*storage.Node
	edges []*storage.Edge
}

func (e *cypherVisibleMemoryEngine) GetNodesByLabelVisibleAt(label string, version storage.MVCCVersion) ([]*storage.Node, error) {
	return e.nodes, nil
}

func (e *cypherVisibleMemoryEngine) GetOutgoingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return e.edges, nil
}

func (e *cypherVisibleMemoryEngine) GetIncomingEdgesVisibleAt(nodeID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return e.edges, nil
}

func (e *cypherVisibleMemoryEngine) GetEdgesByTypeVisibleAt(edgeType string, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return e.edges, nil
}

func (e *cypherVisibleMemoryEngine) GetEdgesBetweenVisibleAt(startID, endID storage.NodeID, version storage.MVCCVersion) ([]*storage.Edge, error) {
	return e.edges, nil
}

func TestAggregateExpressionHelpers(t *testing.T) {
	cases := []struct {
		expr        string
		isAggregate bool
		identity    interface{}
	}{
		{expr: "collect(n)", isAggregate: true, identity: []interface{}{}},
		{expr: " COUNT (*)", isAggregate: true, identity: int64(0)},
		{expr: "sum (n.score)", isAggregate: true, identity: int64(0)},
		{expr: "avg(n.score)", isAggregate: true, identity: nil},
		{expr: "n.name", isAggregate: false, identity: nil},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			require.Equal(t, tc.isAggregate, isAggregateExpression(tc.expr))
			require.Equal(t, tc.identity, aggregateIdentity(tc.expr))
		})
	}

	require.True(t, rowHasAggregate([]returnItem{{expr: "n.name"}, {expr: "max(n.age)"}}))
	require.False(t, rowHasAggregate([]returnItem{{expr: "n.name"}, {expr: "n.age"}}))
}

func TestSmallCypherHelperBranches(t *testing.T) {
	require.Equal(t, []string{"Memory", "Evidence", "Session"}, splitCSVLabels(" Memory, Evidence ,, Session "))
	require.Empty(t, splitCSVLabels(" , , "))

	policy := minimalPolicyMap("node-1")
	require.Equal(t, "node-1", policy["targetId"])
	require.Equal(t, string(knowledgepolicy.ScopeNode), policy["targetScope"])

	writeQueries := []string{
		"CREATE (n)",
		"match (n) merge (m)",
		"MATCH (n) DELETE n",
		"MATCH (n) SET n.name = 'a'",
		"MATCH (n) REMOVE n.name",
	}
	for _, query := range writeQueries {
		require.True(t, looksLikeWriteQuery(query), query)
	}
	require.False(t, looksLikeWriteQuery("MATCH (n) RETURN n"))
}

func TestSliceAndNumericCoercionHelpers(t *testing.T) {
	input := []map[string]interface{}{{"a": 1}, {"b": "two"}}
	converted := toAnySlice(input)
	require.Len(t, converted, 2)
	require.Equal(t, map[string]interface{}{"a": 1}, converted[0])
	require.Equal(t, []interface{}{1, "two"}, toAnySlice([]interface{}{1, "two"}))
	require.Nil(t, toAnySlice("not a slice"))

	intCases := []interface{}{int(1), int8(2), int16(3), int32(4), int64(5), uint(6), uint8(7), uint16(8), uint32(9), uint64(10)}
	for i, v := range intCases {
		got, ok := coerceInt64(v)
		require.True(t, ok)
		require.Equal(t, int64(i+1), got)
	}
	_, ok := coerceInt64("1")
	require.False(t, ok)

	gotFloat, ok := coerceFloat64(float32(1.5))
	require.True(t, ok)
	require.InDelta(t, 1.5, gotFloat, 0.00001)
	gotFloat, ok = coerceFloat64(int64(2))
	require.True(t, ok)
	require.Equal(t, float64(2), gotFloat)
	_, ok = coerceFloat64("2")
	require.False(t, ok)
}

func TestQueryPatternIdentifierHelpers(t *testing.T) {
	require.Equal(t, []string{"a", "b_2", "c"}, extractNodeVariables("MATCH (a:Person)-[:KNOWS]->(b_2 {id: 1}), (c)"))
	require.Equal(t, []string{"_anon"}, extractNodeVariables("MATCH (_anon)"))
	require.Empty(t, extractNodeVariables("MATCH (:Label)-[]->()"))

	name, next, ok := scanIdentifierToken("abc_123 rest", 0)
	require.True(t, ok)
	require.Equal(t, "abc_123", name)
	require.Equal(t, len("abc_123"), next)

	_, _, ok = scanIdentifierToken("1abc", 0)
	require.False(t, ok)
	_, _, ok = scanIdentifierToken("abc", -1)
	require.False(t, ok)
}

func TestRedactSilentErrorListenerSyntaxError(t *testing.T) {
	listener := &redactSilentErrorListener{}
	require.False(t, listener.hadError)
	listener.SyntaxError(nil, nil, 0, 0, "boom", nil)
	require.True(t, listener.hadError)
}

func TestNormalizeInterfaceMap(t *testing.T) {
	got := normalizeInterfaceMap(map[interface{}]interface{}{1: "one", "two": 2, true: "yes"})
	want := map[string]interface{}{"1": "one", "two": 2, "true": "yes"}
	require.True(t, reflect.DeepEqual(want, got), "got %#v", got)
}

func TestCypherCoverage_ExecutorAccessorsAndFulltextExtraction(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	exec.SetLogger(nil)
	require.NotNil(t, exec.Logger())

	exec.SetSlowQueryThreshold(25 * time.Millisecond)
	require.Equal(t, 25*time.Millisecond, exec.SlowQueryThreshold())

	exec.SetCypherMetrics(nil, "tenant-a")
	require.Nil(t, exec.CypherMetrics())
	require.Equal(t, "tenant-a", exec.Database())

	exec.SetCacheMetrics(nil)
	exec.cache = nil
	exec.SetCacheMetrics(nil)

	require.Equal(t, "graph database", exec.extractFulltextQuery("CALL db.index.fulltext.queryNodes('nodes', 'graph database') YIELD node RETURN node"))
	require.Equal(t, "edge search", exec.extractFulltextQuery("CALL db.index.fulltext.queryRelationships(\"rels\", \"edge search\")"))
	require.Empty(t, exec.extractFulltextQuery("CALL db.labels()"))
}

func TestCypherCoverage_ShortestPathListTransforms(t *testing.T) {
	node := &storage.Node{ID: "nornic:n1", Labels: []string{"Person", "Admin"}, Properties: map[string]interface{}{"name": "Ada"}}
	edge := &storage.Edge{ID: "e1", Type: "KNOWS", StartNode: "nornic:n1", EndNode: "nornic:n2", Properties: map[string]interface{}{"since": int64(2024)}}
	path := PathResult{Nodes: []*storage.Node{node}, Relationships: []*storage.Edge{edge}}
	exec := &StorageExecutor{}

	nodeNames, ok := exec.pathListComprehension(path, "[n IN nodes(p) | n.name]", "p")
	require.True(t, ok)
	require.Equal(t, []interface{}{"Ada"}, nodeNames)

	nodeIDs, ok := exec.pathListComprehension(path, "[n IN nodes(p) | elementId(n)]", "p")
	require.True(t, ok)
	require.Equal(t, []interface{}{"nornic:n1"}, nodeIDs)

	nodeLabels, ok := exec.pathListComprehension(path, "[n IN nodes(p) | labels(n)]", "p")
	require.True(t, ok)
	require.Equal(t, []interface{}{[]interface{}{"Person", "Admin"}}, nodeLabels)

	relTypes, ok := exec.pathListComprehension(path, "[r IN relationships(p) | type(r)]", "p")
	require.True(t, ok)
	require.Equal(t, []interface{}{"KNOWS"}, relTypes)

	relProps, ok := exec.pathListComprehension(path, "[r IN relationships(p) | r.since]", "p")
	require.True(t, ok)
	require.Equal(t, []interface{}{int64(2024)}, relProps)

	relValues, ok := exec.pathListComprehension(path, "[r IN relationships(p) | r]", "p")
	require.True(t, ok)
	require.Equal(t, []interface{}{edgeToValueShortestPath(edge)}, relValues)

	_, ok = exec.pathListComprehension(path, "n.name", "p")
	require.False(t, ok)
	_, ok = exec.pathListComprehension(path, "[n nodes(p) | n.name]", "p")
	require.False(t, ok)
	_, ok = exec.pathListComprehension(path, "[n IN nodes(other) | n.name]", "p")
	require.False(t, ok)
	require.Nil(t, applyPathListTransform("n.missing", "n", "node", node, nil))
	require.Nil(t, applyPathListTransform("r.missing", "r", "rel", nil, edge))
	require.Nil(t, applyPathListTransform("unknown(n)", "n", "node", node, nil))
}

func TestCypherCoverage_LiteralAndArgumentHelpers(t *testing.T) {
	require.Equal(t, "null", cypherLiteral(nil))
	require.Equal(t, "'O\\'Reilly'", cypherLiteral("O'Reilly"))
	require.Equal(t, "true", cypherLiteral(true))
	require.Equal(t, "false", cypherLiteral(false))
	require.Equal(t, "7", cypherLiteral(7))
	require.Equal(t, "8", cypherLiteral(int64(8)))
	require.Equal(t, "1.25", cypherLiteral(float64(1.25)))
	require.Equal(t, "2.5", cypherLiteral(float32(2.5)))
	require.Equal(t, "'{custom}'", cypherLiteral(struct{ Name string }{Name: "custom"}))

	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{
		"int":        int64(12),
		"float":      float64(3.5),
		"strings":    []string{"a", "b"},
		"interfaces": []interface{}{"c", "d"},
		"badSlice":   []interface{}{"ok", 1},
	})

	intValue, ok := resolveIntLiteralOrParam(ctx, "$int")
	require.True(t, ok)
	require.Equal(t, 12, intValue)
	intValue, ok = resolveOptionalIntLiteralOrParam(ctx, "")
	require.True(t, ok)
	require.Equal(t, -1, intValue)
	_, ok = resolveIntLiteralOrParam(ctx, "$missing")
	require.False(t, ok)
	_, ok = resolveIntLiteralOrParam(ctx, "not-int")
	require.False(t, ok)

	floatValue, ok := resolveFloatLiteralOrParam(ctx, "$float")
	require.True(t, ok)
	require.Equal(t, 3.5, floatValue)
	_, ok = resolveFloatLiteralOrParam(ctx, "$missing")
	require.False(t, ok)
	_, ok = resolveFloatLiteralOrParam(ctx, "not-float")
	require.False(t, ok)

	stringsValue, ok := resolveStringSliceLiteralOrParam(ctx, "$strings")
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, stringsValue)
	stringsValue, ok = resolveStringSliceLiteralOrParam(ctx, "$interfaces")
	require.True(t, ok)
	require.Equal(t, []string{"c", "d"}, stringsValue)
	stringsValue, ok = resolveStringSliceLiteralOrParam(ctx, "['x', \"y\"]")
	require.True(t, ok)
	require.Equal(t, []string{"x", "y"}, stringsValue)
	_, ok = resolveStringSliceLiteralOrParam(ctx, "$badSlice")
	require.False(t, ok)
	_, ok = resolveStringSliceLiteralOrParam(ctx, "not-slice")
	require.False(t, ok)
}

func TestCypherCoverage_WhereAndComparableHelpers(t *testing.T) {
	require.True(t, isComparableInterfaceValue(nil))
	require.True(t, isComparableInterfaceValue("x"))
	require.False(t, isComparableInterfaceValue([]string{"x"}))

	parsed, ok := parseWhereLiteral("'it''s\\\\ok'")
	require.True(t, ok)
	require.Equal(t, "it's\\ok", parsed)
	parsed, ok = parseWhereLiteral("false")
	require.True(t, ok)
	require.Equal(t, false, parsed)
	parsed, ok = parseWhereLiteral("3.14")
	require.True(t, ok)
	require.Equal(t, 3.14, parsed)
	_, ok = parseWhereLiteral("not-literal")
	require.False(t, ok)

	exec := &StorageExecutor{}
	require.True(t, exec.isLiteralIsNotNullExpr("'x' IS NOT NULL"))
	require.False(t, exec.isLiteralIsNotNullExpr("null IS NOT NULL"))
	require.False(t, exec.isLiteralIsNotNullExpr("n.name IS NOT NULL"))

	items, ok := normalizeWhereList([]string{"a", "b"})
	require.True(t, ok)
	require.Equal(t, []interface{}{"a", "b"}, items)
	items, ok = normalizeWhereList([]int{1, 2})
	require.True(t, ok)
	require.Equal(t, []interface{}{1, 2}, items)
	items, ok = normalizeWhereList([]int64{3, 4})
	require.True(t, ok)
	require.Equal(t, []interface{}{int64(3), int64(4)}, items)
	items, ok = normalizeWhereList([]float64{1.5, 2.5})
	require.True(t, ok)
	require.Equal(t, []interface{}{1.5, 2.5}, items)
	_, ok = normalizeWhereList("nope")
	require.False(t, ok)
}

func TestCypherCoverage_VisibleAtWrapperAndMergeHelpers(t *testing.T) {
	node := &storage.Node{ID: "nornic:n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "Ada"}}
	edge := &storage.Edge{ID: "e1", Type: "LINKS", StartNode: "nornic:n1", EndNode: "nornic:n2", Properties: map[string]interface{}{"rank": 1}}
	visibleEngine := &cypherVisibleMemoryEngine{MemoryEngine: storage.NewMemoryEngine(), nodes: []*storage.Node{node}, edges: []*storage.Edge{edge}}
	wrapper := &transactionStorageWrapper{underlying: visibleEngine}

	nodes, err := wrapper.GetNodesByLabelVisibleAt("Doc", storage.MVCCVersion{})
	require.NoError(t, err)
	require.Equal(t, []*storage.Node{node}, nodes)
	outgoing, err := wrapper.GetOutgoingEdgesVisibleAt("nornic:n1", storage.MVCCVersion{})
	require.NoError(t, err)
	require.Equal(t, []*storage.Edge{edge}, outgoing)
	incoming, err := wrapper.GetIncomingEdgesVisibleAt("nornic:n2", storage.MVCCVersion{})
	require.NoError(t, err)
	require.Equal(t, []*storage.Edge{edge}, incoming)
	edges, err := wrapper.GetEdgesByTypeVisibleAt("LINKS", storage.MVCCVersion{})
	require.NoError(t, err)
	require.Equal(t, []*storage.Edge{edge}, edges)
	edges, err = wrapper.GetEdgesBetweenVisibleAt("nornic:n1", "nornic:n2", storage.MVCCVersion{})
	require.NoError(t, err)
	require.Equal(t, []*storage.Edge{edge}, edges)

	nsNode := &storage.Node{ID: "tenant:n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "Ada"}}
	nsEdge := &storage.Edge{ID: "tenant:e1", Type: "LINKS", StartNode: "tenant:n1", EndNode: "tenant:n2", Properties: map[string]interface{}{"rank": 1}}
	otherTenantEdge := &storage.Edge{ID: "other:e2", Type: "LINKS", StartNode: "other:n1", EndNode: "other:n2", Properties: map[string]interface{}{"rank": 2}}
	nsVisibleEngine := &cypherVisibleMemoryEngine{MemoryEngine: storage.NewMemoryEngine(), nodes: []*storage.Node{nsNode}, edges: []*storage.Edge{nsEdge, otherTenantEdge}}
	nsWrapper := &transactionStorageWrapper{underlying: nsVisibleEngine, namespace: "tenant", separator: ":"}

	outgoing, err = nsWrapper.GetOutgoingEdgesVisibleAt("n1", storage.MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.Equal(t, storage.EdgeID("e1"), outgoing[0].ID)
	require.Equal(t, storage.NodeID("n1"), outgoing[0].StartNode)
	require.Equal(t, storage.NodeID("n2"), outgoing[0].EndNode)

	incoming, err = nsWrapper.GetIncomingEdgesVisibleAt("n2", storage.MVCCVersion{})
	require.NoError(t, err)
	require.Len(t, incoming, 1)
	require.Equal(t, storage.EdgeID("e1"), incoming[0].ID)
	require.Equal(t, storage.NodeID("n1"), incoming[0].StartNode)
	require.Equal(t, storage.NodeID("n2"), incoming[0].EndNode)

	plainWrapper := &transactionStorageWrapper{}
	_, err = plainWrapper.GetNodesByLabelVisibleAt("Doc", storage.MVCCVersion{})
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	_, err = plainWrapper.GetOutgoingEdgesVisibleAt("n1", storage.MVCCVersion{})
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	_, err = plainWrapper.GetIncomingEdgesVisibleAt("n1", storage.MVCCVersion{})
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	_, err = plainWrapper.GetEdgesByTypeVisibleAt("LINKS", storage.MVCCVersion{})
	require.ErrorIs(t, err, storage.ErrNotImplemented)
	_, err = plainWrapper.GetEdgesBetweenVisibleAt("n1", "n2", storage.MVCCVersion{})
	require.ErrorIs(t, err, storage.ErrNotImplemented)

	value := nodeToValue(node)
	require.Equal(t, map[string]interface{}{
		"elementId":  "nornic:n1",
		"labels":     []interface{}{"Doc"},
		"properties": map[string]interface{}{"name": "Ada"},
	}, value)
	require.Equal(t, []interface{}{"a", "b"}, labelStringSlice([]string{"a", "b"}))

	exec := &StorageExecutor{}
	before := idCounter
	require.Greater(t, exec.idCounter(), before)

	require.True(t, mergeCreateConflict(storage.ErrAlreadyExists))
	require.True(t, mergeCreateConflict(errors.New("node already exists")))
	require.False(t, mergeCreateConflict(errors.New("other failure")))

	cloned := cloneNodeForMergeMutation(&storage.Node{
		ID:              "nornic:n2",
		Labels:          []string{"Person"},
		Properties:      map[string]interface{}{"name": "Grace"},
		NamedEmbeddings: map[string][]float32{"v": {1, 2}},
		ChunkEmbeddings: [][]float32{{3, 4}},
		EmbedMeta:       map[string]any{"source": "test"},
	})
	require.Equal(t, storage.NodeID("nornic:n2"), cloned.ID)
	cloned.Labels[0] = "Changed"
	cloned.Properties["name"] = "Changed"
	cloned.NamedEmbeddings["v"][0] = 99
	cloned.ChunkEmbeddings[0][0] = 99
	require.Equal(t, "Changed", cloned.Labels[0])
	require.Nil(t, cloneNodeForMergeMutation(nil))
}
