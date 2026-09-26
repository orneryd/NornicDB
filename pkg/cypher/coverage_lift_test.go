package cypher

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCoverageLiftVectorRelationshipHelpers(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	left := &storage.Node{ID: "left", Labels: []string{"Left"}, Properties: map[string]interface{}{"name": "l"}}
	right := &storage.Node{ID: "right", Labels: []string{"Right"}, Properties: map[string]interface{}{"name": "r"}}
	_, err := store.CreateNode(left)
	require.NoError(t, err)
	_, err = store.CreateNode(right)
	require.NoError(t, err)
	edge := &storage.Edge{ID: "edge", Type: "REL", StartNode: left.ID, EndNode: right.ID, Properties: map[string]interface{}{"embedding": []float32{1, 0}, "active": true}}
	require.NoError(t, store.CreateEdge(edge))

	pattern, ok := exec.parseSimpleMatchRelationshipPattern("(l:Left)-[e:REL]->(r:Right)")
	require.True(t, ok)
	nodeCtx, edgeCtx, ok := exec.buildRelationshipContext(pattern, edge)
	require.True(t, ok)
	require.Equal(t, left.ID, nodeCtx["l"].ID)
	require.Equal(t, right.ID, nodeCtx["r"].ID)
	require.Same(t, edge, edgeCtx["e"])
	assert.True(t, evaluateExpressionBoolWithContext(exec, ctx, "e.active", nodeCtx, edgeCtx))
	assert.False(t, evaluateExpressionBoolWithContext(exec, ctx, "e.missing", nodeCtx, edgeCtx))

	reversePattern, ok := exec.parseSimpleMatchRelationshipPattern("(r:Right)<-[e:REL]-(l:Left)")
	require.True(t, ok)
	nodeCtx, edgeCtx, ok = exec.buildRelationshipContext(reversePattern, edge)
	require.True(t, ok)
	assert.Equal(t, right.ID, nodeCtx["r"].ID)
	assert.Equal(t, left.ID, nodeCtx["l"].ID)
	assert.Same(t, edge, edgeCtx["e"])

	_, _, ok = exec.buildRelationshipContext(parsedSimpleMatchRelationshipPattern{leftVar: "l", leftLabels: []string{"Missing"}, relVar: "e"}, edge)
	assert.False(t, ok)
	_, _, ok = exec.buildRelationshipContext(pattern, nil)
	assert.False(t, ok)

	assert.Equal(t, "[-1,2.5]", formatInlineFloat32Vector([]float32{-1, 2.5}))
	assert.Equal(t, "[]", formatInlineFloat32Vector(nil))
	vec, ok := exec.resolveCosineQueryVector(ctx, "[1.0, 2.0]")
	require.True(t, ok)
	assert.Equal(t, []float32{1, 2}, vec)
	_, ok = exec.resolveCosineQueryVector(ctx, "''")
	assert.False(t, ok)

	for text, want := range map[string]struct {
		rest     string
		distinct bool
	}{
		" DISTINCT n.score": {"n.score", true},
		"DIST":              {"DIST", false},
		"DISTINCT{a: 1}":    {"{a: 1}", true},
		"distinct(n.g)":     {"(n.g)", true},
		"DISTINCT\n x":      {"x", true},
		"distinctName":      {"distinctName", false},
		"DISTINCT":          {"DISTINCT", false},
	} {
		rest, distinct := cutDistinct(text)
		assert.Equal(t, want.rest, rest, text)
		assert.Equal(t, want.distinct, distinct, text)
	}
	assert.True(t, withProjectionContainsVariable([]returnItem{{expr: "e"}, {expr: "score"}}, "E"))
	assert.False(t, withProjectionContainsVariable([]returnItem{{expr: "edge"}}, "e"))
}

func TestCoverageLiftPureBranchHelpers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value interface{}
		want  int
		ok    bool
	}{
		{name: "int", value: 7, want: 7, ok: true},
		{name: "int64", value: int64(8), want: 8, ok: true},
		{name: "integral float64", value: float64(9), want: 9, ok: true},
		{name: "fractional float64", value: float64(9.5), ok: false},
		{name: "integral float32", value: float32(10), want: 10, ok: true},
		{name: "fractional float32", value: float32(10.25), ok: false},
		{name: "trimmed string", value: " 11 ", want: 11, ok: true},
		{name: "bad string", value: "1.5", ok: false},
		{name: "unsupported", value: true, ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := fulltextOptionToInt(tc.value)
			assert.Equal(t, tc.ok, ok)
			if tc.ok {
				assert.Equal(t, tc.want, got)
			}
		})
	}

	result := &ExecuteResult{Rows: [][]interface{}{{"a"}, {"b"}, {"c"}}}
	applyFulltextOptions(result, fulltextQueryOptions{skip: 1, limit: 1})
	require.Equal(t, [][]interface{}{{"b"}}, result.Rows)
	result = &ExecuteResult{Rows: [][]interface{}{{"a"}, {"b"}}}
	applyFulltextOptions(result, fulltextQueryOptions{skip: 2, limit: -1})
	assert.Empty(t, result.Rows)
	applyFulltextOptions(nil, fulltextQueryOptions{skip: 1, limit: 1})

	seen := 0
	result = &ExecuteResult{}
	assert.False(t, appendFulltextOptionedRow(result, fulltextQueryOptions{skip: 1, limit: 1}, &seen, []interface{}{"skip"}))
	assert.True(t, appendFulltextOptionedRow(result, fulltextQueryOptions{skip: 1, limit: 1}, &seen, []interface{}{"keep"}))
	assert.Equal(t, [][]interface{}{{"keep"}}, result.Rows)
	assert.False(t, appendFulltextOptionedRow(nil, fulltextQueryOptions{}, &seen, []interface{}{"ignored"}))
	assert.False(t, appendFulltextOptionedRow(result, fulltextQueryOptions{}, nil, []interface{}{"ignored"}))

	assert.True(t, containsVariableLengthPathPattern("MATCH (a)-[*]->(b) RETURN b"))
	assert.True(t, containsVariableLengthPathPattern("MATCH (a)-[*1..3]->(b) RETURN b"))
	assert.True(t, containsVariableLengthPathPattern("MATCH (a)-[*..5]->(b) RETURN b"))
	assert.False(t, containsVariableLengthPathPattern("MATCH (a)-[r:REL]->(b) RETURN b"))
	assert.False(t, containsVariableLengthPathPattern("MATCH (a)-[*x..5]->(b) RETURN b"))

	props := map[string]struct{}{}
	assert.True(t, collectNodePropertyRefsForProjection("n", "n.name + n.`display name`", props))
	assert.Contains(t, props, "name")
	assert.Contains(t, props, "display name")
	assert.True(t, collectNodePropertyRefsForProjection("n", "note.name", props))
	assert.False(t, collectNodePropertyRefsForProjection("n", "*", props))
	assert.False(t, collectNodePropertyRefsForProjection("n", "properties(n)", props))
	assert.False(t, collectNodePropertyRefsForProjection("n", "n.`unterminated", props))

	desc, ok := parseOrderByScoreDirection("score DESC", "score")
	require.True(t, ok)
	assert.True(t, desc)
	desc, ok = parseOrderByScoreDirection("score ASC", "score")
	require.True(t, ok)
	assert.False(t, desc)
	_, ok = parseOrderByScoreDirection("score SIDEWAYS", "score")
	assert.False(t, ok)
	_, ok = parseOrderByScoreDirection("other DESC", "score")
	assert.False(t, ok)

	exec := NewStorageExecutor(newTestMemoryEngine(t))
	op, target, ok := exec.parseScoreComparisonPredicate(context.Background(), "score >= 0.75", "score")
	require.True(t, ok)
	assert.Equal(t, ">=", op)
	assert.Equal(t, 0.75, target)
	op, target, ok = exec.parseScoreComparisonPredicate(context.Background(), "0.2 < score", "score")
	require.True(t, ok)
	assert.Equal(t, ">", op)
	assert.Equal(t, 0.2, target)
	_, _, ok = exec.parseScoreComparisonPredicate(context.Background(), "score > 0.1 OR score < 0.9", "score")
	assert.False(t, ok)
	_, _, ok = exec.parseScoreComparisonPredicate(context.Background(), "score = 0.5", "score")
	assert.False(t, ok)

	assert.True(t, compareScore(0.8, ">", 0.7))
	assert.True(t, compareScore(0.7, ">=", 0.7))
	assert.True(t, compareScore(0.6, "<", 0.7))
	assert.True(t, compareScore(0.7, "<=", 0.7))
	assert.False(t, compareScore(0.7, "=", 0.7))
	assert.True(t, scorePredicateRejectsLeadingResults(true, "<"))
	assert.True(t, scorePredicateRejectsLeadingResults(false, ">="))
	assert.False(t, scorePredicateRejectsLeadingResults(true, ">"))

	key, ok := relationshipBatchScalarEdgeKey("start", "end", "REL", []relationshipMergeKeyProp{{propName: "name"}}, func(prop relationshipMergeKeyProp) (interface{}, bool) {
		assert.Equal(t, "name", prop.propName)
		return "neo", true
	})
	require.True(t, ok)
	assert.Equal(t, "start\x00end\x00REL\x004:name=string:3:neo;", key)
	_, ok = relationshipBatchScalarEdgeKey("start", "end", "REL", nil, func(prop relationshipMergeKeyProp) (interface{}, bool) { return nil, true })
	assert.False(t, ok)
	_, ok = relationshipBatchScalarEdgeKey("start", "end", "REL", []relationshipMergeKeyProp{{propName: "missing"}}, func(prop relationshipMergeKeyProp) (interface{}, bool) { return nil, false })
	assert.False(t, ok)
	_, ok = relationshipBatchScalarEdgeKey("start", "end", "REL", []relationshipMergeKeyProp{{propName: "bad"}}, func(prop relationshipMergeKeyProp) (interface{}, bool) { return []int{1}, true })
	assert.False(t, ok)

	for _, value := range []interface{}{nil, true, int(1), int8(2), int16(3), int32(4), int64(5), uint(6), uint8(7), uint16(8), uint32(9), uint64(10), float32(1.25), math.NaN()} {
		var builder strings.Builder
		assert.True(t, appendRelationshipBatchScalarKeyValue(&builder, value), "value %T should serialize", value)
		assert.NotEmpty(t, builder.String())
	}
	var builder strings.Builder
	assert.False(t, appendRelationshipBatchScalarKeyValue(&builder, []string{"unsupported"}))
}

func TestCoverageLiftMergeCreateSubqueryAndOrderingHelpers(t *testing.T) {
	node := &storage.Node{
		ID:         "n1",
		Labels:     []string{"Person", "Scientist"},
		Properties: map[string]interface{}{"Name": "Ada", "age": int64(37), "nested": map[string]interface{}{"score": 10}},
		CreatedAt:  time.Unix(100, 0),
		UpdatedAt:  time.Unix(200, 0),
		NamedEmbeddings: map[string][]float32{
			"bio": {1, 2, 3},
		},
		ChunkEmbeddings: [][]float32{{4, 5}},
		EmbedMeta:       map[string]interface{}{"model": "unit"},
	}

	assert.True(t, mergeNodeHasLabels(node, []string{"Person", "Scientist"}))
	assert.False(t, mergeNodeHasLabels(node, []string{"Person", "Missing"}))
	assert.True(t, mergeNodeHasAnyLabel(node, []string{"Missing", "Scientist"}))
	assert.False(t, mergeNodeHasAnyLabel(node, []string{"Missing"}))
	assert.True(t, mergeNodeMatches(node, []string{"Person"}, map[string]interface{}{"Name": "Ada", "age": int64(37)}))
	assert.False(t, mergeNodeMatches(node, []string{"Person"}, map[string]interface{}{"Name": "Grace"}))
	assert.False(t, mergeNodeMatches(nil, []string{"Person"}, nil))
	assert.True(t, mergeNodeMatchesAnyLabel(node, []string{"Missing", "Scientist"}, map[string]interface{}{"Name": "Ada"}))
	assert.False(t, mergeNodeMatchesAnyLabel(node, []string{"Missing"}, map[string]interface{}{"Name": "Ada"}))
	assert.True(t, mergeCreateConflict(storage.ErrAlreadyExists))
	assert.True(t, mergeCreateConflict(errors.New("node already exists")))
	assert.False(t, mergeCreateConflict(errors.New("different failure")))
	assert.True(t, mergePropsContainUnresolvedParamLiteral(map[string]interface{}{"id": "$id"}))
	assert.False(t, mergePropsContainUnresolvedParamLiteral(map[string]interface{}{"id": "literal", "count": 1}))
	assert.Equal(t, mergeLookupCacheKey([]string{"A", "B"}, "id", 7), mergeLookupCacheKey([]string{"A", "B"}, "id", 7))

	clone := cloneNodeForMergeMutation(node)
	require.NotSame(t, node, clone)
	assert.Equal(t, node.ID, clone.ID)
	clone.Properties["Name"] = "Changed"
	clone.Labels[0] = "ChangedLabel"
	clone.NamedEmbeddings["bio"][0] = 99
	clone.ChunkEmbeddings[0][0] = 88
	clone.EmbedMeta["model"] = "changed"
	assert.Equal(t, "Ada", node.Properties["Name"])
	assert.Equal(t, "Person", node.Labels[0])
	assert.Equal(t, float32(1), node.NamedEmbeddings["bio"][0])
	assert.Equal(t, float32(4), node.ChunkEmbeddings[0][0])
	assert.Equal(t, "unit", node.EmbedMeta["model"])
	assert.Nil(t, cloneNodeForMergeMutation(nil))

	for _, tc := range []struct {
		content string
		varName string
		relType string
		props   string
		wantErr string
	}{
		{content: "", varName: "", relType: "", props: ""},
		{content: ":KNOWS", relType: "KNOWS"},
		{content: "r:KNOWS {since: 1843}", varName: "r", relType: "KNOWS", props: "{since: 1843}"},
		{content: "r {weight: 0.5}", varName: "r", props: "{weight: 0.5}"},
		{content: "r:KNOWS {since: 1843} trailing", wantErr: "invalid relationship properties"},
		{content: "r:KNOWS {since: 1843", wantErr: "invalid relationship properties"},
	} {
		t.Run(tc.content, func(t *testing.T) {
			varName, relType, props, err := parseCreateRelationshipContent(tc.content)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.varName, varName)
			assert.Equal(t, tc.relType, relType)
			assert.Equal(t, tc.props, props)
		})
	}

	lhs, rhs, ok := splitTopLevelEqualityCallSubquery("n.name = coalesce($name, 'Ada=Byron')")
	require.True(t, ok)
	assert.Equal(t, "n.name", lhs)
	assert.Equal(t, "coalesce($name, 'Ada=Byron')", rhs)
	lhs, rhs, ok = splitTopLevelEqualityCallSubquery("m.`odd=key` = {value: 'x=y'}")
	require.True(t, ok)
	assert.Equal(t, "m.`odd=key`", lhs)
	assert.Equal(t, "{value: 'x=y'}", rhs)
	_, _, ok = splitTopLevelEqualityCallSubquery("= missingLeft")
	assert.False(t, ok)
	_, _, ok = splitTopLevelEqualityCallSubquery("missingRight = ")
	assert.False(t, ok)
	_, _, ok = splitTopLevelEqualityCallSubquery("n.name")
	assert.False(t, ok)

	assert.Equal(t, "Ada", extractOrderByPropertyValue(node, []string{"name"}))
	assert.Equal(t, time.Unix(100, 0), extractOrderByPropertyValue(node, []string{"createdAt"}))
	node.Properties["createdAt"] = "explicit-created"
	assert.Equal(t, "explicit-created", extractOrderByPropertyValue(node, []string{"createdAt"}))
	assert.Nil(t, extractOrderByPropertyValue(&storage.Node{Properties: nil}, []string{"missing"}))
	value := map[string]interface{}{"properties": map[string]interface{}{"Score": 10}, "score": 5, "outer": map[string]interface{}{"inner": "value"}}
	assert.Equal(t, 10, extractOrderByPropertyValue(value, []string{"score"}))
	assert.Equal(t, "value", extractOrderByPropertyValue(value, []string{"outer", "inner"}))
	assert.Nil(t, extractOrderByPropertyValue(value, []string{"missing"}))
	assert.Nil(t, extractOrderByPropertyValue("not a map", []string{"x"}))
}

func TestCoverageLiftWithClauseExecutionShapes(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()

	assert.Equal(t, -1, findStandaloneWithIndex("MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n"))
	assert.Greater(t, findStandaloneWithIndex("MATCH (n) WITH n RETURN n"), 0)
	assert.True(t, prevWordEqualsIgnoreCase("name STARTS   WITH 'A'", strings.LastIndex("name STARTS   WITH 'A'", "WITH"), "STARTS"))
	assert.False(t, prevWordEqualsIgnoreCase("name MATCH WITH n", strings.LastIndex("name MATCH WITH n", "WITH"), "STARTS"))
	assert.Equal(t, -1, findKeywordNotInBrackets("[x IN xs WHERE x > 1] RETURN x", "WHERE"))
	assert.GreaterOrEqual(t, findKeywordNotInBrackets("WITH 1 AS x RETURN x", "RETURN"), 0)
	assert.True(t, isWhitespace('\n'))

	result, err := exec.executeWith(ctx, "WITH {name: 'Ada', score: 10} AS m WHERE m.name = 'Ada' RETURN m.name AS name, m.score AS score")
	require.NoError(t, err)
	assert.Equal(t, []string{"name", "score"}, result.Columns)
	assert.Equal(t, [][]interface{}{{"Ada", int64(10)}}, result.Rows)

	result, err = exec.executeWith(ctx, "WITH 1 AS n WHERE n > 2 RETURN n AS n")
	require.NoError(t, err)
	assert.Equal(t, []string{"n"}, result.Columns)
	assert.Empty(t, result.Rows)

	result, err = exec.executeWith(ctx, "WITH 1 AS n WHERE n > 2 RETURN collect(n) AS ns, count(n) AS c, avg(n) AS avg")
	require.NoError(t, err)
	assert.Equal(t, []string{"ns", "c", "avg"}, result.Columns)
	assert.Equal(t, [][]interface{}{{[]interface{}{}, int64(0), nil}}, result.Rows)

	result, err = exec.executeWith(ctx, "WITH [[1, 2], ['a', 'b']] AS matrix UNWIND matrix AS row RETURN row")
	require.NoError(t, err)
	assert.Equal(t, []string{"row"}, result.Columns)
	assert.Equal(t, 2, len(result.Rows))
	assert.Equal(t, []interface{}{int64(1), int64(2)}, result.Rows[0][0])
	assert.Equal(t, []interface{}{"a", "b"}, result.Rows[1][0])

	result, err = exec.executeWith(withParams(ctx, map[string]interface{}{"name": "Cy"}), "WITH $name AS name RETURN name")
	require.NoError(t, err)
	assert.Equal(t, [][]interface{}{{"Cy"}}, result.Rows)

	_, err = exec.executeWith(ctx, "RETURN 1")
	require.ErrorContains(t, err, "WITH clause not found")
}

func TestCoverageLiftOperatorsAndRelationshipVectorLimitZero(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	assert.Equal(t, []interface{}{int64(1), int64(2), int64(3)}, exec.add([]interface{}{int64(1)}, []interface{}{int64(2), int64(3)}))
	assert.Equal(t, []interface{}{int64(1), "tail"}, exec.add([]interface{}{int64(1)}, "tail"))
	assert.Equal(t, []interface{}{"head", int64(2)}, exec.add("head", []interface{}{int64(2)}))
	assert.Equal(t, "2025-01-06T00:00:00Z", exec.add("2025-01-01", &CypherDuration{Days: 5}))
	assert.Equal(t, "2025-01-06T00:00:00Z", exec.add(&CypherDuration{Days: 5}, "2025-01-01"))
	assert.Equal(t, int64(15), exec.multiply(int64(5), int64(3)))
	assert.Equal(t, float64(7.5), exec.multiply(2.5, int64(3)))
	assert.Nil(t, exec.multiply("bad", int64(3)))
	assert.Equal(t, int64(5), exec.divide(int64(10), int64(2)))
	assert.Equal(t, int64(2), exec.divide(int64(5), int64(2)))
	assert.Nil(t, exec.divide(int64(10), int64(0)))
	assert.Equal(t, int64(1), exec.modulo(int64(10), int64(3)))
	assert.Nil(t, exec.modulo("bad", int64(3)))
	assert.Equal(t, "2025-01-01T00:00:00Z", exec.subtract("2025-01-06", &CypherDuration{Days: 5}))
	duration := exec.subtract("2025-01-06", "2025-01-01")
	require.IsType(t, &CypherDuration{}, duration)
	assert.Equal(t, int64(5), duration.(*CypherDuration).Days)
	assert.Equal(t, int64(7), exec.subtract(int64(10), int64(3)))
	assert.Equal(t, float64(6.5), exec.subtract(10.0, 3.5))
	assert.Nil(t, exec.subtract("bad", int64(3)))
	assert.True(t, exec.hasStringPredicate("n.name CONTAINS 'Ada'", " CONTAINS "))
	assert.False(t, exec.hasStringPredicate("'CONTAINS' = n.name", " CONTAINS "))

	assert.Nil(t, exec.evaluateSetExpression("null"))
	assert.Equal(t, "Ada", exec.evaluateSetExpression("\"Ada\""))
	assert.Equal(t, true, exec.evaluateSetExpression("true"))
	assert.Equal(t, []interface{}{int64(1), "two", false}, exec.evaluateSetExpression("[1, 'two', false]"))
	assert.IsType(t, CypherDateTime{}, exec.evaluateSetExpression("datetime()"))
	assert.NotEmpty(t, exec.evaluateSetExpression("randomUUID()"))
	assert.Equal(t, "42", exec.evaluateSetExpression("toString(42)"))
	assert.Equal(t, "ell", exec.evaluateSetExpression("substring('hello', 1, 3)"))
	assert.Equal(t, "prefix-42", exec.evaluateSetExpression("'prefix-' + toString(42)"))
	assert.Equal(t, "rawExpression", exec.evaluateSetExpression("rawExpression"))

	assert.True(t, relationshipBatchRowHasRequiredFields(map[string]interface{}{"source": "a", "target": "b", "score": 0.9, "embedding": []float32{1}, "ret": "ok"}, unwindRelationshipMergeBatchPlan{
		matches:       []matchClauseSpec{{rowField: "source"}, {rowField: "target"}},
		merge:         relationshipMergeSpec{rowFieldRefs: map[string]string{"score": "score"}},
		vectorSetters: []relationshipVectorSetterSpec{{rowField: "embedding"}},
		returns:       []rowFieldReturnSpec{{rowField: "ret"}},
	}))
	assert.False(t, relationshipBatchRowHasRequiredFields(map[string]interface{}{"source": "a"}, unwindRelationshipMergeBatchPlan{
		matches: []matchClauseSpec{{rowField: "source"}, {rowField: "target"}},
	}))
	assert.False(t, nodeBatchMatchesValue(nil, nodeBatchMatchKey{label: "Person", prop: "id"}, "1"))
	assert.False(t, nodeBatchMatchesValue(&storage.Node{Labels: []string{"Person"}, Properties: nil}, nodeBatchMatchKey{label: "Person", prop: "id"}, "1"))
	assert.True(t, nodeBatchMatchesValue(&storage.Node{Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "1"}}, nodeBatchMatchKey{label: "Person", prop: "id"}, "1"))
	assert.False(t, nodeBatchMatchesValue(&storage.Node{Labels: []string{"Other"}, Properties: map[string]interface{}{"id": "1"}}, nodeBatchMatchKey{label: "Person", prop: "id"}, "1"))

	assert.Equal(t, map[string]interface{}{"a": int64(1), "b": int64(2)}, mergeMaps(map[string]interface{}{"a": int64(0)}, map[string]interface{}{"a": int64(1), "b": int64(2)}))
	assert.Equal(t, map[string]interface{}{"a": int64(1), "b": "two"}, fromPairs([]interface{}{[]interface{}{"a", int64(1)}, []interface{}{"b", "two"}}))
	assert.Equal(t, map[string]interface{}{"a": int64(1), "b": int64(2)}, fromLists([]interface{}{"a", "b"}, []interface{}{int64(1), int64(2)}))
	assert.Empty(t, fromLists([]interface{}{"a"}, "not a list"))
	apocCtx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{
		"left":  map[string]interface{}{"a": int64(0)},
		"right": map[string]interface{}{"a": int64(1), "b": int64(2)},
		"pairs": []interface{}{[]interface{}{"a", int64(1)}, []interface{}{"b", "two"}},
		"keys":  []interface{}{"a", "b"},
		"vals":  []interface{}{int64(1), int64(2)},
	})
	assert.Equal(t, map[string]interface{}{"a": int64(1), "b": int64(2)}, exec.evaluateExpressionWithContext(apocCtx, "apoc.map.merge($left, $right)", nil, nil))
	assert.Empty(t, exec.evaluateExpressionWithContext(apocCtx, "apoc.map.merge($left)", nil, nil))
	assert.Equal(t, map[string]interface{}{"a": int64(1), "b": "two"}, exec.evaluateExpressionWithContext(apocCtx, "apoc.map.fromPairs($pairs)", nil, nil))
	assert.Equal(t, map[string]interface{}{"a": int64(1), "b": int64(2)}, exec.evaluateExpressionWithContext(apocCtx, "apoc.map.fromLists($keys, $vals)", nil, nil))
	assert.Nil(t, exec.evaluateExpressionWithContext(apocCtx, "apoc.map.fromLists($keys)", nil, nil))

	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "rel_vec_zero")
	exec = NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Entity {uuid:'a'})-[:RELATES_TO {uuid:'r1', fact_embedding:[1.0,0.0,0.0]}]->(:Entity {uuid:'b'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_emb_zero_idx IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity) RETURN r.uuid AS rid, vector.similarity.cosine(r.fact_embedding, [1.0,0.0,0.0]) AS score ORDER BY score DESC LIMIT 0", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"rid", "score"}, result.Columns)
	assert.Empty(t, result.Rows)
	assert.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
}

func TestCoverageLiftUnwindAndRelationshipBatchEdgeBranches(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "unwind_edge_branches")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "bag", Labels: []string{"Bag"}, Properties: map[string]interface{}{"items": []interface{}{"keep", "drop"}}})
	require.NoError(t, err)
	result, err := exec.executeMatchUnwind(ctx, "MATCH (n:Bag) UNWIND n.items AS item WHERE item = 'keep' RETURN item")
	require.NoError(t, err)
	assert.Equal(t, []string{"item"}, result.Columns)
	assert.Equal(t, [][]interface{}{{"keep"}}, result.Rows)

	_, err = store.CreateNode(&storage.Node{ID: "svc", Labels: []string{"Service"}, Properties: map[string]interface{}{"key": "svc-a"}})
	require.NoError(t, err)
	batchRows := []interface{}{map[string]interface{}{"source_key": "svc-a", "uuid": "edge-1"}}
	restQuery := `MATCH (source:Service {key: row.source_key})
MERGE (source)-[rel:PUBLISHES {uuid: row.uuid}]->(target)
RETURN row.uuid AS uuid`
	_, handled, err := exec.executeUnwindRelationshipMergeBatch(ctx, "row", batchRows, restQuery)
	assert.False(t, handled)
	require.NoError(t, err)
}

func TestCoverageLiftTypedAssignmentAndKeywordScanHelpers(t *testing.T) {
	t.Run("assign value conversions", func(t *testing.T) {
		var parsed time.Time
		require.NoError(t, assignValue(reflect.ValueOf(&parsed).Elem(), "2025-01-02T03:04:05Z"))
		assert.Equal(t, time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), parsed)
		require.NoError(t, assignValue(reflect.ValueOf(&parsed).Elem(), nil))
		assert.Equal(t, time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), parsed)

		var directTime time.Time
		expectedTime := time.Date(2025, 6, 30, 12, 0, 0, 0, time.UTC)
		require.NoError(t, assignValue(reflect.ValueOf(&directTime).Elem(), expectedTime))
		assert.Equal(t, expectedTime, directTime)

		var spacedTime time.Time
		require.NoError(t, assignValue(reflect.ValueOf(&spacedTime).Elem(), "2025-01-02 03:04:05"))
		assert.Equal(t, time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC), spacedTime)

		var unix time.Time
		require.NoError(t, assignValue(reflect.ValueOf(&unix).Elem(), int64(10)))
		assert.Equal(t, time.Unix(10, 0), unix)

		var direct string
		require.NoError(t, assignValue(reflect.ValueOf(&direct).Elem(), "Ada"))
		assert.Equal(t, "Ada", direct)

		var text string
		require.NoError(t, assignValue(reflect.ValueOf(&text).Elem(), int64(42)))
		assert.Equal(t, "42", text)

		var flag bool
		require.NoError(t, assignValue(reflect.ValueOf(&flag).Elem(), int64(1)))
		assert.True(t, flag)
		require.NoError(t, assignValue(reflect.ValueOf(&flag).Elem(), 0))
		assert.False(t, flag)

		var numbers []int64
		require.NoError(t, assignValue(reflect.ValueOf(&numbers).Elem(), []interface{}{int64(1), int(2)}))
		assert.Equal(t, []int64{1, 2}, numbers)

		assert.ErrorContains(t, assignValue(reflect.ValueOf(&parsed).Elem(), "not-a-time"), "cannot parse time")
		assert.ErrorContains(t, assignValue(reflect.ValueOf(&numbers).Elem(), "not-a-slice"), "cannot assign")
	})

	t.Run("keyword scanner skips comments and quoted regions", func(t *testing.T) {
		opts := defaultKeywordScanOpts()
		opts.SkipBraces = true
		query := "MATCH (n) // RETURN ignored\nWITH n /* RETURN ignored */ RETURN n"
		assert.Equal(t, strings.LastIndex(query, "RETURN"), keywordIndexFrom(query, "RETURN", 0, opts))
		assert.Equal(t, strings.Index(query, "WITH"), keywordIndexFrom(query, "WITH", -5, opts))
		assert.Equal(t, -1, keywordIndexFrom(query, "RETURN", len(query), opts))

		quoted := "MATCH (n {`RETURN``x`: 'RETURN', name: \"RETURN\"}) RETURN n"
		assert.Equal(t, strings.LastIndex(quoted, "RETURN"), keywordIndexFrom(quoted, "RETURN", 0, opts))
		assert.Equal(t, -1, keywordIndexFrom("RETURN", "   ", 0, opts))
	})
}

func TestCoverageLiftApplySetToNodeMapAndDynamicLabels(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	node := &storage.Node{Labels: []string{"Base"}, Properties: map[string]interface{}{"old": "value"}}
	ctx := context.WithValue(context.Background(), paramsKey, map[string]interface{}{
		"props": map[string]interface{}{"name": "Ada", "age": int64(37)},
		"payload": map[string]interface{}{
			"replacement": map[string]interface{}{"name": "Grace", "active": true},
		},
		"labels": []interface{}{"Engineer", "Base", "MATCH", "bad-label", "Analyst"},
	})

	exec.applyCountedNodeSet(ctx, node, "n", "n = $props", nil, nil, nil)
	assert.Equal(t, map[string]interface{}{"name": "Ada", "age": int64(37)}, node.Properties)
	node.Properties["name"] = "mutated"
	assert.Equal(t, "Ada", getParamsFromContext(ctx)["props"].(map[string]interface{})["name"])

	exec.applyCountedNodeSet(ctx, node, "n", "n = $payload.replacement", nil, nil, nil)
	assert.Equal(t, map[string]interface{}{"name": "Grace", "active": true}, node.Properties)

	exec.applyCountedNodeSet(ctx, node, "n", "n:$($labels), n:Reviewer, n:Reviewer, ignored = true, n", nil, nil, nil)
	assert.ElementsMatch(t, []string{"Base", "Engineer", "MATCH", "Analyst", "Reviewer"}, node.Labels)
	assert.NotContains(t, node.Labels, "bad-label")

	exec.applyCountedNodeSet(ctx, node, "n", "n += $props", nil, nil, nil)
	assert.Equal(t, "Ada", node.Properties["name"])
	assert.Equal(t, int64(37), node.Properties["age"])
}

func TestCoverageLiftShortestPathAndShellParserHelpers(t *testing.T) {
	funcName, inner, idx, ok := extractShortestPathCall("MATCH p = shortestPath( (a)-[:KNOWS*]->(b) ) RETURN p")
	require.True(t, ok)
	assert.Equal(t, "shortestPath", funcName)
	assert.Equal(t, "(a)-[:KNOWS*]->(b)", inner)
	assert.Greater(t, idx, 0)
	assert.Equal(t, "p", extractShortestPathPathVariable("MATCH p = shortestPath((a)-->(b)) RETURN p", idx))

	funcName, inner, idx, ok = extractShortestPathCall("MATCH p = allShortestPaths((a)-[*]->(b)) RETURN p")
	require.True(t, ok)
	assert.Equal(t, "allShortestPaths", funcName)
	assert.Equal(t, "(a)-[*]->(b)", inner)
	assert.Equal(t, "p", extractShortestPathPathVariable("MATCH p = allShortestPaths((a)-[*]->(b)) RETURN p", idx))

	funcName, inner, idx, ok = extractShortestPathCall("MATCH (n) RETURN notshortestPath(n)")
	assert.False(t, ok)
	assert.Empty(t, funcName)
	assert.Empty(t, inner)
	assert.Equal(t, -1, idx)

	funcName, inner, idx, ok = extractShortestPathCall("MATCH p = shortestPath((a)-->(b) RETURN p")
	assert.False(t, ok)
	assert.Equal(t, "shortestPath", funcName)
	assert.Empty(t, inner)
	assert.Greater(t, idx, 0)
	assert.Empty(t, extractShortestPathPathVariable("MATCH (a) RETURN shortestPath((a)-->(b))", idx))
	previousQuery := "MATCH (a) RETURN 1 MATCH p = shortestPath((a)-->(b)) RETURN p"
	_, _, previousIdx, ok := extractShortestPathCall(previousQuery)
	require.True(t, ok)
	assert.Equal(t, "(a) RETURN 1", extractPreviousMatchClause(previousQuery, previousIdx))

	command, consumed, ok := consumeShellMapCommand(":params {name: 'Ada // not comment', note: \"brace } inside\", nested: {x: 1 /* ok */}};\nMATCH (n) RETURN n")
	require.True(t, ok)
	assert.Equal(t, ":params {name: 'Ada // not comment', note: \"brace } inside\", nested: {x: 1 /* ok */}}", command)
	assert.Equal(t, strings.Index(command+";\nMATCH (n) RETURN n", "MATCH"), consumed)

	command, rest, ok := nextShellCommand(" :params {name: 'Ada'}\nMATCH (n) RETURN n")
	require.True(t, ok)
	assert.Equal(t, ":params {name: 'Ada'}", command)
	assert.Equal(t, "MATCH (n) RETURN n", strings.TrimSpace(rest))
	_, _, ok = consumeShellMapCommand(":params {name: 'Ada'")
	assert.False(t, ok)
}

func TestCoverageLiftCallTailParsersAndMaps(t *testing.T) {
	variable, relType, key, expr, ok := parseCallTailRelationshipToken("r:REL {score: vector.similarity.cosine(r.embedding, [1,2])}")
	require.True(t, ok)
	assert.Equal(t, "r", variable)
	assert.Equal(t, "REL", relType)
	assert.Equal(t, "score", key)
	assert.Equal(t, "vector.similarity.cosine(r.embedding, [1,2])", expr)

	variable, relType, key, expr, ok = parseCallTailRelationshipToken("r")
	require.True(t, ok)
	assert.Equal(t, "r", variable)
	assert.Empty(t, relType)
	assert.Empty(t, key)
	assert.Empty(t, expr)

	_, _, _, _, ok = parseCallTailRelationshipToken(":REL")
	assert.False(t, ok)
	_, _, _, _, ok = parseCallTailRelationshipToken("r: {x: 1}")
	assert.False(t, ok)
	_, _, _, _, ok = parseCallTailRelationshipToken("r:REL trailing")
	assert.False(t, ok)

	variable, label, ok := parseCallTailIdentifierAndOptionalType("n:Person")
	require.True(t, ok)
	assert.Equal(t, "n", variable)
	assert.Equal(t, "Person", label)
	_, _, ok = parseCallTailIdentifierAndOptionalType("n:")
	assert.False(t, ok)
	_, _, ok = parseCallTailIdentifierAndOptionalType("n Person")
	assert.False(t, ok)

	inside, rest, ok := parseCallTailDelimited("{a: [1, {b: 'c,d'}]}, tail", '{', '}')
	require.True(t, ok)
	assert.Equal(t, "a: [1, {b: 'c,d'}]", inside)
	assert.Equal(t, ", tail", rest)
	_, _, ok = parseCallTailDelimited("{a: 1", '{', '}')
	assert.False(t, ok)

	key, expr, rest, ok = parseCallTailSinglePropertyMap("{score: coalesce(r.score, 0)} trailing")
	require.True(t, ok)
	assert.Equal(t, "score", key)
	assert.Equal(t, "coalesce(r.score, 0)", expr)
	assert.Equal(t, " trailing", rest)
	_, _, _, ok = parseCallTailSinglePropertyMap("{a: 1, b: 2}")
	assert.False(t, ok)

	assert.Equal(t, 7, findTopLevelByte("a(b, c), d", ','))
	assert.Equal(t, -1, findTopLevelByte("a('b,c')", ','))
}

func TestCoverageLiftCallTailProjectionAndPredicateMatrix(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := withParams(context.Background(), map[string]interface{}{"wantedAge": 41, "names": []interface{}{"Ada", "Cy"}})

	// The plan runs the tail's WITH (and WHERE) and RETURN with the pipeline's
	// appliers.
	plan, ok := exec.parseCallTailProjectionPlan(ctx, "WITH name, age WHERE (name STARTS WITH 'A' OR age = $wantedAge) AND name IN $names RETURN name AS n, age AS years ORDER BY years DESC SKIP 0 LIMIT 10")
	require.True(t, ok)
	seed := &ExecuteResult{
		Columns: []string{"name", "age"},
		Rows:    [][]interface{}{{"Ada", int64(37)}, {"Bob", int64(41)}, {"Cy", int64(41)}},
	}
	rows := make([]pipelineRow, 0, len(seed.Rows))
	for _, row := range seed.Rows {
		rows = append(rows, callTailRow(ctx, seed, row))
	}
	result, ok := exec.executeCallTailProjectionPlan(ctx, plan, rows, []string{"name", "age"})
	require.True(t, ok)
	assert.Equal(t, []string{"name", "age"}, result.Columns)
	assert.Equal(t, [][]interface{}{{"Cy", int64(41)}, {"Ada", int64(37)}}, result.Rows)

	// SKIP / LIMIT that aren't a literal or a bound parameter, aggregation
	// and DISTINCT are left to the pipeline, which evaluates or rejects them.
	for _, tail := range []string{
		"WITH name RETURN name SKIP $missing",
		"WITH name RETURN name LIMIT nope",
		"WITH name RETURN name LIMIT 0 + 1",
		"WITH name RETURN count(name) AS c",
		"WITH name RETURN DISTINCT name",
		"WITH DISTINCT name RETURN name",
		"RETURN name",
		"WITH * RETURN name",
		"WITH name RETURN *",
		"WITH name MATCH (n) RETURN name",
	} {
		_, ok = exec.parseCallTailProjectionPlan(ctx, tail)
		assert.False(t, ok, tail)
	}

	inside, rest, ok := parseCallTailDelimited("{score: vector.similarity.cosine(r.embedding, [1,2])} tail", '{', '}')
	require.True(t, ok)
	assert.Equal(t, "score: vector.similarity.cosine(r.embedding, [1,2])", inside)
	assert.Equal(t, " tail", rest)
	_, _, ok = parseCallTailDelimited("{unterminated", '{', '}')
	assert.False(t, ok)
	key, expr, rest := "", "", ""
	key, expr, rest, ok = parseCallTailSinglePropertyMap("{score: vector.similarity.cosine(r.embedding, [1,2])} trailing")
	require.True(t, ok)
	assert.Equal(t, "score", key)
	assert.Equal(t, "vector.similarity.cosine(r.embedding, [1,2])", expr)
	assert.Equal(t, " trailing", rest)
	_, _, _, ok = parseCallTailSinglePropertyMap("{a: 1, b: 2}")
	assert.False(t, ok)
}

func TestCoverageLiftBindingWherePredicates(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := context.Background()
	alice := &storage.Node{ID: "1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice", "age": int64(30), "tags": []interface{}{"a", "b"}, "flag": true}}
	bob := &storage.Node{ID: "2", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(20), "tags": []interface{}{"c"}, "flag": false}}
	row := binding{"a": alice, "b": bob}

	pred, ok := exec.compileBindingInPredicate("a.name IN ['Alice', 'Carol']", " IN ", false)
	require.True(t, ok)
	assert.True(t, pred.predicate()(row, nil))
	pred, ok = exec.compileBindingInPredicate("a.name NOT IN ['Alice']", " NOT IN ", true)
	require.True(t, ok)
	assert.False(t, pred.predicate()(row, nil))
	pred, ok = exec.compileBindingInPredicate("a.name IN $names", " IN ", false)
	require.True(t, ok)
	assert.True(t, pred.predicate()(row, map[string]interface{}{"names": []interface{}{"Alice"}}))
	assert.False(t, pred.predicate()(row, map[string]interface{}{"names": []interface{}{"Bob"}}))
	assert.False(t, pred.predicate()(row, nil))
	pred, ok = exec.compileBindingInPredicate("a.name IN b.tags", " IN ", false)
	require.True(t, ok)
	assert.False(t, pred.predicate()(row, nil))
	_, ok = exec.compileBindingInPredicate(" IN ['x']", " IN ", false)
	assert.False(t, ok)

	assert.True(t, exec.evaluateBindingWhere(ctx, row, "a.name STARTS WITH 'Ali' AND a.age >= 30", nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, row, "a.name CONTAINS 'lic'", nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, row, "a.name ENDS WITH 'ice'", nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, row, "a <> b", nil))
	assert.False(t, exec.evaluateBindingWhere(ctx, row, "a = b", nil))
	assert.True(t, exec.evaluateBindingWhere(ctx, row, "NOT b.flag", nil))
	assert.False(t, exec.evaluateBindingWhere(ctx, row, "missing.prop = 1", nil))

	resolver, ok := exec.compileBindingValueResolver("a.age")
	require.True(t, ok)
	value, ok := resolver(row, nil)
	require.True(t, ok)
	assert.Equal(t, int64(30), value)
	resolver, ok = exec.compileBindingValueResolver("$age")
	require.True(t, ok)
	value, ok = resolver(row, map[string]interface{}{"age": int64(30)})
	require.True(t, ok)
	assert.Equal(t, int64(30), value)
	_, ok = exec.compileBindingValueResolver("bad expr")
	assert.False(t, ok)

	assert.True(t, exec.compareBindingValuesEqual(int64(1), float64(1)))
	assert.True(t, exec.compareBindingValuesEqual(nil, nil))
	assert.False(t, exec.compareBindingValuesEqual(nil, 1))
}

func TestCoverageLiftSchemaDDLCompatibilityHelpers(t *testing.T) {
	dimensions, similarity := parseVectorOptions("OPTIONS {indexConfig: {`vector.dimensions`: 1536, `vector.similarity_function`: 'euclidean'}}", 384, "cosine")
	assert.Equal(t, 1536, dimensions)
	assert.Equal(t, "euclidean", similarity)
	dimensions, similarity = parseVectorOptions("OPTIONS {indexConfig: {vectorXdimensions: 99, vector.similarity_function_suffix: 'bad'}}", 384, "cosine")
	assert.Equal(t, 384, dimensions)
	assert.Equal(t, "cosine", similarity)

	value, ok := extractVectorOptionValue("OPTIONS {indexConfig: {'vector.dimensions': 768}}", "vector.dimensions")
	require.True(t, ok)
	assert.Equal(t, "768", value)
	_, ok = extractVectorOptionValue("OPTIONS {indexConfig: {vectorXdimensions: 768}}", "vector.dimensions")
	assert.False(t, ok)
	_, ok = extractVectorOptionValue("", "vector.dimensions")
	assert.False(t, ok)
	_, ok = extractVectorOptionValue("OPTIONS {indexConfig: {'vector.dimensions: 768}}", "vector.unknown")
	assert.False(t, ok)

	label, relTypes, isRel, err := parseCreateFulltextForPattern("n:Article")
	require.NoError(t, err)
	assert.Equal(t, "Article", label)
	assert.False(t, isRel)
	assert.Empty(t, relTypes)
	label, relTypes, isRel, err = parseCreateFulltextForPattern("()-[r:LIKES|RATES]-()")
	require.NoError(t, err)
	assert.Empty(t, label)
	assert.True(t, isRel)
	assert.Equal(t, []string{"LIKES", "RATES"}, relTypes)
	_, _, _, err = parseCreateFulltextForPattern("()-[r:LIKES|]-()")
	require.Error(t, err)

	props, tail, err := extractFulltextPropertiesSegment("EACH [n.title, n.body] OPTIONS {indexConfig: {}}")
	require.NoError(t, err)
	assert.Equal(t, "n.title, n.body", props)
	assert.Equal(t, "OPTIONS {indexConfig: {}}", tail)
	props, tail, err = extractFulltextPropertiesSegment("(n.title)")
	require.NoError(t, err)
	assert.Equal(t, "n.title", props)
	assert.Empty(t, tail)
	_, _, err = extractFulltextPropertiesSegment("EACH [n.title")
	require.Error(t, err)
	_, _, err = extractFulltextPropertiesSegment("")
	require.Error(t, err)

	sourceLabel, relType, targetLabel, ok := parsePolicyForClause("(:User)-[:CAN_ACCESS]->(:Document)")
	require.True(t, ok)
	assert.Equal(t, "User", sourceLabel)
	assert.Equal(t, "CAN_ACCESS", relType)
	assert.Equal(t, "Document", targetLabel)
	_, _, _, ok = parsePolicyForClause("(u:User)-[:CAN_ACCESS]->(:Document)")
	assert.False(t, ok)
	_, _, _, ok = parsePolicyForClause("(:User)-[r]->(:Document)")
	assert.False(t, ok)

	mode, ok := parsePolicyModeRequireExpr("ALLOWED")
	require.True(t, ok)
	assert.Equal(t, "ALLOWED", mode)
	mode, ok = parsePolicyModeRequireExpr("DISALLOWED")
	require.True(t, ok)
	assert.Equal(t, "DISALLOWED", mode)
	_, ok = parsePolicyModeRequireExpr("ALLOWED EXTRA")
	assert.False(t, ok)
}

func TestCoverageLiftSchemaDDLParserCompatibilityMatrix(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	name, err := parseOptionalDDLName("`complex index` IF NOT EXISTS")
	require.NoError(t, err)
	assert.Equal(t, "complex index", name)
	name, err = parseOptionalDDLName("")
	require.NoError(t, err)
	assert.Empty(t, name)
	_, err = parseOptionalDDLName("bad name")
	require.Error(t, err)
	_, err = parseOptionalDDLName("idx IF NOT EXISTS trailing")
	require.Error(t, err)
	_, err = parseOptionalDDLName("`unterminated")
	require.Error(t, err)

	propsSegment, tail, err := extractIndexPropertiesSegment("(n.name, n.`full name`) OPTIONS {indexProvider: 'range-1.0'}")
	require.NoError(t, err)
	assert.Equal(t, "n.name, n.`full name`", propsSegment)
	assert.Equal(t, "OPTIONS {indexProvider: 'range-1.0'}", tail)
	propsSegment, tail, err = extractIndexPropertiesSegment("n.name OPTIONS {indexProvider: 'range-1.0'}")
	require.NoError(t, err)
	assert.Equal(t, "n.name", propsSegment)
	assert.Equal(t, "OPTIONS {indexProvider: 'range-1.0'}", tail)
	_, _, err = extractIndexPropertiesSegment("")
	require.Error(t, err)
	_, _, err = extractIndexPropertiesSegment("(n.name")
	require.Error(t, err)

	label, relType, isRelationship, err := parseCreateIndexForPattern("(n:`Article`)")
	require.NoError(t, err)
	assert.Equal(t, "Article", label)
	assert.Empty(t, relType)
	assert.False(t, isRelationship)
	label, relType, isRelationship, err = parseCreateIndexForPattern("()-[r:`LIKES|RATES`]-()")
	require.NoError(t, err)
	assert.Empty(t, label)
	assert.Equal(t, "LIKES|RATES", relType)
	assert.True(t, isRelationship)
	_, _, _, err = parseCreateIndexForPattern("(n)")
	require.Error(t, err)
	_, _, _, err = parseCreateIndexForPattern("()-[r]-()")
	require.Error(t, err)

	parsedIndex, err := exec.parseCreateIndexDDL("CREATE INDEX rel_since_idx IF NOT EXISTS FOR ()-[r:KNOWS]-() ON (r.since, r.weight) OPTIONS {indexProvider: 'range-1.0'}", "CREATE INDEX")
	require.NoError(t, err)
	assert.Equal(t, "rel_since_idx", parsedIndex.indexName)
	assert.True(t, parsedIndex.isRelationship)
	assert.Equal(t, "KNOWS", parsedIndex.relationshipType)
	assert.Equal(t, []string{"since", "weight"}, parsedIndex.properties)
	assert.Equal(t, "OPTIONS {indexProvider: 'range-1.0'}", parsedIndex.optionsClause)
	parsedIndex, err = exec.parseCreateIndexLegacyDDL("CREATE INDEX legacy_person_idx ON :Person(name, `full name`) OPTIONS {indexProvider: 'native-btree-1.0'}")
	require.NoError(t, err)
	assert.Equal(t, "legacy_person_idx", parsedIndex.indexName)
	assert.Equal(t, "Person", parsedIndex.label)
	assert.Equal(t, []string{"name", "full name"}, parsedIndex.properties)
	_, err = exec.parseCreateIndexDDL("CREATE INDEX broken FOR (n:Person) ON ()", "CREATE INDEX")
	require.Error(t, err)
	_, err = exec.parseCreateIndexLegacyDDL("CREATE INDEX legacy ON Person(name)")
	require.Error(t, err)

	domainValues, err := parseDomainValueList("'A', \"B\", 7, 8.5, true, false, bare")
	require.NoError(t, err)
	assert.Equal(t, []interface{}{"A", "B", int64(7), 8.5, true, false, "bare"}, domainValues)
	domainValues, err = parseDomainValueList("")
	require.NoError(t, err)
	assert.Nil(t, domainValues)
	_, err = parseDomainValueList("'unterminated")
	require.Error(t, err)

	relTypes, err := parseFulltextRelationshipTypes("`LIKES|RATES`|KNOWS")
	require.NoError(t, err)
	assert.Equal(t, []string{"LIKES|RATES", "KNOWS"}, relTypes)
	_, err = parseFulltextRelationshipTypes("")
	require.Error(t, err)
	_, err = parseFulltextRelationshipTypes("`BROKEN")
	require.Error(t, err)

	parsedFulltext, err := exec.parseCreateFulltextIndexDDL("CREATE FULLTEXT INDEX rel_ft FOR ()-[r:LIKES|RATES]-() ON EACH [r.summary, r.body] OPTIONS {indexConfig: {}}")
	require.NoError(t, err)
	assert.Equal(t, "rel_ft", parsedFulltext.indexName)
	assert.True(t, parsedFulltext.isRelationship)
	assert.Equal(t, []string{"LIKES", "RATES"}, parsedFulltext.relationshipTypes)
	assert.Equal(t, []string{"summary", "body"}, parsedFulltext.properties)
	_, err = exec.parseCreateFulltextIndexDDL("CREATE FULLTEXT INDEX FOR (n:Doc) ON EACH [n.body]")
	require.Error(t, err)

	expr, options := splitDDLOptionsTail("n.status IN ['A'] OPTIONS {provider: 'native'}")
	assert.Equal(t, "n.status IN ['A']", expr)
	assert.Equal(t, "OPTIONS {provider: 'native'}", options)
	varName, prop, ok := parseConstraintQualifiedProperty("n.`full name`")
	require.True(t, ok)
	assert.Equal(t, "n", varName)
	assert.Equal(t, "full name", prop)
	_, _, ok = parseConstraintQualifiedProperty("n")
	assert.False(t, ok)

	kind, prop, ok := parseConstraintPredicate("EXISTS(n.email)")
	require.True(t, ok)
	assert.Equal(t, "exists", kind)
	assert.Equal(t, "email", prop)
	kind, prop, ok = parseConstraintPredicate("n.email IS UNIQUE")
	require.True(t, ok)
	assert.Equal(t, "unique", kind)
	assert.Equal(t, "email", prop)
	kind, prop, ok = parseConstraintPredicate("n.email IS NOT NULL")
	require.True(t, ok)
	assert.Equal(t, "exists", kind)
	assert.Equal(t, "email", prop)
	_, _, ok = parseConstraintPredicate("n.email IS UNIQUE EXTRA")
	assert.False(t, ok)

	prop, typeName, ok := parseConstraintTypePredicate("n.born IS :: DATE")
	require.True(t, ok)
	assert.Equal(t, "born", prop)
	assert.Equal(t, "DATE", typeName)
	prop, typeName, ok = parseConstraintTypePredicate("n.createdAt IS TYPED LOCAL DATETIME")
	require.True(t, ok)
	assert.Equal(t, "createdAt", prop)
	assert.Equal(t, "LOCAL DATETIME", typeName)
	_, _, ok = parseConstraintTypePredicate("n.born IS DATE")
	assert.False(t, ok)

	nodeKeyProps, ok := exec.parseNodeKeyPropertyList("(n.tenant, n.id) IS NODE KEY")
	require.True(t, ok)
	assert.Equal(t, []string{"tenant", "id"}, nodeKeyProps)
	_, ok = exec.parseNodeKeyPropertyList("n.tenant IS NODE KEY")
	assert.False(t, ok)
	parsedNodeKey, err := exec.parseCreateConstraintNodeKeyDDL("CREATE CONSTRAINT person_key FOR (n:Person) REQUIRE (n.tenant, n.id) IS NODE KEY")
	require.NoError(t, err)
	assert.Equal(t, "person_key", parsedNodeKey.name)
	assert.Equal(t, "Person", parsedNodeKey.label)
	assert.Equal(t, []string{"tenant", "id"}, parsedNodeKey.properties)
	parsedNodeKey, err = exec.parseCreateConstraintNodeKeyDDL("CREATE CONSTRAINT ON (n:Legacy) ASSERT (n.tenant, n.id) IS NODE KEY")
	require.NoError(t, err)
	assert.Empty(t, parsedNodeKey.name)
	assert.Equal(t, "Legacy", parsedNodeKey.label)
	assert.Equal(t, []string{"tenant", "id"}, parsedNodeKey.properties)

	kind, props, ok := exec.parseRelationshipKeyOrCompositeUniquePredicate("(r.tenant, r.id) IS RELATIONSHIP KEY")
	require.True(t, ok)
	assert.Equal(t, "rel_key", kind)
	assert.Equal(t, []string{"tenant", "id"}, props)
	kind, props, ok = exec.parseRelationshipKeyOrCompositeUniquePredicate("(r.a, r.b) IS UNIQUE")
	require.True(t, ok)
	assert.Equal(t, "unique", kind)
	assert.Equal(t, []string{"a", "b"}, props)
	kind, props, ok = exec.parseRelationshipKeyOrCompositeUniquePredicate("r.id IS RELATIONSHIP KEY")
	require.True(t, ok)
	assert.Equal(t, "rel_key", kind)
	assert.Equal(t, []string{"id"}, props)
	_, _, ok = exec.parseRelationshipKeyOrCompositeUniquePredicate("r.id IS UNIQUE")
	assert.False(t, ok)

	parsedRequire, err := exec.parseCreateConstraintForRequireDDL("CREATE CONSTRAINT rel_type FOR ()-[r:LIKES]-() REQUIRE r.weight IS :: FLOAT OPTIONS {provider: 'native'}")
	require.NoError(t, err)
	assert.Equal(t, "rel_type", parsedRequire.name)
	assert.True(t, parsedRequire.isRelationship)
	assert.Equal(t, "LIKES", parsedRequire.label)
	assert.Equal(t, "r.weight IS :: FLOAT", parsedRequire.requireExpr)
	_, err = exec.parseCreateConstraintForRequireDDL("CREATE CONSTRAINT bad FOR (n:Person) REQUIRE n.name IS :: STRING TRAILING")
	require.NoError(t, err)

	temporalProps, ok := exec.parseTemporalConstraintPredicate("(n.account, n.valid_from, n.valid_to) IS TEMPORAL NO OVERLAP")
	require.True(t, ok)
	assert.Equal(t, []string{"account", "valid_from", "valid_to"}, temporalProps)
	_, ok = exec.parseTemporalConstraintPredicate("(n.account) IS TEMPORAL EXTRA")
	assert.False(t, ok)
	parsedTemporal, err := exec.parseCreateConstraintTemporalDDL("CREATE CONSTRAINT temporal FOR (n:Account) REQUIRE (n.account, n.valid_from, n.valid_to) IS TEMPORAL")
	require.NoError(t, err)
	assert.Equal(t, "temporal", parsedTemporal.name)
	assert.Equal(t, "Account", parsedTemporal.label)

	prop, allowedRaw, ok := parseDomainConstraintPredicate("n.status IN ['ACTIVE', 'PAUSED']")
	require.True(t, ok)
	assert.Equal(t, "status", prop)
	assert.Equal(t, "'ACTIVE', 'PAUSED'", allowedRaw)
	_, _, ok = parseDomainConstraintPredicate("n.status IN ['ACTIVE'] trailing")
	assert.False(t, ok)
	parsedDomain, err := exec.parseCreateConstraintDomainDDL("CREATE CONSTRAINT status_domain FOR (n:Account) REQUIRE n.status IN ['ACTIVE', 'PAUSED']")
	require.NoError(t, err)
	assert.Equal(t, "status_domain", parsedDomain.name)
	assert.Equal(t, "Account", parsedDomain.label)
	assert.Equal(t, "status", parsedDomain.property)

	relType, direction, ok := parseRelationshipForDirection("(:User)-[:FOLLOWS]->(:User)")
	require.True(t, ok)
	assert.Equal(t, "FOLLOWS", relType)
	assert.Equal(t, "OUTGOING", direction)
	relType, direction, ok = parseRelationshipForDirection("(:User)<-[:FOLLOWS]-(:User)")
	require.True(t, ok)
	assert.Equal(t, "FOLLOWS", relType)
	assert.Equal(t, "INCOMING", direction)
	_, _, ok = parseRelationshipForDirection("(:User)-[:FOLLOWS]-(:User)")
	assert.False(t, ok)
	maxCount, ok := parseCardinalityRequireExpr("MAX COUNT 2")
	require.True(t, ok)
	assert.Equal(t, 2, maxCount)
	_, ok = parseCardinalityRequireExpr("MAX COUNT 0")
	assert.False(t, ok)
	parsedCardinality, err := exec.parseCreateConstraintCardinalityDDL("CREATE CONSTRAINT follows_limit FOR (:User)-[:FOLLOWS]->(:User) REQUIRE MAX COUNT 2")
	require.NoError(t, err)
	assert.Equal(t, "follows_limit", parsedCardinality.name)
	assert.Equal(t, "FOLLOWS", parsedCardinality.relType)
	assert.Equal(t, "OUTGOING", parsedCardinality.direction)
	assert.Equal(t, 2, parsedCardinality.maxCount)
	_, err = exec.parseCreateConstraintCardinalityDDL("CREATE CONSTRAINT bad FOR (:User)-[:FOLLOWS]->(:User) REQUIRE MAX COUNT 0")
	require.Error(t, err)

	parsedPolicy, err := exec.parseCreateConstraintPolicyDDL("CREATE CONSTRAINT access_policy FOR (:User)-[:CAN_ACCESS]->(:Document) REQUIRE DISALLOWED")
	require.NoError(t, err)
	assert.Equal(t, "access_policy", parsedPolicy.name)
	assert.Equal(t, "User", parsedPolicy.sourceLabel)
	assert.Equal(t, "CAN_ACCESS", parsedPolicy.relType)
	assert.Equal(t, "Document", parsedPolicy.targetLabel)
	assert.Equal(t, "DISALLOWED", parsedPolicy.policyMode)
	_, err = exec.parseCreateConstraintPolicyDDL("CREATE CONSTRAINT bad FOR (:User)-[:CAN_ACCESS]-(:Document) REQUIRE ALLOWED")
	require.Error(t, err)

	parsedRelKey, err := exec.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL("CREATE CONSTRAINT rel_key FOR ()-[r:LIKES]-() REQUIRE (r.tenant, r.id) IS RELATIONSHIP KEY")
	require.NoError(t, err)
	assert.Equal(t, "rel_key", parsedRelKey.name)
	assert.Equal(t, "LIKES", parsedRelKey.label)
	assert.Equal(t, []string{"tenant", "id"}, parsedRelKey.properties)
	assert.Equal(t, "rel_key", parsedRelKey.kind)
	parsedRelKey, err = exec.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL("CREATE CONSTRAINT rel_unique FOR ()-[r:RATED]-() REQUIRE (r.user, r.item) IS UNIQUE")
	require.NoError(t, err)
	assert.Equal(t, "unique", parsedRelKey.kind)
	_, err = exec.parseCreateConstraintRelationshipKeyOrCompositeUniqueDDL("CREATE CONSTRAINT bad FOR (n:Person) REQUIRE n.id IS RELATIONSHIP KEY")
	require.Error(t, err)

	parsedType, err := exec.parseCreateConstraintTypeDDL("CREATE CONSTRAINT person_name_type FOR (n:Person) REQUIRE n.name IS :: STRING")
	require.NoError(t, err)
	assert.Equal(t, "person_name_type", parsedType.name)
	assert.Equal(t, "Person", parsedType.label)
	assert.Equal(t, "name", parsedType.property)
	assert.Equal(t, storage.PropertyTypeString, parsedType.expectedType)
	parsedType, err = exec.parseCreateConstraintTypeDDL("CREATE CONSTRAINT ON (n:Legacy) ASSERT n.active IS TYPED BOOLEAN")
	require.NoError(t, err)
	assert.Equal(t, "Legacy", parsedType.label)
	assert.Equal(t, "active", parsedType.property)
	assert.Equal(t, storage.PropertyTypeBoolean, parsedType.expectedType)
	_, err = exec.parseCreateConstraintTypeDDL("CREATE CONSTRAINT bad FOR (n:Person) REQUIRE n.name IS :: UNSUPPORTED")
	require.Error(t, err)

	parsedSimple, err := exec.parseCreateConstraintSimplePropertyDDL("CREATE CONSTRAINT person_email_unique FOR (n:Person) REQUIRE n.email IS UNIQUE")
	require.NoError(t, err)
	assert.Equal(t, "person_email_unique", parsedSimple.name)
	assert.Equal(t, "Person", parsedSimple.label)
	assert.Equal(t, "email", parsedSimple.property)
	assert.Equal(t, "unique", parsedSimple.kind)
	parsedSimple, err = exec.parseCreateConstraintSimplePropertyDDL("CREATE CONSTRAINT ON (n:Legacy) ASSERT EXISTS(n.email)")
	require.NoError(t, err)
	assert.Equal(t, "Legacy", parsedSimple.label)
	assert.Equal(t, "email", parsedSimple.property)
	assert.Equal(t, "exists", parsedSimple.kind)
	_, err = exec.parseCreateConstraintSimplePropertyDDL("CREATE CONSTRAINT bad FOR (n:Person) REQUIRE n.email IS INDEXED")
	require.Error(t, err)
}

func TestCoverageLiftSchemaDDLExecutionCompatibilityMatrix(t *testing.T) {
	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	runDDL := func(query string) {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.NotNil(t, result)
		assert.Empty(t, result.Columns)
		assert.Empty(t, result.Rows)
	}
	findConstraint := func(name string) storage.Constraint {
		t.Helper()
		for _, constraint := range store.GetSchema().GetAllConstraints() {
			if constraint.Name == name {
				return constraint
			}
		}
		t.Fatalf("constraint %s not found in %#v", name, store.GetSchema().GetAllConstraints())
		return storage.Constraint{}
	}
	findPropertyTypeConstraint := func(name string) storage.PropertyTypeConstraint {
		t.Helper()
		for _, constraint := range store.GetSchema().GetAllPropertyTypeConstraints() {
			if constraint.Name == name {
				return constraint
			}
		}
		t.Fatalf("property type constraint %s not found in %#v", name, store.GetSchema().GetAllPropertyTypeConstraints())
		return storage.PropertyTypeConstraint{}
	}
	hasIndex := func(name string) bool {
		for _, raw := range store.GetSchema().GetIndexes() {
			index, ok := raw.(map[string]interface{})
			if ok && index["name"] == name {
				return true
			}
		}
		return false
	}

	runDDL("CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:MANAGES]->() REQUIRE MAX COUNT 3")
	cardinality := findConstraint("constraint_manages_max_outgoing_3")
	assert.Equal(t, storage.ConstraintCardinality, cardinality.Type)
	assert.Equal(t, storage.ConstraintEntityRelationship, cardinality.EffectiveEntityType())
	assert.Equal(t, "MANAGES", cardinality.Label)
	assert.Equal(t, "OUTGOING", cardinality.Direction)
	assert.Equal(t, 3, cardinality.MaxCount)
	runDDL("CREATE CONSTRAINT max_reviews_per_product IF NOT EXISTS FOR ()<-[r:REVIEWS]-() REQUIRE MAX COUNT 1000")
	runDDL("CREATE CONSTRAINT max_reviews_per_product IF NOT EXISTS FOR ()<-[r:REVIEWS]-() REQUIRE MAX COUNT 1000")
	cardinality = findConstraint("max_reviews_per_product")
	assert.Equal(t, "INCOMING", cardinality.Direction)
	assert.Equal(t, 1000, cardinality.MaxCount)
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT bad_zero FOR ()-[r:MANAGES]->() REQUIRE MAX COUNT 0", nil)
	require.ErrorContains(t, err, "positive integer")
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT bad_text FOR ()-[r:MANAGES]->() REQUIRE MAX COUNT abc", nil)
	require.ErrorContains(t, err, "invalid cardinality require clause")

	runDDL("CREATE CONSTRAINT IF NOT EXISTS FOR (:Employee)-[:MANAGES]->(:Manager) REQUIRE ALLOWED")
	policy := findConstraint("constraint_employee_manages_manager_allowed")
	assert.Equal(t, storage.ConstraintPolicy, policy.Type)
	assert.Equal(t, storage.ConstraintEntityRelationship, policy.EffectiveEntityType())
	assert.Equal(t, "MANAGES", policy.Label)
	assert.Equal(t, "Employee", policy.SourceLabel)
	assert.Equal(t, "Manager", policy.TargetLabel)
	assert.Equal(t, "ALLOWED", policy.PolicyMode)
	runDDL("CREATE CONSTRAINT prevent_admin_delete FOR (:Admin)-[:DELETE]->(:User) REQUIRE DISALLOWED")
	policy = findConstraint("prevent_admin_delete")
	assert.Equal(t, "DISALLOWED", policy.PolicyMode)
	_, err = exec.Execute(ctx, "CREATE CONSTRAINT bad_policy FOR (:Admin)-[:DELETE]->(:User) REQUIRE MAYBE", nil)
	require.Error(t, err)

	runDDL("CREATE CONSTRAINT rel_key_multi IF NOT EXISTS FOR ()-[r:CONNECTS]-() REQUIRE (r.source_id, r.target_id, r.timestamp) IS RELATIONSHIP KEY")
	relKey := findConstraint("rel_key_multi")
	assert.Equal(t, storage.ConstraintRelationshipKey, relKey.Type)
	assert.Equal(t, storage.ConstraintEntityRelationship, relKey.EffectiveEntityType())
	assert.Equal(t, "CONNECTS", relKey.Label)
	assert.Equal(t, []string{"source_id", "target_id", "timestamp"}, relKey.Properties)
	runDDL("CREATE CONSTRAINT single_rel_key FOR ()-[r:UNIQUE_PAIR]-() REQUIRE r.id IS RELATIONSHIP KEY")
	relKey = findConstraint("single_rel_key")
	assert.Equal(t, storage.ConstraintRelationshipKey, relKey.Type)
	assert.Equal(t, []string{"id"}, relKey.Properties)
	runDDL("CREATE CONSTRAINT rel_unique_composite FOR ()-[r:FRIEND_OF]-() REQUIRE (r.user1, r.user2) IS UNIQUE")
	relUnique := findConstraint("rel_unique_composite")
	assert.Equal(t, storage.ConstraintUnique, relUnique.Type)
	assert.Equal(t, storage.ConstraintEntityRelationship, relUnique.EffectiveEntityType())
	assert.Equal(t, []string{"user1", "user2"}, relUnique.Properties)

	runDDL("CREATE CONSTRAINT rel_weight_type FOR ()-[r:RATED]-() REQUIRE r.weight IS :: FLOAT")
	typeConstraint := findPropertyTypeConstraint("rel_weight_type")
	assert.Equal(t, storage.ConstraintEntityRelationship, typeConstraint.EntityType)
	assert.Equal(t, "RATED", typeConstraint.Label)
	assert.Equal(t, "weight", typeConstraint.Property)
	assert.Equal(t, storage.PropertyTypeFloat, typeConstraint.ExpectedType)
	runDDL("CREATE CONSTRAINT rel_status_domain FOR ()-[r:STATUS]-() REQUIRE r.state IN ['NEW', 'DONE']")
	domain := findConstraint("rel_status_domain")
	assert.Equal(t, storage.ConstraintDomain, domain.Type)
	assert.Equal(t, storage.ConstraintEntityRelationship, domain.EffectiveEntityType())
	assert.Equal(t, []interface{}{"NEW", "DONE"}, domain.AllowedValues)
	runDDL("CREATE CONSTRAINT rel_temporal FOR ()-[r:VALID]-() REQUIRE (r.key, r.valid_from, r.valid_to) IS TEMPORAL NO OVERLAP")
	temporal := findConstraint("rel_temporal")
	assert.Equal(t, storage.ConstraintTemporal, temporal.Type)
	assert.Equal(t, storage.ConstraintEntityRelationship, temporal.EffectiveEntityType())
	assert.Equal(t, []string{"key", "valid_from", "valid_to"}, temporal.Properties)

	runDDL("CREATE RANGE INDEX account_age_range FOR (n:Account) ON (n.age)")
	runDDL("CREATE INDEX rel_weight_idx FOR ()-[r:RATED]-() ON (r.weight)")
	runDDL("CREATE VECTOR INDEX rel_embedding_idx FOR ()-[r:RATED]-() ON (r.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}")
	assert.True(t, hasIndex("account_age_range"))
	assert.True(t, hasIndex("rel_weight_idx"))
	assert.True(t, hasIndex("rel_embedding_idx"))
	runDDL("DROP INDEX rel_embedding_idx IF EXISTS")
	runDDL("DROP INDEX rel_embedding_idx IF EXISTS")
	assert.False(t, hasIndex("rel_embedding_idx"))
	_, err = exec.Execute(ctx, "DROP INDEX rel_embedding_idx", nil)
	require.Error(t, err)
}
