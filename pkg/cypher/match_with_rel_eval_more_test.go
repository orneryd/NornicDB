package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEvaluateExpressionFromValues_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_eval_cov"))

	n := &storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	}
	values := map[string]interface{}{
		"n":         n,
		"typedNode": &storage.Node{ID: "map-id", Labels: []string{"A", "B"}},
		"nodeMap": map[string]interface{}{
			"id":         "map-id",
			"labels":     []interface{}{"A", "B"},
			"properties": map[string]interface{}{"title": "T"},
			"title":      "T2",
		},
		"path": map[string]interface{}{
			"length": int64(2),
			"nodes": []*storage.Node{
				{ID: "n1", Labels: []string{"A"}, Properties: map[string]interface{}{"name": "alice"}},
				{ID: "n2", Labels: []string{"B"}, Properties: map[string]interface{}{"name": "bob"}},
			},
			"rels": []*storage.Edge{
				{ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{"w": 1}},
			},
		},
		"xs":      []interface{}{1, 2, 3},
		"asText":  "abcd",
		"rowTime": "2026-03-20T20:22:20Z",
		"nilVal":  nil,
	}

	fixtureAdmission720Context, fixtureAdmission720 := expressionTestRow(context.Background(), "", values)
	_ = fixtureAdmission720
	require.Nil(t, exec.evaluateExpressionFromValues(fixtureAdmission720, "", fixtureAdmission720Context))
	fixtureAdmission721Context, fixtureAdmission721 := expressionTestRow(context.Background(), "n.name", values)
	_ = fixtureAdmission721
	require.Equal(t, "alice", exec.evaluateExpressionFromValues(fixtureAdmission721, "n.name", fixtureAdmission721Context))
	fixtureAdmission722Context, fixtureAdmission722 := expressionTestRow(context.Background(), "nodeMap.title", values)
	_ = fixtureAdmission722
	require.Equal(t, "T2", exec.evaluateExpressionFromValues(fixtureAdmission722, "nodeMap.title", fixtureAdmission722Context))
	fixtureAdmission723Context, fixtureAdmission723 := expressionTestRow(context.Background(), "length(path)", values)
	_ = fixtureAdmission723
	require.Equal(t, int64(2), exec.evaluateExpressionFromValues(fixtureAdmission723, "length(path)", fixtureAdmission723Context))
	fixtureAdmission724Context, fixtureAdmission724 := expressionTestRow(context.Background(), "size(xs)", values)
	_ = fixtureAdmission724
	require.Equal(t, int64(3), exec.evaluateExpressionFromValues(fixtureAdmission724, "size(xs)", fixtureAdmission724Context))
	fixtureAdmission725Context, fixtureAdmission725 := expressionTestRow(context.Background(), "size(asText)", values)
	_ = fixtureAdmission725
	require.Equal(t, int64(4), exec.evaluateExpressionFromValues(fixtureAdmission725, "size(asText)", fixtureAdmission725Context))
	// size(null) is null: the legacy evaluator returned the length of the
	// literal word "missing" (text-fallthrough), which broke size(null)
	// semantics in computed rows.
	fixtureAdmission726Context, fixtureAdmission726 := expressionTestRow(context.Background(), "size(missing)", values)
	_ = fixtureAdmission726
	require.Nil(t, exec.evaluateExpressionFromValues(fixtureAdmission726, "size(missing)", fixtureAdmission726Context))

	fixtureAdmission727Context, fixtureAdmission727 := expressionTestRow(context.Background(), "labels(typedNode)", values)
	_ = fixtureAdmission727
	labels := exec.evaluateExpressionFromValues(fixtureAdmission727, "labels(typedNode)", fixtureAdmission727Context).([]interface{})
	require.Equal(t, []interface{}{"A", "B"}, labels)
	// labels() of a map is a type error, as in Neo4j: a map is not a node.
	labelsCtx := withExpressionFailureSlot(context.Background())
	fixtureAdmission728Context, fixtureAdmission728 := expressionTestContext(labelsCtx, "labels(nodeMap)")
	_ = fixtureAdmission728
	require.Nil(t, exec.evaluateExpressionFromValuesContext(fixtureAdmission728, fixtureAdmission728Context, "labels(nodeMap)", values))
	require.Error(t, getExpressionFailure(labelsCtx))

	fixtureAdmission729Context, fixtureAdmission729 := expressionTestRow(context.Background(), "[r IN relationships(path) | type(r)]", values)
	_ = fixtureAdmission729
	relTypes := exec.evaluateExpressionFromValues(fixtureAdmission729, "[r IN relationships(path) | type(r)]", fixtureAdmission729Context).([]interface{})
	require.Equal(t, []interface{}{"KNOWS"}, relTypes)

	fixtureAdmission730Context, fixtureAdmission730 := expressionTestRow(context.Background(), "relationships(path)", values)
	_ = fixtureAdmission730
	relList := exec.evaluateExpressionFromValues(fixtureAdmission730, "relationships(path)", fixtureAdmission730Context).([]interface{})
	require.Len(t, relList, 1)
	rel := relList[0].(*storage.Edge)
	require.Equal(t, "KNOWS", rel.Type)
	require.Equal(t, "e1", string(rel.ID))

	fixtureAdmission731Context, fixtureAdmission731 := expressionTestRow(context.Background(), "nodes(path)", values)
	_ = fixtureAdmission731
	nodeList := exec.evaluateExpressionFromValues(fixtureAdmission731, "nodes(path)", fixtureAdmission731Context).([]interface{})
	require.Len(t, nodeList, 2)
	require.Equal(t, "n1", string(nodeList[0].(*storage.Node).ID))
	require.Equal(t, "bob", nodeList[1].(*storage.Node).Properties["name"])

	fixtureAdmission732Context, fixtureAdmission732 := expressionTestRow(context.Background(), "elementId(n)", values)
	_ = fixtureAdmission732
	require.Equal(t, "4:match_with_rel_eval_cov:n1", exec.evaluateExpressionFromValues(fixtureAdmission732, "elementId(n)", fixtureAdmission732Context))
	fixtureAdmission733Context, fixtureAdmission733 := expressionTestRow(context.Background(), "id(typedNode)", values)
	_ = fixtureAdmission733
	require.Equal(t, "map-id", exec.evaluateExpressionFromValues(fixtureAdmission733, "id(typedNode)", fixtureAdmission733Context))

	fixtureAdmission734Context, fixtureAdmission734 := expressionTestRow(context.Background(), "{name: n.name, cnt: size(xs)}", values)
	_ = fixtureAdmission734
	mapLit := exec.evaluateExpressionFromValues(fixtureAdmission734, "{name: n.name, cnt: size(xs)}", fixtureAdmission734Context).(map[string]interface{})
	require.Equal(t, "alice", mapLit["name"])
	require.Equal(t, int64(3), mapLit["cnt"])

	fixtureAdmission735Context, fixtureAdmission735 := expressionTestRow(context.Background(), "datetime(rowTime)", values)
	_ = fixtureAdmission735
	dt := exec.evaluateExpressionFromValues(fixtureAdmission735, "datetime(rowTime)", fixtureAdmission735Context)
	dtDateTime, ok := dt.(CypherDateTime)
	require.True(t, ok, "expected CypherDateTime, got %T", dt)
	require.Equal(t, "2026-03-20T20:22:20Z", dtDateTime.Time.UTC().Format(time.RFC3339))
	fixtureAdmission736Context, fixtureAdmission736 := expressionTestRow(context.Background(), "datetime('not-a-time')", values)
	_ = fixtureAdmission736
	require.Nil(t, exec.evaluateExpressionFromValues(fixtureAdmission736, "datetime('not-a-time')", fixtureAdmission736Context))

	fixtureAdmission737Context, fixtureAdmission737 := expressionTestRow(context.Background(), "localdatetime()", values)
	_ = fixtureAdmission737
	local, ok := exec.evaluateExpressionFromValues(fixtureAdmission737, "localdatetime()", fixtureAdmission737Context).(CypherLocalDateTime)
	require.True(t, ok, "expected CypherLocalDateTime")
	require.False(t, local.Time.IsZero())

	fixtureAdmission738Context, fixtureAdmission738 := expressionTestRow(context.Background(), "coalesce(missing, n.name)", values)
	_ = fixtureAdmission738
	coalesced := exec.evaluateExpressionFromValues(fixtureAdmission738, "coalesce(missing, n.name)", fixtureAdmission738Context)
	require.Equal(t, "alice", coalesced)
}

func TestEvaluateCaseExpressionFromValues_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_case_cov"))
	values := map[string]interface{}{
		"k":      "v",
		"status": "open",
		"n":      int64(2),
	}

	fixtureAdmission739Context, fixtureAdmission739 := expressionTestRow(context.Background(), "CASE k WHEN 'v' THEN 'hit' ELSE 'miss' END", values)
	_ = fixtureAdmission739
	require.Equal(t, "hit", exec.evaluateCaseExpressionFromValues(fixtureAdmission739, "CASE k WHEN 'v' THEN 'hit' ELSE 'miss' END", fixtureAdmission739Context))
	fixtureAdmission740Context, fixtureAdmission740 := expressionTestRow(context.Background(), "CASE WHEN status = 'closed' THEN 'closed' ELSE 'open' END", values)
	_ = fixtureAdmission740
	require.Equal(t, "open", exec.evaluateCaseExpressionFromValues(fixtureAdmission740, "CASE WHEN status = 'closed' THEN 'closed' ELSE 'open' END", fixtureAdmission740Context))
	fixtureAdmission741Context, fixtureAdmission741 := expressionTestRow(context.Background(), "CASE WHEN n > 1 THEN 2 ELSE 0 END", values)
	_ = fixtureAdmission741
	require.Equal(t, int64(2), exec.evaluateCaseExpressionFromValues(fixtureAdmission741, "CASE WHEN n > 1 THEN 2 ELSE 0 END", fixtureAdmission741Context))
	fixtureAdmission742Context, fixtureAdmission742 := expressionTestRow(context.Background(), "CASE WHEN n < 0 THEN 2 END", values)
	_ = fixtureAdmission742
	require.Nil(t, exec.evaluateCaseExpressionFromValues(fixtureAdmission742, "CASE WHEN n < 0 THEN 2 END", fixtureAdmission742Context))
}

func TestParseLiteralValueFromComputedRow_AdditionalBranches(t *testing.T) {
	val, ok := parseLiteralValueFromComputedRow("NULL")
	require.True(t, ok)
	require.Nil(t, val)

	val, ok = parseLiteralValueFromComputedRow("TRUE")
	require.True(t, ok)
	require.Equal(t, true, val)

	val, ok = parseLiteralValueFromComputedRow("FALSE")
	require.True(t, ok)
	require.Equal(t, false, val)

	val, ok = parseLiteralValueFromComputedRow("'text'")
	require.True(t, ok)
	require.Equal(t, "text", val)

	val, ok = parseLiteralValueFromComputedRow("123")
	require.True(t, ok)
	require.Equal(t, int64(123), val)

	val, ok = parseLiteralValueFromComputedRow("3.5")
	require.True(t, ok)
	require.Equal(t, 3.5, val)

	_, ok = parseLiteralValueFromComputedRow("not-literal")
	require.False(t, ok)
}

func TestEvaluateConditionFromValues_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_cond_cov"))
	values := map[string]interface{}{
		"a": int64(3),
		"b": int64(2),
		"s": "x",
	}

	fixtureAdmission743Context, fixtureAdmission743 := expressionTestRow(context.Background(), "a >= b", values)
	_ = fixtureAdmission743
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission743, "a >= b", fixtureAdmission743Context))
	fixtureAdmission744Context, fixtureAdmission744 := expressionTestRow(context.Background(), "a > b", values)
	_ = fixtureAdmission744
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission744, "a > b", fixtureAdmission744Context))
	fixtureAdmission745Context, fixtureAdmission745 := expressionTestRow(context.Background(), "b <= a", values)
	_ = fixtureAdmission745
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission745, "b <= a", fixtureAdmission745Context))
	fixtureAdmission746Context, fixtureAdmission746 := expressionTestRow(context.Background(), "b < a", values)
	_ = fixtureAdmission746
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission746, "b < a", fixtureAdmission746Context))
	fixtureAdmission747Context, fixtureAdmission747 := expressionTestRow(context.Background(), "s <> 'y'", values)
	_ = fixtureAdmission747
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission747, "s <> 'y'", fixtureAdmission747Context))
	fixtureAdmission748Context, fixtureAdmission748 := expressionTestRow(context.Background(), "NOT s = 'y'", values)
	_ = fixtureAdmission748
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission748, "NOT s = 'y'", fixtureAdmission748Context))
	// Undefined identifiers evaluate to Cypher null in the shared evaluator
	// (legacy clone returned the raw text, which broke IS NULL semantics).
	fixtureAdmission749Context, fixtureAdmission749 := expressionTestRow(context.Background(), "missing IS NULL", values)
	_ = fixtureAdmission749
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission749, "missing IS NULL", fixtureAdmission749Context))
	fixtureAdmission750Context, fixtureAdmission750 := expressionTestRow(context.Background(), "missing IS NOT NULL", values)
	_ = fixtureAdmission750
	require.False(t, exec.evaluateConditionFromValues(fixtureAdmission750, "missing IS NOT NULL", fixtureAdmission750Context))
	fixtureAdmission751Context, fixtureAdmission751 := expressionTestRow(context.Background(), "a = 3 AND b = 2", values)
	_ = fixtureAdmission751
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission751, "a = 3 AND b = 2", fixtureAdmission751Context))
	fixtureAdmission752Context, fixtureAdmission752 := expressionTestRow(context.Background(), "a = 9 OR b = 2", values)
	_ = fixtureAdmission752
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission752, "a = 9 OR b = 2", fixtureAdmission752Context))
}

func BenchmarkEvaluateConditionFromValues(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "match_with_rel_cond_bench"))
	values := map[string]interface{}{"a": int64(3), "b": int64(2), "s": "x"}
	conditions := []string{"a >= b", "s <> 'y'", "a = 3 AND b = 2", "missing IS NULL"}
	scopes := expressionTestRows(context.Background(), conditions, values)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.evaluateConditionFromValues(scopes[i%len(scopes)].statement, conditions[i%len(conditions)], scopes[i%len(scopes)].values)
	}
}

func BenchmarkEvaluateCaseExpressionFromValues(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "match_with_rel_case_bench"))
	values := map[string]interface{}{"a": int64(3), "b": int64(2), "s": "x"}
	exprs := []string{
		"CASE a WHEN 3 THEN 'three' ELSE 'other' END",
		"CASE WHEN b > 1 THEN s ELSE 'low' END",
	}

	scopes := expressionTestRows(context.Background(), exprs, values)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.evaluateCaseExpressionFromValues(scopes[i%len(scopes)].statement, exprs[i%len(exprs)], scopes[i%len(scopes)].values)
	}
}

func BenchmarkEvaluateExpressionFromValues(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "match_with_rel_expr_bench"))
	node := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}
	path := map[string]interface{}{
		"length": int64(2),
		"nodes": []*storage.Node{
			{ID: "n1"}, {ID: "n2"},
		},
		"rels": []*storage.Edge{
			{ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n2"},
		},
	}
	values := map[string]interface{}{
		"n":    node,
		"path": path,
		"xs":   []interface{}{int64(1), int64(2), int64(3)},
		"a":    int64(3),
		"b":    int64(2),
	}
	exprs := []string{"n", "n.name", "length(path)", "size(xs)", "a + b", "coalesce(missing, n.name)"}

	scopes := expressionTestRows(context.Background(), exprs, values)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.evaluateExpressionFromValues(scopes[i%len(scopes)].statement, exprs[i%len(exprs)], scopes[i%len(scopes)].values)
	}
}

func TestEvaluateMapLiteralFromValues_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_map_cov"))
	values := map[string]interface{}{"x": int64(1), "y": int64(2)}

	fixtureAdmission753Context, fixtureAdmission753 := expressionTestRow(context.Background(), "{}", values)
	_ = fixtureAdmission753
	require.Equal(t, map[string]interface{}{}, exec.evaluateMapLiteralFromValues(fixtureAdmission753, "{}", fixtureAdmission753Context))
	fixtureAdmission754Context, fixtureAdmission754 := expressionTestRow(context.Background(), "{badpair}", values)
	_ = fixtureAdmission754
	require.Equal(t, map[string]interface{}{}, exec.evaluateMapLiteralFromValues(fixtureAdmission754, "{badpair}", fixtureAdmission754Context))

	fixtureAdmission755Context, fixtureAdmission755 := expressionTestRow(context.Background(), "{a: x, b: {inner: y}}", values)
	_ = fixtureAdmission755
	out := exec.evaluateMapLiteralFromValues(fixtureAdmission755, "{a: x, b: {inner: y}}", fixtureAdmission755Context)
	require.Equal(t, int64(1), out["a"])
	nested, ok := out["b"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, int64(2), nested["inner"])
}

func TestEvaluateExpressionFromValues_CoalesceParamContext(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_coalesce_cov"))
	ctx := withParams(context.Background(), map[string]interface{}{"fallback": "z"})
	fixtureAdmission756Context, fixtureAdmission756 := expressionTestContext(ctx, "coalesce(missing, $fallback)")
	_ = fixtureAdmission756
	require.Equal(t, "z", exec.evaluateExpressionWithContext(fixtureAdmission756, fixtureAdmission756Context, "coalesce(missing, $fallback)", nil, nil))
}
