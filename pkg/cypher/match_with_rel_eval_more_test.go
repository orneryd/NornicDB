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

	fixtureAdmission728Context, fixtureAdmission728 := expressionTestRow(context.Background(), "", values)
	_ = fixtureAdmission728
	require.Nil(t, exec.evaluateExpressionFromValues(fixtureAdmission728, "", fixtureAdmission728Context))
	fixtureAdmission729Context, fixtureAdmission729 := expressionTestRow(context.Background(), "n.name", values)
	_ = fixtureAdmission729
	require.Equal(t, "alice", exec.evaluateExpressionFromValues(fixtureAdmission729, "n.name", fixtureAdmission729Context))
	fixtureAdmission730Context, fixtureAdmission730 := expressionTestRow(context.Background(), "nodeMap.title", values)
	_ = fixtureAdmission730
	require.Equal(t, "T2", exec.evaluateExpressionFromValues(fixtureAdmission730, "nodeMap.title", fixtureAdmission730Context))
	fixtureAdmission731Context, fixtureAdmission731 := expressionTestRow(context.Background(), "length(path)", values)
	_ = fixtureAdmission731
	require.Equal(t, int64(2), exec.evaluateExpressionFromValues(fixtureAdmission731, "length(path)", fixtureAdmission731Context))
	fixtureAdmission732Context, fixtureAdmission732 := expressionTestRow(context.Background(), "size(xs)", values)
	_ = fixtureAdmission732
	require.Equal(t, int64(3), exec.evaluateExpressionFromValues(fixtureAdmission732, "size(xs)", fixtureAdmission732Context))
	fixtureAdmission733Context, fixtureAdmission733 := expressionTestRow(context.Background(), "size(asText)", values)
	_ = fixtureAdmission733
	require.Equal(t, int64(4), exec.evaluateExpressionFromValues(fixtureAdmission733, "size(asText)", fixtureAdmission733Context))
	// size(null) is null: the legacy evaluator returned the length of the
	// literal word "missing" (text-fallthrough), which broke size(null)
	// semantics in computed rows.
	fixtureAdmission734Context, fixtureAdmission734 := expressionTestRow(context.Background(), "size(missing)", values)
	_ = fixtureAdmission734
	require.Nil(t, exec.evaluateExpressionFromValues(fixtureAdmission734, "size(missing)", fixtureAdmission734Context))

	fixtureAdmission735Context, fixtureAdmission735 := expressionTestRow(context.Background(), "labels(typedNode)", values)
	_ = fixtureAdmission735
	labels := exec.evaluateExpressionFromValues(fixtureAdmission735, "labels(typedNode)", fixtureAdmission735Context).([]interface{})
	require.Equal(t, []interface{}{"A", "B"}, labels)

	fixtureAdmission736Context, fixtureAdmission736 := expressionTestRow(context.Background(), "[r IN relationships(path) | type(r)]", values)
	_ = fixtureAdmission736
	relTypes := exec.evaluateExpressionFromValues(fixtureAdmission736, "[r IN relationships(path) | type(r)]", fixtureAdmission736Context).([]interface{})
	require.Equal(t, []interface{}{"KNOWS"}, relTypes)

	fixtureAdmission737Context, fixtureAdmission737 := expressionTestRow(context.Background(), "relationships(path)", values)
	_ = fixtureAdmission737
	relList := exec.evaluateExpressionFromValues(fixtureAdmission737, "relationships(path)", fixtureAdmission737Context).([]interface{})
	require.Len(t, relList, 1)
	rel := relList[0].(*storage.Edge)
	require.Equal(t, "KNOWS", rel.Type)
	require.Equal(t, "e1", string(rel.ID))

	fixtureAdmission738Context, fixtureAdmission738 := expressionTestRow(context.Background(), "nodes(path)", values)
	_ = fixtureAdmission738
	nodeList := exec.evaluateExpressionFromValues(fixtureAdmission738, "nodes(path)", fixtureAdmission738Context).([]interface{})
	require.Len(t, nodeList, 2)
	require.Equal(t, "n1", string(nodeList[0].(*storage.Node).ID))
	require.Equal(t, "bob", nodeList[1].(*storage.Node).Properties["name"])

	fixtureAdmission739Context, fixtureAdmission739 := expressionTestRow(context.Background(), "elementId(n)", values)
	_ = fixtureAdmission739
	require.Equal(t, "4:match_with_rel_eval_cov:n1", exec.evaluateExpressionFromValues(fixtureAdmission739, "elementId(n)", fixtureAdmission739Context))
	fixtureAdmission740Context, fixtureAdmission740 := expressionTestRow(context.Background(), "id(typedNode)", values)
	_ = fixtureAdmission740
	require.Equal(t, "map-id", exec.evaluateExpressionFromValues(fixtureAdmission740, "id(typedNode)", fixtureAdmission740Context))

	fixtureAdmission741Context, fixtureAdmission741 := expressionTestRow(context.Background(), "{name: n.name, cnt: size(xs)}", values)
	_ = fixtureAdmission741
	mapLit := exec.evaluateExpressionFromValues(fixtureAdmission741, "{name: n.name, cnt: size(xs)}", fixtureAdmission741Context).(map[string]interface{})
	require.Equal(t, "alice", mapLit["name"])
	require.Equal(t, int64(3), mapLit["cnt"])

	fixtureAdmission742Context, fixtureAdmission742 := expressionTestRow(context.Background(), "datetime(rowTime)", values)
	_ = fixtureAdmission742
	dt := exec.evaluateExpressionFromValues(fixtureAdmission742, "datetime(rowTime)", fixtureAdmission742Context)
	dtDateTime, ok := dt.(CypherDateTime)
	require.True(t, ok, "expected CypherDateTime, got %T", dt)
	require.Equal(t, "2026-03-20T20:22:20Z", dtDateTime.Time.UTC().Format(time.RFC3339))
	fixtureAdmission743Context, fixtureAdmission743 := expressionTestRow(context.Background(), "datetime('not-a-time')", values)
	_ = fixtureAdmission743
	require.Nil(t, exec.evaluateExpressionFromValues(fixtureAdmission743, "datetime('not-a-time')", fixtureAdmission743Context))

	fixtureAdmission744Context, fixtureAdmission744 := expressionTestRow(context.Background(), "localdatetime()", values)
	_ = fixtureAdmission744
	local, ok := exec.evaluateExpressionFromValues(fixtureAdmission744, "localdatetime()", fixtureAdmission744Context).(CypherLocalDateTime)
	require.True(t, ok, "expected CypherLocalDateTime")
	require.False(t, local.Time.IsZero())

	values["missing"] = nil
	fixtureAdmission745Context, fixtureAdmission745 := expressionTestRow(context.Background(), "coalesce(missing, n.name)", values)
	_ = fixtureAdmission745
	coalesced := exec.evaluateExpressionFromValues(fixtureAdmission745, "coalesce(missing, n.name)", fixtureAdmission745Context)
	require.Equal(t, "alice", coalesced)
}

func TestEvaluateCaseExpressionFromValues_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_case_cov"))
	values := map[string]interface{}{
		"k":      "v",
		"status": "open",
		"n":      int64(2),
	}

	fixtureAdmission746Context, fixtureAdmission746 := expressionTestRow(context.Background(), "CASE k WHEN 'v' THEN 'hit' ELSE 'miss' END", values)
	_ = fixtureAdmission746
	require.Equal(t, "hit", exec.evaluateCaseExpressionFromValues(fixtureAdmission746, "CASE k WHEN 'v' THEN 'hit' ELSE 'miss' END", fixtureAdmission746Context))
	fixtureAdmission747Context, fixtureAdmission747 := expressionTestRow(context.Background(), "CASE WHEN status = 'closed' THEN 'closed' ELSE 'open' END", values)
	_ = fixtureAdmission747
	require.Equal(t, "open", exec.evaluateCaseExpressionFromValues(fixtureAdmission747, "CASE WHEN status = 'closed' THEN 'closed' ELSE 'open' END", fixtureAdmission747Context))
	fixtureAdmission748Context, fixtureAdmission748 := expressionTestRow(context.Background(), "CASE WHEN n > 1 THEN 2 ELSE 0 END", values)
	_ = fixtureAdmission748
	require.Equal(t, int64(2), exec.evaluateCaseExpressionFromValues(fixtureAdmission748, "CASE WHEN n > 1 THEN 2 ELSE 0 END", fixtureAdmission748Context))
	fixtureAdmission749Context, fixtureAdmission749 := expressionTestRow(context.Background(), "CASE WHEN n < 0 THEN 2 END", values)
	_ = fixtureAdmission749
	require.Nil(t, exec.evaluateCaseExpressionFromValues(fixtureAdmission749, "CASE WHEN n < 0 THEN 2 END", fixtureAdmission749Context))
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

	fixtureAdmission750Context, fixtureAdmission750 := expressionTestRow(context.Background(), "a >= b", values)
	_ = fixtureAdmission750
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission750, "a >= b", fixtureAdmission750Context))
	fixtureAdmission751Context, fixtureAdmission751 := expressionTestRow(context.Background(), "a > b", values)
	_ = fixtureAdmission751
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission751, "a > b", fixtureAdmission751Context))
	fixtureAdmission752Context, fixtureAdmission752 := expressionTestRow(context.Background(), "b <= a", values)
	_ = fixtureAdmission752
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission752, "b <= a", fixtureAdmission752Context))
	fixtureAdmission753Context, fixtureAdmission753 := expressionTestRow(context.Background(), "b < a", values)
	_ = fixtureAdmission753
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission753, "b < a", fixtureAdmission753Context))
	fixtureAdmission754Context, fixtureAdmission754 := expressionTestRow(context.Background(), "s <> 'y'", values)
	_ = fixtureAdmission754
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission754, "s <> 'y'", fixtureAdmission754Context))
	fixtureAdmission755Context, fixtureAdmission755 := expressionTestRow(context.Background(), "NOT s = 'y'", values)
	_ = fixtureAdmission755
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission755, "NOT s = 'y'", fixtureAdmission755Context))
	// Undefined identifiers evaluate to Cypher null in the shared evaluator
	// (legacy clone returned the raw text, which broke IS NULL semantics).
	fixtureAdmission756Context, fixtureAdmission756 := expressionTestRow(context.Background(), "missing IS NULL", values)
	_ = fixtureAdmission756
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission756, "missing IS NULL", fixtureAdmission756Context))
	fixtureAdmission757Context, fixtureAdmission757 := expressionTestRow(context.Background(), "missing IS NOT NULL", values)
	_ = fixtureAdmission757
	require.False(t, exec.evaluateConditionFromValues(fixtureAdmission757, "missing IS NOT NULL", fixtureAdmission757Context))
	fixtureAdmission758Context, fixtureAdmission758 := expressionTestRow(context.Background(), "a = 3 AND b = 2", values)
	_ = fixtureAdmission758
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission758, "a = 3 AND b = 2", fixtureAdmission758Context))
	fixtureAdmission759Context, fixtureAdmission759 := expressionTestRow(context.Background(), "a = 9 OR b = 2", values)
	_ = fixtureAdmission759
	require.True(t, exec.evaluateConditionFromValues(fixtureAdmission759, "a = 9 OR b = 2", fixtureAdmission759Context))
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

	fixtureAdmission760Context, fixtureAdmission760 := expressionTestContext(context.Background(), "{}")
	_ = fixtureAdmission760
	require.Equal(t, map[string]interface{}{}, parsedTestValue(exec.evaluateMapLiteralFromValues(fixtureAdmission760, fixtureAdmission760Context, "{}", values)))
	fixtureAdmission761Context, fixtureAdmission761 := expressionTestContext(context.Background(), "{badpair}")
	_ = fixtureAdmission761
	_, parseErr := exec.evaluateMapLiteralFromValues(fixtureAdmission761, fixtureAdmission761Context, "{badpair}", values)
	require.Error(t, parseErr)

	fixtureAdmission762Context, fixtureAdmission762 := expressionTestContext(context.Background(), "{a: x, b: {inner: y}}")
	_ = fixtureAdmission762
	out := parsedTestValue(exec.evaluateMapLiteralFromValues(fixtureAdmission762, fixtureAdmission762Context, "{a: x, b: {inner: y}}", values))
	require.Equal(t, int64(1), out["a"])
	nested, ok := out["b"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, int64(2), nested["inner"])
}

func TestEvaluateExpressionFromValues_CoalesceParamContext(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_coalesce_cov"))
	ctx := withValueBindings(withParams(context.Background(), map[string]interface{}{"fallback": "z"}), map[string]interface{}{"missing": nil})
	fixtureAdmission763Context, fixtureAdmission763 := expressionTestContext(ctx, "coalesce(missing, $fallback)")
	_ = fixtureAdmission763
	require.Equal(t, "z", exec.evaluateExpressionWithContext(fixtureAdmission763, fixtureAdmission763Context, "coalesce(missing, $fallback)", nil, nil))
}
