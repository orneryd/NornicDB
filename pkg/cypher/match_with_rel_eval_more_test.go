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

	require.Nil(t, exec.evaluateExpressionFromValues("", values))
	require.Equal(t, "alice", exec.evaluateExpressionFromValues("n.name", values))
	require.Equal(t, "T2", exec.evaluateExpressionFromValues("nodeMap.title", values))
	require.Equal(t, int64(2), exec.evaluateExpressionFromValues("length(path)", values))
	require.Equal(t, int64(3), exec.evaluateExpressionFromValues("size(xs)", values))
	require.Equal(t, int64(4), exec.evaluateExpressionFromValues("size(asText)", values))
	// size(null) is null: the legacy evaluator returned the length of the
	// literal word "missing" (text-fallthrough), which broke size(null)
	// semantics in computed rows.
	require.Nil(t, exec.evaluateExpressionFromValues("size(missing)", values))

	labels := exec.evaluateExpressionFromValues("labels(typedNode)", values).([]interface{})
	require.Equal(t, []interface{}{"A", "B"}, labels)
	// labels() of a map is a type error, as in Neo4j: a map is not a node.
	labelsCtx := withExpressionFailureSlot(context.Background())
	require.Nil(t, exec.evaluateExpressionFromValuesContext(labelsCtx, "labels(nodeMap)", values))
	require.Error(t, getExpressionFailure(labelsCtx))

	relTypes := exec.evaluateExpressionFromValues("[r IN relationships(path) | type(r)]", values).([]interface{})
	require.Equal(t, []interface{}{"KNOWS"}, relTypes)

	relList := exec.evaluateExpressionFromValues("relationships(path)", values).([]interface{})
	require.Len(t, relList, 1)
	rel := relList[0].(*storage.Edge)
	require.Equal(t, "KNOWS", rel.Type)
	require.Equal(t, "e1", string(rel.ID))

	nodeList := exec.evaluateExpressionFromValues("nodes(path)", values).([]interface{})
	require.Len(t, nodeList, 2)
	require.Equal(t, "n1", string(nodeList[0].(*storage.Node).ID))
	require.Equal(t, "bob", nodeList[1].(*storage.Node).Properties["name"])

	require.Equal(t, "4:match_with_rel_eval_cov:n1", exec.evaluateExpressionFromValues("elementId(n)", values))
	require.Equal(t, "map-id", exec.evaluateExpressionFromValues("id(typedNode)", values))

	mapLit := exec.evaluateExpressionFromValues("{name: n.name, cnt: size(xs)}", values).(map[string]interface{})
	require.Equal(t, "alice", mapLit["name"])
	require.Equal(t, int64(3), mapLit["cnt"])

	dt := exec.evaluateExpressionFromValues("datetime(rowTime)", values)
	dtDateTime, ok := dt.(CypherDateTime)
	require.True(t, ok, "expected CypherDateTime, got %T", dt)
	require.Equal(t, "2026-03-20T20:22:20Z", dtDateTime.Time.UTC().Format(time.RFC3339))
	require.Nil(t, exec.evaluateExpressionFromValues("datetime('not-a-time')", values))

	local, ok := exec.evaluateExpressionFromValues("localdatetime()", values).(CypherLocalDateTime)
	require.True(t, ok, "expected CypherLocalDateTime")
	require.False(t, local.Time.IsZero())

	coalesced := exec.evaluateExpressionFromValues("coalesce(missing, n.name)", values)
	require.Equal(t, "alice", coalesced)
}

func TestEvaluateCaseExpressionFromValues_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_case_cov"))
	values := map[string]interface{}{
		"k":      "v",
		"status": "open",
		"n":      int64(2),
	}

	require.Equal(t, "hit", exec.evaluateCaseExpressionFromValues("CASE k WHEN 'v' THEN 'hit' ELSE 'miss' END", values))
	require.Equal(t, "open", exec.evaluateCaseExpressionFromValues("CASE WHEN status = 'closed' THEN 'closed' ELSE 'open' END", values))
	require.Equal(t, int64(2), exec.evaluateCaseExpressionFromValues("CASE WHEN n > 1 THEN 2 ELSE 0 END", values))
	require.Nil(t, exec.evaluateCaseExpressionFromValues("CASE WHEN n < 0 THEN 2 END", values))
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

	require.True(t, exec.evaluateConditionFromValues("a >= b", values))
	require.True(t, exec.evaluateConditionFromValues("a > b", values))
	require.True(t, exec.evaluateConditionFromValues("b <= a", values))
	require.True(t, exec.evaluateConditionFromValues("b < a", values))
	require.True(t, exec.evaluateConditionFromValues("s <> 'y'", values))
	require.True(t, exec.evaluateConditionFromValues("NOT s = 'y'", values))
	// Undefined identifiers evaluate to Cypher null in the shared evaluator
	// (legacy clone returned the raw text, which broke IS NULL semantics).
	require.True(t, exec.evaluateConditionFromValues("missing IS NULL", values))
	require.False(t, exec.evaluateConditionFromValues("missing IS NOT NULL", values))
	require.True(t, exec.evaluateConditionFromValues("a = 3 AND b = 2", values))
	require.True(t, exec.evaluateConditionFromValues("a = 9 OR b = 2", values))
}

func BenchmarkEvaluateConditionFromValues(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "match_with_rel_cond_bench"))
	values := map[string]interface{}{"a": int64(3), "b": int64(2), "s": "x"}
	conditions := []string{"a >= b", "s <> 'y'", "a = 3 AND b = 2", "missing IS NULL"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.evaluateConditionFromValues(conditions[i%len(conditions)], values)
	}
}

func BenchmarkEvaluateCaseExpressionFromValues(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "match_with_rel_case_bench"))
	values := map[string]interface{}{"a": int64(3), "b": int64(2), "s": "x"}
	exprs := []string{
		"CASE a WHEN 3 THEN 'three' ELSE 'other' END",
		"CASE WHEN b > 1 THEN s ELSE 'low' END",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.evaluateCaseExpressionFromValues(exprs[i%len(exprs)], values)
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
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.evaluateExpressionFromValues(exprs[i%len(exprs)], values)
	}
}

func TestEvaluateMapLiteralFromValues_AdditionalBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_map_cov"))
	values := map[string]interface{}{"x": int64(1), "y": int64(2)}

	require.Equal(t, map[string]interface{}{}, exec.evaluateMapLiteralFromValues("{}", values))
	require.Equal(t, map[string]interface{}{}, exec.evaluateMapLiteralFromValues("{badpair}", values))

	out := exec.evaluateMapLiteralFromValues("{a: x, b: {inner: y}}", values)
	require.Equal(t, int64(1), out["a"])
	nested, ok := out["b"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, int64(2), nested["inner"])
}

func TestEvaluateExpressionFromValues_CoalesceParamContext(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_with_rel_coalesce_cov"))
	ctx := withParams(context.Background(), map[string]interface{}{"fallback": "z"})
	require.Equal(t, "z", exec.evaluateExpressionWithContext(ctx, "coalesce(missing, $fallback)", nil, nil))
}
