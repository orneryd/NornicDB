// Unit tests for the WITH-attached WHERE predicate helpers. These cover the
// branches the end-to-end regression tests in with_where_filter_bugs_test.go
// cannot reach directly: malformed input, non-node bindings, quoting, and
// concurrent evaluation.

package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithWhereIsLabelTest(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{"simple label test", "n:Workload", true},
		{"conjunction of labels", "n:A:B", true},
		{"underscore and digits", "n_1:Label_2", true},
		{"surrounding whitespace", "  n:Workload  ", true},
		{"no colon at all", "n.name", false},
		{"comparison operator present", "n:Workload = 1", false},
		{"single quote present", "n:'Workload'", false},
		{"double quote present", "n:\"Workload\"", false},
		{"empty segment after colon", "n:", false},
		{"empty segment before colon", ":Workload", false},
		{"double colon leaves an empty segment", "n::Workload", false},
		{"property access is not a label test", "n:a.b", false},
		{"empty string", "", false},
		{"whitespace only", "   ", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, withWhereIsLabelTest(tc.input))
		})
	}
}

func TestParseWithWhereLabelTest(t *testing.T) {
	t.Run("single label", func(t *testing.T) {
		variable, labels, ok := parseWithWhereLabelTest("n:Workload")
		require.True(t, ok)
		assert.Equal(t, "n", variable)
		assert.Equal(t, []string{"Workload"}, labels)
	})

	t.Run("conjunction keeps every label", func(t *testing.T) {
		variable, labels, ok := parseWithWhereLabelTest("node:A:B:C")
		require.True(t, ok)
		assert.Equal(t, "node", variable)
		assert.Equal(t, []string{"A", "B", "C"}, labels)
	})

	t.Run("rejects a non-label predicate", func(t *testing.T) {
		_, _, ok := parseWithWhereLabelTest("n.name = 'x'")
		assert.False(t, ok)
	})

	t.Run("rejects empty input", func(t *testing.T) {
		_, _, ok := parseWithWhereLabelTest("")
		assert.False(t, ok)
	})
}

func TestWithWhereNodeHasAllLabels(t *testing.T) {
	node := &storage.Node{Labels: []string{"Workload", "Deployable"}}

	cases := []struct {
		name     string
		value    interface{}
		required []string
		want     bool
	}{
		{"single label present", node, []string{"Workload"}, true},
		{"every label present", node, []string{"Workload", "Deployable"}, true},
		{"order does not matter", node, []string{"Deployable", "Workload"}, true},
		{"one label missing fails the conjunction", node, []string{"Workload", "Absent"}, false},
		{"label absent", node, []string{"Absent"}, false},
		{"no labels required is vacuously true", node, nil, true},
		{"nil node cannot satisfy a label test", (*storage.Node)(nil), []string{"Workload"}, false},
		{"missing binding cannot satisfy a label test", nil, []string{"Workload"}, false},
		{"scalar binding is not a node", int64(7), []string{"Workload"}, false},
		{"edge binding is not a node", &storage.Edge{Type: "RUNS_IN"}, []string{"Workload"}, false},
		{"node with no labels", &storage.Node{}, []string{"Workload"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, withWhereNodeHasAllLabels(tc.value, tc.required))
		})
	}
}

func TestWithWhereNeedsFullEvaluator(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{"function call", "toUpper(a.name) = 'X'", true},
		{"conjunction", "a.x = 1 AND a.y = 2", true},
		{"disjunction", "a.x = 1 OR a.y = 2", true},
		{"exclusive or", "a.x = 1 XOR a.y = 2", true},
		{"negation", "NOT a.x = 1", true},
		{"lower-case boolean operator", "a.x = 1 and a.y = 2", true},
		{"bare label test", "n:Workload", true},
		{"plain comparison needs no help", "a.name = 'x'", false},
		{"plain null check needs no help", "a.name IS NULL", false},
		{"empty predicate", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, withWhereNeedsFullEvaluator(tc.input))
		})
	}
}

func TestSubstituteWithWhereLabelTests(t *testing.T) {
	workload := &storage.Node{Labels: []string{"Workload"}}
	values := map[string]interface{}{
		"n":      workload,
		"scalar": int64(3),
	}

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"matching label becomes true", "n:Workload", "true"},
		{"non-matching label becomes false", "n:Absent", "false"},
		{"disjunction of both", "n:Workload OR n:Absent", "true OR false"},
		{"conjunction of labels on one variable", "n:Workload:Absent", "false"},
		{"unknown variable is false", "missing:Workload", "false"},
		{"non-node binding is false", "scalar:Workload", "false"},
		{"predicate with no label test is unchanged", "n.name = 'x'", "n.name = 'x'"},
		{"colon inside a single-quoted string is left alone", "n.name = 'a:b'", "n.name = 'a:b'"},
		{"colon inside a double-quoted string is left alone", "n.name = \"a:b\"", "n.name = \"a:b\""},
		{"empty predicate", "", ""},
		{"trailing colon is not a label test", "n:", "n:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, substituteWithWhereLabelTests(tc.input, values))
		})
	}
}

func TestWithWhereValueContext(t *testing.T) {
	node := &storage.Node{Labels: []string{"Workload"}}
	edge := &storage.Edge{Type: "RUNS_IN"}

	t.Run("splits nodes and edges and drops scalars", func(t *testing.T) {
		nodes, edges := withWhereValueContext(map[string]interface{}{
			"n":      node,
			"rel":    edge,
			"count":  int64(3),
			"name":   "checkout",
			"nilVal": nil,
		})
		assert.Equal(t, map[string]*storage.Node{"n": node}, nodes)
		assert.Equal(t, map[string]*storage.Edge{"rel": edge}, edges)
	})

	t.Run("empty input yields empty non-nil maps", func(t *testing.T) {
		nodes, edges := withWhereValueContext(map[string]interface{}{})
		assert.NotNil(t, nodes)
		assert.NotNil(t, edges)
		assert.Empty(t, nodes)
		assert.Empty(t, edges)
	})

	t.Run("nil input is safe", func(t *testing.T) {
		nodes, edges := withWhereValueContext(nil)
		assert.Empty(t, nodes)
		assert.Empty(t, edges)
	})
}

// Concurrent evaluation must be safe: the claim path evaluates these predicates
// from several workers at once, and the helpers read shared bound values.
func TestEvaluateWithWhereConditionConcurrent(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	values := map[string]interface{}{
		"n":   &storage.Node{Labels: []string{"Workload"}, Properties: map[string]interface{}{"name": "checkout"}},
		"num": int64(10),
	}

	predicates := []struct {
		clause string
		want   bool
	}{
		{"n:Workload", true},
		{"n:Absent", false},
		{"n:Workload OR n:Absent", true},
		{"num >= 10", true},
		{"num > 11", false},
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tc := predicates[i%len(predicates)]
			got := exec.evaluateWithWhereCondition(ctx, tc.clause, values)
			assert.Equal(t, tc.want, got, "predicate %q under concurrency", tc.clause)
		}(i)
	}
	wg.Wait()
}

// The end-to-end path must also hold under concurrent execution, not just the
// helpers in isolation.
func TestWithAttachedWhereConcurrentQueries(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	setupWithWhereFixture(t, store)

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := exec.Execute(ctx, `MATCH (n) WITH n WHERE n:Workload RETURN count(*) AS c`, nil)
			if err != nil {
				errs <- err
				return
			}
			if len(result.Rows) != 1 {
				errs <- fmt.Errorf("got %d rows, want 1", len(result.Rows))
				return
			}
			if fmt.Sprintf("%v", result.Rows[0][0]) != "1" {
				errs <- fmt.Errorf("got count %v, want 1", result.Rows[0][0])
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent query failed: %v", err)
	}
}

// The operand resolver reads properties off bound nodes and edges, and must
// treat a nil binding as an absent value rather than dereferencing it.
func TestEvaluateWithWhereConditionResolvesBoundProperties(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	node := &storage.Node{Labels: []string{"Workload"}, Properties: map[string]interface{}{"name": "checkout"}}
	edge := &storage.Edge{Type: "RUNS_IN", Properties: map[string]interface{}{"reason": "declared"}}

	cases := []struct {
		name   string
		clause string
		values map[string]interface{}
		want   bool
	}{
		{
			name:   "node property matches",
			clause: "n.name = 'checkout'",
			values: map[string]interface{}{"n": node},
			want:   true,
		},
		{
			name:   "node property does not match",
			clause: "n.name = 'other'",
			values: map[string]interface{}{"n": node},
			want:   false,
		},
		{
			name:   "edge property matches",
			clause: "rel.reason = 'declared'",
			values: map[string]interface{}{"rel": edge},
			want:   true,
		},
		{
			name:   "edge property does not match",
			clause: "rel.reason = 'inferred'",
			values: map[string]interface{}{"rel": edge},
			want:   false,
		},
		{
			name:   "nil node binding is treated as absent, not dereferenced",
			clause: "n.name IS NULL",
			values: map[string]interface{}{"n": (*storage.Node)(nil)},
			want:   true,
		},
		{
			name:   "nil edge binding is treated as absent, not dereferenced",
			clause: "rel.reason IS NULL",
			values: map[string]interface{}{"rel": (*storage.Edge)(nil)},
			want:   true,
		},
		{
			name:   "property absent from a present node",
			clause: "n.missing IS NULL",
			values: map[string]interface{}{"n": node},
			want:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, exec.evaluateWithWhereCondition(ctx, tc.clause, tc.values))
		})
	}
}
