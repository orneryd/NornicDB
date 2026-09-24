package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// All cases below drive evaluateExpressionWithContextFullPropsLiterals
// directly, with carefully constructed `expr` / `lowerExpr` / context maps.
// The function is pure with respect to (expr, lowerExpr, nodes, rels,
// paths, allPathEdges, allPathNodes, pathLength), so each branch is
// reachable without invoking the parser.

func freshExecutorForPropsLiterals(t *testing.T) *StorageExecutor {
	t.Helper()
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "props-literals-coverage")
	return NewStorageExecutor(ns)
}

func TestEvaluateExpressionWithContextFullPropsLiterals_EveryBranch(t *testing.T) {
	e := freshExecutorForPropsLiterals(t)
	ctx := context.Background()

	// Common context shared across all subtests for clarity.
	person := &storage.Node{
		ID:         "u1",
		Properties: map[string]any{"name": "Alice", "age": int64(30)},
	}
	scalarBox := &storage.Node{
		ID:         "scalar",
		Properties: map[string]any{"value": int64(42)},
	}
	emptyNode := &storage.Node{ID: "empty", Properties: map[string]any{}}
	edge := &storage.Edge{
		ID:         "r1",
		Type:       "KNOWS",
		Properties: map[string]any{"since": int64(2024)},
	}
	pathRes := &PathResult{
		Length:        3,
		Nodes:         []*storage.Node{person, emptyNode},
		Relationships: []*storage.Edge{edge},
	}

	nodes := map[string]*storage.Node{"p": person, "scalar": scalarBox}
	rels := map[string]*storage.Edge{"r": edge}
	paths := map[string]*PathResult{"path": pathRes}

	call := func(expr string) interface{} {
		return e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, expr, strings.ToLower(expr),
			nodes, rels, paths, nil, nil, 0,
		)
	}

	// ====================== property access ======================
	t.Run("node property hit", func(t *testing.T) {
		require.Equal(t, "Alice", call("p.name"))
	})

	t.Run("node property miss returns nil", func(t *testing.T) {
		require.Nil(t, call("p.nonexistent"))
	})

	t.Run("nil node property access returns nil", func(t *testing.T) {
		nodesNil := map[string]*storage.Node{"p": nil}
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "p.name", "p.name", nodesNil, nil, nil, nil, nil, 0,
		))
	})

	t.Run("has_embedding falls back to ChunkEmbeddings length", func(t *testing.T) {
		nodesEmbed := map[string]*storage.Node{
			"p": {
				ID:              "p",
				ChunkEmbeddings: [][]float32{{0.1, 0.2}},
			},
		}
		got := e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "p.has_embedding", "p.has_embedding", nodesEmbed, nil, nil, nil, nil, 0,
		)
		require.Equal(t, true, got)
	})

	t.Run("has_embedding from EmbedMeta wins", func(t *testing.T) {
		nodesEmbed := map[string]*storage.Node{
			"p": {
				ID:        "p",
				EmbedMeta: map[string]interface{}{"has_embedding": false},
			},
		}
		got := e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "p.has_embedding", "p.has_embedding", nodesEmbed, nil, nil, nil, nil, 0,
		)
		require.Equal(t, false, got, "EmbedMeta override beats ChunkEmbeddings inspection")
	})

	t.Run("edge property hit", func(t *testing.T) {
		require.Equal(t, int64(2024), call("r.since"))
	})

	t.Run("edge property miss returns nil", func(t *testing.T) {
		require.Nil(t, call("r.unknown"))
	})

	t.Run("nil edge property access returns nil", func(t *testing.T) {
		relsNil := map[string]*storage.Edge{"r": nil}
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "r.since", "r.since", nil, relsNil, nil, nil, nil, 0,
		))
	})

	// ====================== variable reference ======================
	t.Run("variable refers to whole node", func(t *testing.T) {
		require.Equal(t, person, call("p"))
	})

	t.Run("a node whose only property is value stays a node", func(t *testing.T) {
		require.Equal(t, scalarBox, call("scalar"))
	})

	t.Run("a value-scope variable returns its value", func(t *testing.T) {
		valueCtx := withValueBindings(ctx, map[string]interface{}{"v": int64(42), "m": map[string]interface{}{"a": int64(1)}})
		require.Equal(t, int64(42), e.evaluateExpressionWithContextFullPropsLiterals(valueCtx, "v", "v", nodes, rels, paths, nil, nil, 0))
		require.Equal(t, int64(1), e.evaluateExpressionWithContextFullPropsLiterals(valueCtx, "m.a", "m.a", nodes, rels, paths, nil, nil, 0))
	})

	t.Run("nil node variable returns nil", func(t *testing.T) {
		nodesNil := map[string]*storage.Node{"p": nil}
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "p", "p", nodesNil, nil, nil, nil, nil, 0,
		))
	})

	t.Run("variable refers to whole edge", func(t *testing.T) {
		require.Equal(t, edge, call("r"))
	})

	t.Run("nil edge variable returns nil", func(t *testing.T) {
		relsNil := map[string]*storage.Edge{"r": nil}
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "r", "r", nil, relsNil, nil, nil, nil, 0,
		))
	})

	t.Run("path variable returns serializable map", func(t *testing.T) {
		got := call("path")
		require.IsType(t, map[string]interface{}{}, got)
		m := got.(map[string]interface{})
		require.Equal(t, pathRes, m["_pathResult"])
		require.Equal(t, 3, m["length"])
		require.Equal(t, []*storage.Node{person, emptyNode}, m["nodes"])
		require.Equal(t, []*storage.Edge{edge}, m["rels"])
	})

	// ====================== literals ======================
	literalCases := []struct {
		name string
		expr string
		want interface{}
	}{
		{"null literal", "null", nil},
		{"NULL upper", "NULL", nil},
		{"true literal", "true", true},
		{"TRUE upper", "TRUE", true},
		{"false literal", "false", false},
		{"single-quoted string", "'hello'", "hello"},
		{"double-quoted string", `"world"`, "world"},
		{"integer literal", "42", int64(42)},
		{"negative integer", "-5", int64(-5)},
		{"float literal", "1.5", float64(1.5)},
	}
	for _, tc := range literalCases {
		t.Run("literal/"+tc.name, func(t *testing.T) {
			got := e.evaluateExpressionWithContextFullPropsLiterals(
				ctx, tc.expr, strings.ToLower(tc.expr),
				nil, nil, nil, nil, nil, 0,
			)
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("array literal parsed via parseArrayValue", func(t *testing.T) {
		got := e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "[1, 2, 3]", "[1, 2, 3]",
			nil, nil, nil, nil, nil, 0,
		)
		require.NotNil(t, got)
	})

	t.Run("map literal parsed via parseProperties", func(t *testing.T) {
		got := e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "{k: 'v'}", "{k: 'v'}",
			nil, nil, nil, nil, nil, 0,
		)
		require.NotNil(t, got)
		m, ok := got.(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, "v", m["k"])
	})

	// ====================== fallback paths ======================
	t.Run("unresolved bare identifier returns nil", func(t *testing.T) {
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "unknown", "unknown",
			nil, nil, nil, nil, nil, 0,
		))
	})

	t.Run("aggregation function in expr context returns nil", func(t *testing.T) {
		for _, fn := range []string{"count(*)", "sum(x)", "avg(x)", "min(x)", "max(x)", "collect(x)"} {
			got := e.evaluateExpressionWithContextFullPropsLiterals(
				ctx, fn, strings.ToLower(fn),
				nil, nil, nil, nil, nil, 0,
			)
			require.Nil(t, got, "aggregation %s must return nil in expression context", fn)
		}
	})

	t.Run("unknown string falls through as itself", func(t *testing.T) {
		got := e.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "?? weirdsymbols", "?? weirdsymbols",
			nil, nil, nil, nil, nil, 0,
		)
		require.Equal(t, "?? weirdsymbols", got)
	})

	// ====================== fabric record bindings ======================
	t.Run("fabric record binding map property", func(t *testing.T) {
		e2 := freshExecutorForPropsLiterals(t)
		e2.fabricRecordBindings = map[string]interface{}{
			"row": map[string]interface{}{"name": "Bob"},
		}
		got := e2.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "row.name", "row.name", nil, nil, nil, nil, nil, 0,
		)
		require.Equal(t, "Bob", got)
	})

	t.Run("fabric record binding map missing property returns nil", func(t *testing.T) {
		e2 := freshExecutorForPropsLiterals(t)
		e2.fabricRecordBindings = map[string]interface{}{
			"row": map[string]interface{}{"name": "Bob"},
		}
		got := e2.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "row.missing", "row.missing", nil, nil, nil, nil, nil, 0,
		)
		require.Nil(t, got)
	})

	t.Run("fabric record binding Node property", func(t *testing.T) {
		e2 := freshExecutorForPropsLiterals(t)
		e2.fabricRecordBindings = map[string]interface{}{
			"row": &storage.Node{Properties: map[string]any{"name": "Carol"}},
		}
		got := e2.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "row.name", "row.name", nil, nil, nil, nil, nil, 0,
		)
		require.Equal(t, "Carol", got)
	})

	t.Run("fabric record binding whole-row returns the binding value", func(t *testing.T) {
		e2 := freshExecutorForPropsLiterals(t)
		bound := map[string]interface{}{"k": "v"}
		e2.fabricRecordBindings = map[string]interface{}{"row": bound}
		got := e2.evaluateExpressionWithContextFullPropsLiterals(
			ctx, "row", "row", nil, nil, nil, nil, nil, 0,
		)
		require.Equal(t, bound, got)
	})
}
