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
	base := newTestMemoryEngine(t)
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
		fixtureAdmission209Context, fixtureAdmission209 := expressionTestContext(ctx, expr)
		_ = fixtureAdmission209
		return e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission209,
			fixtureAdmission209Context, expr, strings.ToLower(expr),
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
		fixtureAdmission210Context, fixtureAdmission210 := expressionTestContext(ctx, "p.name")
		_ = fixtureAdmission210
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission210,
			fixtureAdmission210Context, "p.name", "p.name", nodesNil, nil, nil, nil, nil, 0,
		))
	})

	t.Run("has_embedding falls back to ChunkEmbeddings length", func(t *testing.T) {
		nodesEmbed := map[string]*storage.Node{
			"p": {
				ID:              "p",
				ChunkEmbeddings: [][]float32{{0.1, 0.2}},
			},
		}
		fixtureAdmission211Context, fixtureAdmission211 := expressionTestContext(ctx, "p.has_embedding")
		_ = fixtureAdmission211
		got := e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission211,
			fixtureAdmission211Context, "p.has_embedding", "p.has_embedding", nodesEmbed, nil, nil, nil, nil, 0,
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
		fixtureAdmission212Context, fixtureAdmission212 := expressionTestContext(ctx, "p.has_embedding")
		_ = fixtureAdmission212
		got := e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission212,
			fixtureAdmission212Context, "p.has_embedding", "p.has_embedding", nodesEmbed, nil, nil, nil, nil, 0,
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
		fixtureAdmission213Context, fixtureAdmission213 := expressionTestContext(ctx, "r.since")
		_ = fixtureAdmission213
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission213,
			fixtureAdmission213Context, "r.since", "r.since", nil, relsNil, nil, nil, nil, 0,
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
		fixtureAdmission214Context, fixtureAdmission214 := expressionTestContext(valueCtx, "v")
		_ = fixtureAdmission214
		require.Equal(t, int64(42), e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission214, fixtureAdmission214Context, "v", "v", nodes, rels, paths, nil, nil, 0))
		fixtureAdmission215Context, fixtureAdmission215 := expressionTestContext(valueCtx, "m.a")
		_ = fixtureAdmission215
		require.Equal(t, int64(1), e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission215, fixtureAdmission215Context, "m.a", "m.a", nodes, rels, paths, nil, nil, 0))
	})

	t.Run("nil node variable returns nil", func(t *testing.T) {
		nodesNil := map[string]*storage.Node{"p": nil}
		fixtureAdmission216Context, fixtureAdmission216 := expressionTestContext(ctx, "p")
		_ = fixtureAdmission216
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission216,
			fixtureAdmission216Context, "p", "p", nodesNil, nil, nil, nil, nil, 0,
		))
	})

	t.Run("variable refers to whole edge", func(t *testing.T) {
		require.Equal(t, edge, call("r"))
	})

	t.Run("nil edge variable returns nil", func(t *testing.T) {
		relsNil := map[string]*storage.Edge{"r": nil}
		fixtureAdmission217Context, fixtureAdmission217 := expressionTestContext(ctx, "r")
		_ = fixtureAdmission217
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission217,
			fixtureAdmission217Context, "r", "r", nil, relsNil, nil, nil, nil, 0,
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
			fixtureAdmission218Context, fixtureAdmission218 := expressionTestContext(ctx, tc.expr)
			_ = fixtureAdmission218
			got := e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission218,
				fixtureAdmission218Context, tc.expr, strings.ToLower(tc.expr),
				nil, nil, nil, nil, nil, 0,
			)
			require.Equal(t, tc.want, got)
		})
	}

	t.Run("array literal parsed via parseArrayValue", func(t *testing.T) {
		fixtureAdmission219Context, fixtureAdmission219 := expressionTestContext(ctx, "[1, 2, 3]")
		_ = fixtureAdmission219
		got := e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission219,
			fixtureAdmission219Context, "[1, 2, 3]", "[1, 2, 3]",
			nil, nil, nil, nil, nil, 0,
		)
		require.NotNil(t, got)
	})

	t.Run("map literal parsed via parseProperties", func(t *testing.T) {
		fixtureAdmission220Context, fixtureAdmission220 := expressionTestContext(ctx, "{k: 'v'}")
		_ = fixtureAdmission220
		got := e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission220,
			fixtureAdmission220Context, "{k: 'v'}", "{k: 'v'}",
			nil, nil, nil, nil, nil, 0,
		)
		require.NotNil(t, got)
		m, ok := got.(map[string]interface{})
		require.True(t, ok)
		require.Equal(t, "v", m["k"])
	})

	// ====================== fallback paths ======================
	t.Run("unresolved bare identifier returns nil", func(t *testing.T) {
		fixtureAdmission221Context, fixtureAdmission221 := expressionTestContext(ctx, "unknown")
		_ = fixtureAdmission221
		require.Nil(t, e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission221,
			fixtureAdmission221Context, "unknown", "unknown",
			nil, nil, nil, nil, nil, 0,
		))
	})

	t.Run("aggregation function in expr context returns nil", func(t *testing.T) {
		for _, fn := range []string{"count(*)", "sum(x)", "avg(x)", "min(x)", "max(x)", "collect(x)"} {
			fixtureAdmission222Context, fixtureAdmission222 := expressionTestContext(ctx, fn)
			_ = fixtureAdmission222
			got := e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission222,
				fixtureAdmission222Context, fn, strings.ToLower(fn),
				nil, nil, nil, nil, nil, 0,
			)
			require.Nil(t, got, "aggregation %s must return nil in expression context", fn)
		}
	})

	t.Run("unknown expression records an error", func(t *testing.T) {
		ctx := withExpressionFailureSlot(context.Background())
		fixtureAdmission223Context, fixtureAdmission223 := expressionTestContext(ctx, "?? weirdsymbols")
		_ = fixtureAdmission223
		got := e.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission223,
			fixtureAdmission223Context, "?? weirdsymbols", "?? weirdsymbols",
			nil, nil, nil, nil, nil, 0,
		)
		require.Nil(t, got)
		require.Error(t, getExpressionFailure(ctx))
	})

	// ====================== fabric record bindings ======================
	t.Run("fabric record binding map property", func(t *testing.T) {
		e2 := freshExecutorForPropsLiterals(t)
		e2.fabricRecordBindings = map[string]interface{}{
			"row": map[string]interface{}{"name": "Bob"},
		}
		fixtureAdmission224Context, fixtureAdmission224 := expressionTestContext(ctx, "row.name")
		_ = fixtureAdmission224
		got := e2.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission224,
			fixtureAdmission224Context, "row.name", "row.name", nil, nil, nil, nil, nil, 0,
		)
		require.Equal(t, "Bob", got)
	})

	t.Run("fabric record binding map missing property returns nil", func(t *testing.T) {
		e2 := freshExecutorForPropsLiterals(t)
		e2.fabricRecordBindings = map[string]interface{}{
			"row": map[string]interface{}{"name": "Bob"},
		}
		fixtureAdmission225Context, fixtureAdmission225 := expressionTestContext(ctx, "row.missing")
		_ = fixtureAdmission225
		got := e2.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission225,
			fixtureAdmission225Context, "row.missing", "row.missing", nil, nil, nil, nil, nil, 0,
		)
		require.Nil(t, got)
	})

	t.Run("fabric record binding Node property", func(t *testing.T) {
		e2 := freshExecutorForPropsLiterals(t)
		e2.fabricRecordBindings = map[string]interface{}{
			"row": &storage.Node{Properties: map[string]any{"name": "Carol"}},
		}
		fixtureAdmission226Context, fixtureAdmission226 := expressionTestContext(ctx, "row.name")
		_ = fixtureAdmission226
		got := e2.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission226,
			fixtureAdmission226Context, "row.name", "row.name", nil, nil, nil, nil, nil, 0,
		)
		require.Equal(t, "Carol", got)
	})

	t.Run("fabric record binding whole-row returns the binding value", func(t *testing.T) {
		e2 := freshExecutorForPropsLiterals(t)
		bound := map[string]interface{}{"k": "v"}
		e2.fabricRecordBindings = map[string]interface{}{"row": bound}
		fixtureAdmission227Context, fixtureAdmission227 := expressionTestContext(ctx, "row")
		_ = fixtureAdmission227
		got := e2.evaluateExpressionWithContextFullPropsLiterals(fixtureAdmission227,
			fixtureAdmission227Context, "row", "row", nil, nil, nil, nil, nil, 0,
		)
		require.Equal(t, bound, got)
	})
}
