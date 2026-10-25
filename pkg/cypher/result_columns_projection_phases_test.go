package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/statementfactstest"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A sibling input binds to the original definition; WHERE binds to the new
// output definition, independently of the order of the three projected items.
func TestResultHeaderProjectionPhaseBindings(t *testing.T) {
	checked := 0
	for _, sample := range statementfactstest.ProjectionPermutationCases() {
		if !strings.HasPrefix(sample.Name, "name0/aggregate=false/site6_where/empty=false/") {
			continue
		}
		t.Run(sample.Name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "projection-phase"))
			program := executor.projectedStatementScope(t.Context(), sample.Query, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
			require.True(t, program.complete, "%v", program.err)
			require.NotNil(t, program.bound)
			inputAt := strings.Index(sample.Query, "value+1")
			tailAt := strings.Index(sample.Query, "value=2")
			outerAt := strings.Index(sample.Query, "value")
			outputAt := strings.Index(sample.Query, "2 AS value") + len("2 AS ")
			foundInput, foundTail := false, false
			for _, use := range program.bound.uses {
				if use.name != "value" || use.start != inputAt && use.start != tailAt {
					continue
				}
				require.NotZero(t, use.binding)
				definition := program.bound.bindings[int(use.binding)-1]
				if use.start == inputAt {
					require.Equal(t, outerAt, definition.start)
					foundInput = true
				}
				if use.start == tailAt {
					require.Equal(t, outputAt, definition.start)
					foundTail = true
				}
			}
			require.True(t, foundInput, "projection input binding missing")
			require.True(t, foundTail, "projected predicate binding missing")
		})
		checked++
	}
	require.Equal(t, 6, checked)
}

// The scope view cannot invent a projection for another clause. Its output
// roles remain distinct from reads even when the decoded alias is empty.
func TestResultHeaderProjectionSemanticBoundary(t *testing.T) {
	clause, ok := canonicalQueryClause(t.Context(), "MATCH (n)")
	require.True(t, ok)
	require.False(t, projectionSemanticsForSyntax(clause).eachItem(func(projectionSemanticItem) {
		t.Fatal("a pattern was treated as a projection")
	}))
	clause, ok = canonicalQueryClause(t.Context(), "RETURN 2 AS ``")
	require.True(t, ok)
	outputs := 0
	clause.Walk(func(node cypherantlr.SyntaxNode) func() {
		if projectionOutputSymbol(node) {
			require.True(t, syntaxBindingDefinition(node))
			name, named := node.Name()
			require.True(t, named)
			require.Empty(t, name)
			outputs++
		}
		return nil
	})
	require.Equal(t, 1, outputs)
}

// Pagination is a post-projection phase, including when a sibling shadows the
// name it reads. Its binding must be the output definition, never the old one.
func TestResultHeaderProjectionWindowBindings(t *testing.T) {
	query := "WITH 7 AS value WITH 2 AS value, value+1 AS y ORDER BY value SKIP value-2 LIMIT value RETURN value, y"
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "projection-window"))
	program := executor.projectedStatementScope(t.Context(), query, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
	require.True(t, program.complete, "%v", program.err)
	require.NotNil(t, program.bound)
	outputAt := strings.Index(query, "2 AS value") + len("2 AS ")
	for _, prefix := range []string{"ORDER BY ", "SKIP ", "LIMIT "} {
		position := strings.Index(query, prefix) + len(prefix)
		found := false
		for _, use := range program.bound.uses {
			if use.start == position {
				require.Equal(t, "value", use.name)
				require.NotZero(t, use.binding)
				require.Equal(t, outputAt, program.bound.bindings[int(use.binding)-1].start)
				found = true
			}
		}
		require.True(t, found, "%s binding missing", prefix)
	}
	// Binding is recorded before admission rejects row-dependent pagination.
	// That policy must retain its canonical error, not lose the variable scope.
	_, err := executor.Execute(context.Background(), query, nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	require.ErrorContains(t, err, "independent of row variables")
	query = strings.Replace(query, "SKIP value-2 LIMIT value", "SKIP $skip LIMIT $limit", 1)
	result, err := executor.Execute(context.Background(), query, map[string]interface{}{"skip": int64(0), "limit": int64(2)})
	require.NoError(t, err)
	require.Equal(t, []string{"value", "y"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(2), int64(8)}}, result.Rows)
}

// A presentation adapter never repairs missing runtime columns or partial rows.
// A declined source or a stream/schema mismatch leaves the entire answer alone.
func TestResultHeaderPresentationBoundary(t *testing.T) {
	for _, source := range []string{"RETURN )", "1 + 2", "CREATE ()"} {
		t.Run(source, func(t *testing.T) {
			result := &ExecuteResult{Columns: []string{"original"}, Rows: [][]interface{}{{int64(7)}}}
			restoreProjectionPresentation(source, result)
			require.Equal(t, []string{"original"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
		})
	}
	for _, sample := range []struct {
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{"RETURN )", []string{"original"}, [][]interface{}{{int64(7)}}},
		{"RETURN *, 2 AS extra", []string{"value"}, [][]interface{}{{int64(7)}}},
		{"RETURN *, 2 AS extra", []string{"value", "extra"}, [][]interface{}{{int64(7)}}},
	} {
		t.Run(sample.query+strings.Join(sample.columns, ","), func(t *testing.T) {
			result := &ExecuteResult{Columns: append([]string(nil), sample.columns...), Rows: sample.rows}
			returnProjectionPlanFor(t.Context(), sample.query).restorePresentation(result)
			require.Equal(t, sample.columns, result.Columns)
			require.Equal(t, sample.rows, result.Rows)
		})
	}
}
