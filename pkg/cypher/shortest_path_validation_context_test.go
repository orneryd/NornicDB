package cypher

import (
	"context"
	"strings"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// validateShortestCallBindings reads a shortestPath call's pattern with the
// caller's lifetime (parsePathPatternItemsContext). A canceled read must end
// the statement with the caller's own error instead of reading as "not a
// pattern", and no canceled verdict may be cached for the text: the same
// statement must run healthy afterwards.
//
// Two production chains reach the owner, both through the public Execute and
// under both parsers: the MATCH semantic walk and the label-expression
// rewrite's repeatedRelationshipVariables. The rewrite chain runs first for
// these queries, so the semantic chain is selected by excluding the rewrite's
// frame from the cancellation schedule. The scheduled action is the caller's
// real CancelFunc; no fake error, product hook or replacement parser is used.
func TestShortestPathValidationUsesCallerContext(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		chains := []struct {
			name     string
			query    string
			exclude  string
			evidence []string
		}{
			{
				name:     "semantic walk",
				query:    "MATCH p = shortestPath((a)-[r:T*]->(b)) RETURN p",
				exclude:  ".repeatedRelationshipVariables",
				evidence: []string{".validateMatchClauseBindings", ".semanticScopeWalk", ".parsePathPatternItemsContext"},
			},
			{
				name:     "label rewrite",
				query:    "MATCH p = shortestPath((a:A|B)-[r:T]->(b)) RETURN p",
				exclude:  "",
				evidence: []string{".desugarLabelExpressions", ".repeatedRelationshipVariables", ".parsePathPatternItemsContext"},
			},
		}
		for _, chain := range chains {
			t.Run(chain.name, func(t *testing.T) {
				// The control runs on its own executor: a healthy statement's
				// cached validation must not hide the cancellation schedule.
				control, err := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "shortestctx-control")).Execute(t.Context(), chain.query, nil)
				require.NoError(t, err)
				require.Equal(t, []string{"p"}, control.Columns)
				require.Empty(t, control.Rows)

				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "shortestctx"))
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx := &nor293PlanningContext{Context: parent, action: cancel, targetFunction: ".parsePathPatternItemsContext", excludeFunction: chain.exclude, afterChecks: 1}
				result, err := exec.Execute(ctx, chain.query, nil)
				require.ErrorIs(t, err, context.Canceled, "a canceled shortestPath read must end with the caller's own error")
				require.Nil(t, result)
				require.ErrorIs(t, parent.Err(), context.Canceled, "the standard parent context must really be canceled")
				require.NotEmpty(t, ctx.frames, "public execution did not reach the scheduled pattern read")
				require.Len(t, ctx.targetLines, 1, "the canceled read stops at its first lifetime check")
				for _, frame := range chain.evidence {
					require.True(t, hasCallerFrame(ctx.frames, frame), "missing %s in %v", frame, ctx.frames)
				}

				retry, err := exec.Execute(t.Context(), chain.query, nil)
				require.NoError(t, err, "a canceled read must not be cached as the statement's verdict")
				require.Equal(t, control.Columns, retry.Columns)
				require.Equal(t, control.Rows, retry.Rows)
			})
		}
	})
}

// The same public statement keeps its healthy success and error identity under
// both parsers: the caller-context change did not alter what
// validateShortestCallBindings reports for a valid call, a common-endpoint call
// or a call that reuses a bound relationship.
func TestShortestPathBindingControlsStayHealthy(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "shortestcontrols"))
		_, err := exec.Execute(t.Context(), "CREATE (:SP {id:1})-[:T]->(:SP {id:2})-[:T]->(:SP {id:3})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(t.Context(), "MATCH (a:SP {id:1}), (b:SP {id:3}) MATCH p = shortestPath((a)-[r:T*]->(b)) RETURN length(p)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)

		_, err = exec.Execute(t.Context(), "MATCH p = shortestPath((a)-[r:T*0..]->(a)) RETURN p", nil)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.DatabaseError.Statement.ExecutionFailed", code)

		_, err = exec.Execute(t.Context(), "MATCH (a:SP {id:1})-[r:T*]->(b) WITH a,b,r MATCH p = shortestPath((a)-[r:T*]->(b)) RETURN p", nil)
		code, _ = nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
		require.Contains(t, err.Error(), "Bound relationships not allowed in shortestPath(...)")
	})
}

// A canceled context is a caller error, not a pattern verdict: the direct owner
// reports the cancellation while the same text stays valid for a live caller.
func TestValidateShortestCallBindingsCancellationIsNotAPatternVerdict(t *testing.T) {
	const pattern = "MATCH p = shortestPath((a)-[r:T]->(b)) RETURN p"
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, validateShortestCallBindings(canceled, pattern, nil), context.Canceled)
	require.NoError(t, validateShortestCallBindings(t.Context(), pattern, nil))
}

// The context argument adds no allocation to a statement the label-expression
// rewrite leaves alone: the quick checks return before a rewriter is built.
func TestLabelExpressionRewriteNoEditKeepsZeroAllocations(t *testing.T) {
	ctx := t.Context()
	const query = "RETURN 1 AS value"
	allocations := testing.AllocsPerRun(100, func() {
		if _, rewrite, err := desugarLabelExpressions(ctx, query, nil, false); err != nil || rewrite != nil {
			t.Fatalf("unexpected rewrite: %v", err)
		}
	})
	require.Zero(t, allocations)
}

func hasCallerFrame(frames []string, suffix string) bool {
	for _, frame := range frames {
		if strings.HasSuffix(frame, suffix) {
			return true
		}
	}
	return false
}
