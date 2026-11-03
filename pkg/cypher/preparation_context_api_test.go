package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The existing phase probe schedules a real CancelFunc inside the canonical
// tokenizer, after a prefix has been visited. Done/Deadline/Err remain standard.
func TestParserContextCancellationInsideTokenization(t *testing.T) {
	for _, input := range []struct{ name, query string }{
		{"numeric", "RETURN " + strings.Repeat("1, ", 1024) + "1"},
		{"quoted", "RETURN " + strings.Repeat("'x',", 1024) + "'x'"},
		{"identifier", "RETURN " + strings.Repeat("longname", 1024)},
	} {
		t.Run(input.name, func(t *testing.T) {
			for _, entry := range []string{"Parse", "ParseAndValidate"} {
				t.Run(entry, func(t *testing.T) {
					parent, cancel := context.WithCancel(t.Context())
					defer cancel()
					ctx := &nor293PlanningContext{Context: parent, action: cancel, targetFunction: ".tokenizeSource", excludeFunction: ".ScanQuotedTextContext", afterChecks: 3}
					if entry == "Parse" {
						result, err := NewParser().Parse(ctx, input.query)
						require.ErrorIs(t, err, context.Canceled)
						require.Nil(t, result)
						healthy, err := NewParser().Parse(t.Context(), "RETURN 1 AS value")
						require.NoError(t, err)
						require.NotNil(t, healthy)
					} else {
						executor := NewExecutor()
						result, err := executor.ParseAndValidate(ctx, input.query, nil)
						require.ErrorIs(t, err, context.Canceled)
						require.Nil(t, result)
						healthy, err := executor.ParseAndValidate(t.Context(), "RETURN 1 AS value", nil)
						require.NoError(t, err)
						require.NotNil(t, healthy)
					}
					require.GreaterOrEqual(t, ctx.targetChecks.Load(), int64(3))
					require.Len(t, ctx.targetLines, 3)
					require.Equal(t, ctx.targetLines[1], ctx.targetLines[2], "cancel at the second repeated loop poll, not the final result check")
					require.NotEqual(t, ctx.targetLines[0], ctx.targetLines[1], "initial admission is a different source check")
					require.ErrorIs(t, parent.Err(), context.Canceled)
				})
			}
		})
	}
}

func TestParserCancellationInsideOneQuotedToken(t *testing.T) {
	for _, query := range []string{"RETURN '" + strings.Repeat("x", 8192) + "'", "RETURN `" + strings.Repeat("name", 2048) + "`"} {
		for _, entry := range []string{"Parse", "ParseAndValidate"} {
			t.Run(entry+"/"+query[:8], func(t *testing.T) {
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx := &nor293PlanningContext{Context: parent, action: cancel, targetFunction: ".scanQuotedText", afterChecks: 3}
				if entry == "Parse" {
					result, err := NewParser().Parse(ctx, query)
					require.ErrorIs(t, err, context.Canceled)
					require.Nil(t, result)
				} else {
					result, err := NewExecutor().ParseAndValidate(ctx, query, nil)
					require.ErrorIs(t, err, context.Canceled)
					require.Nil(t, result)
				}
				require.Len(t, ctx.targetLines, 3)
				require.Equal(t, ctx.targetLines[1], ctx.targetLines[2], "cancellation occurs after an owned inner scan chunk, not at result construction")
				require.Equal(t, ctx.targetLines[0], ctx.targetLines[1], "three repeated chunk checks prove two owned chunks were processed")
				require.ErrorIs(t, parent.Err(), context.Canceled)
				reachedTokenizer := false
				for _, frame := range ctx.frames {
					reachedTokenizer = reachedTokenizer || strings.HasSuffix(frame, ".tokenizeSource")
				}
				require.True(t, reachedTokenizer, "public parser's canonical tokenizer must own this quoted scan")
			})
		}
	}
}

func TestPublicAliasTokenizationCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		manager := newCompositeNativeManager(t, nil)
		defer manager.Close()
		store, err := manager.GetStorage("nornic")
		require.NoError(t, err)
		executor := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
		executor.dbManager = &testDatabaseManagerAdapter{manager}
		query := "DROP ALIAS absent" + strings.Repeat(" ", 1024) + "IF EXISTS"
		healthy, err := executor.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.NotNil(t, healthy)
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctx := &nor293PlanningContext{Context: parent, action: cancel, targetFunction: ".tokenizeSource", afterChecks: 2}
		result, err := executor.Execute(ctx, query, nil)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		var status interface{ BoltErrorCode() string }
		if errors.As(err, &status) {
			require.NotEqual(t, "Neo.ClientError.Statement.SyntaxError", status.BoltErrorCode(), "canceled tokenization is not invalid alias syntax")
		}
		require.GreaterOrEqual(t, ctx.targetChecks.Load(), int64(2))
		healthy, err = executor.Execute(t.Context(), query, nil)
		require.NoError(t, err)
		require.NotNil(t, healthy)
	})
}

func TestRetryClassifiersCancelWithoutChangingCommitCause(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	cancel()
	info := analyzeQuery("MERGE (n:Unique {id: 1}) RETURN n")
	cause := &storage.ConstraintViolationError{Type: storage.ConstraintUnique, Label: "Unique", Properties: []string{"id"}}
	statements := []CommitStatement{{Query: "MERGE (n:Unique {id: 1}) RETURN n"}}
	require.True(t, IsRetrySafeMergeCommitQuery(t.Context(), info))
	require.False(t, IsRetrySafeMergeCommitQuery(parent, info))
	require.True(t, MergeUniqueConflictIsRetrySafe(t.Context(), statements, cause))
	require.False(t, MergeUniqueConflictIsRetrySafe(parent, statements, cause))
	require.Equal(t, "Unique", cause.Label)

}

// Credential capture is defensive cleanup: cancellation must not prevent
// decoding the secret whose escaped spelling differs from the error's value.
func TestCanceledCredentialTokenizationStillRedacts(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	cancel()
	query := `CREATE ALIAS a FOR DATABASE d AT 'http://db.invalid' USER 'u' PASSWORD 'owned\u0041credential'`
	ctx := withStatementCredentials(parent, query)
	failure := errors.New("plugin rejected ownedAcredential")
	var output error = failure
	redactAdminCommandError(query, &output, statementCredentials(ctx))
	require.ErrorIs(t, output, failure)
	require.NotContains(t, output.Error(), "ownedAcredential")
	require.Contains(t, output.Error(), "REDACTED")
	require.ErrorIs(t, parent.Err(), context.Canceled)
}

func TestPublicPrivatePreparationKeepsCallerContext(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, query, owner string
			want               [][]any
		}{
			{"cache", "RETURN 1 AS value", ".cacheDependencyScopes", [][]any{{int64(1)}}},
			{"group", "CYPHER 25 UNWIND [1,1] AS x RETURN x AS k, count(*) AS n GROUP BY k", ".groupByEdit", [][]any{{int64(1), int64(2)}}},
			{"subquery", "CYPHER 5 WITH 1 AS x RETURN COUNT { WITH x RETURN x } AS c", ".cypher5ImportGrouping", [][]any{{int64(1)}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "private_ctx"), QueryCachePolicy{MaxEntries: 0})
				control, err := executor.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, control.Rows)
				// Use a fresh family so cached syntax cannot hide the selected producer.
				executor = NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "private_cancel"), QueryCachePolicy{MaxEntries: 0})
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx := &nor293PlanningContext{Context: parent, action: cancel, targetFunction: tc.owner}
				result, err := executor.Execute(ctx, tc.query, nil)
				require.Positive(t, ctx.targetChecks.Load(), "public query must reach selected preparation owner")
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
				retry, err := executor.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, retry.Rows)
			})
		}
	})
}

func TestPublicPathProjectionCancellationRollsBack(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, query   string
			before, after int64
		}{
			{"create", "CREATE p=(:PhasePath)-[:LINK]->(:PhasePath) RETURN p", 4, 6},
			{"merge", "MATCH (a:PhasePath {id:1}), (b:PhasePath {id:2}) MERGE p=(a)-[:LINK]->(b) RETURN p", 2, 2},
			{"match", "MATCH p=(:PhasePath)-[:LINK]->(:PhasePath) RETURN p", 2, 2},
		} {
			t.Run(tc.name, func(t *testing.T) {
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "path_ctx"), QueryCachePolicy{MaxEntries: 0})
				_, err := executor.Execute(t.Context(), "CREATE (:PhasePath {id:1})-[:LINK]->(:PhasePath {id:2})", nil)
				require.NoError(t, err)
				control, err := executor.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err)
				require.Len(t, control.Rows, 1)
				require.NotNil(t, control.Rows[0][0])
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx := &nor293PlanningContext{Context: parent, action: cancel, targetFunction: ".pathToMapContext"}
				result, err := executor.Execute(ctx, tc.query, nil)
				require.Positive(t, ctx.targetChecks.Load())
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
				count, err := executor.Execute(t.Context(), "MATCH (n:PhasePath) RETURN count(n)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{tc.before}}, count.Rows)
				retry, err := executor.Execute(t.Context(), tc.query, nil)
				require.NoError(t, err)
				require.Len(t, retry.Rows, 1)
				count, err = executor.Execute(t.Context(), "MATCH (n:PhasePath) RETURN count(n)", nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{tc.after}}, count.Rows)
			})
		}
	})
}

func TestCanceledProjectionPreservesPriorExpressionFailure(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := withExpressionFailureSlot(parent)
	original := errors.New("retained expression failure")
	recordExpressionFailure(ctx, original)
	cancel()
	require.ErrorIs(t, unresolvedReturnItemError(ctx, "missing"), original)
	forEachParser(t, func(t *testing.T) {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "projection_errors"))
		for _, query := range []string{"RETURN missing", "RETURN (", "RETURN 1/0"} {
			result, err := executor.Execute(t.Context(), query, nil)
			require.Error(t, err)
			require.Nil(t, result)
			require.NotErrorIs(t, err, context.Canceled)
		}
	})
}

// Frozen41118 canonical tokenizer allocation counts, measured with this exact
// input/owner on Go1.26.9 darwin/arm64. Context polling adds no per-read heap work.
func TestTokenizerContextAllocations(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		budget      float64
		tokens      int
	}{
		{"simple", "MATCH (n:Person) WHERE n.id=1 RETURN n", 10, 14},
		{"unicode", "RETURN 'é', 1 AS value", 8, 6},
		{"many", "RETURN " + strings.Repeat("1,", 64) + "1", 17, 130},
	} {
		for _, cancellable := range []bool{false, true} {
			t.Run(tc.name+"/"+map[bool]string{false: "root", true: "request"}[cancellable], func(t *testing.T) {
				ctx := context.Background()
				if cancellable {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(t.Context())
					defer cancel()
				}
				var count int
				allocations := testing.AllocsPerRun(100, func() {
					tokens, _, err := tokenizeSource(ctx, tc.query)
					if err != nil {
						panic(err)
					}
					count = len(tokens)
				})
				require.Equal(t, tc.tokens, count)
				require.LessOrEqual(t, allocations, tc.budget)
			})
		}
	}
}

func TestPublicDropPreparationCancellationKeepsBothKinds(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct{ kind, name, other string }{
			{"fulltext", indexDropFulltextName, indexDropVectorName},
			{"vector", indexDropVectorName, indexDropFulltextName},
		} {
			t.Run(tc.kind, func(t *testing.T) {
				fixture := newIndexDropFixture(t)
				fixture.indexDropCreateBothKinds(t, t.Context())
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx := &nor293PlanningContext{Context: parent, action: cancel, targetFunction: ".mutateSchema"}
				query := "CALL db.index." + tc.kind + ".drop('" + tc.name + "')"
				result, err := fixture.exec.Execute(ctx, query, nil)
				require.Positive(t, ctx.targetChecks.Load())
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
				require.Zero(t, fixture.owner.commits, "preparation cancellation cannot reach commit")
				require.Equal(t, indexDropBothKinds(), readIndexNames(t, t.Context(), fixture.indexDropOutside(), indexDropNamesQuery))
				require.Equal(t, indexDropBothKinds(), readIndexNames(t, t.Context(), fixture.other, indexDropNamesQuery))
				retry, err := fixture.exec.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]any{{tc.name, true}}, retry.Rows)
				require.Equal(t, []string{tc.other}, readIndexNames(t, t.Context(), fixture.indexDropOutside(), indexDropNamesQuery))
			})
		}
	})
}
