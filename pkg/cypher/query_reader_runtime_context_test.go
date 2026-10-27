package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStatementResultColumnsContextCancellationAndLiveContinuation(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	const query = "RETURN 1 AS value"
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	deadline, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	for _, test := range []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"canceled", canceled, context.Canceled},
		{"deadline", deadline, context.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			columns, err := executor.ResultColumns(test.ctx, query, resultmetadata.KnownScope(nil), false, RawSource)
			require.ErrorIs(t, err, test.want)
			require.Nil(t, columns)
			columns, err = executor.ResultColumns(t.Context(), query, resultmetadata.KnownScope(nil), false, RawSource)
			require.NoError(t, err)
			require.Equal(t, []string{"value"}, columns)
			columns, err = executor.StatementResultColumnsContext(test.ctx, query, RawSource)
			require.ErrorIs(t, err, test.want)
			require.Nil(t, columns)
			columns, err = executor.StatementResultColumnsContext(t.Context(), query, RawSource)
			require.NoError(t, err)
			require.Equal(t, []string{"value"}, columns)
		})
	}
}

func TestCanceledRuntimeReaderCachesDoNotPoisonPublicExecute(t *testing.T) {
	for _, owner := range []string{"return plan", "execution policy", "analyzer"} {
		t.Run(owner, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "reader_cache"), QueryCachePolicy{MaxEntries: 10, TTL: time.Minute})
			query := "RETURN 5 AS " + map[string]string{"return plan": "return_plan_recovery", "execution policy": "policy_recovery", "analyzer": "analyzer_recovery"}[owner]
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			switch owner {
			case "return plan":
				plan := returnProjectionPlanFor(canceled, query)
				require.False(t, plan.valid)
				returnProjectionPlans.RLock()
				_, found := returnProjectionPlans.plans[query]
				returnProjectionPlans.RUnlock()
				require.False(t, found)
			case "execution policy":
				policy := cachedExecutionPolicy(canceled, query)
				require.False(t, policy.cacheable)
				_, found := executionPolicyCache.get(query)
				require.False(t, found)
			case "analyzer":
				executor.analyzer.Analyze(canceled, query)
				executor.analyzer.cacheMu.RLock()
				_, found := executor.analyzer.raw[query]
				executor.analyzer.cacheMu.RUnlock()
				require.False(t, found)
			}
			for range 2 {
				result, err := executor.Execute(t.Context(), query, nil)
				require.NoError(t, err)
				require.Len(t, result.Columns, 1)
				require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows)
			}
			hits, _, _, _, _ := executor.cache.Stats()
			require.Equal(t, int64(1), hits, "a canceled classification cannot disable a later live cache entry")
		})
	}
}

func TestRuntimeReaderPublicContextAndLiveShapes(t *testing.T) {
	for _, parser := range []struct {
		name  string
		setup func() func()
	}{
		{"nornic", config.WithNornicParser}, {"antlr", config.WithANTLRParser},
	} {
		t.Run(parser.name, func(t *testing.T) {
			t.Cleanup(parser.setup())
			for index, query := range []string{
				"WITH 3 AS `quoted value` RETURN `quoted value` AS result",
				"/* leading trivia */ with 4 as value\nreturn value AS result",
				"CALL () { RETURN 5 AS value } RETURN value AS result",
				"MATCH (n:ReaderEmpty) RETURN n.value AS result UNION ALL RETURN 6 AS result",
			} {
				t.Run(fmt.Sprint(index), func(t *testing.T) {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "reader_shapes"))
					for _, budget := range []int{16, 64, 256} {
						checkpoint := newCheckpointContext(budget, nil)
						_, err := executor.Execute(checkpoint, query, nil)
						if checkpoint.Context.Err() != nil {
							require.ErrorIs(t, err, context.Canceled)
						}
						checkpoint.cancel()
					}
					result, err := executor.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					require.Equal(t, []string{"result"}, result.Columns)
					require.Equal(t, [][]interface{}{{int64(3 + index)}}, result.Rows)
				})
			}
		})
	}
}

// Live malformed statements retain their original diagnostics when interrupted
// structural readers are arbitrated at semantic admission.
func TestRuntimeReaderLiveMalformedDiagnostics(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, test := range []struct{ query, diagnostic string }{
		{"CALL (1) { RETURN 1 AS value } RETURN value", "CALL imports must contain only variable names"},
		{"RETURN 1 +", ""},
	} {
		_, err := executor.Execute(t.Context(), test.query, nil)
		if test.diagnostic == "" {
			requireRuntimeReaderGrammarDiagnostic(t, test.query, err)
		} else {
			require.ErrorContains(t, err, test.diagnostic)
		}
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}

func requireRuntimeReaderGrammarDiagnostic(t *testing.T, query string, err error) {
	t.Helper()
	source := query
	var rewrite *queryRewrite
	if config.IsCypherQueryNormalizationEnabled() {
		var normalizeErr error
		source, rewrite, normalizeErr = normalizeQueryText(t.Context(), source)
		require.NoError(t, normalizeErr)
	}
	grammarErr := cypherantlr.ValidateContext(t.Context(), source, cyphertext.RawSource)
	require.Error(t, grammarErr)
	_, want := rewrite.restore(nil, syntaxAdmissionError(t.Context(), source, AcceptedSource, grammarErr))
	require.EqualError(t, err, want.Error())
}

// A malformed CALL scope clause is Neo4j's import error on both parsers.
// The grammar rejects the clause, including in expression subquery bodies,
// before either parser's scope walk can check imports. Each parser is
// selected here, on a fresh executor per statement, so no cached verdict
// decides the answer.
func TestMalformedCallScopeClauseDiagnosticOnBothParsers(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, test := range []struct{ name, query string }{
				{"literal import", "CALL (1) { RETURN 1 AS value } RETURN value"},
				{"expression import", "WITH 1 AS x CALL (x + 1) { RETURN 1 AS value } RETURN value"},
				{"nested CALL", "CALL () { CALL (1) { RETURN 1 AS value } RETURN value } RETURN value"},
				{"OPTIONAL CALL", "OPTIONAL CALL (1) { RETURN 1 AS value } RETURN value"},
				{"EXISTS body", "RETURN EXISTS { CALL (1) { RETURN 1 AS value } RETURN value } AS ok"},
				{"COUNT body", "RETURN COUNT { CALL (1) { RETURN 1 AS value } RETURN value } AS count"},
				{"COLLECT body", "RETURN COLLECT { CALL (1) { RETURN 1 AS value } RETURN value } AS values"},
			} {
				t.Run(test.name, func(t *testing.T) {
					executor := NewStorageExecutor(newTestMemoryEngine(t))
					_, err := executor.Execute(t.Context(), test.query, nil)
					require.ErrorContains(t, err, "CALL imports must contain only variable names", test.query)
					requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				})
			}
			for _, test := range []struct{ name, query, nornicDiagnostic string }{
				{"valid scope with another syntax error", "WITH 1 AS x CALL (x) { RETURN x AS value } RETURN value +", "CALL imports must contain only variable names"},
				{"parentheses without a subquery body", "CALL (1) RETURN 1 +", "CALL imports must contain only variable names"},
				{"procedure named call", "CALL call(1) RETURN 1 +", "CALL imports must contain only variable names"},
				{"labels procedure", "CALL db.labels() RETURN 1 +", "CALL imports must contain only variable names"},
				{"apoc procedure", "CALL apoc.x(1) RETURN 1 +", "CALL imports must contain only variable names"},
				{"single quoted text", "RETURN 'CALL (1) { RETURN 1 }' AS text, 1 +", ""},
				{"double quoted text", "RETURN \"CALL (1) { RETURN 1 }\" AS text, 1 +", ""},
				{"line comment", "RETURN 1 // CALL (1) { RETURN 1 }\n+", ""},
				{"block comment", "RETURN 1 /* CALL (1) { RETURN 1 } */ +", ""},
				{"EXISTS body with another syntax error", "RETURN EXISTS { CALL () { RETURN 1 AS value } RETURN value + } AS ok", ""},
				{"COUNT body with another syntax error", "RETURN COUNT { CALL () { RETURN 1 AS value } RETURN value + } AS count", ""},
				{"COLLECT body with another syntax error", "RETURN COLLECT { CALL () { RETURN 1 AS value } RETURN value + } AS values", ""},
			} {
				t.Run(test.name, func(t *testing.T) {
					// None of these has a malformed scope clause. In particular,
					// a procedure's arguments must not supply one.
					executor := NewStorageExecutor(newTestMemoryEngine(t))
					_, err := executor.Execute(t.Context(), test.query, nil)
					if parser == config.ParserTypeANTLR || test.nornicDiagnostic == "" {
						// Preserve the grammar owner's exact localized diagnostic,
						// including its structural source coordinates. Raw ANTLR
						// token excerpts are not the credentials layer's contract.
						requireRuntimeReaderGrammarDiagnostic(t, test.query, err)
					} else {
						// Keep the default parser's existing diagnostic, including
						// its known import error for a later CALL parse failure.
						require.ErrorContains(t, err, test.nornicDiagnostic)
					}
					requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				})
			}
		})
	}
}

func TestQueryBodyOutputPresencePolicies(t *testing.T) {
	for _, test := range []struct {
		query            string
		direct, declared bool
	}{
		{"RETURN 1 AS value", true, true},
		{"CALL () { RETURN 1 AS value }", false, true},
		{"CALL db.labels()", false, true},
		{"RETURN 1 AS value FINISH", false, false},
	} {
		body := readQueryBody(t.Context(), test.query, 0, len(test.query))
		require.Equal(t, test.direct, queryBodyProducesColumns(t.Context(), test.query, body, true), test.query)
		require.Equal(t, test.declared, queryBodyProducesColumns(t.Context(), test.query, body, false), test.query)
	}
}
