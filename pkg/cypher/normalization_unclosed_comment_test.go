package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNormalizationRejectsUnclosedComment(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, normalization := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalization)
		for route, execute := range map[string]func(context.Context, string, map[string]interface{}) (*ExecuteResult, error){
			"public":   exec.Execute,
			"internal": exec.executeInternal,
		} {
			for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
				for _, statement := range []string{
					"RETURN 1 /* incomplete",
					"RETURN 1/*",
					"/* incomplete",
					"/* closed */ RETURN 1 /* incomplete",
					"RETURN 1 /* closed */ \n /* incomplete",
					"RETURN 1 /*/",
				} {
					t.Run(route+"/"+prefix+statement, func(t *testing.T) {
						_, err := execute(boundedContext(t), prefix+statement, nil)
						code := "Neo.ClientError.Statement.SyntaxError"
						if statement == "/* incomplete" {
							code = "Neo.DatabaseError.Statement.ExecutionFailed"
						}
						requireStatusCode(t, err, code)
					})
				}
			}
		}
	}
	config.SetCypherQueryNormalizationEnabled(true)
	for _, statement := range []string{
		"RETURN 1 /* closed */",
		"RETURN 1 /* closed at EOF */",
		"RETURN 1 // incomplete /*",
		"RETURN '/* incomplete' AS text",
		"RETURN \"/* incomplete\" AS text",
		"RETURN 1 AS `/* incomplete`",
		"RETURN 1 /* ' not a quote */",
	} {
		_, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
	}
}

func TestExecutionModesRequireStatement(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	for _, normalization := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalization)
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
		for _, header := range []struct {
			prefix   string
			conflict bool
		}{
			{"EXPLAIN ", false}, {"PROFILE ", false}, {"EXPLAIN/**/", false}, {"PROFILE/**/", false},
			{"PROFILE\t", false}, {"PROFILE\n", false},
			{"/* leading */ PROFILE /* middle */ ", false}, {"// leading\nPROFILE/**/", false},
			{"PROFILE/**/PROFILE ", false}, {"EXPLAIN EXPLAIN ", false},
			{"CYPHER 5 EXPLAIN ", false}, {"CYPHER 5 PROFILE ", false},
			{"EXPLAIN PROFILE ", true}, {"PROFILE EXPLAIN ", true},
			{"EXPLAIN CYPHER 5 PROFILE ", true}, {"PROFILE CYPHER 5 EXPLAIN ", true},
		} {
			prefix := header.prefix
			for _, tail := range []string{"", "/* closed */", "// line at EOF", "/* closed */ // line /* at EOF", "/**/"} {
				t.Run(prefix+tail, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					defer cancel()
					_, err := exec.Execute(ctx, prefix+tail, nil)
					requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				})
			}
			for _, tail := range []string{"/*", "/* incomplete", "/*/", "/* closed */ /* incomplete"} {
				_, err := exec.Execute(boundedContext(t), prefix+tail, nil)
				requireStatusCode(t, err, "Neo.DatabaseError.Statement.ExecutionFailed")
			}
			if header.conflict {
				for _, tail := range []string{"RETURN 1", "RETURN 1 /* incomplete"} {
					_, err := exec.Execute(boundedContext(t), prefix+tail, nil)
					requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
				}
				continue
			}
			if !normalization && prefix[0] == '/' {
				// General leading-comment admission in the default parser
				// with normalization disabled is an inherited limitation.
				continue
			}
			valid := []string{"RETURN 7 AS value", "RETURN '/* incomplete' AS value", "RETURN $x AS value"}
			if normalization {
				// General statement-tail comments with normalization disabled
				// are an inherited parser limitation, outside mode admission.
				valid = append(valid, "RETURN $x AS value /* closed */")
			}
			for _, tail := range valid {
				t.Run(prefix+tail, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					defer cancel()
					result, err := exec.Execute(ctx, prefix+tail, map[string]interface{}{"x": int64(7)})
					require.NoError(t, err)
					require.NotEmpty(t, result.Metadata["planType"])
				})
			}
		}
		for _, query := range []string{"EXPLAIN PROFILE RETURN 1", "PROFILE EXPLAIN RETURN 1"} {
			_, err := exec.Execute(context.Background(), query, nil)
			requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
		}
		for _, tc := range []struct{ query, code string }{
			{"/* closed */", "Neo.ClientError.Statement.SyntaxError"},
			{"EXPLAIN /* closed */", "Neo.ClientError.Statement.SyntaxError"},
			{"EXPLAIN /* incomplete", "Neo.DatabaseError.Statement.ExecutionFailed"},
			{"EXPLAIN RETURN 1 /* incomplete", "Neo.ClientError.Statement.ArgumentError"},
		} {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			_, err := exec.executeProfile(ctx, tc.query)
			cancel()
			requireStatusCode(t, err, tc.code)
		}
	}
}

func TestNormalizationRejectsUnclosedCommentBeforeWrites(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	config.SetCypherQueryNormalizationEnabled(true)
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
	_, err := exec.Execute(context.Background(), "CREATE (:UnclosedComment) /* incomplete", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	result, err := exec.Execute(context.Background(), "MATCH (n:UnclosedComment) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

func boundedContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

func TestCommentAdmissionRetainsRawSourceAndTransactionSemantics(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	for _, normalization := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalization)
		for _, explicit := range []bool{false, true} {
			for _, tc := range []struct{ query, code string }{
				{"CREATE (:AdmissionWrites) /* incomplete", "Neo.ClientError.Statement.SyntaxError"},
				{"/* incomplete", "Neo.DatabaseError.Statement.ExecutionFailed"},
				{"PROFILE /* closed */", "Neo.ClientError.Statement.SyntaxError"},
				{"CYPHER 5 PROFILE /* incomplete", "Neo.DatabaseError.Statement.ExecutionFailed"},
				{"PROFILE /**/ CYPHER 5 /* incomplete", "Neo.DatabaseError.Statement.ExecutionFailed"},
				{"EXPLAIN PROFILE /* incomplete", "Neo.DatabaseError.Statement.ExecutionFailed"},
			} {
				t.Run(tc.query, func(t *testing.T) {
					exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
					// A permissive classifier may inspect malformed source first.
					// Its rewrite/cache entry must not certify execution admission.
					exec.analyzer.Analyze(t.Context(), tc.query)
					if explicit {
						_, err := exec.Execute(boundedContext(t), "BEGIN", nil)
						require.NoError(t, err)
					}
					for i := 0; i < 2; i++ {
						result, err := exec.Execute(boundedContext(t), "RETURN $value AS value", map[string]interface{}{"value": int64(7)})
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
					}
					_, err := exec.Execute(boundedContext(t), tc.query, nil)
					requireStatusCode(t, err, tc.code)
				})
			}
		}
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
		for _, prefix := range []string{"PROFILE /**/ CYPHER 5 ", "EXPLAIN /**/ CYPHER 5 "} {
			result, err := exec.Execute(boundedContext(t), prefix+"RETURN $value AS value", map[string]interface{}{"value": int64(7)})
			require.NoError(t, err)
			require.NotEmpty(t, result.Metadata["planType"])
		}
	}
}
