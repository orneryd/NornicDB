package cypher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Enumerate the lexer's White_Space class, independently of the implementation.
var lexicalWhitespaceCorpus = []rune{'\t', '\n', '\v', '\f', '\r', ' ', '\u0085', '\u00a0', '\u1680', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004', '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200a', '\u2028', '\u2029', '\u202f', '\u205f', '\u3000'}

func TestLexicalUnicodeBoundaryScanners(t *testing.T) {
	for _, gap := range lexicalWhitespaceCorpus {
		t.Run(fmt.Sprintf("U%04X", gap), func(t *testing.T) {
			space := string(gap)
			require.Equal(t, "RETURN café", normalizePipelineWhitespace("RETURN"+space+"café"))
			require.Equal(t, "25", cypherGrammarVersion("CYPHER"+space+"25"+space+"runtime=slotted"+space+"RETURN 1"))
			query := "RETURN" + space + "abs" + space + "(-1)" + space + "AS" + space + "café"
			functions, err := usePropertyFunctions(query)
			require.NoError(t, err)
			require.Equal(t, []string{"abs"}, functions)
			tokens, _, err := tokenizeSource(query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"RETURN", "abs", "(", "-", "1", ")", "AS", "café"}, tokens)
			for _, keyword := range []string{"RETURN", "ORDER BY", "IS NOT NULL"} {
				spelling := strings.ReplaceAll(keyword, " ", space)
				text := "café" + space + spelling + space + "x"
				require.Equal(t, len("café"+space), keywordIndexFrom(text, keyword, 0, defaultKeywordScanOpts()))
				require.Negative(t, keywordIndexFrom("café"+spelling+"界", keyword, 0, defaultKeywordScanOpts()))
			}
			var names []string
			cyphertext.ParameterReferences("a$b"+space+"$café"+space+"$0", func(name string, _, _ int) { names = append(names, name) })
			require.Equal(t, []string{"café", "0"}, names)
		})
	}
}

func TestCredentialUnicodeOutput(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	var logs bytes.Buffer
	exec.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	exec.SetSlowQueryThreshold(time.Nanosecond)
	for _, gap := range lexicalWhitespaceCorpus {
		for position := range 7 {
			gaps := []string{" ", " ", " ", " ", " ", " ", " "}
			gaps[position] = string(gap)
			for spelling, tail := range []string{"'probe741928' probe741928", `"probe741928" probe741928`, "`probe741928` probe741928", "probe741928 probe741928", "PASSWORD 'probe741928' probe741928", "FROM probe741928 TO probe629174", "'probe741928", "`probe741928", "MATCH", "`probe741928`"} {
				t.Run(fmt.Sprintf("U%04X/position%d/spelling%d", gap, position, spelling), func(t *testing.T) {
					tail = strings.ReplaceAll(strings.ReplaceAll(tail, "FROM ", "FROM"+gaps[3]), " TO ", gaps[4]+"TO"+gaps[6])
					query := "CREATE COMPOSITE DATABASE `unicode_output` ALIAS `r` FOR DATABASE `nornic` AT 'http://example.invalid'" + gaps[5] + "USER" + gaps[2] + "'u'" + gaps[0] + "PASSWORD" + gaps[1] + tail
					logs.Reset()
					_, err := exec.Execute(t.Context(), query, nil)
					outputs := []string{RedactLiterals(query)}
					if err != nil {
						outputs = append(outputs, err.Error())
						var localized *localization.LocalizedError
						if errors.As(err, &localized) {
							outputs = append(outputs, localized.Message.Fallback, fmt.Sprint(localized.Message.Data))
						}
					}
					_, nativeErr := exec.executeCompositeCommand(t.Context(), query, compositeCreateCommand)
					if spelling < 8 && nativeErr == nil {
						t.Error("malformed native command was accepted")
					}
					if nativeErr != nil {
						outputs = append(outputs, nativeErr.Error())
						var localized *localization.LocalizedError
						if errors.As(nativeErr, &localized) {
							outputs = append(outputs, localized.Message.Fallback, fmt.Sprint(localized.Message.Data))
						}
					}
					// Call the real producers with original source even when validation
					// returns before Execute's duration-based logging path.
					exec.emitSlowQueryLog(t.Context(), query, nil, time.Second)
					require.NotEmpty(t, logs.String())
					outputs = append(outputs, logs.String())
					_, span := exec.startExecuteSpan(t.Context(), "read", query)
					span.End()
					ended := recorder.Ended()
					found := false
					for _, attr := range ended[len(ended)-1].Attributes() {
						if string(attr.Key) == "cypher.query" {
							found = true
							outputs = append(outputs, attr.Value.AsString())
						}
					}
					require.True(t, found)
					for _, output := range outputs {
						for _, secret := range []string{"probe741928", "probe629174", "MATCH"} {
							if strings.Contains(output, secret) {
								t.Error("credential reached an output boundary; details withheld")
							}
						}
					}
				})
			}
		}
	}
}

func TestUseOwnerGeneratedCacheDependencies(t *testing.T) {
	for _, removal := range []string{"database", "alias"} {
		t.Run(removal, func(t *testing.T) {
			manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "g"}))
			if removal == "alias" {
				require.NoError(t, manager.CreateAlias(t.Context(), "selected", "g"))
			}
			selection := "g"
			if removal == "alias" {
				selection = "selected"
			}
			store, err := manager.GetStorage("nornic")
			require.NoError(t, err)
			exec := NewStorageExecutor(store)
			exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			require.NoError(t, exec.UseDatabaseStatementCaches(manager, "nornic"))
			const live = "RETURN 91 AS live"
			require.NoError(t, exec.validateSyntax(t.Context(), live, false, syntaxStatement))
			for _, gap := range lexicalWhitespaceCorpus {
				for _, graph := range []string{"", "GRAPH" + string(gap)} {
					for _, wrapper := range []string{"%s RETURN 1", "CALL { %s RETURN 1 AS x } RETURN x", "%s RETURN 1 UNION ALL %s RETURN 1"} {
						selector := "UsE" + string(gap) + graph + selection
						query := fmt.Sprintf(wrapper, selector)
						if strings.Count(wrapper, "%s") == 2 {
							query = fmt.Sprintf(wrapper, selector, selector)
						}
						scope, err := manager.StatementCacheScope(selection)
						require.NoError(t, err)
						scopes, valid := exec.cacheDependencyScopes(query)
						require.True(t, valid)
						require.True(t, containsStatementScope(scopes, scope), "selection identity absent for U%04X", gap)
						require.NoError(t, exec.validateSyntax(t.Context(), query, false, syntaxStatement))
						exec.cachedUpperQuery(query)
						require.NotZero(t, exec.cachedSyntaxVerdict(query))
						exec.ensureUpperQueryCache().mu.RLock()
						_, warm := exec.ensureUpperQueryCache().cache[query]
						exec.ensureUpperQueryCache().mu.RUnlock()
						require.True(t, warm)
						if removal == "alias" {
							err = manager.DropAlias(t.Context(), selection)
						} else {
							err = manager.DropDatabase(t.Context(), selection)
						}
						require.NoError(t, err)
						require.False(t, scope.Valid())
						require.Zero(t, exec.cachedSyntaxVerdict(query))
						shard := &exec.ensureSyntaxValidationCache().shards[boundedCacheStringShardIndex(query)]
						shard.mu.RLock()
						_, syntaxSurvived := shard.cache[query]
						_, syntaxDependenciesSurvived := shard.dependencies[query]
						shard.mu.RUnlock()
						require.False(t, syntaxSurvived, "removed selection retained syntax entry")
						require.False(t, syntaxDependenciesSurvived)
						exec.ensureUpperQueryCache().mu.RLock()
						_, survived := exec.ensureUpperQueryCache().cache[query]
						exec.ensureUpperQueryCache().mu.RUnlock()
						require.False(t, survived, "removed selection retained upper entry")
						require.NotZero(t, exec.cachedSyntaxVerdict(live), "unrelated warm entry was retired")
						if removal == "alias" {
							err = manager.CreateAlias(t.Context(), selection, "g")
						} else {
							err = manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: selection})
						}
						require.NoError(t, err)
						fresh, err := manager.StatementCacheScope(selection)
						require.NoError(t, err)
						require.NotSame(t, scope, fresh)
						require.Zero(t, exec.cachedSyntaxVerdict(query))
						require.NoError(t, exec.validateSyntax(t.Context(), query, false, syntaxStatement))
						scopes, valid = exec.cacheDependencyScopes(query)
						require.True(t, valid)
						require.True(t, containsStatementScope(scopes, fresh))
					}
				}
			}
		})
	}
}
