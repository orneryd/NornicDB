package cypher

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// observeFunctionSyntaxFacts runs run and returns every request for
// retained facts whose text matches, with the stack that made it. A nil
// predicate records every request, including cached and marker-free fragments.
func observeFunctionSyntaxFacts(t *testing.T, matches func(text string) bool, run func()) []string {
	t.Helper()
	var mu sync.Mutex
	var requests []string
	observe := func(text string) {
		if matches != nil && !matches(text) {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, fmt.Sprintf("%q\n%s", text, debug.Stack()))
	}
	functionSyntaxFactObserver.Store(&observe)
	defer functionSyntaxFactObserver.Store(nil)
	run()
	return requests
}

// Static checks and statement validation read only the token readers: the
// fact builder belongs to execution and the label rewrite. A statement's
// own text is new on every first execution, so asking for its facts there
// builds them once per statement. The marker makes each statement's text
// its own, as the first-execution benchmark does, and requests made for any
// text holding it fail with their stack.
func TestFunctionSyntaxStatementPathsAskForNoFacts(t *testing.T) {
	const marker = "987654321"
	ctx := context.Background()
	for _, statement := range functionLookupStatements {
		query := statement.query + ", " + marker + " AS r73"
		requests := observeFunctionSyntaxFacts(t, nil, func() {
			require.NoError(t, validateStaticFunctionArguments(query, false))
		})
		require.Empty(t, requests, "the static check of %s asked for facts", statement.name)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
	for _, statement := range functionLookupStatements {
		query := statement.query + ", " + marker + " AS r73"
		requests := observeFunctionSyntaxFacts(t, func(text string) bool {
			return strings.Contains(text, marker)
		}, func() {
			_, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err)
		})
		require.Empty(t, requests, "the first execution of %s asked for facts of its own text", statement.name)
	}
	query := "MATCH (n:Person {id: " + marker + "}) RETURN n.name"
	requests := observeFunctionSyntaxFacts(t, nil, func() {
		require.NoError(t, (&StorageExecutor{}).validateSemanticScopes(ctx, query))
	})
	require.Empty(t, requests, "semantic validation asked for facts of the statement's own text")
}

// Observe explicit validation separately from execution, which may retain facts.
// Every fragment counts, including ones without the statement's numeric marker.
func TestFunctionSyntaxValidationReadersAskForNoFacts(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "validation_readers"))
	for _, query := range []string{
		"RETURN abs((987654321))",
		"RETURN [987654321][0]",
		"RETURN keys /*gap*/ ({a:1})[0] AS v",
		"WITH abs((987654321)) AS x RETURN x",
		"WITH [987654321] AS xs RETURN xs[0]",
		"CALL { RETURN abs((987654321)) AS x } RETURN x",
		"CALL { RETURN [987654321][0] AS x } RETURN x",
	} {
		t.Run(query, func(t *testing.T) {
			for _, validation := range []struct {
				name string
				run  func() error
			}{
				{"static_functions", func() error { return validateStaticFunctionArguments(query, false) }},
				{"static_subscripts", func() error { return validateStaticPropertySubscripts(query, staticTypeScope{}) }},
				{"semantic", func() error { return exec.validateSemanticScopes(context.Background(), query) }},
			} {
				t.Run(validation.name, func(t *testing.T) {
					requests := observeFunctionSyntaxFacts(t, nil, func() {
						require.NoError(t, validation.run())
					})
					require.Empty(t, requests, "validation requested retained facts")
				})
			}
		})
	}
}
