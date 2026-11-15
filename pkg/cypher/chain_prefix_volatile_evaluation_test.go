package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A chained pattern's WHERE is the row predicate: a volatile call in it is
// evaluated once per complete path. The traversal may also decide a conjunct on
// a path prefix (chainPrefixPrune), and a conjunct decided there is evaluated a
// second time on the complete path. A volatile call must therefore never be
// decided on the prefix, whatever its spelling: the function catalog's
// volatility declarations own that decision.

// volatileChainedPredicates holds one predicate per spelling: the whitespace,
// comment and quoted-name variations, a nested call, and each volatile
// function. Each one returns the boolean an a node's keep property compares to.
var volatileChainedPredicates = []string{
	"rand() < 0.5",
	"RAND() < 0.5",
	"Rand( ) < 0.5",
	"rand () < 0.5",
	"rand /* c */ () < 0.5",
	"rand\t() < 0.5",
	"rand\n() < 0.5",
	"`rand`() < 0.5",
	"toInteger(rand() * 2) = 0",
	"toInteger(rand () * 2) = 0",
	"randomUUID() < '8'",
	"RANDOMUUID() < '8'",
	"randomUUID () < '8'",
	"randomuuid () < '8'",
	"apoc.create.uuid() < '8'",
	"apoc.create.uuid () < '8'",
}

// seededChainedVolatileExecutor returns an executor whose graph holds the 2000
// (a:a)-[:R]->(:m)-[:R]->(:z) paths a predicate that keeps half of them is
// counted against.
func seededChainedVolatileExecutor(t *testing.T, namespace string) *StorageExecutor {
	t.Helper()
	exec, _ := newTestExecutor(t, namespace)
	_, err := exec.Execute(context.Background(),
		`UNWIND range(0, 1999) AS i CREATE (:a {keep: true, i: i})-[:R]->(:m {i: i})-[:R]->(:z {i: i})`, nil)
	require.NoError(t, err)
	require.EqualValues(t, 2000, chainedVolatileCount(t, exec,
		`MATCH (a:a)-[:R]->(m:m)-[:R]->(z:z) RETURN count(z)`), "seeded paths")
	return exec
}

// chainedVolatileCount runs a single-row count query and returns its count.
func chainedVolatileCount(t *testing.T, exec *StorageExecutor, query string) int64 {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	require.Len(t, result.Rows, 1, query)
	require.Len(t, result.Rows[0], 1, query)
	return toInt64(result.Rows[0][0])
}

// Built-in spelling controls prove the prefix owner rejects each actual
// catalog-declared volatile call; public invocation counts below prove that
// owner governs execution without relying on probabilistic output bounds.
func TestChainedPatternVolatilePredicatesAreNotEvaluatedOnPrefixes(t *testing.T) {
	bound := map[string]bool{"a": true, "m": true, "z": true}
	for _, predicate := range volatileChainedPredicates {
		require.False(t, chainPrefixDecides(t.Context(), "a.keep = ("+predicate+")", bound, ""), predicate)
	}
	for _, expression := range []string{"datetime() = datetime()", "date.truncate('day') IS NOT NULL"} {
		require.False(t, chainPrefixDecides(t.Context(), expression, bound, ""), expression)
	}
	require.True(t, chainPrefixDecides(t.Context(), "a.i >= 0", bound, ""), "stable bound predicates still prune")
}

// TestChainedPatternEvaluatesInjectedCounterOncePerPath counts the evaluations
// themselves through the public function-injection seam, so the count does not
// depend on a random draw. The counter's value is discarded by design (each
// predicate holds for every row), which leaves one evaluation per complete path
// as the only observable: a conjunct decided on a prefix spends the first 2000
// values there and the row filter spends 2000 more.
func TestChainedPatternEvaluatesInjectedCounterOncePerPath(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, query := range []string{
			`MATCH (a:a)-[:R]->(m:m)-[:R]->(z:z) WHERE m.i >= inspection.value() * 0 RETURN count(z)`,
			`MATCH p = (a:a)-[:R]->(m:m)-[:R]->(z:z) WHERE m.i >= inspection.value() * 0 RETURN count(p)`,
		} {
			var calls int64
			withPluginFunctions(t, map[string]interface{}{
				"inspection.value": func() int64 { calls++; return calls },
			})
			exec := seededChainedVolatileExecutor(t, "volatile_counter")
			require.EqualValues(t, 2000, chainedVolatileCount(t, exec, query), query)
			require.EqualValues(t, 2000, calls, "%s: each complete path evaluates the predicate once", query)
		}
	})
}
