package cypher

import (
	"fmt"
	"strings"
	"testing"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every clause spelling can occupy an identifier-valued invocation role.
// Grammar membership must not hide an unknown function from the public check.
// Quoting a semantic keyword also makes it a name, rather than an operator.
func TestVocabularyConsumerCallableNames(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, word := range cypherantlr.ClauseWords() {
		name := strings.ToLower(word)
		if _, known := builtInCypherFunctions[name]; known {
			continue
		}
		for _, spelling := range []string{name, "`" + name + "`"} {
			query := "RETURN " + spelling + "(1) AS value"
			t.Run(spelling, func(t *testing.T) {
				var calls []string
				err := forEachFunctionCall(query, func(_, normalized string) error {
					calls = append(calls, normalized)
					return nil
				})
				require.NoError(t, err)
				require.Equal(t, []string{name}, calls)
				_, err = executor.Execute(t.Context(), query, nil)
				require.Error(t, err)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
				if spelling == name {
					// This pattern reader's word-role question is unquoted;
					// quoted invocation admission is checked publicly above.
					require.True(t, functionCallParenAt(query, strings.IndexByte(query, '(')), query)
				}
			})
		}
	}
	for _, query := range []string{
		"MATCH (n) WHERE (n.x > 0) RETURN n",
		"CALL db.labels() YIELD label RETURN label",
		"USE nornic RETURN 1",
	} {
		var calls []string
		require.NoError(t, forEachFunctionCall(query, func(_, normalized string) error {
			calls = append(calls, normalized)
			return nil
		}))
		require.Empty(t, calls, query)
	}
}

// Native declarations are opaque to graph expression/order lowering, in every
// execution prefix. The native owner still checks and executes their payload.
func TestVocabularyConsumerNativeRegions(t *testing.T) {
	declarations := []string{
		"CREATE CONSTRAINT unit_key FOR (n:Unit) REQUIRE (n.a, n.b) IS NODE KEY",
		"CREATE CONSTRAINT unit_rel_key FOR ()-[r:UnitRel]-() REQUIRE (r.a, r.b) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT unit_unique FOR (n:UnitUnique) REQUIRE (n.a, n.b) IS UNIQUE",
		"CREATE CONSTRAINT unit_temporal FOR (n:UnitTime) REQUIRE (n.a, n.from, n.to) IS TEMPORAL NO OVERLAP",
	}
	for _, prefix := range []string{"", "/* native region */ ", "CYPHER 5 ", "EXPLAIN ", "PROFILE "} {
		for _, declaration := range declarations {
			query := prefix + declaration
			t.Run(query, func(t *testing.T) {
				text, rewrite, err := desugarLabelExpressions(query, nil, false)
				require.NoError(t, err)
				require.Equal(t, query, text)
				require.Nil(t, rewrite)
				text, rewrite = desugarStandaloneOrderClauses(query)
				require.Equal(t, query, text)
				require.Nil(t, rewrite)
				executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "native_unit"))
				_, err = executor.Execute(t.Context(), query, nil)
				require.NoError(t, err)
			})
		}
	}
}

// Enumerate all ordered-tail subsets and permutations, with and without a
// RETURN continuation. Valid SHOW stages are ordered; graph composition must
// not silently legalize a decreasing native stage by inserting WITH.
func TestVocabularyConsumerShowStagePopulation(t *testing.T) {
	stages := []string{"ORDER /* phrase extent */ BY name", "SKIP 0", "LIMIT 1", "WHERE name <> ''"}
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	var visit func([]int, uint)
	visit = func(order []int, used uint) {
		valid := true
		parts := make([]string, len(order))
		for i, role := range order {
			parts[i] = stages[role]
			if i > 0 && order[i-1] > role {
				valid = false
			}
		}
		for _, suffix := range []string{"", " RETURN name"} {
			query := "SHOW FUNCTIONS YIELD name " + strings.Join(parts, " ") + suffix
			t.Run(fmt.Sprint(order)+suffix, func(t *testing.T) {
				lowered, _ := desugarStandaloneOrderClauses(query)
				require.NotContains(t, lowered, "WITH *", query)
				_, err := executor.Execute(t.Context(), query, nil)
				if valid {
					require.NoError(t, err, query)
				} else {
					require.Error(t, err, query)
				}
			})
		}
		for role := range stages {
			if used&(1<<role) == 0 {
				visit(append(append([]int(nil), order...), role), used|1<<role)
			}
		}
	}
	visit(nil, 0)
	for _, query := range []string{
		"SHOW FUNCTIONS YIELD name ORDER BY name OFFSET 0 LIMIT 1 RETURN name",
		"WITH 2 AS x ORDER BY x RETURN x",
		"CALL { WITH 1 AS x RETURN x } RETURN x",
		"CYPHER 25 RETURN 1 AS x NEXT FOR y IN [x] RETURN y",
	} {
		_, err := executor.Execute(t.Context(), query, nil)
		require.NoError(t, err, query)
	}
}

// Arbitrary-cardinality projections use the same scalar lexical contract,
// including the last member, quotes, nested regions and phrase source extent.
func TestVocabularyConsumerScannerPopulation(t *testing.T) {
	words := cypherantlr.ClauseWords()
	for _, word := range words {
		for _, prefix := range []string{"x ", "'" + word + "' ", "[x IN [] | '" + word + "'] ", "/* " + word + " */ x "} {
			text := prefix + word + " 1"
			want := -1
			for _, candidate := range words {
				if at := keywordIndex(text, candidate); at >= 0 && (want < 0 || at < want) {
					want = at
				}
			}
			require.Equal(t, want, firstKeywordIndexFromDefault(text, 0, words...), text)
		}
	}
	start, end, member := firstKeywordMatchFrom("x ORDER /* comment */ BY x", 0, true, "ORDER BY")
	require.Equal(t, 2, start)
	require.Equal(t, len("x ORDER /* comment */ BY"), end)
	require.Equal(t, 0, member)
	for _, word := range words {
		require.True(t, isNameableClauseKeyword(word+" operand"), word)
	}
}
