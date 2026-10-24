package cypher

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// staticConstructorTypesOracle holds Neo4j 5.26.30's verdicts for
// staticConstructorTypesPopulation, by position: the queries it rejects, and
// the 258 queries that need a parameter's value to be typed as Neo4j types
// them (parameter-aware admission, a separate fix); every other query,
// parameter-bearing ones included, is held to Neo4j's verdict.
//
//go:embed testdata/static_constructor_types_oracle.json
var staticConstructorTypesOracle []byte

// staticConstructorTypesParams is every query's parameter map: one value per
// type class.
func staticConstructorTypesParams() map[string]interface{} {
	return map[string]interface{}{"i": int64(5), "f": 1.5, "s": "a", "l": []interface{}{int64(1)},
		"m": map[string]interface{}{"a": int64(1)}, "z": nil, "b": true}
}

// staticConstructorTypesPopulation applies every composite constructor
// (unary and binary operators, list literals, list comprehensions, CASE,
// head and coalesce) to an operand of every type class: concrete values, an
// operator signature's result (n.k - 2), unions with and without null,
// parameters, a property value, and temporal values and durations. Each
// result is read through a WITH projection by consumers that reject on its
// type.
func staticConstructorTypesPopulation() []string {
	seeds := []string{
		"1", "1.5", "'a'", "true", "[1]", "['a']", "[[1]]", "{a: 1}", "n", "null",
		"n.k - 2", "n.k * 1.5", "n.k + 'a'", "n.k + 1", "-n.k", "+n.k", "n.k / 2", "n.k % 2",
		"(CASE WHEN true THEN n.k ELSE [1] END) - 2", "n.k - duration('PT1S')", "n.k ^ 2",
		"CASE WHEN true THEN 1 ELSE 'a' END", "CASE WHEN true THEN 1 ELSE true END",
		"CASE WHEN true THEN 1.5 ELSE 1 END", "CASE WHEN true THEN [1] ELSE ['a'] END",
		"CASE WHEN true THEN {a: 1} ELSE 1 END", "CASE WHEN true THEN n ELSE true END",
		"CASE WHEN true THEN date('2026-10-22') ELSE duration('P1D') END",
		"CASE WHEN true THEN [1] ELSE true END",
		"CASE WHEN true THEN null ELSE 1 END", "CASE WHEN true THEN null ELSE [1] END",
		"$i", "$f", "$s", "$l", "$m", "$z", "$b",
		"n.k",
		"duration('PT1S')", "date('2026-10-22')", "datetime('2026-10-22T10:00:00Z')", "localtime('10:00')",
		"duration('PT1S') + duration('PT1S')", "date('2026-10-22') + duration('P1D')",
	}
	constructors := []string{
		"%s",
		"-(%s)", "+(%s)",
		"(%s) + 1", "(%s) - 1", "(%s) * 2", "(%s) / 2", "(%s) % 2", "(%s) ^ 2", "(%s) + 'a'",
		"(%s) + [1]", "1 - (%s)", "(%s) + (%s)", "(%s) * 1.5", "(%s) - duration('PT1S')",
		"(%s) * duration('PT1S')",
		"[%s]", "[%s, %s]", "[%s, 1]", "[1, %s]", "[%s, 'a']", "[[%s]]",
		"[x IN [%s] | x]", "[x IN %s | x]", "[x IN [%s] WHERE true]", "[x IN [%s] | -x]",
		"[x IN [1] | %s]", "[x IN [%s] | [x]]",
		"CASE WHEN true THEN %s ELSE %s END", "CASE WHEN true THEN %s ELSE 1 END",
		"CASE WHEN true THEN %s ELSE 'a' END", "CASE WHEN true THEN %s ELSE null END",
		"CASE WHEN true THEN %s END", "CASE 1 WHEN 1 THEN %s ELSE [1] END",
		"head([%s])", "coalesce(%s, %s)",
	}
	consumers := []string{
		"RETURN toUpper(v) AS a", "RETURN v.day AS a", "RETURN v[0] AS a", "RETURN NOT v AS a",
		"RETURN abs(v) AS a", "RETURN [x IN [v] | toUpper(x)] AS a", "SET n += v RETURN 1 AS a",
		"MATCH (v) RETURN 1 AS a",
	}
	var population []string
	for _, seed := range seeds {
		for _, constructor := range constructors {
			expression := strings.ReplaceAll(constructor, "%s", seed)
			for _, consumer := range consumers {
				population = append(population, "UNWIND [] AS r MATCH (n) WITH n, "+expression+" AS v "+consumer)
			}
		}
	}
	return population
}

// Every composite constructor carries its operands' static types, as the
// one type owner derives them, to the consumer that decides: the population
// is rejected exactly where Neo4j 5.26.30 rejects it (a query Neo4j accepts
// is never rejected, and one it rejects is never accepted), in auto-commit
// and in an explicit transaction. The listed queries need parameter-aware
// admission (staticConstructorTypesOracle).
func TestStaticConstructorTypesMatchNeo4j(t *testing.T) {
	var oracle struct {
		Population       int    `json:"population"`
		PopulationSha256 string `json:"populationSha256"`
		Neo4jRejected    []int  `json:"neo4jRejected"`
		Inherited        []int  `json:"inherited"`
	}
	require.NoError(t, json.Unmarshal(staticConstructorTypesOracle, &oracle))
	population := staticConstructorTypesPopulation()
	digest := sha256.Sum256([]byte(strings.Join(population, "\n")))
	// The verdicts are pinned by position: a changed population needs the
	// oracle taken again from Neo4j.
	require.Equal(t, oracle.Population, len(population))
	require.Equal(t, oracle.PopulationSha256, hex.EncodeToString(digest[:]))
	rejected, inherited := map[int]bool{}, map[int]bool{}
	for _, index := range oracle.Neo4jRejected {
		rejected[index] = true
	}
	for _, index := range oracle.Inherited {
		inherited[index] = true
	}
	ctx := context.Background()
	params := staticConstructorTypesParams()
	for _, mode := range []string{"auto", "tx"} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "static_constructor_types_"+mode))
		for index, query := range population {
			if inherited[index] {
				continue
			}
			if mode == "tx" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, query, params)
			if mode == "tx" {
				_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			}
			switch {
			case err != nil && !rejected[index]:
				t.Errorf("%s: rejected, Neo4j accepts: %s: %v", mode, query, err)
			case err == nil && rejected[index]:
				t.Errorf("%s: accepted, Neo4j rejects: %s", mode, query)
			}
		}
	}
}
