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

// staticTypingProofOracle holds the verdicts for staticTypingProofPopulation,
// by position: the queries Neo4j 5.26.30 rejects, and the queries #1552's
// head (9ea9fb11a) already rejected where Neo4j doesn't.
//
//go:embed testdata/static_typing_proof_oracle.json
var staticTypingProofOracle []byte

// staticTypingProofPopulation is a generated population of nested CASE, list
// and map shapes (scalar, list, nested-list, map and entity unions, unary
// results, a CASE right after a map key's colon, null unions, a property
// value, a reduce result) under list, map, CASE, coalesce, tail, reverse,
// reduce and arithmetic wrappers, read by every kind of consumer: predicate positions,
// list, string and numeric functions, operators, subscripts and property
// access. Each consumer reads the shape once through a WITH projection and
// once written in place.
func staticTypingProofPopulation() []string {
	shapes := []string{
		"CASE WHEN true THEN 1 ELSE true END",
		"CASE WHEN true THEN 1 ELSE 'a' END",
		"CASE WHEN true THEN 1.5 ELSE 1 END",
		"CASE WHEN true THEN [1] ELSE ['a'] END",
		"CASE WHEN true THEN [1] ELSE [true] END",
		"CASE WHEN true THEN [1] ELSE 1 END",
		"CASE WHEN true THEN [[1]] ELSE [['a']] END",
		"CASE WHEN true THEN [CASE WHEN true THEN [1] ELSE ['a'] END] ELSE [[true]] END",
		"CASE WHEN true THEN {a:CASE WHEN true THEN 1 ELSE false END} ELSE {} END",
		"CASE WHEN true THEN {a: 1} ELSE 1 END",
		"CASE WHEN true THEN n ELSE true END",
		"-CASE WHEN true THEN 1 ELSE true END",
		"CASE WHEN true THEN null ELSE [1] END",
		"CASE WHEN true THEN null ELSE 1 END",
		"n.k",
		"reduce(a = CASE WHEN true THEN 1 ELSE true END, x IN [] | a)",
	}
	wrappers := []string{"%s", "[%s]", "[%s, %s]", "[[%s]]", "{k:%s}.k", "CASE WHEN true THEN %s ELSE %s END", "coalesce(%s, %s)", "head([%s])",
		"tail(%s)", "reverse(%s)", "reduce(a = %s, x IN [] | a)", "(%s) - 1"}
	consumers := []string{
		"all(x IN [1] WHERE %s)",
		"[x IN [1] WHERE %s | x]",
		"%s AND true",
		"NOT %s",
		"CASE WHEN %s THEN 1 ELSE 2 END",
		"all(x IN %s WHERE CASE WHEN true THEN x ELSE 1 END)",
		"head(%s)",
		"[x IN %s | x]",
		"size(%s)",
		"%s[0]",
		"toUpper(%s)",
		"toUpper(head(%s))",
		"%s + 1",
		"-%s",
		"abs(%s)",
		"%s.a",
		"keys(%s)",
		"labels(%s)",
	}
	fill := func(template, value string) string { return strings.ReplaceAll(template, "%s", value) }
	var population []string
	seen := map[string]bool{}
	add := func(query string) {
		if !seen[query] {
			seen[query] = true
			population = append(population, query)
		}
	}
	for _, shape := range shapes {
		for _, wrapper := range wrappers {
			wrapped := fill(wrapper, shape)
			for _, consumer := range consumers {
				add("UNWIND [] AS r MATCH (n) WITH n, " + wrapped + " AS v RETURN " + fill(consumer, "v") + " AS a")
				add("UNWIND [] AS r MATCH (n) RETURN " + fill(consumer, wrapped) + " AS a")
			}
		}
	}
	return population
}

// A static check rejects a query only on proof: the generated population of
// nested shapes is rejected only where Neo4j 5.26.30 rejects it, or where
// #1552's head already did. Any other rejection (a derivation that claims a
// type it didn't fully derive) fails, in auto-commit and in an explicit
// transaction.
func TestStaticTypingRejectsOnlyOnProof(t *testing.T) {
	var oracle struct {
		Population       int    `json:"population"`
		PopulationSha256 string `json:"populationSha256"`
		Neo4jRejected    []int  `json:"neo4jRejected"`
		BaseRejected     []int  `json:"baseRejected"`
	}
	require.NoError(t, json.Unmarshal(staticTypingProofOracle, &oracle))
	population := staticTypingProofPopulation()
	digest := sha256.Sum256([]byte(strings.Join(population, "\n")))
	// The verdicts are pinned by position: a changed population needs the
	// oracle taken again from Neo4j and #1552's head.
	require.Equal(t, oracle.Population, len(population))
	require.Equal(t, oracle.PopulationSha256, hex.EncodeToString(digest[:]))
	neo4jRejected := map[string]bool{}
	for _, index := range oracle.Neo4jRejected {
		neo4jRejected[population[index]] = true
	}
	proven := map[string]bool{}
	for _, index := range append(append([]int(nil), oracle.Neo4jRejected...), oracle.BaseRejected...) {
		proven[population[index]] = true
	}
	ctx := context.Background()
	for _, mode := range []string{"auto", "tx"} {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "static_typing_proof_"+mode))
		for _, query := range population {
			if mode == "tx" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			_, err := exec.Execute(ctx, query, nil)
			if mode == "tx" {
				_, _ = exec.Execute(ctx, "ROLLBACK", nil)
			}
			if err == nil && neo4jRejected[query] {
				t.Errorf("%s: lost Neo4j rejection: %s", mode, query)
			}
			if err != nil && !proven[query] {
				t.Errorf("%s: rejected without proof: %s: %v", mode, query, err)
			}
		}
	}
}
